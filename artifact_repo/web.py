"""Flask web UI and HTTP endpoints for the artifact repository."""

from __future__ import annotations

import hashlib
import http.client
import logging
import threading
import time
import uuid

from flask import (
    Flask,
    Response,
    abort,
    jsonify,
    redirect,
    render_template,
    request,
    stream_with_context,
    url_for,
)

from .geometry import MAX_SHARDS
from .serving import (
    ServeStats,
    UnsatisfiableRange,
    digest_range,
    iter_product_bytes,
    parse_range_header,
    plan_open_count,
)
from .store import (
    DigestMismatch,
    NoRelease,
    Repo,
    ValidationError,
    WriteFailure,
)

log = logging.getLogger(__name__)

# The stable address always serves whichever release is active *at request
# start*; that release is pinned for the whole response, so an in-flight
# download keeps reading the old product after a new one is published.
STABLE_NAME = "current"


class TrialRegistry:
    """Carries serve statistics from loopback HTTP trials back to /verify.

    The trial token is issued and consumed entirely inside the process.  Only
    a token that was issued and is still outstanding may ask the serving
    generator to hand over its release pin, so an outside request cannot keep
    a retired release (and its body files) alive by sending a made-up token.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stats: dict[str, ServeStats] = {}
        self._events: dict[str, threading.Event] = {}
        self._issued: set[str] = set()
        # Release pins deliberately held past the response so the caller can
        # compare against the exact bytes that were served.
        self._held: dict[str, int] = {}

    def prepare(self, token: str) -> threading.Event:
        """Issue a token before its trial request is sent."""
        with self._lock:
            self._issued.add(token)
            return self._events.setdefault(token, threading.Event())

    def claim(self, token: str | None) -> bool:
        """Mark an issued token as belonging to the response now streaming."""
        if not token:
            return False
        with self._lock:
            if token not in self._issued or token in self._stats:
                return False
            self._issued.discard(token)
            return True

    def hold(self, token: str, release_id: int) -> None:
        with self._lock:
            self._held[token] = release_id

    def held(self, token: str) -> int | None:
        with self._lock:
            return self._held.get(token)

    def release(self, token: str) -> None:
        with self._lock:
            self._held.pop(token, None)
            self._stats.pop(token, None)
            self._events.pop(token, None)
            self._issued.discard(token)

    def submit(self, token: str, stats: ServeStats) -> None:
        with self._lock:
            self._stats[token] = stats
            event = self._events.setdefault(token, threading.Event())
        event.set()

    def wait(self, token: str, timeout: float = 15.0) -> ServeStats | None:
        with self._lock:
            event = self._events.get(token)
        if event is None or not event.wait(timeout):
            return None
        with self._lock:
            return self._stats.get(token)


def _unpin_once(repo: Repo, release_id: int):
    """Return an idempotent unpin callable for one served response."""
    done = threading.Lock()
    released = False

    def unpin() -> None:
        nonlocal released
        with done:
            if released:
                return
            released = True
        try:
            repo.release_pin(release_id)
        except Exception:  # pragma: no cover - defensive
            log.exception("failed to unpin release %s", release_id)

    return unpin


def _stream_file(fh):
    try:
        while True:
            block = fh.read(64 * 1024)
            if not block:
                break
            yield block
    finally:
        fh.close()


def _product_headers(release, status: int, start: int, length: int) -> dict:
    headers = {
        "Accept-Ranges": "bytes",
        "ETag": f'"{release.product_sha256}"',
        "Cache-Control": "no-cache",
        "X-Release-Id": str(release.id),
        "Content-Length": str(length),
    }
    if status == 206:
        headers["Content-Range"] = (
            f"bytes {start}-{start + length - 1}/{release.total_size}"
        )
    return headers


def create_app(repo: Repo, *, simulate_write_failure: bool = False) -> Flask:
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 8 * 1024 * 1024 * 1024  # 8 GiB per shard
    app.extensions["repo"] = repo
    app.extensions["simulate_write_failure"] = simulate_write_failure
    registry = TrialRegistry()
    app.extensions["trial_registry"] = registry

    # ------------------------------------------------------------ product
    def _resolve_range(release):
        try:
            return parse_range_header(request.headers.get("Range"), release.total_size)
        except UnsatisfiableRange:
            resp = Response(status=416)
            resp.headers["Content-Range"] = f"bytes */{release.total_size}"
            resp.headers["Accept-Ranges"] = "bytes"
            return resp

    @app.route("/artifact/<path:name>", methods=["GET"])
    def download(name: str):
        try:
            release = repo.pin_active_release()
        except NoRelease:
            abort(404)
        handed_off = False
        try:
            if name not in (STABLE_NAME, release.filename):
                abort(404)
            spec = _resolve_range(release)
            if isinstance(spec, Response):
                return spec
            if spec is None:
                start, length, status = 0, release.total_size, 200
            else:
                start, length, status = spec.start, spec.length, 206
            stats = ServeStats(
                release_id=release.id,
                start=start,
                length=length,
                planned_opens=plan_open_count(release, start, length),
            )
            headers = _product_headers(release, status, start, length)
            headers["Content-Disposition"] = (
                f'attachment; filename="{release.filename}"'
            )
            token = request.headers.get("X-Trial-Token")
            if not (
                request.remote_addr in ("127.0.0.1", "::1")
                and registry.claim(token)
            ):
                token = None
            release_id = release.id
            handed_off = True
            if token:
                registry.hold(token, release_id)

            unpin = _unpin_once(repo, release_id)
            if token:
                unpin = lambda: None  # the trial caller owns this pin

            def generate():
                try:
                    yield from iter_product_bytes(repo, release, start, length, stats)
                finally:
                    if token:
                        # The trial caller compares against the served bytes
                        # afterwards, so hand it the still-held pin.
                        registry.submit(token, stats)
                    else:
                        unpin()

            response = Response(
                stream_with_context(generate()), status=status, headers=headers
            )
            # Defence in depth: if the server closes the response without ever
            # iterating the generator, call_on_close still releases the pin.
            response.call_on_close(unpin)
            return response
        finally:
            if not handed_off:
                repo.release_pin(release.id)

    @app.route("/artifact/<path:name>", methods=["HEAD"])
    def download_head(name: str):
        try:
            release = repo.pin_active_release()
        except NoRelease:
            abort(404)
        try:
            if name not in (STABLE_NAME, release.filename):
                abort(404)
            spec = _resolve_range(release)
            if isinstance(spec, Response):
                return spec
            if spec is None:
                headers = _product_headers(release, 200, 0, release.total_size)
                return Response(status=200, headers=headers)
            headers = _product_headers(release, 206, spec.start, spec.length)
            return Response(status=206, headers=headers)
        finally:
            repo.release_pin(release.id)

    # --------------------------------------------------------------- shards
    @app.get("/shards/<int:number>")
    def shard_body(number: int):
        for shard in repo.staged_shards():
            if shard.number == number:
                fh = open(repo.body_path_for(shard.body_id), "rb")
                response = Response(
                    _stream_file(fh),
                    mimetype="application/octet-stream",
                    headers={
                        "Content-Length": str(shard.size),
                        "X-Shard-Sha256": shard.sha256,
                        "Content-Disposition": (
                            f'attachment; filename="shard-{number}.bin"'
                        ),
                    },
                )
                response.call_on_close(fh.close)
                return response
        abort(404)

    # ---------------------------------------------------------------- pages
    def default_trial_range(release) -> tuple[int, int]:
        """Default to a range straddling a segment boundary when possible."""
        total = release.total_size
        if total <= 0:
            return 0, 0
        if len(release.segments) > 1:
            first, second = release.segments[0], release.segments[1]
            boundary = first.offset + first.size
            start = max(0, boundary - min(16, first.size))
            length = min(total - start, 32 + min(second.size, 32))
            return start, max(1, length)
        return 0, total

    def context() -> dict:
        active = repo.get_active_release()
        trial_start, trial_length = (
            default_trial_range(active) if active is not None else (0, 0)
        )
        return {
            "stats": repo.stats(),
            "staged": repo.staged_shards(),
            "active": active,
            "releases": repo.release_list(8),
            "attempts": repo.recent_attempts(6),
            "prefill": repo.manifest_prefill(),
            "max_shards": MAX_SHARDS,
            "product_url": url_for("download", name=STABLE_NAME)
            if active is not None
            else None,
            "trial_start": trial_start,
            "trial_length": trial_length,
            "flash": request.args.get("msg"),
            "error": request.args.get("err"),
        }

    @app.get("/")
    def index():
        return render_template("index.html", **context())

    @app.post("/shards")
    def upload_shards():
        number_raw = request.form.get("number", "").strip()
        try:
            number = int(number_raw, 10)
        except ValueError:
            return redirect(url_for("index", err="分片号必须是 1..12 的整数"))
        files = [f for f in request.files.getlist("file") if f and f.filename]
        if not files:
            return redirect(url_for("index", err="请选择一个分片文件"))
        try:
            shard = repo.store_shard(number, files[0].stream)
        except (ValidationError, WriteFailure) as exc:
            return redirect(url_for("index", err=str(exc)))
        return redirect(
            url_for(
                "index",
                msg=f"分片 {number} 已保存，{shard.size} 字节，SHA-256 {shard.sha256}",
            )
        )

    @app.post("/shards/<int:number>/delete")
    def delete_shard(number: int):
        repo.delete_staged_shard(number)
        return redirect(url_for("index", msg=f"分片 {number} 已删除"))

    @app.post("/publish")
    def publish():
        manifest_text = request.form.get("manifest", "")
        hot_text = request.form.get("hot", "")
        max_write_raw = request.form.get("max_write", "").strip()
        filename = request.form.get("filename", "install.img").strip() or "install.img"
        try:
            max_write = int(max_write_raw, 10)
        except ValueError:
            repo.record_attempt(False, "新写正文上限不是整数", {})
            return redirect(url_for("index", err="新写正文字节上限必须是整数"))
        simulate = request.form.get("simulate_failure") == "on"
        started = time.monotonic()
        try:
            result = repo.publish(
                manifest_text=manifest_text,
                hot_ranges_text=hot_text,
                max_write=max_write,
                filename=filename,
                simulate_write_failure=simulate
                or app.extensions["simulate_write_failure"],
            )
        except (ValidationError, DigestMismatch, WriteFailure) as exc:
            elapsed = time.monotonic() - started
            repo.record_attempt(False, str(exc), {"elapsed_s": round(elapsed, 3)})
            return redirect(
                url_for("index", err=f"发布失败，保留旧成品与已收分片：{exc}")
            )
        elapsed = time.monotonic() - started
        repo.record_attempt(
            True,
            f"发布 #{result.release.id} 成功",
            {
                "release_id": result.release.id,
                "segments": [
                    {
                        "numbers": list(s.numbers),
                        "offset": s.offset,
                        "size": s.size,
                        "source": s.source,
                    }
                    for s in result.release.segments
                ],
                "new_write_bytes": result.release.new_write_bytes,
                "elapsed_s": round(elapsed, 3),
                "product_sha256": result.actual_sha256,
            },
        )
        return redirect(
            url_for(
                "index",
                msg=f"发布 #{result.release.id} 成功，成品 {result.release.total_size} "
                f"字节，新写正文 {result.release.new_write_bytes} 字节",
            )
        )

    @app.post("/gc")
    def gc():
        result = repo.gc()
        return redirect(
            url_for(
                "index",
                msg=f"回收：释放 {len(result.deleted_releases)} 个已退役发布的正文引用、"
                f"删除 {len(result.deleted_bodies)} 个无引用正文",
            )
        )

    # --------------------------------------------------------------- trial
    def _loopback_range_trial(path: str, start: int, length: int, token: str):
        """Issue a genuine HTTP Range request against a loopback listener.

        The request travels over TCP to a temporary 127.0.0.1 listener running
        this same Flask app, so the status line, headers and streamed body are
        the real HTTP response.  Server-side serve statistics are matched back
        to this request by an opaque trial token.
        """
        from werkzeug.serving import make_server

        registry.prepare(token)
        server = make_server("127.0.0.1", 0, app, threaded=True)
        port = server.server_port
        thread = threading.Thread(
            target=server.serve_forever, name="trial-range-http", daemon=True
        )
        thread.start()
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=120)
            conn.request(
                "GET",
                path,
                headers={
                    "Range": f"bytes={start}-{start + length - 1}",
                    "X-Trial-Token": token,
                },
            )
            resp = conn.getresponse()
            hasher = hashlib.sha256()
            received = 0
            while True:
                block = resp.read(64 * 1024)
                if not block:
                    break
                hasher.update(block)
                received += len(block)
            status = resp.status
            headers = {k.lower(): v for k, v in resp.getheaders()}
            conn.close()
            stats = registry.wait(token, timeout=15.0)
        finally:
            server.shutdown()
            thread.join(timeout=5)
        return status, headers, received, hasher.hexdigest(), stats

    @app.get("/verify")
    def verify():
        active = repo.get_active_release()
        if active is None:
            return redirect(url_for("index", err="尚未发布成品，无法试取"))
        try:
            start = int(request.args.get("start", "0"), 10)
            length = int(request.args.get("length", "0"), 10)
        except ValueError:
            return redirect(url_for("index", err="试取区间必须是整数"))
        if length <= 0 or start < 0 or start + length > active.total_size:
            return redirect(
                url_for("index", err=f"试取区间无效，成品共 {active.total_size} 字节")
            )
        token = uuid.uuid4().hex
        status, headers, received, http_digest, stats = _loopback_range_trial(
            url_for("download", name=STABLE_NAME), start, length, token
        )
        # The trial always sends a Range header, so a successful response is
        # 206 Partial Content even when the range covers the whole product.
        expected_status = 206
        # Compare against the release that actually served the request, which
        # may differ from the one seen before the trial if a publish landed in
        # between.  The serving generator kept its pin, so these bodies cannot
        # be collected while we compare.
        served_id = int(headers.get("x-release-id", active.id))
        pinned = repo.get_release(served_id)
        if pinned is None:
            held = registry.held(token)
            if held is not None:
                repo.release_pin(held)
            registry.release(token)
            return redirect(url_for("index", err="试取期间成品已被回收，请重试"))
        held = registry.held(token)
        if held is None:
            # The response never reached the streaming generator (e.g. a 404),
            # so take our own pin for the comparison below.
            try:
                held = repo.pin_release(served_id).id
            except NoRelease:
                return redirect(url_for("index", err="试取期间成品已被回收，请重试"))
            registry.hold(token, held)
        try:
            local_digest = digest_range(repo, pinned, start, length)
            served_segments = [
                {
                    "numbers": list(s.numbers),
                    "offset": s.offset,
                    "size": s.size,
                    "source": s.source,
                }
                for s in pinned.segments
                if s.offset < start + length and s.offset + s.size > start
            ]
            planned_opens = plan_open_count(pinned, start, length)
        except OSError as exc:
            return redirect(url_for("index", err=f"试取核对失败：{exc}"))
        finally:
            repo.release_pin(held)
            registry.release(token)
        summary = {
            "request": (
                f"GET /artifact/{STABLE_NAME} · Range: bytes="
                f"{start}-{start + length - 1}"
            ),
            "status": status,
            "expected_status": expected_status,
            "content_range": headers.get("content-range", "（整份响应，200）"),
            "content_length": headers.get("content-length", str(received)),
            "received_bytes": received,
            "http_sha256": http_digest,
            "local_sha256": local_digest,
            "match": http_digest == local_digest and received == length,
            "start": start,
            "length": length,
            "body_files_opened": stats.body_files_opened if stats else None,
            "planned_opens": planned_opens,
            "release_id": pinned.id,
            "release_new_write_bytes": pinned.new_write_bytes,
            "segments": served_segments,
        }
        return render_template("verify.html", summary=summary, **context())

    # ------------------------------------------------------------------- API
    @app.get("/api/state")
    def api_state():
        active = repo.get_active_release()

        def seg_json(release):
            return [
                {
                    "numbers": list(s.numbers),
                    "offset": s.offset,
                    "size": s.size,
                    "source": s.source,
                }
                for s in release.segments
            ]

        return jsonify(
            {
                "active": None
                if active is None
                else {
                    "id": active.id,
                    "filename": active.filename,
                    "total_size": active.total_size,
                    "new_write_bytes": active.new_write_bytes,
                    "product_sha256": active.product_sha256,
                    "segments": seg_json(active),
                    "hot_checks": active.hot_checks,
                },
                "staged": [
                    {"number": s.number, "size": s.size, "sha256": s.sha256}
                    for s in repo.staged_shards()
                ],
                "stats": repo.stats(),
            }
        )

    @app.get("/api/range-check")
    def api_range_check():
        active = repo.get_active_release()
        if active is None:
            return jsonify({"error": "尚未发布成品"}), 404
        try:
            start = int(request.args.get("start", "0"), 10)
            length = int(request.args.get("length", "0"), 10)
        except ValueError:
            return jsonify({"error": "区间必须是整数"}), 400
        if length <= 0 or start < 0 or start + length > active.total_size:
            return jsonify({"error": "区间越界"}), 400
        return jsonify(
            {
                "release_id": active.id,
                "start": start,
                "length": length,
                "body_files_opened": plan_open_count(active, start, length),
                "segments": [
                    {
                        "numbers": list(s.numbers),
                        "offset": s.offset,
                        "size": s.size,
                        "source": s.source,
                    }
                    for s in active.segments
                    if s.offset < start + length and s.offset + s.size > start
                ],
            }
        )

    return app
