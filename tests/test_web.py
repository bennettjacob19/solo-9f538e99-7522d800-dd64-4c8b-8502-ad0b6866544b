import hashlib
import io
import os
import tempfile
import unittest

from artifact_repo.serving import (
    ServeStats,
    UnsatisfiableRange,
    iter_product_bytes,
    parse_range_header,
)
from artifact_repo.store import Repo
from artifact_repo.web import create_app


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class RangeParsingTests(unittest.TestCase):
    def test_plain(self):
        spec = parse_range_header("bytes=10-19", 100)
        self.assertEqual((spec.start, spec.end, spec.length), (10, 19, 10))

    def test_open_ended(self):
        spec = parse_range_header("bytes=90-", 100)
        self.assertEqual((spec.start, spec.end), (90, 99))

    def test_suffix(self):
        spec = parse_range_header("bytes=-10", 100)
        self.assertEqual((spec.start, spec.end), (90, 99))

    def test_clamped_end(self):
        spec = parse_range_header("bytes=95-1000", 100)
        self.assertEqual(spec.end, 99)

    def test_unsatisfiable(self):
        with self.assertRaises(UnsatisfiableRange):
            parse_range_header("bytes=100-", 100)
        with self.assertRaises(UnsatisfiableRange):
            parse_range_header("bytes=20-10", 100)

    def test_ignored(self):
        self.assertIsNone(parse_range_header(None, 100))
        self.assertIsNone(parse_range_header("items=0-1", 100))
        self.assertIsNone(parse_range_header("bytes=0-1,5-6", 100))


class WebTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Repo(self.tmp.name)
        self.parts = [
            bytes(range(256)) * 8,          # 2048
            bytes(range(255, -1, -1)) * 16,  # 4096
            b"\x00" * 1024,
            os.urandom(3000),
        ]
        for n, p in enumerate(self.parts, 1):
            self.repo.store_shard(n, io.BytesIO(p))
        self.product = b"".join(self.parts)
        self.manifest = "\n".join(
            f"{n} {sha(p)}" for n, p in enumerate(self.parts, 1)
        )
        self.app = create_app(self.repo)
        self.client = self.app.test_client()

    def tearDown(self):
        self.tmp.cleanup()

    def publish(self, hot="", max_write=0, **kw):
        return self.repo.publish(
            manifest_text=self.manifest,
            hot_ranges_text=hot,
            max_write=max_write,
            **kw,
        )

    def test_upload_and_replace_via_form(self):
        client = self.app.test_client()
        resp = client.post(
            "/shards",
            data={"number": "5", "file": (io.BytesIO(b"hello"), "s5.bin")},
            content_type="multipart/form-data",
        )
        self.assertEqual(resp.status_code, 302)
        resp = client.post(
            "/shards",
            data={"number": "5", "file": (io.BytesIO(b"world!"), "s5.bin")},
            content_type="multipart/form-data",
        )
        staged = {s.number: s for s in self.repo.staged_shards()}
        self.assertEqual(staged[5].size, 6)
        self.assertEqual(staged[5].sha256, sha(b"world!"))

    def test_range_requests_match_product(self):
        self.publish(hot="0 10168 4", max_write=10168)
        cases = [(0, 1), (2047, 2), (0, 10168), (2048, 4096), (5000, 3000), (10167, 1)]
        for start, length in cases:
            resp = self.client.get(
                "/artifact/current",
                headers={"Range": f"bytes={start}-{start + length - 1}"},
            )
            self.assertEqual(resp.status_code, 206)
            self.assertEqual(resp.headers["Content-Range"], f"bytes {start}-{start + length - 1}/10168")
            self.assertEqual(resp.data, self.product[start : start + length])
        full = self.client.get("/artifact/current")
        self.assertEqual(full.status_code, 200)
        self.assertEqual(full.data, self.product)
        self.assertEqual(full.headers["ETag"], f'"{sha(self.product)}"')

    def test_416(self):
        self.publish()
        resp = self.client.get("/artifact/current", headers={"Range": "bytes=99999-"})
        self.assertEqual(resp.status_code, 416)
        self.assertEqual(resp.headers["Content-Range"], "bytes */10168")

    def test_head(self):
        self.publish()
        resp = self.client.head("/artifact/current", headers={"Range": "bytes=0-99"})
        self.assertEqual(resp.status_code, 206)
        self.assertEqual(resp.headers["Content-Length"], "100")
        self.assertEqual(resp.data, b"")

    def test_verify_reports_real_open_count_and_write_bytes(self):
        result = self.publish(hot="0 10168 2", max_write=10168)
        # Choose a range that crosses a segment boundary in the actual layout.
        segs = result.release.segments
        boundary = segs[0].offset + segs[0].size
        start = boundary - 10
        length = 20
        resp = self.client.get(f"/verify?start={start}&length={length}")
        self.assertEqual(resp.status_code, 200)
        body = resp.get_data(as_text=True)
        self.assertIn(f'id="newwrite">{result.release.new_write_bytes}<', body)
        expected_opens = len(
            [s for s in segs if s.offset < start + length and s.offset + s.size > start]
        )
        self.assertIn(f'id="opened">{expected_opens}<', body)
        self.assertIn("收到的字节与本地成品快照一致", body)

    def test_publish_form_rejects_bad_digest_and_keeps_old(self):
        self.publish()
        old = self.repo.get_active_release()
        for n, p in enumerate(self.parts, 1):
            self.repo.store_shard(n, io.BytesIO(p))
        bad = self.manifest.replace(sha(self.parts[0]), "f" * 64)
        resp = self.client.post(
            "/publish",
            data={"manifest": bad, "hot": "", "max_write": "0", "filename": "install.img"},
            follow_redirects=True,
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(self.repo.get_active_release().id, old.id)
        self.assertEqual(len(self.repo.staged_shards()), 4)
        index = resp.get_data(as_text=True)
        self.assertIn("发布失败", index)
        self.assertIn("已收分片", index)

    def test_stable_url_switches_and_inflight_reader_finishes_old(self):
        first = self.publish()
        pinned = self.repo.pin_active_release()
        self.assertEqual(pinned.id, first.release.id)
        stats = ServeStats()
        # Start streaming one byte at a time and pause mid-way.
        stream = iter_product_bytes(
            self.repo, pinned, 0, len(self.product), stats
        )
        head = next(stream)
        self.assertTrue(self.product.startswith(head))

        # Publish a new product while the reader is mid-flight.
        new_parts = [b"Q" * 2048] + self.parts[1:]
        for n, p in enumerate(new_parts, 1):
            self.repo.store_shard(n, io.BytesIO(p))
        manifest2 = "\n".join(f"{n} {sha(p)}" for n, p in enumerate(new_parts, 1))
        self.repo.publish(manifest_text=manifest2, hot_ranges_text="", max_write=0)
        new_product = b"".join(new_parts)

        # The stable URL now serves the new product.
        resp = self.client.get(
            "/artifact/current", headers={"Range": "bytes=0-1023"}
        )
        self.assertEqual(resp.data, new_product[:1024])
        # The pinned old release is still fully readable.
        rest = b"".join(stream)
        self.assertEqual(head + rest, self.product)
        self.repo.release_pin(pinned.id)

    def test_old_body_survives_until_unpinned(self):
        first = self.publish()
        old_bodies = {s.body_id: self.repo.body_path_for(s.body_id) for s in first.release.segments}
        pinned = self.repo.pin_active_release()
        new_parts = [b"Z" * 2048] + self.parts[1:]
        for n, p in enumerate(new_parts, 1):
            self.repo.store_shard(n, io.BytesIO(p))
        self.repo.publish(
            manifest_text="\n".join(f"{n} {sha(p)}" for n, p in enumerate(new_parts, 1)),
            hot_ranges_text="",
            max_write=0,
        )
        for body_id, path in old_bodies.items():
            self.assertTrue(
                os.path.exists(path),
                "body referenced by a pinned release must survive",
            )
        self.repo.release_pin(pinned.id)
        for body_id, path in old_bodies.items():
            self.assertFalse(os.path.exists(path))
            self.assertIsNone(self.repo.get_release and self._body_row(body_id))

    def _body_row(self, body_id):
        import sqlite3

        conn = sqlite3.connect(self.repo.db_path)
        try:
            row = conn.execute(
                "SELECT id FROM bodies WHERE id=?", (body_id,)
            ).fetchone()
        finally:
            conn.close()
        return row

    def test_reuse_across_releases(self):
        self.publish()
        # Same content uploaded again; digest identical, all reuse, zero write.
        for n, p in enumerate(self.parts, 1):
            self.repo.store_shard(n, io.BytesIO(p))
        second = self.publish()
        self.assertEqual(second.release.new_write_bytes, 0)
        self.assertEqual(second.actual_sha256, sha(self.product))

    def test_shard_numbers_reusable_in_next_round(self):
        self.publish()
        self.repo.store_shard(1, io.BytesIO(b"n" * 100))
        result = self.repo.publish(
            manifest_text=f"1 {sha(b'n' * 100)}", hot_ranges_text="", max_write=0
        )
        self.assertEqual(result.release.total_size, 100)
        self.assertEqual(self.client.get("/artifact/current").data, b"n" * 100)

    def test_api_state(self):
        self.publish()
        data = self.client.get("/api/state").get_json()
        self.assertEqual(data["active"]["total_size"], 10168)
        self.assertEqual(data["active"]["product_sha256"], sha(self.product))


if __name__ == "__main__":
    unittest.main()
