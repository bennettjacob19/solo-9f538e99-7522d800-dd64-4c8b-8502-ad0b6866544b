"""Persistence, publication transactions and body-file garbage collection.

Storage model
-------------
Bodies are immutable files under ``<data>/bodies``.  A *staged shard* row
points at one body; replacing a shard stores a new body and simply re-points
the row, so an already-published release that still uses the old body stays
intact.  Staged shards survive publication, so the next build can replace
only the changed numbers and republish the rest unchanged (zero new bytes).
A *release* references the bodies of its segments through
``release_bodies``.

Publication is pull-based and does not hold the state lock while it writes
bodies: the old release remains the active one until every shard has been
verified, every new body has been written and the assembled product has been
re-hashed.  Only then does one short DB transaction insert the release and point
``meta.active_release_id`` at it; the previous release is retired in the same
transaction but keeps its body references while readers pin it.

Readers pin a release for the duration of an HTTP range response.  A retired
release, and hence its body files, may only be deleted once ``pinned_readers``
reaches zero, which is what lets an in-flight download finish reading the old
product after a new release has taken over the stable URL.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import BinaryIO, Iterable, Sequence

from .geometry import (
    HotRange,
    ManifestEntry,
    Plan,
    PlanError,
    make_plan,
    parse_hot_ranges,
    parse_manifest_entries,
)

log = logging.getLogger(__name__)

SCHEMA_VERSION = "2"
CHUNK = 1024 * 1024
MAX_SHARDS = 12
FILENAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class RepoError(Exception):
    """Base class for user-visible repository errors."""


class ValidationError(RepoError):
    pass


class DigestMismatch(RepoError):
    pass


class WriteFailure(RepoError):
    pass


class NoRelease(RepoError):
    pass


class RecoveryError(RepoError):
    """A shard-recovery request cannot be satisfied as submitted."""


@dataclass(frozen=True)
class Body:
    id: str
    rel_path: str
    size: int
    sha256: str


@dataclass(frozen=True)
class StagedShard:
    number: int
    body_id: str
    rel_path: str
    size: int
    sha256: str
    updated_at: str
    # Where the shard's bytes live inside its immutable body.  Ordinary
    # uploads span the whole file (offset 0); shards recovered while sharing
    # a published body occupy a sub-range of it.
    body_offset: int = 0
    origin: str = "upload"  # "upload" | "recovered-reuse" | "recovered-copy"


@dataclass(frozen=True)
class ReleaseSegment:
    first_number: int
    numbers: tuple[int, ...]
    offset: int
    size: int
    source: str
    body_id: str
    sha256: str
    body_offset: int = 0


@dataclass(frozen=True)
class Release:
    id: int
    filename: str
    manifest: list[dict]
    segments: list[ReleaseSegment]
    total_size: int
    new_write_bytes: int
    product_sha256: str
    hot_checks: list[dict]
    created_at: str
    retired_at: str | None
    pinned_readers: int

    @property
    def is_active(self) -> bool:
        return self.retired_at is None


@dataclass(frozen=True)
class PublishResult:
    release: Release
    plan: Plan
    expected_sha256: str
    actual_sha256: str
    built_bodies: tuple[str, ...]
    old_release_id: int | None


@dataclass(frozen=True)
class GcResult:
    deleted_releases: tuple[int, ...]
    deleted_bodies: tuple[str, ...]
    deleted_paths: tuple[str, ...] = ()


@dataclass(frozen=True)
class RecoveryItem:
    """One shard that a recovery request would pull out of a product."""

    number: int
    product_offset: int
    size: int
    sha256: str
    body_id: str
    body_offset: int
    body_size: int
    mode: str  # "reuse" | "copy"


@dataclass(frozen=True)
class RecoveryResult:
    release_id: int
    filename: str
    items: tuple[RecoveryItem, ...]
    # Bytes actually written to new bodies (sum of the "copy" shards).
    new_write_bytes: int
    # Sum, counting each shared source body once, of the full file sizes the
    # recovered shards now depend on.
    dependency_bytes: int
    reused_bodies: tuple[str, ...]
    copied_bodies: tuple[str, ...]


def _parse_manifest_text(text: str) -> list[tuple[int, str]]:
    rows: list[tuple[int, str]] = []
    for lineno, raw in enumerate((text or "").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        fields = re.split(r"[\s,]+", line)
        if len(fields) != 2:
            raise ValidationError(
                f"清单第 {lineno} 行应为“分片号 SHA-256”，实际有 {len(fields)} 个字段"
            )
        try:
            number = int(fields[0], 10)
        except ValueError:
            raise ValidationError(f"清单第 {lineno} 行：分片号不是整数")
        rows.append((number, fields[1]))
    try:
        return parse_manifest_entries(rows)
    except PlanError as exc:
        raise ValidationError(str(exc)) from exc


def _parse_hot_text(text: str, total_size: int) -> list[HotRange]:
    raw_rows: list[tuple[int, int, int]] = []
    for lineno, raw in enumerate((text or "").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        fields = re.split(r"[\s,]+", line)
        if len(fields) != 3:
            raise ValidationError(
                f"常用区间第 {lineno} 行应为“起始偏移 字节长度 最多打开文件数”"
            )
        try:
            start, length, max_open = (int(x, 10) for x in fields)
        except ValueError:
            raise ValidationError(f"常用区间第 {lineno} 行：三个字段都必须是整数")
        raw_rows.append((start, length, max_open))
    try:
        return parse_hot_ranges(raw_rows, total_size)
    except PlanError as exc:
        raise ValidationError(str(exc)) from exc


class Repo:
    def __init__(self, data_dir: str, *, chunk_size: int = CHUNK):
        self.data_dir = os.path.abspath(data_dir)
        self.body_dir = os.path.join(self.data_dir, "bodies")
        self.tmp_dir = os.path.join(self.data_dir, "tmp")
        self.db_path = os.path.join(self.data_dir, "repo.db")
        self.chunk_size = chunk_size
        self._lock = threading.RLock()
        self._publish_lock = threading.Lock()
        os.makedirs(self.body_dir, exist_ok=True)
        os.makedirs(self.tmp_dir, exist_ok=True)
        self._init_db()

    # ------------------------------------------------------------------ DB
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        return conn

    @contextmanager
    def _tx(self, on_commit=None):
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
            if on_commit is not None:
                on_commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("PRAGMA journal_mode = WAL")
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS meta (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS bodies (
                        id TEXT PRIMARY KEY,
                        rel_path TEXT NOT NULL,
                        size INTEGER NOT NULL CHECK (size >= 0),
                        sha256 TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS shards (
                        number INTEGER PRIMARY KEY CHECK (number BETWEEN 1 AND 12),
                        body_id TEXT NOT NULL REFERENCES bodies(id),
                        sha256 TEXT NOT NULL,
                        size INTEGER NOT NULL CHECK (size >= 0),
                        updated_at TEXT NOT NULL,
                        body_offset INTEGER NOT NULL DEFAULT 0 CHECK (body_offset >= 0),
                        origin TEXT NOT NULL DEFAULT 'upload'
                    );
                    CREATE TABLE IF NOT EXISTS releases (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        filename TEXT NOT NULL,
                        manifest_json TEXT NOT NULL,
                        segments_json TEXT NOT NULL,
                        total_size INTEGER NOT NULL,
                        new_write_bytes INTEGER NOT NULL,
                        product_sha256 TEXT NOT NULL,
                        hot_checks_json TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        retired_at TEXT,
                        pinned_readers INTEGER NOT NULL DEFAULT 0
                    );
                    CREATE TABLE IF NOT EXISTS release_bodies (
                        release_id INTEGER NOT NULL REFERENCES releases(id) ON DELETE CASCADE,
                        body_id TEXT NOT NULL REFERENCES bodies(id),
                        PRIMARY KEY (release_id, body_id)
                    );
                    CREATE INDEX IF NOT EXISTS idx_release_bodies_body
                        ON release_bodies(body_id);
                    CREATE TABLE IF NOT EXISTS attempts (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        created_at TEXT NOT NULL,
                        ok INTEGER NOT NULL,
                        message TEXT NOT NULL,
                        detail_json TEXT NOT NULL
                    );
                    """
                )
                conn.execute(
                    "INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version', ?)",
                    (SCHEMA_VERSION,),
                )
                # Lightweight in-place migration for repositories created by
                # schema v1: whole-body shards gain an offset/origin column.
                shard_cols = {
                    row["name"]
                    for row in conn.execute("PRAGMA table_info(shards)")
                }
                if "body_offset" not in shard_cols:
                    conn.execute(
                        "ALTER TABLE shards ADD COLUMN body_offset INTEGER "
                        "NOT NULL DEFAULT 0 CHECK (body_offset >= 0)"
                    )
                if "origin" not in shard_cols:
                    conn.execute(
                        "ALTER TABLE shards ADD COLUMN origin TEXT "
                        "NOT NULL DEFAULT 'upload'"
                    )
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES('schema_version', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (SCHEMA_VERSION,),
                )
                conn.commit()
            finally:
                conn.close()

        self._clean_tmp()
        with self._publish_lock, self._lock, self._tx() as conn:
            # Pins belong to in-process readers; none survive a restart, so a
            # crash while a download was streaming must not leak old bodies.
            conn.execute("UPDATE releases SET pinned_readers=0")
            # Drop a dangling active pointer (e.g. after an interrupted manual
            # database edit) rather than serving a release that no longer exists.
            row = conn.execute(
                "SELECT value FROM meta WHERE key='active_release_id'"
            ).fetchone()
            if row is not None:
                rel = conn.execute(
                    "SELECT id, retired_at FROM releases WHERE id=?", (int(row[0]),)
                ).fetchone()
                if rel is None or rel["retired_at"] is not None:
                    conn.execute("DELETE FROM meta WHERE key='active_release_id'")
        self.gc()
        self._sweep_orphan_files()

    def _clean_tmp(self) -> None:
        for name in os.listdir(self.tmp_dir):
            path = os.path.join(self.tmp_dir, name)
            try:
                os.unlink(path)
            except OSError:
                log.warning("could not remove leftover temporary file %s", path)

    def _sweep_orphan_files(self) -> None:
        with self._lock:
            conn = self._connect()
            try:
                known = {
                    row["id"] for row in conn.execute("SELECT id FROM bodies")
                }
            finally:
                conn.close()
        removed = 0
        for name in os.listdir(self.body_dir):
            if not name.endswith(".bin"):
                continue
            body_id = name[:-4]
            if body_id not in known:
                try:
                    os.unlink(os.path.join(self.body_dir, name))
                    removed += 1
                except OSError:
                    log.warning("could not remove orphan body %s", name)
        if removed:
            log.info("startup removed %d orphan body file(s)", removed)

    # --------------------------------------------------------------- bodies
    def _body_path(self, rel_path: str) -> str:
        # rel_path is always generated by this module ("bodies/<uuid>.bin"),
        # never supplied by a web client.
        return os.path.join(self.data_dir, rel_path)

    def _append_bytes(
        self,
        fh: BinaryIO,
        data: bytes,
        hasher: "hashlib._Hash",
    ) -> int:
        fh.write(data)
        hasher.update(data)
        return len(data)

    def _write_upload_body(self, fileobj: BinaryIO) -> Body:
        body_id = uuid.uuid4().hex
        rel_path = f"bodies/{body_id}.bin"
        final_path = self._body_path(rel_path)
        tmp_path = os.path.join(self.tmp_dir, f"upload-{body_id}.part")
        hasher = hashlib.sha256()
        size = 0
        try:
            with open(tmp_path, "wb") as out:
                while True:
                    chunk = fileobj.read(self.chunk_size)
                    if not chunk:
                        break
                    size += self._append_bytes(out, chunk, hasher)
                out.flush()
                os.fsync(out.fileno())
            os.replace(tmp_path, final_path)
            self._fsync_dir(self.body_dir)
        except BaseException:
            for path in (tmp_path, final_path):
                try:
                    os.unlink(path)
                except FileNotFoundError:
                    pass
            raise
        return Body(body_id, rel_path, size, hasher.hexdigest())

    @staticmethod
    def _fsync_dir(path: str) -> None:
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    # --------------------------------------------------------------- shards
    def store_shard(self, number: int, fileobj: BinaryIO) -> StagedShard:
        if not isinstance(number, int) or not (1 <= number <= MAX_SHARDS):
            raise ValidationError(f"分片号必须在 1..{MAX_SHARDS}")
        # The body bytes are written outside the publication lock: bodies are
        # immutable and unpublished, so a concurrent publish neither sees nor
        # needs them.  Only the staging-row swap is serialised with publish.
        try:
            body = self._write_upload_body(fileobj)
        except OSError as exc:
            raise WriteFailure(f"保存分片 {number} 失败：{exc}") from exc
        try:
            with self._publish_lock:
                now = utcnow()
                with self._lock, self._tx() as conn:
                    conn.execute(
                        "INSERT INTO bodies(id, rel_path, size, sha256, created_at) "
                        "VALUES(?,?,?,?,?)",
                        (body.id, body.rel_path, body.size, body.sha256, now),
                    )
                    conn.execute(
                        "INSERT INTO shards(number, body_id, sha256, size, updated_at, "
                        "body_offset, origin) VALUES(?,?,?,?,?,0,'upload') "
                        "ON CONFLICT(number) DO UPDATE SET "
                        "body_id=excluded.body_id, sha256=excluded.sha256, "
                        "size=excluded.size, updated_at=excluded.updated_at, "
                        "body_offset=0, origin='upload'",
                        (number, body.id, body.sha256, body.size, now),
                    )
        except BaseException:
            try:
                os.unlink(self._body_path(body.rel_path))
            except FileNotFoundError:
                pass
            raise
        with self._publish_lock:
            self.gc()
        return StagedShard(
            number,
            body.id,
            body.rel_path,
            body.size,
            body.sha256,
            now,
            body_offset=0,
            origin="upload",
        )

    def staged_shards(self) -> list[StagedShard]:
        with self._lock:
            conn = self._connect()
            try:
                rows = conn.execute(
                    "SELECT s.number, s.body_id, b.rel_path, s.size, s.sha256, "
                    "s.updated_at, s.body_offset, s.origin "
                    "FROM shards s JOIN bodies b ON b.id=s.body_id ORDER BY s.number"
                ).fetchall()
            finally:
                conn.close()
        return [
            StagedShard(
                r["number"], r["body_id"], r["rel_path"], r["size"], r["sha256"],
                r["updated_at"], int(r["body_offset"]), str(r["origin"]),
            )
            for r in rows
        ]

    def delete_staged_shard(self, number: int) -> None:
        # Serialise with publication: a shard may not vanish between the
        # publish snapshot and the body verification read.
        with self._publish_lock:
            with self._lock, self._tx() as conn:
                conn.execute("DELETE FROM shards WHERE number=?", (number,))
            self.gc()

    def manifest_prefill(self) -> str:
        return "\n".join(f"{s.number} {s.sha256}" for s in self.staged_shards())

    # ------------------------------------------------------------ recovery
    def _release_shard_lookup(
        self, release: Release
    ) -> dict[int, tuple[int, int, str, int, int, int]]:
        """Map shard number -> (product_offset, size, sha, body_id,
        body_offset, body_size) for every shard contained in a release.

        A merged segment spans several manifest rows inside one body; each
        row's bytes start at ``body_offset + (row_product_offset -
        segment.offset)``.
        """
        body_sizes = self._body_sizes_for(
            {seg.body_id for seg in release.segments}
        )
        out: dict[int, tuple[int, int, str, int, int, int]] = {}
        product_cursor = 0
        for entry in release.manifest:
            number = int(entry["number"])
            size = int(entry["size"])
            sha = str(entry["sha256"])
            segment = next(
                seg
                for seg in release.segments
                if number in seg.numbers
            )
            inner = product_cursor - segment.offset
            body_offset = segment.body_offset + inner
            out[number] = (
                product_cursor,
                size,
                sha,
                segment.body_id,
                body_offset,
                body_sizes[segment.body_id],
            )
            product_cursor += size
        return out

    def _body_sizes_for(self, body_ids: set[str]) -> dict[str, int]:
        if not body_ids:
            return {}
        with self._lock:
            conn = self._connect()
            try:
                placeholders = ",".join("?" for _ in body_ids)
                rows = conn.execute(
                    f"SELECT id, size FROM bodies WHERE id IN ({placeholders})",
                    tuple(body_ids),
                ).fetchall()
            finally:
                conn.close()
        sizes = {row["id"]: int(row["size"]) for row in rows}
        missing = body_ids - sizes.keys()
        if missing:
            raise WriteFailure(f"成品正文记录缺失：{sorted(missing)}")
        return sizes

    def recovery_candidates(self, release_id: int | None = None) -> list[RecoveryItem]:
        """Describe every shard that can currently be pulled from the active
        product, without writing anything.  The home page renders this so the
        operator can see source offsets and pre-compute the two budgets."""
        release = (
            self.get_release(release_id)
            if release_id is not None
            else self.get_active_release()
        )
        if release is None:
            raise NoRelease("尚未发布任何成品，无分片可找回")
        lookup = self._release_shard_lookup(release)
        return [
            RecoveryItem(
                number=number,
                product_offset=info[0],
                size=info[1],
                sha256=info[2],
                body_id=info[3],
                body_offset=info[4],
                body_size=info[5],
                mode="reuse",
            )
            for number, info in sorted(lookup.items())
        ]

    @staticmethod
    def _parse_recovery_selection(
        selected: Iterable[int], modes: dict[int, str]
    ) -> tuple[list[int], dict[int, str]]:
        chosen: list[int] = []
        seen: set[int] = set()
        for raw in selected:
            try:
                number = int(raw)
            except (TypeError, ValueError):
                raise ValidationError("找回分片号必须是 1..12 的整数")
            if not (1 <= number <= MAX_SHARDS):
                raise ValidationError(f"分片号 {number} 超出 1..{MAX_SHARDS}")
            if number in seen:
                continue
            seen.add(number)
            mode = (modes.get(number) or "reuse").strip().lower()
            if mode not in ("reuse", "copy"):
                raise ValidationError(
                    f"分片 {number} 的方式只能是 reuse（复用正文）或 copy（另存）"
                )
            chosen.append(number)
        if not chosen:
            raise ValidationError("请至少勾选一个要找回的分片")
        chosen.sort()
        return chosen, {n: (modes.get(n) or "reuse").strip().lower() for n in chosen}

    def _write_range_body(
        self, source_path: str, offset: int, length: int
    ) -> Body:
        """Materialise one shard's sub-range as its own immutable body."""
        body_id = uuid.uuid4().hex
        rel_path = f"bodies/{body_id}.bin"
        final_path = self._body_path(rel_path)
        tmp_path = os.path.join(self.tmp_dir, f"recover-{body_id}.part")
        hasher = hashlib.sha256()
        written = 0
        try:
            with open(source_path, "rb") as src, open(tmp_path, "wb") as out:
                src.seek(offset)
                left = length
                while left > 0:
                    chunk = src.read(min(self.chunk_size, left))
                    if not chunk:
                        raise OSError(
                            f"源正文 {source_path} 在子区间 {offset}+{length} 内提前结束"
                        )
                    out.write(chunk)
                    hasher.update(chunk)
                    written += len(chunk)
                    left -= len(chunk)
                out.flush()
                os.fsync(out.fileno())
            if written != length:
                raise OSError(f"另存正文长度 {written} 与分片长度 {length} 不一致")
            os.replace(tmp_path, final_path)
            self._fsync_dir(self.body_dir)
        except BaseException:
            for path in (tmp_path, final_path):
                try:
                    os.unlink(path)
                except FileNotFoundError:
                    pass
            raise
        return Body(body_id, rel_path, written, hasher.hexdigest())

    def recover_shards(
        self,
        *,
        numbers: Iterable[int],
        modes: dict[int, str] | None = None,
        max_new_bytes: int,
        max_dependency_bytes: int,
        expected_release_id: int | None = None,
    ) -> RecoveryResult:
        """Recover selected shards out of the current published product.

        The whole batch is all-or-nothing.  Two budgets are checked before any
        byte is written:

        * ``max_new_bytes`` bounds bytes newly written to standalone bodies
          (shards recovered with mode ``copy``);
        * ``max_dependency_bytes`` bounds the total size of the files the
          recovered shards depend on, counting every shared source body once
          at its full file size (mode ``reuse`` keeps a pointer into it).

        Recovery is serialised with uploads and publications through
        ``_publish_lock``; the release is re-read inside the lock so a product
        swap between page render and confirmation aborts the batch.
        """
        modes = modes or {}
        chosen, mode_by_number = self._parse_recovery_selection(numbers, modes)
        if not isinstance(max_new_bytes, int) or max_new_bytes < 0:
            raise ValidationError("本次新写字节上限必须是非负整数")
        if (
            not isinstance(max_dependency_bytes, int)
            or max_dependency_bytes < 0
        ):
            raise ValidationError("依赖文件总大小上限必须是非负整数")

        committed = False
        with self._publish_lock:
            release = self.get_active_release()
            if release is None:
                raise NoRelease("尚未发布任何成品，无分片可找回")
            if (
                expected_release_id is not None
                and int(expected_release_id) != release.id
            ):
                raise RecoveryError(
                    f"确认时成品已换版：页面基于发布 #{expected_release_id}，"
                    f"当前生效发布为 #{release.id}，本次找回整批未补"
                )

            lookup = self._release_shard_lookup(release)
            missing = [n for n in chosen if n not in lookup]
            if missing:
                raise RecoveryError(
                    f"分片 {missing} 不在当前成品的发布清单中，无法找回"
                )

            occupied = {s.number for s in self.staged_shards()}
            clash = [n for n in chosen if n in occupied]
            if clash:
                raise RecoveryError(
                    f"所选分片号 {clash} 已被已收分片占用，整批未补；"
                    "请先删除或覆盖对应分片后再找回"
                )

            # Budget pre-computation: no disk writes happen until both caps
            # are satisfiable.  Shared source bodies are counted once.
            new_write_bytes = 0
            dependency_total = 0
            reused_bodies: set[str] = set()
            plan_items: list[RecoveryItem] = []
            for number in chosen:
                (
                    product_offset,
                    size,
                    sha,
                    body_id,
                    body_offset,
                    body_size,
                ) = lookup[number]
                mode = mode_by_number[number]
                item = RecoveryItem(
                    number=number,
                    product_offset=product_offset,
                    size=size,
                    sha256=sha,
                    body_id=body_id,
                    body_offset=body_offset,
                    body_size=body_size,
                    mode=mode,
                )
                plan_items.append(item)
                if mode == "copy":
                    new_write_bytes += size
                else:
                    if body_id not in reused_bodies:
                        reused_bodies.add(body_id)
                        dependency_total += body_size
            if new_write_bytes > max_new_bytes:
                raise RecoveryError(
                    f"额度不足：本次需新写 {new_write_bytes} 字节（另存分片），"
                    f"超过新写上限 {max_new_bytes} 字节，整批未补"
                )
            if dependency_total > max_dependency_bytes:
                raise RecoveryError(
                    f"额度不足：找回分片依赖的正文文件共 {dependency_total} 字节"
                    f"（共用正文按整份大小只算一次），超过依赖上限 "
                    f"{max_dependency_bytes} 字节，整批未补"
                )

            built: list[tuple[Body, int]] = []  # (new body, shard number)
            built_paths: list[str] = []
            try:
                # Copy mode first, verifying each extracted sub-range against
                # the manifest digest before any row is inserted.
                copy_by_number: dict[int, Body] = {}
                for item in plan_items:
                    if item.mode != "copy":
                        continue
                    source_path = self.body_path_for(item.body_id)
                    try:
                        body = self._write_range_body(
                            source_path, item.body_offset, item.size
                        )
                    except OSError as exc:
                        raise WriteFailure(
                            f"另存分片 {item.number} 失败：{exc}"
                        ) from exc
                    if body.sha256 != item.sha256:
                        raise DigestMismatch(
                            f"分片 {item.number} 另存后 SHA-256 为 {body.sha256}，"
                            f"发布清单要求 {item.sha256}，整批未补"
                        )
                    copy_by_number[item.number] = body
                    built.append((body, item.number))
                    built_paths.append(self._body_path(body.rel_path))

                # Reuse mode: verify the sub-range of the shared body now, so
                # a corrupt/truncated body fails the batch before commit.
                for item in plan_items:
                    if item.mode != "reuse":
                        continue
                    path = self.body_path_for(item.body_id)
                    verifier = hashlib.sha256()
                    try:
                        _size, digest = self._read_file_digest_with_sink(
                            path,
                            verifier,
                            offset=item.body_offset,
                            length=item.size,
                        )
                    except OSError as exc:
                        raise WriteFailure(
                            f"校验分片 {item.number} 所复用的正文失败：{exc}"
                        ) from exc
                    if digest != item.sha256:
                        raise DigestMismatch(
                            f"分片 {item.number} 复用正文子区间 SHA-256 为 {digest}，"
                            f"发布清单要求 {item.sha256}，整批未补"
                        )

                now = utcnow()

                def mark_committed():
                    nonlocal committed
                    committed = True

                with self._lock, self._tx(on_commit=mark_committed) as conn:
                    # Re-check occupancy and the active release inside the
                    # write transaction: another request cannot have staged a
                    # row concurrently (same process, _publish_lock), but this
                    # keeps the invariant explicit and safe.
                    active_row = conn.execute(
                        "SELECT value FROM meta WHERE key='active_release_id'"
                    ).fetchone()
                    if active_row is None or int(active_row[0]) != release.id:
                        raise RecoveryError(
                            "确认时成品已换版，本次找回整批未补"
                        )
                    for item in plan_items:
                        row = conn.execute(
                            "SELECT 1 FROM shards WHERE number=?", (item.number,)
                        ).fetchone()
                        if row is not None:
                            raise RecoveryError(
                                f"分片号 {item.number} 已被占用，整批未补"
                            )
                    for body, number in built:
                        conn.execute(
                            "INSERT INTO bodies(id, rel_path, size, sha256, created_at) "
                            "VALUES(?,?,?,?,?)",
                            (body.id, body.rel_path, body.size, body.sha256, now),
                        )
                    for item in plan_items:
                        if item.mode == "copy":
                            body = copy_by_number[item.number]
                            body_id, body_offset, origin = (
                                body.id,
                                0,
                                "recovered-copy",
                            )
                        else:
                            body_id, body_offset, origin = (
                                item.body_id,
                                item.body_offset,
                                "recovered-reuse",
                            )
                        conn.execute(
                            "INSERT INTO shards(number, body_id, sha256, size, "
                            "updated_at, body_offset, origin) "
                            "VALUES(?,?,?,?,?,?,?)",
                            (
                                item.number,
                                body_id,
                                item.sha256,
                                item.size,
                                now,
                                body_offset,
                                origin,
                            ),
                        )
            except BaseException:
                if not committed:
                    for path in built_paths:
                        try:
                            os.unlink(path)
                        except FileNotFoundError:
                            pass
                raise

            result = RecoveryResult(
                release_id=release.id,
                filename=release.filename,
                items=tuple(plan_items),
                new_write_bytes=new_write_bytes,
                dependency_bytes=dependency_total,
                reused_bodies=tuple(sorted(reused_bodies)),
                copied_bodies=tuple(
                    body.id for body, _n in built
                ),
            )
        self.gc()
        return result
    # --------------------------------------------------------------- publish
    def publish(
        self,
        *,
        manifest_text: str,
        hot_ranges_text: str,
        max_write: int,
        filename: str = "install.img",
        simulate_write_failure: bool = False,
    ) -> PublishResult:
        manifest = _parse_manifest_text(manifest_text)
        if not FILENAME_RE.fullmatch(filename or ""):
            raise ValidationError(
                "成品文件名只能包含字母、数字、点、下划线和连字符，且以字母或数字开头"
            )
        if not isinstance(max_write, int) or max_write < 0:
            raise ValidationError("新写正文字节上限必须是非负整数")

        committed = False
        with self._publish_lock:
            # Snapshot staging.  Staging-row swaps and publications share
            # _publish_lock, so this snapshot cannot change while we build.
            shards = {s.number: s for s in self.staged_shards()}
            if not shards:
                raise ValidationError("没有已收分片，无法发布")
            if len(manifest) > MAX_SHARDS:
                raise ValidationError(f"一次最多发布 {MAX_SHARDS} 个分片")

            entries: list[ManifestEntry] = []
            for number, sha in manifest:
                shard = shards.get(number)
                if shard is None:
                    raise ValidationError(f"分片 {number} 尚未上传")
                if shard.sha256 != sha:
                    raise DigestMismatch(
                        f"分片 {number} 的 SHA-256 与已收文件不符："
                        f"提交 {sha}，已收 {shard.sha256}"
                    )
                entries.append(
                    ManifestEntry(
                        number,
                        sha,
                        shard.size,
                        body_id=shard.body_id,
                        body_offset=shard.body_offset,
                    )
                )

            total_size = sum(e.size for e in entries)
            hot_ranges = _parse_hot_text(hot_ranges_text, total_size)
            try:
                plan = make_plan(entries, hot_ranges, max_write)
            except PlanError as exc:
                raise ValidationError(str(exc)) from exc

            old_release_id = self._active_release_id()
            built_paths: list[str] = []
            built_specs: list[tuple[str, str, int, str, int]] = []
            #                  body_id, rel_path, size, digest, segment_index

            try:
                # 1. Verify every manifest shard byte-for-byte and build the
                #    expected product digest from the manifest order.  A shard
                #    may occupy only a sub-range of its body (when recovered
                #    while sharing a published body), so only that sub-range
                #    is read and hashed.
                expected_hasher = hashlib.sha256()
                for entry in entries:
                    shard = shards[entry.number]
                    path = self._body_path(shard.rel_path)
                    try:
                        size, digest = self._read_file_digest_with_sink(
                            path,
                            expected_hasher,
                            offset=shard.body_offset,
                            length=shard.size,
                        )
                    except OSError as exc:
                        raise WriteFailure(
                            f"读取分片 {entry.number} 正文失败：{exc}"
                        ) from exc
                    if size != entry.size:
                        raise DigestMismatch(
                            f"分片 {entry.number} 已收正文长度为 {size}，"
                            f"清单预期 {entry.size}"
                        )
                    if digest != entry.sha256:
                        raise DigestMismatch(
                            f"分片 {entry.number} 正文 SHA-256 为 {digest}，"
                            f"清单要求 {entry.sha256}"
                        )
                expected_sha = expected_hasher.hexdigest()

                # 2. Materialise merged segments.  Single-shard segments reuse
                #    the uploaded body as-is (zero new bytes).
                build_by_first: dict[int, tuple[str, str, int, str]] = {}
                for seg in plan.build_segments:
                    body_id = uuid.uuid4().hex
                    rel_path = f"bodies/{body_id}.bin"
                    final_path = self._body_path(rel_path)
                    built_paths.append(final_path)
                    try:
                        size, digest = self._write_merged_body(
                            seg.numbers,
                            shards,
                            final_path,
                            simulate_failure=simulate_write_failure,
                        )
                    except OSError as exc:
                        raise WriteFailure(
                            f"写合并正文（分片 {seg.numbers[0]}..{seg.numbers[-1]}）"
                            f"失败：{exc}"
                        ) from exc
                    build_by_first[seg.first_number] = (
                        body_id, rel_path, size, digest
                    )
                    built_specs.append(
                        (body_id, rel_path, size, digest, seg.first_number)
                    )
                if simulate_write_failure and not plan.build_segments:
                    # The drill must not silently succeed on a pure-reuse
                    # layout that never touches the disk.
                    raise WriteFailure(
                        "模拟写盘失败：本次布局没有需要新写的正文，未触发写盘"
                    )

                segments: list[ReleaseSegment] = []
                release_body_ids: set[str] = set()
                for seg in plan.segments:
                    if seg.source == "reuse":
                        shard = shards[seg.numbers[0]]
                        body_id = shard.body_id
                        digest = shard.sha256
                        body_offset = shard.body_offset
                    else:
                        body_id, _rel, _size, digest = build_by_first[seg.first_number]
                        body_offset = 0
                    segments.append(
                        ReleaseSegment(
                            first_number=seg.first_number,
                            numbers=seg.numbers,
                            offset=seg.offset,
                            size=seg.size,
                            source=seg.source,
                            body_id=body_id,
                            sha256=digest,
                            body_offset=body_offset,
                        )
                    )
                    release_body_ids.add(body_id)

                # 3. Re-read the assembled product and compare against the
                #    manifest concatenation digest.  This is the explicit
                #    "product == shards in manifest order" invariant.  Only
                #    each segment's own sub-range of its body file is read,
                #    so neighbouring shards sharing a body are never merged
                #    into this segment's bytes.
                actual_hasher = hashlib.sha256()
                built_by_id = {b[0]: b[1] for b in built_specs}
                for seg in segments:
                    rel = self._body_rel_for_id(seg.body_id, shards, built_by_id)
                    path = self._body_path(rel)
                    try:
                        self._feed_file(
                            path,
                            actual_hasher,
                            offset=seg.body_offset,
                            length=seg.size,
                        )
                    except OSError as exc:
                        raise WriteFailure(f"复核成品正文失败：{exc}") from exc
                actual_sha = actual_hasher.hexdigest()
                if actual_sha != expected_sha:
                    raise DigestMismatch(
                        "成品字节与按清单拼接分片的结果不一致："
                        f"成品 {actual_sha}，清单拼接 {expected_sha}"
                    )
                if sum(s.size for s in segments) != total_size:
                    raise DigestMismatch("成品总长度与清单分片长度之和不一致")

                # 4. One short commit: release rows, body references, active
                #    pointer and retirement of the previous release.
                now = utcnow()

                def mark_committed():
                    nonlocal committed
                    committed = True

                with self._lock, self._tx(on_commit=mark_committed) as conn:
                    for body_id, rel_path, size, digest, _first in built_specs:
                        conn.execute(
                            "INSERT INTO bodies(id, rel_path, size, sha256, created_at) "
                            "VALUES(?,?,?,?,?)",
                            (body_id, rel_path, size, digest, now),
                        )
                    manifest_json = json.dumps(
                        [
                            {"number": e.number, "sha256": e.sha256, "size": e.size}
                            for e in entries
                        ],
                        separators=(",", ":"),
                    )
                    segments_json = json.dumps(
                        [
                            {
                                "first_number": s.first_number,
                                "numbers": list(s.numbers),
                                "offset": s.offset,
                                "size": s.size,
                                "source": s.source,
                                "body_id": s.body_id,
                                "sha256": s.sha256,
                                "body_offset": s.body_offset,
                            }
                            for s in segments
                        ],
                        separators=(",", ":"),
                    )
                    checks_json = json.dumps(
                        [
                            {
                                "start": c.start,
                                "length": c.length,
                                "max_open": c.max_open,
                                "opens": c.opens,
                            }
                            for c in plan.checks
                        ],
                        separators=(",", ":"),
                    )
                    cur = conn.execute(
                        "INSERT INTO releases(filename, manifest_json, segments_json, "
                        "total_size, new_write_bytes, product_sha256, hot_checks_json, "
                        "created_at) VALUES(?,?,?,?,?,?,?,?)",
                        (
                            filename,
                            manifest_json,
                            segments_json,
                            total_size,
                            sum(s.size for s in plan.build_segments),
                            actual_sha,
                            checks_json,
                            now,
                        ),
                    )
                    release_id = int(cur.lastrowid)
                    for body_id in sorted(release_body_ids):
                        conn.execute(
                            "INSERT INTO release_bodies(release_id, body_id) VALUES(?,?)",
                            (release_id, body_id),
                        )
                    if old_release_id is not None:
                        conn.execute(
                            "UPDATE releases SET retired_at=? WHERE id=? "
                            "AND retired_at IS NULL",
                            (now, old_release_id),
                        )
                    conn.execute(
                        "INSERT INTO meta(key, value) VALUES('active_release_id', ?) "
                        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                        (str(release_id),),
                    )
                    release_row = conn.execute(
                        "SELECT * FROM releases WHERE id=?", (release_id,)
                    ).fetchone()
            except BaseException:
                if not committed:
                    for path in built_paths:
                        try:
                            os.unlink(path)
                        except FileNotFoundError:
                            pass
                raise

            release = self._row_to_release(release_row)
            result = PublishResult(
                release=release,
                plan=plan,
                expected_sha256=expected_sha,
                actual_sha256=actual_sha,
                built_bodies=tuple(b[0] for b in built_specs),
                old_release_id=old_release_id,
            )
        self.gc()
        return result

    def _read_file_digest_with_sink(
        self,
        path: str,
        sink: "hashlib._Hash",
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> tuple[int, str]:
        """Read ``length`` bytes of ``path`` starting at ``offset``.

        Only the requested sub-range is hashed and forwarded to ``sink``:
        shards recovered while sharing a published body must not pull the
        neighbouring shards sharing that same file into their digest.
        """
        hasher = hashlib.sha256()
        size = 0
        remaining = length
        with open(path, "rb") as fh:
            if offset:
                fh.seek(offset)
            while remaining is None or remaining > 0:
                buf = self.chunk_size if remaining is None else min(self.chunk_size, remaining)
                chunk = fh.read(buf)
                if not chunk:
                    break
                hasher.update(chunk)
                sink.update(chunk)
                size += len(chunk)
                if remaining is not None:
                    remaining -= len(chunk)
        if length is not None and size != length:
            raise OSError(
                f"正文 {path} 子区间 {offset}+{length} 只读到 {size} 字节"
            )
        return size, hasher.hexdigest()

    def _feed_file(
        self,
        path: str,
        hasher: "hashlib._Hash",
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> int:
        size = 0
        remaining = length
        with open(path, "rb") as fh:
            if offset:
                fh.seek(offset)
            while remaining is None or remaining > 0:
                buf = self.chunk_size if remaining is None else min(self.chunk_size, remaining)
                chunk = fh.read(buf)
                if not chunk:
                    break
                hasher.update(chunk)
                size += len(chunk)
                if remaining is not None:
                    remaining -= len(chunk)
        if length is not None and size != length:
            raise OSError(
                f"正文 {path} 子区间 {offset}+{length} 只读到 {size} 字节"
            )
        return size

    def _write_merged_body(
        self,
        numbers: Sequence[int],
        shards: dict[int, StagedShard],
        final_path: str,
        simulate_failure: bool = False,
    ) -> tuple[int, str]:
        body_id = os.path.basename(final_path)[:-4]
        tmp_path = os.path.join(self.tmp_dir, f"merge-{body_id}.part")
        hasher = hashlib.sha256()
        written = 0
        expected = sum(shards[n].size for n in numbers)
        if simulate_failure and expected == 0:
            raise OSError(
                "simulated disk write failure (merged body would be empty)"
            )
        failure_after = max(1, expected // 2)
        try:
            with open(tmp_path, "wb") as out:
                for number in numbers:
                    shard = shards[number]
                    left = shard.size
                    with open(self._body_path(shard.rel_path), "rb") as src:
                        if shard.body_offset:
                            src.seek(shard.body_offset)
                        while left > 0:
                            chunk = src.read(min(self.chunk_size, left))
                            if not chunk:
                                raise OSError(
                                    f"分片 {number} 正文在子区间内提前结束"
                                )
                            out.write(chunk)
                            hasher.update(chunk)
                            written += len(chunk)
                            left -= len(chunk)
                            if simulate_failure and written >= failure_after:
                                raise OSError(
                                    "simulated disk write failure while merging "
                                    f"shards {numbers[0]}..{numbers[-1]} "
                                    f"(after {written} of {expected} bytes)"
                                )
                out.flush()
                os.fsync(out.fileno())
            os.replace(tmp_path, final_path)
            self._fsync_dir(self.body_dir)
        except BaseException:
            for path in (tmp_path, final_path):
                try:
                    os.unlink(path)
                except FileNotFoundError:
                    pass
            raise
        if written != expected:
            raise WriteFailure(
                f"合并正文长度 {written} 与分片长度之和 {expected} 不一致"
            )
        return written, hasher.hexdigest()

    def _body_rel_for_id(
        self,
        body_id: str,
        shards: dict[int, StagedShard],
        built_by_id: dict[str, str],
    ) -> str:
        if body_id in built_by_id:
            return built_by_id[body_id]
        for shard in shards.values():
            if shard.body_id == body_id:
                return shard.rel_path
        raise WriteFailure(f"找不到正文 {body_id} 的存储路径")

    # -------------------------------------------------------------- releases
    def _active_release_id(self) -> int | None:
        with self._lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT value FROM meta WHERE key='active_release_id'"
                ).fetchone()
            finally:
                conn.close()
        return int(row[0]) if row is not None else None

    @staticmethod
    def _segment_from_json(raw: dict) -> ReleaseSegment:
        return ReleaseSegment(
            first_number=int(raw["first_number"]),
            numbers=tuple(int(n) for n in raw["numbers"]),
            offset=int(raw["offset"]),
            size=int(raw["size"]),
            source=str(raw["source"]),
            body_id=str(raw["body_id"]),
            sha256=str(raw["sha256"]),
            body_offset=int(raw.get("body_offset", 0)),
        )

    def _row_to_release(self, row: sqlite3.Row) -> Release:
        return Release(
            id=int(row["id"]),
            filename=row["filename"],
            manifest=json.loads(row["manifest_json"]),
            segments=[
                self._segment_from_json(s)
                for s in json.loads(row["segments_json"])
            ],
            total_size=int(row["total_size"]),
            new_write_bytes=int(row["new_write_bytes"]),
            product_sha256=row["product_sha256"],
            hot_checks=json.loads(row["hot_checks_json"]),
            created_at=row["created_at"],
            retired_at=row["retired_at"],
            pinned_readers=int(row["pinned_readers"]),
        )

    def get_active_release(self) -> Release | None:
        with self._lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT r.* FROM releases r JOIN meta m "
                    "ON m.key='active_release_id' AND CAST(m.value AS INTEGER)=r.id"
                ).fetchone()
            finally:
                conn.close()
        return self._row_to_release(row) if row is not None else None

    def pin_active_release(self) -> Release:
        """Atomically choose and pin the active release for a streaming read."""
        with self._lock, self._tx() as conn:
            row = conn.execute(
                "SELECT r.* FROM releases r JOIN meta m "
                "ON m.key='active_release_id' AND CAST(m.value AS INTEGER)=r.id"
            ).fetchone()
            if row is None:
                raise NoRelease("尚未发布任何成品")
            conn.execute(
                "UPDATE releases SET pinned_readers=pinned_readers+1 WHERE id=?",
                (row["id"],),
            )
            row = conn.execute(
                "SELECT * FROM releases WHERE id=?", (row["id"],)
            ).fetchone()
        return self._row_to_release(row)

    def pin_release(self, release_id: int) -> Release:
        """Pin a specific release; used when a response names its release."""
        with self._lock, self._tx() as conn:
            row = conn.execute(
                "SELECT * FROM releases WHERE id=?", (release_id,)
            ).fetchone()
            if row is None:
                raise NoRelease(f"发布 #{release_id} 不存在")
            conn.execute(
                "UPDATE releases SET pinned_readers=pinned_readers+1 WHERE id=?",
                (release_id,),
            )
            row = conn.execute(
                "SELECT * FROM releases WHERE id=?", (release_id,)
            ).fetchone()
        return self._row_to_release(row)

    def release_pin(self, release_id: int) -> GcResult:
        with self._lock, self._tx() as conn:
            conn.execute(
                "UPDATE releases SET pinned_readers=MAX(pinned_readers-1, 0) "
                "WHERE id=?",
                (release_id,),
            )
            result = self._gc_locked(conn)
        self._unlink_paths(result.deleted_paths)
        return result

    def get_release(self, release_id: int) -> Release | None:
        with self._lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT * FROM releases WHERE id=?", (release_id,)
                ).fetchone()
            finally:
                conn.close()
        return self._row_to_release(row) if row is not None else None

    def release_list(self, limit: int = 10) -> list[Release]:
        with self._lock:
            conn = self._connect()
            try:
                rows = conn.execute(
                    "SELECT * FROM releases ORDER BY id DESC LIMIT ?", (limit,)
                ).fetchall()
            finally:
                conn.close()
        return [self._row_to_release(r) for r in rows]

    def active_release_id(self) -> int | None:
        return self._active_release_id()

    # ------------------------------------------------------------------- GC
    def _gc_locked(self, conn: sqlite3.Connection) -> GcResult:
        """Free bodies that no live release, staged shard or reader needs.

        Retired release rows are kept as an audit trail.  Once no reader is
        pinning a retired release, its ``release_bodies`` references are
        dropped; any body that then has neither a staged shard nor any other
        release reference is unlinked.  A pinned release keeps its references,
        which is what allows an in-flight download to finish reading the old
        product after a newer release has taken over the stable URL.
        """
        retired = [
            int(r["id"])
            for r in conn.execute(
                "SELECT r.id FROM releases r WHERE r.retired_at IS NOT NULL "
                "AND r.pinned_readers=0 AND EXISTS ("
                "  SELECT 1 FROM release_bodies rb WHERE rb.release_id=r.id)"
            )
        ]
        for release_id in retired:
            conn.execute(
                "DELETE FROM release_bodies WHERE release_id=?", (release_id,)
            )
        unreferenced = conn.execute(
            "SELECT id, rel_path FROM bodies b "
            "WHERE NOT EXISTS (SELECT 1 FROM shards s WHERE s.body_id=b.id) "
            "AND NOT EXISTS (SELECT 1 FROM release_bodies rb WHERE rb.body_id=b.id)"
        ).fetchall()
        paths: list[tuple[str, str]] = []
        for row in unreferenced:
            conn.execute("DELETE FROM bodies WHERE id=?", (row["id"],))
            paths.append((row["id"], row["rel_path"]))
        return GcResult(
            tuple(retired),
            tuple(pid for pid, _ in paths),
            tuple(rel for _, rel in paths),
        )

    def gc(self) -> GcResult:
        with self._lock, self._tx() as conn:
            result = self._gc_locked(conn)
        # Unlink only after the reference-removing transaction committed.
        self._unlink_paths(result.deleted_paths)
        return result

    def _unlink_paths(self, paths: Iterable[str]) -> None:
        for rel_path in paths:
            try:
                os.unlink(self._body_path(rel_path))
            except FileNotFoundError:
                pass
            except OSError:
                log.warning("could not unlink body %s", rel_path)

    def has_unpinned_retired(self) -> bool:
        with self._lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT COUNT(*) FROM releases r WHERE r.retired_at IS NOT NULL "
                    "AND r.pinned_readers=0 AND EXISTS ("
                    "  SELECT 1 FROM release_bodies rb WHERE rb.release_id=r.id)"
                ).fetchone()
            finally:
                conn.close()
        return bool(row[0])

    # ------------------------------------------------------------- attempts
    def record_attempt(self, ok: bool, message: str, detail: dict | None = None) -> None:
        with self._lock, self._tx() as conn:
            conn.execute(
                "INSERT INTO attempts(created_at, ok, message, detail_json) "
                "VALUES(?,?,?,?)",
                (utcnow(), 1 if ok else 0, message, json.dumps(detail or {})),
            )
            # Keep the audit trail bounded.
            conn.execute(
                "DELETE FROM attempts WHERE id NOT IN "
                "(SELECT id FROM attempts ORDER BY id DESC LIMIT 200)"
            )

    def recent_attempts(self, limit: int = 5) -> list[dict]:
        with self._lock:
            conn = self._connect()
            try:
                rows = conn.execute(
                    "SELECT * FROM attempts ORDER BY id DESC LIMIT ?", (limit,)
                ).fetchall()
            finally:
                conn.close()
        out = []
        for row in rows:
            try:
                detail = json.loads(row["detail_json"])
            except (TypeError, ValueError):
                detail = {}
            out.append(
                {
                    "id": row["id"],
                    "created_at": row["created_at"],
                    "ok": bool(row["ok"]),
                    "message": row["message"],
                    "detail": detail,
                }
            )
        return out

    # ------------------------------------------------------------------ info
    def stats(self) -> dict:
        with self._lock:
            conn = self._connect()
            try:
                bodies = conn.execute(
                    "SELECT COUNT(*) AS n, COALESCE(SUM(size),0) AS s FROM bodies"
                ).fetchone()
                shards = conn.execute("SELECT COUNT(*) AS n FROM shards").fetchone()
                releases = conn.execute(
                    "SELECT COUNT(*) AS n FROM releases"
                ).fetchone()
                retired = conn.execute(
                    "SELECT COUNT(*) AS n FROM releases WHERE retired_at IS NOT NULL"
                ).fetchone()
                pending_gc = conn.execute(
                    "SELECT COUNT(*) AS n FROM releases r WHERE r.retired_at "
                    "IS NOT NULL AND r.pinned_readers=0 AND EXISTS ("
                    "  SELECT 1 FROM release_bodies rb WHERE rb.release_id=r.id)"
                ).fetchone()
            finally:
                conn.close()
        return {
            "bodies": int(bodies["n"]),
            "body_bytes": int(bodies["s"]),
            "staged_shards": int(shards["n"]),
            "releases": int(releases["n"]),
            "retired_releases": int(retired["n"]),
            "releases_pending_gc": int(pending_gc["n"]),
            "data_dir": self.data_dir,
        }

    def body_id_for_path(self, path: str) -> str:
        name = os.path.basename(path)
        return name[:-4] if name.endswith(".bin") else name

    def body_path_for(self, body_id: str) -> str:
        with self._lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT rel_path FROM bodies WHERE id=?", (body_id,)
                ).fetchone()
            finally:
                conn.close()
        if row is None:
            raise FileNotFoundError(f"正文记录不存在：{body_id}")
        return self._body_path(row["rel_path"])

    def body_present(self, body_id: str, rel_path: str) -> bool:
        try:
            return os.path.isfile(self._body_path(rel_path))
        except OSError:
            return False
