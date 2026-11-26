"""Shard recovery from a published product.

Covers the two recovery budgets (new bytes / dependency file size with shared
bodies counted once), SHA-256 verification against the release manifest,
all-or-nothing failure modes, durability across restart and GC, re-publishing
recovered shards in a different order mixed with fresh uploads, and the
"one shared body opened once" Range property.
"""

import hashlib
import io
import os
import sqlite3
import tempfile
import unittest

from artifact_repo.serving import ServeStats, iter_product_bytes, plan_open_count
from artifact_repo.store import (
    DigestMismatch,
    NoRelease,
    RecoveryError,
    Repo,
    ValidationError,
    WriteFailure,
)
from artifact_repo.web import create_app


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class RecoveryTestBase(unittest.TestCase):
    #: Three shards.  The first release merges 1+2 into one shared body while
    #: shard 3 reuses its own body, so recovering shard 1 or 2 extracts a
    #: sub-range from inside a merged body.  A 1-open hot range straddling the
    #: 1/2 boundary forces exactly that merge; the 6000-byte write cap rules
    #: out a full three-shard merge (9000 bytes).
    parts = [b"a" * 2000, b"b" * 2000, b"c" * 5000]
    product_size = 9000

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Repo(self.tmp.name)
        for n, p in enumerate(self.parts, 1):
            self.repo.store_shard(n, io.BytesIO(p))
        self.manifest = "\n".join(
            f"{n} {sha(p)}" for n, p in enumerate(self.parts, 1)
        )
        hot = "1900 200 1"  # crosses only the 1/2 boundary at offset 2000
        self.result = self.repo.publish(
            manifest_text=self.manifest,
            hot_ranges_text=hot,
            max_write=6000,
        )
        self.release = self.result.release
        self.assertEqual(
            [tuple(s.numbers) for s in self.release.segments],
            [(1, 2), (3,)],
        )
        # Simulate the build machine losing every original shard file.
        for n in range(1, 4):
            self.repo.delete_staged_shard(n)
        self.assertEqual(self.repo.staged_shards(), [])

    def tearDown(self):
        self.tmp.cleanup()

    def body_file_count(self) -> int:
        return len(
            [
                n
                for n in os.listdir(os.path.join(self.tmp.name, "bodies"))
                if n.endswith(".bin")
            ]
        )


class RecoveryStoreTests(RecoveryTestBase):
    def test_candidates_describe_subranges_of_merged_body(self):
        candidates = {c.number: c for c in self.repo.recovery_candidates()}
        self.assertEqual(set(candidates), {1, 2, 3})
        # Shards 1 and 2 share the merged body; shard 3 owns its whole body.
        self.assertEqual(candidates[1].body_id, candidates[2].body_id)
        self.assertNotEqual(candidates[1].body_id, candidates[3].body_id)
        self.assertEqual(candidates[1].body_offset, 0)
        self.assertEqual(candidates[1].size, 2000)
        self.assertEqual(candidates[2].body_offset, 2000)
        self.assertEqual(candidates[2].size, 2000)
        self.assertEqual(candidates[3].body_offset, 0)
        self.assertEqual(candidates[3].body_size, 5000)
        for n, part in enumerate(self.parts, 1):
            self.assertEqual(candidates[n].sha256, sha(part))
            self.assertEqual(
                candidates[n].product_offset,
                sum(len(p) for p in self.parts[: n - 1]),
            )

    def test_reuse_recovery_writes_no_new_bytes(self):
        before = self.body_file_count()
        result = self.repo.recover_shards(
            numbers=[1, 2, 3],
            modes={},
            max_new_bytes=0,
            # Merged body full size 4000 + shard-3 body full size 5000; each
            # shared body is counted once.
            max_dependency_bytes=self.product_size,
        )
        self.assertEqual(result.new_write_bytes, 0)
        self.assertEqual(result.dependency_bytes, self.product_size)
        self.assertEqual(len(result.reused_bodies), 2)
        self.assertEqual(result.copied_bodies, ())
        self.assertEqual(self.body_file_count(), before)
        staged = {s.number: s for s in self.repo.staged_shards()}
        self.assertEqual(set(staged), {1, 2, 3})
        self.assertTrue(
            all(s.origin == "recovered-reuse" for s in staged.values())
        )
        # Shards 1 and 2 point into the same body file at different offsets.
        self.assertEqual(staged[1].body_id, staged[2].body_id)
        self.assertEqual(staged[1].body_offset, 0)
        self.assertEqual(staged[2].body_offset, 2000)
        for n, part in enumerate(self.parts, 1):
            self.assertEqual(staged[n].sha256, sha(part))
            self.assertEqual(staged[n].size, len(part))

    def test_shared_body_counted_once_for_subset(self):
        # Recovering only shard 2 still depends on the full 4000-byte body.
        result = self.repo.recover_shards(
            numbers=[2],
            modes={},
            max_new_bytes=0,
            max_dependency_bytes=4000,
        )
        self.assertEqual(result.dependency_bytes, 4000)
        # One byte less of budget must fail.
        self.repo.delete_staged_shard(2)
        with self.assertRaises(RecoveryError):
            self.repo.recover_shards(
                numbers=[2],
                modes={},
                max_new_bytes=0,
                max_dependency_bytes=3999,
            )

    def test_copy_recovery_writes_independent_bodies(self):
        before = self.body_file_count()
        result = self.repo.recover_shards(
            numbers=[1, 2],
            modes={1: "copy", 2: "copy"},
            max_new_bytes=4000,
            max_dependency_bytes=0,
        )
        self.assertEqual(result.new_write_bytes, 4000)
        self.assertEqual(result.dependency_bytes, 0)
        self.assertEqual(len(result.copied_bodies), 2)
        self.assertEqual(self.body_file_count(), before + 2)
        staged = {s.number: s for s in self.repo.staged_shards()}
        self.assertTrue(
            all(s.origin == "recovered-copy" for s in staged.values())
        )
        self.assertNotEqual(staged[1].body_id, staged[2].body_id)
        self.assertTrue(all(s.body_offset == 0 for s in staged.values()))
        for n in (1, 2):
            with open(self.repo.body_path_for(staged[n].body_id), "rb") as fh:
                self.assertEqual(fh.read(), self.parts[n - 1])

    def test_mixed_modes_count_shared_body_once(self):
        result = self.repo.recover_shards(
            numbers=[1, 2, 3],
            modes={1: "copy", 2: "reuse", 3: "reuse"},
            max_new_bytes=2000,
            # Only reuse-mode source bodies count: merged body (4000) once,
            # shard-3 body (5000) -> 9000 despite the merged body being shared.
            max_dependency_bytes=self.product_size,
        )
        self.assertEqual(result.new_write_bytes, 2000)
        self.assertEqual(result.dependency_bytes, self.product_size)

    def test_new_write_budget_exceeded_aborts_whole_batch(self):
        with self.assertRaises(RecoveryError) as ctx:
            self.repo.recover_shards(
                numbers=[1, 2],
                modes={1: "copy", 2: "copy"},
                max_new_bytes=3999,
                max_dependency_bytes=0,
            )
        self.assertIn("新写", str(ctx.exception))
        self.assertEqual(self.repo.staged_shards(), [])
        self.assertEqual(self.body_file_count(), 2)

    def test_dependency_budget_exceeded_aborts_whole_batch(self):
        with self.assertRaises(RecoveryError) as ctx:
            self.repo.recover_shards(
                numbers=[1, 2],
                modes={},
                max_new_bytes=0,
                max_dependency_bytes=3999,
            )
        self.assertIn("依赖", str(ctx.exception))
        self.assertEqual(self.repo.staged_shards(), [])

    def test_occupied_shard_number_aborts_batch(self):
        self.repo.store_shard(2, io.BytesIO(b"occupied"))
        with self.assertRaises(RecoveryError) as ctx:
            self.repo.recover_shards(
                numbers=[1, 2],
                modes={},
                max_new_bytes=0,
                max_dependency_bytes=self.product_size,
            )
        self.assertIn("占用", str(ctx.exception))
        staged = {s.number for s in self.repo.staged_shards()}
        self.assertEqual(staged, {2})

    def test_invalid_inputs(self):
        with self.assertRaises(ValidationError):
            self.repo.recover_shards(
                numbers=[], modes={}, max_new_bytes=0, max_dependency_bytes=0
            )
        with self.assertRaises(ValidationError):
            self.repo.recover_shards(
                numbers=[1], modes={1: "weird"},
                max_new_bytes=0, max_dependency_bytes=0,
            )
        with self.assertRaises(ValidationError):
            self.repo.recover_shards(
                numbers=[1], modes={}, max_new_bytes=-1, max_dependency_bytes=0
            )
        with self.assertRaises(ValidationError):
            self.repo.recover_shards(
                numbers=[1], modes={}, max_new_bytes=0, max_dependency_bytes=-1
            )
        with self.assertRaises(ValidationError):
            self.repo.recover_shards(
                numbers=[13], modes={}, max_new_bytes=0, max_dependency_bytes=0
            )

    def test_version_switch_at_confirmation_aborts_batch(self):
        # Publish a second release so the id handed to the page is stale.
        new_parts = [b"z" * 2000] + self.parts[1:]
        for n, p in enumerate(new_parts, 1):
            self.repo.store_shard(n, io.BytesIO(p))
        self.repo.publish(
            manifest_text="\n".join(
                f"{n} {sha(p)}" for n, p in enumerate(new_parts, 1)
            ),
            hot_ranges_text="",
            max_write=0,
        )
        for n in range(1, 4):
            self.repo.delete_staged_shard(n)
        with self.assertRaises(RecoveryError) as ctx:
            self.repo.recover_shards(
                numbers=[1, 2],
                modes={},
                max_new_bytes=0,
                max_dependency_bytes=self.product_size,
                expected_release_id=self.release.id,  # stale
            )
        self.assertIn("换版", str(ctx.exception))
        self.assertEqual(self.repo.staged_shards(), [])

    def test_digest_mismatch_detection(self):
        candidates = {c.number: c for c in self.repo.recovery_candidates()}
        path = self.repo.body_path_for(candidates[1].body_id)
        with open(path, "r+b") as fh:
            fh.seek(0)
            fh.write(b"Z" * 10)
        with self.assertRaises((DigestMismatch, WriteFailure)):
            self.repo.recover_shards(
                numbers=[1],
                modes={},
                max_new_bytes=0,
                max_dependency_bytes=self.product_size,
            )
        self.assertEqual(self.repo.staged_shards(), [])

    def test_recovered_shards_survive_gc_and_restart(self):
        self.repo.recover_shards(
            numbers=[1, 2, 3],
            modes={},
            max_new_bytes=0,
            max_dependency_bytes=self.product_size,
        )
        # The original release retires and gets reclaimed; recovered shards
        # now own the only references to those two body files.
        self.repo.gc()
        self.assertEqual(self.body_file_count(), 2)
        reopened = Repo(self.tmp.name)
        staged = {s.number: s for s in reopened.staged_shards()}
        self.assertEqual(set(staged), {1, 2, 3})
        for n, part in enumerate(self.parts, 1):
            self.assertEqual(staged[n].sha256, sha(part))
            path = reopened.body_path_for(staged[n].body_id)
            with open(path, "rb") as fh:
                fh.seek(staged[n].body_offset)
                self.assertEqual(fh.read(staged[n].size), part)

    def test_republish_recovered_shards_in_new_order_mixed_with_new_shard(self):
        # Recover 1..3 out of the shared/owned bodies, upload a fresh shard 5,
        # then republish in a different order (3, 1, 5, 2) with zero new write.
        self.repo.recover_shards(
            numbers=[1, 2, 3],
            modes={},
            max_new_bytes=0,
            max_dependency_bytes=self.product_size,
        )
        fresh = b"fresh!" * 500  # 3000 bytes
        self.repo.store_shard(5, io.BytesIO(fresh))
        order = [3, 1, 5, 2]
        payload_by_number = {
            1: self.parts[0], 2: self.parts[1], 3: self.parts[2], 5: fresh,
        }
        manifest = "\n".join(
            f"{n} {sha(payload_by_number[n])}" for n in order
        )
        new_product = b"".join(payload_by_number[n] for n in order)
        # Publishing still enforces both publish budgets: max_write 0 and a
        # whole-product open limit of the reused bodies (merged + shard 3 +
        # the fresh upload = 3 distinct files).
        result = self.repo.publish(
            manifest_text=manifest,
            hot_ranges_text=f"0 {len(new_product)} 3",
            max_write=0,
        )
        self.assertEqual(result.release.new_write_bytes, 0)
        self.assertEqual(result.actual_sha256, sha(new_product))
        stats = ServeStats()
        served = b"".join(
            iter_product_bytes(
                self.repo, result.release, 0, len(new_product), stats
            )
        )
        self.assertEqual(served, new_product)

    def test_range_on_recovered_reuse_opens_shared_body_once(self):
        self.repo.recover_shards(
            numbers=[1, 2, 3],
            modes={},
            max_new_bytes=0,
            max_dependency_bytes=self.product_size,
        )
        result = self.repo.publish(
            manifest_text=self.manifest,
            hot_ranges_text="",
            max_write=0,
        )
        release = result.release
        # Two physical bodies back three segments (shards 1+2 share one).
        self.assertEqual(
            len({seg.body_id for seg in release.segments}), 2
        )
        # A range crossing the shard-1/shard-2 boundary touches two segments
        # but must open just one body file.
        start, length = 1990, 20
        self.assertEqual(plan_open_count(release, start, length), 1)
        stats = ServeStats()
        data = b"".join(
            iter_product_bytes(self.repo, release, start, length, stats)
        )
        product = b"".join(self.parts)
        self.assertEqual(data, product[start : start + length])
        self.assertEqual(stats.body_files_opened, 1)
        # Whole product touches both shared bodies.
        self.assertEqual(
            plan_open_count(release, 0, release.total_size), 2
        )

    def test_schema_v1_database_migrates(self):
        self.repo.recover_shards(
            numbers=[1],
            modes={},
            max_new_bytes=0,
            max_dependency_bytes=self.product_size,
        )
        conn = sqlite3.connect(self.repo.db_path)
        try:
            conn.execute(
                "CREATE TABLE shards_v1 AS SELECT number, body_id, sha256, "
                "size, updated_at FROM shards"
            )
            conn.execute("DROP TABLE shards")
            conn.execute(
                "CREATE TABLE shards (number INTEGER PRIMARY KEY, "
                "body_id TEXT NOT NULL, sha256 TEXT NOT NULL, "
                "size INTEGER NOT NULL, updated_at TEXT NOT NULL)"
            )
            conn.execute("INSERT INTO shards SELECT * FROM shards_v1")
            conn.execute("DROP TABLE shards_v1")
            conn.commit()
        finally:
            conn.close()
        reopened = Repo(self.tmp.name)
        staged = {s.number: s for s in reopened.staged_shards()}
        self.assertIn(1, staged)
        self.assertEqual(staged[1].body_offset, 0)
        self.assertEqual(staged[1].origin, "upload")  # migrated default


class RecoveryWebTests(RecoveryTestBase):
    def setUp(self):
        super().setUp()
        self.app = create_app(self.repo)
        self.client = self.app.test_client()

    def test_index_renders_recovery_panel(self):
        resp = self.client.get("/")
        self.assertEqual(resp.status_code, 200)
        body = resp.get_data(as_text=True)
        self.assertIn("从当前成品找回分片", body)
        self.assertIn("name=\"max_new_bytes\"", body)
        self.assertIn("name=\"max_dependency_bytes\"", body)
        for n in range(1, 4):
            self.assertIn(f'value="{n}"', body)

    def test_recovery_form_success_and_usage_flash(self):
        resp = self.client.post(
            "/recovery",
            data={
                "release_id": str(self.release.id),
                "recover": ["1", "2"],
                "mode_1": "reuse",
                "mode_2": "reuse",
                "max_new_bytes": "0",
                "max_dependency_bytes": str(self.product_size),
            },
            follow_redirects=True,
        )
        self.assertEqual(resp.status_code, 200)
        page = resp.get_data(as_text=True)
        self.assertIn("补入 2 个分片", page)
        self.assertIn("实际新写 0 字节", page)
        # Both shards share the 4000-byte merged body, counted once.
        self.assertIn("依赖正文合计 4000 字节", page)
        staged = {s.number for s in self.repo.staged_shards()}
        self.assertEqual(staged, {1, 2})

    def test_recovery_form_budget_failure_keeps_page_and_shards(self):
        resp = self.client.post(
            "/recovery",
            data={
                "release_id": str(self.release.id),
                "recover": ["1", "2"],
                "mode_1": "copy",
                "mode_2": "copy",
                "max_new_bytes": "10",
                "max_dependency_bytes": "0",
            },
            follow_redirects=True,
        )
        page = resp.get_data(as_text=True)
        self.assertIn("整批未补", page)
        self.assertEqual(self.repo.staged_shards(), [])

    def test_occupied_shard_shown_disabled(self):
        self.repo.store_shard(2, io.BytesIO(b"x"))
        page = self.client.get("/").get_data(as_text=True)
        self.assertIn("号已占用", page)

    def test_shard_download_streams_only_subrange(self):
        self.repo.recover_shards(
            numbers=[1, 2],
            modes={},
            max_new_bytes=0,
            max_dependency_bytes=self.product_size,
        )
        for n in (1, 2):
            resp = self.client.get(f"/shards/{n}")
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.data, self.parts[n - 1])
            self.assertEqual(
                resp.headers["X-Shard-Sha256"], sha(self.parts[n - 1])
            )
            self.assertEqual(
                int(resp.headers["Content-Length"]), len(self.parts[n - 1])
            )

    def test_api_state_exposes_recovery_plan(self):
        data = self.client.get("/api/state").get_json()
        self.assertIsNotNone(data["recovery"])
        self.assertEqual(
            sorted(c["number"] for c in data["recovery"]["candidates"]),
            [1, 2, 3],
        )
        self.client.post(
            "/recovery",
            data={
                "release_id": str(self.release.id),
                "recover": ["2"],
                "mode_2": "reuse",
                "max_new_bytes": "0",
                "max_dependency_bytes": str(self.product_size),
            },
        )
        data = self.client.get("/api/state").get_json()
        cand2 = next(
            c for c in data["recovery"]["candidates"] if c["number"] == 2
        )
        self.assertTrue(cand2["occupied"])
        staged2 = next(s for s in data["staged"] if s["number"] == 2)
        self.assertEqual(staged2["origin"], "recovered-reuse")
        self.assertEqual(staged2["body_offset"], cand2["body_offset"])

    def test_recovery_attempt_recorded(self):
        self.client.post(
            "/recovery",
            data={
                "release_id": str(self.release.id),
                "recover": ["1"],
                "mode_1": "reuse",
                "max_new_bytes": "0",
                "max_dependency_bytes": "0",  # too small
            },
        )
        attempts = self.repo.recent_attempts(3)
        self.assertFalse(attempts[0]["ok"])
        self.assertIn("找回失败", attempts[0]["message"])

    def test_stale_release_id_in_form_aborts_recovery(self):
        # A second release takes over; the form still carries the old id.
        new_parts = [b"z" * 2000] + self.parts[1:]
        for n, p in enumerate(new_parts, 1):
            self.repo.store_shard(n, io.BytesIO(p))
        self.repo.publish(
            manifest_text="\n".join(
                f"{n} {sha(p)}" for n, p in enumerate(new_parts, 1)
            ),
            hot_ranges_text="",
            max_write=0,
        )
        for n in range(1, 4):
            self.repo.delete_staged_shard(n)
        resp = self.client.post(
            "/recovery",
            data={
                "release_id": str(self.release.id),  # stale
                "recover": ["1"],
                "mode_1": "reuse",
                "max_new_bytes": "0",
                "max_dependency_bytes": str(self.product_size),
            },
            follow_redirects=True,
        )
        page = resp.get_data(as_text=True)
        self.assertIn("换版", page)
        self.assertIn("整批未补", page)
        self.assertEqual(self.repo.staged_shards(), [])

    def test_verify_loopback_opens_shared_body_once(self):
        # All-reuse re-publish after recovery: shards 1+2 still share one body.
        self.repo.recover_shards(
            numbers=[1, 2, 3],
            modes={},
            max_new_bytes=0,
            max_dependency_bytes=self.product_size,
        )
        self.repo.publish(
            manifest_text=self.manifest,
            hot_ranges_text="",
            max_write=0,
        )
        # A real loopback HTTP Range straddling the shard-1/2 boundary
        # (product offset 2000) must open exactly one body file.
        resp = self.client.get("/verify?start=1990&length=20")
        self.assertEqual(resp.status_code, 200)
        page = resp.get_data(as_text=True)
        self.assertIn('id="opened">1<', page)
        self.assertIn("收到的字节与本地成品快照一致", page)


if __name__ == "__main__":
    unittest.main()
