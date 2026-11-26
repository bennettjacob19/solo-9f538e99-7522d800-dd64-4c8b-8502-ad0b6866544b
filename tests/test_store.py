import hashlib
import io
import os
import tempfile
import unittest

from artifact_repo.store import (
    DigestMismatch,
    Repo,
    ValidationError,
    WriteFailure,
)


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class RepoTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Repo(self.tmp.name)
        self.parts = [b"a" * 1000, b"b" * 2000, b"c" * 3000, b"d" * 4000]
        for n, p in enumerate(self.parts, 1):
            self.repo.store_shard(n, io.BytesIO(p))
        self.manifest = "\n".join(
            f"{n} {sha(p)}" for n, p in enumerate(self.parts, 1)
        )

    def tearDown(self):
        self.tmp.cleanup()

    def test_publish_all_reuse(self):
        result = self.repo.publish(manifest_text=self.manifest, hot_ranges_text="", max_write=0)
        self.assertEqual(result.release.total_size, 10000)
        self.assertEqual(result.release.new_write_bytes, 0)
        self.assertEqual(result.actual_sha256, sha(b"".join(self.parts)))
        self.assertEqual(result.expected_sha256, result.actual_sha256)
        # Staging survives so the next round can republish unchanged shards.
        self.assertEqual(len(self.repo.staged_shards()), 4)

    def test_merge_creates_new_body_and_counts_writes(self):
        result = self.repo.publish(
            manifest_text=self.manifest,
            hot_ranges_text="0 10000 2",
            max_write=10000,
        )
        self.assertGreater(result.release.new_write_bytes, 0)
        self.assertEqual(result.release.new_write_bytes, sum(
            s.size for s in result.release.segments if s.source == "build"
        ))
        self.assertLessEqual(
            max(c["opens"] for c in result.release.hot_checks), 2
        )

    def test_digest_mismatch_keeps_old_release_and_shards(self):
        first = self.repo.publish(manifest_text=self.manifest, hot_ranges_text="", max_write=0)
        for n, p in enumerate(self.parts, 1):
            self.repo.store_shard(n, io.BytesIO(p))
        bad = self.manifest.replace(sha(self.parts[1]), sha(b"wrong"))
        with self.assertRaises((DigestMismatch, ValidationError)):
            self.repo.publish(manifest_text=bad, hot_ranges_text="", max_write=0)
        self.assertEqual(self.repo.get_active_release().id, first.release.id)
        self.assertEqual(len(self.repo.staged_shards()), 4)

    def test_infeasible_budgets_keep_old_artifacts(self):
        first = self.repo.publish(manifest_text=self.manifest, hot_ranges_text="", max_write=0)
        for n, p in enumerate(self.parts, 1):
            self.repo.store_shard(n, io.BytesIO(p))
        with self.assertRaises(ValidationError):
            self.repo.publish(
                manifest_text=self.manifest,
                hot_ranges_text="0 10000 1",
                max_write=9999,
            )
        self.assertEqual(self.repo.get_active_release().id, first.release.id)
        self.assertEqual(len(self.repo.staged_shards()), 4)

    def test_simulated_write_failure_rolls_back(self):
        first = self.repo.publish(manifest_text=self.manifest, hot_ranges_text="", max_write=0)
        for n, p in enumerate(self.parts, 1):
            self.repo.store_shard(n, io.BytesIO(p))
        before = set(os.listdir(os.path.join(self.tmp.name, "bodies")))
        with self.assertRaises(WriteFailure):
            self.repo.publish(
                manifest_text=self.manifest,
                hot_ranges_text="0 10000 2",
                max_write=10000,
                simulate_write_failure=True,
            )
        self.assertEqual(self.repo.get_active_release().id, first.release.id)
        self.assertEqual(len(self.repo.staged_shards()), 4)
        after = set(os.listdir(os.path.join(self.tmp.name, "bodies")))
        self.assertEqual(before, after, "partial merged body must be removed")

    def test_shard_numbers_reusable_after_publish(self):
        self.repo.publish(manifest_text=self.manifest, hot_ranges_text="", max_write=0)
        self.repo.store_shard(1, io.BytesIO(b"x" * 500))
        staged = {s.number: s for s in self.repo.staged_shards()}
        self.assertEqual(sorted(staged), [1, 2, 3, 4])
        self.assertEqual(staged[1].size, 500)
        # The old release still serves the original shard 1 body.
        old = self.repo.get_active_release()
        self.assertEqual(old.segments[0].size, 1000)

    def test_restart_restores_last_release(self):
        result = self.repo.publish(manifest_text=self.manifest, hot_ranges_text="", max_write=0)
        reopened = Repo(self.tmp.name)
        active = reopened.get_active_release()
        self.assertIsNotNone(active)
        self.assertEqual(active.id, result.release.id)
        self.assertEqual(active.product_sha256, result.actual_sha256)

    def test_republish_releases_unreferenced_bodies(self):
        self.repo.publish(manifest_text=self.manifest, hot_ranges_text="", max_write=0)
        new_parts = [b"z" * 1000] + self.parts[1:]
        for n, p in enumerate(new_parts, 1):
            self.repo.store_shard(n, io.BytesIO(p))
        manifest2 = "\n".join(f"{n} {sha(p)}" for n, p in enumerate(new_parts, 1))
        self.repo.publish(manifest_text=manifest2, hot_ranges_text="", max_write=0)
        # The retired release is unpinned, so its old shard-1 body is dropped;
        # only the four bodies of the new release remain.
        self.assertEqual(self.repo.stats()["bodies"], 4)
        self.assertEqual(len(self.repo.staged_shards()), 4)


if __name__ == "__main__":
    unittest.main()
