import hashlib
import io
import os
import random
import tempfile
import unittest

from artifact_repo.geometry import (
    HotRange,
    ManifestEntry,
    PlanError,
    make_plan,
)
from artifact_repo.serving import ServeStats, iter_product_bytes, plan_open_count
from artifact_repo.store import Repo


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class PlanInvariants(unittest.TestCase):
    def test_random_layouts_respect_budgets(self):
        rng = random.Random(7)
        for _ in range(200):
            n = rng.randint(1, 7)
            sizes = [rng.randint(0, 500) for _ in range(n)]
            entries = [ManifestEntry(i + 1, "0" * 64, s) for i, s in enumerate(sizes)]
            total = sum(sizes)
            hot = []
            for _ in range(rng.randint(0, 3)):
                if total == 0:
                    break
                start = rng.randint(0, total - 1)
                length = rng.randint(1, total - start)
                limit = rng.randint(1, n)
                hot.append(HotRange(start, length, limit))
            budget = rng.randint(0, total)
            try:
                plan = make_plan(entries, hot, budget)
            except PlanError:
                continue
            self.assertLessEqual(plan.new_write_bytes, budget)
            for check in plan.checks:
                self.assertLessEqual(check.opens, check.max_open)
            self.assertEqual(plan.total_size, total)
            # Offsets are contiguous and cover the whole product.
            pos = 0
            for seg in plan.segments:
                self.assertEqual(seg.offset, pos)
                pos += seg.size
            self.assertEqual(pos, total)
            # A segment marked "reuse" is exactly one shard.
            for seg in plan.segments:
                if seg.source == "reuse":
                    self.assertEqual(len(seg.numbers), 1)
                else:
                    self.assertGreaterEqual(len(seg.numbers), 2)
            # New-write bytes equal the sum of built segment sizes.
            self.assertEqual(
                plan.new_write_bytes,
                sum(s.size for s in plan.segments if s.source == "build"),
            )

    def test_segments_follow_manifest_order_not_number_order(self):
        entries = [
            ManifestEntry(5, "0" * 64, 10),
            ManifestEntry(2, "0" * 64, 20),
            ManifestEntry(9, "0" * 64, 30),
        ]
        plan = make_plan(entries, [HotRange(0, 60, 1)], max_write=60)
        self.assertEqual([s.numbers for s in plan.segments], [(5, 2, 9)])
        self.assertEqual([s.first_number for s in plan.segments], [5])


class ServingOpenCount(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Repo(self.tmp.name)
        self.parts = [os.urandom(3000 + 500 * i) for i in range(5)]
        for n, p in enumerate(self.parts, 1):
            self.repo.store_shard(n, io.BytesIO(p))
        manifest = "\n".join(
            f"{n} {sha(p)}" for n, p in enumerate(self.parts, 1)
        )
        self.product = b"".join(self.parts)
        # Force two merges: whole-product with limit 3 => at most 3 bodies.
        self.release = self.repo.publish(
            manifest_text=manifest,
            hot_ranges_text=f"0 {len(self.product)} 3",
            max_write=len(self.product),
        ).release

    def tearDown(self):
        self.tmp.cleanup()

    def test_actual_open_count_matches_plan(self):
        total = len(self.product)
        samples = [(0, total), (0, 1), (total - 1, 1), (1234, 5000)]
        for seg in self.release.segments[:-1]:
            boundary = seg.offset + seg.size
            samples.append((boundary - 1, 2))
            samples.append((max(0, boundary - 777), 1554))
        for start, length in samples:
            length = min(length, total - start)
            self.assertGreater(length, 0)
            expected = plan_open_count(self.release, start, length)
            stats = ServeStats()
            data = b"".join(
                iter_product_bytes(self.repo, self.release, start, length, stats)
            )
            self.assertEqual(data, self.product[start : start + length])
            self.assertEqual(stats.body_files_opened, expected)
            self.assertEqual(len(stats.opened_body_ids), expected)
            self.assertLessEqual(expected, 3)

    def test_zero_length_hot_range_is_rejected(self):
        from artifact_repo.geometry import parse_hot_ranges

        with self.assertRaises(PlanError):
            parse_hot_ranges([(0, 0, 1)], 10)


if __name__ == "__main__":
    unittest.main()
