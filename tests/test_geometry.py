import unittest

from artifact_repo.geometry import (
    HotRange,
    ManifestEntry,
    PlanError,
    make_plan,
    _compositions,
)


def entries(*sizes):
    return [ManifestEntry(i + 1, "0" * 64, s) for i, s in enumerate(sizes)]


class CompositionTests(unittest.TestCase):
    def test_composition_count(self):
        self.assertEqual(len(_compositions(4)), 8)

    def test_no_hot_ranges_prefers_pure_reuse(self):
        plan = make_plan(entries(10, 20, 30), [], max_write=0)
        self.assertEqual(plan.new_write_bytes, 0)
        self.assertEqual([s.source for s in plan.segments], ["reuse"] * 3)

    def test_merging_needed_to_meet_open_limit(self):
        # Whole product as one hot range with a 1-open limit forces one body.
        plan = make_plan(
            entries(10, 20, 30), [HotRange(0, 60, 1)], max_write=60
        )
        self.assertEqual(len(plan.segments), 1)
        self.assertEqual(plan.new_write_bytes, 60)

    def test_exactly_enough_write_budget(self):
        plan = make_plan(entries(10, 20), [HotRange(0, 30, 1)], max_write=30)
        self.assertEqual(plan.new_write_bytes, 30)

    def test_write_budget_one_byte_short_is_infeasible(self):
        with self.assertRaises(PlanError):
            make_plan(entries(10, 20), [HotRange(0, 30, 1)], max_write=29)

    def test_open_limit_infeasible_without_enough_shards(self):
        # 3 separate shards, whole-product range, limit 2 -> max 2 opens if we
        # merge two of them, so limit 1 is feasible; use a limit of 0 instead.
        with self.assertRaises(PlanError):
            make_plan(entries(10, 20, 30), [HotRange(0, 60, 0)], max_write=60)

    def test_hot_range_across_boundary(self):
        plan = make_plan(entries(10, 20, 30), [HotRange(9, 2, 1)], max_write=60)
        # a 2-byte range straddling the shard 1/2 boundary touches two shards;
        # merging exactly those two is enough (the third stays separate).
        self.assertEqual(plan.new_write_bytes, 30)
        self.assertEqual(plan.checks[0].opens, 1)
        self.assertEqual([s.source for s in plan.segments], ["build", "reuse"])

    def test_cheapest_split_is_chosen(self):
        # The hot range only spans shards 2..3, so merging those suffices;
        # shard 1 remains a reusable whole body.
        plan = make_plan(entries(10, 20, 30), [HotRange(10, 50, 1)], max_write=60)
        self.assertEqual([s.source for s in plan.segments], ["reuse", "build"])
        self.assertEqual(plan.new_write_bytes, 50)

    def test_no_write_but_open_limits_impossible(self):
        with self.assertRaises(PlanError):
            make_plan(entries(10, 20, 30), [HotRange(0, 60, 2)], max_write=0)

    def test_validation_of_ranges(self):
        from artifact_repo.geometry import parse_hot_ranges

        with self.assertRaises(PlanError):
            parse_hot_ranges([(0, 0, 1)], 10)
        with self.assertRaises(PlanError):
            parse_hot_ranges([(5, 10, 1)], 10)
        with self.assertRaises(PlanError):
            parse_hot_ranges([(0, 1, 0)], 10)
        ok = parse_hot_ranges([(0, 10, 1)], 10)
        self.assertEqual(ok[0].max_open, 1)

    def test_manifest_validation(self):
        from artifact_repo.geometry import parse_manifest_entries

        with self.assertRaises(PlanError):
            parse_manifest_entries([(1, "zz")])
        with self.assertRaises(PlanError):
            parse_manifest_entries([(1, "0" * 64), (1, "0" * 64)])
        with self.assertRaises(PlanError):
            parse_manifest_entries([(13, "0" * 64)])
        with self.assertRaises(PlanError):
            parse_manifest_entries([])


if __name__ == "__main__":
    unittest.main()
