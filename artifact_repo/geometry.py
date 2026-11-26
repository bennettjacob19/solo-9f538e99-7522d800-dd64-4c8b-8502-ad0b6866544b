"""Pure planning logic: partition the manifest into body-file segments and
check the two publishing budgets.

A release is the concatenation of the shards listed *in manifest order*.  The
manifest is cut into contiguous blocks:

* a block of one shard is served by re-using that shard's existing body file
  (no new bytes written, one body file opened for a range);
* a block of two or more adjacent shards is materialised as one new body file
  (its bytes count against the new-write budget, but the whole block is one
  body file opened for a range).

Both budgets must be satisfiable:

* for every hot byte range, the number of body files a reader needs open at
  the same time (i.e. the number of blocks intersecting the range) must not
  exceed that range's limit;
* the sum of bytes of all newly built blocks must not exceed the release's
  new-write cap.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Iterable, Sequence

MAX_SHARDS = 12


class PlanError(Exception):
    """No publishable layout exists for the supplied limits."""


@dataclass(frozen=True)
class ManifestEntry:
    number: int
    sha256: str
    size: int
    # Optional pointer into the immutable body that currently stores this
    # shard's bytes.  Ordinary uploads own a whole body (offset 0); shards
    # recovered from a published product may share a body and store only a
    # sub-range of it.
    body_id: str | None = None
    body_offset: int = 0


@dataclass(frozen=True)
class HotRange:
    start: int
    length: int
    max_open: int


@dataclass(frozen=True)
class SegmentRef:
    """The physical body file (and sub-range) backing one shard."""

    body_id: str
    body_offset: int = 0


@dataclass(frozen=True)
class SegmentPlan:
    first_number: int
    numbers: tuple[int, ...]
    offset: int
    size: int
    source: str  # "reuse" | "build"
    # Distinct body files a reader opens for this segment.  A single-shard
    # segment reusing a shard whose bytes live inside a shared body opens that
    # one body; a merged segment is materialised as its own new body.
    refs: tuple[SegmentRef, ...] = ()

    @property
    def end(self) -> int:
        return self.offset + self.size

    @property
    def body_ids(self) -> tuple[str, ...]:
        return tuple(r.body_id for r in self.refs)


@dataclass(frozen=True)
class RangeCheck:
    start: int
    length: int
    max_open: int
    opens: int


@dataclass
class Plan:
    segments: list[SegmentPlan]
    checks: list[RangeCheck]
    new_write_bytes: int
    total_size: int

    @property
    def build_segments(self) -> list[SegmentPlan]:
        return [s for s in self.segments if s.source == "build"]


@dataclass
class _Candidate:
    lengths: tuple[int, ...]
    segments: list[SegmentPlan]
    writes: int
    checks: list[RangeCheck]


def parse_manifest_entries(rows: Iterable[tuple[int, str]]) -> list[tuple[int, str]]:
    """Normalise and validate the submitted ordered (number, sha256) list."""
    out: list[tuple[int, str]] = []
    seen: set[int] = set()
    for number, sha in rows:
        if not isinstance(number, int) or not (1 <= number <= MAX_SHARDS):
            raise PlanError(f"分片号 {number!r} 超出 1..{MAX_SHARDS}")
        if number in seen:
            raise PlanError(f"分片 {number} 在清单中出现了不止一次")
        seen.add(number)
        h = (sha or "").strip().lower()
        if len(h) != 64 or any(c not in "0123456789abcdef" for c in h):
            raise PlanError(f"分片 {number}：SHA-256 必须是 64 位十六进制")
        out.append((number, h))
    if not out:
        raise PlanError("清单为空")
    if len(out) > MAX_SHARDS:
        raise PlanError(f"一次最多发布 {MAX_SHARDS} 个分片")
    return out


def parse_hot_ranges(rows: Iterable[tuple[int, int, int]], total_size: int) -> list[HotRange]:
    out: list[HotRange] = []
    for start, length, max_open in rows:
        if start < 0:
            raise PlanError(f"常用区间起始偏移 {start} 为负")
        if length <= 0:
            raise PlanError(f"常用区间 {start}+{length}：长度必须为正")
        if max_open < 1:
            raise PlanError(f"常用区间 {start}+{length}：最多打开文件数至少为 1")
        if start + length > total_size:
            raise PlanError(
                f"常用区间 {start}+{length} 超出 {total_size} 字节的成品范围"
            )
        out.append(HotRange(start, length, max_open))
    return out


@lru_cache(maxsize=None)
def _compositions(n: int) -> tuple[tuple[int, ...], ...]:
    """All compositions of n into positive block lengths (2**(n-1) of them)."""
    if n == 0:
        return ((),)
    result: list[tuple[int, ...]] = []

    def rec(remaining: int, prefix: tuple[int, ...]) -> None:
        if remaining == 0:
            result.append(prefix)
            return
        for take in range(1, remaining + 1):
            rec(remaining - take, prefix + (take,))

    rec(n, ())
    return tuple(result)


def _materialise(lengths: Sequence[int], entries: Sequence[ManifestEntry]) -> _Candidate:
    segments: list[SegmentPlan] = []
    pos = 0
    i = 0
    writes = 0
    build_serial = 0
    reuse_serial = 0
    for length in lengths:
        block = entries[i : i + length]
        size = sum(e.size for e in block)
        source = "reuse" if length == 1 else "build"
        if source == "build":
            writes += size
            # A built block becomes its own brand-new body file.  Its real id
            # is assigned later by the store; a unique placeholder keeps the
            # distinct-body open-file accounting correct at planning time.
            build_serial += 1
            refs: tuple[SegmentRef, ...] = (
                SegmentRef(f"__build_{build_serial}__", 0),
            )
        else:
            entry = block[0]
            if entry.body_id:
                # Real pointer: two adjacent recovered shards sharing one body
                # genuinely open one file, so they share this id.
                refs = (SegmentRef(entry.body_id, entry.body_offset),)
            else:
                # Ordinary uploads without a shared-body pointer each own a
                # distinct whole-body file at publish time.
                reuse_serial += 1
                refs = (SegmentRef(f"__reuse_{reuse_serial}__", 0),)
        segments.append(
            SegmentPlan(
                first_number=block[0].number,
                numbers=tuple(e.number for e in block),
                offset=pos,
                size=size,
                source=source,
                refs=refs,
            )
        )
        pos += size
        i += length
    return _Candidate(tuple(lengths), segments, writes, [])


def _opens_for(segments: Sequence[SegmentPlan], hot: HotRange) -> int:
    end = hot.start + hot.length
    # A shared body backing several adjacent single-shard segments is opened
    # only once for one request, so the budget counts distinct body files
    # intersecting the range rather than intersecting segments.
    bodies = set()
    for s in segments:
        if s.offset < end and s.end > hot.start:
            bodies.update(s.body_ids)
    return len(bodies)


def enumerate_plans(
    entries: Sequence[ManifestEntry],
) -> Iterable[_Candidate]:
    for lengths in _compositions(len(entries)):
        yield _materialise(lengths, entries)


def make_plan(
    entries: Sequence[ManifestEntry],
    hot_ranges: Sequence[HotRange],
    max_write: int,
) -> Plan:
    """Find a layout satisfying both budgets.

    Layouts are enumerated exhaustively (a manifest has at most 12 shards, so
    at most 2**11 = 2048 compositions).  Among feasible layouts we minimise
    newly written bytes first and the number of body files second, so a
    release reuses every whole shard it can and only merges where a hot range
    or the write cap demands it.  Feasibility is decided per hot range: the
    number of body-file blocks the range intersects may not exceed that
    range's own open-file limit.  Raises :class:`PlanError` when infeasible.
    """
    if max_write < 0:
        raise PlanError("新写正文字节上限不能为负")
    if not entries:
        raise PlanError("清单为空")
    total = sum(e.size for e in entries)
    if total == 0 and hot_ranges:
        raise PlanError("成品为 0 字节，无法提供常用区间")

    best: _Candidate | None = None
    best_key: tuple[int, int, tuple[int, ...]] | None = None

    # Diagnostics for an infeasible request.
    min_opens_any = None      # fewest peak opens of any layout, ignore writes
    min_write_any = None      # fewest written bytes of any layout, ignore opens
    min_write_for_opens = None  # cheapest layout meeting every open limit

    for cand in enumerate_plans(entries):
        checks = [
            RangeCheck(h.start, h.length, h.max_open, _opens_for(cand.segments, h))
            for h in hot_ranges
        ]
        peak = max((c.opens for c in checks), default=0)
        min_opens_any = peak if min_opens_any is None else min(min_opens_any, peak)
        min_write_any = (
            cand.writes if min_write_any is None else min(min_write_any, cand.writes)
        )
        opens_ok = all(c.opens <= c.max_open for c in checks)
        if opens_ok and (
            min_write_for_opens is None or cand.writes < min_write_for_opens
        ):
            min_write_for_opens = cand.writes
        if not opens_ok or cand.writes > max_write:
            continue
        key = (cand.writes, len(cand.segments), cand.lengths)
        if best_key is None or key < best_key:
            best_key = key
            best = _Candidate(cand.lengths, cand.segments, cand.writes, checks)

    if best is None:
        details: list[str] = []
        if hot_ranges and min_opens_any is not None:
            worst_limit = min(h.max_open for h in hot_ranges)
            if min_opens_any > worst_limit:
                details.append(
                    "无解：每个常用区间至多允许打开 "
                    f"{'、'.join(str(h.max_open) for h in hot_ranges)} 个正文文件，"
                    f"但任何分段方式下这些区间至少要同时打开 {min_opens_any} 个"
                )
        if (
            min_write_for_opens is not None
            and min_write_for_opens > max_write
        ):
            details.append(
                "无解：满足全部打开文件额度最少需新写 "
                f"{min_write_for_opens} 字节正文，超过本次 {max_write} 字节上限"
            )
        if not details:
            if min_write_any is not None and min_write_any > max_write:
                details.append(
                    f"无解：最少需新写 {min_write_any} 字节正文，"
                    f"超过本次 {max_write} 字节上限"
                )
            else:
                details.append("无解：常用区间打开文件额度与新写正文额度无法同时满足")
        raise PlanError("；".join(details))

    return Plan(
        segments=best.segments,
        checks=best.checks,
        new_write_bytes=best.writes,
        total_size=total,
    )
