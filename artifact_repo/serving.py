"""HTTP range serving over a pinned release.

A reader for one byte range opens *all* body files that intersect that range
and keeps them open until the range has been streamed.  That makes the
"maximum number of body files a hot range may open" budget a real runtime
property rather than a planning abstraction: the number of open handles is
exactly the number of distinct body files the range touches.
"""

from __future__ import annotations

import hashlib
import re
from contextlib import ExitStack
from dataclasses import dataclass, field
from typing import Iterator

from .store import Release, Repo

RANGE_RE = re.compile(r"^bytes=(\d*)-(\d*)$")
STREAM_CHUNK = 64 * 1024


@dataclass
class RangeSpec:
    start: int
    end: int  # inclusive

    @property
    def length(self) -> int:
        return self.end - self.start + 1

    def content_range(self, total: int) -> str:
        return f"bytes {self.start}-{self.end}/{total}"


@dataclass
class ServeStats:
    release_id: int = 0
    start: int = 0
    length: int = 0
    planned_opens: int = 0
    body_files_opened: int = 0
    bytes_sent: int = 0
    opened_body_ids: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "release_id": self.release_id,
            "start": self.start,
            "length": self.length,
            "planned_opens": self.planned_opens,
            "body_files_opened": self.body_files_opened,
            "bytes_sent": self.bytes_sent,
            "opened_body_ids": list(self.opened_body_ids),
        }


class UnsatisfiableRange(Exception):
    pass


def parse_range_header(header: str | None, total: int) -> RangeSpec | None:
    """Parse a single-range ``Range`` header.

    Returns ``None`` when the header is absent, malformed or holds several
    ranges: RFC 9110 lets a server ignore bad range *syntax* and serve the
    full representation.  A syntactically valid range outside the
    representation raises :class:`UnsatisfiableRange` (HTTP 416).
    """
    if not header:
        return None
    header = header.strip()
    if "," in header:
        return None
    match = RANGE_RE.match(header)
    if not match:
        return None
    raw_start, raw_end = match.groups()
    if raw_start == "" and raw_end == "":
        return None
    if raw_start == "":
        suffix = int(raw_end)
        if suffix == 0 or total == 0:
            raise UnsatisfiableRange()
        return RangeSpec(max(total - suffix, 0), total - 1)
    start = int(raw_start)
    if raw_end == "":
        end = total - 1
    else:
        end = int(raw_end)
        if end < start:
            raise UnsatisfiableRange()
    if start >= total:
        raise UnsatisfiableRange()
    return RangeSpec(start, min(end, total - 1))


def intersecting_segments(release: Release, start: int, length: int):
    end = start + length
    return [
        seg
        for seg in release.segments
        if seg.offset < end and seg.offset + seg.size > start
    ]


def plan_open_count(release: Release, start: int, length: int) -> int:
    """Distinct body files a request for this range must open.

    Two segments may point into the same immutable body (recovered shards
    sharing one product body); the file is opened only once, so count
    distinct body ids rather than intersecting segments.
    """
    return len(
        {seg.body_id for seg in intersecting_segments(release, start, length)}
    )


def iter_product_bytes(
    repo: Repo, release: Release, start: int, length: int, stats: ServeStats
) -> Iterator[bytes]:
    """Yield the requested product bytes, holding all intersecting bodies open.

    Opening every intersecting body up front and closing them together is
    what makes the open-file budget enforceable: at the peak of the read the
    process holds exactly ``plan_open_count`` handles for this range.  A body
    shared by several segments is opened exactly once; each segment then
    seeks to its own ``body_offset`` and reads only its own bytes.
    """
    hits = intersecting_segments(release, start, length)
    range_end = start + length
    handles: dict[str, object] = {}
    body_of_path: dict[str, str] = {}
    with ExitStack() as stack:
        for seg in hits:
            path = repo.body_path_for(seg.body_id)
            if path not in handles:
                handles[path] = stack.enter_context(open(path, "rb"))
                body_of_path[path] = seg.body_id
        stats.body_files_opened = len(handles)
        stats.opened_body_ids = list(body_of_path.values())
        served = 0
        for seg in hits:
            seg_start = max(start, seg.offset)
            seg_stop = min(range_end, seg.offset + seg.size)
            take = seg_stop - seg_start
            if take <= 0:
                continue
            fh = handles[repo.body_path_for(seg.body_id)]
            # Seek inside the shared body file: this segment's bytes begin at
            # body_offset and the requested slice begins further in.
            fh.seek(seg.body_offset + (seg_start - seg.offset))
            left = take
            while left > 0:
                block = fh.read(min(STREAM_CHUNK, left))
                if not block:
                    raise OSError(f"正文 {seg.body_id} 在区间内提前结束")
                stats.bytes_sent += len(block)
                served += len(block)
                left -= len(block)
                yield block
    if served != length:
        raise OSError("发布清单覆盖的字节数少于请求区间")


def digest_range(repo: Repo, release: Release, start: int, length: int) -> str:
    stats = ServeStats()
    h = hashlib.sha256()
    for block in iter_product_bytes(repo, release, start, length, stats):
        h.update(block)
    return h.hexdigest()
