#!/usr/bin/env python3
"""Exercise the shard-recovery flow against a running repository.

The script publishes a product built from merged bodies, deletes every staged
shard (simulating the lost build machine), recovers the shards from the
current product with both budgets, retires the old release, runs GC, then
re-publishes the recovered shards in a different order mixed with a fresh
upload and audits the result with real Range requests.

Usage: python scripts/recovery_demo.py http://127.0.0.1:8000
"""

from __future__ import annotations

import hashlib
import http.client
import io
import json
import os
import secrets
import sys
import urllib.parse
import urllib.request


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def multipart(fields: dict[str, str | list[str]], files=None) -> tuple[bytes, str]:
    boundary = "----recover" + secrets.token_hex(8)
    out = io.BytesIO()

    def write_field(name: str, value: str) -> None:
        out.write(f"--{boundary}\r\n".encode())
        out.write(f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode())
        out.write(value.encode() + b"\r\n")

    for name, value in fields.items():
        if isinstance(value, list):
            for item in value:  # repeated form field, e.g. multi-checkbox
                write_field(name, item)
        else:
            write_field(name, value)
    for name, (filename, data) in (files or {}).items():
        out.write(f"--{boundary}\r\n".encode())
        out.write(
            f'Content-Disposition: form-data; name="{name}"; filename="{filename}"\r\n'.encode()
        )
        out.write(b"Content-Type: application/octet-stream\r\n\r\n")
        out.write(data + b"\r\n")
    out.write(f"--{boundary}--\r\n".encode())
    return out.getvalue(), f"multipart/form-data; boundary={boundary}"


def post(base: str, path: str, fields: dict[str, str], files=None) -> str:
    body, ctype = multipart(fields, files)
    req = urllib.request.Request(base + path, data=body, headers={"Content-Type": ctype})
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.read().decode(errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.read().decode(errors="replace")


def state(base: str) -> dict:
    with urllib.request.urlopen(base + "/api/state", timeout=30) as resp:
        return json.loads(resp.read())


def get_range(base: str, start: int, end: int) -> tuple[int, bytes]:
    parsed = urllib.parse.urlparse(base)
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=120)
    conn.request("GET", "/artifact/current", headers={"Range": f"bytes={start}-{end}"})
    resp = conn.getresponse()
    data = resp.read()
    status = resp.status
    conn.close()
    return status, data


def main() -> None:
    base = (sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000").rstrip("/")

    shards = [os.urandom(4096) for _ in range(3)]
    product = b"".join(shards)
    for number, payload in enumerate(shards, 1):
        post(
            base, "/shards",
            {"number": str(number)},
            {"file": (f"shard-{number}.bin", payload)},
        )
    manifest = "\n".join(f"{n} {sha(p)}" for n, p in enumerate(shards, 1))
    # Force merging shards 1+2 into one shared body.
    post(
        base, "/publish",
        {
            "filename": "install.img",
            "manifest": manifest,
            "hot": "3900 400 1",
            "max_write": str(len(product)),
        },
    )
    st = state(base)
    release_id = st["active"]["id"]
    print(f"published release {release_id}: segments "
          f"{[s['numbers'] for s in st['active']['segments']]}")

    # Build machine loses every shard original.
    for number in range(1, 4):
        post(base, f"/shards/{number}/delete", {})
    assert state(base)["staged"] == [], "staging should be empty after deletion"
    print("all staged shards deleted (build machine lost originals)")

    # Recover all three from the current product: 1 reused, 2 copied, 3 reused.
    total = len(product)
    page = post(
        base, "/recovery",
        {
            "release_id": str(release_id),
            "recover": ["1", "2", "3"],
            "mode_1": "reuse",
            "mode_2": "copy",
            "mode_3": "reuse",
            "max_new_bytes": str(len(shards[1])),
            "max_dependency_bytes": str(total),
        },
    )
    assert "整批未补" not in page, "recovery failed"
    st = state(base)
    staged = {s["number"]: s for s in st["staged"]}
    assert set(staged) == {1, 2, 3}
    print("recovered shards:", {
        n: {"size": s["size"], "origin": s["origin"],
            "body_offset": s["body_offset"]}
        for n, s in staged.items()
    })
    # Verify individual shard downloads are byte-exact.
    for number, payload in enumerate(shards, 1):
        with urllib.request.urlopen(f"{base}/shards/{number}", timeout=60) as resp:
            assert resp.read() == payload, f"shard {number} download mismatch"
    print("each recovered shard downloads byte-exact with matching SHA-256")
    # Budget-rejection and conflict paths are covered by tests/test_recovery.py.

    # Retire the old release by publishing a new one, then run GC.
    fresh = b"F" * 2048
    post(base, "/shards", {"number": "5"}, {"file": ("s5.bin", fresh)})
    order = [3, 1, 5, 2]
    payload_by_number = {1: shards[0], 2: shards[1], 3: shards[2], 5: fresh}
    new_product = b"".join(payload_by_number[n] for n in order)
    post(
        base, "/publish",
        {
            "filename": "install2.img",
            "manifest": "\n".join(
                f"{n} {sha(payload_by_number[n])}" for n in order
            ),
            # Re-publishing still enforces both publish budgets.  The four
            # segments span four distinct body files here (shard 2 was saved
            # as a standalone copy), so the whole-product open limit is 4.
            "hot": f"0 {len(new_product)} 4",
            "max_write": "0",
        },
    )
    post(base, "/gc", {})
    st = state(base)
    print(f"new release {st['active']['id']} published; old release retired; "
          f"bodies on disk: {st['stats']['bodies']}")

    # Range audit across the recovered-shard boundaries.
    for start, length in [(0, 64), (4095, 2), (4096, 64), (len(new_product) - 1, 1)]:
        status, data = get_range(base, start, start + length - 1)
        assert status == 206
        assert data == new_product[start : start + length], (start, length)
    print("range audit across recovered boundaries: MATCH")

    # Whole product check.
    status, data = get_range(base, 0, len(new_product) - 1)
    assert data == new_product
    print(f"full product {len(data)} bytes matches expected; sha256={sha(data)}")


if __name__ == "__main__":
    main()
