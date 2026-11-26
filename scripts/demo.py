#!/usr/bin/env python3
"""Drive a running repository over HTTP and print a cross-segment range audit.

Usage: python scripts/demo.py http://127.0.0.1:8000
Assumes a fresh repository (no release yet).
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


def multipart(fields: dict[str, str], files: dict[str, tuple[str, bytes]]) -> tuple[bytes, str]:
    boundary = "----demo" + secrets.token_hex(8)
    out = io.BytesIO()
    for name, value in fields.items():
        out.write(f"--{boundary}\r\n".encode())
        out.write(f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode())
        out.write(value.encode() + b"\r\n")
    for name, (filename, data) in files.items():
        out.write(f"--{boundary}\r\n".encode())
        out.write(
            f'Content-Disposition: form-data; name="{name}"; filename="{filename}"\r\n'.encode()
        )
        out.write(b"Content-Type: application/octet-stream\r\n\r\n")
        out.write(data + b"\r\n")
    out.write(f"--{boundary}--\r\n".encode())
    return out.getvalue(), f"multipart/form-data; boundary={boundary}"


def post(base: str, path: str, fields: dict[str, str], files=None) -> str:
    body, ctype = multipart(fields, files or {})
    req = urllib.request.Request(base + path, data=body, headers={"Content-Type": ctype})
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.read().decode(errors="replace")
    except urllib.error.HTTPError as exc:  # redirects disabled handler not used
        return exc.read().decode(errors="replace")


def get_range(base: str, start: int, end: int) -> tuple[int, dict, bytes]:
    parsed = urllib.parse.urlparse(base)
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=120)
    conn.request(
        "GET", "/artifact/current", headers={"Range": f"bytes={start}-{end}"}
    )
    resp = conn.getresponse()
    data = resp.read()
    headers = {k.lower(): v for k, v in resp.getheaders()}
    conn.close()
    return resp.status, headers, data


def state(base: str) -> dict:
    with urllib.request.urlopen(base + "/api/state", timeout=30) as resp:
        return json.loads(resp.read())


def main() -> None:
    base = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
    base = base.rstrip("/")

    shards = [
        os.urandom(64 * 1024 + i * 7919) for i in range(4)
    ]
    product = b"".join(shards)
    for number, payload in enumerate(shards, 1):
        post(
            base,
            "/shards",
            {"number": str(number)},
            {"file": (f"shard-{number}.bin", payload)},
        )
        print(f"uploaded shard {number}: {len(payload)} bytes sha256={sha(payload)}")

    manifest = "\n".join(
        f"{n} {sha(p)}" for n, p in enumerate(shards, 1)
    )
    # Budgets chosen so exactly one merge is required: a 64-byte hot range
    # straddling the shard 1/2 boundary may open only one body file, so shards
    # 1+2 must be merged; the whole-product range then sees 3 bodies (the
    # merged one plus shards 3 and 4) and its limit is 3.  The new-write cap is
    # exactly the merged size, so a layout that also merges 3+4 is rejected.
    merged = len(shards[0]) + len(shards[1])
    hot = f"0 {len(product)} 3\n{len(shards[0]) - 16} 64 1"
    post(
        base,
        "/publish",
        {
            "filename": "install.img",
            "manifest": manifest,
            "hot": hot,
            "max_write": str(merged),
        },
    )
    st = state(base)
    active = st["active"]
    assert active is not None, "publish failed"
    print("\npublished release", active["id"], "size", active["total_size"],
          "new-write bytes", active["new_write_bytes"])
    for seg in active["segments"]:
        print("  segment shards", seg["numbers"], "offset", seg["offset"],
              "size", seg["size"], "source", seg["source"])

    segs = active["segments"]
    print("\ncross-segment HTTP Range audit")
    for prev, nxt in zip(segs, segs[1:]):
        for start, end in [
            (nxt["offset"] - 1, nxt["offset"]),
            (nxt["offset"] - 5000, nxt["offset"] + 5000),
        ]:
            start = max(0, start)
            end = min(len(product) - 1, end)
            status, headers, data = get_range(base, start, end)
            ok = data == product[start : end + 1]
            print(f"  bytes={start}-{end} -> {status} {headers.get('content-range')} "
                  f"{'MATCH' if ok else 'MISMATCH'} ({len(data)} bytes)")
            assert ok

    # Trial fetch through the web UI's real loopback HTTP path.
    import re

    with urllib.request.urlopen(
        f"{base}/verify?start={segs[1]['offset'] - 16}&length=64", timeout=120
    ) as resp:
        page = resp.read().decode()
    opened = re.search(r'id="opened">([^<]*)<', page).group(1)
    written = re.search(r'id="newwrite">([^<]*)<', page).group(1)
    print(f"\n/verify reports opened_body_files={opened}, release_new_write_bytes={written}")

    # Second release: replace shard 2 and republish; stable URL must switch.
    new_shard = os.urandom(len(shards[1]))
    post(base, "/shards", {"number": "2"}, {"file": ("s2.bin", new_shard)})
    new_shards = [shards[0], new_shard] + shards[2:]
    new_product = b"".join(new_shards)
    post(
        base,
        "/publish",
        {
            "filename": "install.img",
            "manifest": "\n".join(f"{n} {sha(p)}" for n, p in enumerate(new_shards, 1)),
            "hot": "",
            "max_write": "0",
        },
    )
    old_id = active["id"]
    st = state(base)
    print("\nafter second publish:", "release", st["active"]["id"], "was", old_id,
          "new-write", st["active"]["new_write_bytes"])
    status, headers, data = get_range(base, 0, len(product) - 1)
    print("  stable URL whole product:", status,
          "matches new product" if data == new_product else "MISMATCH")
    assert data == new_product
    print("  body files now:", st["stats"]["bodies"],
          "releases:", st["stats"]["releases"],
          "retired:", st["stats"]["retired_releases"])


if __name__ == "__main__":
    main()
