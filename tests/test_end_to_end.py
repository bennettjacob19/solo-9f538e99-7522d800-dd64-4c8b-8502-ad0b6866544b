"""End-to-end checks against a real HTTP server (no Docker required)."""

import hashlib
import http.client
import io
import os
import socket
import tempfile
import threading
import time
import unittest
import urllib.request

from werkzeug.serving import make_server

from artifact_repo.store import Repo
from artifact_repo.web import create_app


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class EndToEnd(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.repo = Repo(cls.tmp.name)
        cls.parts = [os.urandom(4096 + i * 1024) for i in range(5)]
        cls.product = b"".join(cls.parts)
        for n, p in enumerate(cls.parts, 1):
            cls.repo.store_shard(n, io.BytesIO(p))
        cls.manifest = "\n".join(
            f"{n} {sha(p)}" for n, p in enumerate(cls.parts, 1)
        )
        # Force merging: a whole-product hot range that may open only 2 bodies.
        cls.result = cls.repo.publish(
            manifest_text=cls.manifest,
            hot_ranges_text=f"0 {len(cls.product)} 2",
            max_write=len(cls.product),
        )
        cls.app = create_app(cls.repo)
        cls.port = free_port()
        cls.server = make_server("127.0.0.1", cls.port, cls.app, threaded=True)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.thread.join(timeout=5)
        cls.tmp.cleanup()

    def range_get(self, start, end):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=30)
        conn.request(
            "GET", "/artifact/current", headers={"Range": f"bytes={start}-{end}"}
        )
        resp = conn.getresponse()
        data = resp.read()
        headers = dict(resp.getheaders())
        conn.close()
        return resp.status, headers, data

    def test_cross_segment_ranges(self):
        segs = self.result.release.segments
        self.assertGreaterEqual(len(segs), 1)
        ranges = [(0, 0), (0, len(self.product) - 1)]
        for seg in segs[1:]:
            ranges.append((seg.offset - 1, seg.offset))
            ranges.append((max(0, seg.offset - 100), min(len(self.product) - 1, seg.offset + 100)))
        for start, end in ranges:
            status, headers, data = self.range_get(start, end)
            self.assertIn(status, (200, 206))
            self.assertEqual(data, self.product[start : end + 1], (start, end))
            if status == 206:
                self.assertEqual(
                    headers["Content-Range"],
                    f"bytes {start}-{end}/{len(self.product)}",
                )

    def test_head_and_416(self):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        conn.request("HEAD", "/artifact/current")
        resp = conn.getresponse()
        resp.read()
        self.assertEqual(resp.status, 200)
        self.assertEqual(int(resp.getheader("Content-Length")), len(self.product))
        conn.close()
        status, _, _ = self.range_get(len(self.product), len(self.product) + 10)
        self.assertEqual(status, 416)

    def test_verify_page_uses_real_http(self):
        with urllib.request.urlopen(
            f"http://127.0.0.1:{self.port}/verify?start=0&length={len(self.product)}",
            timeout=60,
        ) as resp:
            body = resp.read().decode()
        self.assertIn("收到的字节与本地成品快照一致", body)
        self.assertIn('id="newwrite">', body)

    def test_restart_serves_last_release(self):
        reopened = Repo(self.tmp.name)
        active = reopened.get_active_release()
        self.assertIsNotNone(active)
        self.assertEqual(active.product_sha256, self.result.actual_sha256)
        from artifact_repo.serving import iter_product_bytes, ServeStats

        st = ServeStats()
        data = b"".join(
            iter_product_bytes(reopened, active, 0, active.total_size, st)
        )
        self.assertEqual(data, self.product)


class InFlightDownload(unittest.TestCase):
    """A reader that is still streaming must finish reading the old product."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Repo(self.tmp.name)
        self.parts = [os.urandom(2 * 1024 * 1024) for _ in range(4)]
        self.product = b"".join(self.parts)
        for n, p in enumerate(self.parts, 1):
            self.repo.store_shard(n, io.BytesIO(p))
        self.manifest = "\n".join(
            f"{n} {sha(p)}" for n, p in enumerate(self.parts, 1)
        )
        self.repo.publish(manifest_text=self.manifest, hot_ranges_text="", max_write=0)
        self.app = create_app(self.repo)
        self.port = free_port()
        self.server = make_server("127.0.0.1", self.port, self.app, threaded=True)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.thread.join(timeout=5)
        self.tmp.cleanup()

    def test_in_flight_reader_gets_old_product(self):
        import socket  # local names only

        sock = socket.socket()
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        sock.settimeout(30)
        sock.connect(("127.0.0.1", self.port))
        sock.sendall(
            b"GET /artifact/current HTTP/1.1\r\nHost: x\r\n"
            b"Range: bytes=0-" + str(len(self.product) - 1).encode() + b"\r\n\r\n"
        )
        # Read just enough to get the headers plus a little body, then stall so
        # the server-side generator blocks with its release pin still held.
        first = sock.recv(4096)
        time.sleep(0.5)
        first += sock.recv(1024)
        self.assertIn(b"206", first.split(b"\r\n", 1)[0])
        body_start = first.index(b"\r\n\r\n") + 4
        received = first[body_start:]

        old = self.repo.get_active_release()
        old_paths = [self.repo.body_path_for(s.body_id) for s in old.segments]
        self.assertTrue(all(os.path.exists(p) for p in old_paths))

        # Publish a different product while the download is stalled.
        new_parts = [b"W" * 2048] + self.parts[1:]
        for n, p in enumerate(new_parts, 1):
            self.repo.store_shard(n, io.BytesIO(p))
        self.repo.publish(
            manifest_text="\n".join(
                f"{n} {sha(p)}" for n, p in enumerate(new_parts, 1)
            ),
            hot_ranges_text="",
            max_write=0,
        )
        new_product = b"".join(new_parts)
        self.assertNotEqual(new_product, self.product)
        old_release = self.repo.get_release(old.id)
        self.assertIsNotNone(old_release.retired_at)

        # The old bodies must still be on disk mid-flight.
        for path in old_paths:
            self.assertTrue(os.path.exists(path), "pinned body was collected early")

        # Drain the stalled connection; it must yield exactly the old product.
        sock.settimeout(60)
        while len(received) < len(self.product):
            chunk = sock.recv(65536)
            if not chunk:
                break
            received += chunk
        sock.close()
        self.assertEqual(received, self.product)

        # With the reader gone, the retired release's bodies are reclaimed.
        deadline = time.time() + 10
        while time.time() < deadline:
            if not any(os.path.exists(p) for p in old_paths if p not in {
                self.repo.body_path_for(s.body_id)
                for s in self.repo.get_active_release().segments
            }):
                break
            time.sleep(0.1)
        active_paths = {
            self.repo.body_path_for(s.body_id)
            for s in self.repo.get_active_release().segments
        }
        for path in old_paths:
            if path not in active_paths:
                self.assertFalse(os.path.exists(path))

        # The stable URL now serves the new product.
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=30)
        conn.request("GET", "/artifact/current")
        resp = conn.getresponse()
        data = resp.read()
        conn.close()
        self.assertEqual(data, new_product)


if __name__ == "__main__":
    unittest.main()
