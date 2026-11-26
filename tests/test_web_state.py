"""Pin bookkeeping and stable-address behaviour through the Flask test client."""

import hashlib
import io
import tempfile
import unittest

from artifact_repo.store import Repo
from artifact_repo.web import create_app


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class PinBookkeeping(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Repo(self.tmp.name)
        data = b"x" * 4096
        self.repo.store_shard(1, io.BytesIO(data))
        self.repo.publish(
            manifest_text=f"1 {sha(data)}", hot_ranges_text="", max_write=0
        )
        self.app = create_app(self.repo)
        self.client = self.app.test_client()

    def tearDown(self):
        self.tmp.cleanup()

    def pins(self):
        return self.repo.get_active_release().pinned_readers

    def test_download_releases_pin(self):
        resp = self.client.get("/artifact/current")
        self.assertEqual(self.pins(), 1)
        resp.get_data()
        self.assertEqual(self.pins(), 0)

    def test_trial_requests_do_not_leak_pins(self):
        for _ in range(4):
            resp = self.client.get("/verify?start=0&length=10")
            self.assertEqual(resp.status_code, 200)
        self.assertEqual(self.pins(), 0)

    def test_forged_trial_token_cannot_hold_pin(self):
        resp = self.client.get(
            "/artifact/current", headers={"X-Trial-Token": "not-issued-here"}
        )
        resp.get_data()
        resp.close()
        self.assertEqual(self.pins(), 0)

    def test_stable_address_switches_release(self):
        old = self.repo.get_active_release()
        new_data = b"y" * 4096
        self.repo.store_shard(1, io.BytesIO(new_data))
        second = self.repo.publish(
            manifest_text=f"1 {sha(new_data)}", hot_ranges_text="", max_write=0
        )
        self.assertEqual(self.client.get("/artifact/current").data, new_data)
        self.assertEqual(second.actual_sha256, sha(new_data))
        retired = self.repo.get_release(old.id)
        self.assertIsNotNone(retired.retired_at)

    def test_ui_pages_render(self):
        for path in ("/", "/api/state", "/api/range-check?start=0&length=16"):
            resp = self.client.get(path)
            self.assertEqual(resp.status_code, 200, path)
        verify = self.client.get("/verify?start=0&length=16")
        self.assertIn("实际打开正文文件数", verify.get_data(as_text=True))
        self.assertIn("本次发布新写正文字节数", verify.get_data(as_text=True))

    def test_publish_with_simulated_failure_via_form(self):
        old = self.repo.get_active_release()
        self.repo.store_shard(1, io.BytesIO(b"z" * 100))
        body = self.client.post(
            "/publish",
            data={
                "filename": "install.img",
                "manifest": f"1 {sha(b'a' * 10)}",
                "hot": "",
                "max_write": "0",
                "simulate_failure": "on",
            },
            follow_redirects=True,
        ).get_data(as_text=True)
        self.assertIn("发布失败", body)
        self.assertEqual(self.repo.get_active_release().id, old.id)
        self.assertEqual([s.number for s in self.repo.staged_shards()], [1])


if __name__ == "__main__":
    unittest.main()
