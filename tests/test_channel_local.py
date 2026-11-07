"""Local socket control and fail-closed remote evidence. No network."""
from __future__ import annotations

import asyncio
import json
import os
import stat
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from tg2cloud.channel_bulk.local_control import (
    _audit,
    load_allow,
    peer_allowed,
    prepare_directory,
    redact,
    start_local_control,
)
from tg2cloud.channel_bulk.remote_evidence import assess
from tg2cloud.persistence.sqlite import TaskDB


def video(message_id):
    return SimpleNamespace(
        id=message_id,
        chat_id=-100555,
        grouped_id=None,
        video=SimpleNamespace(duration=80, size=1000, mime_type="video/mp4"),
        document=SimpleNamespace(id=message_id, size=1000, mime_type="video/mp4", attributes=[]),
        file=SimpleNamespace(name=f"clip-{message_id}.mp4", ext=".mp4", size=1000, mime_type="video/mp4"),
        media=object(),
        raw_text="",
        message="",
        noforwards=False,
    )


async def ask(path, payload):
    reader, writer = await asyncio.open_unix_connection(str(path))
    writer.write((json.dumps(payload) + "\n").encode())
    await writer.drain()
    line = await reader.readline()
    writer.close()
    await writer.wait_closed()
    return json.loads(line)


class LocalControlTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.db = TaskDB(self.root / "tasks.db")
        self.fetches = []
        self.owner = SimpleNamespace(
            settings=SimpleNamespace(data_dir=self.root, allowed_user_id=1),
            db=self.db,
            log=SimpleNamespace(warning=lambda *args, **kwargs: None),
        )

        async def fetch(offset, limit):
            self.fetches.append((offset, limit))
            if offset:
                return []
            return [video(20)]

        self.owner.channel_bulk_fetch = fetch
        self.control = await start_local_control(self.owner)
        self.sock = self.root / "channel-bulk" / "control.sock"

    async def asyncTearDown(self):
        await self.control.close()
        self.db.close()
        self.tmp.cleanup()

    async def test_startup_does_not_scan_and_same_uid_can_page(self):
        self.assertEqual(self.fetches, [])
        mode = stat.S_IMODE(self.sock.stat().st_mode)
        self.assertEqual(mode, 0o600)
        ready = await ask(self.sock, {"command": "ping", "request_id": "pingping"})
        self.assertTrue(ready["ok"])
        self.assertEqual(self.fetches, [])
        started = await ask(self.sock, {
            "command": "inventory.start", "request_id": "start0001", "chat_id": -100555,
            "page_size": 20, "delay_seconds": 0.5,
        })
        again = await ask(self.sock, {
            "command": "inventory.start", "request_id": "start0001", "chat_id": -100555,
        })
        self.assertTrue(again["replayed"])
        self.assertEqual(again["inventory_id"], started["inventory_id"])
        self.assertEqual(self.db._conn.execute("SELECT COUNT(*) FROM channel_inventory").fetchone()[0], 1)
        mismatch = await ask(self.sock, {
            "command": "inventory.status", "request_id": "start0001", "inventory_id": started["inventory_id"],
        })
        self.assertEqual(mismatch["error"], "replay-mismatch")
        ran = await ask(self.sock, {
            "command": "inventory.run", "request_id": "run000001",
            "inventory_id": started["inventory_id"], "max_pages": 1, "max_seconds": 30,
        })
        self.assertEqual(ran["result"], "bounded")
        self.assertEqual(ran["checkpoint_id"], 20)
        self.assertEqual(len(self.fetches), 1)
        replay = await ask(self.sock, {
            "command": "inventory.run", "request_id": "run000001",
            "inventory_id": started["inventory_id"], "max_pages": 1,
        })
        self.assertTrue(replay["replayed"])
        self.assertEqual(len(self.fetches), 1)
        exported = await ask(self.sock, {
            "command": "inventory.export", "request_id": "export001",
            "inventory_id": started["inventory_id"], "format": "csv", "path": "/tmp/out.csv",
        })
        self.assertEqual(exported["error"], "bad-request")
        exported = await ask(self.sock, {
            "command": "inventory.export", "request_id": "export002",
            "inventory_id": started["inventory_id"], "format": "csv",
        })
        self.assertTrue(str(exported["export"]).startswith("inventory-"))
        self.assertTrue((self.root / "channel-bulk" / "exports" / exported["export"]).is_file())
        preview = await ask(self.sock, {
            "command": "bulk.preview", "request_id": "preview01",
            "inventory_id": started["inventory_id"], "alias": "media",
        })
        self.assertEqual(preview["status"], "preview")
        reopened = TaskDB(self.root / "tasks.db")
        try:
            self.assertEqual(reopened._conn.execute("PRAGMA user_version").fetchone()[0], 3)
            self.assertEqual(
                reopened._conn.execute("SELECT COUNT(*) FROM channel_inventory").fetchone()[0], 1
            )
        finally:
            reopened.close()
        text = (self.root / "channel-bulk" / "audit.log").read_text(encoding="utf-8")
        self.assertNotIn("clip-", text)
        self.assertIn("inventory.run", text)

    async def test_other_uid_symlink_and_loose_allow_are_refused(self):
        other = 0 if os.geteuid() != 0 else 1
        denied = await self._denied(other)
        self.assertEqual(denied["error"], "denied")
        await self.control.close()
        link = self.root / "channel-bulk" / "control.sock"
        if link.exists() or link.is_symlink():
            link.unlink()
        link.symlink_to("/tmp")
        with self.assertRaises(PermissionError):
            prepare_directory(self.root)
        link.unlink()
        allow = self.root / "channel-bulk" / "local-allow"
        allow.write_text("1\n", encoding="utf-8")
        os.chmod(allow, 0o666)
        with self.assertRaises(PermissionError):
            load_allow(self.root / "channel-bulk", os.geteuid())
        self.assertFalse(peer_allowed(other, process_uid=os.geteuid(), allow=set()))
        self.assertTrue(peer_allowed(os.geteuid(), process_uid=os.geteuid(), allow=set()))
        secret = redact("password=hunter2 token=abc https://example.test/a")
        self.assertNotIn("hunter2", secret)
        self.assertNotIn("example.test", secret)
        _audit(self.root / "channel-bulk", {
            "uid": 1, "pid": 2, "command": "ping", "request_id": "audit0001",
            "ok": False, "error": "password=hunter2",
        })
        logged = (self.root / "channel-bulk" / "audit.log").read_text(encoding="utf-8")
        self.assertNotIn("hunter2", logged)
        package = Path("src/tg2cloud/channel_bulk/local_control.py").read_text(encoding="utf-8")
        self.assertNotIn("TelegramClient", package)

    async def _denied(self, uid):
        with patch("tg2cloud.channel_bulk.local_control.peer_cred", return_value=(1, uid, 0)):
            return await ask(self.sock, {"command": "ping", "request_id": "denied001"})

    async def test_cancel_stops_the_bounded_scan(self):
        started = await ask(self.sock, {
            "command": "inventory.start", "request_id": "start0002", "chat_id": -100555,
            "delay_seconds": 0.5,
        })
        gate = asyncio.Event()

        async def blocking(offset, limit):
            self.fetches.append((offset, limit))
            await gate.wait()
            return [video(9)]

        self.owner.channel_bulk_fetch = blocking
        run_task = asyncio.create_task(ask(self.sock, {
            "command": "inventory.run", "request_id": "run000002",
            "inventory_id": started["inventory_id"], "max_pages": 3, "max_seconds": 30,
        }))
        while not self.fetches:
            await asyncio.sleep(0.01)
        cancelled = await ask(self.sock, {
            "command": "inventory.cancel", "request_id": "cancel001",
            "inventory_id": started["inventory_id"],
        })
        self.assertEqual(cancelled["status"], "paused")
        gate.set()
        finished = await run_task
        self.assertEqual(finished["result"], "cancelled")
        self.assertEqual(len(self.fetches), 1)


class EvidenceTests(unittest.TestCase):
    def test_webdav_and_finish_without_identity_are_rejected(self):
        self.assertEqual(assess(
            {"scheme": "webdav", "method": "size", "size": 10, "cache_only": False},
            expected_size=10, expected_dest="library/a", expected_fingerprint="abc",
        ), "reject")
        self.assertEqual(assess(
            {
                "scheme": "clouddrive2-upload-task", "status": "Finish", "size": 10,
                "transferred_bytes": 10, "dest_path": "library/a",
                "source_fingerprint": "abc", "cache_only": False, "read_back": False,
                "destination_id": "id-1",
            },
            expected_size=10, expected_dest="library/a", expected_fingerprint="abc",
        ), "reject")
        self.assertEqual(assess(
            {
                "scheme": "clouddrive2-upload-task", "status": "Finish", "size": 10,
                "transferred_bytes": 10, "dest_path": "library/a",
                "source_fingerprint": "abc", "cache_only": False, "read_back": True,
                "destination_id": "id-1",
            },
            expected_size=10, expected_dest="library/a", expected_fingerprint="abc",
        ), "accept")


if __name__ == "__main__":
    unittest.main()
