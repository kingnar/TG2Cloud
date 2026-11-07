"""Local bulk gates. No Telegram, no live transfer, and no invented quota."""
from __future__ import annotations

import asyncio
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from tg2cloud.channel_bulk.inventory import exclude, start
from tg2cloud.channel_bulk.ledger import charge
from tg2cloud.channel_bulk.local_control import start_local_control
from tg2cloud.persistence.sqlite import TaskDB


async def ask(path, payload):
    reader, writer = await asyncio.open_unix_connection(str(path))
    writer.write((json.dumps(payload) + "\n").encode())
    await writer.drain()
    line = await reader.readline()
    writer.close()
    await writer.wait_closed()
    return json.loads(line)


class ResumeGateTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.db = TaskDB(self.root / "tasks.db")
        self.owner = SimpleNamespace(
            settings=SimpleNamespace(
                data_dir=self.root,
                allowed_user_id=1,
                download_dir=self.root,
                min_free_disk_bytes=0,
            ),
            db=self.db,
            log=SimpleNamespace(warning=lambda *args, **kwargs: None),
        )
        self.control = await start_local_control(self.owner)
        self.sock = self.root / "channel-bulk" / "control.sock"
        self.inventory = start(self.db, -100555, now=1)
        self._item(70, duration=90)
        self._item(4, duration=4)
        self._item(90, duration=90)
        exclude(self.db, -100555, 80, reason="review", now=1)
        self._item(80, duration=120)

    async def asyncTearDown(self):
        await self.control.close()
        self.db.close()
        self.tmp.cleanup()

    def _item(self, message_id, *, duration):
        self.db._conn.execute(
            """INSERT INTO channel_inventory_items(
                inventory_id, message_id, link, duration, size, group_id, media_key,
                file_unique_id, mime, file_name, dedup
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (
                self.inventory["id"], message_id, f"https://t.me/c/555/{message_id}",
                duration, 1000, "", f"document:{message_id}", None, "video/mp4",
                f"clip-{message_id}.mp4", "new",
            ),
        )

    async def test_socket_mode_and_resume_lists_every_closed_gate(self):
        self.assertEqual(self.sock.stat().st_mode & 0o777, 0o600)
        preview = await ask(self.sock, {
            "command": "bulk.preview", "request_id": "preview01",
            "inventory_id": self.inventory["id"], "alias": "media",
        })
        again = await ask(self.sock, {
            "command": "bulk.preview", "request_id": "preview02",
            "inventory_id": self.inventory["id"], "alias": "media",
        })
        self.assertEqual(again["job_id"], preview["job_id"])
        self.assertTrue(again["reused"])
        refused = await ask(self.sock, {
            "command": "bulk.resume", "request_id": "resume001",
            "job_id": preview["job_id"], "manifest_hash": "nope",
        })
        self.assertFalse(refused["ok"])
        self.assertEqual(refused["error"], "blocked")
        codes = {item["code"] for item in refused["blockers"]}
        self.assertEqual(codes, {"manifest", "directory", "egress", "budget", "rate", "attestation"})
        self.assertEqual(
            self.db._conn.execute(
                "SELECT state FROM channel_bulk_jobs WHERE id=?", (preview["job_id"],)
            ).fetchone()[0],
            "preview",
        )
        zero = await ask(self.sock, {
            "command": "bulk.limits", "request_id": "limits000",
            "job_id": preview["job_id"],
            "daily_bytes": 0, "monthly_bytes": 0, "down_bps": 0, "up_bps": 0,
        })
        self.assertEqual(zero["error"], "blocked")
        self.assertFalse(charge(self.db, 10, now=1, daily_cap=0, monthly_cap=0))
        missing = await ask(self.sock, {
            "command": "bulk.attest", "request_id": "attest000", "job_id": preview["job_id"],
        })
        self.assertEqual(missing["code"], "attestation")
        self.assertIsNone(self.db._conn.execute(
            "SELECT id FROM channel_quota_attestation"
        ).fetchone())

    async def test_manifest_freezes_the_reviewed_set_and_resume_is_idempotent(self):
        preview = await ask(self.sock, {
            "command": "bulk.preview", "request_id": "preview11",
            "inventory_id": self.inventory["id"], "alias": "media",
        })
        job_id = preview["job_id"]
        built = await ask(self.sock, {
            "command": "bulk.manifest", "request_id": "manifest1",
            "job_id": job_id, "through_message_id": 70,
        })
        self.assertEqual(built["count"], 1)
        self.assertEqual(built["held_back"], 1)
        self._item(60, duration=90)
        frozen = await ask(self.sock, {
            "command": "bulk.manifest", "request_id": "manifest2",
            "job_id": job_id, "through_message_id": 70,
        })
        self.assertEqual(frozen["count"], 1)
        self.assertGreaterEqual(frozen["held_back"], 1)
        self.assertEqual(frozen["hash"], built["hash"])
        expanded = await ask(self.sock, {
            "command": "bulk.manifest", "request_id": "manifest3",
            "job_id": job_id, "through_message_id": 70, "expand": True,
        })
        self.assertEqual(expanded["count"], 2)
        self.assertNotEqual(expanded["hash"], built["hash"])
        limits = await ask(self.sock, {
            "command": "bulk.limits", "request_id": "limits111",
            "job_id": job_id,
            "daily_bytes": 5000, "monthly_bytes": 9000, "down_bps": 1000, "up_bps": 1000,
        })
        self.assertTrue(limits["ok"])
        denied_ack = await ask(self.sock, {
            "command": "bulk.ack-egress", "request_id": "ack00000", "job_id": job_id, "ack": False,
        })
        self.assertFalse(denied_ack["ok"])
        allowed_ack = await ask(self.sock, {
            "command": "bulk.ack-egress", "request_id": "ack00001", "job_id": job_id, "ack": True,
        })
        self.assertTrue(allowed_ack["ok"])
        attested = await ask(self.sock, {
            "command": "bulk.attest", "request_id": "attest111", "job_id": job_id,
            "remaining_bytes": 8000, "period_end": 2_000_000_000,
        })
        self.assertTrue(attested["ok"])
        current = await ask(self.sock, {
            "command": "bulk.manifest", "request_id": "manifest4",
            "job_id": job_id, "through_message_id": 70, "expand": True,
        })
        self.assertEqual(current["count"], 2)
        self.db._conn.execute(
            "CREATE TABLE IF NOT EXISTS transfer_directories(alias TEXT PRIMARY KEY, path TEXT NOT NULL, owners TEXT NOT NULL)"
        )
        self.db._conn.execute(
            "INSERT INTO transfer_directories(alias, path, owners) VALUES('media', 'library/videos', '[1]')"
        )
        blocked = await ask(self.sock, {
            "command": "bulk.resume", "request_id": "resumeold",
            "job_id": job_id, "manifest_hash": built["hash"],
        })
        self.assertEqual(blocked["error"], "blocked")
        self.assertEqual(blocked["blockers"][0]["code"], "manifest")
        started = []

        async def fake_run(ctx):
            started.append(ctx.job_id)
            return "idle"

        with patch("tg2cloud.channel_bulk.control._account", return_value=1), patch(
            "tg2cloud.channel_bulk.control.run_job", new=fake_run,
        ):
            first = await ask(self.sock, {
                "command": "bulk.resume", "request_id": "resumego1",
                "job_id": job_id, "manifest_hash": current["hash"],
            })
            for _ in range(20):
                if started:
                    break
                await asyncio.sleep(0.01)
            second = await ask(self.sock, {
                "command": "bulk.resume", "request_id": "resumego2",
                "job_id": job_id, "manifest_hash": current["hash"],
            })
        self.assertTrue(first["ok"])
        self.assertFalse(first["idempotent"])
        self.assertTrue(second["idempotent"])
        self.assertEqual(started, [job_id])
        state = self.db._conn.execute(
            "SELECT state FROM channel_bulk_jobs WHERE id=?", (job_id,)
        ).fetchone()[0]
        self.assertEqual(state, "running")

    async def test_other_uid_cannot_resume(self):
        with patch("tg2cloud.channel_bulk.local_control.peer_cred", return_value=(1, os.geteuid() + 1, 0)):
            denied = await ask(self.sock, {
                "command": "bulk.resume", "request_id": "resumeden",
                "job_id": 1, "manifest_hash": "a" * 64,
            })
        self.assertEqual(denied["error"], "denied")


if __name__ == "__main__":
    unittest.main()
