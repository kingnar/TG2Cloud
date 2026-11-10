"""Review captions and still thumbnails. No live Telegram access."""
from __future__ import annotations

import asyncio
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from tests.test_channel_local import ask
from tg2cloud.channel_bulk import inventory
from tg2cloud.channel_bulk.local_control import start_local_control
from tg2cloud.channel_bulk.review import image_kind, read_message, still_thumb

JPEG = b"\xff\xd8\xff" + b"synthetic-thumb"


class VideoSize:
    def __init__(self, size):
        self.size = size


class PhotoSize:
    def __init__(self, size):
        self.size = size


class ReviewTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        from tg2cloud.persistence.sqlite import TaskDB

        self.db = TaskDB(self.root / "tasks.db")
        self.calls = []
        self.owner = SimpleNamespace(
            settings=SimpleNamespace(data_dir=self.root, allowed_user_id=1),
            db=self.db,
            log=SimpleNamespace(warning=lambda *args, **kwargs: None),
        )
        self.owner.channel_bulk_fetch = self._unused_fetch
        self.control = await start_local_control(self.owner)
        self.sock = self.root / "channel-bulk" / "control.sock"
        started = await ask(self.sock, {
            "command": "inventory.start", "request_id": "reviewstart", "chat_id": -100555,
            "delay_seconds": 0.5,
        })
        self.inventory_id = started["inventory_id"]
        self.db._conn.execute(
            """INSERT INTO channel_inventory_items(
                inventory_id, message_id, link, duration, size, group_id, media_key,
                file_unique_id, mime, file_name, dedup
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (self.inventory_id, 11, "https://t.me/c/555/11", 4.0, 1000, "album", None, None, "video/mp4", "clip.mp4", "new"),
        )
        self.db._conn.execute(
            """INSERT INTO channel_inventory_items(
                inventory_id, message_id, link, duration, size, group_id, media_key,
                file_unique_id, mime, file_name, dedup
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (self.inventory_id, 12, "https://t.me/c/555/12", 9.0, 1000, "album", None, None, "video/mp4", "clip.mp4", "new"),
        )

    async def _unused_fetch(self, offset, limit):
        raise AssertionError("inventory paging must not run")

    async def asyncTearDown(self):
        await self.control.close()
        self.db.close()
        self.tmp.cleanup()

    def _reader(self, payloads):
        async def read_one(chat_id, message_id):
            self.calls.append((chat_id, message_id))
            return payloads[message_id]

        return read_one

    async def test_socket_hides_caption_rejects_paths_and_replays(self):
        secret = "synthetic-secret-caption"

        async def read_one(chat_id, message_id):
            self.calls.append(message_id)
            if message_id == 12:
                return {"caption": secret, "thumb": None, "note": "missing-thumbnail"}
            return {"caption": secret, "thumb": JPEG, "note": ""}

        self.owner.channel_bulk_review = read_one
        blocked = await ask(self.sock, {
            "command": "inventory.review", "request_id": "path00001",
            "inventory_id": self.inventory_id, "message_ids": [11], "path": "/tmp/out",
        })
        self.assertEqual(blocked["error"], "bad-request")
        self.assertEqual(self.calls, [])
        other = 0 if os.geteuid() != 0 else 1
        with patch("tg2cloud.channel_bulk.local_control.peer_cred", return_value=(1, other, 0)):
            denied = await ask(self.sock, {
                "command": "inventory.review", "request_id": "deniedrev",
                "inventory_id": self.inventory_id, "message_ids": [11],
            })
        self.assertEqual(denied["error"], "denied")
        first = await ask(self.sock, {
            "command": "inventory.review", "request_id": "review001",
            "inventory_id": self.inventory_id, "message_ids": [11, 12], "max_seconds": 30,
        })
        self.assertEqual(first["result"], "complete")
        self.assertNotIn("caption", first)
        self.assertNotIn(secret, json.dumps(first))
        self.assertEqual(first["export"], "review.json")
        again = await ask(self.sock, {
            "command": "inventory.review", "request_id": "review001",
            "inventory_id": self.inventory_id, "message_ids": [11, 12],
        })
        self.assertTrue(again["replayed"])
        self.assertEqual(self.calls, [11, 12])
        body = json.loads((self.root / "channel-bulk" / "review" / f"inventory-{self.inventory_id}" / "review.json").read_text())
        self.assertEqual(body["classification"], "unclassified")
        self.assertTrue(body["duration_is_not_an_ad_label"])
        self.assertEqual(body["items"][0]["caption"], secret)
        self.assertTrue(body["items"][0]["thumb"].endswith(".jpg"))
        self.assertIsNone(body["items"][1]["thumb"])
        self.assertNotIn("clip.mp4", json.dumps(body))
        audit = (self.root / "channel-bulk" / "audit.log").read_text(encoding="utf-8")
        self.assertNotIn(secret, audit)
        self.assertNotIn("clip.mp4", audit)
        preview = await ask(self.sock, {
            "command": "bulk.preview", "request_id": "stillprev",
            "inventory_id": self.inventory_id, "alias": "media",
        })
        self.assertEqual(preview["status"], "preview")

    async def test_limits_cancel_flood_and_same_client(self):
        from tg2cloud.channel_bulk.review import collect

        sleeps = []

        async def pause(seconds):
            sleeps.append(seconds)

        async def read_one(chat_id, message_id):
            return {"caption": "synthetic", "thumb": JPEG, "note": ""}

        result = await collect(
            self.db, self.root, self.inventory_id, [11, 12], read_one,
            now=lambda: 1_800_000_000.0, sleep=pause, max_seconds=30, max_total_bytes=len(JPEG),
        )
        self.assertEqual(result["result"], "bounded")
        self.assertEqual(sleeps, [0.5])
        self.assertNotIn("caption", result)

        class FloodWaitError(Exception):
            def __init__(self):
                self.seconds = 9

        async def flooding(chat_id, message_id):
            raise FloodWaitError()

        flooded = await collect(
            self.db, self.root, self.inventory_id, [11], flooding,
            now=lambda: 1_800_000_000.0, sleep=pause, max_seconds=30, max_total_bytes=1000,
        )
        self.assertEqual(flooded["result"], "flood")
        self.assertGreater(inventory.load(self.db, self.inventory_id)["cooldown_until"], 1_800_000_000.0)
        self.db._conn.execute("UPDATE channel_inventory SET cooldown_until=0 WHERE id=?", (self.inventory_id,))

        gate = asyncio.Event()
        entered = asyncio.Event()

        async def blocking(chat_id, message_id):
            if message_id == 11:
                entered.set()
                await gate.wait()
            return {"caption": "synthetic", "thumb": None, "note": "missing-thumbnail"}

        from tg2cloud.listening import account

        account.write_state(self.db, cooldown_until=0)
        task = asyncio.create_task(collect(
            self.db, self.root, self.inventory_id, [11, 12], blocking,
            now=time_now, sleep=pause, max_seconds=30, max_total_bytes=1000,
        ))
        await entered.wait()
        inventory.request_cancel(self.db, self.inventory_id, now=1_800_000_000.0)
        gate.set()
        cancelled = await task
        self.assertEqual(cancelled["result"], "cancelled")
        self.assertEqual(cancelled["reviewed"], 1)

        client = SimpleNamespace(download_calls=[])

        async def get_messages(entity, ids):
            return SimpleNamespace(
                id=ids, raw_text="synthetic caption", message="",
                document=SimpleNamespace(thumbs=[VideoSize(5000), PhotoSize(80)]),
                photo=None,
            )

        async def download_media(message, file, thumb=None):
            client.download_calls.append((file, type(thumb).__name__))
            return JPEG

        client.get_messages = get_messages
        client.download_media = download_media
        seen = []

        async def with_client(operation):
            seen.append(client)
            return await operation(client)

        self.owner.channel_bulk_review = None
        self.owner.listener = SimpleNamespace(client=client, with_client=with_client)
        with patch("tg2cloud.listening.service.verified_chat", return_value="entity"), patch(
            "tg2cloud.listening.account.client", side_effect=AssertionError("new client")
        ):
            from tg2cloud.channel_bulk.review import reader_for

            piece = await reader_for(self.owner)(-100555, 11)
            self.assertEqual(seen, [client])
            self.assertEqual(client.download_calls, [(bytes, "PhotoSize")])
            self.assertEqual(image_kind(piece["thumb"]), "jpg")
            only_video = SimpleNamespace(
                id=11, raw_text="", message="",
                document=SimpleNamespace(thumbs=[VideoSize(100)]), photo=None,
            )

            async def only_video_message(entity, ids):
                return only_video

            client.get_messages = only_video_message
            missing = await read_message(client, -100555, 11)
        self.assertIsNone(still_thumb(SimpleNamespace(
            document=SimpleNamespace(thumbs=[VideoSize(100)]), photo=None
        )))
        self.assertEqual(missing["note"], "missing-thumbnail")
        self.assertEqual(len(client.download_calls), 1)


def time_now():
    return 1_800_000_000.0


if __name__ == "__main__":
    unittest.main()
