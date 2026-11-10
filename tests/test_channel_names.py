"""Caption filenames, deleted-source skips, and WebDAV rename. No network."""
from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from urllib.error import HTTPError, URLError

from tg2cloud.channel_bulk.local_control import _dry_run
from tg2cloud.channel_bulk.names import (
    SourceMissing,
    allocate_remote,
    display_filename,
    sanitize_stem,
)
from tg2cloud.channel_bulk.rename_remote import (
    WebDavError,
    WebDavFiles,
    rename_completed,
)
from tg2cloud.channel_bulk.runtime import download_bounded
from tg2cloud.persistence.sqlite import TaskDB


class NameRuleTests(unittest.TestCase):
    def test_sanitizes_caption_and_keeps_chinese_punctuation(self):
        raw = "  城市/夜景：记录*?\"<>|  #旅行  https://example.com/a  \n第二行\n"
        self.assertEqual(sanitize_stem(raw), "城市夜景：记录")
        self.assertEqual(
            display_filename("你好，世界！\n#标签", "1.mp4", 9, "video/mp4"),
            "你好，世界！.mp4",
        )
        self.assertEqual(
            display_filename("第一行\r\n第二行", "clip.mkv", 3, None),
            "第一行.mkv",
        )
        self.assertEqual(display_filename("", "1.mp4", 86, "video/mp4"), "video-86.mp4")
        self.assertEqual(display_filename("   \n  ", "城市.mp4", 86, None), "城市.mp4")
        self.assertEqual(display_filename(None, "7.mp4", 4, None), "video-4.mp4")

    def test_stem_stops_on_a_utf8_boundary(self):
        stem = sanitize_stem("测" * 80)
        self.assertLessEqual(len(stem.encode("utf-8")), 200)
        self.assertEqual(len(stem.encode("utf-8")), 198)
        self.assertTrue(stem.endswith("测"))
        named = display_filename(stem + "测", "movie.MP4", 1, None)
        self.assertTrue(named.endswith(".MP4"))
        self.assertLessEqual(len(named[: -len(".MP4")].encode("utf-8")), 200)

    def test_collision_suffix_uses_the_message_id(self):
        used = {"library/同一标题.mp4"}

        async def taken(path):
            return path in used

        chosen = asyncio.run(allocate_remote("library", "同一标题.mp4", 1479, taken))
        self.assertEqual(chosen, "library/同一标题 (message-1479).mp4")
        used.add(chosen)
        again = asyncio.run(allocate_remote("library", "同一标题.mp4", 1479, taken))
        self.assertEqual(again, "library/同一标题 (message-1479-2).mp4")

    def test_missing_media_is_not_a_runtime_error(self):
        message = SimpleNamespace(document=None)
        with self.assertRaises(SourceMissing) as caught:
            asyncio.run(download_bounded(None, message, Path("unused.part"), None, 10))
        self.assertEqual(str(caught.exception), "消息没有可下载的媒体")


class FakeDav:
    def __init__(self, files, blocked=()):
        self.files = dict(files)
        self.blocked = set(blocked)
        self.moves = []

    def size(self, path):
        if path in self.blocked:
            return None
        return self.files.get(path)

    def move(self, source, destination):
        self.moves.append((source, destination))
        if destination in self.blocked or destination in self.files:
            return 412
        self.files[destination] = self.files.pop(source)
        return 204


class RenameTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = TaskDB(Path(self.tmp.name) / "tasks.db")
        self.db._conn.execute(
            """INSERT INTO channel_bulk_jobs(
                   id, chat_id, inventory_id, state, directory_alias, manifest_hash,
                   controls, whitelist, created_at, updated_at
               ) VALUES(1, -100555, 1, 'paused', 'media', ?, '{}', '["media"]', 0, 0)""",
            ("ab" * 32,),
        )

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def _done(self, message_id, remote, size=100, file_name="1.mp4"):
        task, _created = self.db.create_task(
            chat_id=-100555, message_id=message_id, sender_id=1,
            media_key=f"document:{message_id}", file_name=file_name, file_size=size,
            bulk_job_id=1,
        )
        self.db._conn.execute(
            """UPDATE tasks SET state='completed', remote_path=?, remote_final_path=?, local_path=NULL
               WHERE id=?""",
            (remote, remote, task["id"]),
        )
        self.db._conn.execute(
            """INSERT INTO channel_bulk_items(job_id, message_id, status, task_id)
               VALUES(1, ?, 'done', ?)""",
            (message_id, task["id"]),
        )
        return int(task["id"])

    def test_dry_run_plans_caption_names_and_does_not_touch_dav(self):
        self.assertTrue(_dry_run({}))
        self.assertFalse(_dry_run({"dry_run": False}))
        with self.assertRaises(ValueError):
            _dry_run({"dry_run": "false"})
        first = self._done(10, "library/videos/1 (task-10).mp4")
        second = self._done(11, "library/videos/1 (task-11).mp4")
        captions = {10: "城市夜景，第一集", 11: "城市夜景，第一集"}

        async def fetch(message_id):
            return captions[message_id]

        class Boom:
            def size(self, path):
                raise AssertionError(path)

            def move(self, source, destination):
                raise AssertionError(destination)

        report = asyncio.run(rename_completed(
            self.db, 1, fetch_caption=fetch, dav=Boom(), dry_run=True,
        ))
        self.assertTrue(report["dry_run"])
        by_id = {item["message_id"]: item for item in report["items"]}
        self.assertEqual(by_id[10]["status"], "planned")
        self.assertEqual(by_id[10]["target"], "library/videos/城市夜景，第一集.mp4")
        self.assertEqual(by_id[11]["status"], "planned")
        self.assertEqual(by_id[11]["target"], "library/videos/城市夜景，第一集 (message-11).mp4")
        self.assertEqual(by_id[10]["task_id"], first)
        self.assertEqual(by_id[11]["task_id"], second)
        stored = self.db._conn.execute(
            "SELECT remote_final_path FROM tasks WHERE id=?", (first,)
        ).fetchone()["remote_final_path"]
        self.assertIn("(task-10)", stored)

    def test_real_move_verifies_size_and_is_idempotent(self):
        task_id = self._done(10, "library/videos/1 (task-10).mp4", size=100)
        dav = FakeDav({"library/videos/1 (task-10).mp4": 100})

        async def fetch(_message_id):
            return "城市夜景"

        report = asyncio.run(rename_completed(
            self.db, 1, fetch_caption=fetch, dav=dav, dry_run=False,
        ))
        self.assertEqual(report["items"][0]["status"], "renamed")
        self.assertEqual(report["items"][0]["size"], 100)
        self.assertEqual(dav.files, {"library/videos/城市夜景.mp4": 100})
        self.assertEqual(len(dav.moves), 1)
        saved = self.db._conn.execute(
            "SELECT remote_path, remote_final_path FROM tasks WHERE id=?", (task_id,)
        ).fetchone()
        self.assertEqual(saved["remote_path"], "library/videos/城市夜景.mp4")
        self.assertEqual(saved["remote_final_path"], saved["remote_path"])
        again = asyncio.run(rename_completed(
            self.db, 1, fetch_caption=fetch, dav=dav, dry_run=False,
        ))
        self.assertEqual(again["items"][0]["status"], "already")
        self.assertEqual(len(dav.moves), 1)

    def test_precondition_failure_skips_without_overwrite(self):
        task_id = self._done(10, "library/videos/1 (task-10).mp4", size=100)
        target = "library/videos/城市夜景.mp4"
        dav = FakeDav({"library/videos/1 (task-10).mp4": 100}, blocked={target})

        async def fetch(_message_id):
            return "城市夜景"

        report = asyncio.run(rename_completed(
            self.db, 1, fetch_caption=fetch, dav=dav, dry_run=False,
        ))
        self.assertEqual(report["items"][0]["status"], "conflict")
        self.assertEqual(report["items"][0]["http"], 412)
        self.assertEqual(dav.files, {"library/videos/1 (task-10).mp4": 100})
        saved = self.db._conn.execute(
            "SELECT remote_final_path FROM tasks WHERE id=?", (task_id,)
        ).fetchone()["remote_final_path"]
        self.assertIn("(task-10)", saved)
        self.assertNotIn("secret", str(report))

    def test_reconcile_when_move_already_happened(self):
        task_id = self._done(10, "library/videos/1 (task-10).mp4", size=80)
        dav = FakeDav({"library/videos/城市夜景.mp4": 80})

        async def fetch(_message_id):
            return "城市夜景"

        report = asyncio.run(rename_completed(
            self.db, 1, fetch_caption=fetch, dav=dav, dry_run=False,
        ))
        self.assertEqual(report["items"][0]["status"], "reconciled")
        self.assertEqual(dav.moves, [])
        saved = self.db._conn.execute(
            "SELECT remote_final_path FROM tasks WHERE id=?", (task_id,)
        ).fetchone()["remote_final_path"]
        self.assertEqual(saved, "library/videos/城市夜景.mp4")

    def test_occupied_plain_name_uses_the_message_suffix(self):
        self._done(10, "library/videos/1 (task-10).mp4", size=100)
        dav = FakeDav({
            "library/videos/1 (task-10).mp4": 100,
            "library/videos/城市夜景.mp4": 50,
        })

        async def fetch(_message_id):
            return "城市夜景"

        report = asyncio.run(rename_completed(
            self.db, 1, fetch_caption=fetch, dav=dav, dry_run=False,
        ))
        self.assertEqual(report["items"][0]["status"], "renamed")
        self.assertEqual(report["items"][0]["target"], "library/videos/城市夜景 (message-10).mp4")
        self.assertNotIn("library/videos/1 (task-10).mp4", dav.files)
        self.assertEqual(dav.files["library/videos/城市夜景.mp4"], 50)

    def test_deleted_caption_does_not_move(self):
        self._done(86, "library/videos/6 (task-86).mp4")

        async def fetch(_message_id):
            raise SourceMissing("消息已不存在")

        dav = FakeDav({"library/videos/6 (task-86).mp4": 10})
        report = asyncio.run(rename_completed(
            self.db, 1, fetch_caption=fetch, dav=dav, dry_run=False,
        ))
        self.assertEqual(report["items"][0]["status"], "source_deleted")
        self.assertEqual(report["items"][0]["reason"], "消息已不存在")
        self.assertEqual(dav.moves, [])

    def test_move_header_forbids_overwrite_and_hides_the_secret(self):
        captured = {}

        class Response:
            status = 204

            def read(self):
                return b""

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        def fake_urlopen(request, timeout=60):
            captured["method"] = request.get_method()
            captured["overwrite"] = request.get_header("Overwrite")
            captured["destination"] = request.get_header("Destination")
            captured["authorization"] = request.get_header("Authorization")
            return Response()

        with patch("tg2cloud.channel_bulk.rename_remote.urlopen", fake_urlopen):
            dav = WebDavFiles("http://dav.example/dav", "user", "secret-value")
            self.assertEqual(dav.move("dir/旧 (task-1).mp4", "dir/城市夜景.mp4"), 204)
        self.assertEqual(captured["method"], "MOVE")
        self.assertEqual(captured["overwrite"], "F")
        self.assertIn("%E5", captured["destination"])
        self.assertNotIn("secret-value", captured["destination"])
        self.assertNotIn("secret-value", repr(dav))
        self.assertTrue(captured["authorization"].startswith("Basic "))

        def broken(_request, timeout=60):
            raise URLError("timed out")

        with (
            patch("tg2cloud.channel_bulk.rename_remote.urlopen", broken),
            self.assertRaises(WebDavError) as caught,
        ):
            dav.size("dir/城市夜景.mp4")
        self.assertNotIn("secret-value", str(caught.exception))
        self.assertIsNone(caught.exception.__cause__)

        def missing(_request, timeout=60):
            raise HTTPError("http://dav.example/dav/a", 412, "Precondition Failed", hdrs=None, fp=None)

        with patch("tg2cloud.channel_bulk.rename_remote.urlopen", missing):
            self.assertEqual(dav.move("dir/a.mp4", "dir/b.mp4"), 412)


if __name__ == "__main__":
    unittest.main()
