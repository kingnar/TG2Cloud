"""Channel inventory and paused bulk transfer. No network and no real accounts."""
from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tg2cloud.channel_bulk import inventory, jobs
from tg2cloud.channel_bulk.control import handle
from tg2cloud.channel_bulk.dedup import classify
from tg2cloud.channel_bulk.fingerprint import sample_sha256
from tg2cloud.channel_bulk.ledger import ByteRate, attest, charge, totals
from tg2cloud.channel_bulk.runtime import download_bounded
from tg2cloud.channel_bulk.transfer import run_job
from tg2cloud.domain.actors import user_scope
from tg2cloud.listening import account
from tg2cloud.listening.service import Listener
from tg2cloud.persistence.sqlite import TaskDB
from tg2cloud.telegram.commands import CommandHandlers
from tg2cloud.telegram.views import TaskViews


class DocumentAttributeVideo:
    def __init__(self, duration):
        self.duration = duration


class FloodWaitError(Exception):
    def __init__(self, seconds):
        self.seconds = seconds


def video(message_id, *, duration=None, size=1000, group="", unique=None):
    attributes = [] if duration is None else [DocumentAttributeVideo(duration)]
    return SimpleNamespace(
        id=message_id,
        chat_id=-100555,
        grouped_id=group or None,
        video=SimpleNamespace(duration=duration, size=size, mime_type="video/mp4") if duration is not None else None,
        document=SimpleNamespace(id=message_id, size=size, mime_type="video/mp4", attributes=attributes),
        file=SimpleNamespace(name=f"clip-{message_id}.mp4", ext=".mp4", size=size, mime_type="video/mp4"),
        media=object(),
        raw_text="",
        message="",
        noforwards=False,
        file_unique_id=unique,
    )


class Clock:
    def __init__(self):
        self.wall = 1_800_000_000.0
        self.mono = 0.0
        self.sleeps = []

    def now(self):
        return self.wall

    def monotonic(self):
        return self.mono

    async def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.mono += seconds
        self.wall += seconds


class Transport:
    def __init__(self, *, cache_only=False, fail=None, evidence=False):
        self.cache_only = cache_only
        self.fail = fail
        self.evidence = evidence
        self.downloads = 0
        self.uploads = 0

    async def download(self, *, item, path, rate, size):
        await rate.consume(size)
        self.downloads += 1
        if self.fail == "download":
            raise self.fail_exc
        path.write_bytes(b"x" * size)

    async def upload(self, *, item, path, remote, rate, size):
        await rate.consume(size)
        self.uploads += 1
        if self.fail == "upload":
            raise self.fail_exc

    async def probe(self, *, item, remote, size, fingerprint=None):
        del item
        if not self.evidence:
            return {"size": size, "cache_only": self.cache_only, "scheme": "webdav", "method": "size"}
        return {
            "scheme": "clouddrive2-upload-task",
            "status": "Finish",
            "size": size,
            "transferred_bytes": size,
            "dest_path": remote,
            "source_fingerprint": fingerprint,
            "destination_id": "cloud-file",
            "read_back": True,
            "cache_only": False,
        }


class Ctx:
    def __init__(self, db, job_id, directory, clock, transport, folder, account_id):
        self.db = db
        self.job_id = job_id
        self.admin = 1
        self.account = account_id
        self.directory = directory
        self.download_dir = folder
        self.transport = transport
        self.min_free = 0
        self.free = 10**12
        self.busy = False
        self.notes = []
        self.clock = clock

    def now(self):
        return self.clock.now()

    def monotonic(self):
        return self.clock.monotonic()

    async def sleep(self, seconds):
        await self.clock.sleep(seconds)

    def disk_free(self):
        return self.free

    def listener_busy(self):
        return self.busy

    def notify(self, text):
        self.notes.append(text)


class ChannelBulkTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.db = TaskDB(self.root / "tasks.db")
        self.clock = Clock()
        account.write_state(self.db, account=42, enabled=True, status="测试")
        self.db._conn.execute(
            "INSERT INTO transfer_directories(alias, path, owners) VALUES('media','library/videos','[1]')"
        )

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def test_inventory_checkpoint_flood_and_export(self):
        pages = {
            0: [video(50, duration=4, group="=album"), video(40, duration=80, size=5000)],
            40: [video(30, duration=None, size=200)],
        }

        async def fetch(offset, limit):
            self.assertLessEqual(limit, 50)
            return pages.get(offset, [])

        row = inventory.start(self.db, -100555, page_size=20, delay_seconds=0.5, now=self.clock.now())
        self.assertEqual(row["status"], "paused")

        async def boom(offset, limit):
            raise FloodWaitError(17)

        outcome = asyncio.run(inventory.step(self.db, row["id"], boom, now=self.clock.now, sleep=self.clock.sleep))
        self.assertEqual(outcome, "flood")
        saved = inventory.load(self.db, row["id"])
        self.assertEqual(saved["checkpoint_id"], 0)
        self.assertGreaterEqual(saved["cooldown_until"], self.clock.now() + 17)
        self.assertEqual(account.state(self.db)["account"], 42)
        self.db._conn.execute("UPDATE channel_inventory SET cooldown_until=0 WHERE id=?", (row["id"],))
        outcome = asyncio.run(inventory.step(
            self.db, row["id"], fetch, pages=2, now=self.clock.now, sleep=self.clock.sleep
        ))
        self.assertEqual(outcome, "paged")
        self.assertEqual(inventory.load(self.db, row["id"])["checkpoint_id"], 30)
        self.assertGreaterEqual(self.clock.sleeps[0], 0.5)
        asyncio.run(inventory.step(self.db, row["id"], fetch, now=self.clock.now, sleep=self.clock.sleep))
        self.assertEqual(inventory.load(self.db, row["id"])["status"], "complete")
        summary_text = inventory.status_text(self.db, row["id"])
        self.assertIn("<=5s：1", summary_text)
        self.assertIn("<=30s：1", summary_text)
        self.assertIn("未知时长：1", summary_text)
        exported = inventory.export_text(self.db, row["id"], "csv")
        self.assertIn("message_id", exported)
        self.assertIn("https://t.me/c/555/40", exported)
        self.assertIn("'=album", exported)
        payload = inventory.export_text(self.db, row["id"], "json")
        self.assertIn('"le_10"', payload)
        inventory.exclude(self.db, -100555, 40, now=self.clock.now())
        self.assertIn(40, inventory.exclusions(self.db, -100555))

    def test_short_unknown_are_review_only_and_dedup_skips_any_directory(self):
        done, _ = self.db.create_task(
            chat_id=-100555, message_id=40, sender_id=1, media_key="document:40",
            file_name="old.mp4", file_size=5000, directory_override="other/shelf",
        )
        self.db._conn.execute(
            "UPDATE tasks SET state='completed', target_directory='other/shelf' WHERE id=?", (done["id"],)
        )
        active, _ = self.db.create_task(
            chat_id=9, message_id=9, sender_id=1, media_key="document:12",
            file_name="busy.mp4", file_size=10,
        )
        self.db._conn.execute("UPDATE tasks SET state='downloading' WHERE id=?", (active["id"],))
        blocked, _ = self.db.create_task(
            chat_id=-100555, message_id=15, sender_id=1, file_name="stop.mp4", file_size=10,
            file_unique_id="uid-15",
        )
        self.db._conn.execute("UPDATE tasks SET state='cancelled' WHERE id=?", (blocked["id"],))
        self.assertEqual(classify(self.db, chat_id=-100555, message_id=40, media_key="document:40")[0], "completed")
        self.assertEqual(blocked["target_directory"] != "other/shelf", True)
        row = inventory.start(self.db, -100555, now=self.clock.now())

        async def fetch(offset, limit):
            if offset:
                return []
            return [
                video(40, duration=80, size=5000),
                video(77, duration=80, size=1000),
                video(12, duration=80, size=1000),
                video(4, duration=4, size=100),
                video(8, duration=None, size=100),
                video(15, duration=80, size=100, unique="uid-15"),
            ]

        asyncio.run(inventory.step(self.db, row["id"], fetch, now=self.clock.now, sleep=self.clock.sleep))
        # The public facts helper does not copy a custom attribute; stamp the id used above.
        self.db._conn.execute(
            "UPDATE channel_inventory_items SET file_unique_id='uid-15' WHERE message_id=15"
        )
        job = jobs.open_job(self.db, row["id"], "media", now=self.clock.now())
        self.assertEqual(job["state"], "preview")
        built = jobs.manifest(self.db, job["id"], now=self.clock.now())
        self.assertEqual(built["ids"], [77])
        jobs.update_controls(self.db, job["id"], now=self.clock.now(), allow=[4])
        built = jobs.manifest(self.db, job["id"], now=self.clock.now())
        self.assertEqual(built["ids"], [4, 77])
        kind, task = classify(self.db, chat_id=-100555, message_id=15, file_unique_id="uid-15")
        self.assertEqual(kind, "blocked")
        self.assertEqual(task["state"], "cancelled")

    def _arm(self, job_id, *, daily=10**9, monthly=10**9):
        jobs.update_controls(
            self.db, job_id, now=self.clock.now(),
            daily_bytes=daily, monthly_bytes=monthly, down_bps=10**9, up_bps=10**9,
            egress_ack=True, max_attempts=2,
        )
        attest(self.db, remaining_bytes=10**9, period_end=self.clock.now() + 3600, now=self.clock.now())
        built = jobs.manifest(self.db, job_id, now=self.clock.now())
        decision = jobs.prepare_resume(
            self.db, job_id, built["hash"], now=self.clock.now(), disk_free=10**12, min_free=0,
            directories={"media": "library/videos"},
        )
        self.assertFalse(decision["idempotent"])
        return built

    def _job_with(self, messages):
        row = inventory.start(self.db, -100555, now=self.clock.now())

        async def fetch(offset, limit):
            return [] if offset else messages

        asyncio.run(inventory.step(self.db, row["id"], fetch, now=self.clock.now, sleep=self.clock.sleep))
        job = jobs.open_job(self.db, row["id"], "media", now=self.clock.now())
        return job

    def test_resume_defaults_paused_until_gates_and_remote_review_keeps_local(self):
        job = self._job_with([video(70, duration=90, size=100)])
        self.assertEqual(job["state"], "preview")
        with self.assertRaises(jobs.Refusal):
            jobs.prepare_resume(
                self.db, job["id"], "a" * 64, now=self.clock.now(), disk_free=10**12, min_free=0,
                directories={"media": "library/videos"},
            )
        self.assertEqual(jobs.get_job(self.db, job["id"])["state"], "preview")
        self.db._conn.execute("UPDATE channel_bulk_jobs SET state='running' WHERE id=?", (job["id"],))
        held = jobs.hold_on_startup(self.db, now=self.clock.now())
        self.assertEqual(held["jobs"], 1)
        self.assertEqual(jobs.get_job(self.db, job["id"])["state"], "paused")
        self._arm(job["id"])
        transport = Transport(cache_only=None)
        ctx = Ctx(self.db, job["id"], "library/videos", self.clock, transport, self.root, 42)
        outcome = asyncio.run(run_job(ctx))
        self.assertEqual(outcome, "remote_review")
        task = self.db._conn.execute("SELECT * FROM tasks").fetchone()
        self.assertNotEqual(task["state"], "completed")
        self.assertTrue(Path(task["local_path"]).is_file())
        self.assertIn("任务 #", ctx.notes[0])
        transport.cache_only = True
        jobs.pause(self.db, job["id"], now=self.clock.now(), error=None)
        # The held item is not pending, so a new file is required for the accept path.
        self.assertEqual(jobs.get_job(self.db, job["id"])["state"], "paused")

    def test_webdav_size_is_not_completion_and_read_back_is(self):
        job = self._job_with([video(71, duration=90, size=32)])
        self._arm(job["id"])
        ctx = Ctx(self.db, job["id"], "library/videos", self.clock, Transport(cache_only=False), self.root, 42)
        self.assertEqual(asyncio.run(run_job(ctx)), "remote_review")
        task = self.db._conn.execute("SELECT * FROM tasks").fetchone()
        self.assertNotEqual(task["state"], "completed")
        self.assertTrue(Path(task["local_path"]).is_file())
        self.assertIsNone(task["verification_method"])
        proven = self._job_with([video(72, duration=90, size=32)])
        self._arm(proven["id"])
        ctx = Ctx(
            self.db, proven["id"], "library/videos", self.clock,
            Transport(evidence=True), self.root, 42,
        )
        self.assertEqual(asyncio.run(run_job(ctx)), "idle")
        done = self.db._conn.execute("SELECT * FROM tasks WHERE message_id=72").fetchone()
        self.assertEqual(done["state"], "completed")
        self.assertIsNone(done["local_path"])
        self.assertEqual(done["verification_method"], "remote_review")

    def test_budget_retry_rate_cooldown_disk_and_active_dedup(self):
        self.assertTrue(charge(self.db, 600, now=self.clock.now(), daily_cap=1000, monthly_cap=1000))
        self.assertFalse(charge(self.db, 500, now=self.clock.now(), daily_cap=1000, monthly_cap=1000))
        self.assertEqual(totals(self.db, self.clock.now())["daily"], 600)
        self.assertTrue(charge(self.db, 100, now=self.clock.now(), daily_cap=1000, monthly_cap=1000))
        self.assertEqual(totals(self.db, self.clock.now())["daily"], 700)
        rate = ByteRate(100, monotonic=self.clock.monotonic, sleep=self.clock.sleep)
        asyncio.run(rate.consume(1000))
        self.assertAlmostEqual(self.clock.sleeps[-1], 10.0)

        job = self._job_with([video(4, duration=90, size=100)])
        self._arm(job["id"], daily=10**6, monthly=10**6)
        self.db.create_task(
            chat_id=8, message_id=8, sender_id=1, media_key="document:4",
            file_name="live.mp4", file_size=100,
        )
        self.db._conn.execute("UPDATE tasks SET state='uploading' WHERE media_key='document:4'")
        transport = Transport(cache_only=False)
        ctx = Ctx(self.db, job["id"], "library/videos", self.clock, transport, self.root, 42)
        self.assertEqual(asyncio.run(run_job(ctx)), "idle")
        self.assertEqual(transport.downloads, 0)
        skipped = self.db._conn.execute(
            "SELECT status, last_error FROM channel_bulk_items WHERE message_id=4"
        ).fetchone()
        self.assertEqual(skipped["status"], "skipped")
        self.assertIn("活动", skipped["last_error"])

        self.db._conn.execute("DELETE FROM channel_byte_ledger")
        flood = self._job_with([video(90, duration=90, size=1000)])
        jobs.update_controls(
            self.db, flood["id"], now=self.clock.now(), daily_bytes=10**7, monthly_bytes=10**7,
            down_bps=10**9, up_bps=10**9, egress_ack=True, max_attempts=2,
        )
        attest(self.db, remaining_bytes=10**7, period_end=self.clock.now() + 86400, now=self.clock.now())
        built = jobs.manifest(self.db, flood["id"], now=self.clock.now())
        jobs.prepare_resume(
            self.db, flood["id"], built["hash"], now=self.clock.now(), disk_free=10**12, min_free=0,
            directories={"media": "library/videos"},
        )
        failing = Transport(cache_only=False, fail="download", evidence=True)
        failing.fail_exc = RuntimeError("HTTP 429 retry-after: 12")
        ctx = Ctx(self.db, flood["id"], "library/videos", self.clock, failing, self.root, 42)
        self.assertEqual(asyncio.run(run_job(ctx)), "cooldown")
        self.assertGreaterEqual(jobs.get_job(self.db, flood["id"])["cooldown_until"], self.clock.now() + 12)
        self.assertEqual(totals(self.db, self.clock.now())["daily"], 1000)
        self.db._conn.execute("UPDATE channel_bulk_jobs SET cooldown_until=0 WHERE id=?", (flood["id"],))
        failing.fail = None
        jobs.prepare_resume(
            self.db, flood["id"], built["hash"], now=self.clock.now(), disk_free=10**12, min_free=0,
            directories={"media": "library/videos"},
        )
        self.assertEqual(asyncio.run(run_job(ctx)), "idle")
        self.assertEqual(totals(self.db, self.clock.now())["daily"], 3000)

        disk_job = self._job_with([video(91, duration=90, size=500)])
        self._arm(disk_job["id"])
        ctx = Ctx(self.db, disk_job["id"], "library/videos", self.clock, Transport(), self.root, 42)
        ctx.free = 499
        ctx.min_free = 0
        self.assertEqual(asyncio.run(run_job(ctx)), "disk")
        self.assertEqual(ctx.transport.downloads, 0)

    def test_enabled_source_is_not_disabled_and_scheduler_skips_bulk(self):
        from tg2cloud.persistence.plans import Plans

        plans = Plans(self.db, 1)
        source = plans.save_source(
            1, account=42, chat=-100555, title="kept", cursor=0,
            config={"confirmation": True, "rules": {"all_attachments": True}},
        )
        self.db._conn.execute("UPDATE listener_sources SET enabled=1 WHERE id=?", (source["id"],))
        job = self._job_with([video(80, duration=90, size=16)])
        self._arm(job["id"])
        ctx = Ctx(self.db, job["id"], "library/videos", self.clock, Transport(cache_only=False), self.root, 42)
        asyncio.run(run_job(ctx))
        enabled = self.db._conn.execute("SELECT enabled FROM listener_sources WHERE id=?", (source["id"],)).fetchone()
        self.assertEqual(enabled["enabled"], 1)

        from tg2cloud.transfer.scheduler import _queued_batches, _start_downloads

        bulk, _ = self.db.create_task(
            chat_id=1, message_id=100, sender_id=1, file_name="bulk.mp4", file_size=10, bulk_job_id=4,
        )
        manual, _ = self.db.create_task(
            chat_id=1, message_id=101, sender_id=1, file_name="manual.mp4", file_size=10,
        )
        owner = SimpleNamespace(
            db=self.db,
            destination_healthy=True,
            snapshot=SimpleNamespace(filesystems=[], disk_free=10**12),
            download_tasks={},
            download_window=SimpleNamespace(value=2),
            settings=SimpleNamespace(min_free_disk_bytes=1, local_budget_bytes=10**12, download_dir=self.root),
            _download_one=AsyncMock(),
            log=SimpleNamespace(debug=lambda *args, **kwargs: None),
            _sample_fresh=lambda: True,
            _destination_ready=lambda: True,
        )
        owner._queued_batches = lambda states: _queued_batches(owner, states)
        with patch("tg2cloud.transfer.scheduler.shutil.disk_usage", return_value=SimpleNamespace(free=10**12)):
            asyncio.run(_start_downloads(owner))
        owner._download_one.assert_awaited_once_with(manual["id"])
        self.assertEqual(self.db.get(bulk["id"])["state"], "queued")

    def test_same_listener_client_and_commands_stay_admin_only(self):
        owner = SimpleNamespace(
            settings=SimpleNamespace(allowed_user_id=1, data_dir=self.root, download_dir=self.root, min_free_disk_bytes=1),
            db=self.db,
            log=SimpleNamespace(warning=lambda *args, **kwargs: None),
            _stop=SimpleNamespace(is_set=lambda: False),
        )
        listener = Listener(owner)
        listener.client = SimpleNamespace(is_connected=lambda: True)
        seen = []

        async def operation(client):
            seen.append(client)
            return "same"

        with patch("tg2cloud.listening.account.client", side_effect=AssertionError("new client")):
            self.assertEqual(asyncio.run(listener.with_client(operation)), "same")
        self.assertIs(seen[0], listener.client)

        class Client:
            def __init__(self):
                self.limits = []

            def iter_download(self, document, **kwargs):
                self.limits.append(kwargs.get("limit"))

                async def chunks():
                    yield b"abcd"

                return chunks()

        client = Client()
        message = SimpleNamespace(document=SimpleNamespace(size=4))
        path = self.root / "one.bin"

        async def paced():
            rate = ByteRate(10**9, monotonic=self.clock.monotonic, sleep=self.clock.sleep)
            await download_bounded(client, message, path, rate, 4)

        asyncio.run(paced())
        self.assertEqual(client.limits, [1])
        self.assertEqual(path.read_bytes(), b"abcd")
        package = Path("src/tg2cloud/channel_bulk")
        text = "\n".join(path.read_text() for path in package.glob("*.py"))
        self.assertNotIn("TelegramClient", text)
        self.assertNotIn("delete_messages", text)
        self.assertNotRegex(text, r"remaining_bytes\s*=\s*10")

        admin_help = TaskViews._format_help(admin=True)
        member_help = TaskViews._format_help(admin=False)
        self.assertIn("调度控制：/pause、/resume", admin_help)
        self.assertIn("/inventory", admin_help)
        self.assertNotIn("/bulk", member_help)
        self.assertIn("TG2Cloud 使用帮助", member_help)
        event = SimpleNamespace(reply=AsyncMock())
        with user_scope(2), patch("tg2cloud.telegram.planning.command", new=AsyncMock(return_value=False)):
            asyncio.run(CommandHandlers._handle_command(owner, event, "/inventory start -100555"))
        self.assertIn("仅限管理员", event.reply.await_args.args[0])
        with user_scope(1), patch("tg2cloud.telegram.planning.command", new=AsyncMock(return_value=False)):
            asyncio.run(CommandHandlers._handle_command(owner, event, "/inventory start -100555;rm"))
        self.assertIn("未执行", event.reply.await_args.args[0])
        digest = sample_sha256(path)
        self.assertEqual(len(digest), 64)

    def test_resume_race_is_single_winner(self):
        from concurrent.futures import ThreadPoolExecutor

        job = self._job_with([video(60, duration=90, size=10)])
        jobs.update_controls(
            self.db, job["id"], now=self.clock.now(), daily_bytes=100, monthly_bytes=100,
            down_bps=100, up_bps=100, egress_ack=True,
        )
        attest(self.db, remaining_bytes=100, period_end=self.clock.now() + 100, now=self.clock.now())
        built = jobs.manifest(self.db, job["id"], now=self.clock.now())

        def attempt():
            try:
                return jobs.prepare_resume(
                    self.db, job["id"], built["hash"], now=self.clock.now(), disk_free=1000, min_free=0,
                    directories={"media": "library/videos"},
                )
            except jobs.Refusal as exc:
                return exc

        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: attempt(), range(4)))
        winners = [item for item in results if isinstance(item, dict) and not item.get("idempotent")]
        self.assertEqual(len(winners), 1)
        self.assertEqual(jobs.get_job(self.db, job["id"])["state"], "running")

    def test_low_attestation_blocks_without_assuming_provider_quota(self):
        job = self._job_with([video(61, duration=90, size=500)])
        jobs.update_controls(
            self.db, job["id"], now=self.clock.now(), daily_bytes=10**6, monthly_bytes=10**6,
            down_bps=1000, up_bps=1000, egress_ack=True,
        )
        attest(self.db, remaining_bytes=100, period_end=self.clock.now() + 100, now=self.clock.now())
        built = jobs.manifest(self.db, job["id"], now=self.clock.now())
        jobs.prepare_resume(
            self.db, job["id"], built["hash"], now=self.clock.now(), disk_free=10**9, min_free=0,
            directories={"media": "library/videos"},
        )
        ctx = Ctx(self.db, job["id"], "library/videos", self.clock, Transport(cache_only=False), self.root, 42)
        self.assertEqual(asyncio.run(run_job(ctx)), "attestation")
        self.assertEqual(ctx.transport.downloads, 0)
        reply = asyncio.run(handle(SimpleNamespace(
            db=self.db,
            settings=SimpleNamespace(allowed_user_id=1, data_dir=self.root, download_dir=self.root, min_free_disk_bytes=1),
        ), ["/bulk", "status", str(job["id"])]))
        self.assertIn(str(job["id"]), reply)


if __name__ == "__main__":
    unittest.main()
