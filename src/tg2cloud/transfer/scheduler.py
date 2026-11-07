from __future__ import annotations

import asyncio
import shutil
import time
from collections import deque

from tg2cloud.domain.states import SCHEDULING_STATES


def _sample_fresh(self) -> bool:
    return bool(self.snapshot and 0 <= time.time() - self.snapshot.sampled_at
                <= max(10, self.settings.control_interval * 3))


def _destination_ready(self) -> bool:
    return (self.destination_healthy and 0 <= time.time() - self.destination_last_checked
            <= self.settings.remote_health_interval + 45)


def _stream_count(self) -> int:
    return sum(1 for task_id in self.download_tasks
               if (self.db.get(task_id) or {}).get("transfer_mode") == "stream")


def _errors_in_last_minute(self, errors: deque[float]) -> int:
    cutoff = time.time() - 60
    while errors and errors[0] < cutoff:
        errors.popleft()
    return len(errors)


async def _enforce_disk_emergency(self) -> None:
    if (
        not self.snapshot
        or self.snapshot.disk_free > self.settings.min_free_disk_bytes
        or not self.download_tasks
    ):
        return
    active = list(self.download_tasks.items())
    self.log.error(
        "磁盘已触及安全线，暂停 %s 个下载并清理不完整文件",
        len(active),
    )
    for _, task in active:
        task.cancel()
    await asyncio.gather(
        *(task for _, task in active),
        return_exceptions=True,
    )
    requeued = 0
    for task_id, _ in active:
        current = self.db.get(task_id)
        if current and current["state"] == "queued":
            self.db.update(
                task_id,
                downloaded_bytes=0,
                wait_reason="磁盘触及安全线，已自动暂停并重新排队",
            )
            requeued += 1
    if requeued:
        await self._notify(
            f"⚠️ 磁盘触及安全线，已暂停 {requeued} 个下载并重新排队；"
            "空间恢复后会自动继续。"
        )


async def _resource_loop(self) -> None:
    while not self._stop.is_set():
        try:
            self.snapshot = self.monitor.sample()
            await self._enforce_disk_emergency()
            counts = self.db.counts(states=SCHEDULING_STATES)
            used_local_bytes = self.db.used_local_bytes()
            streams = self._stream_count()
            download_demand = bool(
                self.download_tasks
                or counts.get("queued")
                or counts.get("reserved")
                or counts.get("downloading")
                or counts.get("streaming")
            )
            upload_demand = bool(
                self.upload_tasks
                or counts.get("downloaded")
                or counts.get("waiting_upload")
                or counts.get("cleanup_pending")
                or counts.get("uploading")
                or counts.get("verifying")
                or counts.get("finalizing")
                or streams
            )
            self.download_window.update(
                self.snapshot,
                recent_errors=self._errors_in_last_minute(
                    self.recent_download_errors
                ),
                destination_healthy=self._destination_ready(),
                demand_present=download_demand,
                throughput=self._stage_rate("download") + self._stage_rate("stream"),
                active_count=len(self.download_tasks),
                backlog_pressure=used_local_bytes >= self.settings.local_budget_bytes * 0.8,
            )
            self.upload_window.update(
                self.snapshot,
                recent_errors=self._errors_in_last_minute(
                    self.recent_upload_errors
                ),
                destination_healthy=self._destination_ready(),
                demand_present=upload_demand,
                throughput=self._stage_rate("upload") + self._stage_rate("stream"),
                active_count=len(self.upload_tasks) + streams,
            )
            (self.settings.data_dir / "resource-heartbeat").touch()
            try:
                from tg2cloud.monitoring.readiness import publish
                publish(self, counts, used_local_bytes=used_local_bytes)
            except Exception:  # noqa: BLE001 - optional diagnostics must not change scheduling decisions
                # A diagnostic export must never change scheduling decisions.
                if not getattr(self, "_readiness_warning", False):
                    self.log.warning("业务就绪观察记录暂不可用；不影响原调度逻辑")
                self._readiness_warning = True
            else:
                self._readiness_warning = False
        except Exception:
            self.snapshot = None
            self.log.exception("资源控制循环异常")
        await asyncio.sleep(self.settings.control_interval)


async def _scheduler_loop(self) -> None:
    while not self._stop.is_set():
        try:
            # Drain retained files first; streaming also consumes upload slots.
            await self._start_uploads()
            await self._start_downloads()
            (self.settings.data_dir / "heartbeat").touch()
        except Exception:
            self.log.exception("调度循环异常")
        await asyncio.sleep(1)


def _queued_batches(self, states: tuple[str, ...]):
    after_id = 0
    while True:
        batch = self.db.list_states(
            states,
            limit=200,
            ready_only=True,
            after_id=after_id,
        )
        if not batch:
            return
        # A bounded listener backlog must not jump ahead of manual transfers
        # in the same existing keyset page. Preserve the original cursor.
        after_id = int(batch[-1]["id"])
        yield sorted(batch, key=lambda task: (task.get("read_identity", "bot") != "bot", task["id"]))


async def _start_downloads(self) -> None:
    if not self.destination_healthy or not self._sample_fresh() or not self._destination_ready():
        return
    if self.db.is_paused():
        return
    if self.snapshot.filesystems and any(free <= self.settings.min_free_disk_bytes
                                        for _, free in self.snapshot.filesystems):
        self._resource_pause_reason = "暂存/数据库/日志/配置所在文件系统触及安全线；暂停新下载"
        return
    self._resource_pause_reason = ""
    available_slots = self.download_window.value - len(self.download_tasks)
    if available_slots <= 0:
        return
    for batch in self._queued_batches(("queued",)):
        await asyncio.sleep(0)
        if not self._sample_fresh() or not self._destination_ready() or self.db.is_paused():
            return
        for task in batch:
            if available_slots <= 0:
                return
            task_id = int(task["id"])
            if task_id in self.download_tasks:
                continue
            if task.get("bulk_job_id"):
                continue
            if task.get("read_identity", "bot") != "bot":
                from tg2cloud.listening.account import state as listener_state
                listener = getattr(self, "listener", None)
                account_state = listener_state(self.db)
                if (listener is None or listener.client is None or not account_state["enabled"]
                        or account_state.get("cooldown_until", 0) > time.time()):
                    self.db.set_wait_reason(task_id, "等待监听账号授权、连接或冷却结束；手动任务继续调度", states=("queued",))
                    continue
            if ((task.get("transfer_mode") == "stream"
                 or task["file_size"] > self.settings.local_budget_bytes)
                    and len(self.upload_tasks) + self._stream_count() >= self.upload_window.value):
                self.db.set_wait_reason(task_id, "流式任务等待共享上传窗口", states=("queued",))
                continue
            reserved, reason = self.db.reserve(
                task_id,
                budget_bytes=self.settings.local_budget_bytes,
                current_free_bytes=min(self.snapshot.disk_free, shutil.disk_usage(self.settings.download_dir).free),
                minimum_free_bytes=self.settings.min_free_disk_bytes,
            )
            if not reserved:
                self.log.debug("任务 #%s 暂不放行：%s", task_id, reason)
                continue
            async_task = asyncio.create_task(
                self._download_one(task_id), name=f"download-{task_id}"
            )
            self.download_tasks[task_id] = async_task
            async_task.add_done_callback(
                lambda _, tid=task_id: self.download_tasks.pop(tid, None)
            )
            async_task.add_done_callback(lambda _, tid=task_id: self._progress.pop(tid, None))
            available_slots -= 1


async def _start_uploads(self) -> None:
    if not self.destination_healthy or not self._sample_fresh() or not self._destination_ready():
        return
    available_slots = self.upload_window.value - len(self.upload_tasks) - self._stream_count()
    if available_slots <= 0:
        return
    states = ("cleanup_pending",) if self.db.is_paused() else ("cleanup_pending", "waiting_upload", "downloaded")
    for batch in self._queued_batches(states):
        await asyncio.sleep(0)
        if not self._sample_fresh() or not self._destination_ready():
            return
        for task in batch:
            if available_slots <= 0:
                return
            task_id = int(task["id"])
            if task.get("bulk_job_id"):
                continue
            current = self.db.get(task_id)
            if not current or current.get("cancel_requested") or current["state"] not in states:
                continue
            if self.db.is_paused() and task["state"] != "cleanup_pending":
                continue
            if task_id in self.upload_tasks:
                continue
            admit = getattr(self.db, "admit_upload", None)
            if admit and not admit(task_id):
                self.db.set_wait_reason(task_id, "等待个人并发额度", states=("waiting_upload", "downloaded"))
                continue
            async_task = asyncio.create_task(
                self._upload_one(task_id), name=f"upload-{task_id}"
            )
            self.upload_tasks[task_id] = async_task
            async_task.add_done_callback(
                lambda _, tid=task_id: self.upload_tasks.pop(tid, None)
            )
            async_task.add_done_callback(lambda _, tid=task_id: self._progress.pop(tid, None))
            available_slots -= 1
