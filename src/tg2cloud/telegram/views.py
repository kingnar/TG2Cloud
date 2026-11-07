from __future__ import annotations

import time
import unicodedata
from typing import Any

from telethon import Button

from tg2cloud.domain.errors import (
    needs_review,
    safe_failure,
)
from tg2cloud.domain.states import FAILED_STATES, STATE_LABELS
from tg2cloud.monitoring.progress import dashboard_rates, sample_rate

ORPHAN_CLEANUP_LIMIT = 100
ORPHAN_CLEANUP_TTL_SECONDS = 5 * 60
TASK_ACTION_TTL_SECONDS = 5 * 60
QUEUE_PAGE_SIZE = 5
MAX_QUEUE_PAGE = 10_000
TERMINAL_TASK_STATES = {"completed", "confirmed", "cancelled"}
BOT_MENU_COMMANDS = (
    ("status", "查看系统状态"),
    ("queue", "查看最近任务"),
    ("pause", "暂停任务调度"),
    ("resume", "恢复任务调度"),
    ("doctor", "运行系统诊断"),
    ("orphans", "检查临时文件"),
    ("help", "查看使用帮助"),
)

def state_label(state: str, destination_label: str = "CloudDrive2") -> str:
    # Keep the persisted legacy state distinct for recovery compatibility, but
    # do not expose the retired manual-confirmation workflow in normal Bot UI.
    if state == "confirmed":
        state = "completed"
    return STATE_LABELS.get(state, state).format(destination=destination_label)

def format_status_counts(
    counts: dict[str, int], destination_label: str = "CloudDrive2"
) -> str:
    groups = (
        ("排队", ("queued", "reserved")),
        ("下载中", ("downloading",)),
        ("待上传", ("downloaded", "waiting_upload")),
        (
            "上传中",
            ("uploading", "streaming", "verifying", "finalizing", "cleanup_pending"),
        ),
        ("Bot 完成", ("completed", "confirmed")),
        ("失败", tuple(FAILED_STATES)),
        ("已取消", ("cancelled",)),
    )
    parts: list[str] = []
    known: set[str] = set()
    for label, states in groups:
        known.update(states)
        total = sum(counts.get(state, 0) for state in states)
        if total:
            parts.append(f"{label} {total}")
    parts.extend(
        f"{state_label(state, destination_label)} {count}"
        for state, count in sorted(counts.items())
        if state not in known and count
    )
    return "，".join(parts) or "无任务"

def format_bytes(value: float) -> str:
    size = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.2f}{unit}"
        size /= 1024
    return f"{size:.2f}TB"

def format_rate(value: float | None) -> str:
    return "--" if value is None else "0B/s" if value <= 0 else f"{format_bytes(value)}/s"

def format_dashboard_bytes(value: float | None) -> str:
    if value is None:
        return "--"
    size = max(0.0, float(value))
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"

def format_dashboard_rate(value: float | None) -> str:
    return "--" if value is None else f"{format_dashboard_bytes(value)}/s"

def truncate_display(text: str, max_width: int = 32) -> str:
    """Truncate a Telegram label by approximate rendered character width."""
    if max_width < 2:
        raise ValueError("max_width 必须至少为 2")
    current = 0
    result: list[str] = []
    for index, char in enumerate(text):
        width = 0 if unicodedata.combining(char) else (
            2 if unicodedata.east_asian_width(char) in {"W", "F"} else 1
        )
        if current + width > max_width or (
            index < len(text) - 1 and current + width >= max_width
        ):
            return "".join(result) + "…"
        result.append(char)
        current += width
    return "".join(result)

class TaskViews:
    @staticmethod
    def _format_help(*, admin: bool = True) -> str:
        text = (
            "TG2Cloud 使用帮助\n\n"
            "提交文件：直接发送或转发视频、文档等文件\n"
            "状态查看：/status、/queue [页码]\n"
            "任务查看：/task <编号>\n"
            "个人配额：/quota\n"
            "进度跟踪：/watch <编号>\n"
            "失败重试：/retry <编号|all>\n"
            "重新转存：/restart <编号>（已取消任务，需确认）\n"
            "冲突恢复：/recover <编号>（保留原文件，换名重传，需确认）\n"
            "任务搜索：/search <文件名>；/tasks <状态> [最后编号]\n"
            "取消任务：/cancel <编号>\n"
            "流式传输：/stream <编号>\n"
            "调度控制：/pause、/resume\n"
            "运行诊断：/doctor\n"
            "临时巡检：/orphans\n"
            "快捷菜单：使用输入框左侧原生命令菜单"
        )
        if not admin:
            text = "\n".join(line for line in text.splitlines() if not line.startswith(("调度控制", "运行诊断", "临时巡检")))
            text += "\n家庭成员：仅管理自己的任务，文件保存到共同目录。"
        else:
            text += (
                "\n频道盘点：/inventory start <聊天ID>（只读元数据，不下载、不自动转存）\n"
                "批量转存：/bulk（默认预览；显式 resume 前不会开始）\n"
            )
        return text

    @staticmethod
    def _home_buttons(*, admin: bool = True) -> list[list[Any]]:
        return [
            [
                Button.inline("📋", data=b"queue:1"),
                *([Button.inline("🖥", data=b"vps_resources")] if admin else []),
                Button.inline("🔄", data=b"refresh:home"),
            ],
            [Button.inline("转存设置", data=b"menu:transfer"),
             *([Button.inline("监听管理", data=b"menu:listen")] if admin else [])],
        ]

    @staticmethod
    def _vps_resource_buttons() -> list[list[Any]]:
        return [
            [
                Button.inline("🔄", data=b"refresh:vps_resources"),
                Button.inline("↩️", data=b"menu:home"),
            ]
        ]

    @staticmethod
    def _secondary_buttons(refresh_data: bytes) -> list[list[Any]]:
        return [
            [
                Button.inline("刷新页面", data=refresh_data),
                Button.inline("返回菜单", data=b"menu:home"),
            ]
        ]

    def _task_buttons(self, task: dict[str, Any] | None) -> list[list[Any]]:
        if task is None:
            return self._secondary_buttons(b"queue:1")
        task_id = int(task["id"])
        rows: list[list[Any]] = [
            [
                Button.inline("刷新进度", data=f"task:view:{task_id}".encode()),
                Button.inline("最近任务", data=b"queue:1"),
            ]
        ]
        actions: list[Any] = []
        if task["state"] == "cancelled":
            actions.append(Button.inline("重新转存", data=f"task:restart:{task_id}".encode()))
        elif needs_review(task):
            actions.append(Button.inline("换名重传", data=f"task:recover:{task_id}".encode()))
        elif task["state"] in FAILED_STATES or task.get("cancel_requested"):
            actions.append(
                Button.inline("重新排队", data=f"task:retry:{task_id}".encode())
            )
        if (
            task["state"] in {"queued", "download_failed"}
            and not task.get("local_path")
            and task.get("transfer_mode") != "stream"
            and not task.get("cancel_requested")
        ):
            actions.append(
                Button.inline("切换流式", data=f"task:stream:{task_id}".encode())
            )
        if actions:
            rows.append(actions)
        if not needs_review(task) and task["state"] not in {
            "completed",
            "confirmed",
            "cleanup_pending",
            "cancelled",
        }:
            rows.append(
                [Button.inline("取消任务", data=f"task:cancel:{task_id}".encode())]
            )
        rows.append([Button.inline("返回菜单", data=b"menu:home")])
        return rows

    def _format_operation_result(
        self,
        *,
        status: str,
        operation: str,
        note: str,
        task: dict[str, Any] | None = None,
    ) -> str:
        lines = ["操作结果", "", f"执行状态：{status}"]
        if task is not None:
            lines.extend(
                (
                    f"任务编号：#{task['id']}",
                    f"执行操作：{operation}",
                    f"当前状态：{self._state_label(task['state'])}",
                )
            )
        else:
            lines.append(f"执行操作：{operation}")
        lines.append(f"后续说明：{note}")
        return "\n".join(lines)

    def _state_label(self, state: str) -> str:
        return state_label(
            state, getattr(self.settings, "destination_label", "CloudDrive2")
        )

    def _queue_buttons(
        self, page: int, tasks: list[dict[str, Any]], has_next: bool
    ) -> list[list[Any]]:
        task_buttons = [
            Button.inline(
                f"查看 #{task['id']}", data=f"task:view:{task['id']}".encode()
            )
            for task in tasks
        ]
        rows = [task_buttons[index:index + 2] for index in range(0, len(task_buttons), 2)]
        navigation: list[Any] = []
        if page > 1:
            navigation.append(Button.inline("上一页", data=f"queue:{page - 1}".encode()))
        if has_next:
            navigation.append(Button.inline("下一页", data=f"queue:{page + 1}".encode()))
        if navigation:
            rows.append(navigation)
        rows.append([Button.inline("↩️", data=b"menu:home")])
        return rows

    def _scan_summary(self) -> str:
        scan = getattr(self, "_last_staging_scan", None)
        if scan is None:
            return ""
        return (f"扫描范围：配置目标目录第一层；已检查 {scan.checked} 项，"
                f"发现 {scan.found} 项临时文件；{'完整' if scan.complete else '未完成'}\n")

    def _format_queue_task(self, task: dict[str, Any]) -> str:
        label = self._state_label(task["state"])
        if task.get("transfer_mode") == "stream" and task["state"] in FAILED_STATES:
            label = "流式传输失败"
        text = (
            f"任务编号：#{task['id']}｜{label}｜{format_bytes(task['file_size'])}\n"
            f"文件名称：{truncate_display(str(task['file_name']))}"
        )
        progress = getattr(self, "_progress", {}).get(int(task["id"]))
        if progress and task["state"] in {"downloading", "uploading", "streaming"}:
            speed = sample_rate(progress)
            percent = min(100, 100 * progress["bytes"] / max(1, task["file_size"]))
            text += f"\n传输进度：{percent:.1f}%｜{format_rate(speed)}"
        elif task.get("wait_reason"):
            text += f"\n等待原因：{truncate_display(str(task['wait_reason']))}"
        return text

    def _format_task(self, task: dict[str, Any], verbose: bool = False) -> str:
        label = self._state_label(task["state"])
        if task.get("transfer_mode") == "stream" and task["state"] in FAILED_STATES:
            label = "流式传输失败；无完整本地副本，远端路径记录已保留"
        text = (
            "任务详情\n\n"
            f"任务编号：#{task['id']}\n"
            f"文件名称：{task['file_name']}\n"
            f"文件大小：{format_bytes(task['file_size'])}\n"
            f"当前状态：{label}"
        )
        reason = task.get("wait_reason")
        error = task.get("error")
        if reason:
            text += f"\n等待原因：{reason}"
        if error:
            text += f"\n失败原因：{safe_failure(str(error))}"
        text += (
            "\n传输模式：流式传输（不占用本地任务额度）"
            if task.get("transfer_mode") == "stream"
            else "\n传输模式：普通落盘"
        )
        progress = getattr(self, "_progress", {}).get(int(task["id"]))
        if progress and task["state"] in {"downloading", "uploading", "streaming"}:
            speed = sample_rate(progress)
            percent = min(100, 100 * progress["bytes"] / max(1, task["file_size"]))
            text += (
                f"\n传输进度：{percent:.1f}%｜{format_rate(speed)}"
                f"\n已经耗时：{int(time.monotonic() - progress['started'])} 秒"
            )
        if verbose:
            original = task.get("original_file_name")
            if original and original != task["file_name"]:
                text += f"\n原文件名称：{original}"
            method = task.get("verification_method")
            verified = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(task["verified_at"])) if task.get("verified_at") else "无记录"
            text += (f"\n本地副本：{'完整，已保留' if self._local_complete(task) else '无完整本地副本'}"
                     f"\n验证等级：{'远端大小已验证（非内容哈希）' if method == 'size' else '尚无验证记录'}"
                     f"\n验证时间：{verified}"
                     + ("\n远端状态：需要核对；可换名重传，原文件保持不变" if needs_review(task) else ""))
            text += (
                f"\n已经下载：{format_bytes(task['downloaded_bytes'])}"
                f"\n重试次数：下载 {task['download_retries']}，上传 {task['upload_retries']}"
            )
            if task.get("remote_path"):
                text += f"\n远端路径：{task['remote_path']}"
            if hasattr(self.db, "events"):
                history = self.db.events(int(task["id"]), limit=4)
                if history:
                    text += "\n状态记录：" + " → ".join(
                        self._state_label(row["new_state"])
                        for row in reversed(history)
                    )
        return text

    @staticmethod
    def _dashboard_time() -> str:
        return time.strftime("%H:%M:%S", time.localtime())

    def _runtime_status_line(self) -> str:
        destination = getattr(self.settings, "destination_label", "CloudDrive2")
        if self.snapshot is None or not self.destination_last_checked:
            return f"🟡 状态检查中 · {destination}"
        if not self._sample_fresh():
            return f"🔴 资源状态异常 · {destination}"
        if not self._destination_ready():
            return f"🔴 存储连接异常 · {destination}"
        if self.db.is_paused():
            return f"🟡 调度已暂停 · {destination}"
        return f"🟢 正常运行 · {destination}"

    def _format_status(self) -> str:
        from tg2cloud.domain.actors import current_user

        actor = current_user(self.settings.allowed_user_id)
        member = actor != self.settings.allowed_user_id
        if member:
            counts = self.db.counts(sender_id=actor)
            downloading = counts.get("downloading", 0)
            uploading = sum(counts.get(s, 0) for s in ("uploading", "streaming", "verifying", "finalizing", "cleanup_pending"))
            failed = self.db.weekly_failed_count(sender_id=actor)
            download_rate, upload_rate = dashboard_rates(self, counts, sender_id=actor)
            network_rate = (download_rate + upload_rate) if download_rate is not None and upload_rate is not None else None
        else:
            counts = self.db.counts()
            downloading = len(self.download_tasks)
            uploading = len(self.upload_tasks) + self._stream_count()
            failed = self.db.weekly_failed_count()
            download_rate, upload_rate = dashboard_rates(self, counts)
            network_rate = self.snapshot.network_bytes_per_second if self._sample_fresh() else None
        queued = counts.get("queued", 0)
        return (
            "☁️ **TG2Cloud**\n"
            f"{self._runtime_status_line()}\n\n"
            "📋 **任务**\n"
            f"⏳ {queued}　⬇️ {downloading}　⬆️ {uploading}　❌ {failed}\n\n"
            "📊 **实时状态**\n"
            f"⬇️ {format_dashboard_rate(download_rate)}\n"
            f"⬆️ {format_dashboard_rate(upload_rate)}\n"
            f"🌐 {format_dashboard_rate(network_rate)}\n\n"
            f"🕐 更新于 {self._dashboard_time()}"
        )

    def _format_vps_resources(self) -> str:
        snapshot = getattr(self, "_dashboard_resource_snapshot", self.snapshot)
        cpu = f"{snapshot.cpu_percent:.1f}%" if snapshot else "--"
        memory = (
            f"{format_dashboard_bytes(snapshot.memory_available)} 可用"
            if snapshot
            else "--"
        )
        disk = (
            f"{format_dashboard_bytes(snapshot.disk_free)} 可用"
            if snapshot
            else "--"
        )
        try:
            used = self.db.used_local_bytes() / 1024**3
            budget = self.settings.local_budget_bytes / 1024**3
            local = f"{used:.2f} / {budget:.2f} GB"
        except Exception:
            local = "--"
            self.log.exception("读取 TG2Cloud 本地额度失败")
        return (
            "🖥 **VPS 资源**\n\n"
            f"CPU　{cpu}\n"
            f"内存　{memory}\n"
            f"磁盘　{disk}\n"
            f"本地　{local}\n"
            + (f"保护状态　{self._resource_pause_reason}\n" if getattr(self, "_resource_pause_reason", "") else "")
            + "\n"
            f"🕐 更新于 {self._dashboard_time()}"
        )

    def _format_doctor(self) -> str:
        checked = self.destination_last_checked
        age = f"{max(0, int(time.time() - checked))} 秒前" if checked else "尚未完成"
        probe_details = ""
        if getattr(self.settings, "storage_backend", "clouddrive2") == "openlist":
            kind = getattr(self, "destination_failure_kind", "")
            label = {"rate_limited": "限流等待（HTTP 429）", "unauthorized": "凭据需核对（HTTP 401）"}.get(
                kind, "只读探测通过" if self._destination_ready() else "未就绪"
            )
            remaining = max(0, int(getattr(self, "_destination_next_probe_at", 0) - time.monotonic() + 0.999))
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(checked)) if checked else "尚未完成"
            probe_details = (
                f"OpenList 探测：{label}\n最近实际探测：{timestamp}（VPS 本地时间）\n"
                + (f"下次允许探测：约 {remaining} 秒后；刷新不会绕过冷却\n" if remaining else "下次允许探测：可执行；等待后台检查或主动刷新\n")
                + ("冷却结束不代表服务端已经解除限流。\n" if kind == "rate_limited" else "")
            )
        return (
            "运行诊断\n\n"
            f"资源采样：{'正常' if self._sample_fresh() else '过期或未就绪，停止放行'}\n"
            f"检查时间：{age}\n"
            f"目的目录：{'可以访问（只读检查）' if self._destination_ready() else '未就绪或检查已过期'}\n"
            f"检查说明：{self.destination_error or '目录探测通过，不代表可写'}\n"
            f"{probe_details}"
            f"并发窗口：下载 {self.download_window.value}，上传 {self.upload_window.value}\n"
            f"队列状态：{'用户暂停' if self.db.is_paused() else '调度启用'}\n"
            f"资源保护：{getattr(self, '_resource_pause_reason', '') or '无额外阻塞'}\n"
            "写入验收：请主动执行 manage.sh verify\n"
            "敏感信息：不会在诊断结果中显示"
        )
