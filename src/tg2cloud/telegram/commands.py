from __future__ import annotations

import secrets
import time
from typing import Any

from telethon import Button, events
from telethon.errors import MessageNotModifiedError
from telethon.tl import functions, types

from tg2cloud.domain.actors import current_user
from tg2cloud.domain.errors import (
    needs_review,
)
from tg2cloud.domain.states import FAILED_STATES
from tg2cloud.telegram.views import (
    BOT_MENU_COMMANDS,
    MAX_QUEUE_PAGE,
    ORPHAN_CLEANUP_LIMIT,
    ORPHAN_CLEANUP_TTL_SECONDS,
    QUEUE_PAGE_SIZE,
    TASK_ACTION_TTL_SECONDS,
    truncate_display,
)


class CommandHandlers:
    """Telegram command routing and user-facing status rendering."""

    def _user_home_buttons(self):
        return self._home_buttons(admin=current_user(self.settings.allowed_user_id) == self.settings.allowed_user_id)

    async def _register_bot_menu(self) -> None:
        """Install the native Telegram command menu without blocking startup."""
        commands = [
            types.BotCommand(command=command, description=description)
            for command, description in BOT_MENU_COMMANDS
        ]
        try:
            await self.client(
                functions.bots.SetBotCommandsRequest(
                    scope=types.BotCommandScopeDefault(),
                    lang_code="",
                    commands=commands,
                )
            )
            await self.client(
                functions.bots.SetBotMenuButtonRequest(
                    user_id=types.InputUserEmpty(),
                    button=types.BotMenuButtonCommands(),
                )
            )
        except Exception as exc:  # noqa: BLE001 - optional Telegram UI setup
            self.log.warning("注册 Telegram 原生命令菜单失败：%s", exc)

    @staticmethod
    def _parse_queue_page(parts: list[str]) -> int | None:
        if len(parts) == 1:
            return 1
        if len(parts) != 2:
            return None
        try:
            page = int(parts[1])
        except ValueError:
            return None
        return page if 1 <= page <= MAX_QUEUE_PAGE else None

    def _queue_page(
        self, page: int
    ) -> tuple[str, list[dict[str, Any]], bool]:
        offset = (page - 1) * QUEUE_PAGE_SIZE
        fetched = self.db.list_recent(QUEUE_PAGE_SIZE + 1, offset=offset, sender_id=current_user(self.settings.allowed_user_id))
        tasks = fetched[:QUEUE_PAGE_SIZE]
        has_next = len(fetched) > QUEUE_PAGE_SIZE
        if not tasks:
            return (
                (
                    "最近任务\n\n"
                    f"当前页码：第 {page} 页\n"
                    "任务列表：这一页没有任务"
                ),
                [],
                False,
            )
        text = (
            "最近任务\n\n"
            f"当前页码：第 {page} 页\n\n"
            + "\n\n".join(self._format_queue_task(task) for task in tasks)
        )
        return text, tasks, has_next

    async def _reply_queue(self, event: Any, page: int) -> None:
        text, _, _ = self._queue_page(page)
        await event.reply(text)

    async def _edit_callback(
        self, event: Any, text: str, buttons: list[list[Any]]
    ) -> None:
        try:
            await event.edit(text, buttons=buttons)
        except MessageNotModifiedError:
            return

    async def _callback_command(
        self,
        event: Any,
        handler: Any,
        parts: list[str],
        task_id: int,
    ) -> None:
        owner = self

        class EditReplyProxy:
            async def reply(self, text: str, **options: Any) -> None:
                task = owner.db.get(task_id)
                await owner._edit_callback(event, text, options.get("buttons", owner._task_buttons(task)))

        await handler(EditReplyProxy(), parts)

    async def _show_orphans_callback(self, event: Any) -> None:
        try:
            orphans = await self._find_orphan_staging()
        except NotImplementedError:
            text = "临时巡检\n\n巡检结果：当前目的端不支持巡检"
        except Exception as exc:  # noqa: BLE001 - external diagnostic boundary
            self.log.warning("远端临时文件巡检失败：%s", exc)
            text = "临时巡检\n\n巡检结果：扫描未完成\n安全处理：没有执行删除操作"
        else:
            if not orphans:
                text = (
                    "临时巡检\n\n"
                    "巡检结果：没有发现疑似遗留文件\n"
                    "安全处理：本次只读检查，没有删除内容"
                )
            else:
                shown = "、".join(f"#{task_id}" for task_id, _ in orphans[:20])
                suffix = "（仅显示前 20 项）" if len(orphans) > 20 else ""
                text = (
                    "临时巡检\n\n"
                    f"巡检结果：发现 {len(orphans)} 个疑似遗留文件\n"
                    f"任务编号：{shown}{suffix}\n"
                    "安全处理：本次没有删除任何内容\n"
                    "清理方式：发送 /orphans clean 获取一次性确认码"
                )
        await self._edit_callback(
            event, text + "\n" + self._scan_summary(), self._secondary_buttons(b"menu:orphans")
        )

    async def _handle_callback(self, event: Any) -> None:
        from tg2cloud.telegram.planning import callback
        if await callback(self, event):
            return
        try:
            data = bytes(event.data).decode("ascii")
            parts = data.split(":")
            if data in {
                "menu:home",
                "menu:status",
                "menu:doctor",
                "menu:orphans",
                "vps_resources",
                "refresh:home",
                "refresh:vps_resources",
            }:
                action: tuple[str, Any] = (data, None)
            elif len(parts) == 2 and parts[0] == "queue":
                page = int(parts[1])
                if not 1 <= page <= MAX_QUEUE_PAGE:
                    raise ValueError
                action = ("queue", page)
            elif len(parts) == 2 and parts[0] == "control" and parts[1] in {"pause", "resume"}:
                action = ("control", parts[1])
            elif (
                len(parts) == 3
                and parts[0] == "task"
                and parts[1] in {"view", "retry", "stream", "cancel", "restart", "recover"}
            ):
                task_id = int(parts[2])
                if task_id <= 0:
                    raise ValueError
                action = (f"task:{parts[1]}", task_id)
            elif (
                len(parts) == 4
                and parts[0] == "task"
                and parts[1] in {"cancel_yes", "cancel_no", "recover_yes"}
                and parts[3]
            ):
                task_id = int(parts[2])
                if task_id <= 0:
                    raise ValueError
                action = (f"task:{parts[1]}", (task_id, parts[3]))
            else:
                raise ValueError
        except (UnicodeDecodeError, ValueError, TypeError):
            await event.answer("按钮已经失效，请发送 /help 重新打开菜单。", alert=True)
            return

        await event.answer()
        name, value = action
        actor = current_user(self.settings.allowed_user_id)
        if actor != self.settings.allowed_user_id and name in {
            "control", "menu:doctor", "menu:orphans", "vps_resources", "refresh:vps_resources",
        }:
            await self._edit_callback(event, "此操作仅限管理员；你可以转存和管理自己的任务。", self._user_home_buttons())
            return
        if name.startswith("task:"):
            requested_id = value[0] if isinstance(value, tuple) else value
            owned = self.db.get(requested_id)
            if owned and owned["sender_id"] != actor:
                await self._edit_callback(event, "没有找到这个任务。", self._secondary_buttons(b"queue:1"))
                return
        if name in {"task:restart", "task:recover", "task:recover_yes"}:
            if name == "task:recover_yes":
                await self._confirm_recovery(event, *value)
            else:
                await self._offer_recovery(event, value, rename=name == "task:recover", edit=True)
            return
        if name in {"menu:home", "menu:status", "refresh:home"}:
            await self._refresh_destination_for_dashboard()
            await self._edit_callback(
                event, self._format_status(), self._user_home_buttons()
            )
        elif name in {"vps_resources", "refresh:vps_resources"}:
            self._refresh_resource_snapshot()
            await self._edit_callback(
                event,
                self._format_vps_resources(),
                self._vps_resource_buttons(),
            )
        elif name == "menu:doctor":
            await self._edit_callback(
                event,
                self._format_doctor(),
                self._secondary_buttons(b"menu:doctor"),
            )
        elif name == "menu:orphans":
            await self._show_orphans_callback(event)
        elif name == "queue":
            text, tasks, has_next = self._queue_page(value)
            await self._edit_callback(
                event, text, self._queue_buttons(value, tasks, has_next)
            )
        elif name == "control":
            paused = value == "pause"
            self.db.set_paused(paused)
            text = self._format_operation_result(
                status="操作成功",
                operation="暂停新任务调度" if paused else "恢复任务调度",
                note=(
                    "活动任务继续完成，暂停状态会在重启后保留"
                    if paused
                    else "新任务仍需满足目的端与资源安全条件"
                ),
            )
            await self._edit_callback(event, text, self._user_home_buttons())
        elif name == "task:view":
            task = self.db.get(value)
            text = (
                self._format_task(task, verbose=True)
                if task
                else "任务详情\n\n查询结果：没有找到这个任务"
            )
            await self._edit_callback(event, text, self._task_buttons(task))
        elif name in {"task:retry", "task:stream"}:
            command = "/retry" if name == "task:retry" else "/stream"
            handler = self._command_retry if name == "task:retry" else self._command_stream
            await self._callback_command(event, handler, [command, str(value)], value)
        elif name == "task:cancel":
            task = self.db.get(value)
            if task is None:
                await self._edit_callback(
                    event,
                    "确认取消\n\n查询结果：没有找到这个任务",
                    self._secondary_buttons(b"queue:1"),
                )
                return
            if task["state"] in {
                "completed", "confirmed", "cleanup_pending", "cancelled",
            }:
                await self._edit_callback(
                    event, self._format_task(task, verbose=True), self._task_buttons(task)
                )
                return
            token = secrets.token_hex(3)
            confirmations = getattr(self, "_cancel_confirmations", {})
            confirmations[int(task["id"])] = {
                "actor": actor,
                "token": token,
                "expires_at": time.monotonic() + TASK_ACTION_TTL_SECONDS,
            }
            self._cancel_confirmations = confirmations
            text = (
                "确认取消\n\n"
                f"任务编号：#{task['id']}\n"
                f"文件名称：{truncate_display(str(task['file_name']))}\n"
                f"当前状态：{self._state_label(task['state'])}\n"
                "操作影响：清理本地临时文件及本任务远端文件\n"
                "安全说明：远端无法确认删除时会中止并保留本地数据\n"
                "有效时间：请在 5 分钟内确认"
            )
            buttons = [[
                Button.inline(
                    "确认取消", data=f"task:cancel_yes:{task['id']}:{token}".encode()
                ),
                Button.inline(
                    "放弃操作", data=f"task:cancel_no:{task['id']}:{token}".encode()
                ),
            ]]
            await self._edit_callback(event, text, buttons)
        else:
            task_id, token = value
            plan = getattr(self, "_cancel_confirmations", {}).get(task_id)
            valid = bool(
                plan
                and plan.get("actor", self.settings.allowed_user_id) == actor
                and time.monotonic() <= float(plan["expires_at"])
                and secrets.compare_digest(str(plan["token"]), token)
            )
            if not valid:
                if plan and time.monotonic() > float(plan["expires_at"]):
                    self._cancel_confirmations.pop(task_id, None)
                await self._edit_callback(
                    event,
                    "确认取消\n\n确认状态：按钮不存在或已经过期\n"
                    "后续操作：请重新打开任务详情",
                    self._task_buttons(self.db.get(task_id)),
                )
                return
            self._cancel_confirmations.pop(task_id, None)
            if name == "task:cancel_no":
                task = self.db.get(task_id)
                await self._edit_callback(
                    event,
                    self._format_operation_result(
                        status="已经放弃",
                        operation="取消任务",
                        note="任务保持原状态，没有删除任何内容",
                        task=task,
                    ),
                    self._task_buttons(task),
                )
            else:
                await self._callback_command(
                    event,
                    self._command_cancel,
                    ["/cancel", str(task_id)],
                    task_id,
                )

    async def _handle_command(
        self, event: events.NewMessage.Event, text: str
    ) -> None:
        from tg2cloud.telegram.planning import command as planning_command
        if await planning_command(self, event, text):
            return
        parts = text.split()
        command = parts[0].split("@", 1)[0].lower()
        actor = current_user(self.settings.allowed_user_id)
        if actor != self.settings.allowed_user_id and command in {
            "/pause", "/resume", "/doctor", "/orphans", "/inventory", "/bulk",
        }:
            await event.reply("此操作仅限管理员；你可以转存和管理自己的任务。")
            return
        if command in {"/task", "/watch", "/retry", "/cancel", "/stream", "/confirm", "/restart", "/recover"}:
            task_id = self._parse_task_id(parts)
            task = self.db.get(task_id) if task_id else None
            if task and task["sender_id"] != actor:
                await event.reply("没有找到这个任务。")
                return
        if command == "/start":
            await event.reply(self._format_status(), buttons=self._user_home_buttons())
        elif command == "/help":
            await event.reply(self._format_help(admin=actor == self.settings.allowed_user_id))
        elif command == "/quota":
            from tg2cloud.telegram.quota import format_quota
            try:
                text = format_quota(self.db.quota_usage(actor))
            except (ValueError, KeyError):
                text = "个人用量暂时无法核实，请联系管理员核对时区、数据库和版本。"
            await event.reply(text)
        elif command == "/queue":
            page = self._parse_queue_page(parts)
            if page is None:
                await event.reply("使用说明\n\n正确格式：/queue [页码]")
            else:
                await self._reply_queue(event, page)
        elif command in {"/status", "/performance"}:
            await event.reply(self._format_status(), buttons=self._user_home_buttons())
        elif command == "/task":
            await self._command_task(event, parts)
        elif command in {"/restart", "/recover"}:
            task_id = self._parse_task_id(parts)
            if task_id:
                await self._offer_recovery(event, task_id, rename=command == "/recover")
            else:
                await event.reply(f"用法：{command} <任务编号>")
        elif command in {"/search", "/tasks"}:
            await self._command_search(event, parts)
        elif command == "/watch":
            task_id = self._parse_task_id(parts)
            task = self.db.get(task_id) if task_id else None
            if task is None:
                await event.reply("用法：/watch <任务编号>")
            else:
                message = await event.reply(self._format_task(task, verbose=True))
                self._watch_messages[task_id] = message
        elif command in {"/pause", "/resume"}:
            self.db.set_paused(command == "/pause")
            await event.reply(
                self._format_operation_result(
                    status="操作成功",
                    operation=(
                        "暂停新任务调度" if command == "/pause" else "恢复任务调度"
                    ),
                    note=(
                        "活动任务继续完成，暂停状态会在重启后保留"
                        if command == "/pause"
                        else "新任务仍需满足目的端与资源安全条件"
                    ),
                )
            )
        elif command == "/doctor":
            await event.reply(self._format_doctor())
        elif command == "/stream":
            await self._command_stream(event, parts)
        elif command == "/orphans":
            await self._command_orphans(event, parts)
        elif command == "/confirm":
            await self._command_confirm(event, parts)
        elif command == "/retry":
            await self._command_retry(event, parts)
        elif command == "/cancel":
            await self._command_cancel(event, parts)
        elif command in {"/inventory", "/bulk"}:
            from tg2cloud.channel_bulk.control import handle as channel_bulk_handle

            await event.reply(await channel_bulk_handle(self, parts))
        else:
            await event.reply("未知命令。发送 /help 查看可用命令。")

    @staticmethod
    def _parse_task_id(parts: list[str]) -> int | None:
        if len(parts) != 2:
            return None
        try:
            return int(parts[1].lstrip("#"))
        except ValueError:
            return None

    async def _command_task(
        self, event: events.NewMessage.Event, parts: list[str]
    ) -> None:
        task_id = self._parse_task_id(parts)
        task = self.db.get(task_id) if task_id else None
        if task is None:
            await event.reply("使用说明\n\n正确格式：/task <任务编号>")
            return
        await event.reply(self._format_task(task, verbose=True), buttons=self._task_buttons(task))

    async def _command_confirm(
        self, event: events.NewMessage.Event, parts: list[str]
    ) -> None:
        if len(parts) == 2 and parts[1].lower() == "all":
            task_ids = self.db.confirm_completed(limit=100, sender_id=current_user(self.settings.allowed_user_id))
            if not task_ids:
                await event.reply("当前没有等待人工确认的已完成任务。")
                return
            await event.reply(
                f"✅ 已批量确认 {len(task_ids)} 个任务。\n"
                "这是你在所用云存储官方客户端集中核验后的人工记录；"
                "如果还有更多，可再次发送 /confirm all。"
            )
            return
        task_id = self._parse_task_id(parts)
        if task_id is None:
            await event.reply(
                "用法：/confirm <任务编号|all>\n"
                "只在所用云存储官方客户端看到文件大小正常、可以打开或播放后使用。"
            )
            return
        result, task = self.db.confirm_115(task_id)
        if result == "missing" or task is None:
            await event.reply("没有找到这个任务。用法：/confirm <任务编号>")
            return
        if result == "already":
            await event.reply(f"任务 #{task_id} 已经由你确认过。")
            return
        if result == "invalid":
            await event.reply(
                f"任务当前状态为“{self._state_label(task['state'])}”，还不能确认。\n"
                "必须先等 Bot 传输完成，并在所用云存储官方客户端看到完整文件。"
            )
            return
        await event.reply(
            f"✅ 任务 #{task_id} 已标记为“云存储官方端已由你确认”。\n"
            "这是你的人工确认记录；Bot 没有调用云存储官方接口复验文件。"
        )

    async def _command_stream(
        self, event: events.NewMessage.Event, parts: list[str]
    ) -> None:
        task_id = self._parse_task_id(parts)
        if task_id is None:
            await event.reply("用法：/stream <任务编号>")
            return
        result, task = self.db.request_stream(task_id)
        if result == "missing" or task is None:
            await event.reply("没有找到这个任务。用法：/stream <任务编号>")
        elif result == "retained":
            await event.reply(
                self._format_operation_result(
                    status="没有执行",
                    operation="切换流式传输",
                    note="已有完整本地文件，应保留可恢复上传能力",
                    task=task,
                )
            )
        elif result == "invalid":
            await event.reply(
                self._format_operation_result(
                    status="没有执行",
                    operation="切换流式传输",
                    note="当前任务状态不支持切换流式模式",
                    task=task,
                )
            )
        else:
            await event.reply(
                self._format_operation_result(
                    status="操作成功",
                    operation="切换流式传输",
                    note="已重新排队；中断后通常需要从头传输",
                    task=task,
                )
            )



    async def _command_orphans(
        self, event: events.NewMessage.Event, parts: list[str] | None = None
    ) -> None:
        parts = parts or ["/orphans"]
        if len(parts) not in {1, 2, 3} or (
            len(parts) >= 2 and parts[1].lower() != "clean"
        ):
            await event.reply(
                "使用说明\n\n"
                "只读巡检：/orphans\n"
                "准备清理：/orphans clean\n"
                "确认清理：按 Bot 返回的一次性确认码操作"
            )
            return
        if len(parts) == 3:
            await self._confirm_orphan_cleanup(event, parts[2])
            return
        try:
            orphans = await self._find_orphan_staging()
        except NotImplementedError:
            await event.reply("临时巡检\n\n巡检结果：当前目的端不支持巡检")
            return
        except Exception as exc:  # noqa: BLE001 - external diagnostic boundary
            self.log.warning("远端临时文件巡检失败：%s", exc)
            await event.reply(
                "临时巡检\n\n巡检结果：远端巡检失败，扫描未完成\n" + self._scan_summary() +
                "安全处理：未执行任何删除操作"
            )
            return
        if not orphans:
            await event.reply(
                "临时巡检\n\n"
                + self._scan_summary() +
                "巡检结果：没有发现疑似遗留文件\n"
                "安全处理：本次只读检查，没有删除内容"
            )
            return
        shown = "、".join(f"#{task_id}" for task_id, _ in orphans[:20])
        suffix = "（仅显示前 20 项）" if len(orphans) > 20 else ""
        if len(parts) == 1:
            await event.reply(
                "临时巡检\n\n"
                + self._scan_summary() +
                f"巡检结果：发现 {len(orphans)} 个疑似遗留文件\n"
                f"任务编号：{shown}{suffix}\n"
                "安全处理：本次没有删除任何内容\n"
                "清理方式：发送 /orphans clean 获取一次性确认码"
            )
            return
        planned = tuple(orphans[:ORPHAN_CLEANUP_LIMIT])
        token = secrets.token_hex(3)
        self._orphan_cleanup_plan = {
            "token": token,
            "expires_at": time.monotonic() + ORPHAN_CLEANUP_TTL_SECONDS,
            "objects": planned,
        }
        limited = (
            f"；本次只计划前 {ORPHAN_CLEANUP_LIMIT} 个"
            if len(orphans) > ORPHAN_CLEANUP_LIMIT else ""
        )
        await event.reply(
            "清理确认\n\n"
            f"待清数量：{len(planned)} 个疑似遗留文件{limited}\n"
            "有效时间：5 分钟\n"
            f"确认命令：/orphans clean {token}\n"
            "安全说明：确认时重新扫描，活动任务文件不会删除"
        )


    async def _command_retry(
        self, event: events.NewMessage.Event, parts: list[str]
    ) -> None:
        if len(parts) == 2 and parts[1].lower() == "all":
            tasks = self.db.list_states(FAILED_STATES, limit=100, sender_id=current_user(self.settings.allowed_user_id))
            count = sum(
                self._retry_task(task)
                for task in tasks
                if not task.get("cancel_requested")
                and not needs_review(task)
            )
            await event.reply(
                self._format_operation_result(
                    status="操作成功",
                    operation="批量重试失败任务",
                    note=f"已重新排队 {count} 个；取消清理中的任务不会参与",
                )
            )
            return
        task_id = self._parse_task_id(parts)
        task = self.db.get(task_id) if task_id else None
        if task is None:
            await event.reply("用法：/retry <任务编号>")
            return
        if task["state"] == "cancelled" or needs_review(task):
            await self._offer_recovery(event, task_id, rename=needs_review(task))
            return
        if self._retry_task(task):
            updated = self.db.get(task_id)
            await event.reply(
                self._format_operation_result(
                    status="操作成功",
                    operation="重新加入队列",
                    note="满足目的端与资源安全条件后自动开始",
                    task=updated,
                )
            )
        else:
            await event.reply(
                self._format_operation_result(
                    status="没有执行",
                    operation="重新加入队列",
                    note="状态不适用、任务仍在执行或个人配额不足；可用 /quota 核对",
                    task=task,
                )
            )

    async def _offer_recovery(self, event: Any, task_id: int, *, rename: bool = False,
                              edit: bool = False, message_id: int | None = None) -> None:
        task = self.db.get(task_id)
        actor = current_user(self.settings.allowed_user_id)
        if task and task["sender_id"] != actor:
            if edit:
                await self._edit_callback(event, "没有找到这个任务。", self._secondary_buttons(b"queue:1"))
            else:
                await event.reply("没有找到这个任务。")
            return
        valid = task and task["sender_id"] == actor and (
            task["state"] in FAILED_STATES if rename else task["state"] == "cancelled"
        )
        if not valid or task.get("cancel_requested") or task_id in getattr(self, "_cancelling", set()):
            text, buttons = "任务状态不支持此操作；取消处理中请等待结果。", self._task_buttons(task)
        else:
            token = secrets.token_hex(6)
            plans = getattr(self, "_recovery_confirmations", {})
            plans[task_id] = {"token": token, "expires": time.monotonic() + TASK_ACTION_TTL_SECONDS,
                              "revision": task["updated_at"], "rename": rename, "message_id": message_id,
                              "actor": actor}
            self._recovery_confirmations = plans
            text = (f"确认{'换名重传' if rename else '重新转存'} · #{task_id}\n"
                    "保留历史记录和原远端文件；完整本地副本优先复用，否则重新下载。\n"
                    "新传输使用不冲突的文件名。请在 5 分钟内确认。")
            buttons = [[Button.inline("确认恢复", data=f"task:recover_yes:{task_id}:{token}".encode()),
                        Button.inline("返回详情", data=f"task:view:{task_id}".encode())]]
        if edit:
            await self._edit_callback(event, text, buttons)
        else:
            await event.reply(text, buttons=buttons)

    async def _confirm_recovery(self, event: Any, task_id: int, token: str) -> None:
        plans = getattr(self, "_recovery_confirmations", {})
        plan = plans.get(task_id)
        task = self.db.get(task_id)
        actor = current_user(self.settings.allowed_user_id)
        valid = (plan and task and time.monotonic() <= plan["expires"]
                 and task["sender_id"] == actor and plan.get("actor", self.settings.allowed_user_id) == actor
                 and secrets.compare_digest(plan["token"], token))
        updated = False
        if valid:
            plans.pop(task_id, None)  # Consume before mutation, including failed CAS.
            if (task_id not in getattr(self, "_cancelling", set())
                    and task_id not in self.download_tasks and task_id not in self.upload_tasks):
                updated = self.db.requeue(
                    task_id, sender_id=actor,
                    expected_updated_at=plan["revision"], local_complete=self._local_complete(task),
                    fresh_path=True, restart_cancelled=not plan["rename"], message_id=plan["message_id"],
                )
        text = "已确认恢复；原远端文件保持不变，等待调度。" if updated else "确认已失效、任务状态已改变或个人配额不足；请查看 /quota 并刷新详情。"
        await self._edit_callback(event, text, self._task_buttons(self.db.get(task_id)))

    async def _command_search(self, event: Any, parts: list[str]) -> None:
        state, query, after_id = "", "", 0
        try:
            if parts[0].split("@", 1)[0].lower() == "/tasks":
                if len(parts) not in {2, 3}:
                    raise ValueError
                state = parts[1].lower()
                after_id = int(parts[2].lstrip("#")) if len(parts) == 3 else 0
                if after_id < 0:
                    raise ValueError
            else:
                query = " ".join(parts[1:])[:200]
                if not query:
                    raise ValueError
            tasks = self.db.search(sender_id=current_user(self.settings.allowed_user_id), query=query, state=state,
                                   after_id=after_id)
        except ValueError:
            await event.reply("用法：/search <文件名> 或 /tasks <状态> [最后编号]；编号查询使用 /task。")
            return
        text = "任务查询\n\n" + ("\n\n".join(self._format_queue_task(t) for t in tasks[:5]) or "没有匹配任务")
        if len(tasks) > 5:
            text += f"\n下一页：/tasks {state} {tasks[4]['id']}" if state else "\n还有更多结果，请缩小文件名范围。"
        await event.reply(text, buttons=self._queue_buttons(1, tasks[:5], False))



    async def _refresh_destination_for_dashboard(self) -> None:
        last = getattr(self, "destination_last_checked", 0)
        interval = self.settings.remote_health_interval
        if last > 0 and 0 <= time.time() - last <= interval:
            return  # Refresh speeds immediately using a still-fresh health result.
        refresh = getattr(self, "_probe_destination_once", None)
        if callable(refresh):
            await refresh()

    def _refresh_resource_snapshot(self) -> None:
        try:
            self._dashboard_resource_snapshot = self.monitor.sample()
        except Exception:
            self._dashboard_resource_snapshot = None
            self.log.exception("手动刷新 VPS 资源失败")
