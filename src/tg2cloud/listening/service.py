"""Bounded polling and persisted inbox; transport continues in the existing queue."""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone

from tg2cloud.domain.errors import SourceTemporaryError
from tg2cloud.domain.planning import PlanningError, fields, name_plan
from tg2cloud.domain.source_directories import media_directory
from tg2cloud.listening import account, rules
from tg2cloud.listening.transport import FLOOD_WAITS, DownloadBackoff, download_scope
from tg2cloud.monitoring.observations import record_event
from tg2cloud.persistence.plans import Plans

BATCH = 100
LOOKBACK = 6 * 3600
SOURCE_POLL_TIMEOUT = 30
POLL_ROUND_BUDGET = 30
MAINTENANCE_INTERVAL = 60
MAINTENANCE_BACKLOG_INTERVAL = 5
CONNECT_TIMEOUT = 30


class ReceiveLimited(ValueError):
    """Durable backpressure: retry later without consuming a message cursor."""


class ListenerConnectionError(ConnectionError):
    """Only transient failures inside Telegram connection/authentication I/O."""


async def connected_identity(client):
    from telethon.errors import ServerError, TimedOutError

    async def request():
        await client.connect()
        # is_user_authorized() collapses all RPCError (including server failures)
        # into False. get_me() only translates UnauthorizedError into None.
        return await client.get_me()

    try:
        return await asyncio.wait_for(request(), CONNECT_TIMEOUT)
    except (PermissionError, FileNotFoundError):
        raise  # Local permission/configuration failures must not become network retries.
    except (OSError, asyncio.TimeoutError, ServerError, TimedOutError):
        raise ListenerConnectionError("Telegram 连接暂不可用") from None


class ListenerReadError(RuntimeError):
    """Authorization/content failures require review, not automatic identity fallback."""
    failure_kind = "source_access"


class ListenerRateLimited(ListenerReadError):
    """A bounded wait ended; preserve the cooldown without blaming permissions."""
    failure_kind = "rate_limited"

    def __init__(self, message, *, retry_after_seconds=0):
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class ListenerAttachmentUnavailable(ListenerReadError):
    def __init__(self):
        super().__init__("来源消息或附件暂不可读；可能已删除或不再包含附件，请核对原消息")


AUTH_ERRORS = {"AuthKeyUnregisteredError", "SessionRevokedError", "SessionExpiredError",
               "UserDeactivatedError", "UserDeactivatedBanError", "AuthKeyDuplicatedError"}


def source_error(exc):
    if isinstance(exc, FLOOD_WAITS):
        return f"Telegram 限流，请等待 {max(5, exc.seconds)} 秒后重试；账号冷却期间不会接收新任务"
    name = type(exc).__name__
    if name in AUTH_ERRORS:
        return "个人监听账号授权失效；请在 tg2cloud 的监听管理中重新登录"
    if name in {"UsernameInvalidError", "UsernameNotOccupiedError", "InviteHashInvalidError", "InviteHashExpiredError", "PeerIdInvalidError"}:
        return "来源用户名、聊天 ID 或邀请链接无效或已失效；请核对后重新填写"
    if name in {"ChatForwardsRestrictedError", "MediaEmptyError"}:
        return "Telegram 拒绝读取此附件；请核对来源权限，本程序不会绕过服务端拒绝"
    if name in {"ChannelPrivateError", "UserNotParticipantError", "ChatAdminRequiredError"}:
        return "监听账号尚未加入来源，或没有读取权限；请先在该个人账号中加入并核对权限"
    if isinstance(exc, ListenerReadError):
        return str(exc)  # Only fixed, locally produced messages use this type.
    if isinstance(exc, (OSError, asyncio.TimeoutError)):
        return "连接 Telegram 失败或超时；请检查 VPS 网络后重试，来源尚未添加"
    if isinstance(exc, ValueError):
        return "无法解析来源；请填写当前个人账号已加入的 @用户名、聊天 ID 或有效链接"
    return "来源核验未完成；请检查监听账号与来源后重试，未显示内部错误或凭据"


async def verified_chat(client, reference):
    from telethon.tl import types

    entity = await client.get_entity(reference)
    if (
        not isinstance(entity, (types.Channel, types.Chat))
        or getattr(entity, "left", True)
        or getattr(entity, "deactivated", False)
    ):
        raise ListenerReadError("来源不是已加入且可访问的频道或群组")
    from telethon.tl.custom.participantpermissions import ParticipantPermissions

    permissions = await client.get_permissions(entity, "me")
    if not isinstance(permissions, ParticipantPermissions) or permissions.has_left:
        raise ListenerReadError("无法确认账号仍在来源中；停止接收")
    if permissions.is_banned:
        # Telegram also uses the banned participant type for restricted, still joined readers.
        member = permissions.participant
        rights = member.banned_rights
        if (
            getattr(member, "left", None) is not False
            or getattr(rights, "view_messages", None) is not False
        ):
            raise ListenerReadError("账号已退出或不能读取此来源")
    elif not (permissions.has_default_permissions or permissions.is_admin):
        raise ListenerReadError("无法确认来源成员权限")
    return entity


class CheckedSource:
    def __init__(self, listener, task):
        self.listener, self.task = listener, task
        self.message = None
        self._client = None

    def _identity(self):
        """Recheck durable authorization at every async metadata/download boundary."""
        l, task = self.listener, self.task
        if (
            not l.client
            or l.admin != l.owner.settings.allowed_user_id
            or not l.owner._authorized(l.admin)
            or task["sender_id"] != l.admin
        ):
            raise ListenerReadError("监听账号未连接或任务归属不匹配")
        current = account.state(l.db)
        if not current["enabled"] or task["read_identity"] != "listener:" + str(
            current["account"]
        ):
            raise ListenerReadError(
                "监听授权已停用或读取身份变化，请重新授权后手动重试"
            )
        source = l.plans.source(l.admin, task["source_id"])
        if source["chat"] != task["chat_id"] or source["account"] != current["account"]:
            raise ListenerReadError("来源身份不一致")
        return l.client, source

    async def get_messages(self, entity, *, ids):
        # Failed revalidation must not leave an earlier message authorized.
        self.message = self._client = None
        client, source = self._identity()
        if source["chat"] != entity or self.task["message_id"] != ids:
            raise ListenerReadError("来源身份不一致")
        try:
            await verified_chat(client, entity)
            if self._identity()[0] is not client:
                raise ListenerReadError("监听连接已变化；请重新核验附件")
            message = await client.get_messages(entity, ids=ids)
            if self._identity()[0] is not client:
                raise ListenerReadError("监听连接已变化；请重新核验附件")
            if not message or not rules.facts(message)["attachment"]:
                # One bounded recheck for a transient empty metadata response.
                # No alternate identity, historical scan or unbounded polling.
                await asyncio.sleep(1)
                if self._identity()[0] is not client:
                    raise ListenerReadError("监听连接已变化；请重新核验附件")
                await verified_chat(client, entity)
                if self._identity()[0] is not client:
                    raise ListenerReadError("监听连接已变化；请重新核验附件")
                message = await client.get_messages(entity, ids=ids)
                if self._identity()[0] is not client:
                    raise ListenerReadError("监听连接已变化；请重新核验附件")
        except ListenerReadError:
            raise
        except Exception as exc:
            await self.classify_error(source, exc, client=client)
            if isinstance(exc, (OSError, asyncio.TimeoutError)):
                raise
            raise ListenerReadError("监听来源不可访问，请检查来源或账号状态") from None
        if (
            not message
            or not rules.facts(message)["attachment"]
        ):
            raise ListenerAttachmentUnavailable()
        if rules.facts(message)["media_key"] != self.task["media_key"]:
            raise ListenerReadError("来源附件已变化；停止原任务，避免下载错误文件")
        self.message, self._client = message, client
        return message

    async def download_media(self, message, **options):
        client, _ = self._identity()
        if message is None or message is not self.message or client is not self._client:
            raise ListenerReadError("未核验的监听附件不能下载")
        def validate():
            if self._identity()[0] is not client or self._client is not client:
                raise ListenerReadError("监听连接已变化；停止下载，不切换读取身份")
            current = self.listener.db.get(self.task["id"])
            if not current or current.get("cancel_requested"):
                raise ListenerReadError("任务已取消；停止监听下载")

        def waiting(seconds):
            self.listener.download_limited()
            current = account.state(self.listener.db)
            account.write_state(
                self.listener.db, expected=current, status="Telegram 下载限速冷却",
                cooldown_until=max(current.get("cooldown_until", 0), time.time() + seconds),
            )
            should_log = self.listener.record_wait(self.task, seconds)
            if should_log:
                self.listener.owner.log.info(
                    "任务 #%s Telegram 下载限速，等待 %s 秒；后续等待按任务汇总",
                    self.task["id"], seconds,
                )

        try:
            validate()
            with download_scope(validate, waiting):
                return await client.download_media(message, **options)
        except Exception as exc:
            source = self.listener.plans.source(
                self.listener.admin, self.task["source_id"]
            )
            await self.classify_error(source, exc, client=client)
            raise
        finally:
            self.listener.finish_wait(self.task["id"])

    async def classify_error(self, source, exc, *, client):
        # Every delayed error belongs to the client which issued the request.
        # In particular, an old authorization error must not revoke a new login.
        if self._identity()[0] is not client:
            raise ListenerReadError("监听连接已变化；忽略旧请求错误，请重新核验任务")
        if isinstance(exc, FLOOD_WAITS):
            self.listener.download_limited()
            await self.listener.source_failure(source, exc)
            raise ListenerRateLimited(
                f"Telegram 限流，需等待至少 {max(5, exc.seconds)} 秒；"
                "已记录账号冷却",
                retry_after_seconds=max(5, exc.seconds),
            ) from None
        if type(exc).__name__ in AUTH_ERRORS | {
            "ChannelPrivateError",
            "UserNotParticipantError",
            "ChatAdminRequiredError", "ChatForwardsRestrictedError", "MediaEmptyError",
        }:
            await self.listener.source_failure(source, exc)
            raise ListenerReadError(
                source_error(exc)
            ) from None
        from telethon.errors import ServerError, TimedOutError

        if isinstance(exc, (TimeoutError, TimedOutError)):
            record_event(self.listener.owner, "listener_network")
            raise SourceTemporaryError("network") from None
        if isinstance(exc, ServerError):
            record_event(self.listener.owner, "listener_server")
            raise SourceTemporaryError("server") from None
        if isinstance(exc, OSError) and not isinstance(exc, (PermissionError, FileNotFoundError)):
            record_event(self.listener.owner, "listener_network")
            raise SourceTemporaryError("network") from None


def task_source(owner, task):
    if task.get("read_identity", "bot") == "bot":
        return owner.source
    listener = getattr(owner, "listener", None)
    if listener is None or task["sender_id"] != owner.settings.allowed_user_id:
        raise ListenerReadError("监听任务无法取得匹配的管理员读取身份")
    return CheckedSource(listener, task)


def album_captions(messages):
    """Only associate text from this bounded page and the same chat/album."""
    captions = {}
    for message in sorted(messages, key=lambda item: item.id):
        group = getattr(message, "grouped_id", None)
        text = getattr(message, "raw_text", "") or getattr(message, "message", "")
        if group and text:
            captions.setdefault((message.chat_id, group), text)
    return captions


def message_caption(message, captions):
    text = getattr(message, "raw_text", "") or getattr(message, "message", "")
    if text:
        return text
    original = fields(getattr(getattr(message, "file", None), "name", "") or "")
    if original["episode"] or original["chapter"]:
        return None  # Never borrow a different episode's title.
    return captions.get((message.chat_id, getattr(message, "grouped_id", None)))


class Listener:
    def __init__(self, owner):
        self.owner, self.db, self.admin = (
            owner,
            owner.db,
            owner.settings.allowed_user_id,
        )
        self.plans = Plans(self.db, self.admin)
        self.client = None
        self._rpc_lock = asyncio.Lock()
        self._session = None
        self._last_notice = None
        self._verified_until = {}
        self._poll_after = 0
        self._next_maintenance_at = None
        self._maintenance_warning = False
        self._connect_failures = 0
        self.download_backoff = DownloadBackoff()
        self.wait_records = OrderedDict()

    def record_wait(self, task, seconds):
        record_event(self.owner, "listener_wait")
        now = time.monotonic()
        record = self.wait_records.setdefault(task["id"], {"source": task["source_id"], "count": 0,
                                                       "seconds": 0, "until": 0, "logged": None})
        self.wait_records.move_to_end(task["id"])
        while len(self.wait_records) > 64:
            self.wait_records.popitem(last=False)
        record["count"] += 1
        record["seconds"] += seconds
        record["until"] = max(record["until"], now + seconds)
        due = record["logged"] is None or now - record["logged"] >= 60
        if due:
            record["logged"] = now
        return due

    def finish_wait(self, task_id):
        record = self.wait_records.get(task_id)
        if record:
            record["until"] = 0
            self.owner.log.info("任务 #%s 本次下载等待结束；本进程累计 %s 次、约 %s 秒，结果见任务状态",
                                task_id, record["count"], record["seconds"])

    def wait_summary(self, *, source_id=None, task_id=None):
        records = [r for key, r in self.wait_records.items()
                   if (task_id is None or key == task_id) and (source_id is None or r["source"] == source_id)]
        if not records:
            return ""
        count = sum(r["count"] for r in records)
        seconds = sum(r["seconds"] for r in records)
        active = sum(r["until"] > time.monotonic() for r in records)
        return f"Telegram 等待：{count} 次、累计约 {seconds} 秒；" + (f"当前 {active} 项等待" if active else "当前无等待") + "（本次进程最近 64 项任务）"

    def download_limited(self):
        # Keep this signal separate from the resource window shared by manual tasks.
        window = getattr(self.owner, "download_window", None)
        self.download_backoff.limited(getattr(window, "value", 1))

    def require_admin(self, actor):
        self.plans.authorize(actor, admin=True)
        if actor != self.owner.settings.allowed_user_id or not self.owner._authorized(
            actor
        ):
            raise PermissionError("管理员授权已失效")

    async def with_client(self, operation):
        """Run operation(client) on the listener's current client.

        Does not construct a client or copy the session. The same lock covers
        inventory pages and this listener's history poll.
        """
        if (self.client is None or not self.client.is_connected()) and not await self.connect():
            raise ListenerReadError("个人监听账号未连接")
        client = self.client
        async with self._rpc_lock:
            if self.client is not client or client is None:
                raise ListenerReadError("监听连接已变化")
            return await operation(client)

    async def close(self):
        client, session = self.client, self._session
        self.client = self._session = None
        try:
            if client:
                await client.disconnect()
        finally:
            if session:
                session.close()

    async def connect(self):
        value = account.state(self.db)
        if not value["enabled"]:
            await self.close()
            return False
        if self.client:
            if self.client.is_connected():
                if (value.get("cooldown_until", 0) <= time.time()
                        and value.get("status") in {"Telegram 限流冷却", "Telegram 下载限速冷却"}):
                    account.write_state(self.db, expected=value, status="正常监听", cooldown_until=0)
                return True
            await self.close()  # Telethon may have exhausted its reconnect attempts.
        stack = ExitStack()
        try:
            path = stack.enter_context(account.session_lock(self.owner.settings))
            if not path.with_suffix(".session").is_file():
                raise ListenerReadError("未登录；请先在 Linux 终端授权")
            self.client = account.client(self.owner.settings, path)
            me = await connected_identity(self.client)
            if account.state(self.db) != value or self.owner._stop.is_set():
                await self.close()
                stack.close()
                return False
            if (
                not me
                or getattr(me, "bot", None) is not False
                or me.id != value["account"]
            ):
                raise ListenerReadError("授权失效或个人账号不匹配")
            self._session = stack
            if account.write_state(self.db, expected=value, status="正常监听", cooldown_until=0) is None:
                await self.close()
                return False
            return True
        except BaseException:
            try:
                await self.close()
            finally:
                stack.close()
            raise

    async def add(self, actor, reference, config):
        self.require_admin(actor)
        self.plans.source_config(actor, config)
        if not self.client:
            raise PlanningError("请先在终端登录并启用监听账号")
        try:
            entity, latest = await self._resolve(reference)
        except Exception as exc:  # noqa: BLE001 - map private Telegram exceptions to fixed safe messages
            if isinstance(exc, FLOOD_WAITS) or type(exc).__name__ in AUTH_ERRORS:
                await self.source_failure(None, exc)
            raise ListenerReadError(source_error(exc)) from None
        from telethon.utils import get_peer_id

        source = self.plans.save_source(
            actor, account=account.state(self.db)["account"], chat=get_peer_id(entity),
            title=entity.title, cursor=latest[0].id if latest else 0, config=config,
            target_base=self.owner.settings.cd2_target,
        )
        try:
            return await self.prepare_directory(actor, source)
        except ListenerReadError:
            # Registration is durable and paused; show the source and a repair button, not a duplicate-add trap.
            return self.plans.source(actor, source["id"])

    async def prepare_directory(self, actor, source):
        self.require_admin(actor)
        source = self.plans.source(actor, source["id"])
        if source["deleted"]:
            raise PlanningError("来源已删除")
        if source["config"].get("directory_mode", "legacy") != "source_type":
            return source
        try:
            media_directory(source["config"], base=self.owner.settings.cd2_target)
        except ValueError:
            self._directory_failed(actor, source, "保存根目录已变化；请恢复原配置后重试")
            raise ListenerReadError("来源保存根目录已变化；来源已暂停，请核对并恢复原 Bot 保存配置") from None
        try:
            await self.owner.rclone.ensure_directory(source["config"]["source_folder"])
        except BaseException as exc:
            self._directory_failed(actor, source, "请检查网关后点击重试创建目录")
            if not isinstance(exc, Exception):
                raise
            raise ListenerReadError("来源已登记并暂停；目录创建未完成，请检查网关连接、权限和保存目录后重试") from None
        current = self.plans.source(actor, source["id"])
        if current["revision"] != source["revision"] or current["deleted"]:
            raise PlanningError("来源配置已变化，请刷新；已创建的空目录保留")
        with self.plans.transaction(actor, admin=True):
            self.db._conn.execute(
                "UPDATE listener_sources SET status=? WHERE id=? AND revision=? AND deleted=0",
                ("等待检查" if current["enabled"] else "暂停；来源目录已就绪", source["id"], source["revision"]),
            )
        return self.plans.source(actor, source["id"])

    def _directory_failed(self, actor, source, reason):
        with self.plans.transaction(actor, admin=True):
            self.db._conn.execute(
                "UPDATE listener_sources SET enabled=0,status=? WHERE id=? AND revision=? AND deleted=0",
                ("目录创建未完成；" + reason, source["id"], source["revision"]),
            )

    async def _resolve(self, reference):
        reference = reference.strip()
        if reference.lstrip("-").isdigit():
            reference = int(reference)
        elif reference.startswith(("https://t.me/+", "https://t.me/joinchat/")):
            # Check invite without joining; already-member result exposes only the allowed chat.
            from telethon.tl.functions.messages import CheckChatInviteRequest
            from telethon.tl.types import ChatInviteAlready

            invite = await self.client(
                CheckChatInviteRequest(reference.rsplit("/", 1)[1].lstrip("+"))
            )
            if not isinstance(invite, ChatInviteAlready):
                raise ListenerReadError("尚未加入该来源；请先在个人 Telegram 中申请加入")
            reference = invite.chat
        entity = await verified_chat(self.client, reference)
        latest = await self.client.get_messages(entity, limit=1)
        return entity, latest

    def preview(self, source, message, caption=None):
        item = rules.facts(message, caption)
        matched, reason = rules.evaluate(
            source["config"]["rules"], item, source_allowed=True
        )
        naming = name_plan(
            item["original"],
            item["caption"],
            f"telegram-{item['message_id']}{item['extension'] or '.bin'}",
            kind=item["kind"],
            extension=item["extension"],
            entities=getattr(message, "entities", ())
            if caption is None
            or caption
            == (getattr(message, "raw_text", "") or getattr(message, "message", ""))
            else (),
            options=source["config"]["profile"],
        )
        return item, matched, reason, naming

    async def test(self, actor, source_id, *, config=None):
        self.require_admin(actor)
        source = self.plans.source(actor, source_id)
        if config is not None:
            source = source | {"config": self.plans.source_config(actor, config)}
        if not self.client:
            raise PlanningError("监听账号未连接")
        await verified_chat(self.client, source["chat"])
        messages = await self.client.get_messages(source["chat"], limit=10)
        result = []
        captions = album_captions(messages)
        for m in messages:
            item, matched, reason, naming = self.preview(source, m, message_caption(m, captions))
            naming["directory_preview"] = media_directory(source["config"], kind=item["kind"],
                                                         mime=item["mime"], extension=item["extension"],
                                                         base=self.owner.settings.cd2_target)
            from tg2cloud.listening.management import destination
            naming["effective_destination"] = destination(self.owner, actor, source, kind=item["kind"], mime=item["mime"], extension=item["extension"])
            naming["filter_matched"], naming["filter_reason"] = rules.explain(
                source["config"]["rules"], item, source_allowed=True)
            naming["admission_reason"] = "未发现重复，实际入队仍须通过权限、额度与资源检查"
            if self.db._conn.execute(
                "SELECT 1 FROM tasks WHERE sender_id=? AND media_key=?",
                (actor, rules.facts(m)["media_key"]),
            ).fetchone():
                matched, reason = False, "重复文件（沿用原有去重规则）"
            if self.db._conn.execute(
                "SELECT 1 FROM listener_events WHERE account=? AND chat=? AND message=?",
                (source["account"], source["chat"], m.id),
            ).fetchone():
                matched, reason = False, "重复事件（编辑不自动重处理）"
            if reason.startswith("重复"):
                naming["admission_reason"] = reason
            result.append((m.id, matched, reason, naming))
        return result

    def journal(self, source, message_id, result, *, attachment=False):
        with self.plans.transaction(self.admin, admin=True):
            self.db._conn.execute(
                "INSERT OR IGNORE INTO listener_events(account,chat,message,source,result,plan,created,is_attachment) VALUES(?,?,?,?,?,NULL,?,?)",
                (
                    source["account"],
                    source["chat"],
                    message_id,
                    source["id"],
                    result,
                    time.time(),
                    int(attachment),
                ),
            )

    async def receive(self, source, message, caption=None):
        # Every event decision is persisted before the poll cursor moves; duplicate edits are ignored.
        self.require_admin(self.admin)
        source = self.plans.source(self.admin, source["id"])
        previous_revision = source["revision"]
        if self._verified_until.get(source["chat"], 0) < time.monotonic():
            await verified_chat(self.client, source["chat"])
            self._verified_until[source["chat"]] = time.monotonic() + 10
        source = self.plans.source(self.admin, source["id"])
        if source["revision"] != previous_revision:
            return  # Discard a decision whose rules changed during the permission check.
        current = account.state(self.db)
        if not current["enabled"]:
            return  # A stop during the awaited permission check must not admit a new plan.
        if source["account"] != current["account"]:
            raise PermissionError("监听账号身份发生变化")
        if not source["enabled"] or source["deleted"] or message.id <= source["cursor"]:
            return
        c = self.db._conn
        if c.execute(
            "SELECT 1 FROM listener_events WHERE account=? AND chat=? AND message=?",
            (source["account"], source["chat"], message.id),
        ).fetchone():
            return
        if c.execute("SELECT count(*) FROM listener_events").fetchone()[0] >= 20000:
            raise ReceiveLimited("接收记录达到上限，暂停推进；请处理待确认记录")
        item, matched, reason, naming = self.preview(source, message, caption)
        if not matched:
            self.journal(source, message.id, reason, attachment=item["attachment"])
            return
        if self.db.count_states(("queued", "reserved", "downloading", "streaming")) >= 100:
            raise ReceiveLimited(
                "资源受限：队列达到监听接收上限；保留进度，优先处理手动任务"
            )
        plan, created = self.plans.create(
            self.admin,
            item,
            naming,
            identity="listener:" + str(source["account"]),
            source=source["id"],
            batch=f"{source['id']}:{item['group'] or message.id}",
            directory_alias=source["config"]["directory"],
            target_base=self.owner.settings.cd2_target,
        )
        if not source["config"]["confirmation"]:
            try:
                self.plans.submit(self.admin, plan["id"], plan["revision"])
            except (ValueError, RuntimeError):
                # Persist the exact admission boundary: registered is not the same as queued.
                self.db._conn.execute(
                    "UPDATE listener_events SET result='等待额度或配置核对（待确认保留）' WHERE plan=?",
                    (plan["id"],),
                )
        if created and (self._last_notice is None or time.monotonic() - self._last_notice > 60):
            self._last_notice = time.monotonic()
            await self.owner._notify(
                "监听已登记新附件；使用 /pending 查看待确认，/queue 查看已入队任务。"
            )

    def _poll_current(self, source, client):
        current = self.plans.source(self.admin, source["id"])
        authorization = account.state(self.db)
        return (
            self.client is client
            and current["revision"] == source["revision"]
            and current["enabled"] and not current["deleted"]
            and authorization["enabled"] and authorization["account"] == source["account"]
        )

    async def poll(self, source):
        client = self.client
        await verified_chat(client, source["chat"])
        if not self._poll_current(source, client):
            return
        self._verified_until[source["chat"]] = time.monotonic() + 10
        gap = bool(source.get("gap")) or bool(
            source.get("polled_at") and time.time() - source["polled_at"] > LOOKBACK
        )
        if gap:
            self.db._conn.execute(
                "UPDATE listener_sources SET gap=1,status='超出补拉窗口，可能存在缺口；请核对历史' WHERE id=?",
                (source["id"],),
            )
        # Ascending pages, never a live-event maximum watermark. A batch is bounded.
        # Share the listener RPC lock with inventory so both use this client.
        async with self._rpc_lock:
            if self.client is not client:
                return
            messages = [
                m
                async for m in client.iter_messages(
                    source["chat"],
                    min_id=source["cursor"],
                    offset_date=datetime.now(timezone.utc) - timedelta(seconds=LOOKBACK),
                    reverse=True,
                    limit=BATCH + 10,
                )
            ]
        if len(messages) > BATCH:
            tail = getattr(messages[BATCH - 1], "grouped_id", None)
            messages = messages[:BATCH] + (
                [m for m in messages[BATCH:] if getattr(m, "grouped_id", None) == tail]
                if tail
                else []
            )
        # Preview and ingestion share the same bounded, per-chat album policy.
        captions = album_captions(messages)
        for m in messages:
            if time.time() - m.date.timestamp() < 2:
                break  # Allow a short bounded wait for Album members on the next poll.
            if not self._poll_current(source, client):
                return
            latest = self.plans.source(self.admin, source["id"])
            if time.time() - m.date.timestamp() > LOOKBACK:
                self.journal(source, m.id, "超过 6 小时补拉窗口，存在缺口")
            else:
                await self.receive(latest, m, message_caption(m, captions))
            with self.plans.transaction(self.admin, admin=True):
                if not self._poll_current(source, client):
                    return  # A notification await may overlap pause/delete/configuration.
                self.db._conn.execute(
                    "UPDATE listener_sources SET cursor=?,status=? WHERE id=? AND cursor<?",
                    (
                        m.id,
                        "补拉窗口外可能存在缺口，请核对历史"
                        if gap
                        else "正常监听"
                        if time.time() - m.date.timestamp() <= LOOKBACK
                        else "补拉窗口外存在缺口",
                        source["id"],
                        m.id,
                    ),
                )

        with self.plans.transaction(self.admin, admin=True):
            if self._poll_current(source, client):
                self.db._conn.execute(
                    "UPDATE listener_sources SET polled_at=?,status=CASE WHEN gap=1 THEN '超出补拉窗口，可能存在缺口；请核对历史' ELSE '正常监听' END WHERE id=?",
                    (time.time(), source["id"]),
                )

    async def source_failure(self, source, exc):
        """Return false when the account needs to cool down or be reauthorized."""

        if isinstance(exc, FLOOD_WAITS):
            current = account.state(self.db)
            account.write_state(
                self.db, expected=current,
                status="Telegram 限流冷却",
                cooldown_until=max(current.get("cooldown_until", 0),
                                   time.time() + max(5, exc.seconds)),
            )
            return False
        if type(exc).__name__ in AUTH_ERRORS:
            account.write_state(
                self.db, enabled=False, status="授权失效，请在终端重新登录"
            )
            await self.close()
            await self.owner._notify(
                "监听账号授权失效，已停止监听；普通 Bot 转存保持可用。请在终端重新登录。"
            )
            return False
        paused = False
        if isinstance(exc, ListenerReadError) or type(exc).__name__ in {
            "ChannelPrivateError",
            "UserNotParticipantError",
            "ChatAdminRequiredError", "ChatForwardsRestrictedError", "MediaEmptyError",
        }:
            status, paused = "来源或附件读取被拒绝，已暂停；核对权限后手动启用", True
        elif (
            isinstance(exc, ReceiveLimited)
            or isinstance(exc, ValueError)
            and str(exc) == "待确认已达 200 项；请先处理"
        ):
            status = "资源受限：队列或待确认达到上限，60 秒后重查"
        elif isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
            status = "来源读取超时，60 秒后重查；保留已登记进度，其他来源继续"
        elif isinstance(exc, ValueError):
            status, paused = "规则或计划配置无效，已暂停；请核对后启用", True
        else:
            status = "网络或接收持久化失败，60 秒后重查；未推进失败消息"
        with self.db._lock:
            self.db._conn.execute(
                "UPDATE listener_sources SET status=?,enabled=CASE WHEN ? THEN 0 ELSE enabled END,next_retry=? WHERE id=? AND revision=? AND deleted=0",
                (status, int(paused), time.time() + 60, source["id"], source["revision"]),
            )
        return True

    def _maintain_if_due(self):
        now = time.monotonic()
        if self._next_maintenance_at is not None and now < self._next_maintenance_at:
            return
        self._next_maintenance_at = now + MAINTENANCE_INTERVAL
        try:
            # Commit all retention statements together, independently of account state.
            with self.plans.transaction(self.admin, admin=True):
                backlog = self.plans.expire()
            if backlog:
                self._next_maintenance_at = now + MAINTENANCE_BACKLOG_INTERVAL
        except Exception:  # noqa: BLE001 - maintenance must not revoke a valid login
            if not self._maintenance_warning:
                self.owner.log.warning("监听记录维护暂不可用；稍后重试，不影响监听授权")
            self._maintenance_warning = True
        else:
            self._maintenance_warning = False

    async def _poll_sources(self):
        """Serial requests, bounded source time and a rotating round budget.

        Check the round budget between sources so a source starting near its end
        still gets the full request timeout. No detached requests or parallel
        account clients; wait_for joins cancellation before another source runs.
        """
        started = time.monotonic()
        client = self.client
        sources = self.plans.sources(self.admin)
        sources = ([s for s in sources if s["id"] > self._poll_after]
                   + [s for s in sources if s["id"] <= self._poll_after])
        for saved in sources:
            value = account.state(self.db)
            if (self.owner._stop.is_set() or not value["enabled"]
                    or value.get("cooldown_until", 0) > time.time()
                    or client is None or self.client is not client):
                return
            # An earlier request may have awaited while this source was edited.
            source = self.plans.source(self.admin, saved["id"])
            if (not source["enabled"] or source["deleted"]
                    or source.get("next_retry", 0) > time.time()
                    or source["account"] != value["account"]):
                continue
            self._poll_after = source["id"]
            try:
                await asyncio.wait_for(self.poll(source), timeout=SOURCE_POLL_TIMEOUT)
            except Exception as exc:  # noqa: BLE001 - isolate external source failures
                if self.client is not client or account.state(self.db)["account"] != source["account"]:
                    return  # An old request must not revoke a replacement account/client.
                if not await self.source_failure(source, exc):
                    return
            if time.monotonic() - started >= POLL_ROUND_BUDGET:
                return

    async def run(self):
        try:
            from tg2cloud.channel_bulk.jobs import hold_on_startup

            hold_on_startup(self.db)
        except Exception:  # noqa: BLE001 - a bookkeeping failure must not disable listening
            self.owner.log.warning("频道批量作业未自动挂起；批量任务仍不会被调度器领取")
        control = None
        try:
            from tg2cloud.channel_bulk.local_control import start_local_control

            control = await start_local_control(self.owner)
        except Exception:  # noqa: BLE001 - local control must not stop listening or start a scan
            self.owner.log.warning("本地控制套接字未开启；不会自动盘点")
        try:
            while not self.owner._stop.is_set():
                self._last_loop_at = time.monotonic()
                self._maintain_if_due()
                value = account.state(self.db)
                try:
                    if not value["enabled"]:
                        self._connect_failures = 0
                        await self.close()
                    elif (
                        value.get("cooldown_until", 0) <= time.time()
                        and await self.connect()
                    ):
                        self._connect_failures = 0
                        value = account.state(self.db)
                        await self._poll_sources()
                except Exception as exc:  # noqa: BLE001 - private boundary must not expose credentials
                    await self.close()

                    if self.owner._stop.is_set() or account.state(self.db) != value:
                        continue  # Do not overwrite disable, re-login or account changes.
                    if isinstance(exc, FLOOD_WAITS):
                        account.write_state(
                            self.db, expected=value, status="Telegram 限流冷却",
                            cooldown_until=time.time() + max(5, exc.seconds),
                        )
                    elif isinstance(exc, ListenerConnectionError):
                        self._connect_failures = min(7, self._connect_failures + 1)
                        delay = min(300, 5 * 2 ** (self._connect_failures - 1))
                        account.write_state(
                            self.db, expected=value,
                            status=f"连接暂不可用，{delay} 秒后自动重试",
                            cooldown_until=time.time() + delay,
                        )
                    else:
                        changed = account.write_state(
                            self.db, expected=value, enabled=False,
                            status="授权或配置异常，请在终端检查后重新启用",
                        )
                        if changed is not None:
                            await self.owner._notify(
                                "监听授权或配置异常，已停用监听；普通转存不受影响。"
                                "请在 VPS 终端核对后，从监听管理启用。"
                            )
                await asyncio.sleep(5)
        finally:
            if control is not None:
                await control.close()
            await self.close()
