"""Administrator text commands. Arguments are parsed, never executed."""
from __future__ import annotations

import asyncio
import re
import time
from pathlib import Path

from tg2cloud.channel_bulk import inventory, jobs
from tg2cloud.channel_bulk.ledger import Refusal, attest
from tg2cloud.channel_bulk.schema import ensure_db
from tg2cloud.channel_bulk.transfer import listener_busy, run_job

_ALIAS = re.compile(r"^.{1,32}$")
_HASH = re.compile(r"^[0-9a-f]{64}$")


def _int(value: str, name: str) -> int:
    if not re.fullmatch(r"-?\d{1,18}", value or ""):
        raise Refusal("args", f"{name} 必须是整数")
    return int(value)


def _alias_ok(value: str) -> bool:
    return bool(_ALIAS.fullmatch(value)) and not any(char in "/:%" or ord(char) < 32 or char.isspace() for char in value)


def _aliases(value: str) -> list[str]:
    parts = [part for part in value.split(",") if part]
    if not parts or any(not _alias_ok(part) for part in parts):
        raise Refusal("args", "目录别名无效")
    return parts


class Context:
    def __init__(self, owner, job_id: int, directory: str):
        self.db = owner.db
        self.owner = owner
        self.job_id = job_id
        self.admin = owner.settings.allowed_user_id
        self.account = _account(owner)
        self.directory = directory
        self.download_dir = owner.settings.download_dir
        self.transport = owner.channel_bulk_transport
        self.min_free = int(owner.settings.min_free_disk_bytes)

    def now(self) -> float:
        return time.time()

    def monotonic(self) -> float:
        return time.monotonic()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)

    def disk_free(self) -> int:
        import shutil

        return int(shutil.disk_usage(self.download_dir).free)

    def listener_busy(self) -> bool:
        return listener_busy(self.owner)

    def notify(self, text: str) -> None:
        notifier = getattr(self.owner, "_notify", None)
        if notifier is None:
            return
        try:
            result = notifier(text)
        except Exception:  # noqa: BLE001 - a receipt must not unwind a persisted task
            return
        if hasattr(result, "__await__"):
            asyncio.create_task(result)


def _account(owner) -> int:
    from tg2cloud.listening import account

    value = account.state(owner.db).get("account")
    if not isinstance(value, int):
        raise Refusal("listener", "个人监听账号未登录")
    return value


def _directories(owner) -> dict[str, str]:
    from tg2cloud.persistence.plans import Plans

    return Plans(owner.db, owner.settings.allowed_user_id).directories(owner.settings.allowed_user_id)


async def handle(owner, parts: list[str]) -> str:
    ensure_db(owner.db)
    command = parts[0].split("@", 1)[0].lower()
    try:
        if command == "/inventory":
            return await _inventory(owner, parts[1:])
        if command == "/bulk":
            return await _bulk(owner, parts[1:])
    except Refusal as exc:
        return f"未执行：{exc}"
    except (KeyError, ValueError) as exc:
        return f"未执行：{exc}"
    return "未知命令。发送 /help 查看可用命令。"


async def _inventory(owner, args: list[str]) -> str:
    if not args:
        return "用法：/inventory start <聊天ID> | step <编号> | status <编号> | export <编号> csv|json | exclude <聊天ID> <消息ID>"
    action = args[0]
    if action == "start" and len(args) >= 2:
        page = _int(args[2], "每页") if len(args) >= 3 else 20
        delay = _int(args[3], "间隔毫秒") / 1000 if len(args) >= 4 else 2
        if len(args) > 4:
            raise Refusal("args", "参数过多")
        row = inventory.start(
            owner.db, _int(args[1], "聊天ID"), page_size=page, delay_seconds=delay, now=time.time()
        )
        return f"盘点 #{row['id']} 已建立并暂停。发送 /inventory step {row['id']} 读取一页元数据。不会下载，也不会开始转存。"
    if action == "step" and 2 <= len(args) <= 3:
        pages = _int(args[2], "页数") if len(args) == 3 else 1
        inventory_id = _int(args[1], "盘点编号")
        row = inventory.load(owner.db, inventory_id)

        async def fetch(offset_id: int, limit: int):
            return await _fetch(owner, int(row["chat_id"]), offset_id, limit)

        outcome = await inventory.step(
            owner.db, inventory_id, fetch, pages=pages, now=time.time, sleep=asyncio.sleep
        )
        return inventory.status_text(owner.db, inventory_id) + f"\n本步：{outcome}"
    if action == "status" and len(args) == 2:
        return inventory.status_text(owner.db, _int(args[1], "盘点编号"))
    if action == "export" and len(args) == 3 and args[2] in {"csv", "json"}:
        inventory_id = _int(args[1], "盘点编号")
        body = inventory.export_text(owner.db, inventory_id, args[2])
        directory = Path(owner.settings.data_dir) / "channel-bulk" / "exports"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"inventory-{inventory_id}.{args[2]}"
        path.write_text(body, encoding="utf-8")
        return inventory.status_text(owner.db, inventory_id) + f"\n已写入 channel-bulk/exports/{path.name}"
    if action == "exclude" and 3 <= len(args) <= 4:
        reason = args[3] if len(args) == 4 else "review"
        inventory.exclude(
            owner.db, _int(args[1], "聊天ID"), _int(args[2], "消息ID"), reason=reason, now=time.time()
        )
        return "已记录排除。源消息不会删除。"
    raise Refusal("args", "盘点命令无法识别")


async def _fetch(owner, chat_id: int, offset_id: int, limit: int):
    from tg2cloud.listening.service import verified_chat

    listener = owner.listener

    async def operation(client):
        entity = await verified_chat(client, chat_id)
        found = await client.get_messages(entity, limit=limit, offset_id=offset_id)
        if found is None:
            return []
        return list(found) if isinstance(found, list) else [found]

    return await listener.with_client(operation)


async def _bulk(owner, args: list[str]) -> str:
    usage = (
        "用法：/bulk open <盘点> <别名> | manifest <作业> | limits <作业> <日字节> <月字节> <下行B/s> <上行B/s> "
        "| whitelist <作业> <别名> | allow <作业> <消息ID,消息ID> | attest <剩余字节> <周期结束Unix> "
        "| ack-egress <作业> | resume <作业> <清单哈希> | pause <作业> | cancel <作业> | status <作业>"
    )
    if not args:
        return usage
    action = args[0]
    if action == "open" and len(args) == 3:
        if not _alias_ok(args[2]):
            raise Refusal("args", "目录别名无效")
        job = jobs.open_job(owner.db, _int(args[1], "盘点"), args[2], now=time.time())
        return f"批量作业 #{job['id']} 处于预览，未开始传输。目标别名 {job['directory_alias']}。"
    if action == "limits" and len(args) == 6:
        job = jobs.update_controls(
            owner.db, _int(args[1], "作业"), now=time.time(),
            daily_bytes=_int(args[2], "每日"), monthly_bytes=_int(args[3], "每月"),
            down_bps=_int(args[4], "下行"), up_bps=_int(args[5], "上行"),
        )
        return f"作业 #{job['id']} 已记录本地额度与速率。这不是供应商账单。"
    if action == "whitelist" and len(args) == 3:
        job = jobs.set_whitelist(owner.db, _int(args[1], "作业"), _aliases(args[2]), now=time.time())
        return f"作业 #{job['id']} 白名单：{', '.join(job['whitelist'])}"
    if action == "allow" and len(args) == 3:
        raw = [_int(part, "消息ID") for part in args[2].split(",") if part]
        job = jobs.update_controls(owner.db, _int(args[1], "作业"), now=time.time(), allow=raw)
        return f"作业 #{job['id']} 复核允许 {len(job['controls']['allow'])} 条。"
    if action == "ack-egress" and len(args) == 2:
        job = jobs.update_controls(owner.db, _int(args[1], "作业"), now=time.time(), egress_ack=True)
        return f"作业 #{job['id']} 已确认网关延迟出站也计入本地额度。"
    if action == "attest" and len(args) == 3:
        attest(
            owner.db, remaining_bytes=_int(args[1], "剩余"),
            period_end=float(_int(args[2], "周期结束")), now=time.time(), note="",
        )
        return "已记录你填写的剩余额度和周期。程序没有假定任何默认套餐。"
    if action == "manifest" and len(args) == 2:
        built = jobs.manifest(owner.db, _int(args[1], "作业"), now=time.time())
        preview = ",".join(str(value) for value in built["ids"][:20])
        return f"清单 {built['count']} 条。哈希 {built['hash']}\n预览消息：{preview or '（无）'}"
    if action == "pause" and len(args) == 2:
        job = jobs.pause(owner.db, _int(args[1], "作业"), now=time.time(), error="管理员暂停")
        return f"作业 #{job['id']} 已暂停。活动中的其他任务不会被取消。"
    if action == "cancel" and len(args) == 2:
        job = jobs.cancel(owner.db, _int(args[1], "作业"), now=time.time())
        return f"作业 #{job['id']} 已取消后续条目。不会删除源消息，也不会取消无关任务。"
    if action == "status" and len(args) == 2:
        job = jobs.get_job(owner.db, _int(args[1], "作业"))
        record = jobs.attestation_view(owner.db, time.time())
        remaining = "未确认" if record is None else str(record["effective_remaining"])
        return (
            f"作业 #{job['id']} 状态 {job['state']} 目标 {job['directory_alias']}\n"
            f"确认剩余：{remaining}\n备注：{job.get('error') or '无'}"
        )
    if action == "resume" and len(args) == 3:
        if _HASH.fullmatch(args[2]) is None:
            raise Refusal("manifest", "清单哈希必须是 64 位十六进制")
        return await _resume(owner, _int(args[1], "作业"), args[2])
    raise Refusal("args", "批量命令无法识别")


async def _resume(owner, job_id: int, digest: str) -> str:
    lock = getattr(owner, "_channel_bulk_lock", None)
    if lock is None:
        lock = asyncio.Lock()
        owner._channel_bulk_lock = lock
    if lock.locked():
        return "已有批量执行在本进程中，不会并行启动第二个。"
    reason = jobs.rejection(
        owner.db, job_id, digest, now=time.time(), directories=_directories(owner)
    )
    if reason:
        return f"未执行：{reason}"
    ticket = object()
    owner._channel_bulk_ticket = ticket
    asyncio.create_task(_start_locked(owner, job_id, digest, lock, ticket))
    return f"作业 #{job_id} 已接受恢复。一次只传输一个文件；失败、限流和远端复核都会停住并保留记录。"


async def _start_locked(owner, job_id, digest, lock, ticket) -> None:
    import shutil

    async with lock:
        if getattr(owner, "_channel_bulk_ticket", None) is not ticket:
            return
        try:
            decision = jobs.prepare_resume(
                owner.db, job_id, digest, now=time.time(),
                disk_free=int(shutil.disk_usage(owner.settings.download_dir).free),
                min_free=int(owner.settings.min_free_disk_bytes),
                directories=_directories(owner),
            )
        except Refusal as exc:
            jobs.pause(owner.db, job_id, now=time.time(), error=str(exc))
            return
        if decision.get("idempotent"):
            return
        if not hasattr(owner, "channel_bulk_transport"):
            from tg2cloud.channel_bulk.runtime import ListenerTransport

            owner.channel_bulk_transport = ListenerTransport(owner)
        ctx = Context(owner, job_id, decision["directory"])
        try:
            await run_job(ctx)
        except Exception as exc:  # noqa: BLE001 - pause with the exception class only
            jobs.pause(owner.db, job_id, now=time.time(), error=type(exc).__name__)
