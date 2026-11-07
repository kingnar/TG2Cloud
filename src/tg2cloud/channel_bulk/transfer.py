"""One-file bulk transfer. The scheduler does not pick these tasks up."""
from __future__ import annotations

import time
from pathlib import Path

from tg2cloud.channel_bulk.dedup import classify
from tg2cloud.channel_bulk.fingerprint import remember, sample_sha256
from tg2cloud.channel_bulk.jobs import get_job, pause
from tg2cloud.channel_bulk.ledger import (
    ByteRate,
    attested_remaining,
    charge,
    cooldown_seconds,
    disk_allows,
)
from tg2cloud.domain.errors import RemoteReviewRequired, failure_kind, safe_failure
from tg2cloud.domain.naming import safe_file_name
from tg2cloud.persistence.quotas import QuotaExceeded


def _item_map(db, job_id: int) -> dict[int, dict]:
    with db._lock:
        rows = db._conn.execute(
            "SELECT * FROM channel_bulk_items WHERE job_id=? ORDER BY message_id",
            (int(job_id),),
        ).fetchall()
    return {int(row["message_id"]): dict(row) for row in rows}


def _inventory_map(db, inventory_id: int) -> dict[int, dict]:
    with db._lock:
        rows = db._conn.execute(
            "SELECT * FROM channel_inventory_items WHERE inventory_id=?",
            (int(inventory_id),),
        ).fetchall()
    return {int(row["message_id"]): dict(row) for row in rows}


def _update_item(db, job_id: int, message_id: int, **fields) -> None:
    allowed = {"status", "task_id", "attempts", "bytes_charged", "last_error"}
    unknown = set(fields) - allowed
    if unknown:
        raise ValueError("批量明细字段无效")
    assignments = ", ".join(f"{key}=?" for key in fields)
    with db._lock:
        db._conn.execute(
            f"UPDATE channel_bulk_items SET {assignments} WHERE job_id=? AND message_id=?",
            [*fields.values(), int(job_id), int(message_id)],
        )


def _finish(db, job_id: int, now: float) -> None:
    with db._lock:
        db._conn.execute(
            """UPDATE channel_bulk_jobs SET state='done', error=NULL, updated_at=?
               WHERE id=? AND state='running'""",
            (now, int(job_id)),
        )


def _source_id(db, admin: int, account: int, chat_id: int) -> int:
    """Reuse an existing source without changing its enabled flag."""
    with db._lock:
        row = db._conn.execute(
            "SELECT id, deleted FROM listener_sources WHERE account=? AND chat=?",
            (int(account), int(chat_id)),
        ).fetchone()
    if row is not None and not int(row["deleted"]):
        return int(row["id"])
    from tg2cloud.persistence.plans import Plans

    saved = Plans(db, admin).save_source(
        admin,
        account=int(account),
        chat=int(chat_id),
        title=f"channel {int(chat_id)}",
        cursor=0,
        config={"confirmation": True, "rules": {"all_attachments": True}},
    )
    return int(saved["id"])


def _receipt(task: dict) -> str:
    try:
        from tg2cloud.telegram import planning

        formatter = getattr(planning, "receipt_text", None)
        if formatter is not None:
            return formatter(task)
    except Exception:  # noqa: BLE001 - receipt formatting must not block the queued task
        log = __import__("logging").getLogger("tg2cloud.channel_bulk")
        log.warning("回执格式化失败，改用简短回执")
    return f"任务 #{task['id']}\n文件：{task['file_name']}\n状态：在排队"


async def run_job(ctx) -> str:
    """Process pending items one at a time until a gate pauses the job."""
    while True:
        job = get_job(ctx.db, ctx.job_id)
        if job["state"] != "running":
            return str(job["state"])
        if float(job["cooldown_until"]) > ctx.now():
            pause(ctx.db, ctx.job_id, now=ctx.now(), error="限流冷却未结束")
            return "cooldown"
        if ctx.listener_busy():
            pause(ctx.db, ctx.job_id, now=ctx.now(), error="已有个人号下载，批量不并行也不取消")
            return "busy"
        outcome = await _one(ctx, job)
        if outcome != "continue":
            return outcome


async def _one(ctx, job: dict) -> str:
    items = _item_map(ctx.db, job["id"])
    pending = [row for row in items.values() if row["status"] == "pending"]
    if not pending:
        _finish(ctx.db, job["id"], ctx.now())
        return "idle"
    item = pending[0]
    media = _inventory_map(ctx.db, job["inventory_id"]).get(int(item["message_id"]))
    if media is None:
        _update_item(ctx.db, job["id"], item["message_id"], status="rejected", last_error="盘点记录缺失")
        return "continue"
    kind, match = classify(
        ctx.db,
        chat_id=int(job["chat_id"]),
        message_id=int(item["message_id"]),
        media_key=media.get("media_key"),
        file_unique_id=media.get("file_unique_id"),
    )
    ours = match is not None and int(match.get("bulk_job_id") or 0) == int(job["id"])
    if match is not None and not ours:
        status = "skipped" if kind in {"completed", "active"} else "rejected"
        reason = {
            "completed": "任一目录已完成，跳过且不强制",
            "active": "已有活动任务，跳过",
            "blocked": "已有失败或取消记录，批量不会强制重转",
        }[kind]
        _update_item(ctx.db, job["id"], item["message_id"], status=status, last_error=reason, task_id=match["id"])
        return "continue"
    try:
        task = match if ours else _create(ctx, job, media)
    except QuotaExceeded:
        pause(ctx.db, job["id"], now=ctx.now(), error="成员额度不足，批量停止")
        return "quota"
    if task is None:
        _update_item(ctx.db, job["id"], item["message_id"], status="skipped", last_error="已有任务")
        return "continue"
    _update_item(ctx.db, job["id"], item["message_id"], task_id=task["id"])
    if task["state"] in {"completed", "confirmed"}:
        _update_item(ctx.db, job["id"], item["message_id"], status="done", last_error=None)
        return "continue"
    local = _local_path(ctx, task)
    if task["state"] in {"queued", "download_failed", "reserved", "downloading"} or not local.is_file():
        gate = _pre_io(ctx, job, int(media["size"]))
        if gate:
            return gate
        if not charge(
            ctx.db, int(media["size"]), now=ctx.now(),
            daily_cap=int(job["controls"]["daily_bytes"]),
            monthly_cap=int(job["controls"]["monthly_bytes"]),
        ):
            pause(ctx.db, job["id"], now=ctx.now(), error="每日或每月字节额度不足（含重试）")
            return "budget"
        _add_bytes(ctx.db, job["id"], item["message_id"], int(media["size"]))
        try:
            await _download(ctx, job, task, media, local)
        except Exception as exc:  # noqa: BLE001 - persist the failure and keep the job paused
            return _io_failure(ctx, job, item, task, exc, stage="download")
    fresh = ctx.db.get(int(task["id"])) or task
    if fresh["state"] == "cancelled":
        _update_item(ctx.db, job["id"], item["message_id"], status="skipped", last_error="指纹命中已有任务")
        return "continue"
    upload_gate = _pre_io(ctx, job, int(media["size"]))
    if upload_gate:
        return upload_gate
    if not charge(
        ctx.db, int(media["size"]), now=ctx.now(),
        daily_cap=int(job["controls"]["daily_bytes"]),
        monthly_cap=int(job["controls"]["monthly_bytes"]),
    ):
        pause(ctx.db, job["id"], now=ctx.now(), error="上传前额度不足；本地文件保留")
        return "budget"
    _add_bytes(ctx.db, job["id"], item["message_id"], int(media["size"]))
    try:
        return await _upload(ctx, job, fresh, media, local, item)
    except Exception as exc:  # noqa: BLE001 - persist the failure and keep the local file
        return _io_failure(ctx, job, item, fresh, exc, stage="upload")


def _pre_io(ctx, job: dict, size: int) -> str | None:
    remaining = attested_remaining(ctx.db, ctx.now())
    if remaining is None or remaining < size:
        pause(ctx.db, job["id"], now=ctx.now(), error="实际剩余额度未确认或不足")
        return "attestation"
    if not disk_allows(ctx.disk_free(), ctx.min_free, size):
        pause(ctx.db, job["id"], now=ctx.now(), error="磁盘可用空间不足，批量暂停")
        return "disk"
    return None


def _create(ctx, job: dict, media: dict):
    source_id = _source_id(ctx.db, ctx.admin, ctx.account, int(job["chat_id"]))
    name = safe_file_name(str(media["file_name"]), f"video-{int(media['message_id'])}.mp4")
    task, created = ctx.db.create_task(
        chat_id=int(job["chat_id"]),
        message_id=int(media["message_id"]),
        sender_id=int(ctx.admin),
        media_key=media.get("media_key"),
        file_name=name,
        file_size=int(media["size"]),
        original_file_name=name,
        media_kind="视频",
        mime_type=str(media.get("mime") or "video/mp4"),
        media_extension=Path(name).suffix or ".mp4",
        directory_override=ctx.directory,
        read_identity=f"listener:{int(ctx.account)}",
        source_id=source_id,
        bulk_job_id=int(job["id"]),
        file_unique_id=media.get("file_unique_id"),
    )
    if created:
        ctx.notify(_receipt(task))
        return task
    if int(task.get("bulk_job_id") or 0) == int(job["id"]):
        return task
    return None


def _local_path(ctx, task: dict) -> Path:
    name = safe_file_name(str(task["file_name"]), f"video-{task['id']}.mp4")
    return Path(ctx.download_dir) / f"{int(task['id'])}-{name}"


def _add_bytes(db, job_id: int, message_id: int, nbytes: int) -> None:
    with db._lock:
        db._conn.execute(
            """UPDATE channel_bulk_items SET bytes_charged=bytes_charged+?, attempts=attempts+1
               WHERE job_id=? AND message_id=?""",
            (int(nbytes), int(job_id), int(message_id)),
        )


def _begin_download(db, task_id: int, part: str) -> None:
    state = db.get(task_id)["state"]
    if state == "download_failed":
        db.transition(task_id, "queued", local_path=None, downloaded_bytes=0, error=None)
        state = "queued"
    if state in {"downloaded", "waiting_upload"}:
        db.transition(task_id, "downloading", local_path=part, downloaded_bytes=0, error=None)
        return
    if state == "queued":
        db.transition(task_id, "reserved", local_path=part, downloaded_bytes=0, error=None)
        state = "reserved"
    if state == "reserved":
        db.transition(task_id, "downloading", local_path=part, downloaded_bytes=0, error=None)


async def _download(ctx, job, task, media, local: Path) -> None:
    part = Path(str(local) + ".part")
    _begin_download(ctx.db, int(task["id"]), str(part))
    part.parent.mkdir(parents=True, exist_ok=True)
    rate = ByteRate(int(job["controls"]["down_bps"]), monotonic=ctx.monotonic, sleep=ctx.sleep)
    payload = dict(media)
    payload["chat_id"] = int(job["chat_id"])
    await ctx.transport.download(item=payload, path=part, rate=rate, size=int(media["size"]))
    if part.stat().st_size != int(media["size"]):
        part.unlink(missing_ok=True)
        raise RuntimeError("下载大小不符")
    ctx.db.transition(int(task["id"]), "downloaded", local_path=str(local), downloaded_bytes=int(media["size"]))
    part.replace(local)
    ctx.db.transition(int(task["id"]), "waiting_upload", local_path=str(local), downloaded_bytes=int(media["size"]))
    fingerprint = sample_sha256(local)
    if fingerprint:
        remember(ctx.db, int(task["id"]), fingerprint)
        dup, prior = classify(
            ctx.db,
            chat_id=int(job["chat_id"]),
            message_id=int(media["message_id"]),
            content_fingerprint=fingerprint,
            exclude_id=int(task["id"]),
        )
        if prior is not None and dup in {"completed", "active"}:
            local.unlink(missing_ok=True)
            ctx.db.transition(
                int(task["id"]), "cancelled", local_path=None,
                error="内容指纹命中已有任务，已中止重复上传", wait_reason=None,
            )


async def _upload(ctx, job, task, media, local: Path, item: dict) -> str:
    if not local.is_file():
        pause(ctx.db, job["id"], now=ctx.now(), error="本地文件缺失，批量暂停")
        return "local"
    rate = ByteRate(int(job["controls"]["up_bps"]), monotonic=ctx.monotonic, sleep=ctx.sleep)
    remote = _remote_name(ctx.directory, task)
    if task["state"] == "waiting_upload":
        ctx.db.transition(int(task["id"]), "uploading", remote_temp_path=remote, error=None)
    elif task["state"] == "upload_failed_retained":
        ctx.db.transition(int(task["id"]), "waiting_upload", error=None)
        ctx.db.transition(int(task["id"]), "uploading", remote_temp_path=remote, error=None)
    await ctx.transport.upload(item=media, path=local, remote=remote, rate=rate, size=int(media["size"]))
    ctx.db.transition(int(task["id"]), "verifying")
    probe = await ctx.transport.probe(item=media, remote=remote, size=int(media["size"]))
    proven = (
        isinstance(probe, dict)
        and probe.get("cache_only") is False
        and int(probe.get("size", -1)) == int(media["size"])
    )
    if not proven:
        ctx.db.transition(
            int(task["id"]),
            "verification_failed_retained",
            local_path=str(local),
            remote_path=remote,
            error=safe_failure(RemoteReviewRequired("gateway cache is not acceptance")),
            failure_kind="remote_conflict",
            verification_method=None,
            verified_at=None,
            wait_reason="远端复核未证明文件已离开网关缓存；本地副本保留，未记成功",
        )
        _update_item(
            ctx.db, job["id"], item["message_id"], status="remote_review",
            last_error="cache_only" if not isinstance(probe, dict) or probe.get("cache_only") is not False else "size",
        )
        pause(ctx.db, job["id"], now=ctx.now(), error="远端复核未通过，本地保留")
        return "remote_review"
    ctx.db.transition(
        int(task["id"]),
        "cleanup_pending",
        remote_path=remote,
        remote_final_path=remote,
        remote_temp_path=None,
        uploaded_bytes=int(media["size"]),
        verification_method="remote_review",
        verified_at=time.time(),
        local_path=str(local),
        error=None,
        wait_reason=None,
    )
    local.unlink(missing_ok=True)
    ctx.db.transition(int(task["id"]), "completed", local_path=None, error=None, wait_reason=None)
    _update_item(ctx.db, job["id"], item["message_id"], status="done", last_error=None)
    return "continue"


def _remote_name(directory: str, task: dict) -> str:
    name = safe_file_name(str(task["file_name"]), f"task-{task['id']}.bin")
    path = Path(name)
    stamped = f"{path.stem} (task-{int(task['id'])}){path.suffix}"
    return f"{directory}/{stamped}" if directory else stamped


def _attempts(db, job_id: int, message_id: int) -> int:
    with db._lock:
        row = db._conn.execute(
            "SELECT attempts FROM channel_bulk_items WHERE job_id=? AND message_id=?",
            (int(job_id), int(message_id)),
        ).fetchone()
    return int(row[0]) if row else 0


def _io_failure(ctx, job, item, task, exc: Exception, *, stage: str) -> str:
    seconds = cooldown_seconds(exc)
    attempts = _attempts(ctx.db, job["id"], int(item["message_id"]))
    limit = int(job["controls"]["max_attempts"])
    text = safe_failure(exc) if failure_kind(exc) != "network" or seconds is not None else type(exc).__name__
    if stage == "download":
        part = Path(str(_local_path(ctx, task)) + ".part")
        part.unlink(missing_ok=True)
        current = ctx.db.get(int(task["id"]))
        if current and current["state"] == "downloading":
            ctx.db.transition(int(task["id"]), "download_failed", local_path=None, error=text, downloaded_bytes=0)
    else:
        current = ctx.db.get(int(task["id"]))
        if current and current["state"] in {"uploading", "verifying", "waiting_upload"}:
            ctx.db.transition(
                int(task["id"]), "upload_failed_retained",
                error=text, local_path=str(_local_path(ctx, task)), failure_kind=failure_kind(exc),
            )
    if attempts >= limit:
        _update_item(ctx.db, job["id"], item["message_id"], status="rejected", last_error=text)
        pause(ctx.db, job["id"], now=ctx.now(), error="重试次数已用尽")
        return "rejected"
    _update_item(ctx.db, job["id"], item["message_id"], status="pending", last_error=text)
    if seconds is not None:
        with ctx.db._lock:
            ctx.db._conn.execute(
                "UPDATE channel_bulk_jobs SET cooldown_until=? WHERE id=?",
                (ctx.now() + seconds, int(job["id"])),
            )
        pause(ctx.db, job["id"], now=ctx.now(), error=f"限流冷却 {seconds}s")
        return "cooldown"
    pause(ctx.db, job["id"], now=ctx.now(), error=text)
    return "failed"


def listener_busy(owner) -> bool:
    tasks = getattr(owner, "download_tasks", None) or {}
    db = getattr(owner, "db", None)
    if db is None:
        return False
    for task_id in list(tasks):
        row = db.get(int(task_id))
        if row and str(row.get("read_identity") or "bot") != "bot" and not row.get("bulk_job_id"):
            return True
    return False
