"""Paginated metadata inventory on a caller-supplied fetch function.

Fetching is injected so this module never constructs a Telegram client.
A FloodWait stops the scan, stores the server delay, and keeps the checkpoint.
"""
from __future__ import annotations

import csv
import io
import json
from typing import Any

from tg2cloud.channel_bulk.dedup import classify
from tg2cloud.channel_bulk.ledger import cooldown_seconds
from tg2cloud.channel_bulk.policy import message_link, summarize
from tg2cloud.channel_bulk.schema import ensure_db

VIDEO_PREFIX = "video/"


def load(db, inventory_id: int) -> dict:
    return _row(db, inventory_id)


def _row(db, inventory_id: int) -> dict:
    with db._lock:
        row = db._conn.execute(
            "SELECT * FROM channel_inventory WHERE id=?", (int(inventory_id),)
        ).fetchone()
    if row is None:
        raise KeyError("盘点不存在")
    return dict(row)


def start(db, chat_id: int, *, page_size: int = 20, delay_seconds: float = 2.0, now: float) -> dict:
    """Record a paused inventory. This does not read Telegram."""
    if not isinstance(chat_id, int) or chat_id == 0:
        raise ValueError("聊天 ID 必须是非零整数")
    if not 1 <= int(page_size) <= 50:
        raise ValueError("每页条数必须在 1 到 50 之间")
    if not 0.5 <= float(delay_seconds) <= 60:
        raise ValueError("翻页间隔必须在 0.5 到 60 秒之间")
    ensure_db(db)
    with db._lock:
        db._conn.execute("BEGIN IMMEDIATE")
        try:
            cursor = db._conn.execute(
                """INSERT INTO channel_inventory(
                    chat_id, status, checkpoint_id, scanned, page_size, delay_seconds,
                    cooldown_until, created_at, updated_at
                ) VALUES(?, 'paused', 0, 0, ?, ?, 0, ?, ?)""",
                (int(chat_id), int(page_size), float(delay_seconds), now, now),
            )
            db._conn.execute("COMMIT")
            inventory_id = int(cursor.lastrowid)
        except BaseException:
            db._conn.execute("ROLLBACK")
            raise
    return _row(db, inventory_id)


def describe_message(message, chat_id: int) -> dict[str, Any] | None:
    """Metadata only. Returns None when the message is not a video."""
    document = getattr(message, "document", None)
    video = getattr(message, "video", None)
    file = getattr(message, "file", None)
    mime = ""
    size = 0
    duration = None
    if video is not None:
        duration = getattr(video, "duration", None)
        size = int(getattr(video, "size", 0) or 0)
        mime = str(getattr(video, "mime_type", "") or VIDEO_PREFIX)
    if document is not None:
        mime = mime or str(getattr(document, "mime_type", "") or "")
        size = size or int(getattr(document, "size", 0) or 0)
        for attr in getattr(document, "attributes", []) or []:
            if type(attr).__name__ == "DocumentAttributeVideo":
                duration = getattr(attr, "duration", duration)
                mime = mime or VIDEO_PREFIX
    if file is not None:
        mime = mime or str(getattr(file, "mime_type", "") or "")
        size = size or int(getattr(file, "size", 0) or 0)
    if not (mime.startswith(VIDEO_PREFIX) or video is not None):
        return None
    if duration is not None:
        duration = float(duration)
    media_key = None
    file_unique_id = None
    try:
        from tg2cloud.listening.rules import facts

        item = facts(message)
        media_key = item.get("media_key")
        file_unique_id = item.get("file_unique_id")
        if not size:
            size = int(item.get("size") or 0)
    except Exception:  # noqa: BLE001 - metadata identity is optional
        media_key = None
    name = ""
    if file is not None and getattr(file, "name", None):
        name = str(file.name)
    if not name:
        name = f"video-{int(message.id)}.mp4"
    return {
        "message_id": int(message.id),
        "link": message_link(chat_id, int(message.id)),
        "duration": duration,
        "size": int(size),
        "group_id": str(getattr(message, "grouped_id", "") or ""),
        "media_key": media_key,
        "file_unique_id": file_unique_id,
        "mime": mime[:120],
        "file_name": name[:180],
    }


def _store_item(db, inventory_id: int, chat_id: int, item: dict) -> None:
    kind, _task = classify(
        db,
        chat_id=chat_id,
        message_id=item["message_id"],
        media_key=item.get("media_key"),
        file_unique_id=item.get("file_unique_id"),
    )
    with db._lock:
        db._conn.execute(
            """INSERT INTO channel_inventory_items(
                inventory_id, message_id, link, duration, size, group_id, media_key,
                file_unique_id, mime, file_name, dedup
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(inventory_id, message_id) DO UPDATE SET
                link=excluded.link, duration=excluded.duration, size=excluded.size,
                group_id=excluded.group_id, media_key=excluded.media_key,
                file_unique_id=excluded.file_unique_id, mime=excluded.mime,
                file_name=excluded.file_name, dedup=excluded.dedup""",
            (
                inventory_id,
                item["message_id"],
                item["link"],
                item["duration"],
                item["size"],
                item["group_id"],
                item.get("media_key"),
                item.get("file_unique_id"),
                item["mime"],
                item["file_name"],
                kind,
            ),
        )


async def step(db, inventory_id: int, fetch, *, pages: int = 1, now, sleep) -> str:
    """Read up to `pages` metadata pages. `fetch(offset_id, limit)` returns messages."""
    if not 1 <= int(pages) <= 3:
        raise ValueError("单次最多翻 3 页")
    ensure_db(db)
    outcome = "paged"
    for index in range(int(pages)):
        inventory = _row(db, inventory_id)
        if inventory["status"] == "complete":
            return "complete"
        if float(inventory["cooldown_until"]) > float(now()):
            return "cooldown"
        try:
            batch = await fetch(int(inventory["checkpoint_id"]), int(inventory["page_size"]))
        except Exception as exc:
            seconds = cooldown_seconds(exc)
            if seconds is None:
                _mark(db, inventory_id, status="paused", error=type(exc).__name__, now=float(now()))
                raise
            _mark(
                db,
                inventory_id,
                status="paused",
                error=f"FloodWait {seconds}s",
                cooldown_until=float(now()) + seconds,
                now=float(now()),
            )
            return "flood"
        messages = [message for message in (batch or []) if message is not None and getattr(message, "id", None)]
        if not messages:
            _mark(db, inventory_id, status="complete", error=None, now=float(now()))
            return "complete"
        newest = min(int(message.id) for message in messages)
        previous = int(inventory["checkpoint_id"])
        if previous and newest >= previous:
            _mark(db, inventory_id, status="paused", error="checkpoint stalled", now=float(now()))
            return "stalled"
        for message in messages:
            item = describe_message(message, int(inventory["chat_id"]))
            if item is not None and item["size"] > 0:
                _store_item(db, inventory_id, int(inventory["chat_id"]), item)
        scanned = int(inventory["scanned"]) + len(messages)
        _mark(
            db,
            inventory_id,
            status="paused",
            checkpoint_id=newest,
            scanned=scanned,
            error=None,
            now=float(now()),
        )
        outcome = "paged"
        if index + 1 < int(pages):
            await sleep(float(inventory["delay_seconds"]))
    return outcome


def request_cancel(db, inventory_id: int, *, now: float) -> None:
    ensure_db(db)
    with db._lock:
        db._conn.execute(
            """UPDATE channel_inventory SET cancel_requested=1, status='paused',
               error='cancelled', updated_at=? WHERE id=?""",
            (now, int(inventory_id)),
        )


async def run_bounded(db, inventory_id: int, fetch, *, max_pages: int, max_seconds: float, now, sleep) -> str:
    """Page until complete, cooldown, cancel, or the caller bound. Does not open a client."""
    if not 1 <= int(max_pages) <= 200 or not 1 <= float(max_seconds) <= 600:
        raise ValueError("盘点运行界限无效")
    ensure_db(db)
    with db._lock:
        db._conn.execute(
            "UPDATE channel_inventory SET cancel_requested=0, updated_at=? WHERE id=?",
            (float(now()), int(inventory_id)),
        )
    started = float(now())
    pages = 0
    outcome = "bounded"
    while pages < int(max_pages) and float(now()) - started < float(max_seconds):
        current = _row(db, inventory_id)
        if int(current["cancel_requested"]):
            _mark(db, inventory_id, status="paused", error="cancelled", now=float(now()))
            return "cancelled"
        if float(current["cooldown_until"]) > float(now()):
            return "cooldown"
        outcome = await step(db, inventory_id, fetch, pages=1, now=now, sleep=sleep)
        pages += 1
        if outcome in {"complete", "flood", "stalled", "cooldown"}:
            return outcome
        if pages < int(max_pages) and float(now()) - started < float(max_seconds):
            delay = float(_row(db, inventory_id)["delay_seconds"])
            await sleep(delay)
    _mark(db, inventory_id, status="paused", error="bounded", now=float(now()))
    return "bounded"


_UNSET = object()


def _mark(db, inventory_id: int, *, status: str, now: float, checkpoint_id=None, scanned=None, error=_UNSET, cooldown_until=None) -> None:
    assignments = ["status=?", "updated_at=?"]
    values: list[Any] = [status, now]
    if checkpoint_id is not None:
        assignments.append("checkpoint_id=?")
        values.append(int(checkpoint_id))
    if scanned is not None:
        assignments.append("scanned=?")
        values.append(int(scanned))
    if error is not _UNSET:
        assignments.append("error=?")
        values.append(error)
    if cooldown_until is not None:
        assignments.append("cooldown_until=?")
        values.append(float(cooldown_until))
    values.append(int(inventory_id))
    with db._lock:
        db._conn.execute(
            f"UPDATE channel_inventory SET {', '.join(assignments)} WHERE id=?",
            values,
        )


def exclude(db, chat_id: int, message_id: int, *, reason: str = "review", now: float) -> None:
    """Remember an operator exclusion. Does not delete the source message."""
    if len(reason) > 80 or any(ord(char) < 32 for char in reason):
        raise ValueError("排除原因无效")
    ensure_db(db)
    with db._lock:
        db._conn.execute(
            """INSERT INTO channel_exclusions(chat_id, message_id, reason, created_at)
               VALUES(?,?,?,?)
               ON CONFLICT(chat_id, message_id) DO UPDATE SET reason=excluded.reason""",
            (int(chat_id), int(message_id), reason, now),
        )


def exclusions(db, chat_id: int) -> set[int]:
    ensure_db(db)
    with db._lock:
        rows = db._conn.execute(
            "SELECT message_id FROM channel_exclusions WHERE chat_id=?", (int(chat_id),)
        ).fetchall()
    return {int(row[0]) for row in rows}


def items(db, inventory_id: int) -> list[dict]:
    inventory = _row(db, inventory_id)
    with db._lock:
        rows = [
            dict(row)
            for row in db._conn.execute(
                "SELECT * FROM channel_inventory_items WHERE inventory_id=? ORDER BY message_id DESC",
                (int(inventory_id),),
            )
        ]
    for row in rows:
        kind, _task = classify(
            db,
            chat_id=int(inventory["chat_id"]),
            message_id=int(row["message_id"]),
            media_key=row.get("media_key"),
            file_unique_id=row.get("file_unique_id"),
        )
        row["dedup"] = kind
        row["duration"] = None if row["duration"] is None else float(row["duration"])
    return rows


def hold_running(db, now: float) -> int:
    """Startup must not continue an inventory scan."""
    ensure_db(db)
    with db._lock:
        cursor = db._conn.execute(
            """UPDATE channel_inventory SET status='paused',
               error='restart; explicit step required', updated_at=?
               WHERE status='running'""",
            (now,),
        )
        return int(cursor.rowcount)


def _cell(value: Any) -> str:
    text = "" if value is None else str(value)
    if text[:1] in {"=", "+", "-", "@"}:
        return "'" + text
    return text


def export_text(db, inventory_id: int, kind: str) -> str:
    rows = items(db, inventory_id)
    summary = summarize(rows)
    if kind == "json":
        return json.dumps({"summary": summary, "items": rows}, ensure_ascii=False, indent=2)
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["message_id", "link", "duration_seconds", "size_bytes", "group_id", "dedup", "media_key", "file_unique_id"])
    for row in rows:
        writer.writerow([
            _cell(row["message_id"]),
            _cell(row["link"]),
            _cell("" if row["duration"] is None else row["duration"]),
            _cell(row["size"]),
            _cell(row["group_id"]),
            _cell(row["dedup"]),
            _cell(row.get("media_key") or ""),
            _cell(row.get("file_unique_id") or ""),
        ])
    writer.writerow([])
    for name, value in summary.items():
        writer.writerow([name, value["count"], value["bytes"]])
    return buffer.getvalue()


def status_text(db, inventory_id: int) -> str:
    inventory = _row(db, inventory_id)
    rows = items(db, inventory_id)
    summary = summarize(rows)
    lines = [
        f"盘点 #{inventory['id']} 聊天 {inventory['chat_id']}",
        f"状态：{inventory['status']} 检查点：{inventory['checkpoint_id']} 已读消息：{inventory['scanned']}",
        f"视频 {summary['videos']['count']} 个，{summary['videos']['bytes']} 字节",
    ]
    for edge in (5, 10, 15, 30):
        bucket = summary[f"le_{edge}"]
        lines.append(f"<={edge}s：{bucket['count']} 个，{bucket['bytes']} 字节")
    unknown = summary["unknown"]
    lines.append(f"未知时长：{unknown['count']} 个，{unknown['bytes']} 字节")
    lines.append("短视频和未知时长只作为复核候选，不会自动进入转存。")
    if inventory.get("error"):
        lines.append(f"备注：{inventory['error']}")
    return "\n".join(lines)
