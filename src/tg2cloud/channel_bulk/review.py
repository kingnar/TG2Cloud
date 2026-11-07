"""Read captions and still thumbnails for messages already in an inventory.

Duration is not an advertisement label. This reader does not download the
document or video, does not post, and does not choose a caller-supplied path.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from tg2cloud.channel_bulk import inventory
from tg2cloud.channel_bulk.ledger import cooldown_seconds
from tg2cloud.channel_bulk.schema import ensure_db

MAX_IDS = 20
MAX_SECONDS = 120
MAX_TOTAL_BYTES = 4 * 1024 * 1024
PER_FILE_BYTES = 256 * 1024
CAPTION_LIMIT = 2000


def image_kind(data: bytes) -> str | None:
    if data.startswith(b"\xff\xd8\xff"):
        return "jpg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    return None


def _byte_size(thumb) -> int | None:
    if type(thumb).__name__ == "VideoSize":
        return None
    if hasattr(thumb, "bytes") and isinstance(thumb.bytes, (bytes, bytearray)):
        return len(thumb.bytes)
    sizes = getattr(thumb, "sizes", None)
    if isinstance(sizes, (list, tuple)) and sizes and all(type(item) is int for item in sizes):
        return max(sizes)
    value = getattr(thumb, "size", None)
    if type(value) is int and value >= 0:
        return value
    return None


def still_thumb(message):
    """Largest still thumbnail under the per-file cap, never a video size."""
    document = getattr(message, "document", None)
    photo = getattr(message, "photo", None)
    sizes = list(getattr(document, "thumbs", None) or getattr(photo, "sizes", None) or [])
    chosen = None
    chosen_size = -1
    for item in sizes:
        if type(item).__name__ == "VideoSize":
            continue
        nbytes = _byte_size(item)
        if nbytes is None or nbytes <= 0 or nbytes > PER_FILE_BYTES:
            continue
        if nbytes > chosen_size:
            chosen, chosen_size = item, nbytes
    return chosen


def caption_text(message) -> str:
    raw = getattr(message, "raw_text", None) or getattr(message, "message", None) or ""
    cleaned = []
    for char in str(raw):
        if char in "\n\t" or ord(char) >= 32:
            cleaned.append(char)
        if len(cleaned) >= CAPTION_LIMIT:
            break
    return "".join(cleaned)


def review_directory(data_dir: Path, inventory_id: int) -> Path:
    root = Path(data_dir)
    if root.is_symlink():
        raise PermissionError("data directory is a symlink")
    parent = root / "channel-bulk" / "review"
    if parent.is_symlink():
        raise PermissionError("review directory is a symlink")
    parent.mkdir(mode=0o700, exist_ok=True)
    os.chmod(parent, 0o700)
    directory = parent / f"inventory-{int(inventory_id)}"
    if directory.is_symlink():
        raise PermissionError("review target is a symlink")
    directory.mkdir(mode=0o700, exist_ok=True)
    os.chmod(directory, 0o700)
    resolved = directory.resolve()
    if root.resolve() not in resolved.parents:
        raise PermissionError("review target escaped the data directory")
    return directory


async def read_message(client, chat_id: int, message_id: int) -> dict:
    """Caption plus one still thumbnail. The full document is not requested."""
    from tg2cloud.listening.service import verified_chat

    entity = await verified_chat(client, int(chat_id))
    message = await client.get_messages(entity, ids=int(message_id))
    if not message:
        return {"caption": "", "thumb": None, "note": "missing-message"}
    thumb = still_thumb(message)
    if thumb is None:
        return {"caption": caption_text(message), "thumb": None, "note": "missing-thumbnail"}
    data = await client.download_media(message, bytes, thumb=thumb)
    if not isinstance(data, (bytes, bytearray)):
        return {"caption": caption_text(message), "thumb": None, "note": "missing-thumbnail"}
    return {"caption": caption_text(message), "thumb": bytes(data), "note": ""}


def _known(db, inventory_id: int) -> dict[int, dict]:
    return {int(item["message_id"]): item for item in inventory.items(db, inventory_id)}


def _flood(db, inventory_id: int, seconds: int, now: float) -> None:
    from tg2cloud.listening import account

    until = now + max(0, int(seconds))
    with db._lock:
        db._conn.execute(
            """UPDATE channel_inventory SET status='paused', cooldown_until=?,
               error='FloodWait', updated_at=? WHERE id=?""",
            (until, now, int(inventory_id)),
        )
    account.write_state(db, status="Telegram 限流冷却", cooldown_until=until)


async def collect(
    db,
    data_dir: Path,
    inventory_id: int,
    message_ids: list[int],
    read_one,
    *,
    now,
    sleep,
    max_seconds: int,
    max_total_bytes: int,
) -> dict:
    """Write a review manifest under the fixed directory. The result has no caption text."""
    if not 1 <= len(message_ids) <= MAX_IDS or len(set(message_ids)) != len(message_ids):
        raise ValueError("bad-request")
    if any(type(item) is not int or isinstance(item, bool) or item <= 0 for item in message_ids):
        raise ValueError("bad-request")
    if not 1 <= int(max_seconds) <= MAX_SECONDS:
        raise ValueError("bad-request")
    if not 1 <= int(max_total_bytes) <= MAX_TOTAL_BYTES:
        raise ValueError("bad-request")
    ensure_db(db)
    row = inventory.load(db, inventory_id)
    known = _known(db, inventory_id)
    if any(item not in known for item in message_ids):
        return {"ok": False, "error": "not-inventoried", "inventory_id": int(inventory_id)}
    moment = float(now())
    from tg2cloud.listening import account

    account_cooldown = float(account.state(db).get("cooldown_until") or 0)
    if float(row["cooldown_until"]) > moment or account_cooldown > moment:
        return {"ok": False, "error": "cooldown", "inventory_id": int(inventory_id)}
    directory = review_directory(data_dir, inventory_id)
    with db._lock:
        db._conn.execute(
            "UPDATE channel_inventory SET cancel_requested=0, updated_at=? WHERE id=?",
            (moment, int(inventory_id)),
        )
    started = moment
    total = 0
    records = []
    outcome = "complete"
    for index, message_id in enumerate(message_ids):
        if int(inventory.load(db, inventory_id)["cancel_requested"]):
            outcome = "cancelled"
            break
        if float(now()) - started > int(max_seconds):
            outcome = "bounded"
            break
        if index:
            await sleep(max(0.5, float(row["delay_seconds"])))
        try:
            piece = await read_one(int(row["chat_id"]), int(message_id))
        except Exception as exc:  # noqa: BLE001 - keep the failure class out of the manifest and logs
            seconds = cooldown_seconds(exc)
            if seconds is None:
                records.append(_record(known[message_id], caption="", thumb=None, note="read-failed"))
                continue
            _flood(db, inventory_id, seconds, float(now()))
            outcome = "flood"
            break
        caption = str(piece.get("caption") or "")[:CAPTION_LIMIT]
        blob = piece.get("thumb")
        note = str(piece.get("note") or "")
        filename = None
        thumb_bytes = 0
        if isinstance(blob, (bytes, bytearray)):
            kind = image_kind(bytes(blob))
            if kind is None:
                note = note or "rejected-type"
            elif len(blob) > PER_FILE_BYTES or total + len(blob) > int(max_total_bytes):
                outcome = "bounded"
                note = note or "byte-limit"
                records.append(_record(known[message_id], caption=caption, thumb=None, note=note))
                break
            else:
                filename = f"m{int(message_id)}.{kind}"
                target = directory / filename
                if target.is_symlink() or target.parent.resolve() != directory.resolve():
                    raise PermissionError("review file escaped the fixed directory")
                target.write_bytes(bytes(blob))
                os.chmod(target, 0o600)
                thumb_bytes = len(blob)
                total += thumb_bytes
        records.append(_record(known[message_id], caption=caption, thumb=filename, note=note, thumb_bytes=thumb_bytes))
    manifest = {
        "inventory_id": int(inventory_id),
        "classification": "unclassified",
        "duration_is_not_an_ad_label": True,
        "items": records,
    }
    path = directory / "review.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    os.chmod(path, 0o600)
    missing = sum(1 for item in records if not item["thumb"])
    return {
        "ok": True,
        "inventory_id": int(inventory_id),
        "reviewed": len(records),
        "missing": missing,
        "export": "review.json",
        "result": outcome,
    }


def _record(item: dict, *, caption: str, thumb: str | None, note: str, thumb_bytes: int = 0) -> dict:
    return {
        "message_id": int(item["message_id"]),
        "duration": item.get("duration"),
        "size": int(item["size"]),
        "group_id": str(item.get("group_id") or ""),
        "caption": caption,
        "thumb": thumb,
        "thumb_bytes": int(thumb_bytes),
        "note": note[:40],
    }


def reader_for(owner):
    async def read_one(chat_id: int, message_id: int):
        custom = getattr(owner, "channel_bulk_review", None)
        if custom is not None:
            return await custom(chat_id, message_id)
        listener = owner.listener

        async def operation(client):
            return await read_message(client, chat_id, message_id)

        return await listener.with_client(operation)

    return read_one
