"""Sampled SHA-256 used before upload. Delegates to the deployment helper when present."""
from __future__ import annotations

import hashlib
import logging
from pathlib import Path

log = logging.getLogger("tg2cloud.channel_bulk")

CHUNK = 1024 * 1024
READ = 64 * 1024


def _sample(path: Path) -> str | None:
    size = path.stat().st_size
    if size <= 0:
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        if size <= 3 * CHUNK:
            while True:
                block = handle.read(READ)
                if not block:
                    break
                digest.update(block)
        else:
            for start in (0, max(0, (size - CHUNK) // 2), max(0, size - CHUNK)):
                handle.seek(start)
                left = CHUNK
                while left > 0:
                    block = handle.read(min(READ, left))
                    if not block:
                        break
                    digest.update(block)
                    left -= len(block)
    return digest.hexdigest()


def sample_sha256(path: str | Path) -> str | None:
    try:
        from tg2cloud.transfer.fingerprint import sample_sha256 as deployed
    except ImportError:
        deployed = _sample
    try:
        return deployed(path)
    except Exception as exc:  # noqa: BLE001 - fingerprint must not invent a transfer
        log.warning("采样指纹失败：%s", type(exc).__name__)
        return None


def remember(db, task_id: int, fingerprint: str) -> None:
    with db._lock:
        columns = {row[1] for row in db._conn.execute("PRAGMA table_info(tasks)")}
        if "content_fingerprint" not in columns:
            return
        db._conn.execute(
            "UPDATE tasks SET content_fingerprint=? WHERE id=?",
            (fingerprint, int(task_id)),
        )
