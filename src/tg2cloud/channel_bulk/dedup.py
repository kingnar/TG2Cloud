"""Skip completed work in any directory, and anything still active.

Cancelled and failed rows are not treated as transferable duplicates and are
not reset. Bulk never sets a force flag.
"""
from __future__ import annotations

DONE = ("completed", "confirmed")
ACTIVE = (
    "queued",
    "reserved",
    "downloading",
    "streaming",
    "downloaded",
    "waiting_upload",
    "uploading",
    "verifying",
    "finalizing",
    "cleanup_pending",
)


def _has_column(db, name: str) -> bool:
    with db._lock:
        columns = {row[1] for row in db._conn.execute("PRAGMA table_info(tasks)")}
    return name in columns


def effective_dedup(stored: str | None, live: str) -> str:
    """Keep a reviewed non-new label when the task table still says new."""
    if live != "new":
        return live
    label = stored or "new"
    if label != "new":
        return label
    return "new"


def classify(
    db,
    *,
    chat_id: int,
    message_id: int,
    media_key: str | None = None,
    file_unique_id: str | None = None,
    content_fingerprint: str | None = None,
    exclude_id: int | None = None,
) -> tuple[str, dict | None]:
    """Return (new|completed|active|blocked, matching task)."""
    clauses = [
        "(chat_id=? AND message_id=?)",
        "(? IS NOT NULL AND media_key=?)",
    ]
    params: list = [int(chat_id), int(message_id), media_key, media_key]
    if _has_column(db, "file_unique_id"):
        clauses.append("(? IS NOT NULL AND file_unique_id IS NOT NULL AND file_unique_id=?)")
        params.extend([file_unique_id, file_unique_id])
    if content_fingerprint and _has_column(db, "content_fingerprint"):
        clauses.append(
            "(? IS NOT NULL AND content_fingerprint IS NOT NULL AND content_fingerprint=?)"
        )
        params.extend([content_fingerprint, content_fingerprint])
    sql = "SELECT * FROM tasks WHERE " + " OR ".join(clauses)
    with db._lock:
        rows = [dict(row) for row in db._conn.execute(sql, params)]
    rank = {"completed": 3, "active": 2, "blocked": 1}
    best: tuple[str, dict | None] = ("new", None)
    for row in rows:
        if exclude_id is not None and int(row["id"]) == int(exclude_id):
            continue
        state = row["state"]
        if state in DONE:
            kind = "completed"
        elif state in ACTIVE:
            kind = "active"
        else:
            kind = "blocked"
        if rank[kind] > rank.get(best[0], 0):
            best = (kind, row)
    return best
