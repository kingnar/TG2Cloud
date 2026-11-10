"""Duration buckets, review candidates, links, and manifest selection."""
from __future__ import annotations

import json
from typing import Any

EDGES = (5, 10, 15, 30)
DEFAULT_MIN_DURATION = 30


def message_link(chat_id: int, message_id: int) -> str:
    """Public t.me/c link from a numeric peer id. No stored usernames."""
    text = str(int(chat_id))
    internal = text[4:] if text.startswith("-100") else str(abs(int(chat_id)))
    if not internal.isdigit():
        raise ValueError("聊天 ID 无效")
    return f"https://t.me/c/{internal}/{int(message_id)}"


def review_only(duration: float | None, *, min_duration: int = DEFAULT_MIN_DURATION) -> bool:
    """Short and unknown durations are review candidates, not automatic transfers."""
    if duration is None:
        return True
    return float(duration) <= int(min_duration)


def summarize(items: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    """Cumulative <=5/10/15/30s counts overlap; a 4s item is in every bucket."""

    def stats(pred) -> dict[str, int]:
        rows = [item for item in items if pred(item)]
        return {"count": len(rows), "bytes": int(sum(int(item["size"]) for item in rows))}

    summary = {"videos": stats(lambda _item: True)}
    for edge in EDGES:
        summary[f"le_{edge}"] = stats(
            lambda item, edge=edge: item["duration"] is not None and float(item["duration"]) <= edge
        )
    summary["unknown"] = stats(lambda item: item["duration"] is None)
    summary["review_candidates"] = stats(
        lambda item: review_only(item["duration"], min_duration=DEFAULT_MIN_DURATION)
    )
    return summary


def selected(item: dict[str, Any], *, excluded: set[int], dedup: str, controls: dict) -> bool:
    if int(item["message_id"]) in excluded or dedup != "new":
        return False
    allow = {int(value) for value in controls.get("allow") or []}
    if int(item["message_id"]) in allow:
        return True
    if item["duration"] is None:
        return False
    return float(item["duration"]) > int(controls.get("min_duration", DEFAULT_MIN_DURATION))


def canonical_manifest(
    job_id: int,
    chat_id: int,
    alias: str,
    whitelist: list[str],
    controls: dict,
    ids: list[int],
    *,
    through: int | None = None,
) -> str:
    import hashlib

    payload = {
        "alias": alias,
        "allow": sorted(int(value) for value in controls.get("allow") or []),
        "chat_id": int(chat_id),
        "ids": list(ids),
        "job_id": int(job_id),
        "min_duration": int(controls.get("min_duration", DEFAULT_MIN_DURATION)),
        "whitelist": list(whitelist),
    }
    if through is not None:
        payload["through"] = int(through)
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()
