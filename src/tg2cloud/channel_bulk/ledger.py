"""Local byte budgets, operator quota attestation, rate cap, and cooldowns.

Daily and monthly counters are hard local stops. They are not a cloud invoice.
Each download attempt and each upload attempt charges the file size again so
retries count before a provider's deferred egress number moves.
Nothing here assumes a provider default such as a 10 TiB allotment.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone

from tg2cloud.domain.errors import failure_kind

DEFAULT_429_SECONDS = 900


class Refusal(Exception):
    def __init__(self, code: str, detail: str = ""):
        self.code = code
        super().__init__(detail or code)


class ByteRate:
    """Async byte cap. The clock and sleep are injected so tests do not wait.

    ``bytes_per_second`` may be a positive int, the exact string ``unlimited``,
    or a zero-argument callable returning one of those. ``0`` is never unlimited.
    A callable is read on each chunk so a running job can pick up a new limit.
    """

    def __init__(self, bytes_per_second, *, monotonic, sleep):
        self._limit = bytes_per_second if callable(bytes_per_second) else (lambda: bytes_per_second)
        self._monotonic = monotonic
        self._sleep = sleep
        self._next = None
        self._bps = None
        self._read_limit()

    def _read_limit(self):
        limit = self._limit()
        if limit == "unlimited":
            return None
        if type(limit) is not int or isinstance(limit, bool) or limit <= 0:
            raise Refusal("rate", "下载和上传速率必须是已配置的正数或明确的 unlimited")
        return limit

    async def consume(self, nbytes: int) -> None:
        if nbytes <= 0:
            return
        limit = self._read_limit()
        if limit is None:
            return
        if self._bps != limit:
            self._bps = limit
            self._next = None
        now = float(self._monotonic())
        if self._next is None or self._next < now:
            self._next = now
        self._next += nbytes / limit
        delay = self._next - now
        if delay > 0:
            await self._sleep(delay)


def period_keys(now: float) -> tuple[str, str]:
    moment = datetime.fromtimestamp(now, timezone.utc)
    return moment.strftime("%Y-%m-%d"), moment.strftime("%Y-%m")


def _ledger_get(connection, kind: str, key: str) -> int:
    row = connection.execute(
        "SELECT bytes FROM channel_byte_ledger WHERE period_kind=? AND period_key=?",
        (kind, key),
    ).fetchone()
    return int(row[0]) if row else 0


def _ledger_add(connection, kind: str, key: str, nbytes: int) -> None:
    connection.execute(
        """INSERT INTO channel_byte_ledger(period_kind, period_key, bytes) VALUES(?,?,?)
           ON CONFLICT(period_kind, period_key) DO UPDATE SET bytes=bytes+excluded.bytes""",
        (kind, key, int(nbytes)),
    )


def charge(db, nbytes: int, *, now: float, daily_cap: int, monthly_cap: int) -> bool:
    """Reserve nbytes against daily, monthly, and lifetime counters, or change nothing."""
    if nbytes <= 0 or daily_cap <= 0 or monthly_cap <= 0:
        return False
    day, month = period_keys(now)
    with db._lock:
        db._conn.execute("BEGIN IMMEDIATE")
        try:
            daily = _ledger_get(db._conn, "daily", day)
            monthly = _ledger_get(db._conn, "monthly", month)
            if daily + nbytes > daily_cap or monthly + nbytes > monthly_cap:
                db._conn.execute("ROLLBACK")
                return False
            _ledger_add(db._conn, "daily", day, nbytes)
            _ledger_add(db._conn, "monthly", month, nbytes)
            _ledger_add(db._conn, "total", "all", nbytes)
            db._conn.execute("COMMIT")
            return True
        except BaseException:
            db._conn.execute("ROLLBACK")
            raise


def totals(db, now: float) -> dict[str, int]:
    day, month = period_keys(now)
    with db._lock:
        return {
            "daily": _ledger_get(db._conn, "daily", day),
            "monthly": _ledger_get(db._conn, "monthly", month),
            "total": _ledger_get(db._conn, "total", "all"),
        }


def attest(db, *, remaining_bytes: int, period_end: float, now: float, note: str = "") -> None:
    if remaining_bytes < 0 or period_end <= now:
        raise Refusal("attestation", "剩余额度和周期结束时间需要管理员按实际账单填写")
    if len(note) > 80 or any(ord(char) < 32 for char in note):
        raise Refusal("attestation", "备注过长或含控制字符")
    with db._lock:
        total = _ledger_get(db._conn, "total", "all")
        db._conn.execute(
            """INSERT INTO channel_quota_attestation(id, remaining_bytes, period_end, attested_at, ledger_total, note)
               VALUES(1,?,?,?,?,?)
               ON CONFLICT(id) DO UPDATE SET
                 remaining_bytes=excluded.remaining_bytes,
                 period_end=excluded.period_end,
                 attested_at=excluded.attested_at,
                 ledger_total=excluded.ledger_total,
                 note=excluded.note""",
            (int(remaining_bytes), float(period_end), float(now), total, note),
        )


def attestation(db) -> dict | None:
    with db._lock:
        row = db._conn.execute("SELECT * FROM channel_quota_attestation WHERE id=1").fetchone()
    return dict(row) if row else None


def attested_remaining(db, now: float) -> int | None:
    """Provider remainder minus bytes charged since the attestation."""
    record = attestation(db)
    if record is None or float(record["period_end"]) <= now:
        return None
    spent = max(0, totals(db, now)["total"] - int(record["ledger_total"]))
    return int(record["remaining_bytes"]) - spent


def disk_allows(free_bytes: int, minimum_free: int, file_size: int) -> bool:
    return int(free_bytes) >= int(minimum_free) + int(file_size)


def cooldown_seconds(exc: BaseException) -> int | None:
    """FloodWait and HTTP 429. Missing Retry-After uses a fixed cooldown, not a bypass."""
    if type(exc).__name__ == "FloodWaitError":
        return max(0, int(getattr(exc, "seconds", 0)))
    if failure_kind(exc) != "rate_limited":
        return None
    match = re.search(r"retry-after\s*[:= ]\s*(\d+)", str(exc), re.IGNORECASE)
    if match:
        return max(0, int(match.group(1)))
    return DEFAULT_429_SECONDS
