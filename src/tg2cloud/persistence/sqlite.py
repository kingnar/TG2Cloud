from __future__ import annotations

import json
import sqlite3
import threading
import time
from collections.abc import Iterable
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from tg2cloud.domain.errors import needs_review
from tg2cloud.domain.naming import safe_file_name
from tg2cloud.domain.states import (
    FAILED_STATES,
    GROWING_STATES,
    LOCAL_STATES,
    STATE_LABELS,
    can_transition,
)
from tg2cloud.persistence.migrations import SCHEMA_VERSION, migrate
from tg2cloud.persistence.private import private_sqlite


class TaskDB:
    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.RLock()
        private_sqlite(path, create=True)
        self._conn = sqlite3.connect(
            path, timeout=30, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        try:
            self._initialize()
        except BaseException:
            # Failed migrations and normal interrupts must not leave a live
            # connection to a rejected database or an isolated rehearsal copy.
            self._conn.close()
            raise

    def _initialize(self) -> None:
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.execute("PRAGMA busy_timeout=30000")
            if self._conn.execute("PRAGMA user_version").fetchone()[0] > SCHEMA_VERSION:
                raise RuntimeError("数据库版本高于当前程序；请恢复匹配的程序，不能降级打开")
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    sender_id INTEGER NOT NULL,
                    media_key TEXT,
                    file_name TEXT NOT NULL,
                    file_size INTEGER NOT NULL CHECK(file_size >= 0),
                    transfer_mode TEXT NOT NULL DEFAULT 'local',
                    state TEXT NOT NULL,
                    local_path TEXT,
                    remote_path TEXT,
                    downloaded_bytes INTEGER NOT NULL DEFAULT 0,
                    uploaded_bytes INTEGER NOT NULL DEFAULT 0,
                    download_retries INTEGER NOT NULL DEFAULT 0,
                    upload_retries INTEGER NOT NULL DEFAULT 0,
                    error TEXT,
                    wait_reason TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    next_retry_at REAL NOT NULL DEFAULT 0,
                    UNIQUE(chat_id, message_id)
                );
                CREATE INDEX IF NOT EXISTS idx_tasks_state_created
                    ON tasks(state, created_at);
                CREATE INDEX IF NOT EXISTS idx_tasks_retry
                    ON tasks(state, next_retry_at);
                """
            )
            columns = {
                row["name"]
                for row in self._conn.execute("PRAGMA table_info(tasks)").fetchall()
            }
            if "media_key" not in columns:
                self._conn.execute("ALTER TABLE tasks ADD COLUMN media_key TEXT")
            if "transfer_mode" not in columns:
                self._conn.execute(
                    "ALTER TABLE tasks ADD COLUMN transfer_mode TEXT "
                    "NOT NULL DEFAULT 'local'"
                )
            self._conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_sender_media
                ON tasks(sender_id, media_key)
                WHERE media_key IS NOT NULL
                """
            )
            migrate(self._conn, columns)
            from tg2cloud.channel_bulk.schema import ensure

            ensure(self._conn)


    def is_paused(self) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM service_meta WHERE key='paused'"
            ).fetchone()
            return bool(row and row[0] == "1")

    def family_users(self, admin_id: int) -> dict:
        from tg2cloud.persistence.users import read
        with self._lock:
            return read(self._conn, admin_id)

    def feature_settings(self) -> dict:
        from tg2cloud.persistence.features import read
        with self._lock:
            return read(self._conn)

    def quota_usage(self, user_id: int) -> dict:
        from tg2cloud.persistence.quotas import usage
        with self._lock:
            return usage(self._conn, user_id, time.time(), self.feature_settings()["timezone"])

    def admit_upload(self, task_id: int) -> bool:
        from tg2cloud.persistence.quotas import slot_available
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                task = self._conn.execute("SELECT sender_id,state,cancel_requested FROM tasks WHERE id=?", (task_id,)).fetchone()
                allowed = bool(task and not task["cancel_requested"] and task["state"] in {"downloaded", "waiting_upload", "cleanup_pending"})
                allowed = allowed and (task["state"] == "cleanup_pending" or slot_available(self._conn, task["sender_id"], task_id))
                if allowed and task["state"] != "cleanup_pending":
                    self._conn.execute("INSERT OR IGNORE INTO quota_slots(task_id) VALUES(?)", (task_id,))
                self._conn.execute("COMMIT")
                return allowed
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def change_family_user(self, admin_id: int, request: dict) -> dict:
        from tg2cloud.persistence.users import change
        with self._lock:
            return change(self._conn, admin_id, request)

    def remote_claimed(self, path: str, task_id: int) -> bool:
        with self._lock:
            return self.protected_remote(path) or self._conn.execute(
                "SELECT 1 FROM tasks WHERE remote_final_path=? AND id!=? "
                "AND state!='cancelled' LIMIT 1", (path, task_id)
            ).fetchone() is not None

    def protected_remote(self, path: str) -> bool:
        with self._lock:
            return self._conn.execute("SELECT 1 FROM retained_objects WHERE path=?", (path,)).fetchone() is not None

    def staging_name(self, task_id: int, safe_name: str) -> str:
        with self._lock:
            recovery = self._conn.execute("SELECT MAX(id) FROM task_recoveries WHERE task_id=?", (task_id,)).fetchone()[0]
        attempt = f"r{recovery}-" if recovery else ""
        return f".uploading-{task_id}-{attempt}{safe_name}"

    def set_paused(self, paused: bool) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO service_meta(key,value) VALUES('paused',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                ("1" if paused else "0",),
            )

    def events(self, task_id: int, limit: int = 8) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(row) for row in self._conn.execute(
                "SELECT old_state,new_state,happened_at FROM task_events "
                "WHERE task_id=? ORDER BY id DESC LIMIT ?", (task_id, limit)
            ).fetchall()]

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def requeue(
        self, task_id: int, *, sender_id: int, expected_updated_at: float,
        local_complete: bool, fresh_path: bool = False, restart_cancelled: bool = False,
        message_id: int | None = None,
    ) -> bool:
        """CAS the user-confirmed revision; preserve identity, dedup and attempt history."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
                task = dict(row) if row else None
                eligible = task and task["state"] in {
                    "download_failed", "upload_failed_retained", "verification_failed_retained",
                }
                if restart_cancelled:
                    eligible = task and task["state"] == "cancelled"
                if (not eligible or task["sender_id"] != sender_id
                        or task["updated_at"] != expected_updated_at
                        or task["cancel_requested"] or (needs_review(task) and not fresh_path)):
                    self._conn.execute("ROLLBACK")
                    return False
                previous = {key: task.get(key) for key in (
                    "state", "remote_path", "remote_temp_path", "remote_final_path",
                    "verification_method", "verified_at", "failure_kind",
                )}
                from tg2cloud.persistence.quotas import QuotaExceeded, admit
                try:
                    admit(self._conn, sender_id, task["file_size"], time.time(), self.feature_settings()["timezone"], task_id=task_id)
                except QuotaExceeded:
                    self._conn.execute("ROLLBACK")
                    return False
                now = time.time()
                if fresh_path:
                    for key in ("remote_path", "remote_temp_path", "remote_final_path"):
                        if task.get(key):
                            self._conn.execute("INSERT OR IGNORE INTO retained_objects(path,task_id) VALUES(?,?)",
                                               (task[key], task_id))
                self._conn.execute(
                    "INSERT INTO task_recoveries(task_id,happened_at,operation,previous) VALUES(?,?,?,?)",
                    (task_id, now, "restart" if restart_cancelled else "rename" if fresh_path else "retry",
                     json.dumps(previous, ensure_ascii=False)),
                )
                self._conn.execute(
                    """UPDATE tasks SET state=?, local_path=CASE WHEN ? THEN local_path ELSE NULL END,
                    downloaded_bytes=CASE WHEN ? THEN file_size ELSE 0 END,
                    uploaded_bytes=0, download_retries=0, upload_retries=0,
                    remote_path=CASE WHEN ? THEN NULL ELSE remote_path END,
                    remote_temp_path=CASE WHEN ? THEN NULL ELSE remote_temp_path END,
                    remote_final_path=CASE WHEN ? THEN NULL ELSE remote_final_path END,
                    failure_kind=NULL, verification_method=NULL, verified_at=NULL,
                    error=NULL, wait_reason='用户确认恢复；等待资源与目的端就绪',
                    next_retry_at=0, updated_at=?, message_id=COALESCE(?,message_id) WHERE id=?""",
                    ("waiting_upload" if local_complete else "queued", local_complete, local_complete,
                     fresh_path, fresh_path, fresh_path, now,
                     message_id if task.get("read_identity", "bot") == "bot" else None, task_id),
                )
                self._conn.execute("COMMIT")
                return True
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def defer_listener_download(self, task_id: int, *, sender_id: int, expected_updated_at: float,
                                retry_after_seconds: int, maximum_attempts: int) -> bool:
        """CAS a local pre-upload attempt into its existing durable cooldown queue."""
        if (type(retry_after_seconds) is not int or not 5 <= retry_after_seconds <= 60
                or type(maximum_attempts) is not int or not 1 <= maximum_attempts <= 3):
            return False
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
                if (not row or row["state"] not in {"reserved", "downloading"}
                        or row["transfer_mode"] != "local" or row["cancel_requested"]
                        or row["sender_id"] != sender_id or row["updated_at"] != expected_updated_at
                        or not str(row["read_identity"]).startswith("listener:")
                        or any(row[key] for key in ("remote_path", "remote_temp_path", "remote_final_path"))
                        or row["download_retries"] + 1 >= maximum_attempts):
                    self._conn.execute("ROLLBACK")
                    return False
                account_row = self._conn.execute("SELECT value FROM service_meta WHERE key='listener_account'").fetchone()
                try:
                    account = json.loads(account_row[0]) if account_row and len(account_row[0]) <= 4096 else {}
                except (ValueError, TypeError):
                    account = {}
                source = self._conn.execute(
                    "SELECT account,chat,deleted FROM listener_sources WHERE id=?", (row["source_id"],),
                ).fetchone()
                if (not isinstance(account, dict) or account.get("enabled") is not True
                        or type(account.get("account")) is not int
                        or row["read_identity"] != "listener:" + str(account["account"])
                        or not source or source["deleted"] or source["account"] != account["account"]
                        or source["chat"] != row["chat_id"]):
                    self._conn.execute("ROLLBACK")
                    return False
                now = time.time()
                self._conn.execute(
                    """UPDATE tasks SET state='queued',local_path=NULL,downloaded_bytes=0,
                    download_retries=download_retries+1,error=NULL,failure_kind='rate_limited',
                    wait_reason='Telegram 下载限流，等待冷却后有限恢复',next_retry_at=?,updated_at=?
                    WHERE id=?""", (now + retry_after_seconds, now, task_id),
                )
                # Existing triggers record the state event and release active quota.
                self._conn.execute("COMMIT")
                return True
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def search(self, *, sender_id: int, query: str = "", state: str = "", after_id: int = 0,
               limit: int = 6) -> list[dict[str, Any]]:
        if state and state not in STATE_LABELS and state != "failed":
            raise ValueError("未知任务状态")
        literal = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        states = sorted(FAILED_STATES) if state == "failed" else [state]
        placeholders = ",".join("?" for _ in states)
        # Only the fixed count of parameter placeholders is interpolated.
        sql = ("SELECT * FROM tasks WHERE sender_id=? "  # nosec B608
               f"AND (?='' OR state IN ({placeholders})) "
               "AND (?='' OR file_name LIKE ? ESCAPE '\\') AND (?=0 OR id<?) "
               "ORDER BY id DESC LIMIT ?")
        with self._lock:
            return [dict(row) for row in self._conn.execute(
                sql,
                (sender_id, state, *states, query, f"%{literal}%", after_id, after_id,
                 max(1, min(limit, 50))),
            ).fetchall()]

    @staticmethod
    def _dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
        return dict(row) if row is not None else None

    def create_task(
        self,
        *,
        chat_id: int,
        message_id: int,
        sender_id: int,
        media_key: str | None = None,
        file_name: str,
        file_size: int,
        original_file_name: str | None = None,
        media_kind: str = "", mime_type: str = "", media_extension: str = "",
        target_base: str | None = None, created_at: float | None = None,
        classification_options: dict | None = None,
        directory_override: str | None = None, read_identity: str = "bot", source_id: int | None = None,
        file_unique_id: str | None = None, bulk_job_id: int | None = None,
    ) -> tuple[dict[str, Any], bool]:
        now = time.time() if created_at is None else created_at
        with self._lock:
            nested = self._conn.in_transaction
            self._conn.execute("SAVEPOINT create_task" if nested else "BEGIN IMMEDIATE")
            try:
                # Resolve duplicates before any quota/path decision; repeated
                # forwards must return the original persisted task unchanged.
                row = self._conn.execute(
                    "SELECT * FROM tasks WHERE (chat_id=? AND message_id=?) "
                    "OR (sender_id=? AND media_key=? AND ? IS NOT NULL) "
                    "OR (file_unique_id IS NOT NULL AND ? IS NOT NULL AND file_unique_id=?) LIMIT 1",
                    (chat_id, message_id, sender_id, media_key, media_key,
                     file_unique_id, file_unique_id)).fetchone()
                if row:
                    self._conn.execute("RELEASE create_task" if nested else "COMMIT")
                    return dict(row), False
                from tg2cloud.domain.classification import directory, safe_directory
                from tg2cloud.persistence.quotas import admit
                admit(self._conn, sender_id, file_size, now, self.feature_settings()["timezone"])
                target_directory = directory(classification_options or self.feature_settings(), now, kind=media_kind,
                                             mime=mime_type, extension=media_extension)
                if directory_override is not None:
                    target_directory = safe_directory(directory_override)
                if read_identity != "bot" and (not read_identity.startswith("listener:") or source_id is None):
                    raise ValueError("读取身份无效")
                cursor = self._conn.execute(
                    """
                    INSERT INTO tasks (
                        chat_id, message_id, sender_id, media_key, file_name,
                        file_size, state, created_at, updated_at, original_file_name,
                        target_directory, target_base, read_identity, source_id,
                        file_unique_id, bulk_job_id
                    ) VALUES (?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        chat_id,
                        message_id,
                        sender_id,
                        media_key,
                        file_name,
                        file_size,
                        now,
                        now,
                        original_file_name,
                        target_directory, target_base, read_identity, source_id,
                        file_unique_id, bulk_job_id,
                    ),
                )
                row = self._conn.execute(
                    "SELECT * FROM tasks WHERE id = ?", (cursor.lastrowid,)
                ).fetchone()
                self._conn.execute("INSERT INTO quota_ledger(task_id,user_id,bytes) VALUES(?,?,?)",
                                   (cursor.lastrowid, sender_id, file_size))
                self._conn.execute("RELEASE create_task" if nested else "COMMIT")
                return dict(row), True
            except sqlite3.IntegrityError:
                self._conn.execute("ROLLBACK TO create_task" if nested else "ROLLBACK")
                if nested:
                    self._conn.execute("RELEASE create_task")
                row = self._conn.execute(
                    "SELECT * FROM tasks WHERE chat_id = ? AND message_id = ?",
                    (chat_id, message_id),
                ).fetchone()
                if row is None and media_key:
                    row = self._conn.execute(
                        """
                        SELECT * FROM tasks
                        WHERE sender_id = ? AND media_key = ?
                        """,
                        (sender_id, media_key),
                    ).fetchone()
                if row is None and file_unique_id:
                    row = self._conn.execute(
                        "SELECT * FROM tasks WHERE file_unique_id=? LIMIT 1",
                        (file_unique_id,),
                    ).fetchone()
                if row is None:
                    raise
                return dict(row), False
            except BaseException:
                self._conn.execute("ROLLBACK TO create_task" if nested else "ROLLBACK")
                if nested:
                    self._conn.execute("RELEASE create_task")
                raise

    def get(self, task_id: int) -> dict[str, Any] | None:
        with self._lock:
            return self._dict(
                self._conn.execute(
                    "SELECT * FROM tasks WHERE id = ?", (task_id,)
                ).fetchone()
            )

    def has_message(self, chat_id: int, message_id: int) -> bool:
        """Check an exact received message, not a different forward of its media."""
        with self._lock:
            return self._conn.execute(
                "SELECT 1 FROM tasks WHERE chat_id=? AND message_id=? LIMIT 1",
                (chat_id, message_id),
            ).fetchone() is not None

    def list_states(
        self,
        states: Iterable[str],
        *,
        limit: int = 200,
        ready_only: bool = False,
        after_id: int = 0,
        sender_id: int | None = None,
    ) -> list[dict[str, Any]]:
        state_values = tuple(states)
        if not state_values:
            return []
        placeholders = ",".join("?" for _ in state_values)
        # placeholders are generated only from the tuple length; values stay bound.
        query = f"SELECT * FROM tasks WHERE state IN ({placeholders})"  # nosec B608
        params: list[Any] = list(state_values)
        if sender_id is not None:
            query += " AND sender_id = ?"
            params.append(sender_id)
        if after_id > 0:
            query += " AND id > ?"
            params.append(after_id)
        if ready_only:
            query += " AND next_retry_at <= ? AND cancel_requested=0"
            params.append(time.time())
        query += " ORDER BY id ASC LIMIT ?"
        params.append(limit)
        with self._lock:
            return [
                dict(row)
                for row in self._conn.execute(query, params).fetchall()
            ]

    def list_recent(self, limit: int = 20, offset: int = 0, *, sender_id: int | None = None) -> list[dict[str, Any]]:
        if limit <= 0 or offset < 0:
            raise ValueError("limit 必须大于 0，offset 不能小于 0")
        with self._lock:
            return [
                dict(row)
                for row in self._conn.execute(
                    "SELECT * FROM tasks WHERE (? IS NULL OR sender_id=?) ORDER BY id DESC LIMIT ? OFFSET ?",
                    (sender_id, sender_id, limit, offset),
                ).fetchall()
            ]

    def counts(self, *, sender_id: int | None = None, states: Iterable[str] | None = None) -> dict[str, int]:
        query = "SELECT state, COUNT(*) AS count FROM tasks"
        conditions = []
        params: list[Any] = []
        if sender_id is not None:
            conditions.append("sender_id=?")
            params.append(sender_id)
        if states is not None:
            values = tuple(states)
            if not values:
                return {}
            conditions.append("state IN (" + ",".join("?" for _ in values) + ")")
            params.extend(values)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " GROUP BY state"
        with self._lock:
            return {row["state"]: row["count"]
                    for row in self._conn.execute(query, params).fetchall()}

    def count_states(self, states: Iterable[str]) -> int:
        """Count only relevant states using the existing state index, without caching."""
        values = tuple(states)
        if not values:
            return 0
        placeholders = ",".join("?" for _ in values)
        with self._lock:
            return int(self._conn.execute(
                f"SELECT COUNT(*) FROM tasks WHERE state IN ({placeholders})",  # nosec B608
                values,
            ).fetchone()[0])

    def set_wait_reason(self, task_id: int, reason: str, *, states: Iterable[str]) -> bool:
        """Persist a changed wait reason only while the task is still eligible.

        The read avoids acquiring a writer lock on unchanged waits. The UPDATE
        repeats the state/cancellation guard for writers on other connections.
        reserve() calls this inside its existing BEGIN IMMEDIATE transaction.
        """
        with self._lock:
            task = self._conn.execute(
                "SELECT state,cancel_requested,wait_reason FROM tasks WHERE id=?", (task_id,)
            ).fetchone()
            if (task is None or task["state"] not in states or task["cancel_requested"]
                    or task["wait_reason"] == reason):
                return False
            return self._conn.execute(
                "UPDATE tasks SET wait_reason=?,updated_at=? WHERE id=? AND state=? "
                "AND cancel_requested=0 AND wait_reason IS NOT ?",
                (reason, time.time(), task_id, task["state"], reason),
            ).rowcount == 1

    def weekly_failed_count(self, *, now: float | None = None, sender_id: int | None = None) -> int:
        """Current failed tasks that failed this local calendar week, counted once.

        This is a display filter, never a task deletion or state reset. Legacy
        rows without failure events use their last recorded state timestamp.
        """
        now = time.time() if now is None else now
        local = time.localtime(now)
        today = date(local.tm_year, local.tm_mon, local.tm_mday)
        monday = today - timedelta(days=today.weekday())
        # mktime with isdst=-1 resolves the offset at Monday, not today's offset.
        start = time.mktime((monday.year, monday.month, monday.day, 0, 0, 0, 0, 0, -1))
        placeholders = ",".join("?" for _ in FAILED_STATES)
        states = sorted(FAILED_STATES)
        # Only placeholder count is interpolated; timestamps/states are bound.
        query = f"""SELECT COUNT(*) FROM tasks t WHERE t.state IN ({placeholders})
            AND (EXISTS (SELECT 1 FROM task_events e WHERE e.task_id=t.id
                         AND e.new_state IN ({placeholders}) AND e.happened_at>=? AND e.happened_at<=?)
                 OR (t.updated_at>=? AND t.updated_at<=? AND NOT EXISTS
                     (SELECT 1 FROM task_events e WHERE e.task_id=t.id AND e.new_state IN ({placeholders}))))"""  # nosec B608
        with self._lock:
            return self._conn.execute(query + " AND (? IS NULL OR t.sender_id=?)", (*states, *states, start, now,
                                              start, now, *states, sender_id, sender_id)).fetchone()[0]

    def used_local_bytes(self) -> int:
        placeholders = ",".join("?" for _ in LOCAL_STATES)
        local_sum_query = (
            "SELECT COALESCE(SUM(file_size), 0) AS total "  # nosec B608
            f"FROM tasks WHERE state IN ({placeholders}) "
            "AND transfer_mode != 'stream'"
        )
        with self._lock:
            row = self._conn.execute(
                local_sum_query,
                LOCAL_STATES,
            ).fetchone()
            return int(row["total"])

    def reserve(
        self,
        task_id: int,
        *,
        budget_bytes: int,
        current_free_bytes: int,
        minimum_free_bytes: int,
    ) -> tuple[bool, str]:
        placeholders = ",".join("?" for _ in LOCAL_STATES)
        local_sum_query = (
            "SELECT COALESCE(SUM(file_size), 0) AS total "  # nosec B608
            f"FROM tasks WHERE state IN ({placeholders}) "
            "AND transfer_mode != 'stream'"
        )
        growth_placeholders = ",".join("?" for _ in GROWING_STATES)
        growth_query = (
            "SELECT COALESCE(SUM(CASE "  # nosec B608
            "WHEN file_size > downloaded_bytes "
            "THEN file_size - downloaded_bytes ELSE 0 END), 0) AS total "
            f"FROM tasks WHERE state IN ({growth_placeholders}) "
            "AND transfer_mode != 'stream'"
        )
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                task = self._conn.execute(
                    "SELECT * FROM tasks WHERE id = ?", (task_id,)
                ).fetchone()
                if (task is None or task["state"] != "queued" or task["cancel_requested"]
                        or task["next_retry_at"] > time.time()):
                    self._conn.execute("ROLLBACK")
                    return False, "任务状态已经改变"
                from tg2cloud.persistence.quotas import slot_available
                if not slot_available(self._conn, task["sender_id"], task_id):
                    self.set_wait_reason(task_id, "等待个人并发额度", states=("queued",))
                    self._conn.execute("COMMIT")
                    return False, "等待个人并发额度"
                row = self._conn.execute(
                    local_sum_query,
                    LOCAL_STATES,
                ).fetchone()
                used = int(row["total"])
                growth_row = self._conn.execute(
                    growth_query,
                    GROWING_STATES,
                ).fetchone()
                pending_growth = int(growth_row["total"])
                size = int(task["file_size"])
                requested_stream = task["transfer_mode"] == "stream"
                if requested_stream or size > budget_bytes:
                    if current_free_bytes <= minimum_free_bytes:
                        reason = "等待真实磁盘空间恢复后使用流式模式"
                        self.set_wait_reason(task_id, reason, states=("queued",))
                        self._conn.execute("COMMIT")
                        return False, reason
                    self._conn.execute(
                        """
                        UPDATE tasks
                        SET state = 'reserved', transfer_mode = 'stream',
                            wait_reason = NULL, error = NULL, failure_kind=NULL, next_retry_at=0, updated_at = ?
                        WHERE id = ?
                        """,
                        (time.time(), task_id),
                    )
                    self._conn.execute("COMMIT")
                    return True, (
                        "已按用户选择使用流式模式"
                        if requested_stream
                        else "单文件超过本地预算，已切换流式模式"
                    )
                if used + size > budget_bytes:
                    reason = "等待本地临时空间额度"
                    self.set_wait_reason(task_id, reason, states=("queued",))
                    self._conn.execute("COMMIT")
                    return False, reason
                if (
                    current_free_bytes - pending_growth - size
                    < minimum_free_bytes
                ):
                    shortfall = minimum_free_bytes + pending_growth + size - current_free_bytes
                    reason = (
                        "等待真实磁盘空间（至少还需释放 "
                        f"{shortfall / 1024**3:.2f}GB；可用 /stream #{task_id} 改为流式）"
                    )
                    self.set_wait_reason(task_id, reason, states=("queued",))
                    self._conn.execute("COMMIT")
                    return False, reason
                self._conn.execute(
                    """
                    UPDATE tasks
                    SET state = 'reserved', transfer_mode = 'local',
                        wait_reason = NULL, error = NULL, failure_kind=NULL, next_retry_at=0, updated_at = ?
                    WHERE id = ?
                    """,
                    (time.time(), task_id),
                )
                self._conn.execute("COMMIT")
                return True, ""
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def update(self, task_id: int, **fields: Any) -> None:
        if "state" in fields and fields["state"] not in STATE_LABELS:
            raise ValueError("未知任务状态")
        if "transfer_mode" in fields and fields["transfer_mode"] not in {"local", "stream"}:
            raise ValueError("未知传输模式")
        allowed = {
            "state",
            "failure_kind", "verification_method", "verified_at",
            "transfer_mode",
            "local_path",
            "remote_path",
            "remote_temp_path",
            "remote_final_path",
            "cancel_requested",
            "downloaded_bytes",
            "uploaded_bytes",
            "download_retries",
            "upload_retries",
            "error",
            "wait_reason",
            "next_retry_at",
        }
        unknown = set(fields) - allowed
        if unknown:
            raise ValueError(f"不允许更新字段：{sorted(unknown)}")
        fields["updated_at"] = time.time()
        assignments = ", ".join(f"{key} = ?" for key in fields)
        values = list(fields.values()) + [task_id]
        with self._lock:
            self._conn.execute(
                # assignments contains names from the fixed allowed set above.
                f"UPDATE tasks SET {assignments} WHERE id = ?",  # nosec B608
                values,
            )

    def transition(self, task_id: int, new_state: str, **fields: Any) -> None:
        """Atomically validate and persist one legal state transition."""
        if new_state not in STATE_LABELS:
            raise ValueError("未知任务状态")
        if "state" in fields:
            raise ValueError("状态必须通过 new_state 参数指定")
        allowed = {
            "failure_kind", "verification_method", "verified_at",
            "transfer_mode", "local_path", "remote_path", "remote_temp_path",
            "remote_final_path", "cancel_requested", "downloaded_bytes",
            "uploaded_bytes", "download_retries", "upload_retries", "error",
            "wait_reason", "next_retry_at",
        }
        unknown = set(fields) - allowed
        if unknown:
            raise ValueError(f"不允许更新字段：{sorted(unknown)}")
        if "transfer_mode" in fields and fields["transfer_mode"] not in {"local", "stream"}:
            raise ValueError("未知传输模式")
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT state FROM tasks WHERE id=?", (task_id,)
                ).fetchone()
                if row is None:
                    raise KeyError(f"任务 #{task_id} 不存在")
                old_state = str(row["state"])
                if not can_transition(old_state, new_state):
                    raise ValueError(f"非法任务状态迁移：{old_state} -> {new_state}")
                values_by_name = {"state": new_state, **fields, "updated_at": time.time()}
                assignments = ", ".join(f"{key} = ?" for key in values_by_name)
                values = [*values_by_name.values(), task_id, old_state]
                cursor = self._conn.execute(
                    # assignments contains names from the fixed set above.
                    f"UPDATE tasks SET {assignments} WHERE id=? AND state=?",  # nosec B608
                    values,
                )
                if cursor.rowcount != 1:
                    raise RuntimeError("任务状态在迁移期间发生变化")
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def request_stream(self, task_id: int) -> tuple[str, dict[str, Any] | None]:
        """Switch an idle/retryable download to explicit streaming mode."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT * FROM tasks WHERE id=?", (task_id,)
                ).fetchone()
                if row is None:
                    self._conn.execute("ROLLBACK")
                    return "missing", None
                task = dict(row)
                if task["cancel_requested"] or task["state"] not in {"queued", "download_failed"}:
                    self._conn.execute("ROLLBACK")
                    return "invalid", task
                if task.get("local_path"):
                    self._conn.execute("ROLLBACK")
                    return "retained", task
                if task["state"] == "download_failed" and not can_transition(
                    "download_failed", "queued"
                ):
                    raise ValueError("状态机不允许失败任务重新排队")
                now = time.time()
                self._conn.execute(
                    """UPDATE tasks SET state='queued', transfer_mode='stream',
                    downloaded_bytes=0, uploaded_bytes=0, download_retries=0,
                    upload_retries=0, error=NULL,
                    wait_reason='用户已选择流式模式；等待资源和目的端就绪',
                    next_retry_at=0, updated_at=? WHERE id=?""",
                    (now, task_id),
                )
                updated = self._conn.execute(
                    "SELECT * FROM tasks WHERE id=?", (task_id,)
                ).fetchone()
                self._conn.execute("COMMIT")
                return "updated", dict(updated)
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def confirm_115(
        self, task_id: int
    ) -> tuple[str, dict[str, Any] | None]:
        """Atomically record a user's official-client confirmation.

        Only a completed Bot transfer can be confirmed. This prevents an
        accidental /confirm from changing a task that is still transferring.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT * FROM tasks WHERE id = ?", (task_id,)
                ).fetchone()
                if row is None:
                    self._conn.execute("ROLLBACK")
                    return "missing", None
                if row["state"] == "confirmed":
                    self._conn.execute("ROLLBACK")
                    return "already", dict(row)
                if row["state"] != "completed":
                    self._conn.execute("ROLLBACK")
                    return "invalid", dict(row)
                self._conn.execute(
                    """
                    UPDATE tasks
                    SET state='confirmed', error=NULL, wait_reason=NULL,
                        next_retry_at=0, updated_at=?
                    WHERE id=? AND state='completed'
                    """,
                    (time.time(), task_id),
                )
                updated = self._conn.execute(
                    "SELECT * FROM tasks WHERE id = ?", (task_id,)
                ).fetchone()
                self._conn.execute("COMMIT")
                return "confirmed", dict(updated)
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def confirm_completed(self, limit: int = 100, *, sender_id: int | None = None) -> list[int]:
        """Atomically batch-confirm recent completed transfers."""
        safe_limit = max(1, min(int(limit), 100))
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                rows = self._conn.execute(
                    "SELECT id FROM tasks WHERE state='completed' AND (? IS NULL OR sender_id=?) ORDER BY id DESC LIMIT ?",
                    (sender_id, sender_id, safe_limit),
                ).fetchall()
                task_ids = [int(row["id"]) for row in rows]
                if task_ids:
                    placeholders = ",".join("?" for _ in task_ids)
                    self._conn.execute(
                        # placeholders are generated from the bounded ID list.
                        f"""UPDATE tasks SET state='confirmed', error=NULL,
                        wait_reason=NULL, next_retry_at=0, updated_at=?
                        WHERE state='completed' AND id IN ({placeholders})""",  # nosec B608
                        (time.time(), *task_ids),
                    )
                self._conn.execute("COMMIT")
                return task_ids
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def recover(self, download_dir: Path, *, task_id: int | None = None) -> dict[str, int]:
        recovered = {"queued": 0, "waiting_upload": 0, "failed": 0}
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM tasks
                WHERE state IN (
                    'reserved', 'downloading', 'downloaded', 'waiting_upload',
                    'uploading', 'verifying', 'finalizing', 'streaming',
                    'upload_failed_retained', 'verification_failed_retained'
                )
                AND (? IS NULL OR id=?)
                """, (task_id, task_id),
            ).fetchall()
            for row in rows:
                if row["cancel_requested"]:
                    continue
                if needs_review(dict(row)) or row["failure_kind"] in {"authentication", "permission", "rate_limited"}:
                    # A restart must not retry a MOVE whose result is unknown.
                    recovered["failed"] += 1
                    continue
                task_id = int(row["id"])
                local_path = Path(row["local_path"]) if row["local_path"] else None
                if row["transfer_mode"] == "stream":
                    self._conn.execute(
                        """
                        UPDATE tasks SET state='queued', local_path=NULL,
                            downloaded_bytes=0, upload_retries=0,
                            wait_reason='流式任务在服务重启后自动重新排队',
                            next_retry_at=0, updated_at=?
                        WHERE id=?
                        """,
                        (time.time(), task_id),
                    )
                    recovered["queued"] += 1
                    continue
                if (
                    local_path
                    and local_path.is_file()
                    and not str(local_path).endswith(".part")
                    and local_path.stat().st_size == int(row["file_size"])
                ):
                    self._conn.execute(
                        """
                        UPDATE tasks SET state='waiting_upload', error=NULL,
                            upload_retries=0, next_retry_at=0,
                            wait_reason='服务重启后恢复上传', updated_at=?
                        WHERE id=?
                        """,
                        (time.time(), task_id),
                    )
                    recovered["waiting_upload"] += 1
                    continue
                expected_final = download_dir / (
                    f"{task_id}-"
                    f"{safe_file_name(row['file_name'], str(task_id))}"
                )
                if (
                    expected_final.is_file()
                    and expected_final.stat().st_size == int(row["file_size"])
                ):
                    self._conn.execute(
                        """
                        UPDATE tasks SET state='waiting_upload',
                            local_path=?, downloaded_bytes=file_size,
                            error=NULL, upload_retries=0, next_retry_at=0,
                            wait_reason='已恢复下载完成但未登记的本地文件',
                            updated_at=?
                        WHERE id=?
                        """,
                        (str(expected_final), time.time(), task_id),
                    )
                    recovered["waiting_upload"] += 1
                    continue
                if row["state"] in {
                    "upload_failed_retained",
                    "verification_failed_retained",
                }:
                    self._conn.execute(
                        """
                        UPDATE tasks SET state='queued', local_path=NULL,
                            downloaded_bytes=0, download_retries=0,
                            upload_retries=0,
                            wait_reason='保留记录对应的本地文件已不存在，自动重新排队',
                            next_retry_at=0, updated_at=?
                        WHERE id=?
                        """,
                        (time.time(), task_id),
                    )
                    recovered["queued"] += 1
                    continue
                part_path = download_dir / f"{task_id}.part"
                if part_path.exists():
                    try:
                        part_path.unlink()
                    except OSError:
                        self._conn.execute(
                            """
                            UPDATE tasks SET state='download_failed',
                                error='重启后无法清理不完整文件', updated_at=?
                            WHERE id=?
                            """,
                            (time.time(), task_id),
                        )
                        recovered["failed"] += 1
                        continue
                self._conn.execute(
                    """
                    UPDATE tasks SET state='queued', local_path=NULL,
                        downloaded_bytes=0, download_retries=CASE WHEN read_identity LIKE 'listener:%'
                            THEN download_retries ELSE 0 END,
                        upload_retries=0,
                        wait_reason='本地文件不完整或不存在，服务重启后重新排队',
                        updated_at=?
                    WHERE id=?
                    """,
                    (time.time(), task_id),
                )
                recovered["queued"] += 1
        return recovered
