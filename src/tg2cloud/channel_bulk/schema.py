"""Additive tables for inventory and bulk jobs.

These statements do not change PRAGMA user_version. Production databases that
already migrated to a local schema keep that version.
"""
from __future__ import annotations

import sqlite3


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}


def _add_column(connection: sqlite3.Connection, table: str, name: str, declaration: str) -> None:
    if name not in _columns(connection, table):
        connection.execute(f"ALTER TABLE {table} ADD COLUMN {name} {declaration}")


def ensure(connection: sqlite3.Connection) -> None:
    """Create channel-ops tables and optional task columns. Idempotent."""
    connection.execute("BEGIN IMMEDIATE")
    try:
        _add_column(connection, "tasks", "file_unique_id", "TEXT")
        _add_column(connection, "tasks", "content_fingerprint", "TEXT")
        _add_column(connection, "tasks", "bulk_job_id", "INTEGER")
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_tasks_file_unique_id "
            "ON tasks(file_unique_id) WHERE file_unique_id IS NOT NULL"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_tasks_content_fingerprint "
            "ON tasks(content_fingerprint) WHERE content_fingerprint IS NOT NULL"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_tasks_bulk_job "
            "ON tasks(bulk_job_id) WHERE bulk_job_id IS NOT NULL"
        )
        connection.execute(
            """CREATE TABLE IF NOT EXISTS channel_inventory (
                id INTEGER PRIMARY KEY,
                chat_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                checkpoint_id INTEGER NOT NULL DEFAULT 0,
                scanned INTEGER NOT NULL DEFAULT 0,
                page_size INTEGER NOT NULL,
                delay_seconds REAL NOT NULL,
                cooldown_until REAL NOT NULL DEFAULT 0,
                error TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )"""
        )
        connection.execute(
            """CREATE TABLE IF NOT EXISTS channel_inventory_items (
                inventory_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                link TEXT NOT NULL,
                duration REAL,
                size INTEGER NOT NULL,
                group_id TEXT NOT NULL DEFAULT '',
                media_key TEXT,
                file_unique_id TEXT,
                mime TEXT NOT NULL DEFAULT '',
                file_name TEXT NOT NULL,
                dedup TEXT NOT NULL,
                PRIMARY KEY (inventory_id, message_id)
            )"""
        )
        connection.execute(
            """CREATE TABLE IF NOT EXISTS channel_exclusions (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                reason TEXT NOT NULL,
                created_at REAL NOT NULL,
                PRIMARY KEY (chat_id, message_id)
            )"""
        )
        connection.execute(
            """CREATE TABLE IF NOT EXISTS channel_bulk_jobs (
                id INTEGER PRIMARY KEY,
                chat_id INTEGER NOT NULL,
                inventory_id INTEGER NOT NULL,
                state TEXT NOT NULL,
                directory_alias TEXT NOT NULL,
                manifest_hash TEXT NOT NULL DEFAULT '',
                controls TEXT NOT NULL,
                whitelist TEXT NOT NULL,
                generation INTEGER NOT NULL DEFAULT 0,
                cooldown_until REAL NOT NULL DEFAULT 0,
                error TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )"""
        )
        connection.execute(
            """CREATE TABLE IF NOT EXISTS channel_bulk_items (
                job_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                task_id INTEGER,
                attempts INTEGER NOT NULL DEFAULT 0,
                bytes_charged INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                PRIMARY KEY (job_id, message_id)
            )"""
        )
        connection.execute(
            """CREATE TABLE IF NOT EXISTS channel_byte_ledger (
                period_kind TEXT NOT NULL,
                period_key TEXT NOT NULL,
                bytes INTEGER NOT NULL,
                PRIMARY KEY (period_kind, period_key)
            )"""
        )
        _add_column(connection, "channel_inventory", "cancel_requested", "INTEGER NOT NULL DEFAULT 0")
        connection.execute(
            """CREATE TABLE IF NOT EXISTS channel_local_requests (
                request_id TEXT PRIMARY KEY,
                command TEXT NOT NULL,
                result TEXT NOT NULL,
                created_at REAL NOT NULL
            )"""
        )
        connection.execute(
            """CREATE TABLE IF NOT EXISTS channel_quota_attestation (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                remaining_bytes INTEGER NOT NULL,
                period_end REAL NOT NULL,
                attested_at REAL NOT NULL,
                ledger_total INTEGER NOT NULL,
                note TEXT NOT NULL DEFAULT ''
            )"""
        )
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        raise


def ensure_db(db) -> None:
    with db._lock:
        ensure(db._conn)
