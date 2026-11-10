"""Server-side WebDAV rename for completed bulk files. Overwrite is forbidden."""
from __future__ import annotations

import base64
import re
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote, urljoin
from urllib.request import Request, urlopen

from tg2cloud.channel_bulk.names import (
    SourceMissing,
    display_filename,
    has_task_suffix,
    join_remote,
    name_candidates,
)

_LENGTH = re.compile(r"<(?:[A-Za-z0-9]+:)?getcontentlength>\s*(\d+)\s*</", re.IGNORECASE)


class WebDavError(Exception):
    def __init__(self, status: int) -> None:
        self.status = int(status)
        super().__init__(f"webdav {int(status)}")


class WebDavFiles:
    """MOVE with Overwrite: F. Errors never include the password or the URL."""

    def __init__(self, base_url: str, user: str, password: str) -> None:
        self._base = str(base_url or "").rstrip("/") + "/"
        self._user = user or ""
        self._password = password or ""

    def __repr__(self) -> str:
        return "WebDavFiles()"

    def _url(self, relative: str) -> str:
        return urljoin(self._base, quote(str(relative).lstrip("/"), safe="/"))

    def _open(self, method: str, relative: str, headers: dict[str, str]) -> tuple[int, bytes]:
        request = Request(self._url(relative), method=method, headers=dict(headers))
        token = base64.b64encode(f"{self._user}:{self._password}".encode()).decode("ascii")
        request.add_header("Authorization", f"Basic {token}")
        try:
            with urlopen(request, timeout=60) as response:
                return int(response.status), response.read()
        except HTTPError as exc:
            return int(exc.code), b""
        except Exception:  # noqa: BLE001 - do not keep a message that may contain the endpoint
            raise WebDavError(0) from None

    def size(self, relative: str) -> int | None:
        status, body = self._open("PROPFIND", relative, {"Depth": "0"})
        if status == 404:
            return None
        if status not in {200, 207}:
            raise WebDavError(status)
        found = _LENGTH.findall(body.decode("utf-8", "replace"))
        if not found:
            raise WebDavError(status)
        return int(found[-1])

    def move(self, source: str, destination: str) -> int:
        status, _body = self._open(
            "MOVE", source, {"Destination": self._url(destination), "Overwrite": "F"},
        )
        return status


def _paths(task: dict) -> str:
    return str(task.get("remote_final_path") or task.get("remote_path") or "")


def _db_taken(db, path: str, task_id: int) -> bool:
    if db.remote_claimed(path, task_id):
        return True
    with db._lock:
        row = db._conn.execute(
            """SELECT 1 FROM tasks WHERE id!=? AND state!='cancelled'
               AND (remote_path=? OR remote_final_path=? OR remote_temp_path=?) LIMIT 1""",
            (int(task_id), path, path, path),
        ).fetchone()
    return row is not None


def _commit_paths(db, task_id: int, remote: str) -> bool:
    with db._lock:
        db._conn.execute("BEGIN IMMEDIATE")
        try:
            clash = db._conn.execute(
                """SELECT 1 FROM tasks WHERE id!=? AND state!='cancelled'
                   AND (remote_path=? OR remote_final_path=? OR remote_temp_path=?) LIMIT 1""",
                (int(task_id), remote, remote, remote),
            ).fetchone()
            if clash is not None:
                db._conn.execute("ROLLBACK")
                return False
            cursor = db._conn.execute(
                """UPDATE tasks SET remote_path=?, remote_final_path=?, remote_temp_path=NULL, updated_at=?
                   WHERE id=? AND state='completed'""",
                (remote, remote, time.time(), int(task_id)),
            )
            if cursor.rowcount != 1:
                db._conn.execute("ROLLBACK")
                return False
            db._conn.execute("COMMIT")
            return True
        except BaseException:
            if db._conn.in_transaction:
                db._conn.execute("ROLLBACK")
            raise


def _directory(remote: str) -> str:
    parent = str(Path(remote).parent)
    return "" if parent == "." else parent


async def rename_completed(db, job_id: int, *, fetch_caption, dav=None, dry_run: bool = True) -> dict:
    """Rename done items that still use a task suffix. dry_run does not touch the server."""
    with db._lock:
        rows = db._conn.execute(
            """SELECT i.message_id, i.task_id, t.file_name, t.remote_path, t.remote_final_path, t.file_size
               FROM channel_bulk_items i JOIN tasks t ON t.id=i.task_id
               WHERE i.job_id=? AND i.status='done' AND t.state='completed'
               ORDER BY i.message_id""",
            (int(job_id),),
        ).fetchall()
    report = []
    planned: set[str] = set()
    for row in rows:
        item = dict(row)
        message_id = int(item["message_id"])
        task_id = int(item["task_id"])
        current = _paths(item)
        entry = {"message_id": message_id, "task_id": task_id, "source": current}
        if not has_task_suffix(current):
            entry["status"] = "already"
            report.append(entry)
            continue
        try:
            caption = await fetch_caption(message_id)
        except SourceMissing:
            entry["status"] = "source_deleted"
            entry["reason"] = "消息已不存在"
            report.append(entry)
            continue
        except Exception:  # noqa: BLE001 - keep going and never return the exception text
            entry["status"] = "failed"
            entry["reason"] = "source"
            report.append(entry)
            continue
        filename = display_filename(caption, item.get("file_name"), message_id, None)
        directory = _directory(current)
        candidates = [join_remote(directory, name) for name in name_candidates(filename, message_id)]

        def taken(path: str, current: str = current, task_id: int = task_id) -> bool:
            return path in planned or path == current or _db_taken(db, path, task_id)

        if dry_run:
            target = next((path for path in candidates if not taken(path)), None)
            if target is None:
                entry["status"] = "conflict"
            else:
                planned.add(target)
                entry["target"] = target
                entry["status"] = "planned"
            report.append(entry)
            continue
        if dav is None:
            entry["status"] = "failed"
            entry["reason"] = "webdav"
            report.append(entry)
            continue
        try:
            source_size = dav.size(current)
            if source_size is None:
                reconciled = None
                for candidate in candidates:
                    if taken(candidate):
                        continue
                    found = dav.size(candidate)
                    if found is None:
                        break
                    if found == int(item["file_size"]):
                        reconciled = candidate
                        break
                if reconciled is not None and _commit_paths(db, task_id, reconciled):
                    planned.add(reconciled)
                    entry["target"] = reconciled
                    entry["status"] = "reconciled"
                    entry["size"] = int(item["file_size"])
                else:
                    entry["status"] = "missing_source"
                report.append(entry)
                continue
            target = None
            for candidate in candidates:
                if taken(candidate):
                    continue
                if dav.size(candidate) is None:
                    target = candidate
                    break
        except WebDavError as exc:
            entry["status"] = "failed"
            entry["http"] = exc.status
            report.append(entry)
            continue
        if target is None:
            entry["status"] = "conflict"
            report.append(entry)
            continue
        entry["target"] = target
        try:
            status = dav.move(current, target)
        except WebDavError as exc:
            entry["status"] = "failed"
            entry["http"] = exc.status
            report.append(entry)
            continue
        if status == 412:
            entry["status"] = "conflict"
            entry["http"] = 412
            report.append(entry)
            continue
        if status not in {201, 204}:
            entry["status"] = "failed"
            entry["http"] = status
            report.append(entry)
            continue
        try:
            new_size = dav.size(target)
            old_size = dav.size(current)
        except WebDavError as exc:
            entry["status"] = "unverified"
            entry["http"] = exc.status
            report.append(entry)
            continue
        if new_size == source_size and old_size is None and _commit_paths(db, task_id, target):
            planned.add(target)
            entry["status"] = "renamed"
            entry["size"] = new_size
        else:
            entry["status"] = "unverified"
        report.append(entry)
    return {"ok": True, "job_id": int(job_id), "dry_run": dry_run, "items": report}
