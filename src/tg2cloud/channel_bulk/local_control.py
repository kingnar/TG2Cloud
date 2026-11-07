"""Operator-local Unix socket for inventory. It does not scan on startup.

The socket is mode 0600 and accepts only the process uid, plus uids listed in
an optional mode-0600 allow file. Requests are a fixed JSON command set.
They are not executed as code and they do not open a Telegram client.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import socket
import stat
import struct
import time
from pathlib import Path

from tg2cloud.channel_bulk import inventory, jobs
from tg2cloud.channel_bulk.schema import ensure_db

_REQUEST_ID = re.compile(r"^[A-Za-z0-9_-]{8,64}$")
_SECRET = re.compile(
    r"(?i)(password|token|api_hash|authorization|cookie|secret)(\s*[:=]\s*)\S+"
)
_MAX_BYTES = 65536
_COMMANDS = {
    "inventory.start",
    "inventory.run",
    "inventory.status",
    "inventory.export",
    "inventory.cancel",
    "inventory.review",
    "bulk.preview",
    "bulk.status",
    "remote.verify",
    "ping",
}


def redact(text: str) -> str:
    cleaned = _SECRET.sub(r"\1\2[REDACTED]", text or "")
    cleaned = re.sub(r"(?i)\bbearer\s+\S+", "Bearer [REDACTED]", cleaned)
    cleaned = re.sub(r"https?://\S+", "[URL]", cleaned)
    return cleaned[:400]


def peer_cred(sock: socket.socket) -> tuple[int, int, int]:
    """Return pid, uid, gid from the kernel. Fail closed if the platform cannot."""
    option = getattr(socket, "SO_PEERCRED", None)
    if option is None:
        raise PermissionError("peer credentials are unavailable")
    raw = sock.getsockopt(socket.SOL_SOCKET, option, 12)
    pid, uid, gid = struct.unpack("III", raw[:12])
    return int(pid), int(uid), int(gid)


def peer_allowed(uid: int, *, process_uid: int, allow: set[int]) -> bool:
    if type(uid) is not int or uid < 0:
        return False
    return uid == int(process_uid) or uid in allow


def load_allow(directory: Path, process_uid: int) -> set[int]:
    """Read optional extra uids. A loose or foreign file refuses the socket."""
    path = directory / "local-allow"
    if not path.exists():
        return set()
    info = path.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise PermissionError("allow file must be a regular file")
    if info.st_uid not in {process_uid, 0} or info.st_mode & 0o077:
        raise PermissionError("allow file ownership or mode is unsafe")
    allowed: set[int] = set()
    text = path.read_text(encoding="utf-8")
    for line in text.splitlines():
        item = line.strip()
        if not item or item.startswith("#"):
            continue
        if not item.isdigit():
            raise PermissionError("allow file contains a non-uid")
        allowed.add(int(item))
    return allowed


def prepare_directory(data_dir: Path) -> Path:
    root = Path(data_dir)
    if root.is_symlink():
        raise PermissionError("data directory is a symlink")
    directory = root / "channel-bulk"
    if directory.is_symlink():
        raise PermissionError("control directory is a symlink")
    directory.mkdir(mode=0o700, exist_ok=True)
    os.chmod(directory, 0o700)
    info = directory.lstat()
    if stat.S_ISLNK(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise PermissionError("control directory is not private")
    sock = directory / "control.sock"
    if sock.is_symlink():
        raise PermissionError("control socket is a symlink")
    if sock.exists():
        current = sock.lstat()
        if not stat.S_ISSOCK(current.st_mode) or current.st_uid != os.geteuid():
            raise PermissionError("control socket is not ours")
        sock.unlink()
    return sock


def audit_ready(directory: Path) -> None:
    path = directory / "audit.log"
    if path.is_symlink():
        raise PermissionError("audit log is a symlink")
    flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o600)
    try:
        os.fchmod(fd, 0o600)
    finally:
        os.close(fd)


def _audit(directory: Path, record: dict) -> None:
    path = directory / "audit.log"
    if path.is_symlink():
        raise PermissionError("audit log is a symlink")
    safe = {
        "ts": int(record.get("ts") or time.time()),
        "uid": int(record.get("uid")),
        "pid": int(record.get("pid")),
        "command": str(record.get("command") or "")[:64],
        "request_id": str(record.get("request_id") or "")[:64],
        "ok": bool(record.get("ok")),
        "error": redact(str(record.get("error") or ""))[:80],
        "inventory_id": record.get("inventory_id"),
        "job_id": record.get("job_id"),
    }
    flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o600)
    try:
        os.fchmod(fd, 0o600)
        os.write(fd, (json.dumps(safe, ensure_ascii=False) + "\n").encode("utf-8"))
    finally:
        os.close(fd)


def _recall(db, request_id: str, command: str):
    with db._lock:
        row = db._conn.execute(
            "SELECT command, result FROM channel_local_requests WHERE request_id=?",
            (request_id,),
        ).fetchone()
    if row is None:
        return None
    if row["command"] != command:
        raise ValueError("replay-mismatch")
    return json.loads(row["result"])


def _store(db, request_id: str, command: str, result: dict) -> None:
    with db._lock:
        db._conn.execute(
            """INSERT INTO channel_local_requests(request_id, command, result, created_at)
               VALUES(?,?,?,?)""",
            (request_id, command, json.dumps(result, ensure_ascii=False), time.time()),
        )


class LocalControl:
    def __init__(self, owner):
        self.owner = owner
        self.directory = Path(owner.settings.data_dir) / "channel-bulk"
        self._server = None
        self._inflight: dict[str, asyncio.Future] = {}
        self._gate = asyncio.Lock()
        self._flag = asyncio.Lock()
        self._scanning = False
        self.process_uid = os.geteuid()

    async def start(self) -> None:
        ensure_db(self.owner.db)
        path = prepare_directory(self.owner.settings.data_dir)
        self._allow = load_allow(self.directory, self.process_uid)
        self._server = await asyncio.start_unix_server(self._connection, path=str(path))
        os.chmod(path, 0o600)

    async def close(self) -> None:
        server = self._server
        self._server = None
        if server is None:
            return
        server.close()
        await server.wait_closed()

    async def _connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = {"pid": 0, "uid": -1}
        try:
            sock = writer.get_extra_info("socket")
            pid, uid, _gid = peer_cred(sock)
            peer = {"pid": pid, "uid": uid}
            if not peer_allowed(uid, process_uid=self.process_uid, allow=self._allow):
                await self._reply(writer, {"ok": False, "error": "denied"}, peer, "denied", "")
                return
            raw = await reader.read(_MAX_BYTES + 1)
            if len(raw) > _MAX_BYTES or b"\n" not in raw:
                await self._reply(writer, {"ok": False, "error": "bad-request"}, peer, "", "")
                return
            line = raw.split(b"\n", 1)[0]
            try:
                request = json.loads(line.decode("utf-8"))
            except (UnicodeError, json.JSONDecodeError):
                await self._reply(writer, {"ok": False, "error": "bad-request"}, peer, "", "")
                return
            if not isinstance(request, dict) or request.keys() & {
                "path", "file", "code", "token", "output", "directory", "caption", "title",
            }:
                await self._reply(writer, {"ok": False, "error": "bad-request"}, peer, "", "")
                return
            command = request.get("command")
            request_id = request.get("request_id")
            if command not in _COMMANDS or not isinstance(request_id, str) or not _REQUEST_ID.fullmatch(request_id):
                await self._reply(writer, {"ok": False, "error": "bad-request"}, peer, str(command or ""), "")
                return
            result = await self._once(command, request_id, request)
            await self._reply(writer, result, peer, command, request_id)
        except Exception:  # noqa: BLE001 - the socket must not leak exception text
            try:
                await self._reply(writer, {"ok": False, "error": "failed"}, peer, "", "")
            except Exception:  # noqa: BLE001 - the peer may already be gone
                return
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:  # noqa: BLE001
                return

    async def _reply(self, writer, result, peer, command, request_id) -> None:
        try:
            _audit(self.directory, {
                "uid": peer.get("uid", -1),
                "pid": peer.get("pid", 0),
                "command": command,
                "request_id": request_id,
                "ok": bool(result.get("ok")),
                "error": result.get("error") or "",
                "inventory_id": result.get("inventory_id"),
                "job_id": result.get("job_id"),
            })
        except PermissionError:
            result = {"ok": False, "error": "audit"}
        writer.write((json.dumps(result, ensure_ascii=False) + "\n").encode("utf-8"))
        await writer.drain()

    async def _once(self, command: str, request_id: str, request: dict) -> dict:
        async with self._gate:
            try:
                stored = _recall(self.owner.db, request_id, command)
            except ValueError:
                return {"ok": False, "error": "replay-mismatch"}
            if stored is not None:
                stored = dict(stored)
                stored["replayed"] = True
                return stored
            if request_id in self._inflight:
                waiter = self._inflight[request_id]
                leader = False
            else:
                waiter = asyncio.get_running_loop().create_future()
                self._inflight[request_id] = waiter
                leader = True
        if not leader:
            return await waiter
        try:
            try:
                audit_ready(self.directory)
            except PermissionError:
                result = {"ok": False, "error": "audit"}
                waiter.set_result(result)
                return result
            try:
                result = await self._dispatch(command, request)
            except ValueError:
                result = {"ok": False, "error": "bad-request"}
            except KeyError:
                result = {"ok": False, "error": "missing"}
            if result.get("ok"):
                _store(self.owner.db, request_id, command, result)
            waiter.set_result(result)
            return result
        except Exception:  # noqa: BLE001
            result = {"ok": False, "error": "failed"}
            if not waiter.done():
                waiter.set_result(result)
            return result
        finally:
            self._inflight.pop(request_id, None)

    async def _dispatch(self, command: str, request: dict) -> dict:
        if command == "ping":
            return {"ok": True, "status": "ready"}
        if command == "inventory.start":
            chat_id = _number(request.get("chat_id"), "chat")
            page = int(request.get("page_size") or 20)
            delay = float(request.get("delay_seconds") or 2)
            row = inventory.start(
                self.owner.db, chat_id, page_size=page, delay_seconds=delay, now=time.time()
            )
            return {"ok": True, "inventory_id": row["id"], "status": row["status"]}
        if command == "inventory.status":
            inventory_id = _number(request.get("inventory_id"), "inventory")
            row = inventory.load(self.owner.db, inventory_id)
            return {
                "ok": True,
                "inventory_id": inventory_id,
                "status": row["status"],
                "checkpoint_id": row["checkpoint_id"],
                "scanned": row["scanned"],
            }
        if command == "inventory.export":
            inventory_id = _number(request.get("inventory_id"), "inventory")
            kind = request.get("format")
            if kind not in {"csv", "json"}:
                return {"ok": False, "error": "bad-request"}
            body = inventory.export_text(self.owner.db, inventory_id, kind)
            directory = self.directory / "exports"
            directory.mkdir(mode=0o700, exist_ok=True)
            path = directory / f"inventory-{inventory_id}.{kind}"
            path.write_text(body, encoding="utf-8")
            os.chmod(path, 0o600)
            return {"ok": True, "inventory_id": inventory_id, "export": path.name}
        if command == "inventory.cancel":
            inventory_id = _number(request.get("inventory_id"), "inventory")
            inventory.request_cancel(self.owner.db, inventory_id, now=time.time())
            return {"ok": True, "inventory_id": inventory_id, "status": "paused"}
        if command == "inventory.run":
            return await self._run_inventory(request)
        if command == "inventory.review":
            return await self._review(request)
        if command == "bulk.preview":
            inventory_id = _number(request.get("inventory_id"), "inventory")
            alias = request.get("alias")
            job = jobs.open_job(self.owner.db, inventory_id, str(alias), now=time.time())
            if job["state"] != "preview":
                return {"ok": False, "error": "state", "job_id": job["id"]}
            return {"ok": True, "job_id": job["id"], "status": job["state"]}
        if command == "bulk.status":
            job = jobs.get_job(self.owner.db, _number(request.get("job_id"), "job"))
            return {"ok": True, "job_id": job["id"], "status": job["state"]}
        if command == "remote.verify":
            return await self._verify(request)
        return {"ok": False, "error": "bad-request"}

    async def _run_inventory(self, request: dict) -> dict:
        inventory_id = _number(request.get("inventory_id"), "inventory")
        max_pages = _bound(request.get("max_pages"), default=30, upper=200)
        max_seconds = _bound(request.get("max_seconds"), default=120, upper=600)
        async with self._flag:
            if self._scanning:
                return {"ok": False, "error": "busy", "inventory_id": inventory_id}
            self._scanning = True
        try:
            outcome = await inventory.run_bounded(
                self.owner.db,
                inventory_id,
                lambda offset, limit: self._fetch(inventory_id, offset, limit),
                max_pages=max_pages,
                max_seconds=max_seconds,
                now=time.time,
                sleep=asyncio.sleep,
            )
        finally:
            async with self._flag:
                self._scanning = False
        row = inventory.load(self.owner.db, inventory_id)
        return {
            "ok": True,
            "inventory_id": inventory_id,
            "status": row["status"],
            "checkpoint_id": row["checkpoint_id"],
            "result": outcome,
        }

    async def _fetch(self, inventory_id: int, offset: int, limit: int):
        custom = getattr(self.owner, "channel_bulk_fetch", None)
        if custom is not None:
            return await custom(offset, limit)
        from tg2cloud.channel_bulk.control import _fetch

        row = inventory.load(self.owner.db, inventory_id)
        return await _fetch(self.owner, int(row["chat_id"]), offset, limit)

    async def _review(self, request: dict) -> dict:
        from tg2cloud.channel_bulk.review import (
            MAX_IDS,
            MAX_SECONDS,
            MAX_TOTAL_BYTES,
            collect,
            reader_for,
        )

        inventory_id = _number(request.get("inventory_id"), "inventory")
        raw_ids = request.get("message_ids")
        if not isinstance(raw_ids, list) or not 1 <= len(raw_ids) <= MAX_IDS:
            raise ValueError("bad-request")
        message_ids = [_number(item, "message") for item in raw_ids]
        if len(set(message_ids)) != len(message_ids):
            raise ValueError("bad-request")
        max_seconds = _bound(request.get("max_seconds"), default=60, upper=MAX_SECONDS)
        max_total = _bound(request.get("max_total_bytes"), default=1024 * 1024, upper=MAX_TOTAL_BYTES)
        async with self._flag:
            if self._scanning:
                return {"ok": False, "error": "busy", "inventory_id": inventory_id}
            self._scanning = True
        try:
            result = await collect(
                self.owner.db,
                Path(self.owner.settings.data_dir),
                inventory_id,
                message_ids,
                reader_for(self.owner),
                now=time.time,
                sleep=asyncio.sleep,
                max_seconds=max_seconds,
                max_total_bytes=max_total,
            )
        finally:
            async with self._flag:
                self._scanning = False
        return {key: value for key, value in result.items() if key != "caption"}

    async def _verify(self, request: dict) -> dict:
        from tg2cloud.channel_bulk.cd2api import read_back, safe_relative
        from tg2cloud.channel_bulk.remote_evidence import verification_report

        paths = request.get("paths")
        sizes = request.get("sizes")
        if (
            not isinstance(paths, list)
            or not isinstance(sizes, list)
            or not 1 <= len(paths) <= 5
            or len(paths) != len(sizes)
        ):
            raise ValueError("bad-request")
        items = []
        for path, size in zip(paths, sizes):
            if not safe_relative(path) or type(size) is not int or isinstance(size, bool) or size < 0:
                raise ValueError("bad-request")
            observed = await read_back(path, expected_size=size)
            items.append(verification_report(
                observed, size=size, dest=path, attest=request.get("upload_attest") is True,
            ))
        return {
            "ok": True,
            "checked": len(items),
            "matched": sum(1 for item in items if item["accepted"]),
            "items": items,
        }


def _number(value, name: str) -> int:
    if type(value) is not int or isinstance(value, bool):
        raise ValueError("bad-request")
    if name == "chat":
        if value == 0:
            raise ValueError("bad-request")
    elif value <= 0:
        raise ValueError("bad-request")
    return value


def _bound(value, *, default: int, upper: int) -> int:
    if value is None:
        return default
    if type(value) is not int or isinstance(value, bool) or not 1 <= value <= upper:
        raise ValueError("bad-request")
    return value


async def start_local_control(owner):
    """Bind the socket. Does not read Telegram."""
    settings = getattr(owner, "settings", None)
    if settings is None or not getattr(settings, "data_dir", None):
        return None
    control = LocalControl(owner)
    await control.start()
    return control


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Send one JSON command to the local control socket")
    parser.add_argument("--socket", required=True)
    parser.add_argument("--request", required=True)
    args = parser.parse_args(argv)
    path = Path(args.socket)
    info = path.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISSOCK(info.st_mode):
        print('{"ok":false,"error":"socket"}')
        return 2
    payload = json.loads(args.request)
    if not isinstance(payload, dict):
        print('{"ok":false,"error":"bad-request"}')
        return 2

    async def send() -> str:
        reader, writer = await asyncio.open_unix_connection(str(path))
        writer.write((json.dumps(payload) + "\n").encode("utf-8"))
        await writer.drain()
        line = await reader.readline()
        writer.close()
        await writer.wait_closed()
        return line.decode("utf-8")

    print(asyncio.run(send()).rstrip())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
