"""CloudDrive2 gRPC read-back. WebDAV size and upload-task Finish are not proof.

Documented service ``clouddrive.CloudDriveFileSrv`` (proto version 1.0.11).
A known file is read with ``FindFileByPath``, then ``GetSearchResults``
(``fuzzyMatch`` false), then ``GetSubFiles`` with ``forceRefresh``.
The active upload queue is ``GetUploadFileCount``. Zero means nothing is
uploading. ``GetUploadFileList`` is used only when that count is non-zero, and
only as ``getAll`` (field 1 = true). A name filter plus page is not sent: on
this gateway that shape times out even when the panel queue is idle.
Authorization is ``Bearer`` from
``TG2CLOUD_CD2_API_TOKEN``. Address ``TG2CLOUD_CD2_API_ADDRESS`` defaults to
``127.0.0.1:19798``. Optional prefix ``TG2CLOUD_CD2_API_ROOT``. TLS when
``TG2CLOUD_CD2_API_TLS=1``.

Timing: ``TG2CLOUD_CD2_API_TIMEOUT_SECONDS`` (idle, default 60, range 5–120),
``TG2CLOUD_CD2_API_CALL_SECONDS`` (whole RPC, default 120, range 10–180),
``TG2CLOUD_CD2_API_RETRIES`` (default 3, range 1–5),
``TG2CLOUD_CD2_API_RETRY_BASE_SECONDS`` (default 0.5, range 0.1–10).

``CloudDriveFile.id`` plus size, ``isCloudFile``, and ``isLocal=false`` is the
file evidence. ``fileHashes`` may contain MD5 or SHA1, not the local sample
SHA-256, so that sample is not compared. An empty upload count is
``upload_list=clear``. If the count itself fails, the result is
``upload_list=unsupported`` and bulk success stays rejected. ``remote.verify``
can accept that case only when ``TG2CLOUD_CD2_UPLOAD_ATTEST=1`` and the request
sets ``upload_attest`` true. A completed directory list that does not contain
the name is ``missing``. A transport failure of every lookup is ``timeout``.
"""
from __future__ import annotations

import asyncio
import os
import posixpath
import random
import re
import ssl
from typing import Any

HUFFMAN=[(8184, 13), (8388568, 23), (268435426, 28), (268435427, 28), (268435428, 28), (268435429, 28), (268435430, 28), (268435431, 28), (268435432, 28), (16777194, 24), (1073741820, 30), (268435433, 28), (268435434, 28), (1073741821, 30), (268435435, 28), (268435436, 28), (268435437, 28), (268435438, 28), (268435439, 28), (268435440, 28), (268435441, 28), (268435442, 28), (1073741822, 30), (268435443, 28), (268435444, 28), (268435445, 28), (268435446, 28), (268435447, 28), (268435448, 28), (268435449, 28), (268435450, 28), (268435451, 28), (20, 6), (1016, 10), (1017, 10), (4090, 12), (8185, 13), (21, 6), (248, 8), (2042, 11), (1018, 10), (1019, 10), (249, 8), (2043, 11), (250, 8), (22, 6), (23, 6), (24, 6), (0, 5), (1, 5), (2, 5), (25, 6), (26, 6), (27, 6), (28, 6), (29, 6), (30, 6), (31, 6), (92, 7), (251, 8), (32764, 15), (32, 6), (4091, 12), (1020, 10), (8186, 13), (33, 6), (93, 7), (94, 7), (95, 7), (96, 7), (97, 7), (98, 7), (99, 7), (100, 7), (101, 7), (102, 7), (103, 7), (104, 7), (105, 7), (106, 7), (107, 7), (108, 7), (109, 7), (110, 7), (111, 7), (112, 7), (113, 7), (114, 7), (252, 8), (115, 7), (253, 8), (8187, 13), (524272, 19), (8188, 13), (16380, 14), (34, 6), (32765, 15), (3, 5), (35, 6), (4, 5), (36, 6), (5, 5), (37, 6), (38, 6), (39, 6), (6, 5), (116, 7), (117, 7), (40, 6), (41, 6), (42, 6), (7, 5), (43, 6), (118, 7), (44, 6), (8, 5), (9, 5), (45, 6), (119, 7), (120, 7), (121, 7), (122, 7), (123, 7), (32766, 15), (2044, 11), (16381, 14), (8189, 13), (268435452, 28), (1048550, 20), (4194258, 22), (1048551, 20), (1048552, 20), (4194259, 22), (4194260, 22), (4194261, 22), (8388569, 23), (4194262, 22), (8388570, 23), (8388571, 23), (8388572, 23), (8388573, 23), (8388574, 23), (16777195, 24), (8388575, 23), (16777196, 24), (16777197, 24), (4194263, 22), (8388576, 23), (16777198, 24), (8388577, 23), (8388578, 23), (8388579, 23), (8388580, 23), (2097116, 21), (4194264, 22), (8388581, 23), (4194265, 22), (8388582, 23), (8388583, 23), (16777199, 24), (4194266, 22), (2097117, 21), (1048553, 20), (4194267, 22), (4194268, 22), (8388584, 23), (8388585, 23), (2097118, 21), (8388586, 23), (4194269, 22), (4194270, 22), (16777200, 24), (2097119, 21), (4194271, 22), (8388587, 23), (8388588, 23), (2097120, 21), (2097121, 21), (4194272, 22), (2097122, 21), (8388589, 23), (4194273, 22), (8388590, 23), (8388591, 23), (1048554, 20), (4194274, 22), (4194275, 22), (4194276, 22), (8388592, 23), (4194277, 22), (4194278, 22), (8388593, 23), (67108832, 26), (67108833, 26), (1048555, 20), (524273, 19), (4194279, 22), (8388594, 23), (4194280, 22), (33554412, 25), (67108834, 26), (67108835, 26), (67108836, 26), (134217694, 27), (134217695, 27), (67108837, 26), (16777201, 24), (33554413, 25), (524274, 19), (2097123, 21), (67108838, 26), (134217696, 27), (134217697, 27), (67108839, 26), (134217698, 27), (16777202, 24), (2097124, 21), (2097125, 21), (67108840, 26), (67108841, 26), (268435453, 28), (134217699, 27), (134217700, 27), (134217701, 27), (1048556, 20), (16777203, 24), (1048557, 20), (2097126, 21), (4194281, 22), (2097127, 21), (2097128, 21), (8388595, 23), (4194282, 22), (4194283, 22), (33554414, 25), (33554415, 25), (16777204, 24), (16777205, 24), (67108842, 26), (8388596, 23), (67108843, 26), (134217702, 27), (67108844, 26), (67108845, 26), (134217703, 27), (134217704, 27), (134217705, 27), (134217706, 27), (134217707, 27), (268435454, 28), (134217708, 27), (134217709, 27), (134217710, 27), (134217711, 27), (134217712, 27), (67108846, 26), (1073741823, 30)]

TOKEN_ENV = "TG2CLOUD_CD2_API_TOKEN"
ADDRESS_ENV = "TG2CLOUD_CD2_API_ADDRESS"
ROOT_ENV = "TG2CLOUD_CD2_API_ROOT"
TLS_ENV = "TG2CLOUD_CD2_API_TLS"
TIMEOUT_ENV = "TG2CLOUD_CD2_API_TIMEOUT_SECONDS"
CALL_ENV = "TG2CLOUD_CD2_API_CALL_SECONDS"
RETRY_ENV = "TG2CLOUD_CD2_API_RETRIES"
BACKOFF_ENV = "TG2CLOUD_CD2_API_RETRY_BASE_SECONDS"
DEFAULT_ADDRESS = "127.0.0.1:19798"
_ADDRESS = re.compile(r"^[A-Za-z0-9._-]{1,253}:([0-9]{1,5})$")
_PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
_IN_PROGRESS = {0, 1, 3, 4, 7, 9, 10}


def token_from_env() -> str:
    value = os.environ.get(TOKEN_ENV, "")
    if not value or any(ord(char) < 33 or char in "\"'" for char in value):
        return ""
    return value


def address_from_env() -> tuple[str, int]:
    raw = os.environ.get(ADDRESS_ENV, DEFAULT_ADDRESS).strip()
    match = _ADDRESS.fullmatch(raw)
    if match is None:
        raise ValueError("bad-address")
    port = int(match.group(1))
    if not 1 <= port <= 65535:
        raise ValueError("bad-address")
    return raw.rsplit(":", 1)[0], port


def _env_number(name: str, default: float, low: float, high: float, *, integer: bool) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw) if integer else float(raw)
    except ValueError:
        return default
    if value < low or value > high:
        return default
    return value


def idle_timeout() -> float:
    return float(_env_number(TIMEOUT_ENV, 60, 5, 120, integer=False))


def call_timeout() -> float:
    return float(_env_number(CALL_ENV, 120, 10, 180, integer=False))


def retry_attempts() -> int:
    return int(_env_number(RETRY_ENV, 3, 1, 5, integer=True))


def retry_base() -> float:
    return float(_env_number(BACKOFF_ENV, 0.5, 0.1, 10, integer=False))


def root_from_env() -> str:
    raw = os.environ.get(ROOT_ENV, "").strip()
    if not raw:
        return ""
    if "\\" in raw or "\x00" in raw or any(part == ".." for part in raw.split("/")):
        raise ValueError("bad-root")
    return raw


def safe_relative(path: str) -> bool:
    if not isinstance(path, str) or not path or path.startswith("/") or "\\" in path or "\x00" in path:
        return False
    parts = [part for part in path.split("/") if part not in {"", "."}]
    return bool(parts) and ".." not in parts and len(path) <= 512


def _varint(value: int) -> bytes:
    out = bytearray()
    while True:
        piece = value & 0x7F
        value >>= 7
        if value:
            out.append(piece | 0x80)
        else:
            out.append(piece)
            return bytes(out)


def _read_varint(buf: bytes, pos: int) -> tuple[int, int]:
    value = shift = 0
    while pos < len(buf) and shift < 70:
        byte = buf[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if byte < 128:
            return value, pos
        shift += 7
    raise ValueError("varint")


def _field(number: int, wire: int, payload: bytes = b"") -> bytes:
    head = _varint((number << 3) | wire)
    if wire == 2:
        return head + _varint(len(payload)) + payload
    if wire == 0:
        return head + payload
    raise ValueError("wire")


def encode_subfiles(path: str) -> bytes:
    return _field(1, 2, path.encode()) + _field(2, 0, b"\x01")


def encode_search(path: str, name: str) -> bytes:
    """Exact name search. ``fuzzyMatch`` stays at its proto3 default, false."""
    return _field(1, 2, path.encode()) + _field(2, 2, name.encode()) + _field(3, 0, b"\x01")


def encode_find(parent: str, name: str) -> bytes:
    return _field(1, 2, parent.encode()) + _field(2, 2, name.encode())


def encode_upload_all() -> bytes:
    """Live queue. ``08 01`` is getAll=true and does not ask the gateway to scan history by name."""
    return _field(1, 0, b"\x01")


def parse_fields(buf: bytes) -> list[tuple[int, int, bytes]]:
    fields = []
    pos = 0
    while pos < len(buf):
        key, pos = _read_varint(buf, pos)
        number, wire = key >> 3, key & 7
        if wire == 0:
            value, pos = _read_varint(buf, pos)
            fields.append((number, wire, _varint(value)))
        elif wire == 2:
            length, pos = _read_varint(buf, pos)
            if length < 0 or pos + length > len(buf):
                raise ValueError("length")
            fields.append((number, wire, buf[pos:pos + length]))
            pos += length
        elif wire == 1:
            fields.append((number, wire, buf[pos:pos + 8]))
            pos += 8
        elif wire == 5:
            fields.append((number, wire, buf[pos:pos + 4]))
            pos += 4
        else:
            raise ValueError("wire")
    return fields


def _text(payload: bytes) -> str:
    return payload.decode("utf-8", "replace")


def parse_file(buf: bytes) -> dict[str, Any]:
    info: dict[str, Any] = {
        "id": "", "name": "", "full_path": "", "size": None,
        "is_cloud_file": False, "is_local": False, "hashes": {},
    }
    for number, wire, payload in parse_fields(buf):
        if number == 1 and wire == 2:
            info["id"] = _text(payload)
        elif number == 2 and wire == 2:
            info["name"] = _text(payload)
        elif number == 3 and wire == 2:
            info["full_path"] = _text(payload)
        elif number == 4 and wire == 0:
            info["size"], _ = _read_varint(payload, 0)
        elif number == 34 and wire == 0:
            info["is_cloud_file"] = payload != b"\x00"
        elif number == 37 and wire == 0:
            info["is_local"] = payload != b"\x00"
        elif number == 70 and wire == 2:
            key = value = None
            for inner, iwire, ipayload in parse_fields(payload):
                if inner == 1 and iwire == 0:
                    key, _ = _read_varint(ipayload, 0)
                elif inner == 2 and iwire == 2:
                    value = _text(ipayload)
            if key is not None and value:
                info["hashes"][int(key)] = value
    return info


def parse_messages(blob: bytes) -> list[bytes]:
    messages = []
    pos = 0
    while pos + 5 <= len(blob):
        flag = blob[pos]
        length = int.from_bytes(blob[pos + 1:pos + 5], "big")
        pos += 5
        if flag != 0 or pos + length > len(blob):
            raise ValueError("grpc")
        messages.append(blob[pos:pos + length])
        pos += length
    if pos != len(blob):
        raise ValueError("grpc")
    return messages


def files_from_subfiles(blob: bytes) -> list[dict[str, Any]]:
    found = []
    for message in parse_messages(blob):
        for number, wire, payload in parse_fields(message):
            if number == 1 and wire == 2:
                found.append(parse_file(payload))
    return found


def _upload_item(payload: bytes) -> dict[str, Any]:
    item = {"dest": "", "status": None}
    for inner, iwire, ipayload in parse_fields(payload):
        if inner == 2 and iwire == 2:
            item["dest"] = _text(ipayload)
        elif inner == 8 and iwire == 0:
            item["status"], _ = _read_varint(ipayload, 0)
    return item


def uploads_from_list(blob: bytes) -> list[dict[str, Any]]:
    found = []
    for message in parse_messages(blob):
        for number, wire, payload in parse_fields(message):
            if number == 2 and wire == 2:
                found.append(_upload_item(payload))
    return found


def parse_upload_count(blob: bytes) -> int:
    """Field 1 is the active upload count. An empty message means zero."""
    count = 0
    for message in parse_messages(blob):
        for number, wire, payload in parse_fields(message):
            if number == 1 and wire == 0:
                count, _ = _read_varint(payload, 0)
    return count


def file_from_unary(blob: bytes) -> dict[str, Any] | None:
    messages = parse_messages(blob)
    if len(messages) != 1:
        return None
    return parse_file(messages[0])


def _hpack_int(value: int, prefix: int) -> bytes:
    limit = (1 << prefix) - 1
    if value < limit:
        return bytes([value])
    out = bytearray([limit])
    value -= limit
    while value >= 128:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def _literal(name: str, value: str) -> bytes:
    raw_name = name.encode()
    raw_value = value.encode()
    return bytes([0x00]) + _hpack_int(len(raw_name), 7) + raw_name + _hpack_int(len(raw_value), 7) + raw_value


def _request_headers(host: str, method: str, token: str) -> bytes:
    scheme = "https" if os.environ.get(TLS_ENV, "") == "1" else "http"
    block = bytearray()
    block += _literal(":method", "POST")
    block += _literal(":scheme", scheme)
    block += _literal(":path", "/clouddrive.CloudDriveFileSrv/" + method)
    block += _literal(":authority", host)
    block += _literal("content-type", "application/grpc")
    block += _literal("te", "trailers")
    block += _literal("authorization", "Bearer " + token)
    return bytes(block)


def _decode_int(buf: bytes, pos: int, prefix: int) -> tuple[int, int]:
    limit = (1 << prefix) - 1
    value = buf[pos] & limit
    pos += 1
    if value < limit:
        return value, pos
    shift = 0
    while pos < len(buf):
        byte = buf[pos]
        pos += 1
        value += (byte & 0x7F) << shift
        if byte < 128:
            return value, pos
        shift += 7
    raise ValueError("hpack")


def _huffman(data: bytes) -> str:
    bits = "".join(f"{byte:08b}" for byte in data)
    trie: dict = {}
    for symbol, (code, length) in enumerate(HUFFMAN):
        node = trie
        text = f"{code:0{length}b}"
        for bit in text:
            node = node.setdefault(bit, {})
        node["$"] = symbol
    out = bytearray()
    node = trie
    consumed = 0
    for index, bit in enumerate(bits):
        node = node.get(bit)
        if node is None:
            raise ValueError("huffman")
        if "$" in node:
            if node["$"] == 256:
                break
            out.append(node["$"])
            node = trie
            consumed = index + 1
    padding = bits[consumed:]
    if padding and (len(padding) > 7 or any(bit != "1" for bit in padding)):
        raise ValueError("huffman-pad")
    return out.decode("utf-8", "replace")


def _decode_string(buf: bytes, pos: int) -> tuple[str, int]:
    huffman = bool(buf[pos] & 0x80)
    length, pos = _decode_int(buf, pos, 7)
    raw = buf[pos:pos + length]
    pos += length
    return (_huffman(raw) if huffman else raw.decode()), pos


def _decode_headers(block: bytes) -> dict[str, str]:
    headers: dict[str, str] = {}
    pos = 0
    while pos < len(block):
        lead = block[pos]
        if lead & 0x80:
            index, pos = _decode_int(block, pos, 7)
            name, value = _STATIC.get(index, ("", ""))
            if name:
                headers[name] = value
            continue
        prefix = 6 if lead & 0x40 else 4
        name_index, pos = _decode_int(block, pos, prefix)
        if name_index:
            name = _STATIC.get(name_index, ("", ""))[0]
        else:
            name, pos = _decode_string(block, pos)
        value, pos = _decode_string(block, pos)
        if name:
            headers[name] = value
    return headers


_STATIC = {
    2: (":method", "GET"),
    3: (":method", "POST"),
    4: (":path", "/"),
    6: (":scheme", "http"),
    7: (":scheme", "https"),
    8: (":status", "200"),
}


def _frame(ftype: int, flags: int, stream: int, payload: bytes) -> bytes:
    if len(payload) > 16384:
        raise ValueError("frame")
    return len(payload).to_bytes(3, "big") + bytes([ftype, flags]) + (stream & 0x7FFFFFFF).to_bytes(4, "big") + payload


async def _read_frame(reader: asyncio.StreamReader):
    header = await reader.readexactly(9)
    length = int.from_bytes(header[:3], "big")
    if length > 1024 * 1024:
        raise ValueError("frame")
    payload = await reader.readexactly(length) if length else b""
    return header[3], header[4], int.from_bytes(header[5:9], "big") & 0x7FFFFFFF, payload


async def grpc_unary(
    host: str,
    port: int,
    method: str,
    payload: bytes,
    token: str,
    *,
    tls: bool,
    idle_seconds: float | None = None,
    call_seconds: float | None = None,
) -> bytes:
    idle = idle_timeout() if idle_seconds is None else idle_seconds
    deadline = call_timeout() if call_seconds is None else call_seconds

    async def _once() -> bytes:
        context = ssl.create_default_context() if tls else None
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port, ssl=context), min(15.0, idle),
        )
        try:
            headers = _request_headers(f"{host}:{port}", method, token)
            body = b"\x00" + len(payload).to_bytes(4, "big") + payload
            window = (4).to_bytes(2, "big") + (1024 * 1024).to_bytes(4, "big")
            writer.write(_PREFACE)
            writer.write(_frame(0x4, 0, 0, window))
            writer.write(_frame(0x8, 0, 0, (1024 * 1024).to_bytes(4, "big")))
            writer.write(_frame(0x1, 0x4, 1, headers))
            writer.write(_frame(0x0, 0x1, 1, body))
            await writer.drain()
            data = bytearray()
            status = None
            pending = 0
            while True:
                ftype, flags, stream, frame = await asyncio.wait_for(_read_frame(reader), idle)
                if ftype == 0x4 and not flags & 0x1:
                    writer.write(_frame(0x4, 0x1, 0, b""))
                    await writer.drain()
                elif ftype == 0x6 and not flags & 0x1:
                    writer.write(_frame(0x6, 0x1, 0, frame[:8]))
                    await writer.drain()
                elif ftype in {0x1, 0x9}:
                    decoded = _decode_headers(frame)
                    if "grpc-status" in decoded:
                        status = decoded["grpc-status"]
                elif ftype == 0x0 and stream == 1:
                    data += frame
                    pending += len(frame)
                    if pending >= 32768:
                        credit = pending.to_bytes(4, "big")
                        writer.write(_frame(0x8, 0, 0, credit))
                        writer.write(_frame(0x8, 0, 1, credit))
                        await writer.drain()
                        pending = 0
                elif ftype == 0x7:
                    raise ValueError("goaway")
                if ftype == 0x0 and flags & 0x1:
                    break
                if ftype in {0x1, 0x9} and flags & 0x1 and data:
                    break
            if status not in {None, "0"}:
                raise ValueError("grpc-status")
            return bytes(data)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception as exc:  # noqa: BLE001 - closing a finished stream is not a verification result
                del exc

    return await asyncio.wait_for(_once(), deadline)


def _join(root: str, relative: str) -> str:
    relative = relative.strip("/")
    if not root:
        return "/" + relative if relative else "/"
    base = root if root.startswith("/") else "/" + root.strip("/")
    return base if not relative else base.rstrip("/") + "/" + relative


def _parent_and_name(path: str) -> tuple[str, str]:
    parent, name = posixpath.split(path.strip("/"))
    return parent, name


def _named(info: dict[str, Any] | None, name: str) -> bool:
    if not info:
        return False
    full = str(info.get("full_path") or "").rstrip("/")
    return info.get("name") == name or full.endswith("/" + name)


def _cloud_proof(info: dict[str, Any] | None, name: str) -> bool:
    return bool(
        _named(info, name)
        and info
        and info.get("id")
        and info.get("is_cloud_file") is True
        and info.get("is_local") is False
    )


def _observation(
    relative: str,
    info: dict[str, Any] | None,
    *,
    listing: str,
    upload_list: str,
    proven: bool,
    listing_rpc: str = "",
    listing_attempts: int = 0,
    listing_error: str = "",
    upload_rpc: str = "",
    upload_attempts: int = 0,
    upload_error: str = "",
) -> dict[str, Any]:
    return {
        "scheme": "clouddrive2-file",
        "method": "api",
        "read_back": proven,
        "cache_only": False if proven else None,
        "destination_id": "" if info is None else (info.get("id") or ""),
        "size": None if info is None else info.get("size"),
        "dest_path": relative,
        "is_cloud_file": bool(info and info.get("is_cloud_file")),
        "is_local": bool(info and info.get("is_local")),
        "listing": listing,
        "listing_rpc": listing_rpc,
        "listing_attempts": listing_attempts,
        "listing_error": listing_error,
        "upload_list": upload_list,
        "upload_rpc": upload_rpc,
        "upload_attempts": upload_attempts,
        "upload_error": upload_error,
    }


class _Call:
    def __init__(self) -> None:
        self.value: bytes | None = None
        self.attempts = 0
        self.error = ""


async def _retry(call, method: str, payload: bytes, *, attempts: int, base: float, sleep, rng) -> _Call:
    result = _Call()
    for attempt in range(attempts):
        result.attempts = attempt + 1
        try:
            result.value = await call(method, payload)
            result.error = ""
            return result
        except Exception as exc:  # noqa: BLE001 - one attempt is not the final result
            result.error = "timeout" if isinstance(exc, TimeoutError) else "rpc"
            if attempt + 1 >= attempts:
                return result
            delay = min(base * (2 ** attempt), 8.0) * (0.5 + float(rng()))
            await sleep(delay)
    return result


async def _lookup(call, parent: str, name: str, *, attempts: int, base: float, sleep, rng):
    """Return file, listing, rpc name, attempts, and error class.

    A finished directory list is the only ``missing`` signal.
    """
    found = await _retry(
        call, "FindFileByPath", encode_find(parent, name),
        attempts=attempts, base=base, sleep=sleep, rng=rng,
    )
    if not found.error:
        info = file_from_unary(found.value or b"")
        if _cloud_proof(info, name):
            return info, "ok", "FindFileByPath", found.attempts, ""
    searched = await _retry(
        call, "GetSearchResults", encode_search(parent, name),
        attempts=attempts, base=base, sleep=sleep, rng=rng,
    )
    if not searched.error:
        matches = [item for item in files_from_subfiles(searched.value or b"") if _named(item, name)]
        if len(matches) > 1:
            return None, "ambiguous", "GetSearchResults", searched.attempts, ""
        if len(matches) == 1 and _cloud_proof(matches[0], name):
            return matches[0], "ok", "GetSearchResults", searched.attempts, ""
    listed = await _retry(
        call, "GetSubFiles", encode_subfiles(parent),
        attempts=attempts, base=base, sleep=sleep, rng=rng,
    )
    if listed.error:
        return None, "timeout", "GetSubFiles", listed.attempts, listed.error
    matches = [item for item in files_from_subfiles(listed.value or b"") if _named(item, name)]
    if len(matches) > 1:
        return None, "ambiguous", "GetSubFiles", listed.attempts, ""
    if len(matches) == 1:
        return matches[0], "ok", "GetSubFiles", listed.attempts, ""
    return None, "missing", "GetSubFiles", listed.attempts, "empty"


def _busy(items: list[dict[str, Any]], name: str, full: str) -> bool:
    return any(
        item["dest"].rstrip("/").endswith("/" + name) or item["dest"].rstrip("/") == full.rstrip("/")
        for item in items
        if item["status"] in _IN_PROGRESS
    )


async def _upload_state(call, name: str, full: str, *, attempts: int, base: float, sleep, rng) -> dict[str, Any]:
    """Active queue only. A name-filtered page is not requested."""
    count = await _retry(
        call, "GetUploadFileCount", b"",
        attempts=attempts, base=base, sleep=sleep, rng=rng,
    )
    if count.error:
        return {
            "state": "unsupported",
            "upload_rpc": "GetUploadFileCount",
            "upload_attempts": count.attempts,
            "upload_error": count.error,
        }
    if parse_upload_count(count.value or b"") == 0:
        return {
            "state": "clear",
            "upload_rpc": "GetUploadFileCount",
            "upload_attempts": count.attempts,
            "upload_error": "empty",
        }
    listed = await _retry(
        call, "GetUploadFileList", encode_upload_all(),
        attempts=attempts, base=base, sleep=sleep, rng=rng,
    )
    if listed.error:
        return {
            "state": "inconclusive",
            "upload_rpc": "GetUploadFileList",
            "upload_attempts": listed.attempts,
            "upload_error": listed.error,
        }
    items = uploads_from_list(listed.value or b"")
    return {
        "state": "busy" if _busy(items, name, full) else "clear",
        "upload_rpc": "GetUploadFileList",
        "upload_attempts": listed.attempts,
        "upload_error": "empty" if not items else "",
    }


async def read_back(
    relative_path: str,
    *,
    expected_size: int | None = None,
    rpc=None,
    sleep=None,
    rng=None,
) -> dict[str, Any]:
    """Return an observation. Missing credentials or transport failure is not success."""
    del expected_size
    failed = {
        "scheme": "clouddrive2-file", "read_back": False, "cache_only": None,
        "method": "api", "listing": "failed", "upload_list": "not_asked",
    }
    if not safe_relative(relative_path):
        return failed
    token = token_from_env()
    if rpc is None and not token:
        return {"scheme": "webdav", "method": "size", "read_back": False, "cache_only": None, "listing": "skipped"}
    parent, name = _parent_and_name(relative_path)
    try:
        api_parent = _join(root_from_env(), parent)
    except ValueError:
        return failed
    if rpc is None:
        try:
            host, port = address_from_env()
        except ValueError:
            return failed
        tls = os.environ.get(TLS_ENV, "") == "1"

        async def rpc(method, payload, _host=host, _port=port, _tls=tls, _token=token):
            return await grpc_unary(
                _host, _port, method, payload, _token, tls=_tls,
                idle_seconds=idle_timeout(), call_seconds=call_timeout(),
            )
    pause = asyncio.sleep if sleep is None else sleep
    roll = random.random if rng is None else rng
    attempts, base = retry_attempts(), retry_base()
    info, listing, listing_rpc, listing_attempts, listing_error = await _lookup(
        rpc, api_parent, name, attempts=attempts, base=base, sleep=pause, rng=roll,
    )
    if listing != "ok" or not _cloud_proof(info, name):
        return _observation(
            relative_path, info if listing == "ok" else None, listing=listing, upload_list="not_asked",
            proven=False, listing_rpc=listing_rpc, listing_attempts=listing_attempts, listing_error=listing_error,
        )
    full = (info or {}).get("full_path") or _join(api_parent, name)
    upload = await _upload_state(
        rpc, name, str(full), attempts=attempts, base=base, sleep=pause, rng=roll,
    )
    return _observation(
        relative_path, info, listing="ok", upload_list=upload["state"], proven=upload["state"] == "clear",
        listing_rpc=listing_rpc, listing_attempts=listing_attempts, listing_error=listing_error,
        upload_rpc=upload["upload_rpc"], upload_attempts=upload["upload_attempts"],
        upload_error=upload["upload_error"],
    )
