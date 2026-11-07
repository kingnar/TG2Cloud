"""CloudDrive2 gRPC read-back. WebDAV size and upload-task Finish are not proof.

Documented service ``clouddrive.CloudDriveFileSrv`` (proto version 1.0.11):
``GetSubFiles`` with ``forceRefresh`` and ``GetUploadFileList``. Authorization
is ``Bearer`` from ``TG2CLOUD_CD2_API_TOKEN``. The local gRPC address is
``TG2CLOUD_CD2_API_ADDRESS`` (default ``127.0.0.1:19798``). An optional path
prefix is ``TG2CLOUD_CD2_API_ROOT``. Set ``TG2CLOUD_CD2_API_TLS=1`` for TLS.

``CloudDriveFile.id`` plus size, ``isCloudFile``, and ``isLocal=false`` is the
evidence. ``fileHashes`` may contain MD5 or SHA1, not the local head/mid/tail
sample SHA-256, so a hash is not required and is not treated as that sample.
A missing token, a missing id, a size mismatch, or an upload still in progress
stays fail-closed and keeps the local file.
"""
from __future__ import annotations

import asyncio
import os
import posixpath
import re
import ssl
from typing import Any

HUFFMAN=[(8184, 13), (8388568, 23), (268435426, 28), (268435427, 28), (268435428, 28), (268435429, 28), (268435430, 28), (268435431, 28), (268435432, 28), (16777194, 24), (1073741820, 30), (268435433, 28), (268435434, 28), (1073741821, 30), (268435435, 28), (268435436, 28), (268435437, 28), (268435438, 28), (268435439, 28), (268435440, 28), (268435441, 28), (268435442, 28), (1073741822, 30), (268435443, 28), (268435444, 28), (268435445, 28), (268435446, 28), (268435447, 28), (268435448, 28), (268435449, 28), (268435450, 28), (268435451, 28), (20, 6), (1016, 10), (1017, 10), (4090, 12), (8185, 13), (21, 6), (248, 8), (2042, 11), (1018, 10), (1019, 10), (249, 8), (2043, 11), (250, 8), (22, 6), (23, 6), (24, 6), (0, 5), (1, 5), (2, 5), (25, 6), (26, 6), (27, 6), (28, 6), (29, 6), (30, 6), (31, 6), (92, 7), (251, 8), (32764, 15), (32, 6), (4091, 12), (1020, 10), (8186, 13), (33, 6), (93, 7), (94, 7), (95, 7), (96, 7), (97, 7), (98, 7), (99, 7), (100, 7), (101, 7), (102, 7), (103, 7), (104, 7), (105, 7), (106, 7), (107, 7), (108, 7), (109, 7), (110, 7), (111, 7), (112, 7), (113, 7), (114, 7), (252, 8), (115, 7), (253, 8), (8187, 13), (524272, 19), (8188, 13), (16380, 14), (34, 6), (32765, 15), (3, 5), (35, 6), (4, 5), (36, 6), (5, 5), (37, 6), (38, 6), (39, 6), (6, 5), (116, 7), (117, 7), (40, 6), (41, 6), (42, 6), (7, 5), (43, 6), (118, 7), (44, 6), (8, 5), (9, 5), (45, 6), (119, 7), (120, 7), (121, 7), (122, 7), (123, 7), (32766, 15), (2044, 11), (16381, 14), (8189, 13), (268435452, 28), (1048550, 20), (4194258, 22), (1048551, 20), (1048552, 20), (4194259, 22), (4194260, 22), (4194261, 22), (8388569, 23), (4194262, 22), (8388570, 23), (8388571, 23), (8388572, 23), (8388573, 23), (8388574, 23), (16777195, 24), (8388575, 23), (16777196, 24), (16777197, 24), (4194263, 22), (8388576, 23), (16777198, 24), (8388577, 23), (8388578, 23), (8388579, 23), (8388580, 23), (2097116, 21), (4194264, 22), (8388581, 23), (4194265, 22), (8388582, 23), (8388583, 23), (16777199, 24), (4194266, 22), (2097117, 21), (1048553, 20), (4194267, 22), (4194268, 22), (8388584, 23), (8388585, 23), (2097118, 21), (8388586, 23), (4194269, 22), (4194270, 22), (16777200, 24), (2097119, 21), (4194271, 22), (8388587, 23), (8388588, 23), (2097120, 21), (2097121, 21), (4194272, 22), (2097122, 21), (8388589, 23), (4194273, 22), (8388590, 23), (8388591, 23), (1048554, 20), (4194274, 22), (4194275, 22), (4194276, 22), (8388592, 23), (4194277, 22), (4194278, 22), (8388593, 23), (67108832, 26), (67108833, 26), (1048555, 20), (524273, 19), (4194279, 22), (8388594, 23), (4194280, 22), (33554412, 25), (67108834, 26), (67108835, 26), (67108836, 26), (134217694, 27), (134217695, 27), (67108837, 26), (16777201, 24), (33554413, 25), (524274, 19), (2097123, 21), (67108838, 26), (134217696, 27), (134217697, 27), (67108839, 26), (134217698, 27), (16777202, 24), (2097124, 21), (2097125, 21), (67108840, 26), (67108841, 26), (268435453, 28), (134217699, 27), (134217700, 27), (134217701, 27), (1048556, 20), (16777203, 24), (1048557, 20), (2097126, 21), (4194281, 22), (2097127, 21), (2097128, 21), (8388595, 23), (4194282, 22), (4194283, 22), (33554414, 25), (33554415, 25), (16777204, 24), (16777205, 24), (67108842, 26), (8388596, 23), (67108843, 26), (134217702, 27), (67108844, 26), (67108845, 26), (134217703, 27), (134217704, 27), (134217705, 27), (134217706, 27), (134217707, 27), (268435454, 28), (134217708, 27), (134217709, 27), (134217710, 27), (134217711, 27), (134217712, 27), (67108846, 26), (1073741823, 30)]

TOKEN_ENV = "TG2CLOUD_CD2_API_TOKEN"
ADDRESS_ENV = "TG2CLOUD_CD2_API_ADDRESS"
ROOT_ENV = "TG2CLOUD_CD2_API_ROOT"
TLS_ENV = "TG2CLOUD_CD2_API_TLS"
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


def encode_upload_list() -> bytes:
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


def uploads_from_list(blob: bytes) -> list[dict[str, Any]]:
    found = []
    for message in parse_messages(blob):
        for number, wire, payload in parse_fields(message):
            if number != 2 or wire != 2:
                continue
            item = {"dest": "", "status": None}
            for inner, iwire, ipayload in parse_fields(payload):
                if inner == 2 and iwire == 2:
                    item["dest"] = _text(ipayload)
                elif inner == 8 and iwire == 0:
                    item["status"], _ = _read_varint(ipayload, 0)
            found.append(item)
    return found


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


async def grpc_unary(host: str, port: int, method: str, payload: bytes, token: str, *, tls: bool) -> bytes:
    context = ssl.create_default_context() if tls else None
    reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port, ssl=context), 10)
    try:
        headers = _request_headers(f"{host}:{port}", method, token)
        body = b"\x00" + len(payload).to_bytes(4, "big") + payload
        writer.write(_PREFACE)
        writer.write(_frame(0x4, 0, 0, b""))
        writer.write(_frame(0x1, 0x4, 1, headers))
        writer.write(_frame(0x0, 0x1, 1, body))
        await writer.drain()
        data = bytearray()
        status = None
        while True:
            ftype, flags, stream, frame = await asyncio.wait_for(_read_frame(reader), 20)
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


def _join(root: str, relative: str) -> str:
    relative = relative.strip("/")
    if not root:
        return "/" + relative if relative else "/"
    base = root if root.startswith("/") else "/" + root.strip("/")
    return base if not relative else base.rstrip("/") + "/" + relative


def _parent_and_name(path: str) -> tuple[str, str]:
    parent, name = posixpath.split(path.strip("/"))
    return parent, name


async def read_back(relative_path: str, *, expected_size: int | None = None, rpc=None) -> dict[str, Any]:
    """Return an observation. Missing credentials or any RPC failure is not success."""
    failed = {"scheme": "clouddrive2-file", "read_back": False, "cache_only": None, "method": "api"}
    if not safe_relative(relative_path):
        return failed
    token = token_from_env()
    if rpc is None and not token:
        return {"scheme": "webdav", "method": "size", "read_back": False, "cache_only": None}
    try:
        parent, name = _parent_and_name(relative_path)
        api_parent = _join(root_from_env(), parent)
        if rpc is None:
            host, port = address_from_env()
            tls = os.environ.get(TLS_ENV, "") == "1"

            async def rpc(method, payload):
                return await grpc_unary(host, port, method, payload, token, tls=tls)
        listing = files_from_subfiles(await rpc("GetSubFiles", encode_subfiles(api_parent)))
        matches = [item for item in listing if item["name"] == name]
        uploads = uploads_from_list(await rpc("GetUploadFileList", encode_upload_list()))
    except Exception:  # noqa: BLE001 - an API failure is not remote success
        return failed
    if len(matches) != 1:
        return failed
    info = matches[0]
    full = info["full_path"] or _join(root_from_env(), relative_path)
    busy = any(
        item["dest"].rstrip("/").endswith("/" + name) or item["dest"].rstrip("/") == full.rstrip("/")
        for item in uploads
        if item["status"] in _IN_PROGRESS
    )
    if busy or not info["id"] or info["is_local"] or not info["is_cloud_file"]:
        return failed | {"destination_id": info.get("id") or "", "size": info.get("size"), "dest_path": relative_path}
    observation = {
        "scheme": "clouddrive2-file",
        "read_back": True,
        "cache_only": False,
        "destination_id": info["id"],
        "size": info["size"],
        "dest_path": relative_path,
        "is_cloud_file": True,
        "is_local": False,
        "name": name,
    }
    if expected_size is not None and info["size"] != int(expected_size):
        observation["read_back"] = True
    return observation
