"""CloudDrive2 file-id read-back. No live gateway and no token values from production."""
from __future__ import annotations

import asyncio
import os
import unittest
from pathlib import Path
from unittest.mock import patch

from tg2cloud.channel_bulk.cd2api import (
    _field,
    _huffman,
    _literal,
    _request_headers,
    _varint,
    encode_subfiles,
    encode_upload_page,
    files_from_subfiles,
    grpc_unary,
    read_back,
    uploads_from_list,
)
from tg2cloud.channel_bulk.remote_evidence import assess


def _grpc(payload: bytes) -> bytes:
    return b"\x00" + len(payload).to_bytes(4, "big") + payload


def _file_blob(file_id: str, name: str, size: int) -> bytes:
    body = (
        _field(1, 2, file_id.encode())
        + _field(2, 2, name.encode())
        + _field(4, 0, _varint(size))
        + _field(34, 0, b"\x01")
        + _field(37, 0, b"\x00")
    )
    return _grpc(_field(1, 2, body))


def _raw_file(file_id: str, name: str, size: int) -> bytes:
    return _grpc(
        _field(1, 2, file_id.encode())
        + _field(2, 2, name.encode())
        + _field(4, 0, _varint(size))
        + _field(34, 0, b"\x01")
        + _field(37, 0, b"\x00")
    )


async def _no_sleep(_delay: float) -> None:
    return None


class CodecTests(unittest.TestCase):
    def test_round_trip_file_and_huffman(self):
        from tg2cloud.channel_bulk.cd2api import HUFFMAN

        blob = _file_blob("file-1", "clip.mp4", 10)
        found = files_from_subfiles(blob)
        self.assertEqual(found[0]["id"], "file-1")
        self.assertEqual(found[0]["size"], 10)
        self.assertTrue(found[0]["is_cloud_file"])
        self.assertFalse(found[0]["is_local"])
        self.assertTrue(encode_subfiles("/library").endswith(b"/library\x10\x01"))
        bits = ""
        for char in "ok":
            code, length = HUFFMAN[ord(char)]
            bits += f"{code:0{length}b}"
        while len(bits) % 8:
            bits += "1"
        encoded = int(bits, 2).to_bytes(len(bits) // 8, "big")
        self.assertEqual(_huffman(encoded), "ok")
        headers = _request_headers("127.0.0.1:9", "GetSubFiles", "test-token")
        self.assertIn(b"test-token", headers)
        self.assertEqual(uploads_from_list(_grpc(_field(2, 2, _field(2, 2, b"/library/clip.mp4") + _field(8, 0, _varint(3))))), [
            {"dest": "/library/clip.mp4", "status": 3}
        ])


class ReadBackTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_token_does_not_connect(self):
        env = os.environ.copy()
        env.pop("TG2CLOUD_CD2_API_TOKEN", None)
        with patch.dict(os.environ, env, clear=True), patch(
            "tg2cloud.channel_bulk.cd2api.grpc_unary", side_effect=AssertionError("network")
        ):
            observed = await read_back("library/clip.mp4", expected_size=10)
        self.assertEqual(observed["scheme"], "webdav")
        self.assertFalse(observed["read_back"])
        self.assertEqual(assess(
            {"scheme": "clouddrive2-upload-task", "status": "Finish", "size": 10, "dest_path": "library/clip.mp4"},
            expected_size=10, expected_dest="library/clip.mp4", expected_fingerprint=None,
        ), "reject")

    async def test_file_id_accepts_and_size_or_inflight_does_not(self):
        good = _file_blob("file-1", "clip.mp4", 10)
        uploads = _grpc(b"")

        async def rpc(method, payload):
            if method == "GetUploadFileList":
                return uploads
            return good

        observed = await read_back("library/clip.mp4", expected_size=10, rpc=rpc)
        self.assertEqual(assess(
            observed, expected_size=10, expected_dest="library/clip.mp4", expected_fingerprint="sample",
        ), "accept")
        self.assertNotIn("content_sha256", observed)
        self.assertEqual(assess(
            observed, expected_size=11, expected_dest="library/clip.mp4", expected_fingerprint=None,
        ), "reject")
        busy = _grpc(_field(2, 2, _field(2, 2, b"/library/clip.mp4") + _field(8, 0, _varint(3))))

        async def busy_rpc(method, payload):
            return busy if method == "GetUploadFileList" else good

        held = await read_back("library/clip.mp4", expected_size=10, rpc=busy_rpc)
        self.assertNotEqual(assess(
            held, expected_size=10, expected_dest="library/clip.mp4", expected_fingerprint=None,
        ), "accept")
        self.assertFalse(safe("../secret.mp4"))

    async def test_loopback_unary_and_audit_omits_path(self):
        from tg2cloud.channel_bulk.cd2api import _PREFACE, _frame
        from tg2cloud.channel_bulk.local_control import _audit

        payload = _file_blob("file-9", "clip.mp4", 4)

        async def serve(reader, writer):
            await reader.readexactly(len(_PREFACE))
            while True:
                header = await reader.readexactly(9)
                length = int.from_bytes(header[:3], "big")
                flags = header[4]
                ftype = header[3]
                if length:
                    await reader.readexactly(length)
                if ftype == 0 and flags & 1:
                    break
            writer.write(_frame(0x4, 0, 0, b""))
            writer.write(_frame(0x4, 0x1, 0, b""))
            writer.write(_frame(0x1, 0x4, 1, _literal("grpc-status", "0")))
            writer.write(_frame(0x0, 0x1, 1, payload))
            await writer.drain()
            writer.close()

        server = await asyncio.start_server(serve, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        try:
            blob = await grpc_unary("127.0.0.1", port, "GetSubFiles", encode_subfiles("/library"), "test-token", tls=False)
        finally:
            server.close()
            await server.wait_closed()
        self.assertEqual(files_from_subfiles(blob)[0]["id"], "file-9")
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            _audit(directory, {
                "uid": 1, "pid": 2, "command": "remote.verify", "request_id": "verify001",
                "ok": True, "error": "Bearer test-token",
            })
            text = (directory / "audit.log").read_text(encoding="utf-8")
        self.assertNotIn("test-token", text)
        self.assertIn("Bearer [REDACTED]", text)

    async def test_verify_command_rejects_escape_and_missing_token(self):
        import tempfile

        from tests.test_channel_local import ask
        from tg2cloud.channel_bulk.local_control import start_local_control
        from tg2cloud.persistence.sqlite import TaskDB

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = TaskDB(root / "tasks.db")
            owner = type("O", (), {})()
            owner.settings = type("S", (), {"data_dir": root, "allowed_user_id": 1})()
            owner.db = db
            owner.log = type("L", (), {"warning": staticmethod(lambda *a, **k: None)})()
            control = await start_local_control(owner)
            sock = root / "channel-bulk" / "control.sock"
            try:
                escaped = await ask(sock, {
                    "command": "remote.verify", "request_id": "verifyesc",
                    "paths": ["../clip.mp4"], "sizes": [4],
                })
                self.assertEqual(escaped["error"], "bad-request")
                env = os.environ.copy()
                env.pop("TG2CLOUD_CD2_API_TOKEN", None)
                with patch.dict(os.environ, env, clear=True):
                    missing = await ask(sock, {
                        "command": "remote.verify", "request_id": "verifymiss",
                        "paths": ["library/clip.mp4"], "sizes": [4],
                    })
                self.assertEqual(missing["matched"], 0)
                self.assertFalse(missing["items"][0]["accepted"])
                audit = (root / "channel-bulk" / "audit.log").read_text(encoding="utf-8")
                self.assertNotIn("library/clip.mp4", audit)

                async def inconclusive(path, expected_size=None, **kwargs):
                    del path, kwargs
                    return {
                        "scheme": "clouddrive2-file",
                        "read_back": False,
                        "cache_only": None,
                        "destination_id": "file-kept",
                        "size": expected_size,
                        "dest_path": "library/clip.mp4",
                        "is_cloud_file": True,
                        "is_local": False,
                        "listing": "ok",
                        "upload_list": "inconclusive",
                    }

                with patch("tg2cloud.channel_bulk.cd2api.read_back", inconclusive):
                    shown = await ask(sock, {
                        "command": "remote.verify", "request_id": "verifyhold",
                        "paths": ["library/clip.mp4"], "sizes": [4],
                    })
                self.assertFalse(shown["items"][0]["accepted"])
                self.assertEqual(shown["items"][0]["listing"], "ok")
                self.assertEqual(shown["items"][0]["upload_list"], "inconclusive")
                self.assertEqual(shown["items"][0]["file_id"], "file-kept")
                self.assertTrue(shown["items"][0]["size_ok"])
                audit = (root / "channel-bulk" / "audit.log").read_text(encoding="utf-8")
                self.assertNotIn("file-kept", audit)
                self.assertNotIn("library/clip.mp4", audit)
            finally:
                await control.close()
                db.close()


    async def test_single_file_lookup_and_upload_list_timeout(self):
        from tg2cloud.channel_bulk.cd2api import idle_timeout

        calls = []

        async def direct(method, payload):
            calls.append(method)
            if method == "FindFileByPath":
                return _raw_file("file-1", "clip.mp4", 10)
            if method == "GetUploadFileList":
                self.assertEqual(payload, encode_upload_page("clip.mp4", 1))
                self.assertNotIn(b"\x08\x01", payload)
                return _grpc(b"")
            raise AssertionError(method)

        observed = await read_back(
            "library/clip.mp4", expected_size=10, rpc=direct, sleep=_no_sleep, rng=lambda: 0,
        )
        self.assertEqual(calls, ["FindFileByPath", "GetUploadFileList"])
        self.assertEqual(assess(
            observed, expected_size=10, expected_dest="library/clip.mp4", expected_fingerprint=None,
        ), "accept")

        async def upload_down(method, payload):
            del payload
            if method == "FindFileByPath":
                return _raw_file("file-1", "clip.mp4", 10)
            raise TimeoutError(method)

        held = await read_back(
            "library/clip.mp4", expected_size=10, rpc=upload_down, sleep=_no_sleep, rng=lambda: 0,
        )
        self.assertEqual(held["listing"], "ok")
        self.assertEqual(held["upload_list"], "inconclusive")
        self.assertEqual(held["destination_id"], "file-1")
        self.assertFalse(held["read_back"])
        self.assertEqual(assess(
            held, expected_size=10, expected_dest="library/clip.mp4", expected_fingerprint=None,
        ), "reject")

        async def all_down(method, payload):
            del method, payload
            raise TimeoutError("down")

        missed = await read_back(
            "library/clip.mp4", expected_size=10, rpc=all_down, sleep=_no_sleep, rng=lambda: 0,
        )
        self.assertEqual(missed["listing"], "timeout")
        self.assertEqual(missed["upload_list"], "not_asked")
        self.assertFalse(missed["destination_id"])
        self.assertNotEqual(missed["listing"], "missing")

        async def absent(method, payload):
            del method, payload
            return _grpc(b"")

        gone = await read_back(
            "library/clip.mp4", expected_size=10, rpc=absent, sleep=_no_sleep, rng=lambda: 0,
        )
        self.assertEqual(gone["listing"], "missing")
        self.assertFalse(gone["destination_id"])

        attempts = {"upload": 0}

        async def flaky(method, payload):
            del payload
            if method == "GetSearchResults":
                return _file_blob("file-9", "clip.mp4", 10)
            if method == "FindFileByPath":
                raise TimeoutError("find")
            if method == "GetSubFiles":
                raise AssertionError("listing")
            attempts["upload"] += 1
            if attempts["upload"] < 3:
                raise TimeoutError("upload")
            return _grpc(b"")

        recovered = await read_back(
            "library/clip.mp4", expected_size=10, rpc=flaky, sleep=_no_sleep, rng=lambda: 0,
        )
        self.assertEqual(attempts["upload"], 3)
        self.assertEqual(assess(
            recovered, expected_size=10, expected_dest="library/clip.mp4", expected_fingerprint=None,
        ), "accept")
        with patch.dict(os.environ, {"TG2CLOUD_CD2_API_TIMEOUT_SECONDS": "90"}):
            self.assertEqual(idle_timeout(), 90.0)
        with patch.dict(os.environ, {"TG2CLOUD_CD2_API_TIMEOUT_SECONDS": "999"}):
            self.assertEqual(idle_timeout(), 60.0)


def safe(path: str) -> bool:
    from tg2cloud.channel_bulk.cd2api import safe_relative
    return safe_relative(path)


if __name__ == "__main__":
    unittest.main()
