"""Fail-closed remote acceptance.

WebDAV size is never success. CloudDrive2 upload-task ``Finish`` is never
success either: that flag has no destination file id. The only accept path is
a file read-back (``GetSubFiles`` with ``forceRefresh``) that returns
``CloudDriveFile.id``, a matching size, ``isCloudFile``, and ``isLocal=false``.

``CloudDriveFile.fileHashes`` may include MD5 or SHA1. It does not include the
local head/mid/tail sample SHA-256, so that sample is not compared. If an
observation nevertheless carries ``content_sha256``, it must match the local
sample or the read-back is rejected. ``upload_list=clear`` requires an empty ``GetUploadFileCount`` or a live
``getAll`` queue that does not contain the destination. ``unsupported`` means
that count call failed. It is not acceptance unless the operator turns on
``TG2CLOUD_CD2_UPLOAD_ATTEST=1`` and the verify request sets ``upload_attest``.
A listing ``timeout`` is not a missing file.
"""
from __future__ import annotations

import os
import re
from typing import Any

ATTEST_ENV = "TG2CLOUD_CD2_UPLOAD_ATTEST"


def assess(
    observation: Any,
    *,
    expected_size: int,
    expected_dest: str,
    expected_fingerprint: str | None,
) -> str:
    """Return ``accept`` only for a cloud file id read-back. Otherwise ``reject``."""
    if not isinstance(observation, dict):
        return "reject"
    if observation.get("scheme") in {"webdav", "clouddrive2-upload-task"} or observation.get("method") == "size":
        return "reject"
    if observation.get("scheme") != "clouddrive2-file":
        return "reject"
    if observation.get("listing") not in {None, "ok"}:
        return "reject"
    if observation.get("upload_list") not in {None, "clear"}:
        return "reject"
    if observation.get("read_back") is not True or observation.get("cache_only") is not False:
        return "reject"
    if observation.get("is_cloud_file") is not True or observation.get("is_local") is not False:
        return "reject"
    destination_id = observation.get("destination_id")
    if (
        not isinstance(destination_id, str)
        or not destination_id
        or len(destination_id) > 256
        or any(ord(char) < 32 for char in destination_id)
    ):
        return "reject"
    if observation.get("dest_path") != expected_dest:
        return "reject"
    try:
        size = int(observation.get("size"))
    except (TypeError, ValueError):
        return "reject"
    if size != int(expected_size) or size < 0:
        return "reject"
    if "content_sha256" in observation and observation.get("content_sha256") != expected_fingerprint:
        return "reject"
    return "accept"


def _short(value: Any, limit: int) -> str:
    return str(value or "")[:limit]


_UPLOAD_ERRORS = {"", "empty", "timeout", "status", "no-status", "protocol"}
_FRAMES = re.compile(r"[0-9]{1,3}:[0-9]{1,3}(?:,[0-9]{1,3}:[0-9]{1,3}){0,7}")


def _grpc_status(value: Any) -> str:
    text = str(value or "").strip()
    if text.isdigit() and len(text) <= 2 and text != "0":
        return text
    return ""


def _frames(value: Any) -> str:
    text = str(value or "")
    if _FRAMES.fullmatch(text):
        return text
    return ""


def _flag(observed: dict, key: str):
    if key not in observed or observed[key] is None:
        return None
    if type(observed[key]) is not bool:
        return None
    return observed[key]


def _millis(value: Any):
    if type(value) is not int or isinstance(value, bool) or not 0 <= value <= 600_000:
        return None
    return value


def _attempts(value: Any) -> int:
    if type(value) is not int or isinstance(value, bool) or value < 0:
        return 0
    return min(value, 99)


def verification_report(observed: dict, *, size: int, dest: str, attest: bool = False) -> dict:
    """Operator view of one path. Bulk acceptance still uses ``assess`` only."""
    file_id = observed.get("destination_id") if isinstance(observed.get("destination_id"), str) else ""
    accepted = assess(
        observed, expected_size=size, expected_dest=dest, expected_fingerprint=None,
    ) == "accept"
    attested = False
    if (
        not accepted
        and observed.get("upload_list") == "unsupported"
        and observed.get("listing") == "ok"
        and attest is True
        and os.environ.get(ATTEST_ENV) == "1"
        and file_id
        and observed.get("is_cloud_file") is True
        and observed.get("is_local") is False
        and observed.get("size") == size
        and observed.get("dest_path") == dest
    ):
        accepted = True
        attested = True
    return {
        "accepted": accepted,
        "attested": attested,
        "has_file_id": bool(file_id),
        "size_ok": observed.get("size") == size,
        "listing": _short(observed.get("listing") or "failed", 32),
        "listing_rpc": _short(observed.get("listing_rpc"), 64),
        "listing_attempts": _attempts(observed.get("listing_attempts")),
        "listing_error": _short(observed.get("listing_error"), 16),
        "upload_list": _short(observed.get("upload_list") or "not_asked", 32),
        "upload_rpc": _short(observed.get("upload_rpc"), 64),
        "upload_attempts": _attempts(observed.get("upload_attempts")),
        "upload_error": observed.get("upload_error") if observed.get("upload_error") in _UPLOAD_ERRORS else "protocol",
        "upload_status": _grpc_status(observed.get("upload_status")),
        "upload_status_present": _flag(observed, "upload_status_present"),
        "upload_end_stream": _flag(observed, "upload_end_stream"),
        "upload_frames": _frames(observed.get("upload_frames")),
        "upload_saw_frame": _flag(observed, "upload_saw_frame"),
        "upload_ms": _millis(observed.get("upload_ms")),
        "file_id": file_id[:256],
    }
