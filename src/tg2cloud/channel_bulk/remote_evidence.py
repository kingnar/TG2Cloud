"""Fail-closed remote acceptance.

CloudDrive2 documents a gRPC upload list, ``GetUploadFileList``
(``UploadFileInfo``: destPath, size, transferredBytes, status Finish=5).
That record is the gateway's own task flag. It has no source content hash and
no downstream cloud file id. ``FindFileByPath`` can return ``CloudDriveFile.id``,
but the same API is allowed to answer from directory cache. ``isCloudFile``
means the path is a cloud node, not that the bytes have left the local buffer.

The process only has WebDAV settings. WebDAV size is the cache-facing check and
is never acceptance. This module does not log in, does not read a token, and
does not call the gateway. A later verifier may supply an observation only
after a real post-Finish read-back; until then bulk upload stays retained.
"""
from __future__ import annotations

from typing import Any


def assess(
    observation: Any,
    *,
    expected_size: int,
    expected_dest: str,
    expected_fingerprint: str | None,
) -> str:
    """Return ``accept`` only for a complete post-upload read-back. Otherwise ``reject``."""
    if not isinstance(observation, dict):
        return "reject"
    if observation.get("scheme") == "webdav" or observation.get("method") == "size":
        return "reject"
    if observation.get("cache_only") is not False or observation.get("read_back") is not True:
        return "reject"
    if observation.get("scheme") != "clouddrive2-upload-task" or observation.get("status") != "Finish":
        return "reject"
    if not expected_fingerprint or observation.get("source_fingerprint") != expected_fingerprint:
        return "reject"
    destination_id = observation.get("destination_id")
    if (
        not isinstance(destination_id, str)
        or not destination_id
        or len(destination_id) > 128
        or any(ord(char) < 32 for char in destination_id)
    ):
        return "reject"
    if observation.get("dest_path") != expected_dest:
        return "reject"
    try:
        size = int(observation.get("size"))
        moved = int(observation.get("transferred_bytes"))
    except (TypeError, ValueError):
        return "reject"
    if size != int(expected_size) or moved != size or size < 0:
        return "reject"
    return "accept"
