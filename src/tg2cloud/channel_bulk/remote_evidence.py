"""Fail-closed remote acceptance.

WebDAV size is never success. CloudDrive2 upload-task ``Finish`` is never
success either: that flag has no destination file id. The only accept path is
a file read-back (``GetSubFiles`` with ``forceRefresh``) that returns
``CloudDriveFile.id``, a matching size, ``isCloudFile``, and ``isLocal=false``.

``CloudDriveFile.fileHashes`` may include MD5 or SHA1. It does not include the
local head/mid/tail sample SHA-256, so that sample is not compared. If an
observation nevertheless carries ``content_sha256``, it must match the local
sample or the read-back is rejected.
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
    """Return ``accept`` only for a cloud file id read-back. Otherwise ``reject``."""
    if not isinstance(observation, dict):
        return "reject"
    if observation.get("scheme") in {"webdav", "clouddrive2-upload-task"} or observation.get("method") == "size":
        return "reject"
    if observation.get("scheme") != "clouddrive2-file":
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
