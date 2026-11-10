"""Remote filenames for channel bulk. Caption first, collisions only when needed."""
from __future__ import annotations

import re
from pathlib import Path

_ILLEGAL = re.compile(r'[\\/:*?"<>|\x00-\x1f\x7f]')
_URL = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
_HASHTAG = re.compile(r"#[^\s#]+")
_SPACE = re.compile(r"\s+")
_EXT = re.compile(r"^\.[A-Za-z0-9]{1,8}$")
_DIGITS = re.compile(r"^\d+$")


class SourceMissing(Exception):
    """The channel message is gone or has no downloadable media."""


def first_caption_line(text: str | None) -> str:
    for line in str(text or "").splitlines():
        cleaned = line.strip()
        if cleaned:
            return cleaned
    return ""


def _cap_utf8(value: str, max_bytes: int) -> str:
    encoded = value.encode("utf-8")
    if len(encoded) <= max_bytes:
        return value
    return encoded[:max_bytes].decode("utf-8", errors="ignore")


def sanitize_stem(text: str | None) -> str:
    """Keep Chinese and ordinary punctuation. Drop paths, URLs, and hashtags."""
    stem = first_caption_line(text)
    stem = _URL.sub(" ", stem)
    stem = _HASHTAG.sub(" ", stem)
    stem = _ILLEGAL.sub("", stem)
    stem = _SPACE.sub(" ", stem).strip(" .")
    return _cap_utf8(stem, 200)


def media_extension(file_name: str | None, mime: str | None) -> str:
    suffix = Path(str(file_name or "")).suffix
    if _EXT.fullmatch(suffix):
        return suffix
    if mime and "mp4" in mime.lower():
        return ".mp4"
    return ".mp4"


def fallback_filename(file_name: str | None, message_id: int, mime: str | None) -> str:
    stem = sanitize_stem(Path(str(file_name or "")).stem)
    ext = media_extension(file_name, mime)
    if stem and not _DIGITS.fullmatch(stem):
        return f"{stem}{ext}"
    return f"video-{int(message_id)}{ext}"


def display_filename(caption: str | None, file_name: str | None, message_id: int, mime: str | None) -> str:
    stem = sanitize_stem(caption)
    ext = media_extension(file_name, mime)
    if stem:
        return f"{stem}{ext}"
    return fallback_filename(file_name, message_id, mime)


def has_task_suffix(remote_path: str) -> bool:
    return bool(re.search(r" \(task-\d+\)", Path(str(remote_path or "")).name))


def name_candidates(filename: str, message_id: int) -> list[str]:
    path = Path(filename)
    stem, ext = path.stem, path.suffix or ".mp4"
    message = int(message_id)
    names = [f"{stem}{ext}", f"{stem} (message-{message}){ext}"]
    names.extend(f"{stem} (message-{message}-{number}){ext}" for number in range(2, 100))
    return names


def join_remote(directory: str, name: str) -> str:
    return f"{directory}/{name}" if directory else name


async def allocate_remote(directory: str, filename: str, message_id: int, taken) -> str:
    """Use the plain name. Add a message id only when that name is already taken."""
    for name in name_candidates(filename, message_id):
        full = join_remote(directory, name)
        if not await taken(full):
            return full
    raise RuntimeError("文件名冲突过多，已停止该条")
