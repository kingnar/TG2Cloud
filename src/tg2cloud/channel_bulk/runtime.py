"""Bind bulk I/O to the existing listener client and the existing rclone process.

No second Telegram client and no copied session. Download issues one MTProto
request at a time. Upload is paced before each write to the existing stream.
"""
from __future__ import annotations

from pathlib import Path

CHUNK = 512 * 1024


async def download_bounded(client, message, path: Path, rate, size: int) -> None:
    document = getattr(message, "document", None)
    if document is None or int(getattr(document, "size", 0) or 0) != int(size):
        raise RuntimeError("消息没有与清单一致的文档，已停止该条")
    path.parent.mkdir(parents=True, exist_ok=True)
    offset = 0
    with path.open("wb") as handle:
        while offset < int(size):
            progressed = False
            async for chunk in client.iter_download(
                document,
                offset=offset,
                limit=1,
                request_size=CHUNK,
                chunk_size=CHUNK,
                file_size=int(size),
            ):
                await rate.consume(len(chunk))
                handle.write(chunk)
                offset += len(chunk)
                progressed = True
                break
            if not progressed:
                raise RuntimeError("下载在预期大小前结束")
    if offset != int(size):
        raise RuntimeError("下载大小不符")


async def upload_bounded(rclone, path: Path, remote: str, rate) -> None:
    stream = await rclone.open_upload_stream(remote, path.stat().st_size)
    try:
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(CHUNK)
                if not chunk:
                    break
                await rate.consume(len(chunk))
                await stream.write(chunk)
        await stream.finish()
    except BaseException:
        await stream.abort()
        raise


async def probe_remote(rclone, remote: str) -> dict:
    """Ask the CloudDrive2 file API. WebDAV size is not a result."""
    del rclone
    from tg2cloud.channel_bulk.cd2api import read_back

    return await read_back(remote)


class ListenerTransport:
    """Uses listener.with_client, which refuses to construct another client."""

    def __init__(self, owner):
        self.owner = owner

    async def _message(self, client, item: dict):
        from tg2cloud.listening.service import verified_chat

        entity = await verified_chat(client, int(item["chat_id"]))
        message = await client.get_messages(entity, ids=int(item["message_id"]))
        if not message:
            raise RuntimeError("消息已不存在")
        return message

    async def download(self, *, item, path, rate, size):
        listener = self.owner.listener

        async def operation(client):
            message = await self._message(client, item)
            await download_bounded(client, message, path, rate, size)

        await listener.with_client(operation)

    async def upload(self, *, item, path, remote, rate, size):
        del item, size
        await upload_bounded(self.owner.rclone, path, remote, rate)

    async def probe(self, *, item, remote, size, fingerprint=None):
        del item, size, fingerprint
        return await probe_remote(self.owner.rclone, remote)
