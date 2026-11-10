from __future__ import annotations

import asyncio
import signal

from tg2cloud.config import Settings
from tg2cloud.logging_setup import setup_logging
from tg2cloud.monitoring.resources import (
    ResourceMonitor,
)
from tg2cloud.persistence.paths import database_path
from tg2cloud.persistence.sqlite import TaskDB
from tg2cloud.storage.rclone import RcloneClient
from tg2cloud.telegram.client import create_client
from tg2cloud.telegram.commands import CommandHandlers
from tg2cloud.telegram.handlers import TelegramHandlers
from tg2cloud.telegram.views import TaskViews
from tg2cloud.transfer.ports import Destination, MediaSource
from tg2cloud.transfer.service import TaskService


class TransferService(TaskService, TelegramHandlers, CommandHandlers, TaskViews):
    def __init__(self, settings: Settings, *, source: MediaSource | None = None,
                 destination: Destination | None = None):
        database = database_path(settings.data_dir)
        setup_logging(settings)
        client = create_client(settings)
        super().__init__(settings, source=source, client=client,
                         repository=TaskDB(database),
                         destination=destination or RcloneClient(settings),
                         monitor=ResourceMonitor(settings.download_dir, 数据库=settings.data_dir,
                                                 日志=settings.log_dir, 配置=settings.rclone_config_path.parent))
        from tg2cloud.listening.service import Listener
        self.listener = Listener(self)
        self._register_handlers()

    async def start(self) -> None:
        recovered = self.db.recover(self.settings.download_dir)
        self.log.info("重启恢复结果：%s", recovered)
        from tg2cloud.channel_bulk.jobs import hold_on_startup

        held = hold_on_startup(self.db)
        self.log.info("频道批量保持暂停：%s", held)
        await self.client.start(bot_token=self.settings.bot_token)
        me = await self.client.get_me()
        self.log.info("Telegram Bot 已登录：@%s", me.username)
        await self._register_bot_menu()
        from tg2cloud.monitoring.notifications import run as notifications
        self._background = [
            asyncio.create_task(self.listener.run(), name="optional-listener"),
            asyncio.create_task(notifications(self), name="optional-notifications"),
            asyncio.create_task(self._resource_loop(), name="resource-loop"),
            asyncio.create_task(self._destination_loop(), name="destination-loop"),
            asyncio.create_task(self._scheduler_loop(), name="scheduler-loop"),
            asyncio.create_task(self._watch_loop(), name="watch-loop"),
        ]
        await self._notify(
            "🤖 TG2Cloud 服务已启动。可使用输入框左侧菜单，"
            "或发送 /status 查看状态。"
        )

    async def stop(self) -> None:
        self._stop.set()
        all_tasks = (
            self._background
            + list(self.download_tasks.values())
            + list(self.upload_tasks.values())
        )
        for task in all_tasks:
            task.cancel()
        if all_tasks:
            await asyncio.gather(*all_tasks, return_exceptions=True)
        await self.listener.close()
        await self.client.disconnect()
        self.db.close()

    async def run(self) -> None:
        await self.start()
        await self._stop.wait()

async def async_main() -> None:
    settings = Settings.from_env()
    service = TransferService(settings)
    loop = asyncio.get_running_loop()
    for signame in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signame, service._stop.set)
        except NotImplementedError:
            pass
    try:
        await service.run()
    finally:
        await service.stop()

if __name__ == "__main__":
    asyncio.run(async_main())
