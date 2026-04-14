import asyncio
import logging
from abc import ABC, abstractmethod

from config import config

logger = logging.getLogger(__name__)


class BasePoller(ABC):

    def __init__(self, config_section: str, name: str | None = None) -> None:
        cfg = config.get(config_section, {})
        self.name: str = name or self.__class__.__name__
        self.enabled: bool = cfg.get("enabled", True)
        self.interval: int = max(cfg.get("interval_seconds", 60), 10)
        self._shutdown_event: asyncio.Event = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        if not self.enabled:
            logger.info(f"{self.name}: disabled in config, skipping start")
            return
        logger.info(f"{self.name}: starting (interval={self.interval}s)")
        self._task = asyncio.create_task(self._run())
        self._task.set_name(self.name)

    async def stop(self) -> None:
        if not self._task:
            return
        logger.info(f"{self.name}: stopping...")
        self._shutdown_event.set()
        try:
            await asyncio.wait_for(self._task, timeout=15)
        except asyncio.TimeoutError:
            logger.warning(f"{self.name}: shutdown timed out, cancelling task")
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info(f"{self.name}: stopped")

    async def _run(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await self._poll_cycle()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(f"{self.name}: poll cycle error")

            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=self.interval)
                break
            except asyncio.TimeoutError:
                pass

    @abstractmethod
    async def _poll_cycle(self) -> None: ...
