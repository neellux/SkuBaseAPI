"""Daily refresh of essx_shoot_list from SellerCloud scheduled task 339.

Task 339 runs every 15 minutes, so the cycle reads its newest completed output rather than
waiting on a run of its own. See services/essx_shoot_list_service.py for the rules.

Scheduled with APScheduler at a fixed wall-clock time, mirroring daily_image_import_poller.
"""

import logging
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from config import config

logger = logging.getLogger(__name__)

JOB_ID = "essx_shoot_list_daily"


class EssxShootListPoller:
    """Runs one refresh per day at a fixed wall-clock time via APScheduler."""

    def __init__(self) -> None:
        self.name = self.__class__.__name__
        cfg = config.get("essx_shoot_list_poller", {})
        self.enabled: bool = bool(cfg.get("enabled", False))
        self.task_id: int = int(cfg.get("task_id", 339))
        # SellerCloud reports the task's run times in the account's own timezone.
        self.task_timezone: str = cfg.get("task_timezone", "America/New_York")
        self.session_days: int = int(cfg.get("session_days", 5))
        self.max_age_hours: float = float(cfg.get("max_age_hours", 26))
        self.min_candidate_ratio: float = float(cfg.get("min_candidate_ratio", 0.5))
        self._schedule_hour: int = int(cfg.get("daily_hour", 6))
        self._schedule_minute: int = int(cfg.get("daily_minute", 0))
        self._schedule_tz: ZoneInfo = ZoneInfo(cfg.get("timezone", "America/New_York"))
        self._scheduler: Optional[AsyncIOScheduler] = None

    async def start(self) -> None:
        if not self.enabled:
            logger.info(f"{self.name}: disabled in config, skipping start")
            return
        if self._scheduler and self._scheduler.running:
            logger.info(f"{self.name}: already running")
            return

        self._scheduler = AsyncIOScheduler(timezone=self._schedule_tz)
        self._scheduler.add_job(
            self._poll_cycle,
            trigger=CronTrigger(
                hour=self._schedule_hour,
                minute=self._schedule_minute,
                timezone=self._schedule_tz,
            ),
            id=JOB_ID,
            name=self.name,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
        self._scheduler.start()
        next_run = self._scheduler.get_job(JOB_ID).next_run_time
        logger.info(
            f"{self.name}: scheduled daily at "
            f"{self._schedule_hour:02d}:{self._schedule_minute:02d} "
            f"{self._schedule_tz.key}; next run at {next_run.isoformat(timespec='seconds')}"
        )

    async def stop(self) -> None:
        if not self._scheduler:
            return
        logger.info(f"{self.name}: stopping...")
        self._scheduler.shutdown(wait=False)
        self._scheduler = None
        logger.info(f"{self.name}: stopped")

    async def run_once(self, **overrides: Any) -> Dict[str, Any]:
        """One refresh now (scripts, ops). Raises, unlike the scheduled cycle."""
        from services.essx_shoot_list_service import refresh

        return await refresh(**{
            "task_id": self.task_id,
            "session_days": self.session_days,
            "max_age_hours": self.max_age_hours,
            "min_candidate_ratio": self.min_candidate_ratio,
            "task_timezone": self.task_timezone,
            **overrides,
        })

    async def _poll_cycle(self) -> Optional[Dict[str, Any]]:
        """Never raises: a failed refresh leaves yesterday's list in place."""
        try:
            return await self.run_once()
        except Exception:  # noqa: BLE001
            logger.exception(f"{self.name}: refresh failed; the previous list stays")
            return None


essx_shoot_list_poller = EssxShootListPoller()
