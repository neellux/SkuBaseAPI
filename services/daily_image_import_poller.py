"""
Daily task that back-fills SellerCloud product images for our active children that
have none.

Flow (once per day, after the SellerCloud scheduled task 346 export has run):
  1. Read the latest same-day output of SC scheduled task 346 — a one-column
     (ProductID) list of products with no image — via
     `sellercloud_internal_service.get_scheduled_no_image_ids`.
  2. Intersect with our active `child_products` (drops non-children, gives the
     authoritative parent_sku).
  3. Keep only children whose parent has a GCS `1_1500.jpg` (HEAD pre-check), so the
     import contains rows that can actually succeed.
  4. Build the `image_upload_format` tab-separated file (one row per child pointing at
     the parent's GCS image) and submit it to `/Catalog/Imports/Images` in chunks.

Scheduled with APScheduler at a fixed wall-clock time, mirroring
`secondary_inventory_transfer_poller`.
"""

import asyncio
import io
import logging
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote
from zoneinfo import ZoneInfo

import httpx
import pandas as pd
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from tortoise import connections

from config import config
from services.sellercloud_internal_service import sellercloud_internal_service
from services.sellercloud_service import sellercloud_service

logger = logging.getLogger(__name__)

# image_upload_format column order — must match SellerCloud's image import schema
# (see PhotoManagementNew/API/utils/sellercloud.py:update_images_new).
_IMPORT_COLUMNS = [
    "ProductID", "ImageID", "ImageURL", "IsDefault", "IsMainDescriptionImage",
    "IsSupplementImage", "SupplementImageOrder", "IsOtherImage", "IsSwatchImage",
    "Caption", "ImageSource", "IsWarehouseImage", "_ACTION_",
]


def _build_image_import_tsv(rows: List[Tuple[str, str]]) -> bytes:
    """rows: list of (child_sku, image_url). Returns the tab-separated import file bytes."""
    records = []
    for sku, url in rows:
        records.append({
            "ProductID": sku,
            "ImageID": None,
            "ImageURL": url,
            "IsDefault": True,
            "IsMainDescriptionImage": True,
            "IsSupplementImage": False,
            "SupplementImageOrder": None,
            "IsOtherImage": None,
            "IsSwatchImage": None,
            "Caption": None,
            "ImageSource": None,
            "IsWarehouseImage": None,
            "_ACTION_": None,
        })
    df = pd.DataFrame(records, columns=_IMPORT_COLUMNS)
    buf = io.StringIO()
    df.to_csv(buf, index=False, sep="\t")
    return buf.getvalue().encode("utf-8")


class DailyImageImportPoller:
    """Runs one cycle per day at a fixed wall-clock time via APScheduler."""

    def __init__(self) -> None:
        self.name = self.__class__.__name__
        cfg = config.get("daily_image_import_poller", {})
        self.enabled: bool = cfg.get("enabled", True)
        self.related_task_id: int = int(cfg.get("related_task_id", 346))
        self.require_same_day: bool = bool(cfg.get("require_same_day", True))
        self.gcs_resolution: str = cfg.get("gcs_resolution", "1_1500")
        self.gcs_precheck: bool = bool(cfg.get("gcs_precheck", True))
        self.gcs_concurrency: int = int(cfg.get("gcs_concurrency", 32))
        self.import_chunk_size: int = int(cfg.get("import_chunk_size", 5000))
        self._schedule_hour: int = int(cfg.get("daily_hour", 5))
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
        trigger = CronTrigger(
            hour=self._schedule_hour,
            minute=self._schedule_minute,
            timezone=self._schedule_tz,
        )
        self._scheduler.add_job(
            self._poll_cycle,
            trigger=trigger,
            id="daily_image_import_daily",
            name=self.name,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
        self._scheduler.start()

        next_run = self._scheduler.get_job("daily_image_import_daily").next_run_time
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

    # ------------------------------------------------------------------
    # Cycle
    # ------------------------------------------------------------------
    async def _poll_cycle(self) -> None:
        no_image_ids = await sellercloud_internal_service.get_scheduled_no_image_ids(
            related_task_id=self.related_task_id,
            require_same_day=self.require_same_day,
            tz=self._schedule_tz.key,
        )
        if not no_image_ids:
            logger.info(f"{self.name}: no same-day no-image list; nothing to do")
            return

        children = await self._resolve_children(no_image_ids)
        logger.info(
            f"{self.name}: {len(no_image_ids)} no-image ids -> {len(children)} active children"
        )
        if not children:
            return

        if self.gcs_precheck:
            fixable = await self._filter_by_gcs(children)
            logger.info(
                f"{self.name}: {len(fixable)} children have a GCS {self.gcs_resolution} image "
                f"({len(children) - len(fixable)} skipped — un-photographed)"
            )
        else:
            fixable = [(sku, self._gcs_url(parent)) for sku, parent in children]
        if not fixable:
            logger.info(f"{self.name}: nothing fixable this run")
            return

        job_ids: List[str] = []
        for i in range(0, len(fixable), self.import_chunk_size):
            chunk = fixable[i : i + self.import_chunk_size]
            tsv = _build_image_import_tsv(chunk)
            try:
                job_id = await sellercloud_service.create_image_import(tsv)
                job_ids.append(job_id)
                logger.info(
                    f"{self.name}: submitted import job {job_id} for {len(chunk)} products"
                )
            except Exception:
                logger.exception(
                    f"{self.name}: import submit failed for chunk "
                    f"{i // self.import_chunk_size + 1}"
                )

        logger.info(
            f"{self.name}: done — {len(fixable)} products across {len(job_ids)} import "
            f"jobs {job_ids}"
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    async def _resolve_children(self, product_ids: List[str]) -> List[Tuple[str, str]]:
        """Keep only ids that are our active children; return (sku, parent_sku)."""
        conn = connections.get("product_db")
        rows: List[Dict[str, Any]] = await conn.execute_query_dict(
            "SELECT sku, parent_sku FROM child_products "
            "WHERE sku = ANY($1::text[]) AND is_active = TRUE",
            [product_ids],
        )
        return [(r["sku"], r["parent_sku"]) for r in rows if r.get("parent_sku")]

    def _gcs_url(self, parent_sku: str) -> str:
        return (
            f"https://storage.googleapis.com/lux_products/"
            f"{quote(parent_sku, safe='/')}/{self.gcs_resolution}.jpg"
        )

    async def _filter_by_gcs(
        self, children: List[Tuple[str, str]]
    ) -> List[Tuple[str, str]]:
        """Keep children whose parent has a GCS image (HEAD 200). Returns (sku, image_url)."""
        parents = {parent for _, parent in children}
        exists: Dict[str, bool] = {}
        sem = asyncio.Semaphore(self.gcs_concurrency)

        async with httpx.AsyncClient(timeout=15.0) as client:
            async def check(parent: str) -> None:
                url = self._gcs_url(parent)
                async with sem:
                    try:
                        resp = await client.head(url)
                        exists[parent] = resp.status_code == 200
                    except Exception:
                        exists[parent] = False

            await asyncio.gather(*(check(p) for p in parents))

        return [
            (sku, self._gcs_url(parent))
            for sku, parent in children
            if exists.get(parent)
        ]


daily_image_import_poller = DailyImageImportPoller()
