"""Carries a gallery image save through to SellerCloud, one step per cycle.

SellerCloud copies an image's bytes once and never looks at the GCS URL again, so an
edited top shot has to be pushed. Pushing means a SellerCloud export job (to learn the
ImageIDs currently attached, which the delete rows need) followed by an import job, each
taking minutes, which is far too long to hold an operator's save open. So the save
records a row and returns, and this poller advances it:

    pending -> exporting -> importing -> completed

Same shape as PhotoManagementNew's handle_upload, including its hard-won rule that a
job-status poll must report "not done" on a transient failure rather than raise, since a
raise is latched into the row's error and the row is then skipped forever.
"""
import hashlib
import logging
import struct
import traceback
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import httpx
from tortoise import connections

from config import config
from services import gallery_image_sync_queue
from services.base_poller import BasePoller
from services.sellercloud_service import sellercloud_service
from utils.sellercloud_image_import import (
    add_default_image_row,
    build_image_import_tsv,
    delete_image_row,
    image_rows_from_export,
)

logger = logging.getLogger(__name__)

GCS_BASE_URL = "https://storage.googleapis.com/lux_products"

# One worker at a time across every process. Within a process BasePoller already awaits
# each cycle before starting the next, but two API instances (or one restarting while a
# cycle is in flight) would otherwise both claim the same job and submit two conflicting
# SellerCloud imports for it.
_CYCLE_LOCK_KEY = struct.unpack(
    ">q", hashlib.sha256(b"gallery_image_sync_poller").digest()[:8]
)[0]


def _gcs_url(parent_sku: str, generation: Optional[str] = None) -> str:
    """Slashes stay literal, everything else is escaped: ESSX parents contain '/'."""
    url = f"{GCS_BASE_URL}/{quote(parent_sku, safe='/')}/1_1500.jpg"
    return f"{url}?v={generation}" if generation else url


class GalleryImageSyncPoller(BasePoller):

    def __init__(self) -> None:
        super().__init__(
            config_section="gallery_image_sync_poller", name="GalleryImageSyncPoller"
        )
        cfg = config.get("gallery_image_sync_poller", {})
        self.batch_size: int = cfg.get("batch_size", 5)
        self.stale_timeout_minutes: int = cfg.get("stale_timeout_minutes", 60)

    async def _poll_cycle(self) -> None:
        # Session-scoped advisory lock held on one pooled connection for the whole
        # cycle, the same guard image_service uses per product. A second worker skips
        # its cycle rather than queueing behind this one: the work is still there next
        # interval, and two workers pushing the same job is the outcome worth avoiding.
        pool = connections.get("default")._pool
        async with pool.acquire() as raw_conn:
            if not await raw_conn.fetchval(
                "SELECT pg_try_advisory_lock($1)", _CYCLE_LOCK_KEY
            ):
                logger.info(f"{self.name}: another worker holds the cycle lock, skipping")
                return
            try:
                await self._run_cycle()
            finally:
                try:
                    await raw_conn.fetchval(
                        "SELECT pg_advisory_unlock($1)", _CYCLE_LOCK_KEY
                    )
                except Exception as unlock_err:
                    logger.warning(f"{self.name}: failed to release cycle lock: {unlock_err}")

    async def _run_cycle(self) -> None:
        stale = await gallery_image_sync_queue.fail_stale(self.stale_timeout_minutes)
        if stale:
            logger.warning(f"{self.name}: failed {stale} job(s) stuck waiting on SellerCloud")

        jobs = await gallery_image_sync_queue.claim_batch(self.batch_size)
        if not jobs:
            return

        for job in jobs:
            if self._shutdown_event.is_set():
                logger.info(f"{self.name}: shutdown requested mid-batch, stopping")
                break
            await self._advance(job)

    async def _advance(self, job: Dict[str, Any]) -> None:
        job_id = job["id"]
        try:
            if job["status"] == "pending":
                await self._start_export(job)
            elif job["status"] == "exporting":
                await self._start_import(job)
            elif job["status"] == "importing":
                await self._finish(job)
        except Exception as e:
            logger.exception(
                f"{self.name}: job {job_id} failed for {job['product_id']}"
            )
            await gallery_image_sync_queue.mark_failed(
                job_id, f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            )

    async def _start_export(self, job: Dict[str, Any]) -> None:
        child_skus = list(job["child_skus"] or [])
        if not child_skus:
            logger.info(f"{self.name}: job {job['id']} has no children, completing")
            await gallery_image_sync_queue.mark_completed(job["id"])
            return

        # Checked before spending an export job on an image the operator has already
        # replaced, and again before the import, since minutes pass in between.
        if await self._supersede_if_stale(job):
            return

        export_job_id = await sellercloud_service.create_image_export(child_skus)
        await gallery_image_sync_queue.mark_exporting(job["id"], export_job_id)
        logger.info(
            f"{self.name}: job {job['id']} {job['product_id']} -> export "
            f"{export_job_id} for {len(child_skus)} children"
        )

    async def _start_import(self, job: Dict[str, Any]) -> None:
        if not await sellercloud_service.is_job_complete(job["export_job_id"]):
            return

        if await self._supersede_if_stale(job):
            return

        raw = await sellercloud_service.get_job_output_file(job["export_job_id"])
        existing = image_rows_from_export(raw)

        child_skus = list(job["child_skus"] or [])
        rows: List[Dict[str, Any]] = []
        # Delete first, add second: SellerCloud applies the file in order, and the point
        # is to replace the image rather than leave the product holding both.
        wanted = set(child_skus)
        for image in existing:
            if image["product_id"] in wanted:
                rows.append(delete_image_row(image["product_id"], image["image_id"]))

        image_url = None
        generation = job["gcs_generation"]
        if job["action"] == "replace":
            if not generation:
                # Only for rows written before the save path captured it, or when that
                # write failed. _supersede_if_stale has already confirmed the blob is
                # there, so this adopts whatever GCS currently holds.
                generation, _ = await self._head_generation(job["product_id"])
            if not generation:
                raise RuntimeError(
                    f"No GCS 1_1500.jpg for {job['product_id']}, cannot push an image"
                )
            image_url = _gcs_url(job["product_id"], generation)
            for child_sku in child_skus:
                rows.append(add_default_image_row(child_sku, image_url))

        if not rows:
            logger.info(
                f"{self.name}: job {job['id']} {job['product_id']} had nothing to "
                f"change on SellerCloud"
            )
            await gallery_image_sync_queue.mark_completed(job["id"])
            return

        import_job_id = await sellercloud_service.create_image_import(
            build_image_import_tsv(rows)
        )
        await gallery_image_sync_queue.mark_importing(
            job["id"], import_job_id, image_url, generation
        )
        logger.info(
            f"{self.name}: job {job['id']} {job['product_id']} -> import "
            f"{import_job_id} ({len(rows)} rows, action={job['action']})"
        )

    async def _finish(self, job: Dict[str, Any]) -> None:
        if not await sellercloud_service.is_job_complete(job["import_job_id"]):
            return
        await gallery_image_sync_queue.mark_completed(job["id"])
        logger.info(
            f"{self.name}: job {job['id']} {job['product_id']} done "
            f"({len(job['child_skus'] or [])} children)"
        )

    async def _supersede_if_stale(self, job: Dict[str, Any]) -> bool:
        """End the job if the image it was queued for is no longer the one on GCS.

        Two saves inside a couple of minutes leave the first job holding a generation
        that has already been overwritten. Pushing it would publish a ?v= naming bytes
        that no longer exist, and SellerCloud would be handed the newer image under the
        older version tag, which is exactly the stale-cache trap the version is there to
        prevent. The save that overwrote it queued its own job, so this one is dropped
        rather than failed.

        Only meaningful for `replace`: a `delete_all` job carries no generation, and
        deleting then re-adding is the correct order when a later save re-adds an image.
        """
        if job["action"] != "replace":
            return False

        live_generation, reachable = await self._head_generation(job["product_id"])
        if not reachable:
            # A HEAD that never answered is a transient, not a supersede: leave the job
            # alone and let the next cycle decide.
            return False

        if live_generation is None:
            await gallery_image_sync_queue.mark_superseded(
                job["id"],
                f"Superseded: GCS 1_1500.jpg for {job['product_id']} no longer exists",
            )
            logger.info(
                f"{self.name}: job {job['id']} {job['product_id']} superseded, "
                f"the image is gone from GCS"
            )
            return True

        stored = job["gcs_generation"]
        if stored and stored != live_generation:
            await gallery_image_sync_queue.mark_superseded(
                job["id"],
                f"Superseded: queued for GCS generation {stored}, "
                f"but {job['product_id']} is now at {live_generation}",
            )
            logger.info(
                f"{self.name}: job {job['id']} {job['product_id']} superseded, "
                f"generation {stored} -> {live_generation}"
            )
            return True

        return False

    async def _head_generation(self, product_id: str) -> Tuple[Optional[str], bool]:
        """(generation, reachable). generation is None when the blob is not there."""
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.head(_gcs_url(product_id))
        except Exception as e:
            logger.warning(f"{self.name}: GCS HEAD failed for {product_id}: {e}")
            return None, False
        if resp.status_code == 404:
            return None, True
        if resp.status_code != 200:
            logger.warning(
                f"{self.name}: GCS HEAD for {product_id} returned {resp.status_code}"
            )
            return None, False
        return resp.headers.get("x-goog-generation"), True


gallery_image_sync_poller = GalleryImageSyncPoller()
