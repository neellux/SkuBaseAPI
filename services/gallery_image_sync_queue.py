"""Queue behind gallery_image_sync_sellercloud_jobs.

One row per gallery save whose slot 1 changed. GalleryImageSyncPoller walks each row
pending -> exporting -> importing -> completed, so `status` is the state machine and a
claim never mutates it. Raw SQL over the default connection, mirroring
services/sellercloud_sync_queue.py.
"""
import logging
from typing import Any, Optional

from tortoise import connections

logger = logging.getLogger(__name__)

ACTIVE_STATUSES = ("pending", "exporting", "importing")


def _conn():
    return connections.get("default")


async def enqueue(
    product_id: str,
    child_skus: list[str],
    action: str,
    productimages_id: Optional[str] = None,
    top_shot_md5: Optional[str] = None,
    gcs_generation: Optional[str] = None,
) -> int:
    """Record one save. A second save while the first is still pending updates it in
    place: only the newest image is worth pushing, and two pushes for one product would
    race each other's delete."""
    rows = await _conn().execute_query_dict(
        """
        INSERT INTO gallery_image_sync_sellercloud_jobs
            (product_id, productimages_id, child_skus, action, top_shot_md5, gcs_generation)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (product_id) WHERE status = 'pending'
        DO UPDATE SET
            productimages_id = EXCLUDED.productimages_id,
            child_skus       = EXCLUDED.child_skus,
            action           = EXCLUDED.action,
            top_shot_md5     = EXCLUDED.top_shot_md5,
            gcs_generation   = EXCLUDED.gcs_generation,
            updated_at       = CURRENT_TIMESTAMP
        RETURNING id
        """,
        [product_id, productimages_id, child_skus, action, top_shot_md5, gcs_generation],
    )
    return rows[0]["id"]


async def claim_batch(batch_size: int) -> list[dict[str, Any]]:
    """Oldest active job per product, oldest first, capped at batch_size.

    DISTINCT ON is what serializes a product: only its oldest active job is ever
    returned, so a save made while an earlier push is mid-flight waits for that push to
    reach a terminal state instead of racing its export against the other's import.
    """
    return await _conn().execute_query_dict(
        """
        SELECT * FROM (
            SELECT DISTINCT ON (product_id) *
            FROM gallery_image_sync_sellercloud_jobs
            WHERE status = ANY($2::text[])
            ORDER BY product_id, created_at
        ) t
        ORDER BY created_at
        LIMIT $1
        """,
        [batch_size, list(ACTIVE_STATUSES)],
    )


async def mark_exporting(job_id: int, export_job_id: str) -> None:
    await _conn().execute_query(
        """
        UPDATE gallery_image_sync_sellercloud_jobs
        SET status = 'exporting', export_job_id = $2, updated_at = CURRENT_TIMESTAMP
        WHERE id = $1
        """,
        [job_id, export_job_id],
    )


async def mark_importing(
    job_id: int, import_job_id: str, image_url: Optional[str], gcs_generation: Optional[str]
) -> None:
    await _conn().execute_query(
        """
        UPDATE gallery_image_sync_sellercloud_jobs
        SET status = 'importing', import_job_id = $2, image_url = $3,
            gcs_generation = COALESCE($4, gcs_generation),
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $1
        """,
        [job_id, import_job_id, image_url, gcs_generation],
    )


async def mark_superseded(job_id: int, reason: str) -> None:
    """End a job that the operator has already overtaken.

    Not a failure: the image this job was queued for is no longer the current one, and
    the save that replaced it queued its own job. Pushing anyway would hand SellerCloud
    a ?v= naming a generation that no longer exists.
    """
    await _conn().execute_query(
        """
        UPDATE gallery_image_sync_sellercloud_jobs
        SET status = 'superseded', error = $2,
            completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
        WHERE id = $1
        """,
        [job_id, reason[:4000]],
    )


async def mark_completed(job_id: int) -> None:
    await _conn().execute_query(
        """
        UPDATE gallery_image_sync_sellercloud_jobs
        SET status = 'completed', error = NULL,
            completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
        WHERE id = $1
        """,
        [job_id],
    )


async def mark_failed(job_id: int, error: str) -> None:
    await _conn().execute_query(
        """
        UPDATE gallery_image_sync_sellercloud_jobs
        SET status = 'failed', error = $2, attempts = attempts + 1,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $1
        """,
        [job_id, error[:4000]],
    )


async def fail_stale(timeout_minutes: int) -> int:
    """Park jobs whose SellerCloud job never reported done, so they stop being polled."""
    affected, _ = await _conn().execute_query(
        """
        UPDATE gallery_image_sync_sellercloud_jobs
        SET status = 'failed', attempts = attempts + 1,
            error = 'Timed out waiting for SellerCloud ' || status || ' job after '
                    || $1::text || ' minutes',
            updated_at = CURRENT_TIMESTAMP
        WHERE status IN ('exporting', 'importing')
          AND updated_at < CURRENT_TIMESTAMP - make_interval(mins => $1)
        """,
        [timeout_minutes],
    )
    return affected or 0
