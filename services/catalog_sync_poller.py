"""Keeps the catalog's photography mirror in step with a database it cannot join across.

Every 15 minutes: read, per product, that photography has a productimages row, when the
newest was created and whether its current shoot has been edited, in one aggregate query,
and mirror it into catalog_product_images. Aggregating on the photography side means one
row per product crosses the wire rather than one per shoot and source.

A read that comes back with fewer than 90% of the rows the mirror already holds counts as a
failed read and writes nothing, rather than deleting most of the mirror.

A parent has images once any productimages row exists for it, whatever its source:
photography writes batch_creation when a product is shot and upload or manual when edited
files are delivered. The catalog blocks selection of parents with none.

Edited is narrower, and follows the photo cycle the way photo_upload_poller does: an upload
or manual row exists for the batch of the product's newest row. A product shot and never
edited is not edited, and neither is one re-shot since its last edit. A washtag write that
bumps a batch_creation row above its own edit still is.

The products side used to be mirrored here too, into catalog_products. It is not any more:
catalog_product_cache holds the active parents in memory and the catalog SQL takes the
matching SKUs as a bound array. This poller only ever reads the photography database.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from tortoise import connections
from tortoise.transactions import in_transaction

from config import config
from services.base_poller import BasePoller
from services.photo_upload_poller import READY_SOURCES

logger = logging.getLogger(__name__)

INTERVAL_SECONDS = 900
UPSERT_CHUNK = 5000
MIN_READ_RATIO = 0.9

# Runs on the photography DB. Aggregated there, so one row per product crosses the wire
# rather than one per shoot and source.
#
# edited is photo_upload_poller's edited_for_batch, asked of the newest row by updated_at:
# keep the two in step. When that row is itself an upload or manual row it matches itself.
# IS NOT DISTINCT FROM, not =, because manual rows carry batch_id NULL.
IMAGES_READ_SQL = """
SELECT n.product_id AS sku, n.taken_at,
       EXISTS (
           SELECT 1 FROM productimages e
            WHERE e.product_id = n.product_id
              AND e.image_source = ANY($1::text[])
              AND e.batch_id IS NOT DISTINCT FROM n.batch_id
       ) AS edited
  FROM (
      SELECT DISTINCT ON (product_id) product_id, batch_id,
             max(created_at) OVER (PARTITION BY product_id) AS taken_at
        FROM productimages
       WHERE product_id IS NOT NULL AND product_id <> ''
       ORDER BY product_id, updated_at DESC
  ) n
"""

IMAGES_UPSERT_SQL = """
INSERT INTO catalog_product_images AS i (parent_sku, taken_at, edited)
SELECT * FROM unnest($1::text[], $2::timestamptz[], $3::boolean[])
ON CONFLICT (parent_sku) DO UPDATE
   SET taken_at = EXCLUDED.taken_at, edited = EXCLUDED.edited
 WHERE i.taken_at IS DISTINCT FROM EXCLUDED.taken_at
    OR i.edited IS DISTINCT FROM EXCLUDED.edited
"""

IMAGES_DELETE_SQL = """
DELETE FROM catalog_product_images i
 WHERE NOT EXISTS (SELECT 1 FROM unnest($1::text[]) AS s(sku) WHERE s.sku = i.parent_sku)
"""


class CatalogSyncPoller(BasePoller):
    def __init__(self) -> None:
        super().__init__(config_section="catalog_sync_poller", name="CatalogSyncPoller")
        cfg = config.get("catalog_sync_poller", {})
        # Defaults off: without the migration every cycle would fail.
        self.enabled = bool(cfg.get("enabled", False))
        self.interval = INTERVAL_SECONDS
        self.images_synced_at: Optional[datetime] = None
        self.images_synced_rows: Optional[int] = None

    def status(self) -> Dict[str, Any]:
        return {
            "images_synced_at": self.images_synced_at,
            "images_rows": self.images_synced_rows,
        }

    async def _poll_cycle(self) -> None:
        await self.sync_images()

    async def sync_images(self) -> Optional[int]:
        started = time.monotonic()
        rows: List[Dict[str, Any]] = await connections.get("photography_db").execute_query_dict(
            IMAGES_READ_SQL, [list(READY_SOURCES)]
        )
        default = connections.get("default")
        existing = (
            await default.execute_query_dict("SELECT count(*)::int AS n FROM catalog_product_images")
        )[0]["n"]
        if existing and len(rows) < MIN_READ_RATIO * existing:
            logger.error(
                f"{self.name}: photography DB returned {len(rows)} products with images but the "
                f"mirror holds {existing}; treating it as a failed read and writing nothing"
            )
            return None

        async with in_transaction("default") as conn:
            for i in range(0, len(rows), UPSERT_CHUNK):
                chunk = rows[i : i + UPSERT_CHUNK]
                await conn.execute_query(
                    IMAGES_UPSERT_SQL,
                    [
                        [row["sku"] for row in chunk],
                        [row["taken_at"] for row in chunk],
                        [row["edited"] for row in chunk],
                    ],
                )
            await conn.execute_query(IMAGES_DELETE_SQL, [[row["sku"] for row in rows]])

        self.images_synced_at = datetime.now(timezone.utc)
        self.images_synced_rows = len(rows)
        logger.info(
            f"{self.name}: mirrored images for {len(rows)} products in "
            f"{time.monotonic() - started:.1f}s"
        )
        return len(rows)


catalog_sync_poller = CatalogSyncPoller()
