"""Keeps the catalog's mirrors in step with the databases it cannot join across.

Every 15 minutes, two independent steps (a failure in one does not skip the other):

1. Products. Read every active parent from the products DB in one query, upsert them into
   catalog_products and delete parents that are no longer active, in one transaction.
   Unchanged rows are not rewritten (42k rows every 15 minutes would otherwise churn three
   trigram indexes).
2. Images. Read, per product, that photography has a productimages row and when the newest
   was created, in one aggregate query, and mirror it into catalog_product_images the same
   way.

Both steps treat a read that comes back with fewer than 90% of the rows the mirror already
holds as a failed read and write nothing, rather than deleting most of the mirror.

A parent has images once any productimages row exists for it, whatever its source:
photography writes batch_creation when a product is shot and upload or manual when edited
files are delivered. The catalog blocks selection of parents with none.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from tortoise import connections
from tortoise.transactions import in_transaction

from config import config
from services.base_poller import BasePoller

logger = logging.getLogger(__name__)

INTERVAL_SECONDS = 900
UPSERT_CHUNK = 5000
MIN_READ_RATIO = 0.9

UPSERT_SQL = """
INSERT INTO catalog_products AS c
       (sku, title, mpn, brand, product_type, company_code, product_created_at)
SELECT *
  FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::int[],
              $7::timestamptz[])
ON CONFLICT (sku) DO UPDATE
   SET title = EXCLUDED.title,
       mpn = EXCLUDED.mpn,
       brand = EXCLUDED.brand,
       product_type = EXCLUDED.product_type,
       company_code = EXCLUDED.company_code,
       product_created_at = EXCLUDED.product_created_at
 WHERE (c.title, c.mpn, c.brand, c.product_type, c.company_code, c.product_created_at)
       IS DISTINCT FROM
       (EXCLUDED.title, EXCLUDED.mpn, EXCLUDED.brand, EXCLUDED.product_type,
        EXCLUDED.company_code, EXCLUDED.product_created_at)
"""

# Keyed on the SKU set this cycle read, never on a per-row timestamp: skipping unchanged
# rows above would make a timestamp-based delete remove every unchanged parent.
DELETE_SQL = """
DELETE FROM catalog_products c
 WHERE NOT EXISTS (SELECT 1 FROM unnest($1::text[]) AS s(sku) WHERE s.sku = c.sku)
"""

# Runs on the photography DB. Aggregated there, so one row per product crosses the wire
# rather than one per shoot and source.
IMAGES_READ_SQL = """
SELECT product_id AS sku, max(created_at) AS taken_at
  FROM productimages
 WHERE product_id IS NOT NULL AND product_id <> ''
 GROUP BY product_id
"""

IMAGES_UPSERT_SQL = """
INSERT INTO catalog_product_images AS i (parent_sku, taken_at)
SELECT * FROM unnest($1::text[], $2::timestamptz[])
ON CONFLICT (parent_sku) DO UPDATE
   SET taken_at = EXCLUDED.taken_at
 WHERE i.taken_at IS DISTINCT FROM EXCLUDED.taken_at
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
        self.synced_at: Optional[datetime] = None
        self.synced_rows: Optional[int] = None
        self.images_synced_at: Optional[datetime] = None

    def status(self) -> Dict[str, Any]:
        return {
            "catalog_synced_at": self.synced_at,
            "catalog_rows": self.synced_rows,
            "images_synced_at": self.images_synced_at,
        }

    async def _poll_cycle(self) -> None:
        # Independent mirrors: a products DB outage must not leave image state stale, nor the
        # other way round. The first failure is re-raised once both have had their turn.
        failure: Optional[Exception] = None
        for step in (self.sync_mirror, self.sync_images):
            try:
                await step()
            except Exception as exc:  # noqa: BLE001
                logger.exception(f"{self.name}: {step.__name__} failed")
                failure = failure or exc
        if failure:
            raise failure

    async def sync_mirror(self) -> Optional[int]:
        started = time.monotonic()
        rows: List[Dict[str, Any]] = await connections.get("product_db").execute_query_dict(
            """
            SELECT sku, title, mpn, brand, product_type, company_code, created_at
              FROM parent_products
             WHERE is_active
            """
        )
        default = connections.get("default")
        existing = (await default.execute_query_dict("SELECT count(*)::int AS n FROM catalog_products"))[0]["n"]
        if existing and len(rows) < MIN_READ_RATIO * existing:
            logger.error(
                f"{self.name}: products DB returned {len(rows)} active parents but the mirror "
                f"holds {existing}; treating it as a failed read and writing nothing"
            )
            return None

        all_skus = [row["sku"] for row in rows]
        async with in_transaction("default") as conn:
            for i in range(0, len(rows), UPSERT_CHUNK):
                chunk = rows[i : i + UPSERT_CHUNK]
                await conn.execute_query(
                    UPSERT_SQL,
                    [
                        [row["sku"] for row in chunk],
                        [row["title"] for row in chunk],
                        [row["mpn"] for row in chunk],
                        [row["brand"] for row in chunk],
                        [row["product_type"] for row in chunk],
                        [row["company_code"] for row in chunk],
                        [row["created_at"] for row in chunk],
                    ],
                )
            await conn.execute_query(DELETE_SQL, [all_skus])

        self.synced_at = datetime.now(timezone.utc)
        self.synced_rows = len(rows)
        logger.info(
            f"{self.name}: mirrored {len(rows)} active parents in "
            f"{time.monotonic() - started:.1f}s"
        )
        return len(rows)

    async def sync_images(self) -> Optional[int]:
        started = time.monotonic()
        rows: List[Dict[str, Any]] = await connections.get("photography_db").execute_query_dict(
            IMAGES_READ_SQL
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
                    [[row["sku"] for row in chunk], [row["taken_at"] for row in chunk]],
                )
            await conn.execute_query(IMAGES_DELETE_SQL, [[row["sku"] for row in rows]])

        self.images_synced_at = datetime.now(timezone.utc)
        logger.info(
            f"{self.name}: mirrored images for {len(rows)} products in "
            f"{time.monotonic() - started:.1f}s"
        )
        return len(rows)


catalog_sync_poller = CatalogSyncPoller()
