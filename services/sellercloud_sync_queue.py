import logging
from typing import Any, Optional

from tortoise import connections

logger = logging.getLogger(__name__)


def _conn():
    return connections.get("product_db")


async def enqueue(
    sku: str,
    value: str,
    sync_type: str,
    old_primary_upc: Optional[str] = None,
) -> int:
    result = await _conn().execute_query_dict(
        """
        INSERT INTO sellercloud_alias_sync_history (sku, value, type, old_primary_upc)
        VALUES ($1, $2, $3, $4)
        RETURNING id
        """,
        [sku, value, sync_type, old_primary_upc],
    )
    return result[0]["id"]


async def claim_batch(batch_size: int) -> list[dict[str, Any]]:
    return await _conn().execute_query_dict(
        """
        UPDATE sellercloud_alias_sync_history
        SET status = 'processing'
        WHERE id IN (
            SELECT id FROM sellercloud_alias_sync_history
            WHERE status = 'pending'
            ORDER BY created_at ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id, sku, value, type, retry_count, old_primary_upc
        """,
        [batch_size],
    )


async def mark_synced(sync_id: int) -> None:
    await _conn().execute_query(
        """
        UPDATE sellercloud_alias_sync_history
        SET status = 'synced', synced_at = CURRENT_TIMESTAMP, error_message = NULL
        WHERE id = $1
        """,
        [sync_id],
    )


async def mark_failed(sync_id: int, error_message: str, retry_count: int) -> None:
    await _conn().execute_query(
        """
        UPDATE sellercloud_alias_sync_history
        SET status = 'failed', error_message = $2, retry_count = $3
        WHERE id = $1
        """,
        [sync_id, error_message, retry_count],
    )
