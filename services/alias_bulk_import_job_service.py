"""
Alias bulk import job service.

Manages background processing jobs for bulk imports that exceed the synchronous
threshold (config.bulk_import.max_tracked_items). Module-level functions mirror
the sellercloud_sync_queue.py pattern — no class, just DB access helpers.
"""

import json
import logging
from typing import Any, Optional

from tortoise import connections

logger = logging.getLogger(__name__)


def _conn():
    return connections.get("product_db")


async def create_job(
    items: list[dict[str, Any]],
    donors: Optional[dict[str, dict[str, int]]] = None,
    created_by: Optional[str] = None,
) -> int:
    """Enqueue a new bulk import job. Returns job_id."""
    result = await _conn().execute_query_dict(
        """
        INSERT INTO alias_bulk_import_jobs
            (status, total_items, items, donors, created_by)
        VALUES ('pending', $1, $2::jsonb, $3::jsonb, $4)
        RETURNING id
        """,
        [
            len(items),
            json.dumps(items),
            json.dumps(donors) if donors else None,
            created_by,
        ],
    )
    return result[0]["id"]


async def get_job(job_id: int) -> Optional[dict[str, Any]]:
    """Fetch a job by id. Returns None if not found."""
    rows = await _conn().execute_query_dict(
        """
        SELECT id, status, total_items, processed_items, successful_count, failed_count,
               items, results, donors, created_by, error_message,
               created_at, started_at, completed_at
        FROM alias_bulk_import_jobs
        WHERE id = $1
        """,
        [job_id],
    )
    if not rows:
        return None
    row = rows[0]
    # Parse JSONB columns (tortoise/asyncpg may return them as strings or dicts)
    for key in ("items", "results", "donors"):
        val = row.get(key)
        if isinstance(val, str):
            row[key] = json.loads(val) if val else None
    return row


# Fixed advisory-lock key used to serialize claim attempts across workers/processes.
# Any stable bigint works — this one is arbitrary.
_CLAIM_LOCK_KEY = 7824930842


async def claim_next_job() -> Optional[dict[str, Any]]:
    """Atomically claim the next pending job, enforcing single-job-at-a-time.

    Serialization strategy:
      1. Each claim runs inside a transaction.
      2. pg_try_advisory_xact_lock serializes concurrent claim attempts (one at
         a time can progress past this point).
      3. Inside the locked critical section, we check if any job is already
         'processing'. If yes, return None — the caller must retry later.
      4. Otherwise, mark the oldest pending job as 'processing' and return it.
      5. Transaction commits; lock releases automatically.

    After the claim commits, other workers can check and will see status='processing'
    on the claimed job, so they too return None until it completes.

    Returns None when:
      - Another worker currently holds the claim lock (rare, transient).
      - A job is already processing (the common case).
      - No pending jobs exist.
    """
    from tortoise.transactions import in_transaction

    async with in_transaction("product_db") as txn:
        # Step 1: try to acquire the serialization lock. Non-blocking.
        lock_rows = await txn.execute_query_dict(
            "SELECT pg_try_advisory_xact_lock($1) AS acquired", [_CLAIM_LOCK_KEY]
        )
        if not lock_rows or not lock_rows[0]["acquired"]:
            return None

        # Step 2: is a job already processing? If so, don't claim another.
        existing = await txn.execute_query_dict(
            "SELECT id FROM alias_bulk_import_jobs WHERE status = 'processing' LIMIT 1"
        )
        if existing:
            return None

        # Step 3: claim the oldest pending job.
        rows = await txn.execute_query_dict(
            """
            UPDATE alias_bulk_import_jobs
            SET status = 'processing', started_at = CURRENT_TIMESTAMP
            WHERE id = (
                SELECT id FROM alias_bulk_import_jobs
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT 1
            )
            RETURNING id, total_items, items, donors, created_by
            """
        )

    if not rows:
        return None
    row = rows[0]
    for key in ("items", "donors"):
        val = row.get(key)
        if isinstance(val, str):
            row[key] = json.loads(val) if val else None
    return row


async def append_result(
    job_id: int,
    result: dict[str, Any],
    success: bool,
) -> None:
    """Append a per-item result to a processing job and bump counters."""
    await _conn().execute_query(
        """
        UPDATE alias_bulk_import_jobs
        SET results = results || $2::jsonb,
            processed_items = processed_items + 1,
            successful_count = successful_count + $3,
            failed_count = failed_count + $4
        WHERE id = $1
        """,
        [job_id, json.dumps([result]), 1 if success else 0, 0 if success else 1],
    )


async def mark_completed(job_id: int) -> None:
    """Mark a job as completed."""
    await _conn().execute_query(
        """
        UPDATE alias_bulk_import_jobs
        SET status = 'completed', completed_at = CURRENT_TIMESTAMP
        WHERE id = $1
        """,
        [job_id],
    )


async def mark_failed(job_id: int, error_message: str) -> None:
    """Mark a job as failed (unexpected error during processing)."""
    await _conn().execute_query(
        """
        UPDATE alias_bulk_import_jobs
        SET status = 'failed', completed_at = CURRENT_TIMESTAMP, error_message = $2
        WHERE id = $1
        """,
        [job_id, error_message],
    )
