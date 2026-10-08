"""Platform import jobs: the staging and progress row behind the dashboard's Import.

Module-level functions over raw SQL, mirroring alias_bulk_import_job_service. The one
deliberate difference is claim_next_job: see RECLAIM_AFTER_SECONDS below.

Lifecycle:
    staged      a preview parsed a file and derived a plan; nothing has been written
    pending     the operator approved it
    processing  the poller claimed it
    completed / failed / cancelled

The job row holds the file's SKUs, never the computed plan (KTD1), so the preview and
the approved run each derive the difference from current data.
"""

import json
import logging
from typing import Any, Optional

from tortoise import connections
from tortoise.transactions import in_transaction

logger = logging.getLogger(__name__)

# This table lives in lux_skubase, not the products db.
_DB = "default"

# Fixed advisory-lock key serializing claim attempts across workers. Any stable bigint
# works; this one is arbitrary and distinct from alias_bulk_import_job_service's.
_CLAIM_LOCK_KEY = 7824930851

# A 'processing' job whose heartbeat is older than this is considered abandoned and may
# be re-claimed from its cursor. It must comfortably exceed one parent's apply time: a
# delist takes the per-product advisory lock and runs its own transaction, so a few
# seconds is normal and a slow one is not evidence of a dead worker.
RECLAIM_AFTER_SECONDS = 300

_JSON_COLUMNS = ("skus", "skipped", "preview_counts", "results", "revert")

_ROW = """
    id, platform_id, status, file_name, skus, skipped, preview_counts,
    total_parents, processed_parents, listed_count, delisted_count,
    repaired_count, refused_count, cursor_parent, results, revert,
    created_by, approved_by, error_message,
    created_at, approved_at, started_at, heartbeat_at, completed_at
"""


def _conn():
    return connections.get(_DB)


def _decode(row: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """asyncpg may hand back jsonb as str or as the parsed object; normalize to objects."""
    if row is None:
        return None
    for key in _JSON_COLUMNS:
        val = row.get(key)
        if isinstance(val, str):
            row[key] = json.loads(val) if val else None
    return row


async def create_staged(
    *,
    platform_id: str,
    file_name: Optional[str],
    skus: list[str],
    skipped: list[dict[str, Any]],
    created_by: Optional[str] = None,
) -> int:
    """Stage a parsed file. Writes only this row; no catalog data is touched."""
    rows = await _conn().execute_query_dict(
        """
        INSERT INTO platform_import_jobs
            (platform_id, status, file_name, skus, skipped, created_by)
        VALUES ($1, 'staged', $2, $3::jsonb, $4::jsonb, $5)
        RETURNING id
        """,
        [platform_id, file_name, json.dumps(skus), json.dumps(skipped), created_by],
    )
    return rows[0]["id"]


async def record_preview(
    job_id: int, *, preview_counts: dict[str, Any], total_parents: int
) -> None:
    """Store the counts the operator is about to see, for the apply-time drift check."""
    await _conn().execute_query(
        """
        UPDATE platform_import_jobs
        SET preview_counts = $2::jsonb, total_parents = $3
        WHERE id = $1 AND status = 'staged'
        """,
        [job_id, json.dumps(preview_counts), total_parents],
    )


async def get_job(job_id: int) -> Optional[dict[str, Any]]:
    rows = await _conn().execute_query_dict(
        f"SELECT {_ROW} FROM platform_import_jobs WHERE id = $1", [job_id]
    )
    return _decode(rows[0]) if rows else None


async def latest_for_platform(platform_id: str) -> Optional[dict[str, Any]]:
    """The platform's most recent job.

    This is how the dashboard finds a running or finished import after the dialog is
    closed or the page reloaded (R20). A lookup by id alone cannot do it, because the id
    only ever existed in the preview response.
    """
    rows = await _conn().execute_query_dict(
        f"""
        SELECT {_ROW} FROM platform_import_jobs
        WHERE platform_id = $1
        ORDER BY created_at DESC
        LIMIT 1
        """,
        [platform_id],
    )
    return _decode(rows[0]) if rows else None


async def approve(job_id: int, approved_by: Optional[str]) -> bool:
    """Move a staged job to pending. False if it was not staged (double-approve, cancelled)."""
    rows = await _conn().execute_query_dict(
        """
        UPDATE platform_import_jobs
        SET status = 'pending', approved_by = $2, approved_at = CURRENT_TIMESTAMP
        WHERE id = $1 AND status = 'staged'
        RETURNING id
        """,
        [job_id, approved_by],
    )
    return bool(rows)


async def cancel(job_id: int) -> bool:
    """Discard a staged or pending job. A claimed job is left alone."""
    rows = await _conn().execute_query_dict(
        """
        UPDATE platform_import_jobs
        SET status = 'cancelled', completed_at = CURRENT_TIMESTAMP
        WHERE id = $1 AND status IN ('staged', 'pending')
        RETURNING id
        """,
        [job_id],
    )
    return bool(rows)


async def claim_next_job() -> Optional[dict[str, Any]]:
    """Claim one job, preferring a fresh approval and falling back to a stale reclaim.

    Diverges from alias_bulk_import_job_service.claim_next_job deliberately. That one
    returns None whenever ANY row is 'processing', so a job interrupted mid-run blocks
    every later import permanently. This reconcile can run for thousands of parents
    across a deploy, so an interrupted run is expected, not hypothetical.

    Order inside the advisory lock:
      1. A job already processing with a LIVE heartbeat means a worker is on it: stop.
      2. A job processing with a STALE heartbeat is abandoned: re-claim it. Its
         cursor_parent survives, so the poller resumes rather than repeating.
      3. Otherwise claim the oldest pending job.

    Returns None when another worker holds the claim lock, a live job is running, or
    there is nothing to do.
    """
    async with in_transaction(_DB) as txn:
        lock = await txn.execute_query_dict(
            "SELECT pg_try_advisory_xact_lock($1) AS acquired", [_CLAIM_LOCK_KEY]
        )
        if not lock or not lock[0]["acquired"]:
            return None

        live = await txn.execute_query_dict(
            """
            SELECT id FROM platform_import_jobs
            WHERE status = 'processing'
              AND heartbeat_at IS NOT NULL
              AND heartbeat_at > CURRENT_TIMESTAMP - ($1 || ' seconds')::interval
            LIMIT 1
            """,
            [str(RECLAIM_AFTER_SECONDS)],
        )
        if live:
            return None

        rows = await txn.execute_query_dict(
            f"""
            UPDATE platform_import_jobs
            SET status = 'processing',
                started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
                heartbeat_at = CURRENT_TIMESTAMP
            WHERE id = (
                SELECT id FROM platform_import_jobs
                WHERE status = 'processing'
                   OR status = 'pending'
                ORDER BY (status = 'processing') DESC, approved_at ASC NULLS LAST, id ASC
                LIMIT 1
            )
            RETURNING {_ROW}
            """
        )

    if not rows:
        return None
    row = _decode(rows[0])
    if row and row.get("cursor_parent"):
        logger.info(
            "PlatformImportJob %s re-claimed, resuming after parent %s",
            row["id"],
            row["cursor_parent"],
        )
    return row


async def record_progress(
    job_id: int,
    *,
    cursor_parent: Optional[str] = None,
    listed: int = 0,
    delisted: int = 0,
    repaired: int = 0,
    refused: int = 0,
    parents_done: int = 1,
    result_entries: Optional[list[dict[str, Any]]] = None,
) -> None:
    """Advance the cursor and counters, and bump the heartbeat.

    Called once per parent, inside the same step that commits that parent's writes, so
    the cursor can never run ahead of what actually landed. The heartbeat bump is what
    stops a healthy long run from being re-claimed out from under itself.
    """
    await _conn().execute_query(
        """
        UPDATE platform_import_jobs
        SET cursor_parent     = COALESCE($2, cursor_parent),
            processed_parents = processed_parents + $3,
            listed_count      = listed_count + $4,
            delisted_count    = delisted_count + $5,
            repaired_count    = repaired_count + $6,
            refused_count     = refused_count + $7,
            results           = results || $8::jsonb,
            heartbeat_at      = CURRENT_TIMESTAMP
        WHERE id = $1
        """,
        [
            job_id,
            cursor_parent,
            parents_done,
            listed,
            delisted,
            repaired,
            refused,
            json.dumps(result_entries or []),
        ],
    )


async def heartbeat(job_id: int) -> None:
    """Bump the heartbeat without advancing anything, for a long single step."""
    await _conn().execute_query(
        "UPDATE platform_import_jobs SET heartbeat_at = CURRENT_TIMESTAMP WHERE id = $1",
        [job_id],
    )


async def mark_completed(job_id: int, *, revert: Optional[dict[str, Any]] = None) -> None:
    await _conn().execute_query(
        """
        UPDATE platform_import_jobs
        SET status = 'completed',
            completed_at = CURRENT_TIMESTAMP,
            revert = COALESCE($2::jsonb, revert)
        WHERE id = $1
        """,
        [job_id, json.dumps(revert) if revert is not None else None],
    )


async def mark_failed(
    job_id: int, error_message: str, *, revert: Optional[dict[str, Any]] = None
) -> None:
    """Fail a job, keeping whatever revert record the run accumulated before it stopped."""
    await _conn().execute_query(
        """
        UPDATE platform_import_jobs
        SET status = 'failed',
            completed_at = CURRENT_TIMESTAMP,
            error_message = $2,
            revert = COALESCE($3::jsonb, revert)
        WHERE id = $1
        """,
        [job_id, error_message, json.dumps(revert) if revert is not None else None],
    )
