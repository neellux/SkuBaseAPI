"""Queue behind listing_ai_search_jobs.

One row per verification run. Unlike gallery_image_sync_sellercloud_jobs, whose
`status` is a multi-cycle state machine that a claim must not touch, these jobs
are single-shot: the claim sets 'running' atomically and IS the lock, so two
workers can run different listings in parallel rather than one skipping its
cycle on an advisory lock.

Raw SQL over the default connection, mirroring services/gallery_image_sync_queue.py.
"""

import logging
from typing import Any, Dict, List, Optional

from tortoise import connections

logger = logging.getLogger(__name__)


def _conn():
    return connections.get("default")


async def enqueue(
    listing_id: str,
    product_id: str,
    reason: str = "manual",
    requested_by: Optional[str] = None,
) -> int:
    """Record one run, coalescing onto an existing pending row.

    The unique partial index covers `pending` only, deliberately. A re-run asked
    for while a call is already in flight has to become a second row: folding it
    onto the running job would attach the operator's click to an answer that was
    already decided, and nothing would appear to happen.
    """
    rows = await _conn().execute_query_dict(
        """
        INSERT INTO listing_ai_search_jobs
            (listing_id, product_id, reason, requested_by)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (listing_id) WHERE status = 'pending'
        DO UPDATE SET
            reason          = EXCLUDED.reason,
            requested_by    = EXCLUDED.requested_by,
            next_attempt_at = CURRENT_TIMESTAMP,
            updated_at      = CURRENT_TIMESTAMP
        RETURNING id
        """,
        [listing_id, product_id, reason, requested_by],
    )
    return rows[0]["id"]


async def enqueue_for_listings(pairs: List[tuple], reason: str = "batch_create") -> int:
    """Queue newly created listings in one statement.

    The WHERE clause settles the existing-draft relink path without a branch in
    Python: a newly created listing has ai_search IS NULL and is always
    queued, an already-verified relinked draft is not re-run (it would cost
    $0.021 and ~25s to almost certainly say the same thing), and a draft that was
    never verified or whose last run failed is picked up -- which makes every
    batch a free backfill for the listings it touches.
    """
    if not pairs:
        return 0
    listing_ids = [str(p[0]) for p in pairs]
    product_ids = [str(p[1]) for p in pairs]
    rows = await _conn().execute_query_dict(
        """
        INSERT INTO listing_ai_search_jobs (listing_id, product_id, reason)
        SELECT t.lid, t.pid, $3
        FROM unnest($1::uuid[], $2::text[]) AS t(lid, pid)
        JOIN listings l ON l.id = t.lid
        WHERE l.ai_search IS NULL
           OR l.ai_search->>'status' IS DISTINCT FROM 'done'
        ON CONFLICT (listing_id) WHERE status = 'pending' DO NOTHING
        RETURNING id
        """,
        [listing_ids, product_ids, reason],
    )
    return len(rows)


async def claim_batch(batch_size: int) -> List[Dict[str, Any]]:
    """Take up to batch_size pending jobs and mark them running, atomically.

    FOR UPDATE SKIP LOCKED means a second worker steps over anything this one is
    taking rather than blocking on it, so the claim itself is the mutual
    exclusion and no advisory lock is needed.
    """
    return await _conn().execute_query_dict(
        """
        UPDATE listing_ai_search_jobs
        SET status     = 'running',
            attempts   = attempts + 1,
            started_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE id IN (
            SELECT id FROM listing_ai_search_jobs
            WHERE status = 'pending' AND next_attempt_at <= CURRENT_TIMESTAMP
            ORDER BY next_attempt_at, id
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id, listing_id, product_id, reason, requested_by, attempts
        """,
        [batch_size],
    )


async def mark_completed(
    job_id: int,
    model: str,
    auth_path: str,
    duration_ms: int,
    cost_usd: float,
) -> None:
    await _conn().execute_query(
        """
        UPDATE listing_ai_search_jobs
        SET status = 'completed', error = NULL, model = $2, auth_path = $3,
            duration_ms = $4, cost_usd = $5,
            completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
        WHERE id = $1
        """,
        [job_id, model, auth_path, duration_ms, cost_usd],
    )


async def mark_retry(job_id: int, error: str, delay_seconds: float) -> None:
    """Back to pending, invisible until delay_seconds have passed."""
    await _conn().execute_query(
        """
        UPDATE listing_ai_search_jobs
        SET status = 'pending', error = $2,
            next_attempt_at = CURRENT_TIMESTAMP + make_interval(secs => $3),
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $1
        """,
        [job_id, error[:500], float(delay_seconds)],
    )


async def mark_failed(job_id: int, error: str) -> None:
    await _conn().execute_query(
        """
        UPDATE listing_ai_search_jobs
        SET status = 'failed', error = $2,
            completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
        WHERE id = $1
        """,
        [job_id, error[:500]],
    )


async def mark_skipped(job_id: int, error: str) -> None:
    await _conn().execute_query(
        """
        UPDATE listing_ai_search_jobs
        SET status = 'skipped', error = $2,
            completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
        WHERE id = $1
        """,
        [job_id, error[:500]],
    )


async def requeue_stale(timeout_minutes: int, max_attempts: int) -> int:
    """Recover jobs whose worker died mid-call.

    Re-queued rather than failed, unlike gallery_image_sync_queue.fail_stale:
    there a half-run SellerCloud job may have taken effect, so retrying could
    double-apply. Here the work is single-shot and idempotent -- nothing is
    written until the answer comes back -- so a process restart mid-call should
    simply try again.
    """
    rows = await _conn().execute_query_dict(
        """
        UPDATE listing_ai_search_jobs
        SET status = CASE WHEN attempts >= $2 THEN 'failed' ELSE 'pending' END,
            next_attempt_at = CURRENT_TIMESTAMP,
            error = 'Worker died or hung after ' || attempts::text || ' attempt(s)',
            updated_at = CURRENT_TIMESTAMP
        WHERE status = 'running'
          AND updated_at < CURRENT_TIMESTAMP - make_interval(mins => $1)
        RETURNING id
        """,
        [timeout_minutes, max_attempts],
    )
    return len(rows)


async def active_for_listing(listing_id: str) -> Optional[Dict[str, Any]]:
    """The in-flight job for a listing, if any. Drives the UI's pending/running."""
    rows = await _conn().execute_query_dict(
        """
        SELECT id, status, reason, requested_by, attempts, error,
               created_at, started_at, next_attempt_at
        FROM listing_ai_search_jobs
        WHERE listing_id = $1 AND status IN ('pending', 'running')
        ORDER BY created_at DESC
        LIMIT 1
        """,
        [listing_id],
    )
    return rows[0] if rows else None


async def queue_position(job_id: int) -> int:
    """How many pending jobs are ahead of this one, so the UI can say so."""
    rows = await _conn().execute_query_dict(
        """
        SELECT count(*) AS n FROM listing_ai_search_jobs
        WHERE status = 'pending'
          AND (next_attempt_at, id) < (
              SELECT next_attempt_at, id FROM listing_ai_search_jobs WHERE id = $1
          )
        """,
        [job_id],
    )
    return rows[0]["n"] if rows else 0
