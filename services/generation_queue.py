"""Queue behind batch_generation_jobs. SQL only; imports no services.

One row per product of a batch. GenerationPoller claims pending rows (at most 3 running per
batch, batches in parallel, under an optional global cap), generates each listing, and
completes the row in the same transaction that inserts the listing.

Three rules hold the queue together:

- A claim sets a fresh lease_token. complete, retry_or_fail and mark_interrupted all update
  WHERE status = 'running' AND lease_token matches, so an attempt that lost its job (stale
  requeue, Remove, batch delete) can never write anything.
- attempts counts concluded attempts only. A claim never bumps it, so an interrupted
  attempt (restart, deploy, hung call swept up) costs nothing.
- The claim runs inside an explicit transaction. pg_try_advisory_xact_lock is released at
  transaction end, so under autocommit it would be released before the UPDATE even ran.

Raw SQL over the default connection, like services/ai_search_queue.py.
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from tortoise import connections
from tortoise.transactions import in_transaction

from exceptions.batch_generation_exceptions import (
    GenerationError,
    LeaseLost,
    PermanentGenerationError,
    TransientGenerationError,
)

logger = logging.getLogger(__name__)

OUTSTANDING_STATUSES = ("pending", "running", "failed")


def _conn():
    return connections.get("default")


@dataclass(frozen=True, slots=True)
class ClaimedJob:
    id: int
    batch_id: int
    product_id: str
    info_product_id: Optional[str]
    attempts: int
    lease_token: str
    created_by: str

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "ClaimedJob":
        return cls(
            id=int(row["id"]),
            batch_id=int(row["batch_id"]),
            product_id=row["product_id"],
            info_product_id=row.get("info_product_id"),
            attempts=int(row["attempts"]),
            lease_token=str(row["lease_token"]),
            created_by=row["created_by"],
        )


# ---------------------------------------------------------------------------
# Enqueue
# ---------------------------------------------------------------------------

# WITH ORDINALITY + ORDER BY so ids follow request order, which is the tie-break for
# products with an equal (or no) value.
INSERT_JOBS_SQL = """
INSERT INTO batch_generation_jobs (batch_id, product_id, info_product_id, sort_value, created_by)
SELECT $1, u.parent, u.requested, v.value, $4
  FROM unnest($2::text[], $3::text[]) WITH ORDINALITY AS u(parent, requested, ord)
  LEFT JOIN parent_product_values v ON v.parent_sku = u.parent
 ORDER BY u.ord
ON CONFLICT (batch_id, product_id) DO NOTHING
"""


async def insert_jobs(
    conn,
    batch_id: int,
    parents: Sequence[str],
    requested: Sequence[str],
    created_by: str,
) -> None:
    """One statement, so the statement trigger recounts the batch once. Requires the
    caller's open transaction, the one that created the batch."""
    await conn.execute_query(
        INSERT_JOBS_SQL, [batch_id, list(parents), list(requested), created_by]
    )


# ---------------------------------------------------------------------------
# Claim
# ---------------------------------------------------------------------------

# A per-batch LIMIT rather than a window over every pending job: each open batch reads at
# most $1 rows off idx_bgj_claimable. LIMIT cannot reference the lateral running count, so
# the inner select takes $1 and the outer filter trims to the free slots.
CLAIM_SQL = """
WITH pick AS (
    SELECT c.id
      FROM batches b
     CROSS JOIN LATERAL (
         SELECT COUNT(*)::int AS n
           FROM batch_generation_jobs r
          WHERE r.batch_id = b.id AND r.status = 'running'
     ) ru
     CROSS JOIN LATERAL (
         SELECT p.id, ROW_NUMBER() OVER (ORDER BY p.sort_value DESC NULLS LAST, p.id) AS rn
           FROM (SELECT id, sort_value
                   FROM batch_generation_jobs
                  WHERE batch_id = b.id AND status = 'pending' AND next_attempt_at <= now()
                  ORDER BY sort_value DESC NULLS LAST, id
                  LIMIT $1) p
     ) c
     WHERE b.generation_outstanding > b.generation_failed
       AND c.rn <= $1::int - ru.n
     ORDER BY c.rn, c.id
     -- The global cap's free slots, counted in this statement: its snapshot is taken after
     -- the advisory lock statement, so the count is current. NULL = no global cap.
     LIMIT (SELECT CASE WHEN $2::int IS NULL THEN NULL
                        ELSE GREATEST($2::int - count(*), 0) END
              FROM batch_generation_jobs
             WHERE status = 'running')
)
UPDATE batch_generation_jobs j
   SET status = 'running', lease_token = gen_random_uuid(),
       started_at = now(), updated_at = now()
  FROM pick
 WHERE j.id = pick.id AND j.status = 'pending'
RETURNING j.id, j.batch_id, j.product_id, j.info_product_id, j.attempts, j.lease_token, j.created_by
"""


async def claim(per_batch: int, max_running_total: Optional[int]) -> List[ClaimedJob]:
    """Claim the next jobs and commit before returning.

    Callers start work only after this returns, so the attempts never run inside the claim
    transaction. Returns [] when another claim holds the lock; that claim's completions
    re-arm the poller.
    """
    async with in_transaction("default") as conn:
        locked = await conn.execute_query_dict(
            "SELECT pg_try_advisory_xact_lock(hashtext('batch_generation_claim')) AS ok"
        )
        if not locked or not locked[0]["ok"]:
            return []

        cap = int(max_running_total) if max_running_total else None
        rows = await conn.execute_query_dict(CLAIM_SQL, [int(per_batch), cap])

    return [ClaimedJob.from_row(row) for row in rows]


# ---------------------------------------------------------------------------
# Recovery
# ---------------------------------------------------------------------------


async def requeue_stale(timeout_minutes: int) -> int:
    """Running rows untouched for timeout_minutes: the worker died. Back to pending with the
    lease cleared, so a worker that was only hung can no longer commit. attempts is left
    alone; interruptions is what records it."""
    rows = await _conn().execute_query_dict(
        """
        UPDATE batch_generation_jobs
           SET status = 'pending', lease_token = NULL,
               interruptions = interruptions + 1,
               error = 'Attempt did not finish within ' || $1::text || ' minutes; requeued',
               next_attempt_at = now(), updated_at = now()
         WHERE status = 'running'
           AND updated_at < now() - make_interval(mins => $1::int)
        RETURNING id
        """,
        [int(timeout_minutes)],
    )
    return len(rows)


async def mark_interrupted(pairs: Sequence[Tuple[int, str]]) -> int:
    """Shutdown: hand claimed jobs back without consuming an attempt."""
    if not pairs:
        return 0
    rows = await _conn().execute_query_dict(
        """
        UPDATE batch_generation_jobs j
           SET status = 'pending', lease_token = NULL,
               interruptions = j.interruptions + 1,
               next_attempt_at = now(), updated_at = now()
          FROM unnest($1::bigint[], $2::uuid[]) AS t(id, token)
         WHERE j.id = t.id AND j.lease_token = t.token AND j.status = 'running'
        RETURNING j.id
        """,
        [[int(job_id) for job_id, _ in pairs], [str(token) for _, token in pairs]],
    )
    return len(rows)


# ---------------------------------------------------------------------------
# Completion (inside the caller's transaction)
# ---------------------------------------------------------------------------


async def lock_lease(conn, job: ClaimedJob) -> Dict[str, Any]:
    """Lock this attempt's job row, or raise LeaseLost.

    Taken before the listing insert, so the lock order is always job, then batch (the
    insert's FK check and recount), the same order delete_batch uses. Returns the batch's
    current assignee, which the listing inherits.
    """
    rows = await conn.execute_query_dict(
        """
        SELECT j.id, b.assigned_to
          FROM batch_generation_jobs j
          JOIN batches b ON b.id = j.batch_id
         WHERE j.id = $1 AND j.status = 'running' AND j.lease_token = $2::uuid
           FOR UPDATE OF j
        """,
        [job.id, job.lease_token],
    )
    if not rows:
        raise LeaseLost()
    return rows[0]


async def complete(conn, job: ClaimedJob, listing_id: str, outcome: str) -> None:
    rows = await conn.execute_query_dict(
        """
        UPDATE batch_generation_jobs
           SET status = 'done', attempts = attempts + 1, lease_token = NULL,
               listing_id = $3::uuid, outcome = $4,
               error = NULL, error_display = NULL,
               completed_at = now(), updated_at = now()
         WHERE id = $1 AND status = 'running' AND lease_token = $2::uuid
        RETURNING id
        """,
        [job.id, job.lease_token, str(listing_id), outcome],
    )
    if not rows:
        raise LeaseLost()


# ---------------------------------------------------------------------------
# Failure
# ---------------------------------------------------------------------------


def next_retry_delay(err: GenerationError, attempts_after: int, backoff_seconds: float) -> float:
    retry_after = getattr(err, "retry_after", None)
    if retry_after:
        return float(retry_after)
    return float(backoff_seconds) * (2 ** max(attempts_after - 1, 0))


async def retry_or_fail(
    job: ClaimedJob,
    err: GenerationError,
    max_attempts: int,
    backoff_seconds: float,
) -> Optional[str]:
    """Record a concluded, failed attempt. Returns the new status, or None when the lease
    was already lost (nothing written)."""
    attempts_after = job.attempts + 1
    give_up = isinstance(err, PermanentGenerationError) or attempts_after >= max_attempts
    if not isinstance(err, (PermanentGenerationError, TransientGenerationError)):
        give_up = attempts_after >= max_attempts
    status = "failed" if give_up else "pending"
    delay = 0.0 if give_up else next_retry_delay(err, attempts_after, backoff_seconds)

    rows = await _conn().execute_query_dict(
        """
        UPDATE batch_generation_jobs
           SET status = $3, attempts = attempts + 1, lease_token = NULL,
               error = $4, error_display = $5,
               next_attempt_at = now() + make_interval(secs => $6::double precision),
               updated_at = now()
         WHERE id = $1 AND status = 'running' AND lease_token = $2::uuid
        RETURNING id
        """,
        [
            job.id,
            job.lease_token,
            status,
            (err.detail or err.message)[:2000],
            err.message,
            delay,
        ],
    )
    return status if rows else None


# ---------------------------------------------------------------------------
# Operator actions
# ---------------------------------------------------------------------------


async def retry_failed(batch_id: int, job_id: Optional[int] = None) -> int:
    """Requeue failed jobs of one batch (or one of them) with a clean slate."""
    rows = await _conn().execute_query_dict(
        """
        UPDATE batch_generation_jobs
           SET status = 'pending', attempts = 0, error = NULL, error_display = NULL,
               next_attempt_at = now(), updated_at = now()
         WHERE batch_id = $1
           AND ($2::bigint IS NULL OR id = $2::bigint)
           AND status = 'failed'
        RETURNING id
        """,
        [int(batch_id), job_id],
    )
    return len(rows)


async def remove_job(job_id: int) -> Dict[str, Any]:
    """Delete a pending or failed job, and drop its product's value entry from the batch.

    Returns {"removed": True, "batch_id", "product_id"}, or {"removed": False, "reason"}
    with reason "not_found", "running" or "done".
    """
    async with in_transaction("default") as conn:
        deleted = await conn.execute_query_dict(
            """
            DELETE FROM batch_generation_jobs
             WHERE id = $1 AND status IN ('pending', 'failed')
            RETURNING batch_id, product_id
            """,
            [int(job_id)],
        )
        if not deleted:
            existing = await conn.execute_query_dict(
                "SELECT status FROM batch_generation_jobs WHERE id = $1", [int(job_id)]
            )
            reason = existing[0]["status"] if existing else "not_found"
            return {"removed": False, "reason": reason}

        batch_id = int(deleted[0]["batch_id"])
        product_id = deleted[0]["product_id"]

        # One atomic statement, so a concurrent Remove on the same batch cannot bring this
        # entry back. Skipped when a listing for the same parent is still on the batch.
        await conn.execute_query(
            """
            UPDATE batches
               SET product_values = product_values - $2::text,
                   total_value = (
                       SELECT COALESCE(SUM((e.value ->> 'value')::numeric), 0)
                         FROM jsonb_each(product_values - $2::text) e
                        WHERE jsonb_typeof(e.value -> 'value') = 'number'
                   )
             WHERE id = $1
               AND jsonb_exists(product_values, $2::text)
               AND NOT EXISTS (
                   SELECT 1 FROM listings l WHERE l.batch_id = $1 AND l.product_id = $2::text
               )
            """,
            [batch_id, product_id],
        )

    return {"removed": True, "batch_id": batch_id, "product_id": product_id}


async def delete_jobs_for_batch(conn, batch_id: int) -> None:
    """First step of deleting a batch, inside the caller's transaction.

    Holds the claim lock so no claim can pick this batch's jobs mid-delete, and deletes the
    jobs before the batch: lock order job, then batch, the same as a completing attempt.
    """
    await conn.execute_query("SELECT pg_advisory_xact_lock(hashtext('batch_generation_claim'))")
    await conn.execute_query(
        "DELETE FROM batch_generation_jobs WHERE batch_id = $1", [int(batch_id)]
    )
