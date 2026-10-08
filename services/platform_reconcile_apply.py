"""Apply an approved reconcile plan, one parent at a time, through the existing paths.

Nothing here invents SQL for presence, delists or submission repairs (KTD3). It composes
operations that already carry their own guards, so their refusals, locks and triggers
stay in force. The planner (platform_reconcile_service) decides; this writes.

The per-parent boundary is the unit of commit and of resume: platform_delist_service
already takes the per-product advisory lock and runs its own transaction, so one parent
is the natural edge, and the job's cursor advances only after that parent's writes land.

Four guards here exist because the first draft of the plan did not have them:

  The drift check (R10 in spirit): the run re-derives the plan, so compare against the
  counts the operator approved and refuse to act on more. Approval is approval of a
  number, not a blank cheque.

  The presence read-back: ExternalListingService.record never raises and returns only a
  count, silently skipping any row it could not upsert. Building the revert record from
  the plan rather than the writes would name rows that do not exist, and a systematic
  upsert failure would finish as a successful import with an overstated count.

  Row locking on repairs: the delist half refuses while a submission is in flight, but
  the repair half had no equivalent. spo_poller._batch_upload_pending claims pending
  rows under select_for_update(skip_locked=True) and flips them to processing, so a
  flush overlapping the import would otherwise collide.

  Restore before presence on a relist: the delist entry governs what the completion
  function sees, so closing it first and writing presence second means the recompute
  reads the final state once instead of flapping.
"""

import json
import logging
from typing import Any, Optional

from tortoise import connections

from services import platform_delist_service as pds
from services import platform_import_job_service as jobs
from services import platform_reconcile_service as prs
from services.external_listing_service import CHILD, SOURCE_MANUAL, ExternalListingService
from services.platform_delist_service import DelistRefused

logger = logging.getLogger(__name__)

# Counts that must never exceed what the operator approved. Fewer is fine: the world
# moved on and the plan shrank. More means it grew, and nobody approved the extra.
_BOUNDED = ("delisted_parents", "delisted_sizes", "listed_sizes", "repairs")


class DriftRefused(Exception):
    """The re-derived plan is bigger than the one that was approved."""


def check_drift(approved: Optional[dict[str, Any]], current: dict[str, Any]) -> None:
    """Refuse a run whose plan grew since the preview (U6 step 3b).

    An absent approved record is itself a refusal: a job that recorded no counts cannot
    have been previewed, so there is nothing to have approved.
    """
    if not approved:
        raise DriftRefused("This import has no approved counts to check against")
    grown = [
        f"{key} {approved.get(key, 0)} -> {current.get(key, 0)}"
        for key in _BOUNDED
        if (current.get(key) or 0) > (approved.get(key) or 0)
    ]
    if grown:
        raise DriftRefused("The plan grew since it was approved: " + "; ".join(grown))


def _comment(file_name: Optional[str], job_id: int) -> str:
    """What an operator reads in the product's listing history."""
    where = file_name or "an export"
    return f"Import reconcile from {where} (import {job_id})"


async def _presence_rows_for(platform_id: str, parent_sku: str) -> set[str]:
    """The child SKUs that actually carry a presence row right now."""
    rows = await connections.get("default").execute_query_dict(
        "SELECT sku FROM external_listing_ids "
        "WHERE platform_id = $1 AND parent_sku = $2 AND sku IS NOT NULL",
        [platform_id, parent_sku],
    )
    return {r["sku"] for r in rows}


async def _clear_stale_presence(
    platform_id: str, parent_sku: str, skus: list[str]
) -> list[dict[str, Any]]:
    """Remove presence rows for a product that is gone, returning what was removed.

    Not a delist: platform_delist_service cannot load a product that does not exist, and
    an entry it could write would be unreachable (the listing history 404s without a
    product) while letting Restore recreate presence for a phantom. So the rows go, and
    the returned pre-image is what the import's revert record carries.

    Safe to delete without the un-completion dance the real delist path needs: a parent in
    this state has no listings and no submission rows, verified across all 19 on prod, so
    the presence trigger's recompute finds nothing and no batch can reopen. The DELETE is
    still scoped to this platform and parent, never to the sku list alone.
    """
    conn = connections.get("default")
    removed = await conn.execute_query_dict(
        "DELETE FROM external_listing_ids "
        "WHERE platform_id = $1 AND parent_sku = $2 "
        "RETURNING id::text AS id, sku, level, source, external_id",
        [platform_id, parent_sku],
    )
    if skus and len(removed) != len(skus):
        # The plan was derived from a read; the rows may have moved since.
        logger.info(
            "Import cleared %s stale presence row(s) for %s on %s, plan expected %s",
            len(removed),
            parent_sku,
            platform_id,
            len(skus),
        )
    return [dict(r) for r in removed]


async def _recompute_parent(parent_sku: str) -> None:
    """Recompute every listing of this parent with p_allow_unflag false (KTD7).

    A sweep may complete a listing, never un-complete one.
    """
    await connections.get("default").execute_query(
        "SELECT recompute_listing_submitted(l.id, false) FROM listings l "
        "WHERE l.product_id = $1",
        [parent_sku],
    )


async def _mark_listed(
    platform_id: str, parent_sku: str, skus: list[str]
) -> tuple[list[str], list[str]]:
    """Write presence rows and report what actually landed. Returns (written, missing)."""
    before = await _presence_rows_for(platform_id, parent_sku)
    wanted = [s for s in skus if s not in before]
    if not wanted:
        return [], []
    await ExternalListingService.record(
        platform_id,
        [{"level": CHILD, "sku": s, "parent_sku": parent_sku} for s in wanted],
        source=SOURCE_MANUAL,
    )
    after = await _presence_rows_for(platform_id, parent_sku)
    written = sorted(set(wanted) & after)
    missing = sorted(set(wanted) - after)
    if missing:
        # record() swallowed these. Report them rather than counting them as listed.
        logger.warning(
            "Import presence write did not land for %s on %s: %s",
            parent_sku,
            platform_id,
            missing,
        )
    await _recompute_parent(parent_sku)
    return written, missing


async def apply_parent(
    *,
    plan: prs.ParentPlan,
    platform_id: str,
    user_id: str,
    job_id: int,
    file_name: Optional[str] = None,
) -> dict[str, Any]:
    """Apply one parent's verdict. Never raises for an expected refusal.

    Returns a result dict carrying what changed, what was refused, and the revert
    material for that parent.
    """
    out: dict[str, Any] = {
        "parent_sku": plan.parent_sku,
        "outcome": plan.outcome,
        "listed": [],
        "delisted": [],
        "delist_id": None,
        "restored_delist_id": None,
        "cleared_rows": [],
        "refused": None,
    }

    if not plan.writes:
        # NOTHING, REFUSED and SKIP_UNKNOWN_PARENT all carry straight through, so the
        # preview's "cannot act on" block and the result's agree by construction.
        out["refused"] = plan.reason
        return out

    comment = _comment(file_name, job_id)

    try:
        if plan.outcome == prs.CLEAR_PRESENCE:
            # The product is gone, so there is nothing to delist against. Take the stale
            # presence off and keep the pre-image; this is the only record of it.
            removed = await _clear_stale_presence(
                platform_id, plan.parent_sku, plan.sizes
            )
            out["cleared_rows"] = removed
            out["delisted"] = [r["sku"] for r in removed if r.get("sku")]
            return out

        if plan.outcome == prs.RELIST:
            # R12a. Close the delist first, then record presence.
            await pds.restore(plan.delist_id, user_id)
            out["restored_delist_id"] = plan.delist_id
            written, missing = await _mark_listed(platform_id, plan.parent_sku, plan.sizes)
            out["listed"] = written
            if missing:
                out["refused"] = f"{len(missing)} presence rows did not write"
            return out

        if plan.outcome == prs.MARK_LISTED:
            written, missing = await _mark_listed(platform_id, plan.parent_sku, plan.sizes)
            out["listed"] = written
            if missing:
                out["refused"] = f"{len(missing)} presence rows did not write"
            return out

        # DELIST_PARENT passes None so the service takes every size itself, which keeps
        # "what counts as all of them" in one place.
        child_skus = None if plan.outcome == prs.DELIST_PARENT else list(plan.sizes)
        entry = await pds.delist(
            plan.parent_sku, platform_id, child_skus, comment, user_id
        )
        out["delisted"] = list(plan.sizes)
        out["delist_id"] = (entry or {}).get("id")
        return out

    except DelistRefused as exc:
        # R16: recorded and the run carries on. The service is the authority, so a
        # refusal here is normal, not an error.
        logger.info(
            "Import %s: %s refused for %s on %s (%s)",
            job_id,
            plan.outcome,
            plan.parent_sku,
            platform_id,
            exc.code,
        )
        out["outcome"] = prs.REFUSED
        out["refused"] = exc.message
        return out


async def apply_repair(
    *, repair: prs.Repair, platform_id: str, user_id: str, job_id: int,
    file_name: Optional[str] = None,
) -> dict[str, Any]:
    """Repair one stale submission row under a row lock (R17, R18, R19).

    The lock and the status recheck are what keep a concurrent flush from colliding:
    the planner decided from a snapshot, and the row may have moved since.
    """
    out: dict[str, Any] = {
        "listing_id": repair.listing_id,
        "parent_sku": repair.parent_sku,
        "previous_status": repair.previous_status,
        "outcome": repair.outcome,
        "applied": False,
        "refused": None,
    }
    from tortoise.transactions import in_transaction

    async with in_transaction("default") as txn:
        rows = await txn.execute_query_dict(
            """
            SELECT id, status, platform_meta
            FROM listing_submissions
            WHERE listing_id = $1::uuid AND platform_id = $2
            ORDER BY attempt_number DESC
            LIMIT 1
            FOR UPDATE
            """,
            [repair.listing_id, platform_id],
        )
        if not rows:
            out["refused"] = "The submission row is gone"
            return out
        row = rows[0]
        if row["status"] != repair.previous_status:
            # A flush claimed it, or an operator acted. The plan is stale for this row.
            out["refused"] = f"Status moved to {row['status']}"
            return out

        step = {
            "step": "import_reconcile",
            "outcome": repair.outcome,
            "previous_status": repair.previous_status,
            "file": file_name,
            "import_id": job_id,
            "by": user_id,
        }
        if repair.outcome == "reviewed":
            await txn.execute_query(
                """
                UPDATE listing_submissions
                SET status = 'success', reviewed_at = CURRENT_TIMESTAMP, reviewed_by = $2,
                    platform_meta = jsonb_set(
                        COALESCE(platform_meta, '{}'::jsonb), '{steps}',
                        COALESCE(platform_meta -> 'steps', '[]'::jsonb) || $3::jsonb, true)
                WHERE id = $1
                """,
                [row["id"], user_id, json.dumps([step])],
            )
        else:
            await txn.execute_query(
                """
                UPDATE listing_submissions
                SET status = 'success',
                    platform_meta = jsonb_set(
                        COALESCE(platform_meta, '{}'::jsonb), '{steps}',
                        COALESCE(platform_meta -> 'steps', '[]'::jsonb) || $2::jsonb, true)
                WHERE id = $1
                """,
                [row["id"], json.dumps([step])],
            )
        out["applied"] = True
    await _recompute_parent(repair.parent_sku)
    return out


def revert_entry(results: list[dict[str, Any]]) -> dict[str, Any]:
    """Build R21's revert record from what actually landed.

    A delisted parent is named by its platform_delists entry id rather than by copying
    the pre-image: that entry already holds removed_rows and is exactly what Restore
    reads, so duplicating it would mean two copies that can disagree.
    """
    return {
        "presence_added": [
            {"parent_sku": r["parent_sku"], "skus": r["listed"]}
            for r in results
            if r.get("listed")
        ],
        "delist_entries": [
            {"parent_sku": r["parent_sku"], "delist_id": r["delist_id"]}
            for r in results
            if r.get("delist_id")
        ],
        "presence_cleared": [
            {"parent_sku": r["parent_sku"], "rows": r["cleared_rows"]}
            for r in results
            if r.get("cleared_rows")
        ],
        "delists_restored": [
            {"parent_sku": r["parent_sku"], "delist_id": r["restored_delist_id"]}
            for r in results
            if r.get("restored_delist_id")
        ],
        "repairs": [
            {
                "listing_id": r["listing_id"],
                "previous_status": r["previous_status"],
                "outcome": r["outcome"],
            }
            for r in results
            if r.get("applied")
        ],
    }
