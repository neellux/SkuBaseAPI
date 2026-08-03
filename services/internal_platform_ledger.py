"""State and audit-trail persistence for the consignment pipeline.

Two responsibilities, deliberately in one module so the poller and the repricer pass
cannot drift apart on the requeue rule:

  - internal_platform_state       current state, in-flight claims, idempotency
  - internal_platform_submissions append-only audit

The state map is loaded ONCE per cycle into a dict. Querying it per product would be
~3,000 round trips per cycle against a remote Postgres, and because it sits inside the
per-product loop it would directly extend every cycle.

A submission row is written ONLY when an API call was actually made. An evaluation that
results in no call is not an action. That rule is the difference between roughly
0.2 GB/year and 31 GB/year.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping, Sequence

from tortoise import connections
from tortoise.transactions import in_transaction

from models.db_models import (
    InternalPlatform,
    InternalPlatformAction,
    InternalPlatformSkipReason,
    InternalPlatformState,
    InternalPlatformStatus,
    InternalPlatformSubmission,
)
from services.shopify_client import redact

logger = logging.getLogger(__name__)

# An in-flight claim older than this is assumed to be a crashed process, not work in
# progress. Without this sweep the partial unique index on the claim would block that
# (platform, parent_sku) forever after a single crash.
STALE_INFLIGHT_MINUTES = 60

# Syncio's documented delivery window is 1-3 days. Past this we report, we do not retry.
AWAITING_SYNC_ALERT_DAYS = 4


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def get_platform(platform_id: str) -> InternalPlatform | None:
    return await InternalPlatform.get_or_none(id=platform_id)


async def load_state_map(platform_id: str) -> dict[str, InternalPlatformState]:
    """Whole state table for one platform, keyed by parent_sku. One query per cycle."""
    rows = await InternalPlatformState.filter(internal_platform_id=platform_id)
    return {r.parent_sku: r for r in rows}


async def recover_stale_inflight(platform_id: str) -> int:
    """Release claims left behind by a crashed process.

    Deliberately action-aware: a stale `delete` claim is NOT auto-released, because
    re-attempting an irreversible product delete on the basis of a crash we do not
    understand is exactly the wrong response. Those are surfaced for a human instead.
    """
    cutoff = _now() - timedelta(minutes=STALE_INFLIGHT_MINUTES)
    stale = await InternalPlatformState.filter(
        internal_platform_id=platform_id,
        inflight_action__not_isnull=True,
        inflight_since__lt=cutoff,
    )
    released = 0
    for row in stale:
        if row.inflight_action == InternalPlatformAction.DELETE:
            logger.error(
                "%s: stale DELETE claim on %s since %s - NOT auto-releasing, needs a human",
                platform_id, row.parent_sku, row.inflight_since,
            )
            continue
        logger.warning(
            "%s: releasing stale %s claim on %s (since %s)",
            platform_id, row.inflight_action, row.parent_sku, row.inflight_since,
        )
        row.inflight_action = None
        row.inflight_since = None
        await row.save(update_fields=["inflight_action", "inflight_since", "updated_at"])
        released += 1
    return released


async def close_orphaned_audit_rows(platform_id: str) -> int:
    """Settle audit rows left open by a process that died mid-batch.

    A row is opened before its API call and closed after, so a kill in between leaves it
    PENDING forever. Batching widened that window from one product to eighty: killing the
    poller on 2026-07-29 stranded 81 reprice rows, and nothing in the system would ever
    have closed them.

    They are closed FAILED, not SUCCESS, and the error says so: the process died between
    the request and the bookkeeping, so whether Shopify applied the mutation is genuinely
    unknown. Failed-but-unknown is the honest reading, and it costs nothing operationally
    because requeueing is decided by the state row's drift, not by this status.

    `delete` is excluded on purpose, matching recover_stale_inflight(): an orphaned delete
    is the one case where the pre-image in `payload` is the only surviving record of what
    the product was, and it should be seen by a human, not tidied away by a sweep.
    """
    cutoff = _now() - timedelta(minutes=STALE_INFLIGHT_MINUTES)
    conn = connections.get("default")
    rows = await conn.execute_query_dict(
        "UPDATE internal_platform_submissions "
        "   SET status = $2, updated_at = now(), "
        "       error = 'process interrupted between the API call and the ledger write; "
        "outcome unknown' "
        " WHERE internal_platform_id = $1 AND status = 'pending' "
        "   AND action <> 'delete' AND created_at < $3 "
        "RETURNING id",
        [platform_id, str(InternalPlatformStatus.FAILED), cutoff],
    )
    return len(rows)


async def claim(platform_id: str, parent_sku: str,
                action: InternalPlatformAction) -> InternalPlatformState | None:
    """Take the in-flight claim, or return None if something else holds it.

    Backed by the partial unique index, so this is safe even if two processes race.

    The connection name is explicit and must stay that way: this app registers three
    databases (default, product_db, photography_db), and Tortoise refuses a bare
    in_transaction() when more than one exists. Every other call site in the codebase
    names its connection; this one did not, and because claim() is only reached on the
    WRITE path it survived every read-side test and failed on the first real Submit.
    """
    async with in_transaction("default"):
        row, _ = await InternalPlatformState.get_or_create(
            internal_platform_id=platform_id,
            parent_sku=parent_sku,
            defaults={"current_status": InternalPlatformStatus.PENDING},
        )
        if row.inflight_action is not None:
            return None
        row.inflight_action = action
        row.inflight_since = _now()
        await row.save(update_fields=["inflight_action", "inflight_since", "updated_at"])
        return row


async def release_many(platform_id: str, parent_skus: Sequence[str]) -> None:
    """Drop claims without touching the audit rows. The crash-safety net, not the
    normal close - finish_and_release() is what a completed batch calls."""
    if not parent_skus:
        return
    conn = connections.get("default")
    await conn.execute_query(
        "UPDATE internal_platform_state SET inflight_action = NULL, inflight_since = NULL, "
        "  updated_at = now() "
        "WHERE internal_platform_id = $1 AND parent_sku = ANY($2::text[])",
        [platform_id, list(parent_skus)],
    )


async def claim_and_record(
    platform_id: str, action: InternalPlatformAction,
    items: Sequence[tuple[str, str | None, str | None, Mapping[str, Any] | None]],
) -> dict[str, int]:
    """Claim a batch AND open its audit rows in one round trip. {parent_sku: row id}.

    items is (parent_sku, source_gid, dest_gid, payload). Only claimed SKUs appear in the
    result, so the keys are the batch the caller may act on - a row another process holds
    is simply absent rather than silently double-processed, the same guarantee claim()
    gives. The WHERE clause is backed by the partial unique index.

    These were two statements, and against a database ~0.57s away that is the whole cost:
    the work is microseconds and the wire is everything. The data-modifying CTE lets the
    INSERT read the UPDATE's RETURNING within a single statement, so the claim and its
    audit row can no longer be separated by a crash either.

    Ordering is not guaranteed by RETURNING and callers must key off the returned mapping
    rather than assume it matches `items`.
    """
    if not items:
        return {}
    skus, sources, dests, payloads = [], [], [], []
    for sku, src, dest, payload in items:
        skus.append(sku)
        sources.append(src)
        dests.append(dest)
        payloads.append(json.dumps(dict(payload)) if payload else None)

    conn = connections.get("default")
    rows = await conn.execute_query_dict(
        "WITH claimed AS ("
        "  UPDATE internal_platform_state"
        "     SET inflight_action = $2, inflight_since = now(), updated_at = now()"
        "   WHERE internal_platform_id = $1 AND parent_sku = ANY($3::text[])"
        "     AND inflight_action IS NULL"
        "  RETURNING parent_sku"
        ") "
        "INSERT INTO internal_platform_submissions"
        "  (internal_platform_id, parent_sku, action, status,"
        "   source_product_gid, dest_product_gid, payload) "
        "SELECT $1::varchar, c.parent_sku, $2::varchar, $4::varchar,"
        "       v.source_gid, v.dest_gid, v.payload::jsonb "
        "  FROM claimed c "
        "  JOIN unnest($3::text[], $5::text[], $6::text[], $7::text[])"
        "       AS v(parent_sku, source_gid, dest_gid, payload)"
        "    ON v.parent_sku = c.parent_sku "
        "RETURNING id, parent_sku",
        [platform_id, str(action), skus, str(InternalPlatformStatus.PENDING),
         sources, dests, payloads],
    )
    return {r["parent_sku"]: r["id"] for r in rows}


async def finish_and_release(
    platform_id: str, *,
    release_skus: Sequence[str],
    ok_ids: Sequence[int] = (),
    bad_ids: Sequence[int] = (),
    error: str | None = None,
    dest_gids: Mapping[str, str] | None = None,
) -> None:
    """Close a batch: audit rows settled, destination GIDs recorded, claims dropped.

    Four statements collapsed into one - success rows, failure rows, and the state update
    that both records the resolved destination product and releases the claim. The two
    audit updates are data-modifying CTEs, which Postgres runs to completion whether or
    not the primary query reads them.

    Releasing every claimed SKU while setting a GID for only the successful ones is why
    the state update COALESCEs: a failure passes a NULL gid and keeps whatever it had.
    """
    if not release_skus:
        return
    gids = dest_gids or {}
    conn = connections.get("default")
    await conn.execute_query(
        "WITH ok AS ("
        "  UPDATE internal_platform_submissions SET status = $1, updated_at = now()"
        "   WHERE id = ANY($2::bigint[])"
        "), bad AS ("
        "  UPDATE internal_platform_submissions"
        "     SET status = $3, error = $4, updated_at = now()"
        "   WHERE id = ANY($5::bigint[])"
        ") "
        "UPDATE internal_platform_state s"
        "   SET inflight_action = NULL, inflight_since = NULL,"
        "       dest_product_gid = COALESCE(v.gid, s.dest_product_gid),"
        "       updated_at = now() "
        "  FROM unnest($7::text[], $8::text[]) AS v(sku, gid) "
        " WHERE s.internal_platform_id = $6 AND s.parent_sku = v.sku",
        [str(InternalPlatformStatus.SUCCESS), list(ok_ids),
         str(InternalPlatformStatus.FAILED), redact(error) if error else None,
         list(bad_ids), platform_id,
         list(release_skus), [gids.get(sku) for sku in release_skus]],
    )


async def release(row: InternalPlatformState) -> None:
    row.inflight_action = None
    row.inflight_since = None
    await row.save(update_fields=["inflight_action", "inflight_since", "updated_at"])


async def record(
    *,
    platform_id: str,
    parent_sku: str,
    action: InternalPlatformAction,
    status: InternalPlatformStatus,
    source_gid: str | None = None,
    dest_gid: str | None = None,
    payload: Mapping[str, Any] | None = None,
    result: Mapping[str, Any] | None = None,
    error: str | None = None,
    skip_reason: InternalPlatformSkipReason | None = None,
    actor: str | None = None,
    triggered_by: str = "scheduler",
) -> InternalPlatformSubmission:
    """Append one audit row. Call ONLY when an API call was actually attempted.

    payload/result are whitelisted by the caller. Never pass a raw httpx response or a
    header map: headers carry the access token, and this table is long-lived.
    """
    return await InternalPlatformSubmission.create(
        internal_platform_id=platform_id,
        parent_sku=parent_sku,
        action=action,
        status=status,
        skip_reason=skip_reason,
        source_product_gid=source_gid,
        dest_product_gid=dest_gid,
        payload=dict(payload) if payload else None,
        result=dict(result) if result else None,
        error=redact(error) if error else None,
        actor=actor,
        triggered_by=triggered_by,
    )


async def record_pre_image(
    *,
    platform_id: str,
    parent_sku: str,
    action: InternalPlatformAction,
    dest_gid: str,
    source_gid: str | None,
    before: Mapping[str, Any],
    actor: str | None = None,
) -> InternalPlatformSubmission:
    """Commit the pre-image BEFORE an irreversible mutation.

    For a delete this is the only reconstruction material that will ever exist, and
    post-hoc logging of a delete is not an audit trail. Committing first also closes the
    window where the mutation succeeds, the process dies, and no row exists at all -
    which would leave the product unmanaged and, under the ledger-scoping invariant,
    untouchable by the automation forever.
    """
    return await record(
        platform_id=platform_id,
        parent_sku=parent_sku,
        action=action,
        status=InternalPlatformStatus.PENDING,
        source_gid=source_gid,
        dest_gid=dest_gid,
        payload={"before": dict(before)},
        actor=actor,
    )


async def finish(
    row: InternalPlatformSubmission,
    *,
    status: InternalPlatformStatus,
    result: Mapping[str, Any] | None = None,
    error: str | None = None,
    skip_reason: InternalPlatformSkipReason | None = None,
) -> None:
    row.status = status
    if result is not None:
        row.result = dict(result)
    if error is not None:
        row.error = redact(error)
    if skip_reason is not None:
        row.skip_reason = skip_reason
    await row.save(update_fields=["status", "result", "error", "skip_reason", "updated_at"])


async def mark_listed(state: InternalPlatformState, source_gid: str) -> None:
    state.source_product_gid = source_gid
    state.listed_at = _now()
    state.current_status = "pending_normalization"
    state.delist_strikes = 0
    await state.save(update_fields=[
        "source_product_gid", "listed_at", "current_status", "delist_strikes", "updated_at",
    ])


async def mark_normalized_many(platform_id: str, parent_skus: Sequence[str]) -> int:
    """Advance converged products from pending_normalization to listed. One statement.

    Convergence is the evidence, not a completion callback: these are products the
    destination poller re-read and found already matching the desired vendor, tags and
    source-derived price, so there is nothing left to normalize. That is a stronger claim
    than "we wrote to it and the write returned 200" - it survives a partial batch, a
    crash between mutation and bookkeeping, and a manual edit on the storefront.

    This replaces a mark_normalized() that took a desired_hash the poller never computed
    and, in consequence, was never called from anywhere - which is why every delivered
    product sat at pending_normalization forever even after its vendor, tags and price
    had all been corrected successfully.

    The status predicate is deliberately narrow. Only pending_normalization is advanced:
    a delisting or skipped row that happens to look converged must keep its own status,
    and re-running this cannot move a row that is already listed.
    """
    if not parent_skus:
        return 0
    conn = connections.get("default")
    rows = await conn.execute_query_dict(
        "UPDATE internal_platform_state "
        "   SET current_status = 'listed', normalize_done_at = now(), "
        "       skip_reason = NULL, last_error = NULL, updated_at = now() "
        " WHERE internal_platform_id = $1 AND parent_sku = ANY($2::text[]) "
        "   AND current_status = 'pending_normalization' "
        "RETURNING parent_sku",
        [platform_id, list(parent_skus)],
    )
    return len(rows)


async def mark_skipped(state: InternalPlatformState,
                       reason: InternalPlatformSkipReason, detail: str) -> None:
    state.skip_reason = reason
    state.last_error = detail[:2000]
    state.current_status = "skipped"
    await state.save(update_fields=[
        "skip_reason", "last_error", "current_status", "updated_at",
    ])


async def mark_pending_delist(state: InternalPlatformState, cause: str) -> None:
    """Soak satisfied: this product is queued for a human to delist.

    Deliberately NOT the delete itself. The delete is irreversible and now happens only
    when someone presses Delist, so this status is the reviewable step in between.
    """
    state.current_status = "pending_delisting"
    # [:40], not [:50]. The column is varchar(40); a longer cause raised on write rather
    # than truncating. Latent until now only because every existing cause is a short code.
    state.skip_reason = cause[:40] if cause else None
    await state.save(update_fields=["current_status", "skip_reason", "updated_at"])


async def mark_delisted(state: InternalPlatformState) -> None:
    """Untagged on source and removed from the destination.

    dest_product_gid is cleared: the destination product no longer exists, and leaving a
    dangling GID would make footprint() keep counting it and would give a later delist
    something to try to delete twice.

    listed_at is cleared for the same reason, and it is not optional. "Awaiting Syncio" is
    defined purely as `listed_at IS NOT NULL AND dest_product_gid IS NULL` - it never looks
    at current_status - so clearing only the GID leaves the row in a state indistinguishable
    from a product still waiting on delivery. Every successful delist then reappeared as an
    awaiting-Syncio row, and since listed_at was weeks old it cleared the four-day staleness
    cutoff instantly rather than aging into it: on 2026-08-03 one delist run put 155 products
    into "waiting over 4 days" the moment it finished deleting them. The row is no longer
    tagged on the source, so the timestamp saying it is has to go with the GID.

    delisted_at carries the history that listed_at used to imply.
    """
    state.current_status = "delisted"
    state.dest_product_gid = None
    state.listed_at = None
    state.delisted_at = _now()
    state.delist_strikes = 0
    await state.save(update_fields=[
        "current_status", "dest_product_gid", "listed_at", "delisted_at",
        "delist_strikes", "updated_at",
    ])


async def ready_for_listing(platform_id: str,
                            limit: int | None = None) -> list[InternalPlatformState]:
    """Products the scan judged eligible, in the order the Products tab shows them.

    Ordered by title so "the next five" means the five a human would predict from the
    screen, with parent_sku as the tiebreaker since title is not unique - the same
    deterministic pair the /products endpoint sorts on.
    """
    qs = InternalPlatformState.filter(
        internal_platform_id=platform_id, current_status="ready_for_listing"
    ).order_by("title", "parent_sku")
    if limit:
        qs = qs.limit(limit)
    return await qs


async def pending_delists(platform_id: str) -> list[InternalPlatformState]:
    """Everything queued for the Delist button, oldest first."""
    return await InternalPlatformState.filter(
        internal_platform_id=platform_id, current_status="pending_delisting"
    ).order_by("updated_at")


async def last_submit_at(platform_id: str) -> datetime | None:
    """When this pipeline last successfully tagged something.

    Derived from the ledger rather than kept in a column: every successful tag already
    writes an action='list', status='success' row, so this cannot drift from reality the
    way a separately-maintained timestamp would.
    """
    rows = await InternalPlatformSubmission.filter(
        internal_platform_id=platform_id,
        action=InternalPlatformAction.LIST,
        status=InternalPlatformStatus.SUCCESS,
    ).order_by("-created_at").limit(1).values_list("created_at", flat=True)
    return rows[0] if rows else None


async def apply_scan_statuses(
    platform_id: str, desired: Mapping[str, tuple[str, str | None, int]]
) -> int:
    """Upsert (status, skip_reason, variant_count) for every parent the scan judged.

    `desired` is parent_sku -> (current_status, skip_reason, variant_count), already
    diffed by the caller against the loaded state map, so only genuine changes arrive.

    Multi-row VALUES rather than one statement per row: the database is remote at ~0.57s
    a round trip, and a scan can touch thousands of parents. 200 rows x 4 params stays far
    under Postgres' 65,535 parameter ceiling.
    """
    if not desired:
        return 0
    conn = connections.get("default")
    items = list(desired.items())
    written = 0
    for i in range(0, len(items), 200):
        chunk = items[i:i + 200]
        values, params = [], []
        for n, (parent_sku, (status, reason, variants)) in enumerate(chunk):
            base = n * 5
            values.append(
                f"(${base + 1}, ${base + 2}, ${base + 3}, ${base + 4}, ${base + 5})")
            params.extend([platform_id, parent_sku, status, reason, variants])
        await conn.execute_query(
            "INSERT INTO internal_platform_state "
            "  (internal_platform_id, parent_sku, current_status, skip_reason, variant_count) "
            f"VALUES {', '.join(values)} "
            "ON CONFLICT (internal_platform_id, parent_sku) DO UPDATE SET "
            "  current_status = EXCLUDED.current_status, "
            "  skip_reason    = EXCLUDED.skip_reason, "
            "  variant_count  = EXCLUDED.variant_count, "
            "  updated_at     = CURRENT_TIMESTAMP",
            params,
        )
        written += len(chunk)
    return written


async def bump_delist_strike(state: InternalPlatformState) -> int:
    """Soak counter. Delist fires only once this reaches the configured threshold.

    A single bad source read cannot trigger deletes; the product has to fail
    qualification on consecutive cycles.
    """
    state.delist_strikes += 1
    await state.save(update_fields=["delist_strikes", "updated_at"])
    return state.delist_strikes


async def clear_delist_strikes(state: InternalPlatformState) -> None:
    if state.delist_strikes:
        state.delist_strikes = 0
        await state.save(update_fields=["delist_strikes", "updated_at"])


async def apply_delist_strikes(platform_id: str, bump: Sequence[str],
                               clear: Sequence[str]) -> int:
    """Advance and reset the soak counter for a whole cycle, in one statement.

    The per-row bump_delist_strike() ran INSIDE the scan loop, so a cycle that died
    halfway left the products it had already reached one strike closer to deletion than
    the rest - a partial write to the counter that decides an irreversible action. Doing
    it once, after every gate has passed, means a cycle either advances the soak for
    everything it judged or for nothing.

    The bump is `delist_strikes + 1` computed in SQL rather than from the value the caller
    read, so it stays correct even if the row moved underneath the scan.
    """
    if not bump and not clear:
        return 0
    conn = connections.get("default")
    rows = await conn.execute_query_dict(
        "UPDATE internal_platform_state s "
        "   SET delist_strikes = CASE WHEN s.parent_sku = ANY($2::text[]) "
        "                             THEN s.delist_strikes + 1 ELSE 0 END, "
        "       updated_at = now() "
        " WHERE s.internal_platform_id = $1 "
        "   AND (s.parent_sku = ANY($2::text[]) OR s.parent_sku = ANY($3::text[])) "
        "RETURNING parent_sku",
        [platform_id, list(bump), list(clear)],
    )
    return len(rows)


async def awaiting_sync(platform_id: str,
                        days: int = AWAITING_SYNC_ALERT_DAYS) -> list[InternalPlatformState]:
    """Tagged on source, never delivered by Syncio. Report only, never auto-retry."""
    cutoff = _now() - timedelta(days=days)
    return await InternalPlatformState.filter(
        internal_platform_id=platform_id,
        dest_product_gid__isnull=True,
        listed_at__not_isnull=True,
        listed_at__lt=cutoff,
    )


async def orphaned_delists(platform_id: str) -> list[InternalPlatformSubmission]:
    """Source untagged but the destination action failed. Input to the orphan sweep."""
    return await InternalPlatformSubmission.filter(
        internal_platform_id=platform_id,
        action__in=[InternalPlatformAction.DELETE],
        status=InternalPlatformStatus.FAILED,
    ).order_by("-updated_at")


async def products_in_flight(platform_id: str) -> int:
    """Products tagged on source that Syncio has not delivered yet.

    The predicate is identical to awaiting_sync's: listed_at set, dest_product_gid still
    null. This is what the submit gate budgets against, because Syncio's throughput limit
    is counted in whole products.
    """
    conn = connections.get("default")
    rows = await conn.execute_query_dict(
        "SELECT count(*) AS n FROM internal_platform_state "
        "WHERE internal_platform_id = $1 AND listed_at IS NOT NULL "
        "AND dest_product_gid IS NULL",
        [platform_id],
    )
    return int(rows[0]["n"]) if rows else 0


# One row per scanned product. Written by refresh_product_facts below.
@dataclass(frozen=True, slots=True)
class ProductFacts:
    """What Shopify says about a product, as opposed to what the pipeline decided."""

    source_gid: str
    title: str
    image_url: str | None
    product_type: str | None
    inventory: int
    variant_count: int
    source_price: Decimal | None
    source_compare_at: Decimal | None
    sts_price: Decimal | None
    variants: tuple[dict, ...] = ()

    def differs_from(self, row: InternalPlatformState) -> bool:
        """True when this row would actually change. Keeps steady-state writes near zero."""
        return (
            row.source_product_gid != self.source_gid
            or row.title != self.title
            or row.image_url != self.image_url
            or row.product_type != self.product_type
            or row.inventory != self.inventory
            or row.variant_count != self.variant_count
            or row.source_price != self.source_price
            or row.source_compare_at != self.source_compare_at
            or row.sts_price != self.sts_price
            or (row.variants or []) != [dict(v) for v in self.variants]
        )


_FACT_COLUMNS = ("internal_platform_id", "parent_sku", "source_product_gid", "title",
                 "image_url", "product_type", "inventory", "variant_count",
                 "source_price", "source_compare_at", "sts_price", "variants")


async def refresh_product_facts(
    platform_id: str, facts: Mapping[str, ProductFacts]
) -> int:
    """Upsert the Shopify-derived facts for every parent the scan saw.

    Deliberately separate from apply_scan_statuses, and the separation is the point.
    derive_scan_status returns None for anything TAGGED - correct, because a tagged
    product's status belongs to the tag/normalize/delist paths and the destination poller
    owns `listed`. But that means a listed product would never have its title, price or
    stock refreshed if facts rode along with status. Facts are safe to write for every
    product; status is not.

    So the DO UPDATE clause below MUST NOT mention current_status or skip_reason. A new
    row gets the table's default status ('pending') on INSERT and the scan's status pass
    corrects it moments later; an existing row keeps whatever the pipeline set.

    Also writes source_product_gid, which apply_scan_statuses omits - that omission is why
    scan-created rows had no Shopify link in the UI.
    """
    if not facts:
        return 0
    conn = connections.get("default")
    items = list(facts.items())
    width = len(_FACT_COLUMNS)
    written = 0
    for i in range(0, len(items), 200):
        chunk = items[i:i + 200]
        values, params = [], []
        for n, (parent_sku, f) in enumerate(chunk):
            base = n * width
            values.append("(" + ", ".join(f"${base + k + 1}" for k in range(width)) + ")")
            params.extend([platform_id, parent_sku, f.source_gid, f.title, f.image_url,
                           f.product_type, f.inventory, f.variant_count,
                           f.source_price, f.source_compare_at, f.sts_price,
                           json.dumps([dict(v) for v in f.variants])])
        await conn.execute_query(
            f"INSERT INTO internal_platform_state ({', '.join(_FACT_COLUMNS)}) "
            f"VALUES {', '.join(values)} "
            "ON CONFLICT (internal_platform_id, parent_sku) DO UPDATE SET "
            "  source_product_gid = EXCLUDED.source_product_gid, "
            "  title              = EXCLUDED.title, "
            "  image_url          = EXCLUDED.image_url, "
            "  product_type       = EXCLUDED.product_type, "
            "  inventory          = EXCLUDED.inventory, "
            "  variant_count      = EXCLUDED.variant_count, "
            "  source_price       = EXCLUDED.source_price, "
            "  source_compare_at  = EXCLUDED.source_compare_at, "
            "  sts_price          = EXCLUDED.sts_price, "
            "  variants           = EXCLUDED.variants, "
            "  updated_at         = CURRENT_TIMESTAMP",
            params,
        )
        written += len(chunk)
    return written


async def flag_reassigned(platform_id: str, items: Mapping[str, str],
                          queue: bool = False) -> int:
    """Mark orphaned rows as reassigned_sku, and optionally queue them for delisting.

    `items` is parent_sku -> human detail, e.g. "RHD-MOTW-0040/L -> RHD-MOTW-0038/L".

    FLAGGING and QUEUEING are separate on purpose, and the default is flag-only. Marking a
    row costs nothing and is reversible; moving it to pending_delisting puts it in the queue
    the Delist button drains, and execute_deletes is TRUE in production. So the pipeline can
    run for as long as it takes to build confidence with `queue=False`, identifying every
    affected row without any of them becoming deletable.

    `queue=False` deliberately leaves current_status ALONE. A row that is `listed` stays
    `listed` - it describes where the product is, and this function has no basis to change
    that. Only the skip_reason and the detail are written.

    Separate from apply_scan_statuses because this writes last_error and must NOT touch
    variant_count - these rows have no scanned product to take a count from, and passing a
    stale one back would overwrite a real value with a guess.

    Deliberately an UPDATE, never an upsert. Every target already exists; an INSERT path
    could conjure a pending_delisting row for a parent that has no state at all.
    """
    if not items:
        return 0
    conn = connections.get("default")
    written = 0
    rows = list(items.items())
    status_set = "  current_status = 'pending_delisting', " if queue else ""
    for i in range(0, len(rows), 200):
        chunk = rows[i:i + 200]
        values, params = [], []
        for n, (parent_sku, detail) in enumerate(chunk):
            base = n * 2
            values.append(f"(${base + 3}::text, ${base + 4}::text)")
            params.extend([parent_sku, (detail or "")[:2000]])
        await conn.execute_query(
            "UPDATE internal_platform_state AS s SET "
            + status_set +
            "  skip_reason    = $2, "
            "  last_error     = v.detail, "
            "  updated_at     = CURRENT_TIMESTAMP "
            f"FROM (VALUES {', '.join(values)}) AS v(parent_sku, detail) "
            "WHERE s.internal_platform_id = $1 AND s.parent_sku = v.parent_sku",
            [platform_id, InternalPlatformSkipReason.REASSIGNED_SKU.value, *params],
        )
        written += len(chunk)
    return written


async def reconcile_stock(platform_id: str,
                          rows: Mapping[str, tuple[int, list[dict]]]) -> int:
    """Correct stored stock against what a COMPLETE scan just saw on Shopify.

    `rows` is parent_sku -> (row_inventory, variants), already diffed by the caller so only
    genuine changes arrive.

    This is the ONLY writer that may act on a SKU's absence from Shopify, and it is
    deliberately narrow: stock columns only. It must never touch current_status,
    skip_reason or dest_product_gid. A row whose product vanished is a row with no stock,
    which is a fact; whether it should be delisted is a judgement, and keeping the two
    apart is what stops a truncated scan cascading into the delete queue.

    Why this exists at all: the scan writes by RESOLVED parent, so once a merge repoints a
    SKU the old row stops being a possible write target and freezes. Measured 2026-08-03,
    every one of the 9 units the state table claimed on reassigned SKUs was a frozen
    merge-day figure; live Shopify held 0 for all of them.
    """
    if not rows:
        return 0
    conn = connections.get("default")
    written = 0
    items = list(rows.items())
    for i in range(0, len(items), 200):
        chunk = items[i:i + 200]
        values, params = [], []
        for n, (parent_sku, (inventory, variants)) in enumerate(chunk):
            base = n * 3
            values.append(
                f"(${base + 2}::text, ${base + 3}::int, ${base + 4}::jsonb)")
            params.extend([parent_sku, inventory, json.dumps(variants)])
        await conn.execute_query(
            "UPDATE internal_platform_state AS s SET "
            "  inventory  = v.inventory, "
            "  variants   = v.variants, "
            "  updated_at = CURRENT_TIMESTAMP "
            f"FROM (VALUES {', '.join(values)}) AS v(parent_sku, inventory, variants) "
            "WHERE s.internal_platform_id = $1 AND s.parent_sku = v.parent_sku",
            [platform_id, *params],
        )
        written += len(chunk)
    return written


async def footprint(platform_id: str) -> int:
    """Distinct destination products we own. The denominator for percentage caps.

    Deliberately not the store catalog: 5% of the destination's 11,745 products is 587,
    which would likely be 60% of our actual footprint and therefore no cap at all.
    """
    rows = await InternalPlatformState.filter(
        internal_platform_id=platform_id, dest_product_gid__isnull=False
    ).values_list("dest_product_gid", flat=True)
    return len({r for r in rows if r})


async def cycle_counts(platform_id: str, since_minutes: int = 60) -> dict[str, int]:
    """Per-action outcome counts for the last cycle. Feeds the run report."""
    cutoff = _now() - timedelta(minutes=since_minutes)
    rows = await InternalPlatformSubmission.filter(
        internal_platform_id=platform_id, created_at__gte=cutoff
    ).values_list("action", "status")
    counts: dict[str, int] = {}
    for action, status in rows:
        counts[f"{action}.{status}"] = counts.get(f"{action}.{status}", 0) + 1
    return counts
