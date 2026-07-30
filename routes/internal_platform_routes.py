"""Endpoints for the internal-platform consignment pipeline.

Backs the Internal Platforms page: pipeline health, per-product state, and the action
ledger for the 1nventory -> Syncio -> Shop The Sample flow.

Every observability route is a GET and none of them mutate anything. Two POSTs act:

  /submit  tags qualifying candidates on the source. Gated by `execute`, by the Syncio
           capacity ceiling, and by the submit cooldown - the same gates the scheduled
           pass obeys, so the button cannot out-run delivery.

  /delist  untags on the source and DELETES the destination product. Irreversible.
           Gated by `execute_deletes`, and acts only on rows already soaked into
           `pending_delisting`, so nothing is deleted that has not been reviewable on the
           Products tab first.

Both refuse with a 409 before doing any work when their switch is off, rather than
running and reporting that they wrote nothing.

Conventions follow routes/submissions_routes.py: query parameters only (no path params),
no auth dependency (AuthMiddleware covers everything outside /api), guard-clause
HTTPExceptions, and heavy aggregates via raw SQL rather than Python-side loops.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from tortoise import connections

from models.api_models import (
    InternalPlatformActivityResponse,
    InternalPlatformDelistResponse,
    InternalPlatformSubmitResponse,
    InternalPlatformOverviewResponse,
    InternalPlatformPollerStatus,
    InternalPlatformProductDetailResponse,
    InternalPlatformProductsResponse,
    InternalPlatformStateRow,
    InternalPlatformStoreStatus,
    InternalPlatformSubmissionDetail,
    InternalPlatformSubmissionRow,
)
from models.db_models import (
    InternalPlatform,
    InternalPlatformState,
    InternalPlatformSubmission,
)
from services import internal_platform_ledger as ledger
from services.internal_platform_dest_poller import internal_platform_dest_poller
from services.internal_platform_source_poller import internal_platform_source_poller
from services.internal_platform_rules import check_submit_cooldown, check_syncio_capacity
from services.shopify_client import (
    ShopifyError,
    get_shopify_client,
    writes_allowed_by_config,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/internal_platforms", tags=["internal_platforms"])

DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200
DEFAULT_PLATFORM = "shopthesample"

# A filter value, not a stored status. current_status has no code for "delivered yet?"
# because that fact lives in dest_product_gid, so the distinction cannot be expressed by
# filtering current_status alone. Kept out of the InternalPlatformStatus vocabulary
# deliberately: nothing ever writes it, and a status the database can hold but the
# pipeline never sets is worse than an explicit filter-only value.
AWAITING_SYNCIO = "awaiting_syncio"

# What each store needs for the pipeline to function end to end. Surfaced on the
# overview so the page can explain exactly why nothing is running.
SOURCE_REQUIRED_SCOPES = ("read_products", "write_products")
DEST_REQUIRED_SCOPES = ("read_products", "write_products", "read_locations", "write_inventory")


async def _get_platform(platform_id: str) -> InternalPlatform:
    """Load the platform row, or 404.

    HTTPException detail is surfaced to the user as a snackbar by the UI's axios
    interceptor, so it stays short and non-technical. Anything a developer needs -
    "has the migration run?" - is logged instead.
    """
    platform = await ledger.get_platform(platform_id)
    if platform is None:
        logger.warning(
            "internal_platforms: platform %r not found in internal_platforms; "
            "has add_internal_platforms.sql run on this database?",
            platform_id,
        )
        raise HTTPException(status_code=404, detail=f"Platform {platform_id} not found")
    return platform


def _state_row(row: InternalPlatformState) -> InternalPlatformStateRow:
    # Statuses are stored in operator-facing form (pending_normalization / listed),
    # so no display-time remapping is needed. See
    # migrations/rename_internal_platform_statuses.sql.
    return InternalPlatformStateRow(
        parent_sku=row.parent_sku,
        title=row.title,
        image_url=row.image_url,
        product_type=row.product_type,
        inventory=row.inventory,
        source_price=row.source_price,
        source_compare_at=row.source_compare_at,
        sts_price=row.sts_price,
        variant_count=row.variant_count,
        variants=row.variants or [],
        current_status=row.current_status,
        source_product_gid=row.source_product_gid,
        dest_product_gid=row.dest_product_gid,
        inflight_action=row.inflight_action,
        skip_reason=row.skip_reason,
        last_error=row.last_error,
        delist_strikes=row.delist_strikes,
        listed_at=row.listed_at,
        normalize_done_at=row.normalize_done_at,
        location_done_at=row.location_done_at,
        delisted_at=row.delisted_at,
        updated_at=row.updated_at,
    )


def _submission_row(row: InternalPlatformSubmission) -> InternalPlatformSubmissionRow:
    return InternalPlatformSubmissionRow(
        id=row.id,
        parent_sku=row.parent_sku,
        action=row.action,
        status=row.status,
        skip_reason=row.skip_reason,
        source_product_gid=row.source_product_gid,
        dest_product_gid=row.dest_product_gid,
        error=row.error,
        actor=row.actor,
        triggered_by=row.triggered_by,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


async def _store_status(store_key: str, role: str,
                        required: tuple[str, ...]) -> InternalPlatformStoreStatus:
    """Live scope check. Read-only: asks Shopify what the app was granted."""
    if not store_key:
        return InternalPlatformStoreStatus(
            store_key="", role=role, required_scopes=list(required),
            missing_scopes=list(required), reachable=False,
            error="No store configured on the platform row",
        )
    try:
        client = await get_shopify_client(store_key)
        granted = list(await client.granted_scopes())
    except ShopifyError as exc:
        return InternalPlatformStoreStatus(
            store_key=store_key, role=role, required_scopes=list(required),
            missing_scopes=list(required), reachable=False, error=str(exc),
        )
    except Exception as exc:  # config missing, network down
        logger.warning("internal_platforms: store %s unreachable: %s", store_key, exc)
        return InternalPlatformStoreStatus(
            store_key=store_key, role=role, required_scopes=list(required),
            missing_scopes=list(required), reachable=False, error=str(exc),
        )

    # write_x implies read_x on Shopify, so treat it as satisfying the read.
    effective = set(granted)
    for scope in granted:
        if scope.startswith("write_"):
            effective.add("read_" + scope[len("write_"):])
    missing = [s for s in required if s not in effective]
    return InternalPlatformStoreStatus(
        store_key=store_key, role=role, granted_scopes=granted,
        required_scopes=list(required), missing_scopes=missing, reachable=True,
    )


@router.get("/overview", response_model=InternalPlatformOverviewResponse)
async def get_overview(
    platform: str = Query(DEFAULT_PLATFORM, description="Internal platform identifier"),
    window_minutes: int = Query(1440, ge=1, le=43200, description="Recent-activity window"),
):
    """Pipeline health plus the setup status that explains an idle pipeline."""
    try:
        row = await _get_platform(platform)
        conn = connections.get("default")

        # Every item below is independent, and the database is remote - each round trip
        # costs ~0.6s. Run sequentially this endpoint took 6.4s, which the UI then repeats
        # on a 30s poll. Gathered, it costs about as much as its slowest single query.
        (
            status_rows,
            skip_rows,
            counts_rows,
            orphan_rows,
            recent,
            source_status,
            dest_status,
        ) = await asyncio.gather(
            conn.execute_query_dict(
                "SELECT current_status, count(*) AS n FROM internal_platform_state "
                "WHERE internal_platform_id = $1 GROUP BY current_status",
                [platform],
            ),
            conn.execute_query_dict(
                "SELECT skip_reason, count(*) AS n FROM internal_platform_state "
                "WHERE internal_platform_id = $1 AND skip_reason IS NOT NULL "
                "GROUP BY skip_reason ORDER BY n DESC",
                [platform],
            ),
            # Five scalars over internal_platform_state in ONE statement. They were five
            # separate round trips at ~0.57s each against a remote database, for five
            # integers derived from the same rows. FILTER aggregates give the same answers
            # for the cost of one scan. The equivalent ledger helpers are kept for callers
            # that need the actual rows; this endpoint only ever took len() of them.
            conn.execute_query_dict(
                "SELECT count(*) AS tracked, "
                "  count(*) FILTER (WHERE listed_at IS NOT NULL "
                "                   AND dest_product_gid IS NULL) AS awaiting, "
                "  count(DISTINCT dest_product_gid) "
                "    FILTER (WHERE dest_product_gid IS NOT NULL) AS live, "
                "  COALESCE(sum(variant_count) FILTER (WHERE listed_at IS NOT NULL "
                "                   AND dest_product_gid IS NULL), 0) AS in_flight, "
                "  count(*) FILTER (WHERE listed_at IS NOT NULL "
                "                   AND dest_product_gid IS NULL "
                "                   AND listed_at < $2) AS stale "
                "FROM internal_platform_state WHERE internal_platform_id = $1",
                [platform, datetime.now(timezone.utc)
                 - timedelta(days=ledger.AWAITING_SYNC_ALERT_DAYS)],
            ),
            # Orphan count and last-submit timestamp in ONE pass over the same table.
            # The cooldown needs the timestamp on every overview render, and a second
            # round trip here would undo part of what the gather won.
            conn.execute_query_dict(
                "SELECT count(*) FILTER (WHERE action = 'delete' "
                "                        AND status = 'failed') AS orphaned, "
                "       max(created_at) FILTER (WHERE action = 'list' "
                "                        AND status = 'success') AS last_submit_at "
                "FROM internal_platform_submissions WHERE internal_platform_id = $1",
                [platform],
            ),
            ledger.cycle_counts(platform, since_minutes=window_minutes),
            _store_status(row.source_store, "source", SOURCE_REQUIRED_SCOPES),
            _store_status(row.dest_store, "destination", DEST_REQUIRED_SCOPES),
        )

        status_counts = {r["current_status"]: r["n"] for r in status_rows}
        skip_counts = {r["skip_reason"]: r["n"] for r in skip_rows}
        c = counts_rows[0] if counts_rows else {}
        tracked = c.get("tracked", 0)
        awaiting = c.get("awaiting", 0)
        live = c.get("live", 0)
        in_flight = int(c.get("in_flight", 0) or 0)
        stale = c.get("stale", 0)
        orphaned = orphan_rows[0]["orphaned"] if orphan_rows else 0
        last_submit = orphan_rows[0]["last_submit_at"] if orphan_rows else None

        # Submit-gate state, so the button can render its true state before being pressed.
        # Two independent gates: the ceiling limits one batch's size, the cooldown limits
        # how soon the next batch may follow.
        capacity = check_syncio_capacity(
            in_flight, internal_platform_source_poller.max_products_in_flight
        )
        cooldown = check_submit_cooldown(
            in_flight, last_submit, datetime.now(timezone.utc),
            internal_platform_source_poller.submit_cooldown_hours,
        )
        submit_blocked = capacity.blocked or not cooldown.allowed
        gate_message = capacity.message if capacity.blocked else (
            cooldown.message if not cooldown.allowed else capacity.message
        )

        pollers = [
            InternalPlatformPollerStatus(
                name="Destination reconciler",
                enabled=internal_platform_dest_poller.enabled,
                execute=internal_platform_dest_poller.execute,
                cadence=f"every {internal_platform_dest_poller.interval}s",
            ),
            InternalPlatformPollerStatus(
                name="Source reconciler",
                enabled=internal_platform_source_poller.enabled,
                execute=internal_platform_source_poller.execute,
                execute_deletes=internal_platform_source_poller.execute_deletes,
                cadence=(
                    f"scan every {internal_platform_source_poller.interval_seconds}s, "
                    f"delist daily {internal_platform_source_poller.daily_hour:02d}:"
                    f"{internal_platform_source_poller.daily_minute:02d} "
                    f"{internal_platform_source_poller.timezone}"
                ),
            ),
        ]

        blockers: list[str] = []
        if not row.enabled:
            blockers.append(f"Platform '{platform}' is disabled")
        for store in (source_status, dest_status):
            if not store.reachable:
                blockers.append(f"{store.role.title()} store unreachable: {store.error}")
            elif store.missing_scopes:
                blockers.append(
                    f"{store.role.title()} store missing scope(s): "
                    f"{', '.join(store.missing_scopes)}"
                )
        for p in pollers:
            if not p.enabled:
                blockers.append(f"{p.name} is disabled")
            elif not p.execute:
                blockers.append(f"{p.name} is in dry-run (execute=false)")

        return InternalPlatformOverviewResponse(
            platform_id=row.id,
            name=row.name,
            source_store=row.source_store,
            dest_store=row.dest_store,
            trigger_tag=row.trigger_tag,
            platform_enabled=row.enabled,
            tracked=tracked,
            live=live,
            awaiting_sync=awaiting,
            stale_awaiting_sync=stale,
            failed=status_counts.get("failed", 0),
            flagged=status_counts.get("skipped", 0),
            orphaned_delists=orphaned,
            status_counts=status_counts,
            skip_reason_counts=skip_counts,
            recent_activity=recent,
            activity_window_minutes=window_minutes,
            products_in_flight=in_flight,
            max_products_in_flight=internal_platform_source_poller.max_products_in_flight,
            submit_blocked=submit_blocked,
            submit_gate_message=gate_message,
            auto_submit=internal_platform_source_poller.auto_submit,
            can_submit_for_real=internal_platform_source_poller.execute,
            ready_for_listing=status_counts.get("ready_for_listing", 0),
            pending_delisting=status_counts.get("pending_delisting", 0),
            can_delist_for_real=internal_platform_source_poller.execute_deletes,
            auto_delist=internal_platform_source_poller.auto_delist,
            writes_allowed=writes_allowed_by_config(),
            source_poller_enabled=internal_platform_source_poller.enabled,
            source_poller_execute=internal_platform_source_poller.execute,
            dest_poller_enabled=internal_platform_dest_poller.enabled,
            dest_poller_execute=internal_platform_dest_poller.execute,
            stores=[source_status, dest_status],
            pollers=pollers,
            blockers=blockers,
        )
    except HTTPException:
        raise
    except Exception:
        logger.exception("internal_platforms: failed to build overview")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/products", response_model=InternalPlatformProductsResponse)
async def get_products(
    platform: str = Query(DEFAULT_PLATFORM, description="Internal platform identifier"),
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    status: str | None = Query(
        None,
        description="Filter by current_status, or the synthetic value 'awaiting_syncio' "
        "for the undelivered half of pending_normalization",
    ),
    skip_reason: str | None = Query(None, description="Filter by skip_reason"),
    q: str | None = Query(None, description="Parent SKU contains (case-insensitive)"),
    exclude_skipped: bool = Query(
        False,
        description="Omit rows the automation declined to act on. Applied in SQL so "
        "total and pagination stay correct.",
    ),
):
    """One row per tracked parent SKU, with its current pipeline state."""
    try:
        await _get_platform(platform)
        qs = InternalPlatformState.filter(internal_platform_id=platform)
        if exclude_skipped:
            qs = qs.exclude(current_status="skipped")
        # `pending_normalization` spans two stages that look nothing alike to an
        # operator: tagged on source and waiting days for Syncio to deliver, versus
        # delivered and waiting minutes for the next destination cycle. Only
        # dest_product_gid tells them apart. AWAITING_SYNCIO is the undelivered half, and
        # it uses the SAME predicate as the Overview tile of that name - previously the
        # tile counted undelivered rows while the filter of the identical name returned
        # both halves, so the two disagreed about what "Awaiting Syncio" meant.
        if status == AWAITING_SYNCIO:
            qs = qs.filter(
                current_status="pending_normalization",
                dest_product_gid__isnull=True,
                listed_at__not_isnull=True,
            )
        elif status:
            qs = qs.filter(current_status=status)
        if skip_reason:
            qs = qs.filter(skip_reason=skip_reason)
        if q:
            qs = qs.filter(parent_sku__icontains=q.strip())

        total = await qs.count()
        # Sorted by product name, NOT by updated_at. The scan touches updated_at whenever
        # it writes a row, so a recency sort reshuffled the table under the reader every
        # time the 30s poll landed - rows moved between pages mid-read. parent_sku is the
        # tiebreaker because title is not unique, and without a deterministic second key
        # the same row can appear on two pages or on neither.
        rows = await (
            qs.order_by("title", "parent_sku")
            .offset((page - 1) * page_size)
            .limit(page_size)
        )

        return InternalPlatformProductsResponse(
            platform_id=platform,
            items=[_state_row(r) for r in rows],
            total=total,
            page=page,
            page_size=page_size,
        )
    except HTTPException:
        raise
    except Exception:
        logger.exception("internal_platforms: failed to load products")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/activity", response_model=InternalPlatformActivityResponse)
async def get_activity(
    platform: str = Query(DEFAULT_PLATFORM, description="Internal platform identifier"),
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    action: str | None = Query(None, description="Filter by action"),
    status: str | None = Query(None, description="Filter by status"),
    skip_reason: str | None = Query(None, description="Filter by skip_reason"),
    parent_sku: str | None = Query(None, description="Exact parent SKU"),
    since_hours: int | None = Query(None, ge=1, le=8760, description="Only the last N hours"),
):
    """The action ledger, newest first."""
    try:
        await _get_platform(platform)
        qs = InternalPlatformSubmission.filter(internal_platform_id=platform)
        if action:
            qs = qs.filter(action=action)
        if status:
            qs = qs.filter(status=status)
        if skip_reason:
            qs = qs.filter(skip_reason=skip_reason)
        if parent_sku:
            qs = qs.filter(parent_sku=parent_sku.strip())
        if since_hours:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
            qs = qs.filter(created_at__gte=cutoff)

        total = await qs.count()
        rows = await qs.order_by("-created_at").offset((page - 1) * page_size).limit(page_size)

        return InternalPlatformActivityResponse(
            platform_id=platform,
            items=[_submission_row(r) for r in rows],
            total=total,
            page=page,
            page_size=page_size,
        )
    except HTTPException:
        raise
    except Exception:
        logger.exception("internal_platforms: failed to load activity")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/product_detail", response_model=InternalPlatformProductDetailResponse)
async def get_product_detail(
    platform: str = Query(DEFAULT_PLATFORM, description="Internal platform identifier"),
    parent_sku: str = Query(..., description="Parent SKU to drill into"),
):
    """Current state plus the full action timeline for one parent SKU."""
    try:
        await _get_platform(platform)
        sku = parent_sku.strip()

        state = await InternalPlatformState.get_or_none(
            internal_platform_id=platform, parent_sku=sku
        )
        rows = await InternalPlatformSubmission.filter(
            internal_platform_id=platform, parent_sku=sku
        ).order_by("-created_at")

        if state is None and not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No record for {sku}",
            )

        status_counts: dict[str, int] = {}
        timeline: list[InternalPlatformSubmissionDetail] = []
        for r in rows:
            status_counts[r.status] = status_counts.get(r.status, 0) + 1
            base = _submission_row(r)
            timeline.append(
                InternalPlatformSubmissionDetail(
                    **base.model_dump(), payload=r.payload, result=r.result
                )
            )

        return InternalPlatformProductDetailResponse(
            platform_id=platform,
            parent_sku=sku,
            state=_state_row(state) if state else None,
            timeline=timeline,
            status_counts=status_counts,
        )
    except HTTPException:
        raise
    except Exception:
        logger.exception("internal_platforms: failed to load product detail")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/submit", response_model=InternalPlatformSubmitResponse)
async def submit(
    platform: str = Query(DEFAULT_PLATFORM, description="Internal platform identifier"),
):
    """Run the source pass on demand: tag qualifying candidates on the source store.

    Not a separate pipeline - the same pass the schedule runs, with the schedule bypassed
    (mirrors spo_poller.manual_flush). It still honours the Syncio capacity gate, and it
    refuses outright when `execute` is off rather than reporting a dry run: a button that
    silently does nothing is worse than one that says why.

    Runs synchronously: the source scan is a single paginated read and the caller wants
    the resulting counts. Unlike the grailed flush there is no slow third-party call to
    hide behind a background task.
    """
    try:
        await _get_platform(platform)

        if internal_platform_source_poller.platform_id != platform:
            logger.warning(
                "internal_platforms: submit requested for %r but the source poller is "
                "configured for %r; check internal_platform_source_poller.platform_id",
                platform, internal_platform_source_poller.platform_id,
            )
            raise HTTPException(status_code=400, detail="Platform not configured")

        # Fail before the scan, not after: a 60s sweep that ends in "wrote nothing" wastes
        # a minute and a full cost bucket to tell the caller what config already knows.
        if not internal_platform_source_poller.execute:
            logger.warning(
                "internal_platforms: submit refused, execute=false. Set execute = true "
                "under [internal_platform_source_poller] in config.toml and restart the "
                "API to enable source-store writes."
            )
            raise HTTPException(status_code=409, detail="Submissions are turned off")

        report = await internal_platform_source_poller.manual_submit()

        in_flight = await ledger.products_in_flight(platform)
        return InternalPlatformSubmitResponse(
            platform_id=platform,
            submitted=report.tagged,
            variants_submitted=report.variants_submitted,
            held_back=report.held_back,
            blocked=bool(report.aborted),
            gate_message=report.gate_message,
            products_in_flight=in_flight,
            max_products_in_flight=internal_platform_source_poller.max_products_in_flight,
        )
    except HTTPException:
        raise
    except Exception:
        logger.exception("internal_platforms: manual submit failed")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/delist", response_model=InternalPlatformDelistResponse)
async def delist(
    platform: str = Query(DEFAULT_PLATFORM, description="Internal platform identifier"),
):
    """Execute every queued delist: untag on the source, then delete on the destination.

    IRREVERSIBLE. Shopify has no undelete, so each product goes through the full guarded
    flow in _apply_delists - untag, re-read to confirm the tag is gone, nine ownership
    checks, and a pre-image committed to the ledger BEFORE the mutation. That ordering is
    mandatory: deleting while the source is still tagged makes Syncio recreate the product.

    Acts only on rows already in `pending_delisting`, so the set has soaked and has been
    visible on the Products tab. There is deliberately no numeric cap - the review step is
    the control, not arithmetic.
    """
    try:
        await _get_platform(platform)

        if internal_platform_source_poller.platform_id != platform:
            logger.warning(
                "internal_platforms: delist requested for %r but the source poller is "
                "configured for %r; check internal_platform_source_poller.platform_id",
                platform, internal_platform_source_poller.platform_id,
            )
            raise HTTPException(status_code=400, detail="Platform not configured")

        # Refuse before touching Shopify. execute_deletes is the master switch for the
        # irreversible half and is separate from `execute` precisely so tagging can be on
        # while deleting is not.
        if not internal_platform_source_poller.execute_deletes:
            logger.warning(
                "internal_platforms: delist refused, execute_deletes=false. Set "
                "execute_deletes = true under [internal_platform_source_poller] in "
                "config.toml and restart the API to permit destination deletes."
            )
            raise HTTPException(status_code=409, detail="Delisting is turned off")

        report = await internal_platform_source_poller.manual_delist()
        still_pending = len(await ledger.pending_delists(platform))

        return InternalPlatformDelistResponse(
            platform_id=platform,
            untagged=report.untagged,
            deleted=report.deleted,
            failed=report.failed,
            still_pending=still_pending,
            blocked=bool(report.aborted),
            gate_message=report.gate_message,
        )
    except HTTPException:
        raise
    except Exception:
        logger.exception("internal_platforms: manual delist failed")
        raise HTTPException(status_code=500, detail="Internal server error")
