import asyncio
import io
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Optional

import openpyxl
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from models.api_models import (
    CreateBatchResponse,
    ImportDetailResponse,
    ImportListingDetail,
    ImportSummary,
    SubmissionsDashboardResponse,
)
from models.db_models import (
    AppSettings,
    ListingSubmission,
    SubmissionStatus,
)
from services import spo_service as spo_service_module
from services.ebay_poller import ebay_poller
from services.grailed_poller import grailed_poller
from services.spo_poller import spo_poller
from services.spo_service import spo_service
from services.template_service import TemplateService
from tortoise import connections
from utils.submission_steps import record_step

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/submissions", tags=["submissions"])


DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200


def _effective_status(sub: ListingSubmission) -> str:
    """A failed submission that has been manually reviewed counts as success.

    Legacy rows only: mark_import_reviewed now writes SUCCESS to the row itself,
    so reviewing resolves the failure everywhere rather than just on this screen.
    Kept for rows reviewed before that change, which are still stored as failed.
    """
    if sub.status == SubmissionStatus.FAILED and sub.reviewed_at is not None:
        return SubmissionStatus.SUCCESS
    return sub.status


def _effective_import_status(status_counts: dict[str, int] | None) -> str:
    """Roll a per-status breakdown up to a single dominant status for the import.

    In-progress beats finished: any processing → processing; otherwise any pending
    → pending; otherwise any failed → failed; otherwise success.
    """
    counts = status_counts or {}
    if counts.get(SubmissionStatus.PROCESSING, 0) > 0:
        return SubmissionStatus.PROCESSING
    if counts.get(SubmissionStatus.PENDING, 0) > 0:
        return SubmissionStatus.PENDING
    if counts.get(SubmissionStatus.FAILED, 0) > 0:
        return SubmissionStatus.FAILED
    return SubmissionStatus.SUCCESS


async def _get_platform_settings_for(platform_id: str) -> dict[str, Any]:
    settings = await AppSettings.first()
    if not settings:
        raise HTTPException(status_code=404, detail="App settings not initialized")
    platform_settings = (settings.platform_settings or {}).get(platform_id)
    if not platform_settings or not platform_settings.get("manual_fallback"):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Platform '{platform_id}' is not configured for manual fallback. "
                "Enable platform_settings.{platform_id}.manual_fallback first."
            ),
        )
    return platform_settings


async def _pending_counts_by_platform() -> dict[str, int]:
    """Pending submission count per platform_id, for the dashboard tab badges.

    Pending is unaffected by reviewed_at (that only flips failed->success), so a
    plain status='pending' count is correct. One grouped query keeps this cheap.
    """
    conn = connections.get("default")
    rows = await conn.execute_query_dict(
        "SELECT platform_id, count(*) AS n FROM listing_submissions "
        "WHERE status = $1 GROUP BY platform_id",
        [SubmissionStatus.PENDING],
    )
    return {row["platform_id"]: row["n"] for row in rows}


async def _sku_counts_by_import(platform_id: str) -> dict[int, int]:
    """Total SKU/product rows per import for a platform.

    A listing's SKUs are the keys of data.child_size_overrides; the import's SKU
    total is the sum across its listings, which equals the row count written to
    the SPO product file. One grouped SQL query keeps this off the Python path.
    """
    conn = connections.get("default")
    rows = await conn.execute_query_dict(
        "SELECT (ls.platform_meta->>'product_import_id')::int AS import_id, "
        "COALESCE(SUM(k.n), 0) AS n "
        "FROM listing_submissions ls JOIN listings l ON l.id = ls.listing_id "
        "CROSS JOIN LATERAL (SELECT count(*) AS n FROM jsonb_object_keys("
        "  CASE WHEN jsonb_typeof(l.data->'child_size_overrides') = 'object' "
        "       THEN l.data->'child_size_overrides' ELSE '{}'::jsonb END)) k "
        "WHERE ls.platform_id = $1 AND ls.platform_meta ? 'product_import_id' "
        "GROUP BY 1",
        [platform_id],
    )
    return {row["import_id"]: row["n"] for row in rows}


async def _aggregate_platform(platform_id: str) -> dict[str, Any]:
    submissions = await ListingSubmission.filter(platform_id=platform_id).all()
    sku_counts = await _sku_counts_by_import(platform_id)

    pending = processing = failed = success = 0
    grouped: dict[int, list[ListingSubmission]] = defaultdict(list)

    # The top-line counts are deduped to the LATEST attempt per parent listing so
    # a parent that failed then was retried to success (or failed N times) counts
    # once, by its current state - not once per historical attempt row. Orphaned
    # rows (listing since deleted) have no parent to act on and are excluded from
    # the counts. The per-import grouping below still uses every row.
    latest_per_listing: dict[Any, ListingSubmission] = {}
    for sub in submissions:
        if sub.listing_id is not None:
            current = latest_per_listing.get(sub.listing_id)
            if current is None or sub.attempt_number > current.attempt_number:
                latest_per_listing[sub.listing_id] = sub

        meta = sub.platform_meta or {}
        import_id = meta.get("product_import_id")
        if isinstance(import_id, int):
            grouped[import_id].append(sub)

    for sub in latest_per_listing.values():
        eff = _effective_status(sub)
        if eff == SubmissionStatus.PENDING:
            pending += 1
        elif eff == SubmissionStatus.PROCESSING:
            processing += 1
        elif eff == SubmissionStatus.FAILED:
            failed += 1
        elif eff == SubmissionStatus.SUCCESS:
            success += 1

    imports: list[ImportSummary] = []
    for import_id, group in grouped.items():
        status_counts: dict[str, int] = defaultdict(int)
        for sub in group:
            status_counts[_effective_status(sub)] += 1

        # Prefer the actual upload time recorded by the sweep (platform_meta.uploaded_at).
        # Imports that predate that field fall back to the oldest submission's
        # created_at, which can be days earlier than the real upload.
        uploaded_ats = []
        stored_file_name = None
        batch_number = None
        for s in group:
            meta = s.platform_meta or {}
            raw = meta.get("uploaded_at")
            if raw:
                try:
                    uploaded_ats.append(datetime.fromisoformat(raw))
                except (TypeError, ValueError):
                    pass
            if not stored_file_name and meta.get("file_name"):
                stored_file_name = meta["file_name"]
            if batch_number is None and meta.get("batch_number") is not None:
                batch_number = meta["batch_number"]

        created_at = (
            min(uploaded_ats)
            if uploaded_ats
            else min((s.created_at for s in group), default=None)
        )

        # Prefer the persisted file name; older SPO imports predate that field, so
        # reconstruct the same spo_products_<timestamp>.xlsx name from created_at.
        # Only SPO has an uploaded file - grailed batches leave this null.
        file_name = stored_file_name
        if not file_name and platform_id == "spo" and created_at is not None:
            file_name = f"spo_products_{created_at.strftime('%Y%m%d_%H%M%S')}.xlsx"

        imports.append(
            ImportSummary(
                import_id=import_id,
                platform_id=platform_id,
                submission_count=len(group),
                sku_count=sku_counts.get(import_id, 0),
                file_name=file_name,
                batch_number=batch_number,
                status_counts=dict(status_counts),
                created_at=created_at,
                updated_at=max((s.updated_at for s in group), default=None),
            )
        )

    return {
        "pending": pending,
        "processing": processing,
        "failed": failed,
        "success": success,
        "imports": imports,
    }


@router.get("/dashboard", response_model=SubmissionsDashboardResponse)
async def get_dashboard(
    platform: str = Query(..., description="Platform identifier, e.g. 'spo' or 'all'"),
    page: int = Query(1, ge=1, description="1-indexed page number"),
    page_size: int = Query(
        DEFAULT_PAGE_SIZE,
        ge=1,
        le=MAX_PAGE_SIZE,
        description="Imports per page",
    ),
    status: str | None = Query(
        None,
        description="Optional status filter; only imports with at least one submission in this status are returned",
    ),
):
    try:
        if platform == "all":
            settings = await AppSettings.first()
            if not settings:
                raise HTTPException(status_code=404, detail="App settings not initialized")
            platform_ids = [
                pid
                for pid, cfg in (settings.platform_settings or {}).items()
                if isinstance(cfg, dict) and cfg.get("manual_fallback")
            ]
            min_batch_size = 0
        else:
            platform_settings = await _get_platform_settings_for(platform)
            platform_ids = [platform]
            min_batch_size = int(platform_settings.get("min_batch_size", 200) or 200)

        pending = processing = failed = success = 0
        all_imports: list[ImportSummary] = []
        for pid in platform_ids:
            data = await _aggregate_platform(pid)
            pending += data["pending"]
            processing += data["processing"]
            failed += data["failed"]
            success += data["success"]
            all_imports.extend(data["imports"])

        if status:
            all_imports = [
                imp
                for imp in all_imports
                if _effective_import_status(imp.status_counts) == status
            ]

        all_imports.sort(key=lambda i: i.updated_at or i.created_at, reverse=True)
        total_imports = len(all_imports)
        start = (page - 1) * page_size
        end = start + page_size
        page_slice = all_imports[start:end]

        return SubmissionsDashboardResponse(
            platform_id=platform,
            pending_count=pending,
            processing_count=processing,
            failed_count=failed,
            success_count=success,
            min_batch_size=min_batch_size,
            imports=page_slice,
            total_imports=total_imports,
            page=page,
            page_size=page_size,
            platform_pending_counts=await _pending_counts_by_platform(),
        )
    except HTTPException:
        raise
    except Exception:
        logger.exception("Error building submissions dashboard")
        raise HTTPException(status_code=500, detail="Failed to load dashboard")


@router.post("/create_batch", response_model=CreateBatchResponse)
async def create_batch(platform: str = Query(..., description="Platform identifier, e.g. 'spo'")):
    await _get_platform_settings_for(platform)

    if platform == "spo":
        # SPO uploads an XLSX and returns an import id quickly; the long-running
        # import is polled afterwards, so this can stay synchronous.
        try:
            result = await spo_poller.manual_flush()
        except Exception:
            logger.exception("Manual spo flush failed")
            raise HTTPException(status_code=500, detail="Manual spo flush failed")
        return CreateBatchResponse(
            platform=platform,
            submission_count=result.get("submission_count", 0),
            product_import_id=result.get("product_import_id"),
        )

    if platform == "grailed":
        # Grailed's AppScript call is synchronous and can take a minute for a large
        # batch, so run the flush in the background and return immediately. The rows
        # flip to processing then success/failed; the dashboard polls for the result.
        pending = await ListingSubmission.filter(
            platform_id="grailed", status=SubmissionStatus.PENDING
        ).count()

        async def _run_grailed_flush():
            try:
                await grailed_poller.manual_flush()
            except Exception:
                logger.exception("Background grailed flush failed")

        asyncio.create_task(_run_grailed_flush())
        return CreateBatchResponse(
            platform=platform, submission_count=pending, product_import_id=None
        )

    if platform == "ebay":
        # Posts the specifics file synchronously, like SPO. ImportEbaySpecifics answers
        # with a QueuedJobResponse, so its ID is a real SellerCloud queued job: it becomes
        # the import id the dashboard keys on, and get_job_status can follow it.
        try:
            result = await ebay_poller.manual_flush()
        except Exception as exc:
            logger.exception("eBay specifics flush failed")
            # The snackbar shows `detail` verbatim, so surface SellerCloud's own message
            # rather than a generic failure the operator cannot act on.
            body = getattr(getattr(exc, "response", None), "text", "") or ""
            raise HTTPException(
                status_code=500,
                detail=f"eBay import failed: {body[:200]}" if body else "eBay specifics flush failed",
            )

        if result.get("sent") and not result.get("ok"):
            raise HTTPException(
                status_code=502,
                detail=f"SellerCloud rejected the import: {str(result.get('response'))[:200]}",
            )

        return CreateBatchResponse(
            platform=platform,
            submission_count=result.get("submission_count", 0),
            product_import_id=result.get("job_id"),
            rows=result.get("rows"),
            blocked={str(k): v for k, v in (result.get("blocked") or {}).items()} or None,
        )

    raise HTTPException(
        status_code=404,
        detail=f"Manual batch creation is not implemented for platform '{platform}'",
    )


async def _load_import_submissions(
    platform: str, import_id: int
) -> list[ListingSubmission]:
    submissions = await ListingSubmission.filter(
        platform_id=platform,
        platform_meta__contains={"product_import_id": import_id},
    ).prefetch_related("listing")

    if not submissions:
        # Fall back to in-memory filter if the JSON contains-query is unsupported
        # for the column type on this DB.
        all_subs = await ListingSubmission.filter(platform_id=platform).prefetch_related(
            "listing"
        )
        submissions = [
            s
            for s in all_subs
            if (s.platform_meta or {}).get("product_import_id") == import_id
        ]

    return submissions


def _build_import_detail(
    platform: str, import_id: int, submissions: list[ListingSubmission]
) -> ImportDetailResponse:
    status_counts: dict[str, int] = defaultdict(int)
    details: list[ImportListingDetail] = []
    batch_number = None
    for sub in submissions:
        status_counts[_effective_status(sub)] += 1
        if batch_number is None:
            batch_number = (sub.platform_meta or {}).get("batch_number")
        listing = sub.listing
        title = None
        product_id = None
        listing_id = None
        skus: list[str] = []
        if listing:
            product_id = listing.product_id
            listing_id = str(listing.id)
            data = listing.data or {}
            title = data.get("title") or data.get("name")
            skus = list((data.get("child_size_overrides") or {}).keys())
        details.append(
            ImportListingDetail(
                submission_id=sub.id,
                listing_id=listing_id,
                product_id=product_id,
                title=title,
                status=sub.status,
                platform_status=sub.platform_status,
                error_display=sub.error_display,
                sku_errors=(sub.platform_meta or {}).get("sku_errors") or None,
                skus=skus,
                updated_skus=(sub.platform_meta or {}).get("updated_references") or [],
                updated_at=sub.updated_at,
                reviewed_at=sub.reviewed_at,
            )
        )

    return ImportDetailResponse(
        import_id=import_id,
        platform_id=platform,
        batch_number=batch_number,
        submissions=details,
        status_counts=dict(status_counts),
    )


@router.get("/imports", response_model=ImportDetailResponse)
async def get_import_detail(
    id: int = Query(..., description="Platform-side import id"),
    platform: str = Query(..., description="Platform identifier, e.g. 'spo'"),
):
    import_id = id
    await _get_platform_settings_for(platform)

    submissions = await _load_import_submissions(platform, import_id)
    if not submissions:
        raise HTTPException(status_code=404, detail=f"Import {import_id} not found")

    return _build_import_detail(platform, import_id, submissions)


@router.post("/imports/mark_reviewed", response_model=ImportDetailResponse)
async def mark_import_reviewed(
    request: Request,
    id: int = Query(..., description="Platform-side import id"),
    platform: str = Query(..., description="Platform identifier, e.g. 'spo'"),
):
    import_id = id
    await _get_platform_settings_for(platform)

    submissions = await _load_import_submissions(platform, import_id)
    if not submissions:
        raise HTTPException(status_code=404, detail=f"Import {import_id} not found")

    user_id = request.state.user["id"]
    now = datetime.now(timezone.utc)
    # Reviewing resolves the failure, so the row becomes a real success rather
    # than a failure the dashboard translates on the way out. _effective_status
    # only ever applied here; every other surface (the listing platform pills,
    # the batch product tabs, the submit button label) reads the raw status, so
    # a reviewed row used to stay red everywhere outside this screen.
    # error_display and platform_meta.sku_errors are left in place: the row is
    # resolved, not rewritten, and the import detail still shows what failed.
    reviewed_ids: list[int] = []
    for sub in submissions:
        if sub.status == SubmissionStatus.FAILED and sub.reviewed_at is None:
            sub.status = SubmissionStatus.SUCCESS
            sub.reviewed_at = now
            sub.reviewed_by = user_id
            await sub.save(update_fields=["status", "reviewed_at", "reviewed_by"])
            reviewed_ids.append(sub.id)

    await record_step(
        reviewed_ids, "reviewed", reviewed_by=user_id, previous_status="failed"
    )

    logger.info(
        "Import %s (%s): marked %d failed submission(s) reviewed by %s",
        import_id,
        platform,
        len(reviewed_ids),
        user_id,
    )

    return _build_import_detail(platform, import_id, submissions)


@router.post("/imports/error_template")
async def download_error_template(
    id: int = Query(..., description="Platform-side import id"),
    platform: str = Query(..., description="Platform identifier, e.g. 'spo'"),
):
    import_id = id
    await _get_platform_settings_for(platform)

    if platform != "spo":
        raise HTTPException(
            status_code=404,
            detail=f"Error template download is not implemented for platform '{platform}'",
        )

    submissions = await _load_import_submissions(platform, import_id)
    failed = [
        s
        for s in submissions
        if s.status == SubmissionStatus.FAILED and s.reviewed_at is None
    ]
    if not failed:
        raise HTTPException(
            status_code=404,
            detail=f"No failed submissions in import {import_id}",
        )

    template = await TemplateService.get_template_by_id("default")
    field_definitions = template.field_definitions if template else []

    # Prefer the {sku -> error} map we stored on platform_meta when the
    # SpoPoller observed the failure (avoids re-hitting SPO and avoids the
    # 500-char truncation in error_display). For older rows that pre-date
    # the persisted map, fall back to re-fetching SPO's per-SKU reports.
    sku_errors: dict[str, str] = {}
    for sub in failed:
        stored = (sub.platform_meta or {}).get("sku_errors") or {}
        if isinstance(stored, dict):
            for sku, err in stored.items():
                if sku and err:
                    sku_errors[sku] = err

    if not sku_errors:
        for fetch, label in (
            (spo_service.get_transformation_error_report, "transformation"),
            (spo_service.get_error_report, "product"),
        ):
            try:
                entries = await fetch(import_id)
            except Exception:
                logger.exception(
                    "Failed to fetch SPO %s error report for import %s",
                    label,
                    import_id,
                )
                continue
            for entry in entries:
                sku = entry.get("sku")
                err = entry.get("error")
                if sku and err:
                    sku_errors[sku] = err

    rows_with_error: list[tuple[dict[str, Any], str]] = []
    for sub in failed:
        listing = sub.listing
        if not listing:
            continue
        try:
            products = await spo_service.build_product_rows(
                listing, listing.data or {}, field_definitions
            )
        except Exception:
            logger.exception(
                "Skipping submission %s in error template export, build_product_rows failed",
                sub.id,
            )
            continue
        fallback = sub.error_display or "Submission failed"
        for product in products:
            sku = product.get("sku")
            error_text = sku_errors.get(sku) or fallback
            rows_with_error.append((product, error_text))

    if not rows_with_error:
        raise HTTPException(
            status_code=422,
            detail="Could not rebuild any failed product rows for export",
        )

    def _build_xlsx() -> bytes:
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Sheet1"
        ws.append(spo_service_module.DISPLAY_HEADERS + ["Error"])
        ws.append(spo_service_module.API_HEADERS + ["error"])
        for product, error_text in rows_with_error:
            row = [product.get(field_id) for field_id in spo_service_module.API_HEADERS]
            row.append(error_text)
            ws.append(row)
        buf = io.BytesIO()
        wb.save(buf)
        return buf.getvalue()

    loop = asyncio.get_event_loop()
    payload = await loop.run_in_executor(None, _build_xlsx)

    return StreamingResponse(
        io.BytesIO(payload),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": (
                f'attachment; filename="spo_errors_import_{import_id}.xlsx"'
            )
        },
    )
