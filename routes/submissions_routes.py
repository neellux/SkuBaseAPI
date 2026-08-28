import asyncio
import csv
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
    → pending; otherwise any awaiting_action → awaiting_action; otherwise any failed
    → failed; otherwise success.

    awaiting_action outranks failed on purpose. It is the only status in the list an
    operator can act on from this screen, and an import showing "failed" because two of its
    rows never listed would bury the fact that the other three hundred are waiting on an
    upload. It sits below processing and pending because those still resolve on their own.
    """
    counts = status_counts or {}
    if counts.get(SubmissionStatus.PROCESSING, 0) > 0:
        return SubmissionStatus.PROCESSING
    if counts.get(SubmissionStatus.PENDING, 0) > 0:
        return SubmissionStatus.PENDING
    if counts.get(SubmissionStatus.AWAITING_ACTION, 0) > 0:
        return SubmissionStatus.AWAITING_ACTION
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

    awaiting_action is deliberately NOT added in. Both are work outstanding, but they are
    different work: pending is "nobody has sent this yet" and drives the submit button,
    awaiting_action is "the platform has it and a person owes it a step". Summing them
    would recreate exactly the ambiguity a separate status was introduced to remove. It is
    counted alongside instead, by the function below.
    """
    conn = connections.get("default")
    rows = await conn.execute_query_dict(
        "SELECT platform_id, count(*) AS n FROM listing_submissions "
        "WHERE status = $1 GROUP BY platform_id",
        [SubmissionStatus.PENDING],
    )
    return {row["platform_id"]: row["n"] for row in rows}


async def _awaiting_action_counts_by_platform() -> dict[str, int]:
    """Submissions the platform accepted that still owe a human a step, per platform."""
    conn = connections.get("default")
    rows = await conn.execute_query_dict(
        "SELECT platform_id, count(*) AS n FROM listing_submissions "
        "WHERE status = $1 GROUP BY platform_id",
        [SubmissionStatus.AWAITING_ACTION],
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
            platform_awaiting_action_counts=await _awaiting_action_counts_by_platform(),
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
        # Claimed and given an import id synchronously, then worked in the background --
        # the same shape as grailed, and for a stronger reason: an eBay batch is four
        # SellerCloud round trips and the custom export alone takes about a minute. Holding
        # the request open for that would time out the browser long before it finished.
        #
        # The id comes from ebay_batch_seq rather than from SellerCloud: none of the queued
        # job ids exist yet at this point, and the ones that arrive later belong to
        # individual steps rather than to the batch.
        import_id, submission_ids = await ebay_poller.begin_batch()
        if not submission_ids:
            return CreateBatchResponse(
                platform=platform, submission_count=0, product_import_id=None
            )

        async def _run_ebay_batch():
            try:
                await ebay_poller.run_batch(import_id, submission_ids)
            except Exception:
                # Already recorded on the rows by _submit_batch, which fails them and
                # writes SellerCloud's own message into the step history.
                logger.exception("Background eBay batch %s failed", import_id)

        asyncio.create_task(_run_ebay_batch())
        return CreateBatchResponse(
            platform=platform,
            submission_count=len(submission_ids),
            product_import_id=import_id,
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
    platform: str, import_id: int, submissions: list[ListingSubmission],
    image_counts: dict[str, int] | None = None,
) -> ImportDetailResponse:
    status_counts: dict[str, int] = defaultdict(int)
    details: list[ImportListingDetail] = []
    batch_number = None
    publish_job_id = None
    for sub in submissions:
        status_counts[_effective_status(sub)] += 1
        if batch_number is None:
            batch_number = (sub.platform_meta or {}).get("batch_number")
        # Every submission in an import carries the same ebay_jobs map, so the first one
        # that has it answers for the batch.
        if publish_job_id is None:
            publish_job_id = ((sub.platform_meta or {}).get("ebay_jobs") or {}).get("publish")
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
                image_count=(image_counts or {}).get(product_id or "", 0),
                item_ids=(sub.external_id or {}).get("item_ids") or None,
                updated_at=sub.updated_at,
                reviewed_at=sub.reviewed_at,
            )
        )

    return ImportDetailResponse(
        import_id=import_id,
        platform_id=platform,
        batch_number=batch_number,
        publish_job_id=str(publish_job_id) if publish_job_id else None,
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

    # One extra query, and only for the platform that has a manual image step. Without it
    # the dialog cannot tell a product that is missing from the image file from one that is
    # in it, because the difference is whether the parent has photos at all.
    image_counts = (
        await _images_by_parent([s.listing.product_id for s in submissions if s.listing])
        if platform == "ebay"
        else {}
    )
    return _build_import_detail(platform, import_id, submissions, image_counts)


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


# --------------------------------------------------------------- eBay image revise step
# Publishing to eBay does not attach a listing's images. An operator does that by uploading
# a File Exchange file by hand, which is why an eBay submission parks in AWAITING_ACTION
# instead of going straight to SUCCESS.

GCS_ROOT = "https://storage.googleapis.com/lux_products"
# CSV with a single header row. eBay's downloaded template carries a "#INFO Version=1.0.0
# Template=..." line above the header, but File Exchange only requires it on the templates
# it hands out; an upload with two header rows reads the second one as data.
REVISE_HEADER_ROW = ("Action", "Item number", "PicURL")
PIC_SEPARATOR = " | "


def _require_manual_image_step(platform: str) -> None:
    """These endpoints are eBay's, and deliberately not behind manual_fallback.

    _get_platform_settings_for gates the SPO endpoints on
    platform_settings.<id>.manual_fallback, which means "the automated path failed, a human
    picked it up". eBay's image upload is not a fallback from anything: it is the only way
    images are ever attached, on every listing, every time. Gating it on a fallback flag
    would make the normal path look like an exception.
    """
    if platform != "ebay":
        raise HTTPException(
            status_code=404,
            detail=f"There is no manual image step for platform '{platform}'",
        )


async def _images_by_parent(parents: list[str]) -> dict[str, int]:
    """Image count per parent, from the NEWEST productimages row for each.

    Newest matters. A re-shoot writes a new row and GCS keeps the old blobs, so a product
    whose current shoot has 4 images can still have 5_fullsize.jpg sitting in the bucket
    from the previous one. Counting what exists in GCS would attach a stale image to a live
    listing; the table is the only thing that knows which shoot is current.

    created_at, not updated_at: editing a product's washtags writes to its older
    batch_creation row, which would otherwise make a stale shoot look like the newest.
    """
    wanted = [p for p in parents if p]
    if not wanted:
        return {}
    rows = await connections.get("photography_db").execute_query_dict(
        """
        SELECT DISTINCT ON (product_id) product_id, product_images_count
        FROM productimages
        WHERE product_id = ANY($1::text[])
        ORDER BY product_id, created_at DESC
        """,
        [wanted],
    )
    return {r["product_id"]: (r["product_images_count"] or 0) for r in rows}


def _revise_rows(
    submissions: list[ListingSubmission], image_counts: dict[str, int]
) -> list[tuple[str, str]]:
    """(item number, pipe-joined image URLs) per listed child, ready for the sheet.

    One row per CHILD, because eBay addresses a variation by its own item number, and every
    child of a parent carries that parent's images.
    """
    out: list[tuple[str, str]] = []
    for sub in submissions:
        parent = sub.listing.product_id if sub.listing else None
        count = image_counts.get(parent, 0)
        if not parent or not count:
            continue
        urls = PIC_SEPARATOR.join(
            f"{GCS_ROOT}/{parent}/{index}_fullsize.jpg" for index in range(1, count + 1)
        )
        for _sku, item_id in sorted(((sub.external_id or {}).get("item_ids") or {}).items()):
            out.append((str(item_id), urls))
    return out


def _revise_csv(rows: list[tuple[str, str]]) -> bytes:
    """The File Exchange upload: comma-delimited, ONE header row.

    eBay's downloaded template carries a "#INFO Version=1.0.0 Template=..." line above the
    header, but that is metadata on the file eBay hands out, not something an upload needs.
    Sent back with two header rows, File Exchange reads the second one as a data row.

    CRLF line endings, because File Exchange is a Windows-era format that has been reported
    to read a whole LF-only file as a single row. csv.writer emits them directly, so nothing
    depends on newline translation at write time.
    """
    buf = io.StringIO(newline="")
    writer = csv.writer(buf, lineterminator="\r\n")
    writer.writerow(REVISE_HEADER_ROW)
    for item_id, urls in rows:
        writer.writerow(["Revise", item_id, urls])
    # utf-8-sig: the BOM is what stops Excel opening a UTF-8 CSV as latin-1 and mangling an
    # accented brand name inside a URL. eBay ignores it.
    return buf.getvalue().encode("utf-8-sig")


def _batch_ids(submissions: list[ListingSubmission]) -> list[int]:
    """SkuBase batches the import touched, ascending. Names the revise download.

    A list rather than one value: an eBay flush claims every pending row, so an import can
    span batches when two were submitted close together. batch is nullable and SET_NULL on
    delete, so a listing whose batch is gone contributes nothing rather than a None the
    caller has to filter again.
    """
    return sorted(
        {
            sub.listing.batch_id
            for sub in submissions
            if sub.listing and sub.listing.batch_id is not None
        }
    )


def _revise_filename(import_id: int, batch_ids: list[int]) -> str:
    """Name the download after the batch, because that is what the operator is working on.

    The import id alone said nothing an operator could match to the batch page they came
    from. Sent on Content-Disposition and read back by ImportDetailDialog, so the name lives
    on the one side that knows what went into the file.

    Truncated past three batches rather than listing them all, so a flush that swept a dozen
    batches cannot produce a filename a file manager refuses to write.
    """
    if not batch_ids:
        return f"ebay_image_revise_import_{import_id}.csv"
    if len(batch_ids) == 1:
        return f"ebay_image_revise_batch_{batch_ids[0]}_import_{import_id}.csv"
    if len(batch_ids) <= 3:
        joined = "-".join(str(b) for b in batch_ids)
        return f"ebay_image_revise_batches_{joined}_import_{import_id}.csv"
    return (
        f"ebay_image_revise_batches_{batch_ids[0]}-and-{len(batch_ids) - 1}-more"
        f"_import_{import_id}.csv"
    )


@router.post("/imports/image_revise_template")
async def download_image_revise_template(
    id: int = Query(..., description="Platform-side import id"),
    platform: str = Query("ebay", description="Platform identifier"),
):
    """The CSV an operator uploads to eBay to attach images to a published import."""
    import_id = id
    _require_manual_image_step(platform)

    submissions = await _load_import_submissions(platform, import_id)
    if not submissions:
        raise HTTPException(status_code=404, detail=f"Import {import_id} not found")

    image_counts = await _images_by_parent(
        [s.listing.product_id for s in submissions if s.listing]
    )
    rows = _revise_rows(submissions, image_counts)
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No eBay item numbers yet for import {import_id}. They appear over the "
                "hour or so after publishing; try again shortly."
            ),
        )

    payload = _revise_csv(rows)
    filename = _revise_filename(import_id, _batch_ids(submissions))

    return StreamingResponse(
        io.BytesIO(payload),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/imports/mark_images_uploaded", response_model=ImportDetailResponse)
async def mark_images_uploaded(
    request: Request,
    id: int = Query(..., description="Platform-side import id"),
    platform: str = Query("ebay", description="Platform identifier"),
):
    """Close out an import the operator has uploaded images for.

    One click, but per row. A child that never got an eBay item number never listed, so it
    got no images either and there is nothing to confirm: marking it successful would record
    it as live when it is not. Those fail instead, carrying eBay's own reason where the
    publish job gave one.
    """
    import_id = id
    _require_manual_image_step(platform)

    submissions = await _load_import_submissions(platform, import_id)
    if not submissions:
        raise HTTPException(status_code=404, detail=f"Import {import_id} not found")

    waiting = [s for s in submissions if s.status == SubmissionStatus.AWAITING_ACTION]
    if not waiting:
        # Not an error. A second click, or a colleague who got there first, should read as
        # "already done" rather than as a failure.
        return _build_import_detail(platform, import_id, submissions)

    user_id = request.state.user["id"]
    now = datetime.now(timezone.utc)
    completed_ids: list[int] = []
    failed_ids: list[int] = []

    for sub in waiting:
        item_ids = (sub.external_id or {}).get("item_ids") or {}
        if item_ids:
            sub.status = SubmissionStatus.SUCCESS
            sub.completed_at = now
            sub.completed_by = user_id
            await sub.save(
                update_fields=["status", "completed_at", "completed_by", "updated_at"]
            )
            completed_ids.append(sub.id)
            continue

        errors = (sub.platform_meta or {}).get("publish_errors") or {}
        reason = next(iter(errors.values()), None) if isinstance(errors, dict) else None
        sub.status = SubmissionStatus.FAILED
        sub.error_display = reason or "eBay returned no item number for any child"
        await sub.save(update_fields=["status", "error_display", "updated_at"])
        failed_ids.append(sub.id)

    if completed_ids:
        await record_step(
            completed_ids, "images_uploaded", completed_by=user_id
        )
    if failed_ids:
        await record_step(failed_ids, "failed", stage="publish", reason="no eBay item number")

    logger.info(
        "Import %s (%s): %d completed, %d failed after image upload confirmed by %s",
        import_id, platform, len(completed_ids), len(failed_ids), user_id,
    )
    return _build_import_detail(platform, import_id, submissions)
