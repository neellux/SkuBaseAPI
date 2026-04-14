import asyncio
import logging
import traceback
from datetime import datetime, timezone
from typing import List, Optional

import orjson
from fastapi import APIRouter, HTTPException, Query, Request
from models.api_models import (
    BatchConfirmationRequest,
    BatchConfirmationResponse,
    BatchFilterOptionsResponse,
    BatchListResponse,
    BatchProductConfirmationData,
    BatchResponse,
    ChildrenResponse,
    CreateBatchRequest,
    CreateListingRequest,
    ListingResponse,
    ListingSchemaResponse,
    ProductConfirmationData,
    ProductTypeInfoResponse,
    SaveSizeMappingRequest,
    SizingSchemeData,
    SizingSchemesResponse,
    SubmitListingRequest,
    UpdateBatchRequest,
    UpdateListingRequest,
)
from models.db_models import AppSettings, Listing, ListingSubmission
from services.batch_service import BatchService
from services.grailed_service import grailed_service
from services.listing_options_service import listing_options_service
from services.listing_service import ListingService
from services.product_info_service import ProductInfoService
from services.sellercloud_service import sellercloud_service
from services.template_service import TemplateService
from tortoise import Tortoise
from tortoise.transactions import in_transaction
from utils.load_app_data import add_user_data, app_users

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/listings", tags=["listings"])


def _log_task_exception(task: asyncio.Task) -> None:
    if not task.cancelled() and task.exception():
        logger.error(
            "Background submission task failed",
            exc_info=task.exception(),
        )


@router.get("/product/confirm", response_model=ProductConfirmationData)
async def get_product_confirmation_data(
    product_id: str = Query(..., description="Product ID from SellerCloud"),
):
    product = await sellercloud_service.get_product_by_id(product_id)
    if not product:
        raise HTTPException(status_code=404, detail=f"Product {product_id} not found")

    (
        all_valid,
        missing_images,
        total_count,
    ) = await sellercloud_service.validate_product_images_on_gcs(product["ID"])
    if not all_valid:
        raise HTTPException(status_code=400, detail="Product images not found")

    parent_product_id = product.get("PARENT_ID", product_id)
    draft_listing = await ListingService.get_draft_listing_by_product_id(parent_product_id)
    existing_listing_id = str(draft_listing.id) if draft_listing else None

    return ProductConfirmationData(product=product, existing_listing_id=existing_listing_id)


@router.post("", response_model=ListingResponse)
async def create_listing(request_data: CreateListingRequest, request: Request):
    created_by = request.state.user["id"]

    if created_by not in app_users:
        raise HTTPException(status_code=400, detail="Creating user not found")
    if request_data.assigned_to and request_data.assigned_to not in app_users:
        raise HTTPException(status_code=400, detail="Assigned user not found")

    full_product_id = request_data.product_id
    parent_product_id = (
        "/".join(full_product_id.split("/")[:-1]) if "/" in full_product_id else full_product_id
    )

    request_data.product_id = parent_product_id
    request_data.info_product_id = full_product_id

    listing = await ListingService.create_listing(request_data, created_by)
    return listing


@router.get("/images", response_model=List[str])
async def get_listing_images(listing_id: str = Query(..., description="Listing ID")):
    listing = await ListingService.get_listing_by_id(listing_id)
    if not listing:
        raise HTTPException(status_code=404, detail="Listing not found")
    product_id_to_fetch = listing.info_product_id or listing.product_id

    images = await sellercloud_service.get_product_images(product_id_to_fetch)
    return images


@router.get("/children", response_model=ChildrenResponse)
async def get_listing_children(
    listing_id: str = Query(..., description="Listing ID"),
    product_type: Optional[str] = Query(None, description="Override ProductType for validation"),
):
    listing = await ListingService.get_listing_by_id(listing_id)
    if not listing:
        raise HTTPException(status_code=404, detail="Listing not found")

    product_id_to_fetch = listing.info_product_id or listing.product_id

    override_sizing_scheme = listing.data.get("SIZING_SCHEME") if listing.data else None

    children_data = await sellercloud_service.get_product_children(
        product_id_to_fetch,
        override_product_type=product_type,
        override_sizing_scheme=override_sizing_scheme,
    )
    return children_data


@router.get("/sizing_schemes", response_model=SizingSchemesResponse)
async def get_sizing_schemes(
    product_type: str = Query(..., description="ProductType to fetch sizing schemes for"),
):
    try:
        conn = Tortoise.get_connection("default")

        result = await conn.execute_query_dict(
            """
            SELECT t.sizing_types, ss.sizing_scheme,
                   array_agg(ss.size ORDER BY ss."order") as sizes,
                   json_agg(json_build_object('id', ss.id::text, 'size', ss.size) ORDER BY ss."order") as size_entries
            FROM listingoptions_types t
            CROSS JOIN listingoptions_sizing_schemes ss
            WHERE t.type = $1
              AND ss.sizing_types ? t.sizing_types
            GROUP BY t.sizing_types, ss.sizing_scheme
            ORDER BY ss.sizing_scheme
            """,
            [product_type],
        )

        sizing_type = result[0]["sizing_types"] if result else None

        schemes = [
            SizingSchemeData(
                sizing_scheme=row["sizing_scheme"],
                sizes=row["sizes"],
                size_entries=(
                    orjson.loads(row["size_entries"])
                    if isinstance(row.get("size_entries"), str)
                    else row.get("size_entries")
                ),
            )
            for row in result
        ]

        return SizingSchemesResponse(schemes=schemes, sizing_type=sizing_type)

    except Exception as e:
        logger.error(f"Failed to fetch sizing schemes: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/platform_size_records")
async def get_platform_size_records(
    platform_id: str = Query(..., description="Platform ID to get size records for"),
    sizing_type: Optional[str] = Query(None, description="Sizing type to filter by"),
):
    try:
        records = await listing_options_service.get_platform_size_records(platform_id, sizing_type)
        return {"sizes": records}
    except Exception as e:
        logger.error(f"Failed to fetch platform size records: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/save_size_mapping")
async def save_size_mapping(body: SaveSizeMappingRequest):
    try:
        result = await listing_options_service.save_size_mapping(
            body.sizing_scheme_entry_id,
            body.platform_id,
            body.platform_value,
            body.sizing_type,
        )
        if result.get("error"):
            raise HTTPException(status_code=409, detail=result)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to save size mapping: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/product_type_info", response_model=ProductTypeInfoResponse)
async def get_product_type_info(
    product_type: str = Query(..., description="ProductType to fetch info for"),
):
    try:
        product_type_info = await listing_options_service.get_product_type_info(product_type)
        return ProductTypeInfoResponse(**product_type_info)
    except Exception as e:
        logger.error(f"Failed to fetch product type info: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/detail", response_model=ListingResponse)
async def get_listing(listing_id: str = Query(..., description="Listing ID")):
    listing = await ListingService.get_listing_by_id(listing_id)
    if not listing:
        raise HTTPException(status_code=404, detail="Listing not found")

    listing_dict = listing.model_dump()
    listing_dict = await add_user_data(
        data=listing_dict, keys=["assigned_to", "created_by"], new_keys=["name"]
    )

    return listing_dict


@router.put("", response_model=ListingResponse)
async def update_listing(
    request: UpdateListingRequest,
    listing_id: str = Query(..., description="Listing ID"),
):
    listing = await ListingService.update_listing(listing_id, request)
    if not listing:
        raise HTTPException(status_code=404, detail="Listing not found")

    return listing


@router.delete("")
async def delete_listing(listing_id: str = Query(..., description="Listing ID")):
    success = await ListingService.delete_listing(listing_id)
    if not success:
        raise HTTPException(status_code=404, detail="Listing not found")
    return {"message": "Listing deleted successfully"}


@router.get("/submission_status")
async def get_submission_status(
    listing_id: str = Query(..., description="Listing ID"),
):
    listing_model = await Listing.get_or_none(id=listing_id)
    if not listing_model:
        raise HTTPException(status_code=404, detail="Listing not found")

    submissions = await ListingSubmission.filter(listing=listing_model).all()

    platforms = {}
    for sub in submissions:
        if (
            sub.platform_id not in platforms
            or sub.attempt_number > platforms[sub.platform_id].attempt_number
        ):
            platforms[sub.platform_id] = sub

    platform_statuses = {
        pid: {
            "status": sub.status,
            "error_display": sub.error_display,
            "platform_status": sub.platform_status,
            "external_id": sub.external_id,
        }
        for pid, sub in platforms.items()
    }

    all_complete = (
        all(s["status"] in ("success", "failed", "queued") for s in platform_statuses.values())
        if platform_statuses
        else True
    )

    return {
        "platforms": platform_statuses,
        "all_complete": all_complete,
    }


@router.post("/submit")
async def submit_listing(
    request: Request,
    body: SubmitListingRequest = None,
    listing_id: str = Query(..., description="Listing ID"),
    skip_brand_color_update: bool = Query(False, description="Skip BRAND_COLOR alias update"),
):
    submitted_by = request.state.user["id"]

    listing = await ListingService.get_listing_by_id(listing_id)
    if not listing:
        raise HTTPException(status_code=404, detail="Listing not found")

    template = await TemplateService.get_template_by_id("default")
    if not template:
        raise HTTPException(status_code=500, detail="Template not found for submission")

    listing_model = await Listing.get_or_none(id=listing_id)
    if not listing_model:
        raise HTTPException(status_code=404, detail="Listing not found")

    platforms = body.platforms if body and body.platforms else None
    if not platforms:
        settings = await AppSettings.first()
        platforms = settings.platforms if settings and settings.platforms else ["sellercloud"]

    settings = await AppSettings.first()
    platform_settings = (
        settings.platform_settings if settings and settings.platform_settings else {}
    )

    form_data_for_mpn = listing.data or {}
    form_mpn = (
        form_data_for_mpn.get("manufacturer_sku")
        or form_data_for_mpn.get("ManufacturerSKU")
        or form_data_for_mpn.get("mpn")
    )
    form_brand = form_data_for_mpn.get("brand_name")

    if form_mpn and form_brand:
        conn = Tortoise.get_connection("product_db")
        stored_result = await conn.execute_query_dict(
            "SELECT mpn FROM parent_products WHERE sku = $1 AND is_active = TRUE",
            [listing.product_id],
        )
        if stored_result:
            stored_mpn = stored_result[0].get("mpn") or ""
            if form_mpn.strip().lower() != stored_mpn.strip().lower():
                conflict_result = await conn.execute_query_dict(
                    """
                    SELECT sku FROM parent_products
                    WHERE LOWER(brand) = LOWER($1)
                      AND LOWER(mpn) = LOWER($2)
                      AND sku != $3
                      AND is_active = TRUE
                    LIMIT 1
                    """,
                    [form_brand, form_mpn, listing.product_id],
                )
                if conflict_result:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "type": "mpn_conflict",
                            "conflicting_sku": conflict_result[0]["sku"],
                            "original_mpn": stored_mpn,
                        },
                    )

    child_size_overrides = listing.data.get("child_size_overrides", {})
    if child_size_overrides:
        size_to_children = {}
        for child_sku, size in child_size_overrides.items():
            if size:
                if size not in size_to_children:
                    size_to_children[size] = []
                size_to_children[size].append(child_sku)

        duplicates = {s: c for s, c in size_to_children.items() if len(c) > 1}
        if duplicates:
            duplicate_details = [f"Size '{s}': {', '.join(c)}" for s, c in duplicates.items()]
            raise HTTPException(
                status_code=400,
                detail=f"Duplicate sizes: {'; '.join(duplicate_details)}",
            )

        empty_sizes = [
            sku for sku, size in child_size_overrides.items() if not size or not size.strip()
        ]
        if empty_sizes:
            raise HTTPException(
                status_code=400, detail=f"Missing sizes for: {', '.join(empty_sizes)}"
            )

    if not skip_brand_color_update:
        form_data = listing.data or {}
        color = form_data.get("stanadard_color")
        brand_color = form_data.get("brand_color")
        if color and brand_color and color.lower() != brand_color.lower():
            await sellercloud_service.validate_brand_color(color, brand_color)
            await sellercloud_service.add_color_alias(color, brand_color)

    non_sc_platforms = [p for p in platforms if p != "sellercloud"]
    if non_sc_platforms:
        form_data = listing.data or {}
        sizing_scheme = form_data.get("SIZING_SCHEME")
        child_sizes = list(set(v for v in child_size_overrides.values() if v and v.strip()))
        if sizing_scheme and child_sizes:
            product_type = form_data.get("product_type")
            sizing_type = None
            if product_type:
                conn = Tortoise.get_connection("default")
                type_result = await conn.execute_query_dict(
                    "SELECT sizing_types FROM listingoptions_types WHERE type = $1 LIMIT 1",
                    [product_type],
                )
                if type_result:
                    sizing_type = type_result[0]["sizing_types"]

            unmapped = await listing_options_service.check_unmapped_sizes(
                sizing_scheme, child_sizes, non_sc_platforms, sizing_type
            )
            if unmapped:
                raise HTTPException(
                    status_code=422,
                    detail={
                        "type": "unmapped_sizes",
                        "sizing_scheme": sizing_scheme,
                        "platforms_with_missing": unmapped,
                    },
                )

    submission_records = {}
    try:
        async with in_transaction("default") as conn:
            await Listing.select_for_update().using_db(conn).get(id=listing_id)

            for platform_id in platforms:
                latest = (
                    await ListingSubmission.filter(
                        listing=listing_model,
                        platform_id=platform_id,
                    )
                    .using_db(conn)
                    .order_by("-attempt_number")
                    .first()
                )

                if latest and latest.status in ("queued", "pending", "processing"):
                    logger.info(f"Skipping {platform_id}: already in {latest.status}")
                    continue

                ps = platform_settings.get(platform_id, {})
                allow_resubmit = ps.get("allow_resubmit", True)
                if latest and latest.status == "success" and not allow_resubmit:
                    logger.info(f"Skipping {platform_id}: resubmission not allowed")
                    continue

                batch_submit = ps.get("batch_submit", False)
                requires_images = ps.get("requires_images", False)
                if batch_submit or (listing_model.upload_status == "pending" and requires_images):
                    platform_initial_status = "queued"
                else:
                    platform_initial_status = "pending"

                attempt_number = (latest.attempt_number + 1) if latest else 1
                submission = await ListingSubmission.create(
                    listing=listing_model,
                    platform_id=platform_id,
                    status=platform_initial_status,
                    submitted_by=submitted_by,
                    attempt_number=attempt_number,
                    using_db=conn,
                )
                submission_records[platform_id] = submission.id
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create submission records: {str(e)}",
        )

    if not submission_records:
        raise HTTPException(
            status_code=409,
            detail="No platforms available for submission",
        )

    listing_model.submitted_by = submitted_by
    await listing_model.save()

    pending_platforms = [
        p
        for p in platforms
        if not platform_settings.get(p, {}).get("batch_submit", False)
        and not (
            listing_model.upload_status == "pending"
            and platform_settings.get(p, {}).get("requires_images", False)
        )
    ]

    if not pending_platforms:
        return {"status": "queued", "platforms": platforms}

    task = asyncio.create_task(
        _run_submissions_background(
            listing_id=listing_id,
            product_id=listing.product_id,
            form_data=listing.data,
            field_definitions=template.field_definitions or [],
            platforms=pending_platforms,
            submission_record_ids=submission_records,
            submitted_by=submitted_by,
        )
    )
    task.add_done_callback(_log_task_exception)

    return {"status": "submitting", "platforms": platforms}


async def _run_submissions_background(
    listing_id: str,
    product_id: str,
    form_data: dict,
    field_definitions: list,
    platforms: list,
    submission_record_ids: dict,
    submitted_by: str,
):

    async def _submit_to_sellercloud(submission_id: int):
        submission = await ListingSubmission.get(id=submission_id)
        try:
            await sellercloud_service.submit_listing_to_sellercloud(
                product_id=product_id,
                form_data=form_data,
                field_definitions=field_definitions,
            )
            submission.status = "success"
            await submission.save()
            logger.info(f"Successfully submitted listing {listing_id} to SellerCloud")
        except Exception as e:
            logger.error(f"Failed to submit to SellerCloud: {str(e)}", exc_info=True)
            submission.status = "failed"
            submission.error = traceback.format_exc()
            submission.error_display = "Failed to submit"
            await submission.save()

    async def _submit_to_grailed(submission_id: int):
        submission = await ListingSubmission.get(id=submission_id)
        try:
            listing_model = await Listing.get_or_none(id=listing_id)
            if not listing_model:
                submission.status = "failed"
                submission.error = "Listing not found"
                submission.error_display = "Failed to submit"
                await submission.save()
                return
            await grailed_service.submit_listing(
                listing=listing_model,
                form_data=form_data,
                field_definitions=field_definitions,
                submission=submission,
            )
            logger.info(f"Successfully submitted listing {listing_id} to Grailed")
        except Exception as e:
            logger.error(f"Failed to submit to Grailed: {str(e)}", exc_info=True)
            submission = await ListingSubmission.get(id=submission_id)
            if submission.status not in ("success", "failed"):
                submission.status = "failed"
                submission.error = traceback.format_exc()
                submission.error_display = "Failed to submit"
                await submission.save()

    settings = await AppSettings.first()
    platform_settings = (
        settings.platform_settings if settings and settings.platform_settings else {}
    )

    submission_tasks = []
    for platform_id, sid in submission_record_ids.items():
        if platform_settings.get(platform_id, {}).get("batch_submit", False):
            continue
        if platform_id == "sellercloud":
            submission_tasks.append(_submit_to_sellercloud(sid))
        elif platform_id == "grailed":
            submission_tasks.append(_submit_to_grailed(sid))

    if submission_tasks:
        await asyncio.gather(*submission_tasks, return_exceptions=True)

    post_submission_errors = []

    child_size_overrides = form_data.get("child_size_overrides", {})
    if child_size_overrides:
        try:
            conn = Tortoise.get_connection("product_db")
            size_updates_json = orjson.dumps(child_size_overrides).decode()
            result = await conn.execute_query_dict(
                "SELECT update_children_sizes_for_parent($1, $2::jsonb) as result",
                [product_id, size_updates_json],
            )
            db_result_raw = result[0]["result"] if result else None
            db_result = (
                orjson.loads(db_result_raw) if isinstance(db_result_raw, str) else db_result_raw
            )
            if db_result and not db_result.get("success"):
                post_submission_errors.append(f"child_size_update: {db_result.get('error')}")
            else:
                logger.info(
                    f"Updated sizes for {len(child_size_overrides)} children via DB function"
                )
        except Exception as e:
            logger.error(f"Failed to update child sizes: {e}", exc_info=True)
            post_submission_errors.append(f"child_size_update: {traceback.format_exc()}")

    title_value = form_data.get("title")
    mpn_value = (
        form_data.get("manufacturer_sku")
        or form_data.get("ManufacturerSKU")
        or form_data.get("mpn")
    )
    brand_value = form_data.get("brand_name")
    style_name_value = form_data.get("style_name")
    sizing_scheme_value = form_data.get("SIZING_SCHEME")
    brand_color_value = form_data.get("brand_color")
    color_value = form_data.get("standard_color")

    parent_fields = {
        "title": title_value,
        "mpn": mpn_value,
        "brand": brand_value,
        "style_name": style_name_value,
        "sizing_scheme": sizing_scheme_value,
        "brand_color": brand_color_value,
        "color": color_value,
    }
    parent_fields_to_update = {k: v for k, v in parent_fields.items() if v}

    if parent_fields_to_update:
        try:
            conn = Tortoise.get_connection("product_db")
            updates = []
            params = []
            param_idx = 1
            for col, val in parent_fields_to_update.items():
                updates.append(f"{col} = ${param_idx}")
                params.append(val)
                param_idx += 1
            updates.append("updated_at = CURRENT_TIMESTAMP")
            params.append(product_id)
            query = f"UPDATE parent_products SET {', '.join(updates)} WHERE sku = ${param_idx}"
            await conn.execute_query(query, params)
            logger.info(
                f"Updated parent {product_id}: {', '.join(f'{k}={v}' for k, v in parent_fields_to_update.items())}"
            )
        except Exception as e:
            logger.error(f"Failed to update parent in local DB: {e}", exc_info=True)
            post_submission_errors.append(f"parent_update: {traceback.format_exc()}")

    try:
        sync_result = await ProductInfoService.sync_columns_with_template(field_definitions)
        if sync_result.get("added"):
            logger.info(f"Added columns to product_info: {sync_result['added']}")
    except Exception as e:
        logger.error(f"Failed to sync product_info columns: {e}", exc_info=True)
        post_submission_errors.append(f"sync_columns: {traceback.format_exc()}")

    try:
        await ProductInfoService.upsert_product_info(
            parent_sku=product_id,
            data=form_data,
            field_definitions=field_definitions,
        )
        logger.info(f"Saved listing data to product_info for parent_sku: {product_id}")
    except Exception as e:
        logger.error(f"Failed to save to product_info: {e}", exc_info=True)
        post_submission_errors.append(f"product_info: {traceback.format_exc()}")

    listing_model = await Listing.get_or_none(id=listing_id)
    if listing_model:
        settings = await AppSettings.first()
        enabled_platforms = (
            settings.platforms if settings and settings.platforms else ["sellercloud"]
        )
        successful_platforms = set(
            await ListingSubmission.filter(
                listing=listing_model,
                platform_id__in=enabled_platforms,
                status="success",
            ).values_list("platform_id", flat=True)
        )
        all_succeeded = set(enabled_platforms) == successful_platforms
        if all_succeeded and not listing_model.submitted:
            listing_model.submitted = True
            listing_model.submitted_at = datetime.now(timezone.utc)

        if post_submission_errors:
            listing_model.error = "\n---\n".join(post_submission_errors)
        else:
            listing_model.error = None
        await listing_model.save()


@router.post("/disable_product")
async def disable_product(
    product_id: str = Query(..., description="Product ID to disable (child ID with size)"),
):
    try:
        await sellercloud_service.disable_product(product_id)

        try:
            conn = Tortoise.get_connection("product_db")
            await conn.execute_query(
                "UPDATE child_products SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP WHERE sku = $1",
                [product_id],
            )
        except Exception as db_err:
            logger.warning(
                f"Product {product_id} disabled in SellerCloud but failed to update product DB: {db_err}"
            )

        return {"message": f"Product {product_id} disabled successfully"}
    except Exception as e:
        logger.error(f"Failed to disable product {product_id}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to disable product: {str(e)}",
        )


@router.get("", response_model=List[ListingResponse])
async def get_listings(
    assigned_to: Optional[str] = Query(None, description="Filter by assigned user"),
    submitted: Optional[bool] = Query(None, description="Filter by submission status"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=100, description="Items per page"),
):
    listings, total = await ListingService.get_all_listings(
        assigned_to=assigned_to, submitted=submitted, page=page, page_size=page_size
    )

    listings = await add_user_data(
        data=listings, keys=["assigned_to", "created_by"], new_keys=["name"]
    )

    return listings


@router.get("/schema", response_model=ListingSchemaResponse)
async def get_listing_schema(template_id: str = Query(..., description="Template ID")):
    schema = await ListingService.get_listing_schema(template_id)
    if not schema:
        raise HTTPException(status_code=404, detail="Template not found")

    return schema


@router.post("/batch/confirm", response_model=BatchConfirmationResponse)
async def batch_product_confirmation(request: BatchConfirmationRequest):
    products = []
    success_count = 0
    existing_draft_count = 0
    error_count = 0

    for product_id in request.product_ids:
        try:
            product = await sellercloud_service.get_product_by_id(product_id)

            if not product:
                products.append(
                    BatchProductConfirmationData(
                        product_id=product_id,
                        status="not_found",
                        error=f"Product {product_id} not found in SellerCloud",
                    )
                )
                error_count += 1
                continue

            parent_product_id = (
                "/".join(product_id.split("/")[:-1]) if "/" in product_id else product_id
            )
            draft_listing = await ListingService.get_draft_listing_by_product_id(parent_product_id)

            if draft_listing:
                products.append(
                    BatchProductConfirmationData(
                        product_id=product_id,
                        product=product,
                        existing_listing_id=str(draft_listing.id),
                        status="existing_draft",
                    )
                )
                existing_draft_count += 1
            else:
                (
                    all_valid,
                    missing_images,
                    total_count,
                ) = await sellercloud_service.validate_product_images_on_gcs(product["ID"])
                if not all_valid:
                    products.append(
                        BatchProductConfirmationData(
                            product_id=product_id,
                            product=product,
                            status="error",
                            error="Product images not found",
                        )
                    )
                    error_count += 1
                else:
                    products.append(
                        BatchProductConfirmationData(
                            product_id=product_id, product=product, status="success"
                        )
                    )
                    success_count += 1

        except Exception as e:
            logger.error(f"Error processing product {product_id}: {str(e)}", exc_info=True)
            products.append(
                BatchProductConfirmationData(
                    product_id=product_id,
                    status="error",
                    error=f"Error processing product {product_id}",
                )
            )
            error_count += 1

    return BatchConfirmationResponse(
        products=products,
        total_count=len(request.product_ids),
        success_count=success_count,
        existing_draft_count=existing_draft_count,
        error_count=error_count,
    )


@router.post("/batch", response_model=BatchResponse)
async def create_batch(request_data: CreateBatchRequest, request: Request):
    created_by = request.state.user["id"]
    if created_by not in app_users:
        raise HTTPException(status_code=400, detail="Creating user not found")
    if request_data.assigned_to and request_data.assigned_to not in app_users:
        raise HTTPException(status_code=400, detail="Assigned user not found")

    batch = await BatchService.create_batch(request_data, created_by)
    return batch


@router.get("/batch/detail", response_model=BatchResponse)
async def get_batch(batch_id: int = Query(..., description="Batch ID")):
    batch = await BatchService.get_batch_by_id(batch_id, include_listings=True)
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found")

    batch_dict = batch.model_dump()
    batch_with_user_data = await add_user_data(
        data=batch_dict, keys=["assigned_to", "created_by"], new_keys=["name"]
    )

    if batch_with_user_data.get("listings"):
        batch_with_user_data["listings"] = await add_user_data(
            data=batch_with_user_data["listings"],
            keys=["assigned_to", "created_by"],
            new_keys=["name"],
        )

    return batch_with_user_data


@router.get("/batches/filter_options", response_model=BatchFilterOptionsResponse)
async def get_batch_filter_options():
    filter_options = await BatchService.get_filter_options()
    return filter_options


@router.get("/batches", response_model=List[BatchListResponse])
async def get_batches(
    assigned_to: Optional[List[str]] = Query(
        None, description="Filter by assigned user (multi-select)"
    ),
    priority: Optional[List[str]] = Query(None, description="Filter by priority (multi-select)"),
    status: Optional[List[str]] = Query(None, description="Filter by status (multi-select)"),
    date_from: Optional[str] = Query(None, description="Filter by created date from (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="Filter by created date to (YYYY-MM-DD)"),
    search: Optional[str] = Query(None, description="Search by product ID"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
):
    date_from_obj = None
    date_to_obj = None

    if date_from:
        try:
            date_from_obj = datetime.strptime(date_from, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date_from format. Use YYYY-MM-DD")

    if date_to:
        try:
            date_to_obj = datetime.strptime(date_to, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date_to format. Use YYYY-MM-DD")

    batches, total = await BatchService.get_all_batches(
        assigned_to=assigned_to,
        priority=priority,
        status=status,
        date_from=date_from_obj,
        date_to=date_to_obj,
        search=search,
        page=page,
        page_size=page_size,
    )
    batches = await add_user_data(data=batches, keys=["assigned_to"], new_keys=["name"])

    return batches


@router.put("/batch", response_model=BatchListResponse)
async def update_batch(
    request_data: UpdateBatchRequest,
    batch_id: int = Query(..., description="Batch ID"),
):
    if request_data.assigned_to and request_data.assigned_to not in app_users:
        raise HTTPException(status_code=400, detail="Assigned user not found")

    batch = await BatchService.update_batch(batch_id, request_data)
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found")

    batch_dict = batch.model_dump()
    batch_with_user_data = await add_user_data(
        data=batch_dict, keys=["assigned_to"], new_keys=["name"]
    )

    return batch_with_user_data


@router.delete("/batch")
async def delete_batch(batch_id: int = Query(..., description="Batch ID")):
    success = await BatchService.delete_batch(batch_id)
    if not success:
        raise HTTPException(status_code=404, detail="Batch not found")

    return {"message": "Batch deleted successfully"}
