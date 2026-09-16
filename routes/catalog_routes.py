"""The catalog browser.

GET  /catalog/products      one page of parent products with value, coverage and state
GET  /catalog/summary       counts for the header and the select-all banner
POST /catalog/batch/create  create a batch from checked rows or from "all matching"

Query parameters only. The POST also requires X-Requested-With: SkuBase (see
batch_generation_routes). Registered with the auth service: both GETs under view_batches,
the POST under manage_batches.
"""

import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import ValidationError

from models.api_models import (
    CatalogBatchCreateRequest,
    CatalogBatchCreateResponse,
    CatalogFilters,
    CatalogRow,
    CatalogSummaryResponse,
)
from routes.batch_generation_routes import require_skubase_client
from services import catalog_service
from utils.load_app_data import app_users

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/catalog", tags=["catalog"])


async def catalog_filters(
    search: Optional[str] = Query(None, max_length=100, description="SKU, child SKU, MPN or title"),
    coverage_platform: Optional[List[str]] = Query(
        None,
        description="Platforms to filter coverage on (repeat the key for several); every one must be in a chosen state",
    ),
    coverage_state: Optional[List[str]] = Query(
        None, description="listed, in_progress, failed, none or excluded (repeat the key for several)"
    ),
    value_min: Optional[float] = Query(None, ge=0),
    value_max: Optional[float] = Query(None, ge=0),
    in_stock: bool = Query(False, description="Only products with on-hand quantity"),
    unvalued: bool = Query(False, description="Only products with no daily value yet"),
    listing_state: Optional[str] = Query(
        None, description="never_listed, in_open_batch or not_in_open_batch"
    ),
    listing_status: Optional[str] = Query(
        None, description="images_pending, platforms_pending or listed"
    ),
    has_images: Optional[bool] = Query(None, description="Photography has images for the parent"),
    company: Optional[List[int]] = Query(
        None,
        description="SellerCloud company codes (repeat the key for several); a product matches any of them",
    ),
    sort: str = Query("value_desc", description="value_desc, newest or sku"),
) -> CatalogFilters:
    try:
        filters = CatalogFilters(
            search=(search or "").strip() or None,
            coverage_platform=coverage_platform or [],
            coverage_state=coverage_state or [],
            value_min=value_min,
            value_max=value_max,
            in_stock=in_stock,
            unvalued=unvalued,
            listing_state=listing_state,
            listing_status=listing_status,
            has_images=has_images,
            company=company or [],
            sort=sort,
        )
    except ValidationError:
        raise HTTPException(status_code=400, detail="Invalid catalog filter")
    await catalog_service.require_known_platforms(filters.coverage_platform)
    return filters


@router.get("/products", response_model=List[CatalogRow])
async def get_catalog_products(
    filters: CatalogFilters = Depends(catalog_filters),
    # Offset paging over ~42k parents at up to 100 per page; the cap only stops absurd offsets.
    page: int = Query(1, ge=1, le=5000),
    page_size: int = Query(50, ge=1, le=100),
):
    return await catalog_service.get_page(filters, page, page_size)


@router.get("/summary", response_model=CatalogSummaryResponse)
async def get_catalog_summary(filters: CatalogFilters = Depends(catalog_filters)):
    return await catalog_service.get_summary(filters)


@router.post(
    "/batch/create",
    response_model=CatalogBatchCreateResponse,
    dependencies=[Depends(require_skubase_client)],
)
async def create_catalog_batch(body: CatalogBatchCreateRequest, request: Request):
    created_by = request.state.user["id"]
    if created_by not in app_users:
        raise HTTPException(status_code=400, detail="Creating user not found")
    if body.assigned_to and body.assigned_to not in app_users:
        raise HTTPException(status_code=400, detail="Assigned user not found")
    if body.selection.mode == "filter":
        await catalog_service.require_known_platforms(body.selection.filters.coverage_platform)

    return await catalog_service.create_batch(
        body.selection,
        expected_count=body.expected_count,
        comment=body.comment,
        assigned_to=body.assigned_to,
        priority=body.priority,
        created_by=created_by,
    )
