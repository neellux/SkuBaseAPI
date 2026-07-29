import logging
from typing import Optional

from fastapi import APIRouter, File, HTTPException, Query, UploadFile
from fastapi.responses import Response
from tortoise import Tortoise
from models.db_models import Template
from models.api_models import (
    AddProductRequest,
    AddProductResponse,
    AddSizeRequest,
    AddSizeResponse,
    CostPriceResponse,
    CheckBrandMpnResponse,
    CountriesResponse,
    CreateSkuRequest,
    CreateSkuResponse,
    BulkAddSizesRequest,
    BulkAddSizesResponse,
    ReassignAddSizeRequest,
    ReassignAddSizeResponse,
    UpdateParentProductRequest,
    UpdateParentProductResponse,
    ReassignChildRequest,
    ReassignChildResponse,
    ProductSearchResponse,
    ProductDetailsResponse,
    BulkReassignRequest,
    BulkReassignResponse,
    BulkReassignStatusResponse,
    AddUPCRequest,
    AddUPCResponse,
    SetPrimaryUPCRequest,
    SetPrimaryUPCResponse,
    DeleteUPCRequest,
    DeleteUPCResponse,
    AddKeywordRequest,
    AddKeywordResponse,
    DeleteKeywordRequest,
    DeleteKeywordResponse,
    BulkImportValidateResponse,
    BulkImportRequest,
    BulkImportResponse,
    BulkImportJobStatusResponse,
)
from services.product_service import ProductService
from services.sellercloud_service import sellercloud_service
from services.grailed_service import COUNTRY_CODE_MAP
from services import alias_bulk_import_job_service
from config import config

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/products", tags=["products"])


@router.get("/countries", response_model=CountriesResponse)
async def get_countries():
    return CountriesResponse(success=True, countries=sorted(COUNTRY_CODE_MAP.keys()))


@router.post("", response_model=AddProductResponse)
async def add_product(request: AddProductRequest):
    try:
        result = await ProductService.add_product(
            child_sku=request.child_sku,
            title=request.title,
            upc=request.upc,
            mpn=request.mpn,
            brand_code=request.brand_code,
            type_code=request.type_code,
            serial_number=request.serial_number,
            company_code=request.company_code,
        )

        if not result.get("success"):
            raise HTTPException(
                status_code=400, detail=result.get("errors", [{"error": "Failed to add product"}])
            )

        return AddProductResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding product: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/add_size", response_model=AddSizeResponse)
async def add_size_to_parent(request: AddSizeRequest):
    try:
        result = await ProductService.add_size_to_parent(
            parent_sku=request.parent_sku,
            size=request.size,
            upc=request.upc,
            cost_price=request.cost_price,
        )

        if not result.get("success"):
            if result.get("error_code") == "upc_conflict":
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": "upc_conflict",
                        "conflicting_sku": result.get("conflicting_sku"),
                        "message": result.get("error"),
                    },
                )
            status_code = 500 if result.get("sellercloud_created") else 400
            raise HTTPException(
                status_code=status_code, detail=result.get("error", "Failed to add size")
            )

        return AddSizeResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding size: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/bulk_add_sizes", response_model=BulkAddSizesResponse)
async def bulk_add_sizes(request: BulkAddSizesRequest):
    try:
        result = await ProductService.bulk_add_sizes_to_parent(
            parent_sku=request.parent_sku,
            sizes=[s.dict() for s in request.sizes],
        )
        # Per-size failures (e.g. UPC already in use) are returned in the body so
        # the dialog can show them inline; only raise for the no-detail case.
        if not result.get("success") and not result.get("failures"):
            raise HTTPException(
                status_code=400, detail=result.get("error", "Failed to add sizes")
            )
        return BulkAddSizesResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error bulk-adding sizes: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/cost_price", response_model=CostPriceResponse)
async def get_cost_price(parent_sku: str = Query(..., description="Parent product SKU")):
    try:
        result = await ProductService.get_cost_price(parent_sku)
        if not result.get("success"):
            raise HTTPException(
                status_code=400, detail=result.get("error", "Failed to fetch cost price")
            )
        return CostPriceResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching cost price: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/check_brand_mpn", response_model=CheckBrandMpnResponse)
async def check_brand_mpn(
    brand: str = Query(..., description="Brand name"),
    mpn: str = Query(..., description="Manufacturer Part Number"),
):
    try:
        result = await ProductService.check_brand_mpn(brand, mpn)
        if not result.get("success"):
            raise HTTPException(
                status_code=400,
                detail=result.get("error", "Failed to check brand and MPN"),
            )
        return CheckBrandMpnResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error checking brand+mpn: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/create_sku", response_model=CreateSkuResponse)
async def create_sku(request: CreateSkuRequest):
    try:
        result = await ProductService.create_sku_with_sizes(
            company_code=request.company_code,
            brand=request.brand,
            brand_code=request.brand_code,
            mpn=request.mpn,
            title=request.title,
            product_type=request.product_type,
            type_code=request.type_code,
            sizing_scheme=request.sizing_scheme,
            style_name=request.style_name,
            brand_color=request.brand_color,
            color=request.color,
            retail_price=request.retail_price,
            country_of_origin=request.country_of_origin,
            season=request.season,
            sizes=[s.dict() for s in request.sizes],
        )

        if not result.get("success"):
            # Validation failures (e.g. UPC already in use) are returned in the
            # body so the dialog can show them per-size on Step 3. Only raise for
            # the no-detail / internal case.
            if result.get("failures"):
                return CreateSkuResponse(**result)
            raise HTTPException(
                status_code=400, detail=result.get("error", "Failed to create SKU")
            )

        # Push the parent attributes to the new SellerCloud children, mirroring
        # update_product_info. Best-effort: a failure here is a warning, not an error.
        parent_sku = result["parent_sku"]
        changes = {
            "title": request.title,
            "product_type": request.product_type,
            "sizing_scheme": request.sizing_scheme,
            "style_name": request.style_name,
            "brand_color": request.brand_color,
            "color": request.color,
            "mpn": request.mpn,
            "brand": request.brand,
        }
        try:
            template = await Template.get_or_none(id="default")
            field_defs = template.field_definitions if template else []
            sync_result = await sellercloud_service.update_children_basic_info(
                parent_sku=parent_sku,
                changes=changes,
                field_definitions=field_defs,
            )
            if not sync_result.get("success"):
                failed = sync_result.get("failed", [])
                result["sellercloud_warning"] = (
                    f"Created; SellerCloud attribute sync failed for "
                    f"{len(failed)} child product(s)."
                )
        except Exception as e:
            logger.error(f"SellerCloud sync failed for {parent_sku}: {e}", exc_info=True)
            result["sellercloud_warning"] = "Created; SellerCloud attribute sync failed."

        return CreateSkuResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating SKU: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/reassign_add_size", response_model=ReassignAddSizeResponse)
async def reassign_add_size(request: ReassignAddSizeRequest):
    try:
        result = await ProductService.add_placeholder_size_to_parent(
            parent_sku=request.parent_sku,
            size=request.size,
        )

        if not result.get("success"):
            status_code = 500 if result.get("sellercloud_created") else 400
            raise HTTPException(
                status_code=status_code, detail=result.get("error", "Failed to add size")
            )

        return ReassignAddSizeResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding placeholder size: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/update_product_info", response_model=UpdateParentProductResponse)
async def update_product_info(
    request: UpdateParentProductRequest,
    parent_sku: str = Query(..., description="Parent product SKU"),
    skip_brand_color_update: bool = Query(False, description="Skip BRAND_COLOR alias update"),
):
    try:
        if not skip_brand_color_update and request.color and request.brand_color:
            if request.color.lower() != request.brand_color.lower():
                await sellercloud_service.validate_brand_color(request.color, request.brand_color)
                await sellercloud_service.add_color_alias(request.color, request.brand_color)

        result = await ProductService.update_parent_product(
            sku=parent_sku,
            title=request.title,
            product_type=request.product_type,
            sizing_scheme=request.sizing_scheme,
            style_name=request.style_name,
            brand_color=request.brand_color,
            color=request.color,
            mpn=request.mpn,
            brand=request.brand,
        )

        if not result.get("success"):
            raise HTTPException(status_code=404, detail=result.get("error", "Product not found"))

        changes = {
            k: v
            for k, v in {
                "title": request.title,
                "product_type": request.product_type,
                "sizing_scheme": request.sizing_scheme,
                "style_name": request.style_name,
                "brand_color": request.brand_color,
                "color": request.color,
                "mpn": request.mpn,
                "brand": request.brand,
            }.items()
            if v is not None
        }

        if changes:
            try:
                template = await Template.get_or_none(id="default")
                field_defs = template.field_definitions if template else []
                sync_result = await sellercloud_service.update_children_basic_info(
                    parent_sku=parent_sku,
                    changes=changes,
                    field_definitions=field_defs,
                )
                if not sync_result.get("success"):
                    failed = sync_result.get("failed", [])
                    result["sellercloud_warning"] = (
                        f"Updated locally; SellerCloud sync failed for "
                        f"{len(failed)} child product(s)."
                    )
            except Exception as e:
                logger.error(
                    f"SellerCloud sync failed for {parent_sku}: {e}", exc_info=True
                )
                result["sellercloud_warning"] = (
                    "Updated locally; SellerCloud sync failed."
                )

        return UpdateParentProductResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating product: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/reassign/preview")
async def get_reassign_preview(
    child_sku: str = Query(..., description="Source child SKU to reassign"),
    new_parent_sku: str = Query(..., description="Target parent SKU"),
    target_child_sku: str = Query(..., description="Target child SKU for inventory transfer"),
):
    try:
        result = await ProductService.get_reassign_preview(
            child_sku=child_sku, new_parent_sku=new_parent_sku, target_child_sku=target_child_sku
        )
        return result
    except Exception as e:
        logger.error(f"Error getting reassign preview: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to get preview")


@router.put("/reassign", response_model=ReassignChildResponse)
async def reassign_child_parent(request: ReassignChildRequest):
    try:
        result = await ProductService.reassign_child_parent(
            child_sku=request.child_sku,
            new_parent_sku=request.new_parent_sku,
            target_child_sku=request.target_child_sku,
        )

        if not result.get("success"):
            raise HTTPException(
                status_code=400, detail=result.get("message", "Failed to update parent")
            )

        return ReassignChildResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error reassigning child: {str(e)}", exc_info=True)
        raise HTTPException(status_code=400, detail="Failed to update parent")


@router.get("/product_types")
async def get_product_types():
    try:
        conn = Tortoise.get_connection("default")
        result = await conn.execute_query_dict(
            "SELECT type, sku_acronym FROM listingoptions_types ORDER BY type"
        )
        return {
            "product_types": [
                {"type": r["type"], "sku_acronym": r["sku_acronym"]} for r in result
            ]
        }
    except Exception as e:
        logger.error(f"Error fetching product types: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/colors")
async def get_colors():
    try:
        conn = Tortoise.get_connection("default")
        result = await conn.execute_query_dict("SELECT color FROM listingoptions_colors ORDER BY color")
        return {"colors": [r["color"] for r in result]}
    except Exception as e:
        logger.error(f"Error fetching colors: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/brands")
async def get_brands():
    try:
        conn = Tortoise.get_connection("default")
        result = await conn.execute_query_dict(
            "SELECT brand, sku_code FROM listingoptions_brands ORDER BY brand"
        )
        return {
            "brands": [
                {"brand": r["brand"], "sku_code": r["sku_code"]} for r in result
            ]
        }
    except Exception as e:
        logger.error(f"Error fetching brands: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/search", response_model=ProductSearchResponse)
async def search_products(
    q: str = Query(..., min_length=1, description="Search query (SKU prefix)"),
    is_parent: Optional[bool] = Query(
        None, description="Filter by True (parents) or False (children)"
    ),
    limit: int = Query(50, ge=1, le=200, description="Maximum results"),
):
    try:
        result = await ProductService.search_products(query=q, is_parent=is_parent, limit=limit)

        return ProductSearchResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error searching products: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/details", response_model=ProductDetailsResponse)
async def get_product_details(sku: str = Query(..., description="Product SKU (parent or child)")):
    try:
        result = await ProductService.get_product_details(sku)

        if not result.get("success"):
            raise HTTPException(status_code=404, detail=result.get("error", "Product not found"))

        return ProductDetailsResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting product details: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/reassign/bulk/preview")
async def get_bulk_reassign_preview(
    old_parent_sku: str = Query(..., description="Source parent SKU"),
    new_parent_sku: str = Query(..., description="Target parent SKU"),
):
    try:
        result = await ProductService.get_bulk_reassign_preview(
            old_parent_sku=old_parent_sku, new_parent_sku=new_parent_sku
        )
        return result
    except Exception as e:
        logger.error(f"Error getting bulk reassign preview: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to get bulk preview")


@router.post("/reassign/bulk", response_model=BulkReassignResponse)
async def create_bulk_reassignment(request: BulkReassignRequest):
    try:
        mappings = [
            {"old_child_sku": m.old_child_sku, "new_child_sku": m.new_child_sku}
            for m in request.mappings
        ]

        result = await ProductService.create_bulk_reassignment(
            old_parent_sku=request.old_parent_sku,
            new_parent_sku=request.new_parent_sku,
            mappings=mappings,
        )

        if not result.get("success"):
            raise HTTPException(
                status_code=400, detail=result.get("error", "Failed to create bulk reassignment")
            )

        return BulkReassignResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating bulk reassignment: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/reassign/bulk/status", response_model=BulkReassignStatusResponse)
async def get_bulk_reassignment_status(
    bulk_id: int = Query(..., description="Bulk reassignment ID")
):
    try:
        result = await ProductService.get_bulk_reassignment_status(bulk_id)

        if not result.get("success"):
            raise HTTPException(
                status_code=404, detail=result.get("error", "Bulk reassignment not found")
            )

        return BulkReassignStatusResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting bulk status: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/reassign/bulk/process")
async def process_bulk_assignment(bulk_id: int = Query(..., description="Bulk reassignment ID")):
    try:
        result = await ProductService.process_next_bulk_assignment(bulk_id)
        return result
    except Exception as e:
        logger.error(f"Error processing bulk assignment: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to process assignment")


# ============================================================================
# Export Endpoint
# ============================================================================


@router.get("/export")
async def export_products(
    type: str = Query("primary", description="Export type: 'primary', 'secondary_skus', or 'parent_skus'"),
):
    """Export products as CSV.

    'parent_skus' covers active primary children plus the secondary children
    tracked in the secondary_skus matview, with an Is Primary column telling
    them apart.
    """
    import io
    import pandas as pd

    try:
        conn = Tortoise.get_connection("product_db")

        if type == "parent_skus":
            # Two kinds of row, with deliberately different filters. Primaries:
            # active ones only. Secondaries: those tracked by the secondary_skus
            # matview, i.e. reassigned into a live primary. is_active is NOT
            # checked on that branch, because merging runs DISABLE_PRODUCT_SC
            # which sets is_active = FALSE on nearly every secondary. Matview
            # membership already implies is_primary = FALSE, so the branches
            # cannot overlap. Any predicate added below must parenthesise the OR.
            query = """
                SELECT
                    cp.parent_sku,
                    cp.sku,
                    cp.size,
                    cp.is_primary,
                    cu.upc AS primary_upc
                FROM child_products cp
                LEFT JOIN child_upcs cu
                    ON cu.child_sku = cp.sku AND cu.is_primary_upc = TRUE
                WHERE (cp.is_primary = TRUE AND cp.is_active = TRUE)
                   OR EXISTS (
                        SELECT 1 FROM secondary_skus s WHERE s.secondary_sku = cp.sku
                      )
                ORDER BY cp.parent_sku, cp.is_primary DESC, cp.sku
            """
            results = await conn.execute_query_dict(query)
            await ProductService.apply_size_sort(
                results, key_tail=lambda r: (0 if r["is_primary"] else 1, r["sku"])
            )
            df = pd.DataFrame(
                results, columns=["parent_sku", "sku", "primary_upc", "is_primary"]
            )
            df.columns = ["Parent SKU", "SKU", "Primary UPC", "Is Primary"]
            filename = "parent_skus_export.csv"
        elif type == "secondary_skus":
            query = """
                SELECT secondary_sku, current_primary_sku
                FROM secondary_skus
                ORDER BY secondary_sku
            """
            results = await conn.execute_query_dict(query)
            df = pd.DataFrame(results, columns=["secondary_sku", "current_primary_sku"])
            filename = "secondary_skus_export.csv"
        else:
            query = """
                SELECT
                    cp.sku,
                    cp.parent_sku,
                    cp.size,
                    cu.upc,
                    CASE WHEN cu.is_primary_upc THEN 'primary' ELSE 'secondary' END as type,
                    CASE WHEN cu.is_primary_upc THEN 0 ELSE 1 END as type_order
                FROM child_upcs cu
                JOIN child_products cp ON cu.child_sku = cp.sku
                WHERE cp.is_primary = TRUE AND cp.is_active = TRUE

                UNION ALL

                SELECT
                    cp.sku,
                    cp.parent_sku,
                    cp.size,
                    k as upc,
                    'keyword' as type,
                    2 as type_order
                FROM child_products cp, unnest(cp.keywords) k
                WHERE cp.is_primary = TRUE AND cp.is_active = TRUE
                    AND cp.keywords IS NOT NULL AND array_length(cp.keywords, 1) > 0

                ORDER BY sku, type_order, upc
            """
            results = await conn.execute_query_dict(query)
            await ProductService.apply_size_sort(
                results, key_tail=lambda r: (r["type_order"], r.get("upc") or "")
            )
            df = pd.DataFrame(results, columns=["sku", "upc", "type"])
            df.columns = ["SKU", "UPC", "Type"]
            filename = "products_export.csv"

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        return Response(
            content=csv_buffer.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    except Exception as e:
        logger.error(f"Error exporting products: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# UPC Management Endpoints
# ============================================================================


@router.post("/upc", response_model=AddUPCResponse)
async def add_upc(request: AddUPCRequest):
    try:
        result = await ProductService.add_upc(sku=request.sku, upc=request.upc)

        if not result.get("success"):
            error = result.get("error", "Failed to add UPC")
            status = 409 if "already exists" in error else 404 if "not found" in error else 400
            raise HTTPException(status_code=status, detail=error)

        return AddUPCResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding UPC: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/primary_upc", response_model=SetPrimaryUPCResponse)
async def set_primary_upc(request: SetPrimaryUPCRequest):
    try:
        result = await ProductService.set_primary_upc(sku=request.sku, upc=request.upc)

        if not result.get("success"):
            error = result.get("error", "Failed to update primary UPC")
            status = 400 if "EAN-8" in error else 404 if "not found" in error else 400
            raise HTTPException(status_code=status, detail=error)

        return SetPrimaryUPCResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error setting primary UPC: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/upc", response_model=DeleteUPCResponse)
async def delete_upc(request: DeleteUPCRequest):
    try:
        result = await ProductService.delete_upc(sku=request.sku, upc=request.upc)

        if not result.get("success"):
            error = result.get("error", "Failed to delete UPC")
            status = 400 if "primary" in error.lower() else 404 if "not found" in error else 400
            raise HTTPException(status_code=status, detail=error)

        return DeleteUPCResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting UPC: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# Keyword Management Endpoints
# ============================================================================


@router.post("/keyword", response_model=AddKeywordResponse)
async def add_keyword(request: AddKeywordRequest):
    try:
        result = await ProductService.add_keyword(sku=request.sku, keyword=request.keyword)

        if not result.get("success"):
            error = result.get("error", "Failed to add keyword")
            status = 409 if "already exists" in error else 404 if "not found" in error else 400
            raise HTTPException(status_code=status, detail=error)

        return AddKeywordResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding keyword: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/keyword", response_model=DeleteKeywordResponse)
async def delete_keyword(request: DeleteKeywordRequest):
    try:
        result = await ProductService.delete_keyword(sku=request.sku, keyword=request.keyword)

        if not result.get("success"):
            error = result.get("error", "Failed to delete keyword")
            status = 404 if "not found" in error else 400
            raise HTTPException(status_code=status, detail=error)

        return DeleteKeywordResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting keyword: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ============================================================================
# Bulk Import Endpoints
# ============================================================================


@router.post("/bulk_import/validate", response_model=BulkImportValidateResponse)
async def validate_bulk_import(file: UploadFile = File(...)):
    try:
        content = await file.read()
        filename = file.filename or ""
        result = await ProductService.validate_bulk_import(content, filename)
        return BulkImportValidateResponse(**result)
    except Exception as e:
        logger.error(f"Error validating bulk import: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/bulk_import", response_model=BulkImportResponse)
async def bulk_import(request: BulkImportRequest):
    try:
        import asyncio
        from services.alias_bulk_import_poller import alias_bulk_import_poller

        items = [item.model_dump() for item in request.items]
        max_tracked = config.get("bulk_import", {}).get("max_tracked_items", 50)

        # Always enqueue as a job so progress is trackable via /bulk_import/jobs/{id}.
        job_id = await alias_bulk_import_job_service.create_job(items)

        # Kick off processing in the background so there's no poller delay.
        # claim_next_job enforces single-job-at-a-time serialization (via an
        # advisory lock + processing-status check). If another job is already
        # processing, we return None and the poller will pick ours up later.
        # Always process whatever we claim (may be an older pending job ahead
        # of ours in FIFO order); the poller is a backstop.
        async def _run_job():
            claimed = None
            try:
                claimed = await alias_bulk_import_job_service.claim_next_job()
                if claimed:
                    await alias_bulk_import_poller._process_job(
                        claimed["id"], claimed["items"] or []
                    )
                    await alias_bulk_import_job_service.mark_completed(claimed["id"])
            except Exception as e:
                logger.exception(f"bulk_import background task failed (job_id={job_id})")
                if claimed:
                    try:
                        await alias_bulk_import_job_service.mark_failed(
                            claimed["id"], f"{type(e).__name__}: {e}"
                        )
                    except Exception:
                        pass

        asyncio.create_task(_run_job())

        logger.info(
            f"Bulk import with {len(items)} items enqueued as job {job_id} "
            f"(threshold max_tracked={max_tracked} — UI decides poll vs fire-and-forget)"
        )
        return BulkImportResponse(
            success=True,
            total_items=len(items),
            successful_count=0,
            failed_count=0,
            results=[],
            async_job=True,
            job_id=job_id,
        )
    except Exception as e:
        logger.error(f"Error processing bulk import: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/bulk_import/jobs", response_model=BulkImportJobStatusResponse)
async def get_bulk_import_job(id: int = Query(..., description="Bulk import job id")):
    try:
        job = await alias_bulk_import_job_service.get_job(id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job {id} not found")

        def _iso(ts):
            return ts.isoformat() if ts else None

        return BulkImportJobStatusResponse(
            job_id=job["id"],
            status=job["status"],
            total_items=job["total_items"],
            processed_items=job["processed_items"],
            successful_count=job["successful_count"],
            failed_count=job["failed_count"],
            results=job.get("results") or [],
            error_message=job.get("error_message"),
            created_at=_iso(job.get("created_at")),
            started_at=_iso(job.get("started_at")),
            completed_at=_iso(job.get("completed_at")),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching bulk import job {id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

