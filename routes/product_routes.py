import logging
from typing import Optional

from fastapi import APIRouter, File, HTTPException, Query, UploadFile
from fastapi.responses import Response
from tortoise import Tortoise
from models.api_models import (
    AddProductRequest,
    AddProductResponse,
    AddSizeRequest,
    AddSizeResponse,
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
)
from services.product_service import ProductService
from services.sellercloud_service import sellercloud_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/products", tags=["products"])


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
        result = await conn.execute_query_dict("SELECT type FROM listingoptions_types")
        return {"product_types": [r["type"] for r in result]}
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
        result = await conn.execute_query_dict("SELECT brand FROM listingoptions_brands")
        return {"brands": [r["brand"] for r in result]}
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
    type: str = Query("primary", description="Export type: 'primary' or 'secondary_skus'"),
):
    """Export products as CSV. Type 'primary' exports UPCs/keywords, 'secondary_skus' exports secondary-to-primary mappings."""
    import io
    import pandas as pd

    try:
        conn = Tortoise.get_connection("product_db")

        if type == "secondary_skus":
            query = """
                SELECT
                    sec.sku as old_sku,
                    pri.sku as new_sku
                FROM child_products sec
                JOIN child_products pri
                    ON pri.parent_sku = sec.parent_sku
                    AND pri.size = sec.size
                    AND pri.is_primary = TRUE
                WHERE sec.is_primary = FALSE
                ORDER BY sec.sku
            """
            results = await conn.execute_query_dict(query)
            df = pd.DataFrame(results, columns=["old_sku", "new_sku"])
            filename = "secondary_skus_export.csv"
        else:
            query = """
                SELECT
                    cp.sku,
                    cu.upc,
                    CASE WHEN cu.is_primary_upc THEN 'primary' ELSE 'secondary' END as type,
                    CASE WHEN cu.is_primary_upc THEN 0 ELSE 1 END as type_order
                FROM child_upcs cu
                JOIN child_products cp ON cu.child_sku = cp.sku
                WHERE cp.is_primary = TRUE AND cp.is_active = TRUE

                UNION ALL

                SELECT
                    cp.sku,
                    k as upc,
                    'keyword' as type,
                    2 as type_order
                FROM child_products cp, unnest(cp.keywords) k
                WHERE cp.is_primary = TRUE AND cp.is_active = TRUE
                    AND cp.keywords IS NOT NULL AND array_length(cp.keywords, 1) > 0

                ORDER BY sku, type_order, upc
            """
            results = await conn.execute_query_dict(query)
            df = pd.DataFrame(results, columns=["sku", "upc", "type", "type_order"])
            df = df.drop(columns=["type_order"])
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
        items = [item.model_dump() for item in request.items]
        result = await ProductService.process_bulk_import(items)
        return BulkImportResponse(**result)
    except Exception as e:
        logger.error(f"Error processing bulk import: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

