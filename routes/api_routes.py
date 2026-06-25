import logging

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from tortoise import Tortoise
from models.api_models import BatchResponse, CreateBatchRequest
from services.batch_service import BatchService
from utils.load_app_data import add_user_data
from exceptions.batch_exceptions import BatchCreationError

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/create_batch", response_model=BatchResponse, include_in_schema=False)
async def create_batch_public(request_data: CreateBatchRequest):
    created_by = "system"

    try:
        batch = await BatchService.create_batch(request_data, created_by)

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

    except BatchCreationError as e:
        logger.error(f"Batch creation failed: {e}")
        raise HTTPException(status_code=400, detail=e.to_dict())
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error creating batch: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create batch: {str(e)}")


@router.get("/export", include_in_schema=False)
async def export_public(
    type: str = Query("parent_skus", description="Export type: 'parent_skus'"),
):
    """Export product tables as CSV (publicly accessible).

    Currently supports type='parent_skus': each row is a parent SKU mapped to its
    primary (active) child SKU and that child's primary UPC.
    """
    import io
    import pandas as pd

    if type != "parent_skus":
        raise HTTPException(status_code=400, detail=f"Unsupported export type: {type}")

    try:
        conn = Tortoise.get_connection("product_db")

        query = """
            SELECT
                cp.parent_sku,
                cp.sku,
                cu.upc AS primary_upc
            FROM child_products cp
            LEFT JOIN child_upcs cu
                ON cu.child_sku = cp.sku AND cu.is_primary_upc = TRUE
            WHERE cp.is_primary = TRUE AND cp.is_active = TRUE
            ORDER BY cp.parent_sku, cp.sku
        """
        results = await conn.execute_query_dict(query)
        df = pd.DataFrame(results, columns=["parent_sku", "sku", "primary_upc"])
        df.columns = ["Parent SKU", "SKU", "Primary UPC"]

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        return Response(
            content=csv_buffer.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=parent_skus.csv"},
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error exporting parent SKUs: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
