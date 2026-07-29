import logging
import secrets

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import Response
from tortoise import Tortoise
from config import API_KEY
from models.api_models import BatchResponse, CreateBatchRequest
from services.batch_service import BatchService
from services.product_service import ProductService
from utils.load_app_data import add_user_data
from exceptions.batch_exceptions import BatchCreationError

logger = logging.getLogger(__name__)

router = APIRouter()


async def require_api_key(x_api_key: str = Header(None, alias="X-API-KEY")):
    """Validate the X-API-KEY header against the configured API key.

    Fails closed: if no API key is configured, every request is rejected.
    """
    if not API_KEY or not x_api_key or not secrets.compare_digest(x_api_key, API_KEY):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


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


async def _attach_class_names(rows):
    """Set row['class_name'] on each row, resolved from lux_skubase by product_type.

    parent_products.product_type equals listingoptions_types.type; the parent
    class hierarchy (class_name) hangs off listingoptions_types.parent_id ->
    listingoptions_types_parents. The map is small (a few hundred rows), so we
    fetch it once and map in memory. Unknown/NULL product_type resolves to None.
    """
    if not rows:
        return
    conn = Tortoise.get_connection("default")
    mapping = await conn.execute_query_dict(
        """
        SELECT t.type AS product_type, p.class_name
        FROM listingoptions_types t
        LEFT JOIN listingoptions_types_parents p ON p.id = t.parent_id
        """
    )
    class_by_type = {r["product_type"]: r["class_name"] for r in mapping}
    for row in rows:
        row["class_name"] = class_by_type.get(row.get("product_type"))


# Registry of supported public CSV exports. Each SQL query must expose
# parent_sku and size so ProductService.apply_size_sort can order rows by
# canonical size within each parent. raw_columns is the key order pulled from
# the result dicts; display_columns is the matching header row written to CSV.
# key_tail is optional and is passed straight to apply_size_sort as a final
# tiebreak for rows that share a parent and size.
_EXPORTS = {
    "parent_skus": {
        # Two kinds of row, with deliberately different filters. Primaries: active
        # ones only. Secondaries: those tracked by the secondary_skus matview,
        # i.e. reassigned into a live primary. is_active is NOT checked on that
        # branch, because merging runs DISABLE_PRODUCT_SC which sets
        # is_active = FALSE on nearly every secondary. Matview membership already
        # implies is_primary = FALSE, so the branches cannot overlap. Any
        # predicate added below must parenthesise the OR.
        "query": """
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
        """,
        "raw_columns": ["parent_sku", "sku", "primary_upc", "is_primary"],
        "display_columns": ["Parent SKU", "SKU", "Primary UPC", "Is Primary"],
        "filename": "parent_skus.csv",
        "key_tail": lambda r: (0 if r["is_primary"] else 1, r["sku"]),
    },
    # Every child SKU in the internal catalog (active and inactive, primary and
    # secondary) joined to its parent's product info.
    "product_info": {
        "query": """
            SELECT
                cp.sku,
                cp.parent_sku,
                pp.title,
                pp.style_name,
                pp.brand,
                pp.product_type,
                cp.size,
                pp.sizing_scheme,
                cp.is_active
            FROM child_products cp
            JOIN parent_products pp ON pp.sku = cp.parent_sku
            ORDER BY cp.parent_sku, cp.sku
        """,
        "raw_columns": [
            "sku", "parent_sku", "title", "style_name", "brand",
            "product_type", "class_name", "size", "sizing_scheme", "is_active",
        ],
        "display_columns": [
            "SKU", "Parent SKU", "Title", "Style Name", "Brand",
            "Type", "General Type", "Size", "Sizing Scheme", "Active",
        ],
        "filename": "luxinternal_products.csv",
        "enrich": _attach_class_names,
    },
}


@router.get("/export", include_in_schema=False, dependencies=[Depends(require_api_key)])
async def export_public(
    type: str = Query(
        "parent_skus", description="Export type: 'parent_skus' or 'product_info'"
    ),
):
    """Export product tables as CSV (publicly accessible).

    Supported types:
    - 'parent_skus': each row is a parent SKU mapped to one of its child SKUs
      and that child's primary UPC. Covers active primary children plus the
      secondary children tracked in the secondary_skus matview, with an
      Is Primary column telling them apart. Secondaries have no UPC, because
      merging moves it onto the primary they were reassigned into.
    - 'product_info': every child SKU in the internal catalog (active and
      inactive) joined to its parent's product info, with columns SKU, Parent
      SKU, Title, Style Name, Brand, Type, General Type, Size, Sizing Scheme.
    """
    import io
    import pandas as pd

    export = _EXPORTS.get(type)
    if export is None:
        raise HTTPException(status_code=400, detail=f"Unsupported export type: {type}")

    try:
        conn = Tortoise.get_connection("product_db")

        results = await conn.execute_query_dict(export["query"])
        enrich = export.get("enrich")
        if enrich is not None:
            await enrich(results)
        await ProductService.apply_size_sort(
            results, key_tail=export.get("key_tail", lambda r: ())
        )
        df = pd.DataFrame(results, columns=export["raw_columns"])
        df.columns = export["display_columns"]

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        return Response(
            content=csv_buffer.getvalue(),
            media_type="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="{export["filename"]}"'
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error exporting '{type}': {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
