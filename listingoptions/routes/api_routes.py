import logging
import traceback
from typing import Any, Dict, List

import pandas as pd
from fastapi import APIRouter, Body, HTTPException, Query
from listingoptions.models.api_models import (
    AddAliasResponse,
    BulkGetProductTypesByClassRequest,
    BulkGetSizingSchemesByProductTypeRequest,
    BulkSearchByNameRequest,
    BulkSearchTypeNameRequest,
    PaginatedResponse,
    ParentTypeResponse,
    SizeWithSchemesResponse,
    SizingSchemeDetailResponse,
    TableSchema,
)
from listingoptions.services.database_service import DatabaseService
from services.parent_type_service import ParentTypeService
from listingoptions.services.sizing_service import SizingService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="", tags=["API"])


@router.get("/records", response_model=PaginatedResponse, include_in_schema=False)
async def search_records(
    table_name: str = Query(..., description="Name of the table"),
    search: str = Query(..., description="Search term"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=1000, description="Number of items per page"),
):
    try:
        records, total = await DatabaseService.get_records_by_primary_column_search(
            table_name=table_name,
            search_value=search,
            page=page,
            page_size=page_size,
        )
        total_pages = (total + page_size - 1) // page_size
        return PaginatedResponse(
            items=records,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error searching records: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/brand_by_name")
async def get_brand_by_name(
    name: str = Query(..., description="Brand name to search for"),
    search_alias: bool = Query(True, description="Search in brand aliases"),
):
    try:
        (
            records,
            total,
        ) = await DatabaseService.get_records_by_primary_column_exact_search(
            table_name="brands",
            search_value=name,
            page=1,
            page_size=1,
            exclude_primary_and_alias=False,
            search_alias=search_alias,
        )

        if not records or total == 0:
            raise HTTPException(status_code=404, detail=f"Brand '{name}' not found")

        return records[0]

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting brand by name '{name}': {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/color_by_name")
async def get_color_by_name(
    name: str = Query(..., description="Color name to search for"),
    search_alias: bool = Query(True, description="Search in color aliases"),
):
    try:
        (
            records,
            total,
        ) = await DatabaseService.get_records_by_primary_column_exact_search(
            table_name="colors",
            search_value=name,
            page=1,
            page_size=1,
            exclude_primary_and_alias=False,
            search_alias=search_alias,
        )

        if not records or total == 0:
            raise HTTPException(status_code=404, detail=f"Color '{name}' not found")

        return records[0]

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting color by name '{name}': {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/type_by_name")
async def get_type_by_name(
    name: str = Query(..., description="Type name to search for"),
):
    try:
        (
            records,
            total,
        ) = await DatabaseService.get_records_by_primary_column_exact_search(
            table_name="types",
            search_value=name,
            page=1,
            page_size=1,
            exclude_primary_and_alias=False,
            search_alias=False,
        )

        if not records or total == 0:
            raise HTTPException(status_code=404, detail=f"Type '{name}' not found")

        return records[0]

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting type by name '{name}': {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/bulk_brands_by_name")
async def bulk_get_brands_by_name(
    request: BulkSearchByNameRequest,
):
    try:
        records = await DatabaseService.get_records_by_primary_column_exact_search_bulk(
            table_name="brands",
            search_values=request.names,
            search_alias=request.search_alias,
        )
        return records
    except Exception as e:
        logger.error(f"Error getting bulk brands by name: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/bulk_colors_by_name")
async def bulk_get_colors_by_name(
    request: BulkSearchByNameRequest,
):
    try:
        records = await DatabaseService.get_records_by_primary_column_exact_search_bulk(
            table_name="colors",
            search_values=request.names,
            search_alias=request.search_alias,
        )
        return records
    except Exception as e:
        logger.error(f"Error getting bulk colors by name: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/bulk_types_by_name")
async def bulk_get_types_by_name(
    request: BulkSearchTypeNameRequest,
):
    try:
        records = await DatabaseService.get_records_by_primary_column_exact_search_bulk(
            table_name="types",
            search_values=request.types,
            search_alias=False,
        )
        return records
    except Exception as e:
        logger.error(f"Error getting bulk types by name: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/get_table", include_in_schema=False)
async def export_table(
    table_name: str = Query(..., description="Name of the table to export"),
):
    try:
        schema = await DatabaseService.get_table_schema(table_name)
        if not schema:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found.")

        records = await DatabaseService.get_all_records_for_export(table_name)

        df = pd.DataFrame(records) if records else pd.DataFrame()

        column_map = {col["name"]: col["display_name"] for col in schema.column_schema}

        ordered_columns = [
            col["name"] for col in sorted(schema.column_schema, key=lambda c: c.get("order", 999))
        ]

        if table_name == "types":
            extra_cols = [
                "division",
                "dept",
                "gender",
                "class_name",
                "reporting_category",
            ]
            for col in extra_cols:
                if col not in ordered_columns:
                    ordered_columns.append(col)
                if col not in column_map:
                    column_map[col] = col.replace("_", " ").title()

        if not df.empty:
            for col in df.columns:
                if col not in ordered_columns:
                    ordered_columns.append(col)
                if col not in column_map:
                    column_map[col] = col.replace("_", " ").title()

        final_columns = [col for col in ordered_columns if col in df.columns]
        df = df[final_columns]

        df.rename(columns=column_map, inplace=True)

        return df.to_dict(orient="records")

    except HTTPException:
        raise
    except Exception:
        logger.error(f"Error exporting table {table_name}: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/product_types_by_class")
async def get_product_types_by_class(
    name: str = Query(..., description="Class name to search for"),
):
    try:
        product_types = await DatabaseService.get_product_types_by_class(name)

        if not product_types:
            raise HTTPException(
                status_code=404,
                detail=f"No product types found for class '{name}'",
            )

        return product_types

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting product types by class '{name}': {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/bulk_product_types_by_class")
async def bulk_get_product_types_by_class(
    request: BulkGetProductTypesByClassRequest,
):
    try:
        product_types = await DatabaseService.bulk_get_product_types_by_class(request.class_names)
        return product_types
    except Exception as e:
        logger.error(f"Error getting bulk product types by class: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/sizing_schemes_by_product_type",
    response_model=List[SizingSchemeDetailResponse],
)
async def get_sizing_schemes_by_product_type(
    product_type: str = Query(..., description="Product type name to search for"),
):
    try:
        sizing_schemes = await DatabaseService.get_sizing_schemes_by_product_type(product_type)
        return sizing_schemes

    except Exception as e:
        logger.error(f"Error getting sizing schemes by product type '{product_type}': {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/bulk_sizing_schemes_by_product_type")
async def bulk_get_sizing_schemes_by_product_type(
    request: BulkGetSizingSchemesByProductTypeRequest,
):
    try:
        sizing_schemes = await DatabaseService.bulk_get_sizing_schemes_by_product_type(
            request.product_types
        )
        return sizing_schemes
    except Exception as e:
        logger.error(f"Error getting bulk sizing schemes by product type: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/get_classes", response_model=List[ParentTypeResponse])
async def get_classes():
    try:
        result = await ParentTypeService.get_all_parent_types(fetch_all=True)
        return result["items"]

    except Exception as e:
        logger.error(f"Error getting classes: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/get_types")
async def get_types():
    try:
        records = await DatabaseService.get_all_types()
        return records

    except Exception as e:
        logger.error(f"Error getting types: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/get_brands")
async def get_brands():
    try:
        records = await DatabaseService.get_all_brands()
        return records

    except Exception as e:
        logger.error(f"Error getting brands: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/get_sizes", response_model=List[SizeWithSchemesResponse])
async def get_sizes():
    try:
        return await SizingService.get_all_sizes_with_schemes()

    except Exception as e:
        logger.error(f"Error getting sizes: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/tables/list", response_model=List[TableSchema], include_in_schema=False)
async def list_tables():
    try:
        tables = await DatabaseService.list_tables()
        return [
            TableSchema(
                table=table.table,
                display_name=table.display_name,
                primary_business_column=table.primary_business_column,
                column_schema=table.column_schema or [],
                list_schema=table.list_schema or [],
                list_type=table.list_type,
                created_at=table.created_at,
                updated_at=table.updated_at,
            )
            for table in tables
        ]
    except Exception as e:
        logger.error(f"Error listing tables: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")
