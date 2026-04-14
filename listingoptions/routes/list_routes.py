from fastapi import APIRouter, HTTPException, Query, UploadFile, File
from fastapi.responses import StreamingResponse
from typing import List, Optional, Dict, Any
from uuid import UUID
from listingoptions.models.api_models import (
    DefaultListEntry,
    SizingListEntry,
    DefaultListResponse,
    SizingListResponse,
    SuccessResponse,
    PaginatedResponse,
    DefaultListInternalValuesUpdate,
)
from listingoptions.services.database_service import DatabaseService
import logging
import json
import csv
import io
import re
import pandas as pd
from tortoise.exceptions import IntegrityError
import traceback

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/lists", tags=["lists"])


@router.get("/records", response_model=PaginatedResponse)
async def get_list_records(
    table_name: str = Query(..., description="Name of the table"),
    list_type: str = Query(..., description="Type of list (default or sizing)"),
    platform_id: Optional[str] = Query(None, description="Platform ID to filter by"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=1000, description="Number of items per page"),
    search: Optional[str] = Query(None, description="Search term"),
    all: bool = Query(False, description="Return all records without pagination"),
    sizing_type: Optional[str] = Query(None, description="Sizing type filter (sizes table only)"),
):
    try:
        if list_type not in ["default", "sizing"]:
            raise HTTPException(status_code=400, detail="list_type must be 'default' or 'sizing'")

        effective_page = 1 if all else page
        effective_page_size = 1000000 if all else page_size

        records, total = await DatabaseService.get_list_records(
            table_name=table_name,
            list_type=list_type,
            platform_id=platform_id,
            page=effective_page,
            page_size=effective_page_size,
            search=search,
            sizing_type=sizing_type,
        )

        total_pages = 1 if all else (total + page_size - 1) // page_size

        return PaginatedResponse(
            items=records,
            total=total,
            page=effective_page,
            page_size=effective_page_size,
            total_pages=total_pages,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting list records: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/default", response_model=SuccessResponse)
async def add_default_list_entry(
    entry: DefaultListEntry,
    table_name: str = Query(..., description="Name of the table"),
):
    try:
        entry_id = await DatabaseService.add_default_list_entry(table_name, entry)
        return SuccessResponse(message="Platform mapping created successfully")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error adding default list entry: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/sizing", response_model=SuccessResponse)
async def add_sizing_list_entry(
    entry: SizingListEntry,
    table_name: str = Query(..., description="Name of the table"),
):
    try:
        entry_id = await DatabaseService.add_sizing_list_entry(table_name, entry)
        return SuccessResponse(message="Sizing mapping created successfully")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error adding sizing list entry: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/default", response_model=SuccessResponse)
async def update_default_list_entry(
    entry: DefaultListEntry,
    table_name: str = Query(..., description="Name of the table"),
    entry_id: UUID = Query(..., description="ID of the entry to update"),
):
    try:
        await DatabaseService.update_default_list_entry(table_name, entry_id, entry)
        return SuccessResponse(message="Platform mapping updated successfully")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error updating default list entry: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/default/internal_values", response_model=SuccessResponse)
async def sync_default_internal_values(
    payload: DefaultListInternalValuesUpdate,
    table_name: str = Query(..., description="Name of the table"),
):
    try:
        conflicts = await DatabaseService.check_internal_value_conflicts(
            table_name=table_name,
            platform_id=payload.platform_id,
            platform_value=payload.platform_value,
            internal_values=payload.internal_values,
            sizing_type=payload.sizing_type,
        )

        if conflicts and not payload.confirmed:
            raise HTTPException(
                status_code=409,
                detail=conflicts,
            )

        added, deleted, _ = await DatabaseService.sync_default_list_internal_values(
            table_name=table_name,
            platform_id=payload.platform_id,
            platform_value=payload.platform_value,
            internal_values=payload.internal_values,
            force=payload.confirmed,
            sizing_type=payload.sizing_type,
        )
        return SuccessResponse(message="Platform mapping updated successfully")
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except IntegrityError as e:
        logger.error(f"Integrity error during sync: {str(e)}")
        raise HTTPException(
            status_code=409,
            detail="A database integrity error occurred. This may be due to a race condition or invalid data.",
        )
    except Exception as e:
        logger.error(f"Error syncing internal values: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/sizing", response_model=SuccessResponse)
async def update_sizing_list_entry(
    entry: SizingListEntry,
    table_name: str = Query(..., description="Name of the table"),
    entry_id: UUID = Query(..., description="ID of the entry to update"),
):
    try:
        await DatabaseService.update_sizing_list_entry(table_name, entry_id, entry)
        return SuccessResponse(message="Sizing mapping updated successfully")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error updating sizing list entry: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/entry", response_model=SuccessResponse)
async def delete_list_entry(
    table_name: str = Query(..., description="Name of the table"),
    list_type: str = Query(..., description="Type of list (default or sizing)"),
    entry_id: UUID = Query(..., description="ID of the entry to delete"),
):
    try:
        if list_type not in ["default", "sizing"]:
            raise HTTPException(status_code=400, detail="list_type must be 'default' or 'sizing'")

        await DatabaseService.delete_list_entry(table_name, list_type, entry_id)
        return SuccessResponse(message="Platform mapping deleted successfully")
    except Exception as e:
        logger.error(f"Error deleting list entry: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/platforms", response_model=List[Dict[str, Any]])
async def get_platforms_for_table(
    table_name: str = Query(..., description="Name of the table"),
):
    try:
        platforms = await DatabaseService.get_platforms_for_table(table_name)
        return platforms
    except Exception as e:
        logger.error(f"Error getting platforms for table: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/entry", response_model=Dict[str, Any])
async def get_list_entry(
    table_name: str = Query(..., description="Name of the table"),
    list_type: str = Query(..., description="Type of list (default or sizing)"),
    entry_id: UUID = Query(..., description="ID of the entry"),
):
    try:
        if list_type not in ["default", "sizing"]:
            raise HTTPException(status_code=400, detail="list_type must be 'default' or 'sizing'")

        entry = await DatabaseService.get_list_entry_by_id(table_name, list_type, entry_id)
        if not entry:
            raise HTTPException(status_code=404, detail="List entry not found")

        return entry
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting list entry: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/bulk_import", response_model=SuccessResponse)
async def bulk_import_list_entries(
    file: UploadFile = File(...),
    table_name: str = Query(..., description="Name of the table"),
    list_type: str = Query(..., description="Type of list (default or sizing)"),
    platform_id: Optional[str] = Query(
        None, description="Platform ID for single-platform CSV upload"
    ),
):
    try:
        if list_type not in ["default", "sizing"]:
            raise HTTPException(status_code=400, detail="list_type must be 'default' or 'sizing'")

        if platform_id and list_type == "sizing":
            raise HTTPException(
                status_code=400,
                detail="Single-column 'Values' upload is only supported for 'default' list types, not 'sizing'.",
            )

        content = await file.read()
        entries = []
        is_single_platform_value_upload = False

        if file.filename.endswith(".csv"):
            try:
                csv_content = content.decode("utf-8-sig")
                csv_reader = csv.DictReader(io.StringIO(csv_content))

                if platform_id:
                    if not csv_reader.fieldnames or "Values" not in csv_reader.fieldnames:
                        raise HTTPException(
                            status_code=400,
                            detail="CSV for single platform upload must have a 'Values' header.",
                        )
                    if len(csv_reader.fieldnames) > 1:
                        raise HTTPException(
                            status_code=400,
                            detail="CSV for single platform upload must contain only one column: 'Values'.",
                        )
                    is_single_platform_value_upload = True

                entries = list(csv_reader)

            except Exception as e:
                logger.error(f"Error processing CSV file: {str(e)}")
                raise HTTPException(
                    status_code=400, detail=f"Invalid CSV format or content: {str(e)}"
                )
        elif file.filename.endswith(".json"):
            if platform_id:
                raise HTTPException(
                    status_code=400,
                    detail="platform_id parameter is only for CSV 'Values' upload.",
                )
            try:
                data = json.loads(content.decode("utf-8"))
                entries = data if isinstance(data, list) else [data]
            except json.JSONDecodeError:
                raise HTTPException(status_code=400, detail="Invalid JSON format")
        else:
            raise HTTPException(status_code=400, detail="File must be JSON or CSV format")

        if not entries:
            raise HTTPException(
                status_code=400, detail="No entries found in the file or file is empty."
            )

        if not is_single_platform_value_upload:
            required_fields = {
                "default": [
                    "platform_value",
                    "platform",
                    "primary_table_column",
                ],
                "sizing": ["sizing_scheme", "platform_value", "platform", "value"],
            }
            for entry in entries:
                missing_fields = [
                    field
                    for field in required_fields[list_type]
                    if field not in entry or entry[field] is None
                ]
                if missing_fields:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Missing or null required fields for some entries: {missing_fields}. Entry: {entry}",
                    )

        imported_count = await DatabaseService.bulk_import_list_entries(
            table_name=table_name,
            list_type=list_type,
            entries=entries,
            platform_id_for_upload=platform_id if is_single_platform_value_upload else None,
        )

        return SuccessResponse(message=f"Successfully imported platform mappings")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error during bulk import: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.post("/create_mapping_table", response_model=SuccessResponse)
async def create_mapping_table(
    table_name: str = Query(..., description="Name of the table"),
    list_type: str = Query(..., description="Type of list (default or sizing)"),
):
    try:
        if list_type not in ["default", "sizing"]:
            raise HTTPException(status_code=400, detail="list_type must be 'default' or 'sizing'")
        success = await DatabaseService.create_mapping_table(table_name, list_type)
        if success:
            return SuccessResponse(
                message=f"{list_type.capitalize()} mapping table created successfully"
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating mapping table: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/export")
async def export_list_entries(
    table_name: str = Query(..., description="Name of the table"),
    list_type: str = Query(..., description="Type of list (default or sizing)"),
    platform_id: str = Query(..., description="Platform ID to export"),
):
    try:
        if list_type not in ["default", "sizing"]:
            raise HTTPException(status_code=400, detail="list_type must be 'default' or 'sizing'")

        platform = await DatabaseService.get_platform_by_id(platform_id)
        platform_name = platform.name if platform else platform_id

        records = await DatabaseService.get_all_list_records_for_export(
            table_name=table_name,
            list_type=list_type,
            platform_id=platform_id,
        )

        df = pd.DataFrame(records) if records else pd.DataFrame()

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            if list_type == "default":
                sheet_name = platform_name[:31]
            else:
                sheet_name = f"{platform_name}_{list_type}"[:31]
            df.to_excel(writer, index=False, sheet_name=sheet_name)

        output.seek(0)

        if list_type == "default":
            filename = f"{table_name}_{platform_name}_export.xlsx"
        else:
            filename = f"{table_name}_{platform_name}_{list_type}_export.xlsx"

        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}

        return StreamingResponse(
            output,
            headers=headers,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error exporting list entries: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")
