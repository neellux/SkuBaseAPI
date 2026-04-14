from fastapi import APIRouter, HTTPException, Query, Depends, Request, Path, Body
from typing import List, Optional, Dict, Any, Tuple
from listingoptions.models.api_models import (
    CreateTableRequest,
    AddColumnRequest,
    UpdateColumnRequest,
    TableSchema,
    RecordData,
    SuccessResponse,
    ErrorResponse,
    PaginatedResponse,
    FuzzyCheckRequest,
    FuzzyCheckResponse,
    ListSchemaDefinition,
    ListSchemaDefinitionUpdate,
    DefaultListEntry,
)
import traceback
import time
from listingoptions.services.database_service import DatabaseService
from listingoptions.models.db_models import Platform
from fastapi.responses import StreamingResponse
import pandas as pd
import io


import logging
import uuid
import re
from tortoise import Tortoise

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/tables", tags=["tables"])


@router.post("/create", response_model=SuccessResponse)
async def create_table(request: CreateTableRequest):
    try:
        await DatabaseService.create_table(request)
        return SuccessResponse(message=f"Table {request.table_name} created successfully")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating table: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/add_column", response_model=SuccessResponse)
async def add_column(request: AddColumnRequest):
    try:
        await DatabaseService.add_column(request)
        return SuccessResponse(
            message=f"Column {request.column.name} added to table {request.table_name}"
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error adding column: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/update_column", response_model=SuccessResponse)
async def update_column(request: UpdateColumnRequest):
    try:
        await DatabaseService.update_column(request)
        return SuccessResponse(
            message=f"Column {request.column_name} in table {request.table_name} updated successfully"
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error updating column: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/reorder_columns", response_model=SuccessResponse)
async def reorder_columns(
    table_name: str = Query(..., description="Name of the table"),
    ordered_column_names: List[str] = Body(
        ..., description="List of column names in the desired order"
    ),
):
    try:
        await DatabaseService.reorder_columns(table_name, ordered_column_names)
        return SuccessResponse(message=f"Columns for table {table_name} have been reordered.")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error reordering columns: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/list", response_model=List[TableSchema])
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


@router.get("/schema", response_model=TableSchema)
async def get_table_schema(
    request: Request,
    table_name: str = Query(..., description="Name of the table"),
):
    try:
        permissions = request.state.user["permissions"]
        db_schema = await DatabaseService.get_table_schema(table_name)
        if not db_schema:
            raise HTTPException(status_code=404, detail=f"Table {table_name} not found")
        sorted_columns = sorted(db_schema.column_schema or [], key=lambda c: c.get("order", 999))

        all_platforms = await DatabaseService.get_all_platforms()
        platform_options = {p.id: p.name for p in all_platforms}
        platform_any_of = [{"const": p.id, "title": p.name} for p in all_platforms]

        json_schema_props = {}
        ui_schema_props = {}
        required_fields = []
        primary_col_name_for_grid: Optional[str] = None
        first_platform_field_name_for_grid: Optional[str] = None

        primary_col = None
        primary_col_display_name_text = db_schema.primary_business_column

        for col in sorted_columns:
            if col["name"] == db_schema.primary_business_column:
                primary_col = col
                primary_col_display_name_text = col.get(
                    "display_name", db_schema.primary_business_column
                )
                break

        if primary_col and primary_col.get("display_in_form", True):
            primary_col_name_for_grid = primary_col["name"]
            prop = {
                "title": primary_col["display_name"],
            }
            ui_prop = {}
            if "edit_record_names" not in permissions:
                ui_prop["ui:disabled"] = True

            if primary_col["type"] == "text":
                prop["type"] = "string"
                if primary_col.get("options"):
                    if primary_col.get("multiselect"):
                        prop["type"] = "array"
                        prop["items"] = {
                            "type": "string",
                            "enum": primary_col["options"],
                        }

                    else:
                        prop["enum"] = primary_col["options"]
                        ui_prop["ui:widget"] = "select"
                else:
                    if primary_col["min"] is not None:
                        prop["minLength"] = int(primary_col["min"])
                    if primary_col["max"] is not None:
                        prop["maxLength"] = int(primary_col["max"])
                    if primary_col["regex"]:
                        prop["pattern"] = primary_col["regex"]
                        if primary_col.get("regex_error_message"):
                            ui_prop.setdefault("ui:options", {})["errorMessage"] = primary_col[
                                "regex_error_message"
                            ]
            elif primary_col["type"] == "number":
                prop["type"] = "number"
                if primary_col.get("options"):
                    try:
                        prop["enum"] = [float(o) for o in primary_col.get("options", [])]
                        ui_prop["ui:widget"] = "select"
                    except (ValueError, TypeError):
                        logger.warning(
                            f"Could not convert options to float for column {primary_col['name']} in table {table_name}"
                        )
                else:
                    if primary_col["min"] is not None:
                        prop["minimum"] = float(primary_col["min"])
                    if primary_col["max"] is not None:
                        prop["maximum"] = float(primary_col["max"])
            elif primary_col["type"] == "bool":
                prop["type"] = "boolean"
                ui_prop["ui:widget"] = "checkbox"
            elif primary_col["type"] == "platform_list":
                prop["type"] = "array"
                prop["items"] = {
                    "type": "string",
                    "anyOf": platform_any_of,
                }
                prop["uniqueItems"] = True
                ui_prop["ui:widget"] = "checkboxes"
            elif primary_col["type"] == "text_list":
                prop["type"] = "array"
                prop["uniqueItems"] = True

                prop["items"] = {"type": "string"}

                if col.get("options"):
                    prop["items"]["enum"] = col["options"]

                else:
                    if col["min"] is not None:
                        prop["items"]["minLength"] = int(col["min"])
                    if col["max"] is not None:
                        prop["items"]["maxLength"] = int(col["max"])
                    if col["regex"]:
                        prop["items"]["pattern"] = col["regex"]
                        if col.get("regex_error_message"):
                            ui_prop.setdefault("ui:options", {})["errorMessage"] = col[
                                "regex_error_message"
                            ]
                    ui_prop["ui:widget"] = "TagsWidget"

            if primary_col["default"] is not None:
                prop["default"] = primary_col["default"]

            json_schema_props[primary_col["name"]] = prop
            if ui_prop:
                ui_schema_props[primary_col["name"]] = ui_prop

            if primary_col["is_required"]:
                required_fields.append(primary_col["name"])
        else:
            logger.warning(
                f"Primary business column '{db_schema.primary_business_column}' not found in column_schema for table '{table_name}'. Using it as display name fallback."
            )

        enabled_platform_ids = []
        enabled_list_schemas = []
        for list_schema in db_schema.list_schema or []:
            if list_schema.get("list_type") == "default" and list_schema.get("enabled", True):
                enabled_platform_ids.append(list_schema["platform_id"])
                enabled_list_schemas.append(list_schema)

        for list_schema in enabled_list_schemas:
            platform_id = list_schema["platform_id"]
            platform_name = platform_options.get(platform_id, platform_id)
            platform_values = []

            field_name = (
                f"platform_mapping_for_{platform_id}_of_{db_schema.primary_business_column}"
            )

            field_definition = {
                "type": "string",
                "title": f"{platform_name} Value for {primary_col_display_name_text}",
            }

            if list_schema.get("min_length"):
                field_definition["minLength"] = list_schema["min_length"]
            if list_schema.get("max_length"):
                field_definition["maxLength"] = list_schema["max_length"]
            if list_schema.get("regex"):
                field_definition["pattern"] = list_schema["regex"]

            json_schema_props[field_name] = field_definition

            current_field_ui_props = {
                "ui:widget": "PlatformAutocomplete",
                "ui:options": {
                    "freeSolo": True,
                    "suggestions": platform_values if platform_values else [],
                },
                "ui:placeholder": f"Select or enter {platform_name} value",
            }
            ui_schema_props[field_name] = current_field_ui_props

            if not first_platform_field_name_for_grid:
                first_platform_field_name_for_grid = field_name

            required_fields.append(field_name)

        for col in sorted_columns:
            if col["name"] == db_schema.primary_business_column:
                continue

            if not col.get("display_in_form", True):
                continue

            prop = {
                "title": col["display_name"],
            }
            ui_prop = {}

            if col["type"] == "text":
                prop["type"] = "string"
                if col.get("options"):
                    if col.get("multiselect"):
                        prop["type"] = "array"
                        prop["items"] = {"type": "string", "enum": col["options"]}
                        ui_prop["ui:widget"] = "checkboxes"
                    else:
                        prop["enum"] = col["options"]
                        ui_prop["ui:widget"] = "select"
                else:
                    if col["min"] is not None:
                        prop["minLength"] = int(col["min"])
                    if col["max"] is not None:
                        prop["maxLength"] = int(col["max"])
                    if col["regex"]:
                        prop["pattern"] = col["regex"]
                        if col.get("regex_error_message"):
                            ui_prop.setdefault("ui:options", {})["errorMessage"] = col[
                                "regex_error_message"
                            ]
            elif col["type"] == "number":
                prop["type"] = "number"
                if col.get("options"):
                    try:
                        prop["enum"] = [float(o) for o in col.get("options", [])]
                        ui_prop["ui:widget"] = "select"
                    except (ValueError, TypeError):
                        logger.warning(
                            f"Could not convert options to float for column {col['name']} in table {table_name}"
                        )
                else:
                    if col["min"] is not None:
                        prop["minimum"] = float(col["min"])
                    if col["max"] is not None:
                        prop["maximum"] = float(col["max"])
            elif col["type"] == "bool":
                prop["type"] = "boolean"
                ui_prop["ui:widget"] = "checkbox"
            elif col["type"] == "platform_list":
                prop["type"] = "array"
                prop["items"] = {
                    "type": "string",
                    "anyOf": platform_any_of,
                }
                prop["uniqueItems"] = True
                ui_prop["ui:widget"] = "checkboxes"
            elif col["type"] == "text_list":
                prop["type"] = "array"
                prop["uniqueItems"] = True

                prop["items"] = {"type": "string"}

                if col.get("options"):
                    prop["items"]["enum"] = col["options"]

                else:
                    if col["min"] is not None:
                        prop["items"]["minLength"] = int(col["min"])
                    if col["max"] is not None:
                        prop["items"]["maxLength"] = int(col["max"])
                    if col["regex"]:
                        prop["items"]["pattern"] = col["regex"]
                        if col.get("regex_error_message"):
                            ui_prop.setdefault("ui:options", {})["errorMessage"] = col[
                                "regex_error_message"
                            ]
                    ui_prop["ui:widget"] = "TagsWidget"

            if col["default"] is not None:
                prop["default"] = col["default"]

            json_schema_props[col["name"]] = prop
            if ui_prop:
                ui_schema_props[col["name"]] = ui_prop

            if col["is_required"]:
                required_fields.append(col["name"])

        if primary_col_name_for_grid and first_platform_field_name_for_grid:
            ui_schema_props.setdefault(primary_col_name_for_grid, {})["ui:xs"] = 6

            ui_schema_props.setdefault(first_platform_field_name_for_grid, {})["ui:xs"] = 6

        final_json_schema = {
            "type": "object",
            "title": db_schema.display_name or db_schema.table,
            "properties": json_schema_props,
            "required": required_fields,
        }

        final_ui_schema = ui_schema_props

        return TableSchema(
            table=db_schema.table,
            display_name=db_schema.display_name,
            primary_business_column=db_schema.primary_business_column,
            column_schema=sorted_columns,
            list_schema=db_schema.list_schema or [],
            list_type=db_schema.list_type,
            json_schema=final_json_schema,
            ui_schema=final_ui_schema,
            created_at=db_schema.created_at,
            updated_at=db_schema.updated_at,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting table schema: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/records", response_model=PaginatedResponse)
async def get_table_records(
    request: Request,
    table_name: str = Query(..., description="Name of the table"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=1000, description="Number of items per page"),
    search: Optional[str] = Query(None, description="Search term"),
):
    try:
        all_query_params = dict(request.query_params)
        defined_param_names = {"table_name", "page", "page_size", "search"}

        filter_dict = {
            key: value for key, value in all_query_params.items() if key not in defined_param_names
        }

        records, total = await DatabaseService.get_table_records(
            table_name=table_name,
            page=page,
            page_size=page_size,
            filters=filter_dict,
            search=search,
        )

        total_pages = (total + page_size - 1) // page_size

        return PaginatedResponse(
            items=records,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
        )
    except Exception as e:
        logger.error(f"Error getting table records: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/records", response_model=SuccessResponse)
async def create_record(
    record: RecordData,
    table_name: str = Query(..., description="Name of the table"),
    force: bool = Query(False, description="Force creation even if duplicates are found"),
):
    start_time = time.time()
    logger.info(f"Starting create_record for table {table_name}")
    """Create a new record in a table"""
    try:
        table_schema = await DatabaseService.get_table_schema(table_name)
        if not table_schema:
            raise HTTPException(
                status_code=404,
                detail=f"Table {table_name} not found for schema processing.",
            )

        expected_primary_business_column_in_key = table_schema.primary_business_column

        parsed_platform_mappings: Dict[str, str] = {}
        main_record_data: Dict[str, Any] = {}

        for key, value in record.data.items():
            prefix = "platform_mapping_for_"
            suffix = f"_of_{expected_primary_business_column_in_key}"
            if key.startswith(prefix) and key.endswith(suffix) and value is not None:
                try:
                    str_value = str(value).strip()
                    if not str_value:
                        main_record_data[key] = value
                        continue

                    parsed_platform_id = key[len(prefix) : -len(suffix)]
                    if parsed_platform_id:
                        parsed_platform_mappings[parsed_platform_id] = str_value
                    else:
                        main_record_data[key] = value
                except Exception:
                    main_record_data[key] = value
            else:
                main_record_data[key] = value

        all_platforms = await DatabaseService.get_all_platforms()
        all_platform_ids = {p.id for p in all_platforms}

        for col_def in table_schema.column_schema:
            col_name = col_def["name"]
            if col_name in main_record_data:
                value = main_record_data[col_name]
                if value is None:
                    continue

                col_type = col_def["type"]

                if col_type == "platform_list":
                    if not isinstance(value, list):
                        raise HTTPException(
                            status_code=400,
                            detail=f"Column '{col_name}' must be a list.",
                        )
                    for item in value:
                        if item not in all_platform_ids:
                            raise HTTPException(
                                status_code=400,
                                detail=f"Invalid platform ID '{item}' in column '{col_name}'.",
                            )

                if col_type == "text_list":
                    if not isinstance(value, list):
                        raise HTTPException(
                            status_code=400,
                            detail=f"Column '{col_name}' must be a list.",
                        )

                    min_len = col_def.get("min")
                    max_len = col_def.get("max")
                    pattern = col_def.get("regex")

                    for item in value:
                        if not isinstance(item, str):
                            raise HTTPException(
                                status_code=400,
                                detail=f"All items in '{col_name}' must be strings.",
                            )
                        if min_len is not None and len(item) < int(min_len):
                            raise HTTPException(
                                status_code=400,
                                detail=f"Each item in '{col_name}' must have at least {min_len} characters.",
                            )
                        if max_len is not None and len(item) > int(max_len):
                            raise HTTPException(
                                status_code=400,
                                detail=f"Each item in '{col_name}' must have at most {max_len} characters.",
                            )
                        if pattern and not re.fullmatch(pattern, item):
                            raise HTTPException(
                                status_code=400,
                                detail=f"Each item in '{col_name}' must match the pattern: {pattern}.",
                            )

        if not force:
            collected_warnings: Dict[str, FuzzyCheckResponse] = {}
            fuzzy_check_threshold = 0.8
            columns_to_check_for_duplicates: Dict[str, Any] = {}

            for col_schema in table_schema.column_schema:
                col_name = col_schema["name"]
                if (
                    (col_schema.get("fuzzy_check") or col_schema.get("is_unique"))
                    and col_name in main_record_data
                    and main_record_data[col_name] is not None
                ):
                    columns_to_check_for_duplicates[col_name] = main_record_data[col_name]

            if columns_to_check_for_duplicates:
                start_time = time.time()
                logger.info(
                    f"Starting duplicate check for columns {list(columns_to_check_for_duplicates.keys())} in table {table_name}"
                )
                batch_fuzzy_results = await DatabaseService.batch_fuzzy_check_values(
                    table_name=table_name,
                    columns_to_check=columns_to_check_for_duplicates,
                    threshold=fuzzy_check_threshold,
                )
                logger.info(f"Duplicate check results: {batch_fuzzy_results}")
                logger.info(
                    f"Duplicate check for columns {list(columns_to_check_for_duplicates.keys())} in table {table_name} completed in {time.time() - start_time} seconds."
                )
                if batch_fuzzy_results:
                    warnings = {}
                    for col, res in batch_fuzzy_results.items():
                        if res.exact_matches or res.similar_values:
                            warnings[col] = res.model_dump()

                    if warnings:
                        raise HTTPException(
                            status_code=409,
                            detail={
                                "message": "Potential duplicate values found.",
                                "warnings": warnings,
                            },
                        )

        record_id = await DatabaseService.insert_record(table_name, main_record_data)
        logger.info(
            f"Record {record_id} created in table {table_name} in {time.time() - start_time} seconds."
        )

        processed_platform_mappings_list: List[Tuple[str, str]] = []
        if parsed_platform_mappings:
            primary_business_value = main_record_data.get(table_schema.primary_business_column)

            if primary_business_value is None or str(primary_business_value).strip() == "":
                logger.warning(
                    f"Primary business column '{table_schema.primary_business_column}' value is missing or empty "
                    f"in submitted data for table '{table_name}' (record ID: {record_id}). "
                    f"Skipping platform list entry processing that depends on it."
                )
            else:
                for (
                    platform_id_str,
                    platform_user_value,
                ) in parsed_platform_mappings.items():
                    active_list_schema_for_platform = False
                    for ls_def in table_schema.list_schema or []:
                        if (
                            ls_def.get("platform_id") == platform_id_str
                            and ls_def.get("list_type") == "default"
                            and ls_def.get("enabled", True)
                        ):
                            active_list_schema_for_platform = True
                            break

                    if active_list_schema_for_platform:
                        processed_platform_mappings_list.append(
                            (platform_id_str, platform_user_value)
                        )
                    else:
                        logger.warning(
                            f"No active 'default' list schema found or configured for platform_id '{platform_id_str}' "
                            f"in table '{table_name}' for record {record_id}. Skipping platform mapping for value '{platform_user_value}'."
                        )

                if processed_platform_mappings_list:
                    start_time = time.time()
                    logger.info(
                        f"starting bulk upsert for record {record_id} in table {table_name}"
                    )
                    (
                        updated_count,
                        inserted_count,
                    ) = await DatabaseService.bulk_upsert_default_list_entries(
                        table_name=table_name,
                        platform_mappings=processed_platform_mappings_list,
                        record_id_from_main_table=uuid.UUID(record_id),
                        internal_value=str(primary_business_value),
                        primary_business_column_name=table_schema.primary_business_column,
                    )
                    logger.info(
                        f"Bulk platform mapping processing for record {record_id} in table {table_name} completed: {updated_count} updated, {inserted_count} inserted. Took {time.time() - start_time} seconds."
                    )
                    processed_mappings_count = updated_count + inserted_count
                else:
                    processed_mappings_count = 0
        else:
            processed_mappings_count = 0

        return SuccessResponse(
            message=f"Record {record_id} created successfully in table {table_name}"
            + (
                f" and {processed_mappings_count} platform mappings processed."
                if processed_mappings_count > 0
                else ". No platform mappings were processed (either none provided, primary business value missing, or no active list schemas)."
            )
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating record: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/records", response_model=SuccessResponse)
async def update_record(
    request: Request,
    record: RecordData,
    table_name: str = Query(..., description="Name of the table"),
    record_id: str = Query(..., description="ID of the record to update"),
    force: bool = Query(False, description="Force update even if duplicates are found"),
):
    try:
        permissions = request.state.user["permissions"]
        table_schema = await DatabaseService.get_table_schema(table_name)
        if not table_schema:
            raise HTTPException(
                status_code=404,
                detail=f"Table {table_name} not found for schema check",
            )

        expected_primary_business_column_in_key = table_schema.primary_business_column
        parsed_platform_mappings: Dict[str, str] = {}
        main_record_data: Dict[str, Any] = {}

        for key, value in record.data.items():
            prefix = "platform_mapping_for_"
            suffix = f"_of_{expected_primary_business_column_in_key}"
            if key.startswith(prefix) and key.endswith(suffix) and value is not None:
                try:
                    str_value = str(value).strip()
                    if not str_value:
                        continue

                    parsed_platform_id = key[len(prefix) : -len(suffix)]
                    if parsed_platform_id:
                        parsed_platform_mappings[parsed_platform_id] = str_value
                    else:
                        main_record_data[key] = value
                except Exception:
                    main_record_data[key] = value
            else:
                main_record_data[key] = value

        all_platforms = await DatabaseService.get_all_platforms()
        all_platform_ids = {p.id for p in all_platforms}

        for col_def in table_schema.column_schema:
            col_name = col_def["name"]
            if col_name in main_record_data:
                value = main_record_data[col_name]
                if value is None:
                    continue

                col_type = col_def["type"]

                if col_type == "platform_list":
                    if not isinstance(value, list):
                        raise HTTPException(
                            status_code=400,
                            detail=f"Column '{col_name}' must be a list.",
                        )
                    for item in value:
                        if item not in all_platform_ids:
                            raise HTTPException(
                                status_code=400,
                                detail=f"Invalid platform ID '{item}' in column '{col_name}'.",
                            )

                if col_type == "text_list":
                    if not isinstance(value, list):
                        raise HTTPException(
                            status_code=400,
                            detail=f"Column '{col_name}' must be a list.",
                        )

                    min_len = col_def.get("min")
                    max_len = col_def.get("max")
                    pattern = col_def.get("regex")

                    for item in value:
                        if not isinstance(item, str):
                            raise HTTPException(
                                status_code=400,
                                detail=f"All items in '{col_name}' must be strings.",
                            )
                        if min_len is not None and len(item) < int(min_len):
                            raise HTTPException(
                                status_code=400,
                                detail=f"Each item in '{col_name}' must have at least {min_len} characters.",
                            )
                        if max_len is not None and len(item) > int(max_len):
                            raise HTTPException(
                                status_code=400,
                                detail=f"Each item in '{col_name}' must have at most {max_len} characters.",
                            )
                        if pattern and not re.fullmatch(pattern, item):
                            raise HTTPException(
                                status_code=400,
                                detail=f"Each item in '{col_name}' must match the pattern: {pattern}.",
                            )

        if not force:
            collected_warnings: Dict[str, FuzzyCheckResponse] = {}
            fuzzy_check_threshold = 0.3
            columns_to_check_for_duplicates: Dict[str, Any] = {}

            for col_schema in table_schema.column_schema:
                col_name = col_schema["name"]
                if (
                    (col_schema.get("fuzzy_check") or col_schema.get("is_unique"))
                    and col_name in main_record_data
                    and main_record_data[col_name] is not None
                ):
                    columns_to_check_for_duplicates[col_name] = main_record_data[col_name]

            if columns_to_check_for_duplicates:
                batch_fuzzy_results = await DatabaseService.batch_fuzzy_check_values(
                    table_name=table_name,
                    columns_to_check=columns_to_check_for_duplicates,
                    threshold=fuzzy_check_threshold,
                    exclude_record_id=record_id,
                )
                if batch_fuzzy_results:
                    warnings = {}
                    for col, res in batch_fuzzy_results.items():
                        if res.exact_matches or res.similar_values:
                            warnings[col] = res.model_dump()

                    if warnings:
                        raise HTTPException(
                            status_code=409,
                            detail={
                                "message": "Potential duplicate values found.",
                                "warnings": warnings,
                            },
                        )
        await DatabaseService.update_record(table_name, record_id, main_record_data, permissions)

        processed_mappings_count = 0
        primary_business_value = main_record_data.get(table_schema.primary_business_column)

        if primary_business_value is None or str(primary_business_value).strip() == "":
            logger.warning(
                f"Primary business column '{table_schema.primary_business_column}' value is missing or empty "
                f"in submitted update for table '{table_name}' (record ID: {record_id}). "
                f"Skipping platform list entry processing."
            )
        else:
            all_platform_ids = {p.id for p in all_platforms}
            for platform_id in all_platform_ids:
                if platform_id not in parsed_platform_mappings:
                    parsed_platform_mappings[platform_id] = None

            processed_platform_mappings_list = []
            for (
                platform_id_str,
                platform_user_value,
            ) in parsed_platform_mappings.items():
                if any(
                    ls_def.get("platform_id") == platform_id_str
                    and ls_def.get("list_type") == "default"
                    and ls_def.get("enabled", True)
                    for ls_def in table_schema.list_schema or []
                ):
                    processed_platform_mappings_list.append((platform_id_str, platform_user_value))
                else:
                    logger.warning(
                        f"No active 'default' list schema found for platform_id '{platform_id_str}' "
                        f"in table '{table_name}' for record {record_id}. Skipping platform mapping."
                    )
            if processed_platform_mappings_list:
                (
                    updated_count,
                    inserted_count,
                ) = await DatabaseService.bulk_upsert_default_list_entries(
                    table_name=table_name,
                    platform_mappings=processed_platform_mappings_list,
                    record_id_from_main_table=uuid.UUID(record_id),
                    internal_value=str(primary_business_value),
                    primary_business_column_name=table_schema.primary_business_column,
                )
                processed_mappings_count = updated_count + inserted_count

        message = f"Record {record_id} updated successfully in table {table_name}"
        if processed_mappings_count > 0:
            message += f" and {processed_mappings_count} platform mappings processed."

        return SuccessResponse(message=message)

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error updating record: {str(traceback.format_exc())}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/records", response_model=SuccessResponse)
async def delete_record(
    table_name: str = Query(..., description="Name of the table"),
    record_id: str = Query(..., description="ID of the record to delete"),
):
    try:
        await DatabaseService.delete_record(table_name, record_id)
        return SuccessResponse(
            message=f"Record {record_id} deleted successfully from table {table_name}"
        )
    except Exception as e:
        logger.error(f"Error deleting record: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/fuzzy_check", response_model=FuzzyCheckResponse)
async def fuzzy_check_value_route(
    table_name: str = Query(..., description="Name of the table"),
    column_name: str = Query(..., description="Name of the column"),
    value: str = Query(..., description="Value to check for duplicates"),
    threshold: float = Query(0.3, ge=0.0, le=1.0, description="Similarity threshold"),
):
    try:
        similar_values, exact_matches = await DatabaseService.fuzzy_check_value(
            table_name, column_name, value, threshold
        )
        return FuzzyCheckResponse(similar_values=similar_values, exact_matches=exact_matches)
    except Exception as e:
        logger.error(f"Error in fuzzy check: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/fuzzy_check_list", response_model=FuzzyCheckResponse)
async def fuzzy_check_list_route(
    table_name: str = Query(..., description="Name of the table"),
    column_name: str = Query(..., description="Name of the column"),
    values: List[str] = Body(..., description="Values to check"),
    threshold: float = Query(0.3, ge=0.0, le=1.0, description="Similarity threshold"),
    exclude_record_id: Optional[str] = Query(
        None, description="ID of the record to exclude from the check"
    ),
):
    try:
        results = await DatabaseService.batch_fuzzy_check_values(
            table_name=table_name,
            columns_to_check={column_name: values},
            threshold=threshold,
            exclude_record_id=exclude_record_id,
        )

        response = results.get(column_name)
        if response:
            return response
        return FuzzyCheckResponse(similar_values=[], exact_matches=[])

    except Exception as e:
        logger.error(f"Error in fuzzy check list: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/enable_fuzzy_primary_columns", response_model=SuccessResponse)
async def enable_fuzzy_for_primary_business_columns_route():
    try:
        count = await DatabaseService.enable_fuzzy_for_primary_business_columns()
        return SuccessResponse(
            message=f"Fuzzy checking enabled for {count} primary business columns."
        )
    except Exception as e:
        logger.error(f"Error enabling fuzzy checking: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/exists", response_model=Dict[str, bool])
async def check_table_exists(
    table_name: str = Query(..., description="Name of the table"),
):
    try:
        exists = await DatabaseService.table_exists(table_name)
        return {"exists": exists}
    except Exception as e:
        logger.error(f"Error checking table existence: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/column_exists", response_model=Dict[str, bool])
async def check_column_exists(
    table_name: str = Query(..., description="Name of the table"),
    column_name: str = Query(..., description="Name of the column"),
):
    try:
        exists = await DatabaseService.column_exists(table_name, column_name)
        return {"exists": exists}
    except Exception as e:
        logger.error(f"Error checking column existence: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/primary_values", response_model=List[str])
async def get_primary_column_values(
    table_name: str = Query(..., description="Name of the table"),
    search: Optional[str] = Query(None, description="Optional search filter (ILIKE)"),
):
    try:
        schema = await DatabaseService.get_table_schema(table_name)
        if not schema or not schema.primary_business_column:
            raise HTTPException(
                status_code=404, detail="Table or primary business column not found"
            )
        primary_col = schema.primary_business_column
        sql = f'SELECT DISTINCT "{primary_col}" as v FROM "{DatabaseService._table(table_name)}"'
        params: List[Any] = []
        if search:
            sql += f' WHERE "{primary_col}" ILIKE $1'
            params.append(f"%{search}%")
        sql += " ORDER BY v LIMIT 10000"
        rows = await Tortoise.get_connection("default").execute_query_dict(sql, params)
        return [r["v"] for r in rows if r["v"] is not None]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching primary values for {table_name}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/platform_values", response_model=List[str])
async def get_platform_values(
    table_name: str = Query(..., description="Name of the table"),
    platform_id: str = Query(..., description="ID of the platform"),
):
    try:
        schema = await DatabaseService.get_table_schema(table_name)
        if not schema or not schema.primary_business_column:
            raise HTTPException(
                status_code=404, detail="Table or primary business column not found"
            )

        values = await DatabaseService.get_platform_values_for_dropdown(
            table_name=table_name,
            platform_id=platform_id,
            primary_table_column=schema.primary_business_column,
        )
        return values
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching platform values: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post(
    "/list_schemas",
    response_model=ListSchemaDefinition,
    summary="Add a list schema definition to a table",
    tags=["list_schemas"],
)
async def add_list_schema_to_table(
    table_name: str = Query(..., description="The name of the table"),
    list_def_create: ListSchemaDefinition = Body(...),
):
    try:
        if table_name == "sizes":
            table_name = "sizing_lists"

        if not await DatabaseService.table_exists(table_name):
            raise HTTPException(status_code=404, detail=f"Table {table_name} not found.")

        created_list_schema = await DatabaseService.add_list_schema_definition(
            table_name=table_name, list_def_create=list_def_create
        )
        return created_list_schema
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error adding list schema definition: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/list_schemas",
    response_model=List[ListSchemaDefinition],
    summary="Get all list schema definitions for a table",
    tags=["list_schemas"],
)
async def get_list_schemas_for_table(
    table_name: str = Query(..., description="The name of the table"),
):
    try:
        if not await DatabaseService.table_exists(table_name):
            raise HTTPException(status_code=404, detail=f"Table {table_name} not found.")

        list_schemas = await DatabaseService.get_list_schema_definitions(table_name=table_name)
        return list_schemas
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting list schema definitions: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put(
    "/list_schemas",
    response_model=ListSchemaDefinition,
    summary="Update a list schema definition",
    tags=["list_schemas"],
)
async def update_list_schema_for_table(
    table_name: str = Query(..., description="The name of the table"),
    platform_id: str = Query(..., description="The platform ID of the list schema to update"),
    list_type: str = Query(
        ...,
        description="The list type (default or sizing) of the list schema to update",
    ),
    list_def_update: ListSchemaDefinitionUpdate = Body(...),
):
    try:
        if list_type not in ["default", "sizing"]:
            raise HTTPException(
                status_code=400,
                detail="Invalid list_type. Must be 'default' or 'sizing'.",
            )

        if not await DatabaseService.table_exists(table_name):
            raise HTTPException(status_code=404, detail=f"Table {table_name} not found.")

        updated_list_schema = await DatabaseService.update_list_schema_definition(
            table_name=table_name,
            platform_id=platform_id,
            list_type=list_type,
            list_def_update=list_def_update,
        )
        return updated_list_schema
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error updating list schema definition: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete(
    "/list_schemas",
    response_model=SuccessResponse,
    summary="Delete a list schema definition",
    tags=["list_schemas"],
)
async def delete_list_schema_from_table(
    table_name: str = Query(..., description="The name of the table"),
    platform_id: str = Query(..., description="The platform ID of the list schema to delete"),
    list_type: str = Query(
        ...,
        description="The list type (default or sizing) of the list schema to delete",
    ),
):
    try:
        if list_type not in ["default", "sizing"]:
            raise HTTPException(
                status_code=400,
                detail="Invalid list_type. Must be 'default' or 'sizing'.",
            )

        if not await DatabaseService.table_exists(table_name):
            raise HTTPException(status_code=404, detail=f"Table {table_name} not found.")

        await DatabaseService.delete_list_schema_definition(
            table_name=table_name, platform_id=platform_id, list_type=list_type
        )
        return SuccessResponse(message="List definition deleted successfully")
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error deleting list schema definition: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/records_lookup", response_model=Dict[str, uuid.UUID])
async def lookup_record_id_by_column_value(
    table_name: str = Query(..., description="Name of the table"),
    column_name: str = Query(..., description="Name of the column to search in"),
    value: str = Query(..., description="Value to search for"),
    exclude_record_id: Optional[str] = Query(
        None, description="ID of a record to exclude from the search"
    ),
):
    try:
        record_id = await DatabaseService.get_record_id_by_column_value(
            table_name=table_name,
            column_name=column_name,
            value=value,
            exclude_record_id=exclude_record_id,
        )
        if record_id:
            return {"id": record_id}
        else:
            raise HTTPException(
                status_code=404,
                detail=f"No record found in table '{table_name}' where column '{column_name}' matches value '{value}'.",
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Error looking up record in table '{table_name}' by column '{column_name}' for value '{value}': {str(e)}"
        )
        raise HTTPException(status_code=500, detail="Internal server error during lookup.")


@router.get("/records/mappings", response_model=Dict[str, Any])
async def get_record_mappings(
    table_name: str = Query(..., description="Name of the table"),
    record_id: str = Query(..., description="ID of the record"),
):
    try:
        mappings = await DatabaseService.get_platform_mappings_for_record(table_name, record_id)
        return mappings
    except Exception as e:
        logger.error(f"Error getting record mappings: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/platform_list_options", response_model=Dict[str, List[str]])
async def get_all_platform_dropdown_values(
    table_name: str = Query(..., description="Name of the table"),
):
    try:
        db_schema = await DatabaseService.get_table_schema(table_name)
        if not db_schema:
            raise HTTPException(status_code=404, detail=f"Table {table_name} not found")

        enabled_platform_ids = []
        for list_schema in db_schema.list_schema or []:
            if list_schema.get("list_type") == "default" and list_schema.get("enabled", True):
                enabled_platform_ids.append(list_schema["platform_id"])

        if not enabled_platform_ids:
            return {}

        all_platform_values = await DatabaseService.get_all_platform_values_for_dropdown(
            table_name, enabled_platform_ids, db_schema.primary_business_column
        )
        return all_platform_values
    except Exception as e:
        logger.error(f"Error getting all platform dropdown values: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/types/check_code", response_model=Dict[str, Any])
async def check_type_code_uniqueness(
    type_code: int = Query(..., description="The type code to check"),
    exclude_record_id: Optional[str] = Query(
        None, description="ID of a record to exclude from the check"
    ),
):
    try:
        conflicting_type_name = await DatabaseService.check_type_code_exists(
            type_code, exclude_record_id
        )
        if conflicting_type_name is not None:
            return {"exists": True, "type_name": conflicting_type_name}
        return {"exists": False, "type_name": None}
    except Exception as e:
        logger.error(f"Error in type_code uniqueness check: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error during type code check.")


@router.get("/export")
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

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=table_name)

        output.seek(0)

        headers = {"Content-Disposition": f'attachment; filename="{table_name}_export.xlsx"'}

        return StreamingResponse(
            output,
            headers=headers,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error exporting table {table_name}: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Internal server error")
