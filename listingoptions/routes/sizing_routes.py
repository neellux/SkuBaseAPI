import io
import logging
import uuid
from typing import Dict, List, Optional, Union

import pandas as pd
from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query, status
from fastapi.responses import StreamingResponse
from listingoptions.models.api_models import (
    AllSizingSchemesResponse,
    FullSizingSchemeCreate,
    SizingSchemeDetailResponse,
    SizingSchemeEntryCreate,
    SizingSchemeEntryDB,
    SizingSchemeListedName,
    UpdateSizeOrderRequest,
)
from listingoptions.services.sizing_service import SizingService
from tortoise import Tortoise
from tortoise.exceptions import DoesNotExist, IntegrityError

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/sizing_schemes", tags=["Sizing Schemes"])


@router.get(
    "/sizes",
    summary="Get sizing scheme options for default list mapping",
    description="Returns {id, label} objects for use in the sizes default list Autocomplete dropdown.",
)
async def get_sizing_scheme_options(search: Optional[str] = Query(None)):
    try:
        sql = "SELECT id, sizing_scheme || ':' || size as label FROM \"listingoptions_sizing_schemes\""
        params = []
        if search:
            sql += " WHERE sizing_scheme || ':' || size ILIKE $1"
            params.append(f"%{search}%")
        sql += " ORDER BY label LIMIT 100000"
        rows = await Tortoise.get_connection("default").execute_query_dict(sql, params)
        return [{"id": str(r["id"]), "label": r["label"]} for r in rows]
    except Exception as e:
        logger.error(f"Failed to retrieve sizing scheme options: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve sizing scheme options.",
        )


@router.post(
    "",
    response_model=SizingSchemeDetailResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new sizing scheme with its sizes",
    description="Creates a new sizing scheme and initializes it with a list of sizes and their orders. The scheme name must be unique.",
)
async def create_new_sizing_scheme(scheme_create: FullSizingSchemeCreate = Body(...)):
    try:
        return await SizingService.create_full_sizing_scheme(scheme_create)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error creating sizing scheme: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred.",
        )


@router.get(
    "",
    response_model=Union[List[SizingSchemeListedName], AllSizingSchemesResponse],
    summary="List all sizing schemes",
    description="Retrieves a list of all sizing scheme names, or optionally with full details including sizes.",
)
async def list_all_sizing_schemes(
    include_details: bool = Query(False, description="Include sizes for each scheme"),
):
    try:
        if include_details:
            return await SizingService.get_all_sizing_schemes_with_details()
        else:
            return await SizingService.get_all_sizing_scheme_names()
    except Exception as e:
        logger.error(f"Failed to retrieve sizing schemes: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve sizing schemes.",
        )


@router.get(
    "/detail",
    response_model=SizingSchemeDetailResponse,
    summary="Get details of a specific sizing scheme",
    description="Retrieves the details of a specific sizing scheme, including all its sizes and their order, queried by scheme_name.",
)
async def get_sizing_scheme_by_name(
    scheme_name: str = Query(..., title="The name of the sizing scheme to retrieve"),
):
    try:
        scheme_details = await SizingService.get_sizing_scheme_details(scheme_name)
        if scheme_details is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Sizing scheme '{scheme_name}' not found.",
            )
        return scheme_details
    except DoesNotExist:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Sizing scheme '{scheme_name}' not found.",
        )
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.put(
    "",
    response_model=SizingSchemeDetailResponse,
    summary="Update an entire sizing scheme (overwrite sizes and orders)",
    description="Replaces all existing sizes and their orders for the specified sizing scheme (queried by scheme_name) with the new list provided. If the scheme does not exist, it returns 404.",
)
async def update_entire_sizing_scheme(
    scheme_name: str = Query(..., title="The name of the sizing scheme to update"),
    update_request: UpdateSizeOrderRequest = Body(...),
):
    try:
        return await SizingService.update_scheme_size_orders(scheme_name, update_request)
    except DoesNotExist as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.delete(
    "",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete an entire sizing scheme",
    description="Deletes an entire sizing scheme (queried by scheme_name) and all its associated sizes. Returns 204 No Content on success.",
)
async def delete_entire_sizing_scheme(
    scheme_name: str = Query(..., title="The name of the sizing scheme to delete"),
):
    try:
        await SizingService.delete_sizing_scheme(scheme_name)
    except DoesNotExist as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post(
    "/sizes",
    response_model=SizingSchemeEntryDB,
    status_code=status.HTTP_201_CREATED,
    summary="Add a new size to a sizing scheme",
    description="Adds a single new size to an existing sizing scheme (queried by scheme_name). The combination of (scheme name, size value) must be unique.",
)
async def add_new_size_to_sizing_scheme(
    scheme_name: str = Query(..., title="The name of the sizing scheme"),
    entry_create: SizingSchemeEntryCreate = Body(...),
):
    try:
        return await SizingService.add_size_to_scheme(scheme_name, entry_create)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except IntegrityError:
        logger.error(
            f"Integrity error adding size '{entry_create.size}' to scheme '{scheme_name}'. Likely duplicate size."
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Failed to add size to scheme '{scheme_name}' due to a conflict (e.g., duplicate size). Ensure size is unique within the scheme.",
        )
    except Exception as e:
        logger.error(
            f"Unexpected error adding size '{entry_create.size}' to scheme '{scheme_name}': {str(e)}"
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get(
    "/sizes/detail",
    response_model=SizingSchemeEntryDB,
    summary="Get a specific size entry from a scheme",
    description="Retrieves a specific size entry by its value from within a given sizing scheme (queried by scheme_name and size_value).",
)
async def get_specific_size_from_scheme(
    scheme_name: str = Query(..., title="The name of the sizing scheme"),
    size_value: str = Query(..., title="The value of the size to retrieve (e.g., 'S', 'M', '42')"),
):
    try:
        entry = await SizingService.get_size_entry(scheme_name, size_value)
        if entry is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Size '{size_value}' not found in scheme '{scheme_name}'.",
            )
        return entry
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.put(
    "/sizes",
    response_model=SizingSchemeEntryDB,
    summary="Update a specific size entry in a scheme",
    description="Updates the details (e.g., its order or even its value) of a specific size within a scheme. The current size is identified by query parameters scheme_name and current_size_value. The new size details are in the body. If changing size value, the new value must be unique within the scheme.",
)
async def update_specific_size_in_scheme(
    scheme_name: str = Query(..., title="The name of the sizing scheme"),
    current_size_value: str = Query(..., title="The current value of the size to update"),
    entry_update: SizingSchemeEntryCreate = Body(...),
):
    try:
        updated_entry = await SizingService.update_single_size_entry(
            scheme_name, current_size_value, entry_update
        )
        return updated_entry
    except DoesNotExist as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except IntegrityError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Update failed due to a data conflict.",
        )
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.delete(
    "/sizes",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a specific size from a scheme",
    description="Deletes a specific size entry from a sizing scheme, identified by query parameters scheme_name and size_value. Returns 204 No Content on success.",
)
async def delete_specific_size_from_scheme(
    scheme_name: str = Query(..., title="The name of the sizing scheme"),
    size_value: str = Query(..., title="The value of the size to delete"),
):
    try:
        await SizingService.delete_size_from_scheme(scheme_name, size_value)
    except DoesNotExist as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get(
    "/entries_by_name",
    response_model=List[SizingSchemeEntryDB],
    summary="Get all size entries with IDs for a specific sizing scheme name",
    description="Retrieves all size entries (including their database IDs) for a given scheme name, ordered by their specified 'order'.",
)
async def get_all_scheme_entries_by_name(
    scheme_name: str = Query(..., title="The name of the sizing scheme"),
):
    try:
        entries = await SizingService.get_sizing_scheme_entries_by_name(scheme_name)
        if not entries:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No entries found for sizing scheme '{scheme_name}' or scheme does not exist.",
            )
        return entries
    except DoesNotExist:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Sizing scheme '{scheme_name}' not found.",
        )
    except Exception as e:
        logger.error(f"Error retrieving entries for scheme {scheme_name}: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get(
    "/sizing_type_options",
    response_model=List[str],
    summary="Get available options for sizing types",
    description="Retrieves the list of predefined string options for the 'sizing_types' field.",
)
async def get_sizing_type_options():
    try:
        return await SizingService.get_sizing_type_options()
    except Exception as e:
        logger.error(f"Failed to retrieve sizing type options: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve sizing type options.",
        )


@router.get(
    "/platform_default_sizes",
    response_model=Dict[str, List[str]],
    summary="Get default size values for platforms",
    description="Retrieves a map of platform IDs to a list of their common or default size values.",
)
async def get_platform_default_sizes():
    try:
        return await SizingService.get_platform_default_sizes()
    except Exception as e:
        logger.error(f"Failed to retrieve platform default sizes: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve platform default sizes.",
        )


@router.get(
    "/export",
    summary="Export all sizing schemes to Excel",
    description="Exports all sizing schemes and their sizes to an Excel file.",
)
async def export_sizing_schemes():
    try:
        rows = await SizingService.export_all_sizing_schemes()

        df = (
            pd.DataFrame(rows)
            if rows
            else pd.DataFrame(columns=["Sizing Scheme", "Size", "Sizing Types"])
        )

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Sizing Schemes")

        output.seek(0)

        headers = {"Content-Disposition": 'attachment; filename="sizing_schemes_export.xlsx"'}

        return StreamingResponse(
            output,
            headers=headers,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    except Exception as e:
        logger.error(f"Error exporting sizing schemes: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to export sizing schemes.",
        )
