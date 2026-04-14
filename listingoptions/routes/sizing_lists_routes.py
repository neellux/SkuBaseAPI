from fastapi import APIRouter, HTTPException, Depends, Path, Body, Query, status
from typing import List, Optional
import uuid
import logging

from listingoptions.services.sizing_lists_service import SizingListService
from listingoptions.models.api_models import (
    SizingListPlatformEntryCreate,
    SizingListPlatformEntryUpdate,
    SizingListPlatformEntryDetail,
    PaginatedSizingListPlatformEntryResponse,
    PaginationParams,
)
from tortoise.exceptions import DoesNotExist, IntegrityError

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/sizing_lists", tags=["Sizing Lists"])


@router.post(
    "",
    response_model=SizingListPlatformEntryDetail,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new Sizing List Entry",
    description="Creates a new mapping between a Sizing Scheme entry, a Platform, and a platform-specific value.",
)
async def create_sizing_list_platform_entry(
    entry_create: SizingListPlatformEntryCreate = Body(...),
):
    try:
        return await SizingListService.create_sizing_list_entry(entry_create)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error creating sizing list entry: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while creating the sizing list entry.",
        )


@router.get(
    "",
    response_model=PaginatedSizingListPlatformEntryResponse,
    summary="List all Sizing List Entries",
    description="Retrieves a paginated list of all sizing list entries, with optional filters.",
)
async def list_all_sizing_list_platform_entries(
    pagination: PaginationParams = Depends(),
    sizing_scheme_name_filter: Optional[str] = Query(
        None, description="Filter by sizing scheme name (case-insensitive contains)."
    ),
    platform_id_filter: Optional[str] = Query(None, description="Filter by exact platform ID."),
    size_value_filter: Optional[str] = Query(
        None, description="Filter by size value (case-insensitive contains)."
    ),
    platform_value_filter: Optional[str] = Query(
        None, description="Filter by platform value (case-insensitive contains)."
    ),
    sizing_type_filter: Optional[str] = Query(None, description="Filter by exact sizing type."),
):
    try:
        return await SizingListService.get_all_sizing_list_entries(
            pagination=pagination,
            sizing_scheme_name_filter=sizing_scheme_name_filter,
            platform_id_filter=platform_id_filter,
            size_value_filter=size_value_filter,
            platform_value_filter=platform_value_filter,
            sizing_type_filter=sizing_type_filter,
        )
    except Exception as e:
        logger.error(f"Failed to retrieve sizing list entries: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve sizing list entries.",
        )


@router.get(
    "/detail",
    response_model=SizingListPlatformEntryDetail,
    summary="Get a specific Sizing List Entry",
    description="Retrieves details for a specific sizing list entry by its ID.",
)
async def get_sizing_list_platform_entry(
    entry_id: uuid.UUID = Query(..., description="The ID of the sizing list entry to retrieve."),
):
    entry = await SizingListService.get_sizing_list_entry_by_id(entry_id)
    if not entry:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Sizing list entry with ID {entry_id} not found.",
        )
    return entry


@router.put(
    "/update",
    response_model=SizingListPlatformEntryDetail,
    summary="Update a Sizing List Entry",
    description="Updates the platform_value of a specific sizing list entry.",
)
async def update_sizing_list_platform_entry(
    entry_id: uuid.UUID = Query(..., description="The ID of the sizing list entry to update."),
    entry_update: SizingListPlatformEntryUpdate = Body(...),
):
    try:
        updated_entry = await SizingListService.update_sizing_list_entry(entry_id, entry_update)
        if not updated_entry:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Sizing list entry with ID {entry_id} not found for update.",
            )
        return updated_entry
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except DoesNotExist:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Sizing list entry with ID {entry_id} not found.",
        )
    except Exception as e:
        logger.error(f"Error updating sizing list entry {entry_id}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred while updating entry {entry_id}.",
        )


@router.delete(
    "/delete",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a Sizing List Entry",
    description="Deletes a specific sizing list entry by its ID.",
)
async def delete_sizing_list_platform_entry(
    entry_id: uuid.UUID = Query(..., description="The ID of the sizing list entry to delete."),
):
    success = await SizingListService.delete_sizing_list_entry(entry_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Sizing list entry with ID {entry_id} not found for deletion.",
        )
