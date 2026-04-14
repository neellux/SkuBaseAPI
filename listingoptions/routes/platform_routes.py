from fastapi import APIRouter, HTTPException, Query, File, UploadFile, Form
from typing import List, Optional
from listingoptions.models.api_models import PlatformResponse, SuccessResponse
from listingoptions.models.db_models import Platform
from listingoptions.services.database_service import DatabaseService
import logging
import base64

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/platforms", tags=["platforms"])

MAX_ICON_SIZE_KB = 5


@router.get("/list", response_model=List[PlatformResponse])
async def list_platforms():
    try:
        platforms = await DatabaseService.get_all_platforms()
        return [
            PlatformResponse(
                id=platform.id,
                name=platform.name,
                icon=platform.icon,
                icon_mime_type=platform.icon_mime_type,
            )
            for platform in platforms
        ]
    except Exception as e:
        logger.error(f"Error listing platforms: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/get", response_model=PlatformResponse)
async def get_platform(platform_id: str = Query(..., description="Platform ID")):
    try:
        platform = await DatabaseService.get_platform_by_id(platform_id)
        if not platform:
            raise HTTPException(status_code=404, detail=f"Platform {platform_id} not found")

        return PlatformResponse(
            id=platform.id,
            name=platform.name,
            icon=platform.icon,
            icon_mime_type=platform.icon_mime_type,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting platform: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/create", response_model=SuccessResponse)
async def create_platform(
    id: str = Form(...),
    name: str = Form(...),
    icon: Optional[UploadFile] = File(None),
):
    try:
        existing_platform = await DatabaseService.get_platform_by_id(id)
        if existing_platform:
            raise HTTPException(status_code=400, detail=f"Platform {id} already exists")

        icon_base64 = None
        icon_mime_type = None
        if icon:
            if icon.size > MAX_ICON_SIZE_KB * 1024:
                raise HTTPException(
                    status_code=400,
                    detail=f"Icon size exceeds {MAX_ICON_SIZE_KB}KB limit.",
                )
            contents = await icon.read()
            icon_base64 = base64.b64encode(contents).decode("utf-8")
            icon_mime_type = icon.content_type

        platform = await Platform.create(
            id=id, name=name, icon=icon_base64, icon_mime_type=icon_mime_type
        )
        await DatabaseService.update_platform_cache(platform)
        return SuccessResponse(message=f"Platform {platform.id} created successfully")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating platform: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/update", response_model=SuccessResponse)
async def update_platform(
    platform_id: str = Query(..., description="Platform ID to update"),
    name: str = Form(...),
    icon: Optional[UploadFile] = File(None),
):
    try:
        platform = await DatabaseService.get_platform_by_id(platform_id)
        if not platform:
            raise HTTPException(status_code=404, detail=f"Platform {platform_id} not found")

        platform.name = name

        if icon:
            if icon.size > MAX_ICON_SIZE_KB * 1024:
                raise HTTPException(
                    status_code=400,
                    detail=f"Icon size exceeds {MAX_ICON_SIZE_KB}KB limit.",
                )
            contents = await icon.read()
            platform.icon = base64.b64encode(contents).decode("utf-8")
            platform.icon_mime_type = icon.content_type

        await platform.save()

        await DatabaseService.update_platform_cache(platform)
        return SuccessResponse(message=f"Platform {platform.id} updated successfully")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating platform: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/delete", response_model=SuccessResponse)
async def delete_platform(
    platform_id: str = Query(..., description="Platform ID to delete"),
):
    try:
        platform = await DatabaseService.get_platform_by_id(platform_id)
        if not platform:
            raise HTTPException(status_code=404, detail=f"Platform {platform_id} not found")

        await platform.delete()
        await DatabaseService.remove_platform_from_cache(platform_id)
        return SuccessResponse(message=f"Platform {platform_id} deleted successfully")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting platform: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/exists", response_model=dict)
async def check_platform_exists(
    platform_id: str = Query(..., description="Platform ID to check"),
):
    try:
        platform = await DatabaseService.get_platform_by_id(platform_id)
        return {"exists": platform is not None}
    except Exception as e:
        logger.error(f"Error checking platform existence: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")
