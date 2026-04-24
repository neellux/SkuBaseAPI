import logging

from fastapi import APIRouter, HTTPException
from models.api_models import (
    AppVariablesResponse,
    EnabledPlatformsResponse,
    PlatformMetaResponse,
    PlatformSettingsResponse,
    SettingsResponse,
    UpdateAppVariablesRequest,
    UpdateEnabledPlatformsRequest,
    UpdatePlatformSettingsRequest,
    UpdateSettingsRequest,
)
from models.db_models import AppSettings
from services.listing_options_service import listing_options_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/settings", tags=["settings"])


@router.get("/field_templates", response_model=SettingsResponse)
async def get_field_templates():
    try:
        settings = await AppSettings.first()

        if not settings:
            settings = await AppSettings.create(field_templates={})

        return SettingsResponse(
            id=settings.id,
            field_templates=settings.field_templates or {},
            created_at=settings.created_at,
            updated_at=settings.updated_at,
        )
    except Exception as e:
        logger.error(f"Error fetching field templates: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/field_templates", response_model=SettingsResponse)
async def update_field_templates(request: UpdateSettingsRequest):
    try:
        settings = await AppSettings.first()

        if not settings:
            settings = await AppSettings.create(field_templates=request.field_templates or {})
        else:
            if request.field_templates is not None:
                is_platform_format = request.field_templates and all(
                    isinstance(v, dict) for v in request.field_templates.values()
                )

                if is_platform_format:
                    allowed_platforms = {"sellercloud", "grailed", "ebay"}
                    allowed_fields = {"title", "description"}
                    for platform_id, templates in request.field_templates.items():
                        if platform_id not in allowed_platforms:
                            raise HTTPException(
                                status_code=400,
                                detail=f"Invalid platform: {platform_id}. Allowed: {allowed_platforms}",
                            )
                        for field_name in templates.keys():
                            if field_name not in allowed_fields:
                                raise HTTPException(
                                    status_code=400,
                                    detail=f"Invalid field template name: {field_name}",
                                )
                            if not isinstance(templates[field_name], str):
                                raise HTTPException(
                                    status_code=400,
                                    detail=f"Template for {platform_id}.{field_name} must be a string",
                                )
                else:
                    allowed_fields = {"title", "description"}
                    provided_fields = set(request.field_templates.keys())
                    invalid_fields = provided_fields - allowed_fields

                    if invalid_fields:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Invalid field template names: {invalid_fields}.",
                        )

                    for field_name, template_value in request.field_templates.items():
                        if not isinstance(template_value, str):
                            raise HTTPException(
                                status_code=400,
                                detail=f"Template for {field_name} must be a string",
                            )

                settings.field_templates = request.field_templates

            await settings.save(update_fields=["field_templates"])

        return SettingsResponse(
            id=settings.id,
            field_templates=settings.field_templates or {},
            created_at=settings.created_at,
            updated_at=settings.updated_at,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating field templates: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to update field templates")


@router.get("/variables", response_model=AppVariablesResponse)
async def get_app_variables():
    try:
        settings = await AppSettings.first()

        default_variables = [{"id": "max_batches", "name": "Maximum Batch Size", "value": 50}]

        if not settings:
            settings = await AppSettings.create(field_templates={}, app_variables=default_variables)

        return AppVariablesResponse(
            app_variables=settings.app_variables or default_variables,
            updated_at=settings.updated_at,
        )
    except Exception as e:
        logger.error(f"Error fetching app variables: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/variables", response_model=AppVariablesResponse)
async def update_app_variables(request: UpdateAppVariablesRequest):
    try:
        settings = await AppSettings.first()

        if not settings:
            settings = await AppSettings.create(
                field_templates={}, app_variables=request.app_variables
            )
        else:
            settings.app_variables = request.app_variables
            await settings.save(update_fields=["app_variables"])

        return AppVariablesResponse(
            app_variables=settings.app_variables,
            updated_at=settings.updated_at,
        )
    except Exception as e:
        logger.error(f"Error updating app variables: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to update app variables")


@router.get("/platform_settings", response_model=PlatformSettingsResponse)
async def get_platform_settings():
    try:
        settings = await AppSettings.first()

        if not settings:
            settings = await AppSettings.create(field_templates={}, platform_settings={})

        return PlatformSettingsResponse(
            platform_settings=settings.platform_settings or {},
            updated_at=settings.updated_at,
        )
    except Exception as e:
        logger.error(f"Error fetching platform settings: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/platform_settings", response_model=PlatformSettingsResponse)
async def update_platform_settings(request: UpdatePlatformSettingsRequest):
    try:
        settings = await AppSettings.first()

        if not settings:
            settings = await AppSettings.create(
                field_templates={}, platform_settings=request.platform_settings
            )
        else:
            settings.platform_settings = request.platform_settings
            await settings.save(update_fields=["platform_settings"])

        settings = await AppSettings.first()

        return PlatformSettingsResponse(
            platform_settings=settings.platform_settings,
            updated_at=settings.updated_at,
        )
    except Exception as e:
        logger.error(f"Error updating platform settings: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to update platform settings")


@router.get("/platform_meta", response_model=PlatformMetaResponse)
async def get_platform_meta():
    try:
        settings = await AppSettings.first()
        enabled_platform_ids = (
            settings.platforms if settings and settings.platforms else ["sellercloud", "grailed"]
        )
        platform_settings = (settings.platform_settings or {}) if settings else {}

        all_platforms = await listing_options_service.get_platforms()

        platforms = [
            {**p, "settings": platform_settings.get(p["id"], {})}
            for p in all_platforms
            if p.get("id") in enabled_platform_ids
        ]

        return PlatformMetaResponse(platforms=platforms)
    except Exception as e:
        logger.error(f"Error fetching platform meta: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch platform metadata")


@router.get("/platforms", response_model=EnabledPlatformsResponse)
async def get_enabled_platforms():
    try:
        settings = await AppSettings.first()

        if not settings:
            settings = await AppSettings.create(
                field_templates={}, platforms=["sellercloud", "grailed"]
            )

        return EnabledPlatformsResponse(
            platforms=settings.platforms or ["sellercloud", "grailed"],
            updated_at=settings.updated_at,
        )
    except Exception as e:
        logger.error(f"Error fetching enabled platforms: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/platforms", response_model=EnabledPlatformsResponse)
async def update_enabled_platforms(request: UpdateEnabledPlatformsRequest):
    try:
        settings = await AppSettings.first()

        if not settings:
            settings = await AppSettings.create(field_templates={}, platforms=request.platforms)
        else:
            settings.platforms = request.platforms
            await settings.save(update_fields=["platforms"])

        return EnabledPlatformsResponse(
            platforms=settings.platforms,
            updated_at=settings.updated_at,
        )
    except Exception as e:
        logger.error(f"Error updating enabled platforms: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to update enabled platforms")
