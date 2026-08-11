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
from services.template_render import extract_placeholders
from services.template_service import TemplateService

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
            strict_template_validation=bool(settings.strict_template_validation),
            created_at=settings.created_at,
            updated_at=settings.updated_at,
        )
    except Exception as e:
        logger.error(f"Error fetching field templates: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


async def _valid_field_ids() -> set | None:
    """Best-effort set of valid placeholder field ids; None if the field list can't be loaded."""
    try:
        fields = await TemplateService.get_template_fields("default")
        return {f["id"] for f in fields if f.get("id")}
    except Exception as e:  # never block a save because an upstream field list is unavailable
        logger.warning(f"Could not load valid field list for template validation: {e}")
        return None


@router.put("/field_templates", response_model=SettingsResponse)
async def update_field_templates(request: UpdateSettingsRequest):
    try:
        settings = await AppSettings.first()

        if not settings:
            settings = await AppSettings.create(
                field_templates=request.field_templates or {},
                strict_template_validation=bool(request.strict_template_validation),
            )
            return SettingsResponse(
                id=settings.id,
                field_templates=settings.field_templates or {},
                strict_template_validation=bool(settings.strict_template_validation),
                created_at=settings.created_at,
                updated_at=settings.updated_at,
            )

        update_fields = []

        if request.strict_template_validation is not None:
            settings.strict_template_validation = request.strict_template_validation
            update_fields.append("strict_template_validation")

        if request.field_templates is not None:
            enabled_platforms = set(
                settings.platforms or ["sellercloud", "grailed", "spo"]
            )
            strict = bool(settings.strict_template_validation)
            # The valid-field list requires an upstream (SellerCloud) call, so only load it when
            # strict validation is on - keeps ordinary autosaves cheap.
            valid_ids = await _valid_field_ids() if strict else None

            for platform_id, templates in request.field_templates.items():
                if not isinstance(templates, dict):
                    raise HTTPException(
                        status_code=400,
                        detail=f"Templates for '{platform_id}' must be an object keyed by field name.",
                    )
                if platform_id not in enabled_platforms:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Invalid platform: {platform_id}. Allowed: {sorted(enabled_platforms)}",
                    )
                for field_name, template_value in templates.items():
                    if not isinstance(template_value, str):
                        raise HTTPException(
                            status_code=400,
                            detail=f"Template for {platform_id}.{field_name} must be a string",
                        )

                    if strict and valid_ids is not None:
                        if field_name not in valid_ids:
                            raise HTTPException(
                                status_code=400,
                                detail=f"Invalid field template name: {field_name}",
                            )
                        if template_value:
                            unknown = sorted(
                                {
                                    token
                                    for token in extract_placeholders(template_value)
                                    if token not in valid_ids
                                }
                            )
                            if unknown:
                                raise HTTPException(
                                    status_code=400,
                                    detail=(
                                        f"{platform_id}.{field_name} references unknown fields: "
                                        f"{', '.join(unknown)}"
                                    ),
                                )

            settings.field_templates = request.field_templates
            update_fields.append("field_templates")

        if update_fields:
            await settings.save(update_fields=update_fields)

        return SettingsResponse(
            id=settings.id,
            field_templates=settings.field_templates or {},
            strict_template_validation=bool(settings.strict_template_validation),
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


SPO_DEFAULT_SETTINGS = {
    "manual_fallback": False,
    "min_batch_size": 200,
    "require_type_mapping": False,
    "require_color_mapping": False,
    "require_brand_mapping": False,
}

GRAILED_DEFAULT_SETTINGS = {
    "manual_fallback": True,
    "min_batch_size": 100,
}

# eBay submits in batches like grailed and spo, so manual_fallback parks its rows in
# `queued` for a poller rather than dispatching them inline.
#
# All three mapping gates are on, unlike spo and grailed. A platform is skipped by the gate
# chain entirely unless at least one require_* flag is truthy
# (listing_options_service.check_unmapped_mappings), so these are what make eBay participate.
#
# Note this does NOT enable eBay. Enablement is membership in app_settings.platforms, which
# these defaults never touch.
EBAY_DEFAULT_SETTINGS = {
    "manual_fallback": True,
    "min_batch_size": 100,
    "allow_resubmit": True,
    "requires_images": True,
    "require_type_mapping": True,
    "require_color_mapping": True,
    "require_brand_mapping": True,
}


def _hydrate_platform_settings(platform_settings: dict) -> dict:
    merged = dict(platform_settings or {})
    for platform_id, defaults in (
        ("spo", SPO_DEFAULT_SETTINGS),
        ("grailed", GRAILED_DEFAULT_SETTINGS),
        ("ebay", EBAY_DEFAULT_SETTINGS),
    ):
        platform = dict(merged.get(platform_id) or {})
        for key, default_value in defaults.items():
            platform.setdefault(key, default_value)
        merged[platform_id] = platform
    return merged


@router.get("/platform_settings", response_model=PlatformSettingsResponse)
async def get_platform_settings():
    try:
        settings = await AppSettings.first()

        if not settings:
            settings = await AppSettings.create(field_templates={}, platform_settings={})

        return PlatformSettingsResponse(
            platform_settings=_hydrate_platform_settings(settings.platform_settings or {}),
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
