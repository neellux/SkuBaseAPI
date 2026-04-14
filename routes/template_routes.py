import logging
from typing import List

from fastapi import APIRouter, HTTPException, Query
from models.api_models import (
    AddFieldToTemplateRequest,
    CreateTemplateRequest,
    ProductFieldSearchResponse,
    ReorderTemplateFieldsRequest,
    TemplateResponse,
    UpdateTemplateFieldRequest,
    UpdateTemplateRequest,
    UpdateTemplateWithFieldsRequest,
)
from services.template_service import TemplateService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/templates", tags=["templates"])


@router.get("/list", response_model=List[TemplateResponse])
async def list_templates(
    active_only: bool = Query(True, description="Filter by active templates only"),
):
    try:
        return await TemplateService.get_all_templates(active_only=active_only)
    except Exception as e:
        logger.error(f"Error fetching templates: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/product_fields")
async def get_product_fields():
    try:
        fields = await TemplateService.get_product_fields()
        return [
            ProductFieldSearchResponse(
                id=field.get("ID", ""),
                tags=field.get("tags", []),
                display_name=field.get("ID", ""),
            )
            for field in fields
        ]
    except Exception as e:
        logger.error(f"Error getting product fields: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/search_product_fields")
async def search_product_fields(
    q: str = Query("", description="Search query for product fields"),
):
    try:
        fields = await TemplateService.search_product_fields(q)
        return [
            ProductFieldSearchResponse(
                id=field.get("ID", ""),
                tags=field.get("tags", []),
                display_name=field.get("display_name", field.get("ID", "")),
            )
            for field in fields
        ]
    except Exception as e:
        logger.error(f"Error searching product fields: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/template_fields")
async def get_template_fields(
    template_id: str = Query("default", description="Template ID to get field definitions from"),
):
    try:
        fields = await TemplateService.get_template_fields(template_id)
        return fields
    except Exception as e:
        logger.error(f"Error getting template fields: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/listingoptions_meta")
async def get_listing_tables():
    try:
        tables = await TemplateService.get_listing_tables()
        return tables
    except ValueError as e:
        logger.warning(f"Validation error getting listing tables: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting listing tables: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/detail", response_model=TemplateResponse)
async def get_template(
    template_id: str = Query(..., description="The ID of the template to retrieve"),
):
    try:
        template = await TemplateService.get_template_by_id(template_id)
        if not template:
            raise HTTPException(
                status_code=404, detail=f"Template with ID '{template_id}' not found"
            )
        return template
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching template: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/create", response_model=TemplateResponse)
async def create_template(request: CreateTemplateRequest):
    try:
        return await TemplateService.create_template(request)
    except ValueError as e:
        logger.warning(f"Validation error creating template: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating template: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to create template")


@router.put("", response_model=TemplateResponse)
async def update_template(
    request: UpdateTemplateRequest,
    template_name: str = Query(..., description="The name of the template to update"),
):
    try:
        template = await TemplateService.update_template(template_name, request)
        if not template:
            raise HTTPException(status_code=404, detail=f"Template '{template_name}' not found")
        return template
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating template: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to update template")


@router.put("/with_fields", response_model=TemplateResponse)
async def update_template_with_fields(
    request: UpdateTemplateWithFieldsRequest,
    template_name: str = Query(..., description="The name of the template to update"),
):
    try:
        template = await TemplateService.update_template_with_fields(template_name, request)
        if not template:
            raise HTTPException(status_code=404, detail=f"Template '{template_name}' not found")
        return template
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating template with fields: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to update template")


@router.post("/add_field", response_model=TemplateResponse)
async def add_field_to_template(request: AddFieldToTemplateRequest):
    try:
        template = await TemplateService.add_field_to_template(request)
        if not template:
            raise HTTPException(
                status_code=404, detail=f"Template '{request.template_name}' not found"
            )
        return template
    except ValueError as e:
        logger.warning(f"Validation error adding field to template: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error adding field to template: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to add field to template")


@router.put("/update_field", response_model=TemplateResponse)
async def update_template_field(request: UpdateTemplateFieldRequest):
    try:
        template = await TemplateService.update_template_field(request)
        if not template:
            raise HTTPException(
                status_code=404, detail=f"Template '{request.template_name}' not found"
            )
        return template
    except ValueError as e:
        logger.warning(f"Validation error updating template field: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error updating template field: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to update template field")


@router.delete("/field", response_model=TemplateResponse)
async def remove_field_from_template(
    template_name: str = Query(..., description="The name of the template"),
    field_name: str = Query(..., description="The name of the field to remove"),
):
    try:
        template = await TemplateService.remove_field_from_template(template_name, field_name)
        if not template:
            raise HTTPException(status_code=404, detail=f"Template '{template_name}' not found")
        return template
    except ValueError as e:
        logger.warning(f"Validation error removing field from template: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error removing field from template: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to remove field from template")


@router.post("/reorder_fields", response_model=TemplateResponse)
async def reorder_template_fields(request: ReorderTemplateFieldsRequest):
    try:
        template = await TemplateService.reorder_template_fields(request)
        if not template:
            raise HTTPException(
                status_code=404, detail=f"Template '{request.template_name}' not found"
            )
        return template
    except Exception as e:
        logger.error(f"Error reordering template fields: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to reorder template fields")


@router.delete("")
async def delete_template(
    template_name: str = Query(..., description="The name of the template to delete"),
):
    try:
        deleted = await TemplateService.delete_template(template_name)
        if not deleted:
            raise HTTPException(status_code=404, detail=f"Template '{template_name}' not found")
        return {"message": f"Template '{template_name}' deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting template: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to delete template")
