import logging
from typing import List, Dict, Any, Optional
from tortoise.exceptions import IntegrityError
from models.db_models import Template
from models.api_models import (
    CreateTemplateRequest,
    UpdateTemplateRequest,
    UpdateTemplateWithFieldsRequest,
    AddFieldToTemplateRequest,
    UpdateTemplateFieldRequest,
    ReorderTemplateFieldsRequest,
    TemplateResponse,
)
from services.sellercloud_service import sellercloud_service
from services.listing_options_service import listing_options_service
from services.product_info_service import ProductInfoService
import traceback

logger = logging.getLogger(__name__)


class TemplateService:

    _cache_by_id: Dict[str, TemplateResponse] = {}
    _cache_by_name: Dict[str, TemplateResponse] = {}

    @classmethod
    def _add_to_cache(cls, template: TemplateResponse):
        template_id = str(template.id)
        cls._cache_by_id[template_id] = template
        cls._cache_by_name[template.name] = template

    @classmethod
    def _remove_from_cache(cls, template_name: str):
        if template_name in cls._cache_by_name:
            cached_template = cls._cache_by_name.pop(template_name, None)
            if cached_template:
                template_id = str(cached_template.id)
                cls._cache_by_id.pop(template_id, None)

    @classmethod
    async def get_all_templates(cls, active_only: bool = True) -> List[TemplateResponse]:
        try:
            query = Template.all()
            if active_only:
                query = query.filter(is_active=True)

            templates = await query.order_by("name")

            response_list = []
            for template in templates:
                template_response = TemplateResponse(
                    id=template.id,
                    name=template.name,
                    display_name=template.display_name,
                    description=template.description,
                    field_definitions=template.field_definitions or [],
                    field_count=template.field_count,
                    is_active=template.is_active,
                    created_at=template.created_at,
                    updated_at=template.updated_at,
                )
                response_list.append(template_response)
                cls._add_to_cache(template_response)

            return response_list

        except Exception as e:
            logger.error(f"Error fetching templates: {e}")
            raise

    @classmethod
    async def get_template_by_name(cls, template_name: str) -> Optional[TemplateResponse]:
        if template_name in cls._cache_by_name:
            return cls._cache_by_name[template_name]
        try:
            template = await Template.get_or_none(name=template_name)

            if not template:
                return None

            template_response = TemplateResponse(
                id=template.id,
                name=template.name,
                display_name=template.display_name,
                description=template.description,
                field_definitions=template.field_definitions or [],
                field_count=template.field_count,
                is_active=template.is_active,
                created_at=template.created_at,
                updated_at=template.updated_at,
            )
            cls._add_to_cache(template_response)
            return template_response

        except Exception as e:
            logger.error(f"Error fetching template {template_name}: {e}")
            raise

    @classmethod
    async def get_template_by_id(cls, template_id: str) -> Optional[TemplateResponse]:
        if template_id in cls._cache_by_id:
            return cls._cache_by_id[template_id]
        try:
            template = await Template.get_or_none(id=template_id)

            if not template:
                return None

            template_response = TemplateResponse(
                id=template.id,
                name=template.name,
                display_name=template.display_name,
                description=template.description,
                field_definitions=template.field_definitions or [],
                field_count=template.field_count,
                is_active=template.is_active,
                created_at=template.created_at,
                updated_at=template.updated_at,
            )
            cls._add_to_cache(template_response)
            return template_response

        except Exception as e:
            logger.error(f"Error fetching template with id {template_id}: {e}")
            raise

    @classmethod
    async def create_template(cls, request: CreateTemplateRequest) -> TemplateResponse:
        try:
            field_definitions_dict = []
            for i, field_def in enumerate(request.field_definitions):
                field_dict = field_def.model_dump()
                field_dict["order"] = i
                field_definitions_dict.append(field_dict)

            template = await Template.create(
                name=request.name,
                display_name=request.display_name,
                description=request.description,
                field_definitions=field_definitions_dict,
            )

            logger.info(f"Created template: {template.name}")

            template_response = TemplateResponse(
                id=template.id,
                name=template.name,
                display_name=template.display_name,
                description=template.description,
                field_definitions=template.field_definitions or [],
                field_count=template.field_count,
                is_active=template.is_active,
                created_at=template.created_at,
                updated_at=template.updated_at,
            )
            cls._add_to_cache(template_response)
            return template_response

        except IntegrityError as e:
            logger.error(f"Template name {request.name} already exists: {e}")
            raise ValueError(f"Template with name '{request.name}' already exists")
        except Exception as e:
            logger.error(f"Error creating template: {e}")
            raise

    @classmethod
    async def update_template(
        cls, template_name: str, request: UpdateTemplateRequest
    ) -> Optional[TemplateResponse]:
        try:
            template = await Template.get_or_none(name=template_name)

            if not template:
                return None

            if request.display_name is not None:
                template.display_name = request.display_name
            if request.description is not None:
                template.description = request.description
            if request.is_active is not None:
                template.is_active = request.is_active

            await template.save()

            logger.info(f"Updated template: {template.name}")

            template_response = TemplateResponse(
                id=template.id,
                name=template.name,
                display_name=template.display_name,
                description=template.description,
                field_definitions=template.field_definitions or [],
                field_count=template.field_count,
                is_active=template.is_active,
                created_at=template.created_at,
                updated_at=template.updated_at,
            )
            cls._add_to_cache(template_response)
            return template_response

        except Exception as e:
            logger.error(f"Error updating template {template_name}: {e}")
            raise

    @classmethod
    async def update_template_with_fields(
        cls, template_name: str, request: UpdateTemplateWithFieldsRequest
    ) -> Optional[TemplateResponse]:
        try:
            template = await Template.filter(name=template_name).first()
            if not template:
                return None

            if request.display_name is not None:
                template.display_name = request.display_name
            if request.description is not None:
                template.description = request.description
            if request.is_active is not None:
                template.is_active = request.is_active

            if request.field_definitions is not None:
                field_defs = []
                for field_def in request.field_definitions:
                    field_dict = field_def.dict() if hasattr(field_def, "dict") else field_def
                    field_defs.append(field_dict)
                template.field_definitions = field_defs

                try:
                    await ProductInfoService.sync_columns_with_template(field_defs)
                except Exception as e:
                    logger.warning(f"Failed to sync product_info columns: {e}")

            await template.save()

            template_response = TemplateResponse(
                id=template.name,
                name=template.name,
                display_name=template.display_name,
                description=template.description,
                field_definitions=template.field_definitions or [],
                field_count=len(template.field_definitions or []),
                is_active=template.is_active,
                created_at=template.created_at,
                updated_at=template.updated_at,
            )
            cls._add_to_cache(template_response)
            return template_response

        except Exception as e:
            logger.error(f"Error updating template with fields {template_name}: {e}")
            raise

    @classmethod
    async def add_field_to_template(
        cls, request: AddFieldToTemplateRequest
    ) -> Optional[TemplateResponse]:
        try:
            template = await Template.get_or_none(name=request.template_name)

            if not template:
                return None

            if template.get_field_by_name(request.field.name):
                raise ValueError(
                    f"Field with name '{request.field.name}' already exists in template"
                )

            field_dict = request.field.model_dump()
            template.add_field(field_dict)

            try:
                await ProductInfoService.add_column(request.field.name, request.field.type)
            except Exception as e:
                logger.warning(
                    f"Failed to add product_info column for field '{request.field.name}': {e}"
                )

            await template.save()

            logger.info(f"Added field '{request.field.name}' to template '{template.name}'")

            template_response = TemplateResponse(
                id=template.id,
                name=template.name,
                display_name=template.display_name,
                description=template.description,
                field_definitions=template.field_definitions or [],
                field_count=template.field_count,
                is_active=template.is_active,
                created_at=template.created_at,
                updated_at=template.updated_at,
            )
            cls._add_to_cache(template_response)
            return template_response

        except Exception as e:
            logger.error(f"Error adding field to template {request.template_name}: {e}")
            raise

    @classmethod
    async def update_template_field(
        cls, request: UpdateTemplateFieldRequest
    ) -> Optional[TemplateResponse]:
        try:
            template = await Template.get_or_none(name=request.template_name)

            if not template:
                return None

            if not template.field_definitions:
                raise ValueError("Template has no fields to update")

            field_found = False
            for field in template.field_definitions:
                if field.get("name") == request.field_name:
                    field_found = True
                    for key, value in request.update_data.items():
                        if value is not None:
                            field[key] = value
                    break

            if not field_found:
                raise ValueError(f"Field '{request.field_name}' not found in template")

            await template.save()

            logger.info(f"Updated field '{request.field_name}' in template '{template.name}'")

            template_response = TemplateResponse(
                id=template.id,
                name=template.name,
                display_name=template.display_name,
                description=template.description,
                field_definitions=template.field_definitions or [],
                field_count=template.field_count,
                is_active=template.is_active,
                created_at=template.created_at,
                updated_at=template.updated_at,
            )
            cls._add_to_cache(template_response)
            return template_response

        except Exception as e:
            logger.error(f"Error updating field in template {request.template_name}: {e}")
            raise

    @classmethod
    async def remove_field_from_template(
        cls, template_name: str, field_name: str
    ) -> Optional[TemplateResponse]:
        try:
            template = await Template.get_or_none(name=template_name)

            if not template:
                return None

            removed = template.remove_field(field_name)

            if not removed:
                raise ValueError(f"Field '{field_name}' not found in template")

            await template.save()

            logger.info(f"Removed field '{field_name}' from template '{template.name}'")

            template_response = TemplateResponse(
                id=template.id,
                name=template.name,
                display_name=template.display_name,
                description=template.description,
                field_definitions=template.field_definitions or [],
                field_count=template.field_count,
                is_active=template.is_active,
                created_at=template.created_at,
                updated_at=template.updated_at,
            )
            cls._add_to_cache(template_response)
            return template_response

        except Exception as e:
            logger.error(f"Error removing field from template {template_name}: {e}")
            raise

    @classmethod
    async def reorder_template_fields(
        cls, request: ReorderTemplateFieldsRequest
    ) -> Optional[TemplateResponse]:
        try:
            template = await Template.get_or_none(name=request.template_name)

            if not template:
                return None

            template.reorder_fields(request.field_order)
            await template.save()

            logger.info(f"Reordered fields in template '{template.name}'")

            template_response = TemplateResponse(
                id=template.id,
                name=template.name,
                display_name=template.display_name,
                description=template.description,
                field_definitions=template.field_definitions or [],
                field_count=template.field_count,
                is_active=template.is_active,
                created_at=template.created_at,
                updated_at=template.updated_at,
            )
            cls._add_to_cache(template_response)
            return template_response

        except Exception as e:
            logger.error(f"Error reordering fields in template {request.template_name}: {e}")
            raise

    @classmethod
    async def delete_template(cls, template_name: str) -> bool:
        try:
            template = await Template.get_or_none(name=template_name)

            if not template:
                return False

            template.is_active = False
            await template.save()
            cls._remove_from_cache(template.name)

            logger.info(f"Deleted template: {template.name}")
            return True

        except Exception as e:
            logger.error(f"Error deleting template {template_name}: {e}")
            raise

    @staticmethod
    async def get_product_fields() -> List[Dict[str, Any]]:
        try:
            fields = await sellercloud_service.get_product_fields()
            return fields
        except Exception:
            logger.error(f"Error getting product fields: {traceback.format_exc()}")
            raise

    @staticmethod
    async def search_product_fields(query: str) -> List[Dict[str, Any]]:
        try:
            fields = await sellercloud_service.get_product_fields()

            if not query:
                return fields

            query_lower = query.lower()
            filtered_fields = []

            for field in fields:
                field_id = field.get("ID", "").lower()
                if query_lower in field_id:
                    field_with_display = field.copy()
                    field_with_display["display_name"] = str(field_id).replace("_", " ").title()
                    filtered_fields.append(field_with_display)

            return filtered_fields

        except Exception as e:
            logger.error(f"Error searching product fields: {e}")
            raise

    @staticmethod
    async def get_listing_tables() -> List[Dict[str, Any]]:
        try:
            tables = await listing_options_service.get_tables()
            return tables
        except Exception as e:
            logger.error(f"Error getting listing tables: {e}")
            raise

    @classmethod
    async def get_template_fields(cls, template_id: str = "default") -> List[Dict[str, Any]]:
        merged_fields = {}

        try:
            sc_fields = await sellercloud_service.get_product_fields()
            for field in sc_fields:
                field_id = field.get("ID", "")
                if field_id:
                    merged_fields[field_id] = {
                        "id": field_id,
                        "display_name": field_id.replace("_", " ").title(),
                        "tags": field.get("tags", []),
                        "source": "sellercloud",
                    }
        except Exception as e:
            logger.warning(f"Failed to load SellerCloud fields: {e}")

        template = await cls.get_template_by_id(template_id)
        if template and template.field_definitions:
            for field_def in template.field_definitions:
                field_id = field_def.get("name")
                if field_id:
                    display_name = field_def.get("display_name", field_id)
                    merged_fields[field_id] = {
                        "id": field_id,
                        "display_name": display_name,
                        "tags": [],
                        "source": "template",
                    }

        return sorted(merged_fields.values(), key=lambda x: x["id"].lower())
