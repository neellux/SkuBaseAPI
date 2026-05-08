import logging
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from models.api_models import (
    CreateListingRequest,
    FieldDefinition,
    ListingResponse,
    ListingSchemaResponse,
    TemplateResponse,
    UpdateListingRequest,
)
from models.db_models import AppSettings, Listing, Template
from services.ai_service import AIService
from services.listing_options_service import listing_options_service
from services.sellercloud_service import sellercloud_service
from services.template_service import TemplateService
from tortoise import connections

logger = logging.getLogger(__name__)


class ListingService:
    DEFAULT_CUSTOM_COLUMNS = [
        "SIZING_SCHEME",
        "GENDER",
    ]

    DEFAULT_NORMAL_FIELDS = ["ID"]

    @staticmethod
    def _get_ai_tagging_fields(
        field_definitions: List[Dict[str, Any]],
    ) -> List[FieldDefinition]:
        fields_for_ai = []
        if not field_definitions:
            return fields_for_ai

        for field_dict in field_definitions:
            try:
                field = FieldDefinition(**field_dict)
                if field.ai_tagging:
                    fields_for_ai.append(field)
            except ValueError as e:
                logger.warning(
                    f"Skipping invalid field definition for AI tagging: {field_dict.get('name', 'unknown')} - {e}"
                )

        return fields_for_ai

    @staticmethod
    async def _generate_product_name(data: Dict[str, Any]) -> str:
        DEFAULT_TEMPLATE = "{BrandName} {BRAND_COLOR/COLOR} {STYLE_NAME}"

        try:
            settings = await AppSettings.first()
            if settings and settings.field_templates:
                template = settings.field_templates.get("ProductName")
                if not template:
                    template = DEFAULT_TEMPLATE
            else:
                template = DEFAULT_TEMPLATE
        except Exception as e:
            logger.warning(f"Failed to fetch product name template, using default: {e}")
            template = DEFAULT_TEMPLATE

        logger.debug(f"Using product name template: {template}")

        placeholder_pattern = r"\{([^}]+)\}"
        matches = re.finditer(placeholder_pattern, template)

        populated_template = template
        for match in matches:
            placeholder_content = match.group(1)
            placeholder_full = match.group(0)

            if "/" in placeholder_content:
                field_options = [opt.strip() for opt in placeholder_content.split("/")]

                replacement_value = None
                for field_name in field_options:
                    field_value = data.get(field_name)
                    if field_value is not None and str(field_value).strip() != "":
                        replacement_value = str(field_value).strip()
                        logger.debug(
                            f"Fallback placeholder {placeholder_full}: using '{field_name}' = '{replacement_value}'"
                        )
                        break

                if replacement_value:
                    populated_template = populated_template.replace(
                        placeholder_full, replacement_value
                    )
                else:
                    populated_template = populated_template.replace(placeholder_full, "")
                    logger.debug(
                        f"No valid value found for fallback placeholder {placeholder_full}, removing"
                    )
            else:
                field_name = placeholder_content.strip()
                field_value = data.get(field_name)

                if field_value is not None and str(field_value).strip() != "":
                    replacement_value = str(field_value).strip()
                    populated_template = populated_template.replace(
                        placeholder_full, replacement_value
                    )
                    logger.debug(
                        f"Placeholder {placeholder_full}: replaced with '{replacement_value}'"
                    )
                else:
                    populated_template = populated_template.replace(placeholder_full, "")
                    logger.debug(
                        f"No valid value found for placeholder {placeholder_full}, removing"
                    )

        product_name = " ".join(populated_template.split())

        logger.debug(f"Generated ProductName: {product_name}")
        return product_name

    @staticmethod
    async def _check_photos_uploaded(product_id: str) -> bool:
        try:
            photo_conn = connections.get("photography_db")
            rows = await photo_conn.execute_query_dict(
                """
                SELECT image_source
                FROM productimages
                WHERE product_id = $1
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                [product_id],
            )
            if not rows:
                return False
            return rows[0].get("image_source") in ("upload", "manual")
        except Exception as e:
            logger.warning(f"Failed to check photo upload status for {product_id}: {e}")
            return False

    @staticmethod
    async def _load_mapped_options(
        field_definitions: List[Dict[str, Any]],
    ) -> Dict[str, List[Any]]:
        if not field_definitions:
            return {}

        mapped_fields = []
        for field in field_definitions:
            if field.get("mapped_table") and field.get("mapped_column"):
                mapped_fields.append(
                    {
                        "name": field.get("name"),
                        "table": field.get("mapped_table"),
                        "column": field.get("mapped_column"),
                    }
                )

        logger.info(f"Loading options for {len(mapped_fields)} mapped fields")
        if not mapped_fields:
            return {}

        options_map = {}

        table_schemas = {}
        try:
            tables = await listing_options_service.get_tables()
            for table in tables:
                table_name = table.get("table")
                if table_name and table.get("column_schema"):
                    table_schemas[table_name] = {}
                    for column in table["column_schema"]:
                        column_name = column.get("name")
                        if column_name:
                            table_schemas[table_name][column_name] = column
            logger.info(f"Loaded schemas for {len(table_schemas)} tables from API")
        except Exception as e:
            logger.warning(
                f"Could not fetch table schemas from API: {e}. Will query database for all fields."
            )

        fields_needing_db_query = []

        for field in mapped_fields:
            field_name = field["name"]
            table = field["table"]
            column = field["column"]

            if table in table_schemas and column in table_schemas[table]:
                column_schema = table_schemas[table][column]
                predefined_options = column_schema.get("options")

                if predefined_options and len(predefined_options) > 0:
                    options_map[field_name] = predefined_options
                    logger.debug(
                        f"Using {len(predefined_options)} predefined options for {field_name} from {table}.{column}"
                    )
                else:
                    fields_needing_db_query.append(field)
            else:
                fields_needing_db_query.append(field)

        if fields_needing_db_query:
            logger.info(
                f"Querying database for {len(fields_needing_db_query)} fields without predefined options"
            )
            try:
                union_queries = []
                for field in fields_needing_db_query:
                    union_queries.append(
                        f"SELECT DISTINCT '{field['name']}' as field_name, {field['column']}::TEXT as option_value "
                        f"FROM listingoptions_{field['table']} WHERE {field['column']} IS NOT NULL"
                    )

                full_query = " UNION ALL ".join(union_queries)

                conn = connections.get("default")
                results = await conn.execute_query_dict(full_query)

                for row in results:
                    field_name = row["field_name"]
                    option_value = row["option_value"]

                    if field_name not in options_map:
                        options_map[field_name] = []

                    if option_value is not None and str(option_value).strip():
                        options_map[field_name].append(option_value)

                for field in fields_needing_db_query:
                    field_name = field["name"]
                    if field_name in options_map:
                        options_map[field_name] = sorted(set(options_map[field_name]))
                        logger.debug(
                            f"Loaded {len(options_map[field_name])} options from database for {field_name}"
                        )

            except Exception as e:
                logger.error(f"Error loading options from listing_options database: {e}")

        logger.info(f"Successfully loaded options for {len(options_map)} fields total")
        return options_map

    @staticmethod
    async def create_listing(
        request: CreateListingRequest,
        created_by: str,
        sellercloud_template: Optional[TemplateResponse] = None,
        mapped_options: Optional[Dict[str, List[Any]]] = None,
    ) -> ListingResponse:
        try:
            ai_response_data = None
            ai_description = None
            original_description = None
            start_time = time.time()
            product_data = None

            if sellercloud_template is None:
                sellercloud_template = await TemplateService.get_template_by_id("default")

            if not sellercloud_template:
                logger.warning(
                    "default template not found, creating listing with provided data only"
                )
                prefilled_data = request.data
            else:
                product_data = await sellercloud_service.get_product_by_id(
                    request.product_id, only_required_fields=False
                )

                if not product_data:
                    logger.warning(
                        f"Product {request.product_id} not found in SellerCloud, using provided data only"
                    )
                    prefilled_data = request.data
                else:
                    prefilled_data = await ListingService._process_product_data_for_template(
                        product_data, sellercloud_template, request.data
                    )

                    product_type = prefilled_data.get("ProductType") or product_data.get(
                        "ProductType"
                    )
                    if product_type:
                        try:
                            product_type_info = await listing_options_service.get_product_type_info(
                                product_type
                            )

                            if product_type_info.get("is_alias_match"):
                                canonical_type = product_type_info.get("type")
                                if canonical_type:
                                    prefilled_data["ProductType"] = canonical_type
                                    logger.info(
                                        f"Replaced ProductType alias '{product_type}' with canonical type '{canonical_type}'"
                                    )

                            if product_type_info.get("gender") is not None:
                                prefilled_data["GENDER"] = product_type_info["gender"]
                                logger.debug(
                                    f"Set GENDER to {product_type_info['gender']} from types table"
                                )

                            if product_type_info.get("item_weight_oz") is not None:
                                prefilled_data["shipping_weight"] = int(
                                    product_type_info["item_weight_oz"]
                                )
                                logger.debug(
                                    f"Set ShippingWeight to {product_type_info['item_weight_oz']} from types table"
                                )
                        except Exception as e:
                            logger.warning(
                                f"Failed to fetch product type info for {product_type}: {e}"
                            )

                    color = prefilled_data.get("standard_color")
                    if color:
                        try:
                            color_info = await listing_options_service.get_color_info(color)

                            if color_info.get("is_alias_match"):
                                canonical_color = color_info.get("color")
                                if canonical_color:
                                    prefilled_data["standard_color"] = canonical_color
                                    if not prefilled_data.get("brand_color"):
                                        prefilled_data["brand_color"] = color
                                        logger.info(
                                            f"Replaced color alias '{color}' with canonical color '{canonical_color}', set brand_color to '{color}'"
                                        )
                                    else:
                                        logger.info(
                                            f"Replaced color alias '{color}' with canonical color '{canonical_color}', preserved existing brand_color '{prefilled_data.get('BRAND_COLOR')}'"
                                        )
                        except Exception as e:
                            logger.warning(f"Failed to fetch color info for {color}: {e}")

                    original_description = prefilled_data.get("LongDescription")

                    fields_for_ai = ListingService._get_ai_tagging_fields(
                        sellercloud_template.field_definitions or []
                    )

                    if fields_for_ai:
                        if mapped_options is None:
                            mapped_options = await ListingService._load_mapped_options(
                                sellercloud_template.field_definitions or []
                            )

                        ai_content = await AIService.generate_ai_content(
                            product_data, fields_for_ai, mapped_options
                        )
                        ai_response_data = ai_content.get("aspects")
                        ai_description = ai_content.get("description")

                        if ai_response_data:
                            for key, value in ai_response_data.items():
                                if key not in prefilled_data:
                                    prefilled_data[key] = value

                        if ai_description:
                            prefilled_data["description"] = ai_description
                            logger.debug("Set LongDescription to AI-generated description")

            style_name = prefilled_data.get("STYLE_NAME")
            if style_name and len(str(style_name).strip()) >= 3:
                generated_name = await ListingService._generate_product_name(prefilled_data)
                if generated_name:
                    prefilled_data["ProductName"] = generated_name
                    logger.debug(f"Generated ProductName '{generated_name}' from template")
            elif "ProductName" in prefilled_data:
                product_name_source = None
                if product_data and product_data.get("ProductName"):
                    product_name_source = product_data["ProductName"]
                else:
                    product_name_source = prefilled_data.get("ProductName")

                if isinstance(product_name_source, str) and product_name_source.strip():
                    prefilled_data["ProductName"] = re.split(
                        r"\s+size\s+",
                        product_name_source,
                        flags=re.IGNORECASE,
                        maxsplit=1,
                    )[0].strip()

            upload_status = "pending"
            if await ListingService._check_photos_uploaded(request.product_id):
                upload_status = "uploaded"

            listing = await Listing.create(
                product_id=request.product_id,
                info_product_id=request.info_product_id,
                assigned_to=request.assigned_to,
                data=prefilled_data,
                ai_response=ai_response_data,
                ai_description=ai_description,
                original_description=original_description,
                upload_status=upload_status,
                created_by=created_by,
            )

            return await ListingService._to_response(listing)

        except Exception as e:
            logger.error(f"Error creating listing: {e}")
            raise

    @staticmethod
    async def get_listing_by_id(listing_id: str) -> Optional[ListingResponse]:
        try:
            listing = await Listing.get_or_none(id=listing_id)
            if not listing:
                return None

            return await ListingService._to_response(listing)

        except Exception as e:
            logger.error(f"Error fetching listing {listing_id}: {e}")
            raise

    @staticmethod
    async def update_listing(
        listing_id: str, request: UpdateListingRequest
    ) -> Optional[ListingResponse]:
        try:
            listing = await Listing.get_or_none(id=listing_id)
            if not listing:
                return None

            if request.assigned_to is not None:
                listing.assigned_to = request.assigned_to

            if request.data is not None:
                listing.data = request.data

            if request.ai_response is not None:
                listing.ai_response = request.ai_response

            if request.ai_description is not None:
                listing.ai_description = request.ai_description

            if request.submitted is not None:
                listing.submitted = request.submitted
                if request.submitted and not listing.submitted_at:
                    listing.submitted_at = datetime.now()

            if request.submitted_by is not None:
                listing.submitted_by = request.submitted_by

            await listing.save()

            return await ListingService._to_response(listing)

        except Exception as e:
            logger.error(f"Error updating listing {listing_id}: {e}")
            raise

    @staticmethod
    async def delete_listing(listing_id: str) -> bool:
        try:
            listing = await Listing.get_or_none(id=listing_id)
            if not listing:
                return False
            await listing.delete()
            logger.info(f"Deleted listing {listing_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting listing {listing_id}: {e}")
            raise

    @staticmethod
    async def get_draft_listing_by_product_id(product_id: str) -> Optional[Listing]:
        try:
            listing = (
                await Listing.filter(
                    product_id=product_id, submitted=False, batch_id=None
                )
                .order_by("-created_at")
                .first()
            )
            return listing
        except Exception as e:
            logger.error(f"Error fetching draft listing for product {product_id}: {e}")
            raise

    @staticmethod
    async def get_all_listings(
        assigned_to: Optional[str] = None,
        submitted: Optional[bool] = None,
        page: int = 1,
        page_size: int = 50,
    ) -> tuple[List[ListingResponse], int]:
        try:
            query = Listing.all()

            if assigned_to is not None:
                query = query.filter(assigned_to=assigned_to)

            if submitted is not None:
                query = query.filter(submitted=submitted)

            total = await query.count()

            listings = (
                await query.offset((page - 1) * page_size).limit(page_size).order_by("-created_at")
            )

            response_listings = []
            for listing in listings:
                response_listings.append(await ListingService._to_response(listing))

            return response_listings, total

        except Exception as e:
            logger.error(f"Error fetching listings: {e}")
            raise

    @staticmethod
    async def get_listing_schema(template_id: str) -> Optional[ListingSchemaResponse]:
        try:
            template = await Template.get_or_none(id=template_id)
            if not template:
                return None

            mapped_options = await ListingService._load_mapped_options(
                template.field_definitions or []
            )

            json_schema, ui_schema = await ListingService._convert_template_to_schema(
                template, mapped_options
            )

            return ListingSchemaResponse(
                json_schema=json_schema,
                ui_schema=ui_schema,
                template_info={
                    "id": template.id,
                    "name": template.name,
                    "display_name": template.display_name,
                    "description": template.description,
                },
            )

        except Exception as e:
            logger.error(f"Error generating listing schema for template {template_id}: {e}")
            raise

    @staticmethod
    async def _convert_template_to_schema(
        template: Template, mapped_options: Dict[str, List[Any]] = None
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        if not template.field_definitions:
            return {"type": "object", "properties": {}, "required": []}, {}

        if mapped_options is None:
            mapped_options = {}

        json_schema_props = {}
        ui_schema_props = {}
        required_fields = []

        sorted_fields = sorted(template.field_definitions, key=lambda f: f.get("order", 999))

        for field in sorted_fields:
            field_name = field.get("name")
            if not field_name or not field.get("display_in_form", True):
                continue

            prop = {"title": field.get("display_name", field_name)}
            ui_prop = {}

            ui_size = field.get("ui_size")
            if ui_size and isinstance(ui_size, int) and 1 <= ui_size <= 12:
                ui_prop["ui:grid"] = {"xs": ui_size}

            field_type = field.get("type")

            field_options = mapped_options.get(field_name, field.get("options"))

            if field_type == "text":
                prop["type"] = "string"
                if field_options:
                    if field.get("multiselect"):
                        prop["type"] = "array"
                        prop["items"] = {"type": "string", "enum": field_options}
                        ui_prop["ui:widget"] = "checkboxes"
                    else:
                        prop["enum"] = field_options
                        ui_prop["ui:widget"] = "select"

                else:
                    if field.get("min") is not None:
                        prop["minLength"] = int(field["min"])
                    if field.get("max") is not None:
                        prop["maxLength"] = int(field["max"])
                    if field.get("regex"):
                        prop["pattern"] = field["regex"]
                        if field.get("regex_error_message"):
                            ui_prop.setdefault("ui:options", {})["errorMessage"] = field[
                                "regex_error_message"
                            ]

            elif field_type == "number":
                prop["type"] = "number"
                if field_options:
                    try:
                        prop["enum"] = [float(o) for o in field_options]
                        ui_prop["ui:widget"] = "select"
                    except (ValueError, TypeError):
                        pass
                else:
                    if field.get("min") is not None:
                        prop["minimum"] = float(field["min"])
                    if field.get("max") is not None:
                        prop["maximum"] = float(field["max"])

            elif field_type == "bool":
                prop["type"] = "boolean"
                ui_prop["ui:widget"] = "checkbox"

            elif field_type == "text_list":
                prop["type"] = "array"
                prop["uniqueItems"] = True
                prop["items"] = {"type": "string"}

                if field_options:
                    prop["items"]["enum"] = field_options
                else:
                    if field.get("min") is not None:
                        prop["items"]["minLength"] = int(field["min"])
                    if field.get("max") is not None:
                        prop["items"]["maxLength"] = int(field["max"])
                    if field.get("regex"):
                        prop["items"]["pattern"] = field["regex"]
                        if field.get("regex_error_message"):
                            ui_prop.setdefault("ui:options", {})["errorMessage"] = field[
                                "regex_error_message"
                            ]
                    ui_prop["ui:widget"] = "TagsWidget"

            elif field_type == "rich_text":
                prop["type"] = "string"
                prop["format"] = "rich_text"

                if field.get("min") is not None:
                    prop["minLength"] = int(field["min"])
                if field.get("max") is not None:
                    prop["maxLength"] = int(field["max"])
                if field.get("regex"):
                    prop["pattern"] = field["regex"]
                    if field.get("regex_error_message"):
                        ui_prop.setdefault("ui:options", {})["errorMessage"] = field[
                            "regex_error_message"
                        ]

                ui_prop["ui:widget"] = "RichTextWidget"
                ui_prop.setdefault("ui:options", {})["multiline"] = True

            if field.get("default") is not None:
                prop["default"] = field["default"]

            json_schema_props[field_name] = prop
            if ui_prop:
                ui_schema_props[field_name] = ui_prop

            if field.get("is_required", False):
                required_fields.append(field_name)

        final_json_schema = {
            "type": "object",
            "title": template.display_name or template.name,
            "properties": json_schema_props,
            "required": required_fields,
        }

        return final_json_schema, ui_schema_props

    @staticmethod
    async def _process_product_data_for_template(
        product_data: Dict[str, Any],
        template: TemplateResponse,
        user_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        try:
            prefilled_data = user_data.copy()

            if not template.field_definitions:
                return prefilled_data

            custom_columns = product_data.get("CustomColumns", [])
            custom_columns_map = {}
            for custom_col in custom_columns:
                column_name = custom_col.get("ColumnName", "")
                if column_name:
                    custom_columns_map[column_name] = custom_col.get("Value")

            for field_def in template.field_definitions:
                field_name = field_def.get("name")
                if not field_name or field_name in prefilled_data:
                    continue

                platforms = field_def.get("platforms") or []
                sc_mapping = None
                for platform in platforms:
                    if platform.get("platform_id") == "sellercloud":
                        sc_mapping = platform
                        break

                if sc_mapping:
                    sc_field_id = sc_mapping.get("field_id")
                    is_custom = sc_mapping.get("is_custom", False)

                    if is_custom:
                        if sc_field_id in custom_columns_map:
                            column_value = custom_columns_map[sc_field_id]
                            if column_value is not None and str(column_value).strip() != "":
                                prefilled_data[field_name] = column_value
                                logger.debug(
                                    f"Prefilled custom field '{field_name}' (SC: {sc_field_id}) with value: {column_value}"
                                )
                    else:
                        if sc_field_id in product_data:
                            product_value = product_data[sc_field_id]
                            if product_value is not None and str(product_value).strip() != "":
                                prefilled_data[field_name] = product_value
                                logger.debug(
                                    f"Prefilled standard field '{field_name}' (SC: {sc_field_id}) with value: {product_value}"
                                )
                else:
                    platform_tags = field_def.get("platform_tags", [])

                    if "custom" in platform_tags:
                        if field_name in custom_columns_map:
                            column_value = custom_columns_map[field_name]
                            if column_value is not None and str(column_value).strip() != "":
                                prefilled_data[field_name] = column_value
                                logger.debug(
                                    f"Prefilled custom field '{field_name}' with value: {column_value}"
                                )
                    else:
                        if field_name in product_data:
                            product_value = product_data[field_name]
                            if product_value is not None and str(product_value).strip() != "":
                                prefilled_data[field_name] = product_value
                                logger.debug(
                                    f"Prefilled standard field '{field_name}' with value: {product_value}"
                                )

            for field_name in ListingService.DEFAULT_CUSTOM_COLUMNS:
                if field_name in prefilled_data:
                    continue

                if field_name in custom_columns_map:
                    column_value = custom_columns_map[field_name]
                    if column_value is not None and str(column_value).strip() != "":
                        prefilled_data[field_name] = column_value
                        logger.debug(
                            f"Prefilled hardcoded custom field '{field_name}' with value: {column_value}"
                        )

            for field_name in ListingService.DEFAULT_NORMAL_FIELDS:
                if field_name in prefilled_data:
                    continue

                if field_name in product_data:
                    product_value = product_data[field_name]
                    if product_value is not None and str(product_value).strip() != "":
                        if field_name == "ID" and "/" in str(product_value):
                            parent_id = str(product_value).split("/")[0]
                            prefilled_data[field_name] = parent_id
                            logger.debug(
                                f"Prefilled hardcoded normal field '{field_name}' with parent ID: {parent_id}"
                            )
                        else:
                            prefilled_data[field_name] = product_value
                            logger.debug(
                                f"Prefilled hardcoded normal field '{field_name}' with value: {product_value}"
                            )

            logger.info(
                f"Prefilled {len(prefilled_data)} fields for product {product_data.get('ID', 'unknown')}"
            )
            return prefilled_data

        except Exception as e:
            logger.error(f"Error processing product data for template: {e}")
            return user_data

    @staticmethod
    async def _to_response(listing: Listing) -> ListingResponse:
        successful_submissions = await listing.submissions.filter(status="success").all()
        submitted_platforms = list(set(s.platform_id for s in successful_submissions))
        return ListingResponse(
            id=str(listing.id),
            product_id=listing.product_id,
            info_product_id=listing.info_product_id,
            assigned_to=listing.assigned_to,
            data=listing.data,
            ai_response=listing.ai_response,
            ai_description=listing.ai_description,
            original_description=listing.original_description,
            submitted=listing.submitted,
            submitted_at=listing.submitted_at,
            submitted_by=listing.submitted_by,
            submitted_platforms=submitted_platforms,
            upload_status=listing.upload_status,
            created_by=listing.created_by,
            created_at=listing.created_at,
            updated_at=listing.updated_at,
        )
