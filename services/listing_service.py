import asyncio
import logging
import re
import time
from dataclasses import dataclass
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
from services.ebay_aspect_service import ebay_aspect_service
from services.ebay_service import EbayService
from services.ai_service import AIService
from services.listing_options_service import listing_options_service
from services.product_service import format_mpn
from services.sellercloud_service import sellercloud_service
from services.template_render import render_template, resolve_field_template
from services.template_service import TemplateService
import orjson
from tortoise import connections

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AiAspectField:
    """An eBay aspect presented to the AI prompt builder.

    Duck-types the handful of attributes AIService reads off a template field
    (`.name`, `.type`, `.options`, `.multiselect`, `.ai_tagging`) without pretending to be
    one. A FieldDefinition cannot represent an eBay aspect: its `name` validator requires a
    Python identifier and most aspect names contain spaces.
    """

    name: str
    type: str
    options: Optional[List[Any]] = None
    multiselect: bool = False
    ai_tagging: bool = True


class ListingService:
    DEFAULT_CUSTOM_COLUMNS = [
        "SIZING_SCHEME",
        "GENDER",
    ]

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
    async def _get_ebay_ai_aspects(
        product_type: Optional[str], category_id: Optional[str] = None
    ) -> tuple[List["AiAspectField"], Dict[str, List[Any]]]:
        """eBay aspects to hand the AI, resolved for one eBay category.

        Settings are aspect level, but the aspect's allowed values are not: `Brand` offers
        19,161 values under Men's Dress Shirts and 4,523 under Women's Dresses, so the
        category has to be decided before the values are asked for. `category_id` is the
        listing's; without one the type's default is used. Silent no-op when the type maps
        nowhere, which is the case for 103 of 239 types today.

        These are NOT FieldDefinitions. FieldDefinition.name must be a valid Python
        identifier, and eBay aspect names routinely contain spaces ("Size Type", "Sleeve
        Length", "Outer Shell Material"), so most of them can never satisfy that validator.
        AIService only reads .name/.type/.options/.multiselect/.ai_tagging off these, so a
        narrow carrier is both sufficient and honest about not being a template field.
        """
        if not product_type:
            return [], {}
        try:
            # Same gate as the form path. Without it the model is asked to fill aspects for
            # a platform that is switched off, and its answers land in listings.data where
            # nothing later distinguishes them from values an operator chose.
            if not await ebay_aspect_service.is_enabled():
                return [], {}
            category_id = await ebay_aspect_service.resolve_listing_category(
                product_type, category_id
            )
            if not category_id:
                return [], {}
            rows = await ebay_aspect_service.get_ai_aspects_for_category(
                product_type, category_id
            )
        except Exception as e:  # noqa: BLE001 - AI enrichment must never block creation
            logger.warning(f"Could not load eBay AI aspects for {product_type!r}: {e}")
            return [], {}

        fields, options = [], {}
        for row in rows:
            values = row["values"] or None
            fields.append(
                AiAspectField(
                    name=row["aspect_name"],
                    type=row["field_type"],
                    options=values,
                    multiselect=row["cardinality"] == "MULTI",
                )
            )
            if values:
                options[row["aspect_name"]] = values
        return fields, options

    @staticmethod
    async def _generate_product_name(data: Dict[str, Any]) -> str:
        # Internal field ids, not the SellerCloud ones. listings.data was renamed by
        # transform_listing_data_to_internal_fields.sql (2026-01-22) and this default
        # was missed, so every placeholder resolved to empty and the generated title
        # came out blank.
        DEFAULT_TEMPLATE = "{brand_name} {brand_color/standard_color} {style_name}"

        template = DEFAULT_TEMPLATE
        try:
            settings = await AppSettings.first()
            if settings:
                resolved = resolve_field_template(
                    settings.field_templates, "sellercloud", "title"
                )
                if resolved:
                    template = resolved
        except Exception as e:
            logger.warning(f"Failed to fetch product name template, using default: {e}")

        logger.debug(f"Using product name template: {template}")

        product_name, _missing = render_template(template, data, collapse_whitespace=True)

        logger.debug(f"Generated title: {product_name}")
        return product_name

    @staticmethod
    async def _check_photos_uploaded(product_id: str) -> bool:
        try:
            photo_conn = connections.get("photography_db")
            # Ordered by created_at, not updated_at: editing a product's washtags
            # writes to its older batch_creation row (see image_service), which
            # would otherwise make that row look like the current one and report
            # a product whose photos are uploaded as still pending.
            rows = await photo_conn.execute_query_dict(
                """
                SELECT image_source
                FROM productimages
                WHERE product_id = $1
                ORDER BY created_at DESC
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
    def normalize_mpn(data: Dict[str, Any]) -> bool:
        """Canonicalize manufacturer_sku (the MPN) in place. Returns True if changed.

        Reuses product_service.format_mpn, the same function SKU creation uses, so
        a listing and the SKUs built from it can never disagree about the same MPN.

        Normalized at the source rather than per platform because the value is read
        from this one key by every consumer: SellerCloud's ManufacturerSKU, and the
        {manufacturer_sku} placeholder that appears in BOTH the sellercloud and
        grailed description templates. Formatting it at each of those would be three
        copies of the same rule. SPO maps it to `sku` in the template, but that column
        is overwritten with the child SKU (spo_service), so SPO is unaffected either
        way.

        Idempotent, so re-running it on a normalized value is a no-op. The return
        value lets the submit path skip a pointless write when nothing changed.
        """
        mpn = data.get("manufacturer_sku")
        if mpn is None:
            return False
        # No whitespace-only special case: format_mpn collapses "   " to "", which is
        # what it means. Leaving it would be the one value that escapes normalization.
        formatted = format_mpn(str(mpn))
        if formatted == mpn:
            return False
        data["manufacturer_sku"] = formatted
        logger.info(f"Normalized MPN {mpn!r} -> {formatted!r}")
        return True

    @staticmethod
    async def _apply_product_type_derived(
        data: Dict[str, Any],
        *,
        fallback_product_type: Optional[str] = None,
    ) -> None:
        """Refresh the fields derived from the product type so the data stays in sync.

        GENDER and shipping_weight are both fully server-derived: neither is in the
        form, so neither can carry an operator override, and both are recomputed on
        every save. A stale GENDER is what makes a SellerCloud submit fail on a
        missing template field; a stale weight silently ships the wrong
        PackageWeightLbs/Oz and the wrong SPO weight. Recomputing unconditionally is
        also what makes correcting item_weight_oz in Listing Options actually reach
        existing listings, which is the replacement for the form field that used to
        let an operator fix it by hand. The product type alias is canonicalized.

        listings.data is keyed by internal field ids, so the type is read from and
        written back to "product_type". This used to read "ProductType" and write the
        canonical value there too, which had two consequences: the function was a
        complete no-op on the update path (which passes no fallback, so the lookup
        found nothing and returned early), and on the create path it minted a second
        uppercase key, leaving rows where the form and SPO read one key and
        SellerCloud the other. The SellerCloud id survives only as the create-time
        fallback, which comes straight off the raw catalog payload.

        Mutates `data` in place; never raises (a lookup failure leaves the existing
        values untouched rather than blanking them).
        """
        product_type = data.get("product_type") or fallback_product_type
        if not product_type:
            return

        try:
            info = await listing_options_service.get_product_type_info(product_type)
        except Exception as e:
            logger.warning(f"Failed to fetch product type info for {product_type}: {e}")
            return

        if info.get("is_alias_match"):
            canonical_type = info.get("type")
            if canonical_type:
                data["product_type"] = canonical_type
                logger.info(
                    f"Replaced product type alias '{product_type}' with canonical type '{canonical_type}'"
                )

        if info.get("gender") is not None:
            data["GENDER"] = info["gender"]
            logger.debug(f"Set GENDER to {info['gender']} from types table")

        # Guarded rather than unconditional: an unrecognised type resolves no weight,
        # and leaving the existing value alone is always better than blanking it. The
        # SellerCloud submit path drops empty values from the payload entirely.
        if info.get("item_weight_oz") is not None:
            data["shipping_weight"] = int(info["item_weight_oz"])
            logger.debug(
                f"Set shipping_weight to {info['item_weight_oz']} from types table"
            )

    @staticmethod
    @staticmethod
    async def _run_ai_search_inline(
        request: CreateListingRequest, prefilled_data: Dict[str, Any], enabled: bool
    ) -> Optional[Dict[str, Any]]:
        """Run the AI web/tag search now, or return None to leave it to the poller.

        Never raises. A search that fails must not cost a listing, let alone the
        whole batch it is in: this runs inside create_batch's transaction, where
        an exception rolls back every listing created so far. The caller falls
        back to queueing whatever comes back None, so nothing is lost either way.
        """
        if not enabled:
            return None
        try:
            from services.ai_search_service import is_configured, run_for_fields

            if not is_configured():
                return None
            return await run_for_fields(
                prefilled_data,
                product_id=request.info_product_id or request.product_id,
                parent_product_id=request.product_id,
                reason="listing_create",
            )
        except Exception:  # noqa: BLE001
            logger.exception(
                f"Inline AI search failed for {request.product_id}; it will be queued instead"
            )
            return None

    @staticmethod
    async def create_listing(
        request: CreateListingRequest,
        created_by: str,
        sellercloud_template: Optional[TemplateResponse] = None,
        mapped_options: Optional[Dict[str, List[Any]]] = None,
        ai_search_inline: bool = False,
    ) -> ListingResponse:
        """Create one listing, prefilled from SellerCloud and filled in by AI.

        ai_search_inline runs the AI web/tag search alongside the aspects call
        rather than queueing it for the poller, so the suggestions are already on
        the listing when the operator opens it. It roughly triples how long this
        takes (~5s to ~18s), which is why the caller decides: single-listing
        creation always does it, and a batch only when it is small enough to
        finish inside the caller's timeout. See BatchService.INLINE_AI_SEARCH_MAX.
        """
        try:
            ai_response_data = None
            ai_description = None
            original_description = None
            verification = None
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
                product_data = await sellercloud_service.get_product_for_listing(
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

                    await ListingService._apply_product_type_derived(
                        prefilled_data,
                        fallback_product_type=product_data.get("ProductType"),
                    )

                    # standard_color is prefilled from SellerCloud's COLOR custom
                    # column, which holds the brand's own name for the color
                    # ("Apollo | Rock"), not a canonical one. SC's BRAND_COLOR
                    # column is empty across the catalog, so that raw value is
                    # the only thing carrying the brand color: capture it before
                    # the lookup below rewrites standard_color.
                    color = prefilled_data.get("standard_color")
                    if color:
                        # Unconditional, not just on an alias hit: when the brand
                        # simply calls it "Black" the two end up equal, which is
                        # truthful (there is no separate brand name) and leaves
                        # the required brand_color field populated instead of
                        # absent. Set outside the try so a lookup failure still
                        # yields a brand color.
                        if not prefilled_data.get("brand_color"):
                            prefilled_data["brand_color"] = color

                        try:
                            color_info = await listing_options_service.get_color_info(color)

                            # Assign whenever the lookup resolved rather than
                            # gating on is_alias_match: an exact hit is a no-op,
                            # and a case-only difference ("grey") gets normalised
                            # instead of being treated as an alias.
                            canonical_color = color_info.get("color")
                            if canonical_color:
                                prefilled_data["standard_color"] = canonical_color
                                if canonical_color != color:
                                    logger.info(
                                        f"Resolved color '{color}' to canonical color '{canonical_color}', brand_color is '{prefilled_data.get('brand_color')}'"
                                    )
                        except Exception as e:
                            logger.warning(f"Failed to fetch color info for {color}: {e}")

                    original_description = prefilled_data.get("LongDescription")

                    fields_for_ai = ListingService._get_ai_tagging_fields(
                        sellercloud_template.field_definitions or []
                    )

                    # eBay aspects with AI tagging on, for whatever category this listing's
                    # product type maps to. They are appended as ordinary AI fields because
                    # the AI path is entirely name-driven and the prompt already speaks
                    # eBay's language (aspectName / aspectOptions / itemToAspectCardinality
                    # in utils/prompts/aspects_prompt.txt).
                    #
                    # The options MUST come from this listing's category. The same aspect
                    # name carries a different list per category: Brand offers 19,161 values
                    # under Men's Dress Shirts and 4,523 under Women's Dresses, and handing
                    # the model the wrong list is how it invents a brand the category will
                    # not accept.
                    ebay_type = prefilled_data.get("product_type")
                    ebay_categories = (
                        await ebay_aspect_service.get_categories_for_type(ebay_type)
                        if ebay_type and await ebay_aspect_service.is_enabled()
                        else []
                    )
                    # The category is decided in THIS call, not a follow-up turn. The type
                    # is already known here, so nothing has to wait for the model; and on
                    # Chat Completions reasoning is discarded between turns, so a
                    # conversation would buy only prefix-cache reuse of the images while
                    # tripling the reasoning passes and inviting the model to anchor on its
                    # own turn-1 answer.
                    #
                    # Skipped when there is one candidate, which is every type today: asking
                    # a vision model to choose from a list of one is pure cost.
                    if len(ebay_categories) > 1:
                        fields_for_ai = fields_for_ai + [
                            AiAspectField(
                                name="ebay_category_id",
                                type="text",
                                options=[c["category_id"] for c in ebay_categories],
                            )
                        ]

                    ebay_fields, ebay_options = await ListingService._get_ebay_ai_aspects(
                        ebay_type,
                        ebay_categories[0]["category_id"] if ebay_categories else None,
                    )
                    if ebay_fields:
                        fields_for_ai = fields_for_ai + ebay_fields

                    if fields_for_ai:
                        if mapped_options is None:
                            mapped_options = await ListingService._load_mapped_options(
                                sellercloud_template.field_definitions or []
                            )
                        # Template mappings win on a name clash: an operator who mapped a
                        # template field to a list column chose that list deliberately.
                        mapped_options = {**ebay_options, **(mapped_options or {})}

                        # The two model calls go together rather than one after
                        # the other. They need nothing from each other: aspects
                        # reads the photographs, the search reads the tag and the
                        # web, and the fields the search checks (brand, MPN,
                        # style) are already in prefilled_data from SellerCloud -
                        # ai_response has never once carried manufacturer_sku or
                        # brand_color, measured across 3,418 listings. So the
                        # pair costs max(3.4s, ~18s) instead of their sum.
                        ai_content, verification = await asyncio.gather(
                            AIService.generate_ai_content(
                                product_data, fields_for_ai, mapped_options
                            ),
                            ListingService._run_ai_search_inline(
                                request, prefilled_data, ai_search_inline
                            ),
                        )
                        ai_response_data = ai_content.get("aspects")
                        ai_description = ai_content.get("description")

                        if ai_response_data:
                            # Never take the model's word for a category id. An unmapped one
                            # would render aspects the submit path will never send, and it
                            # is stored on the listing where nothing later re-checks it.
                            chosen = ai_response_data.get("ebay_category_id")
                            if chosen is not None:
                                allowed = {c["category_id"] for c in ebay_categories}
                                if str(chosen) in allowed:
                                    ai_response_data["ebay_category_id"] = str(chosen)
                                else:
                                    logger.warning(
                                        "AI chose eBay category %r for %r, which is not one "
                                        "of %s. Falling back to the type default.",
                                        chosen,
                                        ebay_type,
                                        sorted(allowed),
                                    )
                                    ai_response_data.pop("ebay_category_id", None)

                            for key, value in ai_response_data.items():
                                if key not in prefilled_data:
                                    prefilled_data[key] = value

                        if ai_description:
                            prefilled_data["description"] = ai_description
                            logger.debug("Set LongDescription to AI-generated description")

            # The SellerCloud product name as prefilled, captured before the block below
            # rewrites it from the title template. This is what the title field's restore
            # button reverts to, so it is read here and never written again.
            original_title = prefilled_data.get("title")

            style_name = prefilled_data.get("style_name")
            if style_name and len(str(style_name).strip()) >= 3:
                generated_name = await ListingService._generate_product_name(prefilled_data)
                if generated_name:
                    prefilled_data["title"] = generated_name
                    logger.debug(f"Generated title '{generated_name}' from template")
            elif "title" in prefilled_data:
                # product_data is still keyed by SellerCloud field ids, so ProductName is
                # correct on that side; prefilled_data is keyed by internal field ids.
                product_name_source = None
                if product_data and product_data.get("ProductName"):
                    product_name_source = product_data["ProductName"]
                else:
                    product_name_source = prefilled_data.get("title")

                if isinstance(product_name_source, str) and product_name_source.strip():
                    prefilled_data["title"] = re.split(
                        r"\s+size\s+",
                        product_name_source,
                        flags=re.IGNORECASE,
                        maxsplit=1,
                    )[0].strip()

            # The description template's {ID} placeholder is the parent seller
            # SKU, which request.product_id already holds for every caller.
            prefilled_data["ID"] = request.product_id

            # Last, so it also catches an MPN that arrived from the SellerCloud
            # prefill or from AI rather than from the operator. original_data below
            # is snapshotted after this, so the creation baseline records the
            # normalized value that would actually be submitted.
            ListingService.normalize_mpn(prefilled_data)

            upload_status = "pending"
            if await ListingService._check_photos_uploaded(request.product_id):
                upload_status = "uploaded"

            listing = await Listing.create(
                product_id=request.product_id,
                info_product_id=request.info_product_id,
                assigned_to=request.assigned_to,
                data=prefilled_data,
                # The creation-time baseline: what prefill and AI handed the
                # operator, before any edit. Write-once, so nothing else in the
                # codebase may assign it. A separate dict so the two can never
                # alias. Not on ListingResponse: read it from the database.
                original_data=dict(prefilled_data),
                ai_response=ai_response_data,
                ai_description=ai_description,
                original_description=original_description,
                original_title=original_title,
                upload_status=upload_status,
                created_by=created_by,
            )

            if verification:
                # A targeted UPDATE rather than a column on the model: everywhere
                # else this value is written by the poller minutes later, and a
                # bare listing.save() from a stale instance must never be able to
                # put an old copy back. Inside the same transaction, so a
                # rolled-back batch takes its verifications with it.
                await connections.get("default").execute_query(
                    "UPDATE listings SET ai_search = $2::jsonb WHERE id = $1",
                    [str(listing.id), orjson.dumps(verification).decode()],
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
                # ID is server-owned. A client payload that omits it must not
                # be able to drop it, since the description template requires it.
                new_data = {**request.data, "ID": listing.product_id}
                # Recompute the type-derived fields (GENDER, shipping_weight, and the
                # canonical product type) so editing the product type and saving keeps
                # them in sync, rather than leaving a stale GENDER the submit path would
                # reject or a weight that no longer matches the type. This call did
                # nothing at all until 2026-07-31: it looked up "ProductType" while
                # listings.data is keyed "product_type", so it returned immediately.
                await ListingService._apply_product_type_derived(new_data)
                ListingService.normalize_mpn(new_data)
                listing.data = new_data

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

            # Sent alongside `data` by the listing form, never on its own, so the flag and
            # the hand-edited title it protects land in the same write. Split across two
            # requests they would race, and the loser would be the edit.
            if request.title_auto_update is not None:
                listing.title_auto_update = request.title_auto_update

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
    async def get_latest_listing_by_product_id(product_id: str) -> Optional[Listing]:
        """Most recent listing for a parent SKU, submitted or not, batched or not.

        Product search wants whatever listing already exists for the product, so the
        operator lands on the previously submitted data instead of a second listing
        row for the same parent. get_draft_listing_by_product_id stays as it is for
        the batch paths: those may only ever adopt an unattached draft, never a
        submitted listing or one belonging to another batch.
        """
        try:
            return await Listing.filter(product_id=product_id).order_by("-created_at").first()
        except Exception as e:
            logger.error(f"Error fetching latest listing for product {product_id}: {e}")
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
    def _ebay_category_field(
        candidates: List[Dict[str, Any]], category_id: str
    ) -> Dict[str, Any]:
        """The category selector, as a template-shaped field dict.

        A real schema field rather than a bespoke control, so it participates in formData,
        autosave and validation without a parallel path. `enum` carries the ids and
        `ui:enumNames` the paths, which is what lets the operator read a breadcrumb while
        the listing stores something stable.

        A `default` ONLY when the type maps to exactly one category. RJSF materialises a
        JSON Schema default into formData and the next autosave freezes it into the
        listing, which is why a multi-candidate type still carries none: the type's default
        can be changed later and a frozen copy would not hear about it.

        With one candidate there is nothing to drift from. The stored value is identical to
        what resolve_listing_category would return for an empty listing, and if the type is
        later remapped, that resolver IGNORES a stored id which is no longer among the
        type's candidates and falls back to the current default. So the freeze cannot
        outlive the mapping, which is the hazard the multi-candidate case is avoiding.

        Making the operator open a dropdown to pick its only entry is a keystroke that can
        have no other outcome, the same reasoning as a single-value aspect.

        `order` is the fallback for a category that offers no Department aspect (5 of the
        62 reachable ones). _get_ebay_form_aspects overrides it to sit beside Department
        everywhere else. Explicit rather than relying on 999 being both the aspects' base
        and the sort key's default: a tie-break is not a statement of intent.

        ui_size 12 because the eBay group now renders in its own half-width column beside
        the description, so a size here is measured against that column rather than the
        form: 12 is half the form, which is where this field's breadcrumb labels (up to 255
        characters) stop truncating past the second level. It was 8, pairing with
        Department's 4 to fill one row back when the group sat full width beneath the form.
        Unlike the aspects, this field is synthesized rather than stored in
        pm_ebay_aspect_settings, so its width is not settable from the eBay aspects page.
        """
        field = {
            "name": "ebay_category_id",
            "display_name": "eBay category",
            "type": "text",
            "options": [c["category_id"] for c in candidates],
            "option_labels": [
                " > ".join(c["path"]) if c["path"] else c["name"] for c in candidates
            ],
            "display_in_form": True,
            "section": "ebay",
            "order": 950,
            "ui_size": 12,
        }
        if len(candidates) == 1:
            field["default"] = candidates[0]["category_id"]
        return field

    @staticmethod
    async def _get_ebay_form_aspects(
        product_type: Optional[str], category_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """eBay aspects to render on the listing form, as template-shaped field dicts.

        Nothing is returned at all while eBay is a disabled platform, the category selector
        included. An earlier version rendered the section unrequired so values could be
        authored ahead of enabling, but a section for a platform that cannot be submitted
        to is a dead end on the busiest screen in the app, and the selector on its own is
        worse: it invites a choice that decides nothing.

        Once eBay IS enabled the aspects become JSON Schema `required`. The submit button
        is disabled while any validation error stands (ListingView.jsx:5203), and that
        check does not know which platform a field belongs to, so marking an eBay aspect
        required while eBay is switched off would block submission to SPO and Grailed over
        a field nothing consumes yet.
        """
        if not product_type:
            return []
        try:
            # First, and before any eBay query: on a database where eBay is off this
            # returns after one read instead of three, and that read is shared with the
            # mapping lookup below.
            settings = await AppSettings.first()
            if not await ebay_aspect_service.is_enabled(settings):
                return []
            # Resolved, not trusted: an id that is not this type's is ignored in favour of
            # the default, so a hand-edited query string cannot render aspects for a
            # category the submit path will never send.
            category_id = await ebay_aspect_service.resolve_listing_category(
                product_type, category_id
            )
            if not category_id:
                # The type maps to no eBay category. Return the PICKER ALONE rather than
                # nothing, so the "+" that adds one has somewhere to live. These 88 of 234
                # types are precisely the ones that need it, and they were the only ones it
                # could not reach: with no eBay field in the schema there is no eBay section,
                # and with no section there is nothing to hang the button on.
                #
                # No candidates, so _convert_template_to_schema sets neither `enum` nor
                # ui:widget "select" -- an empty options list is falsy there -- and the field
                # arrives as a plain string that buildUiSchema still routes to
                # EbayCategoryWidget by name.
                #
                # An excluded type stays hidden regardless: the form drops the whole section
                # from ebayDefaults.excluded_by, which this function cannot answer for anyway
                # because it never sees the brand.
                return [ListingService._ebay_category_field([], None)]
            candidates = await ebay_aspect_service.get_categories_for_type(product_type)
            ebay_settings = (
                (settings.platform_settings if settings else None) or {}
            ).get("ebay") or {}
            aspects = await ebay_aspect_service.get_form_aspects_for_category(
                product_type,
                category_id,
                mark_required=True,
                ebay_settings=ebay_settings,
            )
            category_field = ListingService._ebay_category_field(candidates, category_id)
            # Placed relative to Department rather than at a fixed number. Department's
            # sort_order runs 1 to 5 across the 62 reachable categories, so no constant
            # sits after it everywhere. The half step keeps it ahead of whatever aspect
            # follows. _convert_template_to_schema sorts on `order`, so list position here
            # decides nothing.
            anchor = next((a for a in aspects if a.get("name") == "Department"), None)
            if anchor is not None:
                category_field["order"] = anchor.get("order", 999) + 0.5
            return aspects + [category_field]
        except Exception as e:  # noqa: BLE001 - the form must render without eBay
            logger.warning(f"Could not load eBay form aspects for {product_type!r}: {e}")
            return []

    @staticmethod
    async def get_listing_schema(
        template_id: str,
        product_type: Optional[str] = None,
        ebay_category_id: Optional[str] = None,
    ) -> Optional[ListingSchemaResponse]:
        try:
            template = await Template.get_or_none(id=template_id)
            if not template:
                return None

            # eBay aspects set to "On form" belong to the eBay category the product type
            # maps to, so the schema differs per type and the caller has to say which.
            # Without a type the schema is the template alone, which is what every caller
            # got before eBay aspects existed.
            ebay_fields = await ListingService._get_ebay_form_aspects(
                product_type, ebay_category_id
            )

            # eBay fields go through the same options load: a `form` aspect may carry an
            # optional mapping target, and the point of that mapping is to take the list
            # from a SkuBase table instead of eBay's own.
            mapped_options = await ListingService._load_mapped_options(
                (template.field_definitions or []) + ebay_fields
            )

            json_schema, ui_schema = await ListingService._convert_template_to_schema(
                template, mapped_options, ebay_fields
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
        template: Template,
        mapped_options: Dict[str, List[Any]] = None,
        extra_fields: List[Dict[str, Any]] = None,
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        """Build the form schema from the template, plus any per-listing extra fields.

        `extra_fields` are template-shaped dicts contributed by a platform (today: eBay
        aspects the operator put on the form). They run through this same loop rather than
        a builder of their own, so they pick up every type, constraint and widget rule the
        template fields get.
        """
        template_fields = template.field_definitions or []

        # A name collision would have the extra field overwrite a template property, so
        # the template keeps the name and the loser is reported rather than dropped in
        # silence. Template field names are Python identifiers and eBay aspect names carry
        # spaces, so in practice this is a guard, not a routine path.
        owned = {f.get("name") for f in template_fields}
        accepted_extras = []
        for extra in extra_fields or []:
            if extra.get("name") in owned:
                logger.warning(
                    f"Ignoring extra field {extra.get('name')!r}: the template already "
                    "defines a field with that name"
                )
                continue
            accepted_extras.append(extra)

        all_fields = list(template_fields) + accepted_extras
        if not all_fields:
            return {"type": "object", "properties": {}, "required": []}, {}

        if mapped_options is None:
            mapped_options = {}

        json_schema_props = {}
        ui_schema_props = {}
        required_fields = []

        sorted_fields = sorted(all_fields, key=lambda f: f.get("order", 999))

        for field in sorted_fields:
            field_name = field.get("name")
            if not field_name or not field.get("display_in_form", True):
                continue

            prop = {"title": field.get("display_name", field_name)}
            ui_prop = {}

            # eBay values for an aspect eBay will not enforce. Deliberately NOT an enum:
            # RJSF would validate against it and reject a value eBay would have accepted.
            # Carried in the uiSchema instead, where a freeSolo widget can offer them.
            suggestions = field.get("suggestions")
            if suggestions and not field.get("options"):
                ui_prop["ui:suggestions"] = suggestions
                ui_prop["ui:widget"] = "SuggestAutocomplete"

            ui_size = field.get("ui_size")
            if ui_size and isinstance(ui_size, int) and 1 <= ui_size <= 12:
                ui_prop["ui:grid"] = {"xs": ui_size}

            # Read by CustomObjectFieldTemplate, which lifts sectioned fields out of the
            # main grid and renders them under their own heading beneath it.
            if field.get("section"):
                ui_prop["ui:section"] = field["section"]

            # What the field will send while left empty. Carried as a placeholder rather
            # than a JSON Schema `default` on purpose: RJSF materialises a default into
            # formData, and the next save would freeze it into the listing, so a later
            # change to that default could never reach the listing again.
            if field.get("placeholder"):
                ui_prop["ui:placeholder"] = field["placeholder"]

            field_type = field.get("type")

            field_options = mapped_options.get(field_name, field.get("options"))

            # Paired positionally with `enum`, so they are only emitted together. A
            # mapped_table list can override `options` while option_labels passes through
            # untouched, and a desynced pair means the operator picks one category and the
            # listing stores another.
            option_labels = field.get("option_labels")
            if option_labels and field_options and len(option_labels) == len(field_options):
                ui_prop["ui:enumNames"] = option_labels
            elif option_labels:
                logger.warning(
                    f"Dropping ui:enumNames for {field_name}: "
                    f"{len(option_labels)} labels for {len(field_options or [])} options"
                )


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
            company_code=await EbayService.company_code(listing.product_id)
            if listing.product_id
            else None,
            info_product_id=listing.info_product_id,
            assigned_to=listing.assigned_to,
            data=listing.data,
            ai_response=listing.ai_response,
            ai_description=listing.ai_description,
            original_description=listing.original_description,
            original_title=listing.original_title,
            title_auto_update=listing.title_auto_update,
            submitted=listing.submitted,
            submitted_at=listing.submitted_at,
            submitted_by=listing.submitted_by,
            submitted_platforms=submitted_platforms,
            upload_status=listing.upload_status,
            created_by=listing.created_by,
            created_at=listing.created_at,
            updated_at=listing.updated_at,
        )
