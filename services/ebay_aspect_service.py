"""Read access to the eBay aspect reference data and its per-category configuration.

The reference tables (pm_ebay_categories, pm_ebay_category_aspects, pm_ebay_aspect_values)
are loaded from the offline dump by scripts/ebay_load_dump_to_db.py. Nothing here calls the
eBay API.

See docs/plans/2026-08-05-feat-ebay-aspect-mapping-plan.md
"""

import logging
from collections import Counter
from typing import Any, Dict, List, Optional

import orjson
from tortoise import Tortoise

from models.db_models import AppSettings
from services.listing_options_service import listing_options_service

logger = logging.getLogger(__name__)

DEFAULT_MARKETPLACE = "EBAY_US"

# A value list longer than this is served by typeahead rather than inlined into the schema.
# Of the 1,631 (category, aspect) pairs a Lux type can actually reach, 1,579 sit at or below
# it, so 96.8% filter client-side with no request per keystroke. What stays above is
# essentially one aspect: Brand, at up to 19,161 values, plus Model, Product Line and
# Silhouette. A 2,991-value list is 39 kB of JSON before gzip.
INLINE_VALUE_LIMIT = 3000

# The same decision for the AI prompt, which cannot take the UI's number: 3,000 options is
# not a list, it is most of a context window. At 1,000 the prompt gains 35 more lists, the
# largest holding 876 values (~2,700-3,300 tokens), and the worst category in that band
# offers 4 of them totalling 1,127 values. 70 lists stay above the cut and the model is
# asked for free text instead.
AI_VALUE_LIMIT = 1000

VALID_SOURCES = ("mapped_field", "form", "type_based")

# Where a per-platform mapping stands between a SkuBase value and what eBay accepts. An
# aspect fed from one of these is answered by the mapping dialog the submit gate already
# raises, so eBay must not ask for it a second time: a per-aspect value beside a mapping is
# a second source that can only disagree with it.
#
# Keyed on WHAT THE ASPECT IS MAPPED TO, not on its name. `Color` mapped to `brand_color`
# is free text off the listing and needs no mapping at all; the same aspect mapped to
# `standard_color` is a SkuBase colour that only the colour mapping can turn into an eBay
# one. The aspect name is identical in both cases.
#
# `flag` None means unconditional: the size gate has no require_size_mapping and runs for
# every non-SellerCloud platform whenever the listing has a sizing scheme and children.
MAPPING_SOURCES = (
    # (mapping, template fields, listingoptions tables, platform_settings flag)
    ("brand", {"brand_name"}, {"brands", "listingoptions_brands"}, "require_brand_mapping"),
    ("color", {"standard_color"}, {"colors", "listingoptions_colors"}, "require_color_mapping"),
    ("size", set(), {"sizes", "listingoptions_sizes"}, None),
)

# `Size Type` has no mapped field of its own: it is the PREFIX of the size mapping row
# ("Regular 32"), so one mapping feeds both it and `Size`. That is why the prefix carries
# meaning for eBay and is discarded on Grailed.
SIZE_PREFIX_ASPECTS = {"Size Type"}

# Form order. The operator's sort_order leads when set; everything untouched keeps eBay's
# relative order behind it, so placing two aspects never requires placing all 167. Ties fall
# through to eBay's order then the name, which makes the result total rather than arbitrary.
#
# Needed because eBay's own ordering contradicts itself: across the 62 reachable categories
# 662 of 3,278 co-occurring aspect pairs (20.2%) flip their relative order by category.
ASPECT_ORDER_SQL = (
    "s.sort_order IS NULL, s.sort_order, a.is_required DESC, a.sort_order, a.aspect_name"
)


def derive_display_name(aspect_name: str) -> str:
    """Default label for an aspect: eBay's own name, unchanged.

    No prefix. On the eBay Fields page every row is an eBay aspect already, so prefixing
    all 167 of them says nothing and costs the width that the name needs. See
    derive_form_label for the one place the prefix earns itself.
    """
    return aspect_name


def derive_form_label(display_name: Optional[str], aspect_name: str) -> str:
    """Label where an eBay aspect sits among a listing's own fields.

    Here the prefix is the whole point: `Department` and `Style` are indistinguishable
    from native template fields once they are rendered in the same form.

    An operator override is used verbatim and NOT prefixed, matching how display_name
    behaves everywhere else in this layer: null means "derive one", set means "use exactly
    this". Someone who types a label has said what they want it to read.
    """
    return display_name or f"Ebay {aspect_name}"


def derive_sellercloud_field(aspect_name: str) -> str:
    """Default SellerCloud column name: eBay's aspect name, verbatim.

    Taken from sellercloud_ebay_import_template.xls, whose header row is the eBay aspect
    names unchanged -- spaces, slashes and all (`Size Type`, `Country/Region of
    Manufacture`, `Jacket/Coat Length`). 161 of the 167 aspect names a Lux type can reach
    match a column exactly.

    This replaced a guess of `Ebay` + the name with spaces stripped (`EbaySize`), which the
    template shows was wrong on both counts. Nothing had been stored under the old rule --
    every settings row still held NULL -- so the correction reaches every aspect at once
    and there is nothing to migrate.

    Still derived rather than written into the rows: only a handful of the 4,926 aspect
    names have a settings row at all, so storing this would mean inventing 167 of them and
    leaving a future correction unable to tell a default from a deliberate edit.
    """
    return aspect_name


def _placeholder(default) -> Optional[str]:
    """A default rendered for a text input. None when there is no default."""
    if default is None:
        return None
    if isinstance(default, list):
        return ", ".join(str(v) for v in default)
    return str(default)


def decode_json(value, default):
    """Decode a JSONB column returned by a raw query.

    Tortoise's execute_query_dict hands JSONB back as the raw string asyncpg produced, not
    a parsed object: `path` arrives as '["Clothing...", "Men"]' and `overrides` as '{}'.
    Spreading that string into a dict raises TypeError, and treating it as a list silently
    produces per-character nonsense. Same fix as alias_bulk_import_job_service.py:64.
    """
    if value is None:
        return default
    if isinstance(value, (str, bytes)):
        try:
            return orjson.loads(value)
        except orjson.JSONDecodeError:
            logger.warning("Could not decode JSON column value: %r", value[:120])
            return default
    return value


class EbayAspectService:

    @staticmethod
    async def is_enabled(settings: Optional[Any] = None) -> bool:
        """Whether eBay is an ENABLED platform, meaning membership in app_settings.platforms.

        NOT the same question as "is eBay configured". platform_settings carries an eBay
        block on every database whether or not it is enabled, because
        _hydrate_platform_settings merges EBAY_DEFAULT_SETTINGS into the response
        unconditionally. Anything deciding whether to show or compute eBay work has to ask
        this, not that.

        `settings` lets a caller that has already loaded AppSettings pass it in, the same
        way active_mappings does, so the schema path does not pay for a second read.
        """
        if settings is None:
            settings = await AppSettings.first()
        return "ebay" in ((settings.platforms if settings else None) or [])

    @staticmethod
    async def active_mappings(ebay_settings: Optional[Dict[str, Any]] = None) -> set:
        """Which per-platform mappings are actually collected for eBay.

        Brand and colour drop out when their `require_*_mapping` flag is off: nothing
        collects the mapping then, so an aspect fed from that value has to be filled some
        other way. Size never drops out, the size gate having no flag.

        `ebay_settings` lets a caller that has already loaded AppSettings pass it in. The
        schema path had two independent `AppSettings.first()` calls, and against a database
        ~400ms away that duplicate was a fifth of the endpoint's latency.
        """
        if ebay_settings is None:
            settings = await AppSettings.first()
            ebay_settings = (
                (settings.platform_settings if settings else None) or {}
            ).get("ebay") or {}
        ebay = ebay_settings
        return {
            mapping
            for mapping, _fields, _tables, flag in MAPPING_SOURCES
            if flag is None or ebay.get(flag)
        }

    @staticmethod
    def resolve_mapping(row: Dict[str, Any], active: set) -> Optional[str]:
        """The mapping that answers this aspect, or None.

        Reads the aspect's own configuration, so the answer changes when the operator
        re-points it: `Color` on `brand_color` resolves to None, the same aspect on
        `standard_color` resolves to "color".
        """
        if row.get("aspect_name") in SIZE_PREFIX_ASPECTS:
            return "size" if "size" in active else None

        field = row.get("mapped_field")
        table = row.get("mapped_table")
        for mapping, fields, tables, _flag in MAPPING_SOURCES:
            if mapping not in active:
                continue
            if (field and field in fields) or (table and table in tables):
                return mapping
        return None

    @staticmethod
    async def get_categories_for_type(
        product_type: str, marketplace_id: str = DEFAULT_MARKETPLACE
    ) -> List[Dict[str, Any]]:
        """Every eBay category this Lux type maps to, its default first.

        Reads `listingoptions_types.ebay_category_id`, a JSONB array of leaf ids where
        ELEMENT 0 IS THE DEFAULT. The array rather than the shared mapping table because
        that table carries UNIQUE (primary_id, platform_id, primary_table_column) and so
        holds exactly one category per type; see add_ebay_category_ids_array.sql.

        Name and path come from pm_ebay_categories at read time, never stored here: they
        are eBay's to change on any tree reload, and a stored copy is precisely how 135 of
        the 141 original mapping rows quietly stopped resolving.

        Aliases are matched the way the submit gate matches them
        (listing_options_service.get_platform_type). Without that, a listing whose
        product_type is an alias passes the gate as "mapped" and then shows no eBay section
        at all, and the mismatch appears and disappears across a reload because
        _apply_product_type_derived canonicalises the type on save.

        Ordered explicitly: an unordered list reshuffles between requests and the dropdown
        moves under the operator.
        """
        conn = Tortoise.get_connection("default")
        rows = await conn.execute_query_dict(
            """
            SELECT cat.category_id,
                   c.name,
                   c.path,
                   cat.ord = 1 AS is_default
            FROM listingoptions_types t
            -- WITH ORDINALITY because the array's ORDER is data: the first element is the
            -- type's default, and an unnest without it loses that. Ordinality is 1-based.
            CROSS JOIN LATERAL jsonb_array_elements_text(t.ebay_category_id)
                 WITH ORDINALITY AS cat(category_id, ord)
            JOIN pm_ebay_categories c
              ON  c.category_id    = cat.category_id
             AND c.marketplace_id  = $2
            WHERE LOWER(t.type) = LOWER($1)
               OR EXISTS (
                    SELECT 1 FROM jsonb_array_elements_text(t.aliases) AS alias
                    WHERE LOWER(alias) = LOWER($1))
            ORDER BY is_default DESC, c.name
            """,
            [product_type, marketplace_id],
        )
        for row in rows:
            row["path"] = decode_json(row.get("path"), [])
        return rows

    @staticmethod
    async def resolve_listing_category(
        product_type: Optional[str], requested: Optional[str] = None
    ) -> Optional[str]:
        """The category a listing's eBay fields resolve against.

        One resolver rather than a check in each of the three callers. `category_id` reaches
        them from a query string, and an id that is not this type's would otherwise render a
        perfectly valid-looking aspect set for a category the submit path will never send --
        which the operator then fills in and autosaves.

        An unrecognised request is ignored, not honoured, and logged. Returns None when the
        type maps nowhere, which callers read as "no eBay section on this listing".
        """
        if not product_type:
            return None
        candidates = await EbayAspectService.get_categories_for_type(product_type)
        if not candidates:
            return None

        allowed = {c["category_id"] for c in candidates}
        if requested:
            if requested in allowed:
                return requested
            logger.warning(
                "Ignoring eBay category %r: not mapped to product type %r",
                requested,
                product_type,
            )
        default = next((c["category_id"] for c in candidates if c["is_default"]), None)
        return default or candidates[0]["category_id"]

    @staticmethod
    async def platform_excluded_for(
        product_type: Optional[str], brand: Optional[str] = None
    ) -> Optional[str]:
        """Why eBay is off for this listing, or None.

        A brand or type explicitly excluded from a platform means the item is **not listed
        there at all** -- the phrasing and the behaviour both come from the submit gate
        (`listing_routes.py`), which drops that platform from every later gate. eBay fields
        on such a listing would ask for values nothing will ever send.
        """
        if product_type and "ebay" in await listing_options_service.get_excluded_platforms(
            "types", "type", product_type
        ):
            return "type"
        if brand and "ebay" in await listing_options_service.get_excluded_platforms(
            "brands", "brand", brand
        ):
            return "brand"
        return None

    @staticmethod
    async def get_mapped_categories(marketplace_id: str = DEFAULT_MARKETPLACE) -> List[Dict]:
        """eBay categories that at least one Lux product type maps to.

        The configuration page is deliberately scoped to these. A category nothing maps to
        can never be reached by a listing, so configuring it would be busywork.

        The category comes from listingoptions_types.ebay_category_id, NOT from
        listingoptions_types_default_list. That table's platform_id='ebay' rows still hold
        pre-restructure PATH STRINGS that no longer resolve against the current tree;
        ebay_category_id is the numeric leaf id and is what prod actually maintains.

        The cast is ::bigint::text because the column is numeric (so a bare ::text would
        render 57988 as '57988.0' or similar) while category_id is varchar.

        A type whose id is absent from pm_ebay_categories simply does not appear here,
        which is the intended "unmapped, disabled" behaviour. get_unmapped_types lists them.
        """
        conn = Tortoise.get_connection("default")
        rows = await conn.execute_query_dict(
            """
            SELECT c.category_id,
                   c.name,
                   c.path,
                   count(DISTINCT t.id)                       AS type_count,
                   array_agg(DISTINCT t.type ORDER BY t.type) AS types,
                   count(DISTINCT a.id)                       AS aspect_count,
                   count(DISTINCT a.id) FILTER (WHERE a.is_required) AS required_count,
                   count(DISTINCT cfg.id)                     AS configured_count
            FROM listingoptions_types t
            JOIN listingoptions_types_default_list d
              ON  d.primary_id          = t.id
             AND d.platform_id          = 'ebay'
             AND d.primary_table_column = 'type'
            JOIN pm_ebay_categories c
              ON  c.category_id   = d.platform_meta ->> 'category_id'
             AND c.marketplace_id = $1
            LEFT JOIN pm_ebay_category_aspects a
              ON a.category_id = c.category_id
             AND a.marketplace_id = c.marketplace_id
            LEFT JOIN pm_ebay_aspect_settings cfg
              ON cfg.marketplace_id = a.marketplace_id
             AND cfg.aspect_name    = a.aspect_name
             AND cfg.enabled
            GROUP BY c.category_id, c.name, c.path
            ORDER BY c.name
            """,
            [marketplace_id],
        )
        for row in rows:
            row["path"] = decode_json(row.get("path"), [])
        return rows

    @staticmethod
    async def get_unmapped_types() -> List[Dict]:
        """Lux types with no usable eBay category.

        Two distinct causes, distinguished by `reason` so the page can say which:

          no_category   ebay_category_id is NULL, nobody has mapped this type yet
          unknown_id    an id is set but no such leaf exists in the loaded tree. Five
                        types are in this state today (185075, 50637, 185080), which are
                        production-tree categories the sandbox tree 134 does not carry.

        The page shows these disabled rather than hiding them, so the gap stays visible.
        """
        conn = Tortoise.get_connection("default")
        return await conn.execute_query_dict(
            """
            SELECT t.id,
                   t.type,
                   (SELECT d.platform_meta ->> 'category_id'
                    FROM listingoptions_types_default_list d
                    WHERE d.primary_id = t.id AND d.platform_id = 'ebay'
                      AND d.primary_table_column = 'type'
                    LIMIT 1) AS stored_value,
                   CASE WHEN EXISTS (
                            SELECT 1 FROM listingoptions_types_default_list d
                            WHERE d.primary_id = t.id AND d.platform_id = 'ebay'
                              AND d.primary_table_column = 'type')
                        THEN 'unknown_id' ELSE 'no_category' END AS reason
            FROM listingoptions_types t
            -- NOT EXISTS, not a LEFT JOIN with a NULL test. Under one row per type those
            -- read the same; once a type can hold several, the LEFT JOIN version means
            -- "ANY row failed to match" and lists a type that is perfectly well mapped.
            WHERE NOT EXISTS (
                SELECT 1
                FROM listingoptions_types_default_list d
                JOIN pm_ebay_categories c
                  ON  c.category_id   = d.platform_meta ->> 'category_id'
                 AND c.marketplace_id = $1
                WHERE d.primary_id = t.id AND d.platform_id = 'ebay'
                  AND d.primary_table_column = 'type')
            ORDER BY t.type
            """,
            [DEFAULT_MARKETPLACE],
        )

    @staticmethod
    async def get_category_aspects(
        category_id: str, marketplace_id: str = DEFAULT_MARKETPLACE
    ) -> Optional[Dict[str, Any]]:
        """Every aspect a category offers, merged with its configuration.

        Value lists at or below INLINE_VALUE_LIMIT come back inline. Larger ones come back
        as a values_id the caller resolves through the typeahead endpoint.

        `mode` matters as much as the values themselves: 68% of aspects are FREE_TEXT and
        many of those still ship suggestions. Those are `suggestions`, not `options`, and
        must not become a closed enum.
        """
        conn = Tortoise.get_connection("default")

        category = await conn.execute_query_dict(
            "SELECT category_id, name, path, tree_version FROM pm_ebay_categories "
            "WHERE marketplace_id = $1 AND category_id = $2",
            [marketplace_id, category_id],
        )
        if not category:
            return None

        rows = await conn.execute_query_dict(
            f"""
            SELECT a.aspect_name,
                   a.is_required,
                   a.mode,
                   a.data_type,
                   a.cardinality,
                   a.usage,
                   a.max_length,
                   a.variations,
                   a.values_id,
                   a.sort_order AS ebay_sort_order,
                   a.constraint_json,
                   v.value_count,
                   CASE WHEN v.value_count <= $3 THEN v.values_json ELSE NULL END AS values_json,
                   s.enabled,
                   s.source,
                   s.mapped_field,
                   s.mapped_table,
                   s.mapped_column,
                   s.display_name,
                   s.sellercloud_field,
                   s.ai_tagging,
                   s.ui_size,
                   s.sort_order,
                   s.min_length,
                   s.regex,
                   s.default_value,
                   -- Scoped to THIS category, unlike everything else on the settings row.
                   s.category_defaults -> $2 AS category_default,
                   ch.verbs AS pending_changes
            FROM pm_ebay_category_aspects a
            LEFT JOIN pm_ebay_aspect_values v
              ON v.values_id = a.values_id
            -- Settings join on the NAME only: they are aspect level, so one row drives
            -- every category that offers the aspect.
            LEFT JOIN pm_ebay_aspect_settings s
              ON  s.marketplace_id = a.marketplace_id
              AND s.aspect_name    = a.aspect_name
            -- Anything a reload moved under this aspect that nobody has acknowledged yet.
            LEFT JOIN LATERAL (
                SELECT array_agg(DISTINCT c.verb) AS verbs
                FROM pm_ebay_reference_load_changes c
                WHERE c.aspect_name = a.aspect_name
                  AND c.category_id = a.category_id
                  AND NOT c.acknowledged
            ) ch ON TRUE
            WHERE a.marketplace_id = $1 AND a.category_id = $2
            ORDER BY {ASPECT_ORDER_SQL}
            """,
            [marketplace_id, category_id, INLINE_VALUE_LIMIT],
        )

        active = await EbayAspectService.active_mappings()
        aspects = [
            EbayAspectService._present_aspect(
                row, EbayAspectService.resolve_mapping(row, active)
            )
            for row in rows
        ]
        category[0]["path"] = decode_json(category[0].get("path"), [])
        return {
            "category": category[0],
            "aspects": aspects,
            "required_count": sum(1 for a in aspects if a["ebay"]["is_required"]),
            "enabled_count": sum(1 for a in aspects if a["settings"]["enabled"]),
            "configured_count": sum(1 for a in aspects if a["configured"]),
        }

    @staticmethod
    def _present_aspect(
        row: Dict[str, Any], resolved_by: Optional[str] = None
    ) -> Dict[str, Any]:
        """One aspect as this category sees it: eBay's facts plus the aspect-level settings.

        The two halves are returned separately and deliberately never merged into one flat
        bag. `ebay` is what this category dictates and is read-only; `settings` is what the
        operator owns. Flattening them is how a stored copy of an eBay value survives a
        reload and masks the newer truth.
        """
        values = decode_json(row.get("values_json"), None)
        selection_only = row["mode"] == "SELECTION_ONLY"

        # A required aspect is always on, whether or not a settings row exists. Callers must
        # not read a missing row as "off".
        enabled = bool(row["is_required"] or row["enabled"])

        return {
            "aspect_name": row["aspect_name"],
            # --- eBay's facts for THIS category. Never editable. ---
            "ebay": {
                "is_required": row["is_required"],
                "mode": row["mode"],
                "data_type": row["data_type"],
                "cardinality": row["cardinality"],
                "usage": row["usage"],
                # eBay's own position. Exposed so the page can reproduce the server's sort
                # locally after a drag, instead of waiting for a save and a refetch to see
                # the new order.
                "sort_order": row["ebay_sort_order"],
                "max_length": row["max_length"],
                "variations": row["variations"],
                "value_count": row["value_count"] or 0,
                "values_id": row["values_id"],
                # Closed list vs suggestions. Only `options` may become an enum: 53% of
                # aspects are FREE_TEXT yet still ship values, and those are hints.
                "options": values if (selection_only and values) else None,
                "suggestions": values if (not selection_only and values) else None,
                "values_inlined": values is not None,
                "field_type": EbayAspectService.derive_field_type(row),
                "constraint": decode_json(row.get("constraint_json"), {}) or {},
            },
            # --- operator-owned, set once for the aspect name ---
            "settings": {
                "enabled": enabled,
                "source": row["source"] or "type_based",
                "mapped_field": row["mapped_field"],
                "mapped_table": row["mapped_table"],
                "mapped_column": row["mapped_column"],
                # The stored override, null when nobody has set one, alongside the value
                # that will actually be used. They are separate on purpose: an editor bound
                # to the effective value would hold the derived default in its input and
                # save it straight back, turning every untouched aspect into an explicit
                # override and leaving a later bulk map unable to tell the two apart.
                # The UI binds to these and shows *_effective as placeholder text.
                "display_name": row["display_name"],
                "display_name_effective": (
                    row["display_name"] or derive_display_name(row["aspect_name"])
                ),
                "sellercloud_field": row["sellercloud_field"],
                "sellercloud_field_effective": (
                    row["sellercloud_field"] or derive_sellercloud_field(row["aspect_name"])
                ),
                "ai_tagging": bool(row["ai_tagging"]),
                "ui_size": row["ui_size"],
                # Null means "not placed": the aspect keeps eBay's own order behind
                # everything that has been placed. 0 is a real position, so the two must
                # stay distinguishable.
                "sort_order": row["sort_order"],
                "min_length": row["min_length"],
                "regex": row["regex"],
                "default_value": decode_json(row.get("default_value"), None),
            },
            # --- operator-owned, but scoped to THIS category ---
            # Deliberately outside `settings`: everything in there is aspect level and
            # reaches every category offering the name. Burying a category-scoped value in
            # the same bag is how the next reader ships a bug.
            "category_default": decode_json(row.get("category_default"), None),
            # "brand" | "color" | "size" when an existing mapping already answers this
            # aspect. The source, the default and AI tagging are all moot for those: the
            # value comes from the mapping the submit gate collects.
            "resolved_by": resolved_by,
            "configured": row["source"] is not None,
            "pending_changes": row.get("pending_changes") or [],
        }

    @staticmethod
    def derive_field_type(row: Dict[str, Any]) -> str:
        """The FieldDefinition type eBay's data_type and cardinality imply.

        Derived, never stored: data_type and cardinality are eBay's to change, and a stored
        copy would outlive the change. Not operator-editable for the same reason.
        """
        if row["data_type"] == "NUMBER":
            return "number"
        if row["cardinality"] == "MULTI":
            # text_list is the only type the template supports multiselect on.
            return "text_list"
        return "text"

    @staticmethod
    async def search_aspect_values(
        values_id: str, search: str = "", limit: int = 50
    ) -> Dict[str, Any]:
        """Typeahead over one stored value list.

        Filtering happens in Postgres so a 79,116-entry list never crosses the wire.

        No ORDER BY: ebay_compact_aspects.py stores each list already sorted, so the
        unnest emits in order and LIMIT can stop as soon as it has enough matches. With
        the sort in place Postgres had to unnest and rank the whole list before applying
        LIMIT, which on the largest list meant materialising 79,116 rows and spilling to
        temp (measured 57ms, temp read=233 written=233).
        """
        conn = Tortoise.get_connection("default")
        rows = await conn.execute_query_dict(
            """
            SELECT value
            FROM pm_ebay_aspect_values v,
                 LATERAL jsonb_array_elements_text(v.values_json) AS value
            WHERE v.values_id = $1
              AND ($2 = '' OR value ILIKE '%' || $2 || '%')
            LIMIT $3
            """,
            [values_id, search, limit],
        )
        total = await conn.execute_query_dict(
            "SELECT value_count FROM pm_ebay_aspect_values WHERE values_id = $1", [values_id]
        )
        return {
            "values": [row["value"] for row in rows],
            "total": total[0]["value_count"] if total else 0,
            "truncated": len(rows) >= limit,
        }

    @staticmethod
    async def get_ai_aspects_for_category(
        product_type: Optional[str], category_id: str
    ) -> List[Dict[str, Any]]:
        """AI-tagged eBay aspects for the category a Lux product type maps to.

        The join chain is product type -> ebay_category_id -> that category's aspects, so
        the allowed values returned are the ones eBay accepts in THAT category. The same
        aspect name carries a different list elsewhere (Brand: 61 distinct lists across 62
        categories), and handing the model the wrong list produces values the category will
        reject.

        Only aspects the operator enabled AND turned AI tagging on for. Value lists above
        AI_VALUE_LIMIT come back as None: a prompt cannot carry 19,161 options, so the
        model is asked for free text instead of being given a truncated list that would
        look authoritative.
        """
        if await EbayAspectService.platform_excluded_for(product_type):
            return []
        conn = Tortoise.get_connection("default")
        rows = await conn.execute_query_dict(
            """
            SELECT a.aspect_name,
                   a.cardinality,
                   s.display_name,
                   s.mapped_field,
                   s.mapped_table,
                   CASE WHEN a.data_type = 'NUMBER' THEN 'number'
                        WHEN a.cardinality = 'MULTI' THEN 'text_list'
                        ELSE 'text' END AS field_type,
                   CASE WHEN v.value_count <= $3 THEN v.values_json ELSE NULL END AS values
            FROM pm_ebay_category_aspects a
            JOIN pm_ebay_aspect_settings s
              ON  s.marketplace_id = a.marketplace_id
             AND s.aspect_name     = a.aspect_name
            LEFT JOIN pm_ebay_aspect_values v
              ON v.values_id = a.values_id
            WHERE a.category_id   = $1
              AND a.marketplace_id = $2
              AND s.enabled
              AND s.ai_tagging
            ORDER BY a.sort_order
            """,
            # AI_VALUE_LIMIT, not the UI's. The two used to be one constant, and raising it
            # for the form would otherwise have quadrupled what every prompt carries.
            [category_id, DEFAULT_MARKETPLACE, AI_VALUE_LIMIT],
        )
        # An aspect a mapping already answers is not the model's to guess. Asking anyway
        # spends the tokens and produces a suggestion the mapping overwrites.
        active = await EbayAspectService.active_mappings()
        rows = [r for r in rows if not EbayAspectService.resolve_mapping(r, active)]

        # JSONB arrives as a string from a raw query; an undecoded list would reach the
        # prompt as a single string of characters.
        for row in rows:
            row["values"] = decode_json(row.get("values"), None)
        return rows

    @staticmethod
    async def get_form_aspects_for_category(
        product_type: Optional[str],
        category_id: str,
        mark_required: bool = True,
        ebay_settings: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Aspects the operator put on the listing form, shaped as template field dicts.

        Same join chain as the AI path, and for the same reason: the allowed values belong
        to the eBay CATEGORY the product type maps to, not to the aspect name. `Brand`
        carries 61 distinct lists across 62 categories.

        The return shape is what _convert_template_to_schema already consumes, so an eBay
        field goes through exactly the same type, validation and ui:grid handling as a
        template field. A parallel schema builder would drift from that one within a
        release.

        `mark_required` is the caller's, not eBay's. eBay's own is_required is about what
        eBay rejects a listing over, and turning that into a JSON Schema `required` entry
        blocks the submit button for EVERY platform, so it stays off until eBay is actually
        an enabled platform.

        Returns nothing at all when the product type is excluded from eBay, and drops any
        aspect an existing mapping already answers.
        """
        if await EbayAspectService.platform_excluded_for(product_type):
            return []
        conn = Tortoise.get_connection("default")
        rows = await conn.execute_query_dict(
            f"""
            SELECT a.aspect_name,
                   a.is_required,
                   a.mode,
                   a.data_type,
                   a.cardinality,
                   a.max_length,
                   -- Aliased: the operator's s.sort_order below owns the plain name, and
                   -- two columns called sort_order would have the later one silently win.
                   a.sort_order AS ebay_sort_order,
                   s.display_name,
                   s.mapped_table,
                   s.mapped_column,
                   s.ui_size,
                   s.sort_order,
                   s.min_length,
                   s.regex,
                   COALESCE(s.category_defaults -> a.category_id, s.default_value)
                       AS effective_default,
                   CASE WHEN v.value_count <= $3 THEN v.values_json ELSE NULL END AS values_json
            FROM pm_ebay_category_aspects a
            JOIN pm_ebay_aspect_settings s
              ON  s.marketplace_id = a.marketplace_id
             AND s.aspect_name     = a.aspect_name
            LEFT JOIN pm_ebay_aspect_values v
              ON v.values_id = a.values_id
            WHERE a.category_id   = $1
              AND a.marketplace_id = $2
              AND s.source = 'form'
              -- A required aspect is on whether or not the operator ticked it, matching
              -- _present_aspect. Reading a missing tick as "off" would drop exactly the
              -- fields eBay will reject the listing over.
              AND (s.enabled OR a.is_required)
            ORDER BY {ASPECT_ORDER_SQL}
            """,
            [category_id, DEFAULT_MARKETPLACE, INLINE_VALUE_LIMIT],
        )

        active = await EbayAspectService.active_mappings(ebay_settings)

        fields = []
        for index, row in enumerate(rows):
            # Collected by the mapping dialogs the submit gate raises for every platform.
            # A form field beside one is a second source that can only disagree with it.
            if EbayAspectService.resolve_mapping(row, active):
                continue
            values = decode_json(row.get("values_json"), None)
            field_type = EbayAspectService.derive_field_type(row)
            default = decode_json(row.get("effective_default"), None)
            field = {
                "name": row["aspect_name"],
                "display_name": derive_form_label(
                    row["display_name"], row["aspect_name"]
                ),
                "type": field_type,
                # Only a closed list becomes an enum. 53% of aspects are FREE_TEXT yet
                # still ship values, and eBay does not itself enforce those.
                "options": values if (row["mode"] == "SELECTION_ONLY" and values) else None,
                # The same values when eBay will NOT enforce them. Offered on the form as a
                # freeSolo list: an operator picks from eBay's own vocabulary without being
                # trapped by it, which is what FREE_TEXT means. Type in Coats, Jackets &
                # Vests is exactly this -- 7 published values, none of them mandatory.
                "suggestions": values if (row["mode"] != "SELECTION_ONLY" and values) else None,
                # Optional for a `form` aspect, and its whole purpose is to take the list
                # from a SkuBase table instead of eBay's. _load_mapped_options reads these
                # two keys and its result wins over `options` above.
                "mapped_table": row["mapped_table"],
                "mapped_column": row["mapped_column"],
                "multiselect": row["cardinality"] == "MULTI",
                # A defaulted aspect always has a value at submit, so demanding a keystroke
                # for it would disable the submit button for every platform over a field
                # that is already answered.
                "is_required": bool(mark_required and row["is_required"] and default is None),
                "ui_size": row["ui_size"],
                # Emitted as a real JSON Schema default, so the form LOADS with the value
                # rather than hinting at it.
                #
                # This is a reversal, and the cost is real: RJSF materialises a schema
                # default into formData, so the next autosave writes it to listings.data and
                # the value becomes a frozen per-listing copy that a later change to the
                # default will not reach. That is the trade accepted deliberately -- a
                # default nobody can see is a default nobody trusts, and an operator needs
                # to read the value that will be sent, not a grey hint that disappears the
                # moment they type.
                #
                # The submit path still resolves an ABSENT value the same way
                # (data -> category default -> aspect default), so listings created before
                # a default existed keep working without it being stored.
                "default": default,
                # Kept alongside it: on a field the operator clears, the placeholder still
                # says what will be sent if they leave it empty.
                "placeholder": _placeholder(default),
                "display_in_form": True,
                # Groups these under their own heading on the form instead of letting
                # them interleave with the listing's own fields.
                "section": "ebay",
                # After every template field, whose orders top out at 999. The operator's
                # placement wins when set; an unplaced aspect keeps eBay's own sequence and
                # sits behind every placed one, matching ASPECT_ORDER_SQL. The 100,000
                # offset is what puts it behind: eBay's sort_order tops out in the tens.
                "order": (
                    1000 + row["sort_order"]
                    if row["sort_order"] is not None
                    else 100000 + (row["ebay_sort_order"] or index)
                ),
            }
            # min/max/regex are string constraints. On a number field the same keys become
            # `minimum`/`maximum`, so an aspectMaxLength of 65 would silently cap the VALUE
            # at 65 rather than its length.
            if field_type != "number":
                field["min"] = row["min_length"]
                field["max"] = row["max_length"]
                field["regex"] = row["regex"]
            fields.append(field)
        return fields

    @staticmethod
    async def get_listing_defaults(
        product_type: str, category_id: str, brand: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Every aspect carrying a default for the category this product type maps to.

        What the listing's eBay Fields dialog shows. Defaults are never written into
        listings.data, so this is the only way a listing learns what it will send for an
        aspect nobody typed into. A per-listing override lives in listings.data under the
        aspect name and simply wins; the caller compares the two.

        Returns None when the type has no eBay category, which the caller reads as "no
        eBay section on this listing" rather than as an error.

        `brand` is optional and only used for the exclusion check. It is accepted here and
        not on /listings/schema because this endpoint is called once per listing, while the
        schema is cached per product type -- keying that cache on the brand as well would
        turn one round trip per type into one per (type, brand) pair.
        """
        excluded_by = await EbayAspectService.platform_excluded_for(product_type, brand)
        if excluded_by:
            return {"category": None, "aspects": [], "excluded_by": excluded_by}

        conn = Tortoise.get_connection("default")
        rows = await conn.execute_query_dict(
            """
            SELECT a.category_id,
                   c.name AS category_name,
                   a.aspect_name,
                   a.is_required,
                   a.mode,
                   a.cardinality,
                   a.data_type,
                   a.max_length,
                   a.values_id,
                   v.value_count,
                   s.source,
                   s.display_name,
                   s.mapped_field,
                   s.mapped_table,
                   CASE WHEN a.data_type = 'NUMBER' THEN 'number'
                        WHEN a.cardinality = 'MULTI' THEN 'text_list'
                        ELSE 'text' END AS field_type,
                   CASE WHEN v.value_count <= $3 THEN v.values_json ELSE NULL END AS values_json,
                   COALESCE(s.category_defaults -> a.category_id, s.default_value)
                       AS default_value
            FROM pm_ebay_categories c
            JOIN pm_ebay_category_aspects a
              ON  a.category_id    = c.category_id
             AND a.marketplace_id  = c.marketplace_id
            JOIN pm_ebay_aspect_settings s
              ON  s.marketplace_id = a.marketplace_id
             AND s.aspect_name     = a.aspect_name
            LEFT JOIN pm_ebay_aspect_values v
              ON v.values_id = a.values_id
            WHERE c.category_id    = $1
              AND c.marketplace_id = $2
              AND (s.enabled OR a.is_required)
              -- Only aspects that actually carry a default. An aspect with none has
              -- nothing for this dialog to show and is either on the form already or
              -- resolved elsewhere.
              AND COALESCE(s.category_defaults -> a.category_id, s.default_value) IS NOT NULL
            ORDER BY a.is_required DESC, a.sort_order
            """,
            [category_id, DEFAULT_MARKETPLACE, INLINE_VALUE_LIMIT],
        )

        # Dropped here rather than in SQL: whether an aspect is mapping-resolved depends on
        # its own mapped_field/mapped_table, not on its name.
        active = await EbayAspectService.active_mappings()
        rows = [r for r in rows if not EbayAspectService.resolve_mapping(r, active)]
        if not rows:
            # No aspects with defaults is not the same as no eBay category. Resolve the
            # category separately so the caller can still render the section.
            category = await conn.execute_query_dict(
                """
                SELECT c.category_id, c.name
                FROM pm_ebay_categories c
                WHERE c.category_id = $1 AND c.marketplace_id = $2
                """,
                [category_id, DEFAULT_MARKETPLACE],
            )
            if not category:
                return None
            return {"category": category[0], "aspects": []}

        aspects = []
        for row in rows:
            values = decode_json(row.get("values_json"), None)
            aspects.append(
                {
                    "aspect_name": row["aspect_name"],
                    "display_name": derive_form_label(
                        row["display_name"], row["aspect_name"]
                    ),
                    "field_type": row["field_type"],
                    "cardinality": row["cardinality"],
                    "mode": row["mode"],
                    "max_length": row["max_length"],
                    "is_required": row["is_required"],
                    "source": row["source"],
                    # Same split as everywhere else: a closed list may become a dropdown,
                    # a FREE_TEXT list is only a hint.
                    "options": values if (row["mode"] == "SELECTION_ONLY" and values) else None,
                    "suggestions": values if (row["mode"] != "SELECTION_ONLY" and values) else None,
                    "values_id": row["values_id"],
                    "value_count": row["value_count"] or 0,
                    "default_value": decode_json(row.get("default_value"), None),
                }
            )
        return {
            "category": {
                "category_id": rows[0]["category_id"],
                "name": rows[0]["category_name"],
            },
            "aspects": aspects,
        }

    @staticmethod
    async def get_aspect_impact(
        aspect_name: str, marketplace_id: str = DEFAULT_MARKETPLACE
    ) -> Dict[str, Any]:
        """Which categories one aspect-level change reaches, and where eBay overrides it.

        Settings are per aspect name, so saving `Brand` touches all 62 categories offering
        it. The operator is shown that number before saving. `overrides` lists the
        categories whose eBay constraints deviate from the majority, because those keep
        behaving differently no matter what is set here.
        """
        conn = Tortoise.get_connection("default")
        rows = await conn.execute_query_dict(
            """
            SELECT a.category_id, c.name, c.path, a.is_required, a.mode, a.cardinality,
                   a.max_length, v.value_count
            FROM pm_ebay_category_aspects a
            JOIN pm_ebay_categories c
              ON c.category_id = a.category_id AND c.marketplace_id = a.marketplace_id
            LEFT JOIN pm_ebay_aspect_values v ON v.values_id = a.values_id
            WHERE a.marketplace_id = $1 AND a.aspect_name = $2
              -- Only categories a Lux product type can actually reach.
              AND a.category_id IN (
                    SELECT d.platform_meta ->> 'category_id'
                    FROM listingoptions_types_default_list d
                    WHERE d.platform_id = 'ebay'
                      AND d.primary_table_column = 'type'
                      AND d.primary_id IS NOT NULL
                      AND d.platform_meta ->> 'category_id' IS NOT NULL)
            ORDER BY c.name
            """,
            [marketplace_id, aspect_name],
        )
        for row in rows:
            row["path"] = decode_json(row.get("path"), [])

        # Majority behaviour, so the editor can name the exceptions rather than just
        # asserting that some exist.
        def majority(field):
            counts = Counter(r[field] for r in rows)
            return counts.most_common(1)[0][0] if counts else None

        common = {f: majority(f) for f in ("is_required", "mode", "cardinality")}
        # `values` carries what eBay actually says in the deviating category, not just which
        # field deviates. The editor tabulates override against majority side by side, and
        # naming the field alone left the operator to go look the value up per category.
        deviating = [
            {
                "category_id": r["category_id"],
                "name": r["name"],
                # Names repeat across genders (`Boots` is two categories), so the table
                # needs the path to tell one override row from the other.
                "path": r["path"],
                "differs": [f for f in common if r[f] != common[f]],
                "values": {f: r[f] for f in common if r[f] != common[f]},
            }
            for r in rows
            if any(r[f] != common[f] for f in common)
        ]
        return {
            "aspect_name": aspect_name,
            "category_count": len(rows),
            "categories": rows,
            "common": common,
            "deviating": deviating,
            "distinct_value_lists": len({r["value_count"] for r in rows if r["value_count"]}),
        }

    @staticmethod
    async def save_aspect_settings(
        settings: List[Dict[str, Any]],
        marketplace_id: str = DEFAULT_MARKETPLACE,
        category_id: Optional[str] = None,
    ) -> int:
        """Upsert aspect-level settings, plus this category's default value.

        Scoped to the aspects in the payload; anything absent is left alone, so a save from
        a filtered view cannot wipe settings the operator could not see.

        Nothing eBay-derived is accepted here. A caller that sends `is_required` or
        `options` is sending something this table has no column for, and it is dropped
        rather than stored, because a stored copy would survive the next reload and mask it.

        `category_default` is the one field on the payload that is NOT aspect level. It
        writes a single key of `category_defaults`, and the merge happens in SQL rather than
        as a read-modify-write: two operators saving the same aspect from different
        categories would otherwise each write back the map they read, and the second would
        drop the first's key.
        """
        conn = Tortoise.get_connection("default")
        written = 0
        for entry in settings:
            name = entry.get("aspect_name")
            source = entry.get("source") or "type_based"
            if source not in VALID_SOURCES:
                raise ValueError(f"Invalid source '{source}' for {name}")

            # A category-scoped write with no category names no key to write. Caught here
            # rather than silently dropped, because a default that vanishes on save reads
            # as data loss.
            has_default = "category_default" in entry
            if has_default and not category_id:
                raise ValueError(f"{name}: a default value needs a category")

            mapped_field = entry.get("mapped_field")
            mapped_table = entry.get("mapped_table")
            mapped_column = entry.get("mapped_column")
            if source == "mapped_field" and not (
                mapped_field or (mapped_table and mapped_column)
            ):
                raise ValueError(f"{name}: choose a field to map to")
            if mapped_table and mapped_column:
                await EbayAspectService._assert_mapping_exists(name, mapped_table, mapped_column)

            await conn.execute_query(
                """
                INSERT INTO pm_ebay_aspect_settings
                    (marketplace_id, aspect_name, enabled, source, mapped_field,
                     mapped_table, mapped_column, display_name, sellercloud_field,
                     ai_tagging, ui_size, sort_order, min_length, regex, default_value,
                     category_defaults, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $17, $12, $13,
                        $14::jsonb,
                        CASE WHEN $15::text IS NULL OR $16::jsonb IS NULL THEN '{}'::jsonb
                             ELSE jsonb_build_object($15, $16::jsonb) END,
                        now())
                ON CONFLICT (marketplace_id, aspect_name) DO UPDATE SET
                    enabled       = EXCLUDED.enabled,
                    source        = EXCLUDED.source,
                    mapped_field  = EXCLUDED.mapped_field,
                    mapped_table  = EXCLUDED.mapped_table,
                    mapped_column = EXCLUDED.mapped_column,
                    display_name  = EXCLUDED.display_name,
                    sellercloud_field = EXCLUDED.sellercloud_field,
                    ai_tagging    = EXCLUDED.ai_tagging,
                    ui_size       = EXCLUDED.ui_size,
                    sort_order    = EXCLUDED.sort_order,
                    min_length    = EXCLUDED.min_length,
                    regex         = EXCLUDED.regex,
                    default_value = EXCLUDED.default_value,
                    -- One key, in place. EXCLUDED would carry only the key being written
                    -- and wipe every other category's default on the same aspect.
                    category_defaults = CASE
                        WHEN $15::text IS NULL
                            THEN pm_ebay_aspect_settings.category_defaults
                        WHEN $16::jsonb IS NULL
                            THEN pm_ebay_aspect_settings.category_defaults - $15
                        ELSE jsonb_set(pm_ebay_aspect_settings.category_defaults,
                                       ARRAY[$15], $16::jsonb) END,
                    updated_at    = now()
                """,
                [
                    marketplace_id,
                    name,
                    bool(entry.get("enabled", True)),
                    source,
                    mapped_field,
                    mapped_table,
                    mapped_column,
                    entry.get("display_name") or None,
                    entry.get("sellercloud_field") or None,
                    bool(entry.get("ai_tagging")),
                    entry.get("ui_size"),
                    entry.get("min_length"),
                    entry.get("regex") or None,
                    orjson.dumps(entry.get("default_value")).decode()
                    if entry.get("default_value") is not None
                    else None,
                    # NULL leaves the map untouched, so an entry that never mentions
                    # category_default cannot clear one it did not know about.
                    category_id if has_default else None,
                    orjson.dumps(entry["category_default"]).decode()
                    if has_default and entry.get("category_default") is not None
                    else None,
                    # $17, appended rather than slotted in so the sixteen existing
                    # positions keep their numbers.
                    entry.get("sort_order"),
                ],
            )
            written += 1
        return written

    @staticmethod
    async def _assert_mapping_exists(aspect_name: str, table: str, column: str) -> None:
        """Reject a mapping target that does not exist.

        Template fields carry the same mapped_table/mapped_column pointer with no such
        check: nothing validates it on save, ManageTemplates silently title-cases a broken
        pointer into a plausible label, and listing_service._load_mapped_options
        interpolates the stored column name straight into SQL. That hole has never bitten
        only because listingoptions columns are append-only in practice. Not inheriting it.
        """
        tables = await listing_options_service.get_tables()
        match = next((t for t in tables if t.get("table") == table), None)
        if not match:
            raise ValueError(f"{aspect_name}: no such list table '{table}'")
        columns = {c.get("name") for c in (match.get("column_schema") or [])}
        if column not in columns:
            raise ValueError(f"{aspect_name}: '{table}' has no column '{column}'")

    @staticmethod
    async def acknowledge_changes(
        aspect_name: str, marketplace_id: str = DEFAULT_MARKETPLACE
    ) -> int:
        """Clear the reload-change flags for one aspect once the operator has seen them."""
        conn = Tortoise.get_connection("default")
        result = await conn.execute_query(
            "UPDATE pm_ebay_reference_load_changes SET acknowledged = TRUE "
            "WHERE aspect_name = $1 AND NOT acknowledged",
            [aspect_name],
        )
        return result[0] if result else 0


ebay_aspect_service = EbayAspectService()
