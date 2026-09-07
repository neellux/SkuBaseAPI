"""GOAT row building. One row per PARENT listing, 18 fixed columns.

Unlike SPO, which fans out a row per child SKU, GOAT is a parent-level export:
`Custom SKU` is the parent and the `Sizes` column is not sent at all. That is why
there is no size-mapping gate here.

The module is split so the expensive half is testable and the cheap half is
batched:

    load_lookups()  ALL of the database I/O, ONCE per batch
    build_row()     a plain def with no I/O at all

That split is not stylistic. Copying spo_service's per-submission structure costs
seven sequential awaits per row - including AppSettings.first() twice, once for
field_templates and once for platform_settings - which against a remote database
measured at ~0.5s per round trip (spo_poller.py:391-395) is 700 round trips and
roughly six minutes for a 100-row batch, inside a poller that runs one cycle at a
time. Batched it is five queries and a couple of seconds.

Taking `product_id: str` rather than a Listing model is also deliberate: it means
the unit tests need no Tortoise initialisation.
"""

import logging
import re
from dataclasses import dataclass, fields as dataclass_fields
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Final, Mapping, Sequence
from urllib.parse import quote

from tortoise import connections

from config import config
from exceptions.goat_exceptions import GoatBuildError
from services.template_render import build_field_value

logger = logging.getLogger(__name__)

PLATFORM_ID: Final = "goat"

GCS_BUCKET: Final = config.get("gcs_bucket_products", "lux_products")
GCS_BASE_URL: Final = f"https://storage.googleapis.com/{GCS_BUCKET}"

# `1_1500.jpg`, NOT the `1_fullsize.jpg` that spo_service uses. This matches the
# sample sheets GOAT supplied and the variant gallery_image_sync_poller and
# image_service treat as canonical for outbound links.
IMAGE_VARIANT: Final = "1_1500.jpg"

# Column order IS the sheet. Verified byte-for-byte against the live template
# tab on 2026-09-04. STV and Denied were prepended by the GOAT side; both are
# real checkboxes (dataValidation condition BOOLEAN), so they must be written as
# JSON booleans, never as the strings "true"/"false" - see GoatRow.as_cells.
#
# We write both as False. They are GOAT's to tick:
#   Denied ticked -> that listing was rejected, it never gets a GOAT SKU
#   STV ticked    -> add the `gsync` tag when the row syncs to 1nventory
SHEET_HEADERS: Final[tuple[str, ...]] = (
    "STV",
    "Denied",
    "SKU (GOAT)",
    "Style Code",
    "Custom SKU",
    "Image",
    "Name",
    "Brand",
    "Season",
    "Product Type",
    "Main Color",
    "Colorway (Brand Color)",
    "Gender",
    "Composition",
    "Care Instructions",
    "Country of Manufacture",
    "Retail Price",
    "Size Range",
    "Size Unit",
    "Sizes",
)

# The two columns the read-back needs, by NAME. Resolved against the sheet's own
# header row rather than by offset, so an inserted column cannot silently shift
# them and attribute a SKU to the wrong product.
COL_STV: Final = "STV"
COL_DENIED: Final = "Denied"
COL_GOAT_SKU: Final = "SKU (GOAT)"
COL_CUSTOM_SKU: Final = "Custom SKU"
COL_STYLE_CODE: Final = "Style Code"

# The five columns the read-back needs. Contiguous in the sheet (A..E), so one
# range per tab covers them.
READBACK_COLUMNS: Final = (COL_STV, COL_DENIED, COL_GOAT_SKU, COL_STYLE_CODE, COL_CUSTOM_SKU)

# The Master Sheet dashboard.
MASTER_HEADERS: Final[tuple[str, ...]] = (
    "Sheet Name",
    "Status",
    "Listings Submitted",
    "Listings Approved",
    "Listings Denied",
    "Listings Pending",
)
STATUS_DONE: Final = "Done"
STATUS_PENDING: Final = "Pending"

OUTCOME_APPROVED: Final = "approved"
OUTCOME_DENIED: Final = "denied"
OUTCOME_PENDING: Final = "pending"

# GOAT's own size-chart codes. A-F are ordinary charts; the Size Unit for those
# comes from the scheme's region_code as normal.
#
# X is different and is the reason this is an enum rather than free text: an X
# scheme means "no GOAT chart applies", so the Size Unit is ALSO X and the Sizes
# column - blank for every other code - carries the product's actual sizes as a
# comma-separated list. GOAT reads the raw sizes instead of mapping them.
GOAT_CODES: Final[tuple[str, ...]] = ("A", "B", "C", "D", "F", "X")
GOAT_CODE_NO_CHART: Final = "X"


class GoatStage(StrEnum):
    """platform_status values. Free text in the column; an enum here so the
    poller and any label map agree on the spelling.

    Lives in the service, not the poller: a route that needs a label must not
    have to import the poller and its singleton.
    """

    SHEET_WRITING = "sheet_writing"
    AWAITING_SKU = "awaiting_sku"
    AWAITING_1NVENTORY = "awaiting_1nventory"
    LISTED = "listed"
    # GOAT ticked Denied. The submission is finished - GOAT gave an answer - so it
    # is terminal and never reaches stage 3, where it would have no SKU to write.
    DENIED = "denied"


# listingoptions_types_parents.gender holds exactly these six values across 85
# parent types. GOAT takes three. The identity entries are kept on purpose: a
# table listing every live value documents the vocabulary, where a
# transforms-only dict would leave a reader unsure whether "Mens" is passthrough
# or unhandled.
GOAT_GENDER: Final[Mapping[str, str]] = MappingProxyType(
    {
        "Mens": "Mens",
        "Womens": "Womens",
        "Unisex": "Unisex",
        "Boys": "Unisex",
        "Girls": "Unisex",
        "Does Not Apply": "Unisex",
    }
)
GENDER_FALLBACK: Final = "Unisex"

# Which local listing field feeds each mapped column by default. An operator can
# override any of these from the template editor by mapping a field to the goat
# platform with the column key as its field_id; unmapped columns keep these.
DEFAULT_SOURCES: Final[Mapping[str, str]] = MappingProxyType(
    {
        "style_code": "manufacturer_sku",
        "name": "title",
        "brand": "brand_name",
        "main_color": "standard_color",
        "colorway": "brand_color",
        "composition": "material",
        "country_of_manufacture": "country_of_origin",
        "retail_price": "list_price",
    }
)

# Zero-width and non-breaking characters arrive from SellerCloud's COLOR column
# and flow into brand_color, which GOAT column 10 sends raw. spo_service hit this
# with two Asics colourways; stripping here keeps them out of the sheet.
_INVISIBLE = re.compile(r"[​‌‍⁠﻿\xa0]")

# GOAT SKUs observed on the 1nventory store look like `FG05S18D 99LN 0099`.
_GOAT_SKU_ALLOWED = re.compile(r"^[A-Za-z0-9 ._/-]+$")
MAX_GOAT_SKU_LENGTH: Final = 64


def clean_cell(value: Any) -> str:
    """Every cell is a string by the time it reaches Sheets.

    valueInputOption=RAW ships whatever JSON type it is given, so a float
    list_price would land as `1299.0` on one row and `1299` on another. Format
    once, here, on purpose.
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    text = _INVISIBLE.sub("", str(value))
    return text.strip()


@dataclass(frozen=True, slots=True)
class GoatRow:
    """One sheet row. Field order matches SHEET_HEADERS exactly."""

    # Written as real booleans. Field order matches SHEET_HEADERS exactly.
    stv: bool = False
    denied: bool = False
    goat_sku: str = ""
    style_code: str = ""
    custom_sku: str = ""
    image: str = ""
    name: str = ""
    brand: str = ""
    season: str = ""
    product_type: str = ""
    main_color: str = ""
    colorway: str = ""
    gender: str = ""
    composition: str = ""
    care_instructions: str = ""
    country_of_manufacture: str = ""
    retail_price: str = ""
    size_range: str = ""
    size_unit: str = ""
    sizes: str = ""

    def as_cells(self) -> list[Any]:
        """Cells for one sheet row.

        The two checkbox columns bypass clean_cell deliberately: it stringifies
        bools, and writing "false" into a cell whose data validation is BOOLEAN
        breaks the checkbox. Under valueInputOption=RAW a JSON false is stored as
        a real boolean, which is what the checkbox reads.
        """
        return [
            bool(getattr(self, f.name))
            if f.name in BOOLEAN_COLUMNS
            else clean_cell(getattr(self, f.name))
            for f in dataclass_fields(self)
        ]


# GoatRow fields that are checkbox columns rather than text.
BOOLEAN_COLUMNS: Final[frozenset[str]] = frozenset({"stv", "denied"})


@dataclass(frozen=True, slots=True)
class TypeTaxonomy:
    sizing_type: str | None
    gender: str | None


@dataclass(frozen=True, slots=True)
class SizingScheme:
    goat_code: str | None
    region_code: str | None
    # Every size on the scheme, in the scheme's own display order. Only read for
    # an X scheme, but fetched for all of them because it costs nothing: it comes
    # out of the same grouped query as the two codes.
    sizes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class GoatLookups:
    """Everything build_row needs from the database, resolved once per batch."""

    field_templates: Mapping[str, Any]
    field_defs_by_name: Mapping[str, Any]
    column_sources: Mapping[str, str]
    require_brand_mapping: bool
    require_color_mapping: bool
    brand_known: frozenset[str]
    brand_by_name: Mapping[str, str]
    color_by_name: Mapping[str, str]
    taxonomy_by_type: Mapping[str, TypeTaxonomy]
    scheme_by_name: Mapping[str, SizingScheme]


def column_sources(field_definitions: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    """DEFAULT_SOURCES, overridden by any goat entry in field_definitions.

    A field def maps a local field to a platform: {"name": "style_name",
    "platforms": [{"platform_id": "goat", "field_id": "name"}]} routes
    `style_name` into the sheet's Name column.
    """
    sources = dict(DEFAULT_SOURCES)
    for field_def in field_definitions or ():
        local_name = field_def.get("name")
        if not local_name:
            continue
        for platform in field_def.get("platforms") or ():
            if platform.get("platform_id") == PLATFORM_ID and platform.get("field_id"):
                sources[platform["field_id"]] = local_name
    return sources


async def load_lookups(
    form_datas: Sequence[Mapping[str, Any]],
    field_definitions: Sequence[Mapping[str, Any]],
    platform_settings: Mapping[str, Any],
    field_templates: Mapping[str, Any],
) -> GoatLookups:
    """Every database read the batch needs, in four queries.

    Brand and colour are looked up with a LATERAL so that "no such brand" stays
    distinguishable from "brand exists but has no GOAT mapping" - the gate
    message needs to say which, and a plain join collapses them.
    """
    conn = connections.get("default")
    sources = column_sources(field_definitions)

    def distinct(local_name: str) -> list[str]:
        seen = {
            clean_cell(fd.get(local_name)).lower()
            for fd in form_datas
            if clean_cell(fd.get(local_name))
        }
        return sorted(seen)

    brands = distinct(sources.get("brand", "brand_name"))
    colors = distinct(sources.get("main_color", "standard_color"))
    types = distinct("product_type")
    schemes = sorted(
        {clean_cell(fd.get("SIZING_SCHEME")) for fd in form_datas if clean_cell(fd.get("SIZING_SCHEME"))}
    )

    brand_known: set[str] = set()
    brand_by_name: dict[str, str] = {}
    if brands:
        rows = await conn.execute_query_dict(
            """
            SELECT q.needle, b.id IS NOT NULL AS known, bdl.platform_value
              FROM unnest($1::text[]) AS q(needle)
              LEFT JOIN LATERAL (
                  SELECT b.id FROM listingoptions_brands b
                   WHERE LOWER(b.brand) = q.needle
                      OR EXISTS (SELECT 1 FROM jsonb_array_elements_text(b.aliases) a
                                  WHERE LOWER(a) = q.needle)
                   LIMIT 1
              ) b ON TRUE
              LEFT JOIN listingoptions_brands_default_list bdl
                     ON bdl.primary_id = b.id AND bdl.platform_id = $2
            """,
            [brands, PLATFORM_ID],
        )
        for row in rows:
            if row["known"]:
                brand_known.add(row["needle"])
            if row["platform_value"]:
                brand_by_name[row["needle"]] = row["platform_value"]

    color_by_name: dict[str, str] = {}
    if colors:
        rows = await conn.execute_query_dict(
            """
            SELECT q.needle, cdl.platform_value
              FROM unnest($1::text[]) AS q(needle)
              LEFT JOIN LATERAL (
                  SELECT c.id FROM listingoptions_colors c
                   WHERE LOWER(c.color) = q.needle
                      OR EXISTS (SELECT 1 FROM jsonb_array_elements_text(c.aliases) a
                                  WHERE LOWER(a) = q.needle)
                   LIMIT 1
              ) c ON TRUE
              LEFT JOIN listingoptions_colors_default_list cdl
                     ON cdl.primary_id = c.id AND cdl.platform_id = $2
            """,
            [colors, PLATFORM_ID],
        )
        color_by_name = {r["needle"]: r["platform_value"] for r in rows if r["platform_value"]}

    # sizing_types AND gender in ONE query. oneinventory_service._load_taxonomy
    # already joins these two tables; this is the same join widened to a batch.
    taxonomy_by_type: dict[str, TypeTaxonomy] = {}
    if types:
        rows = await conn.execute_query_dict(
            """
            SELECT LOWER(t.type) AS needle, t.sizing_types, p.gender
              FROM listingoptions_types t
              LEFT JOIN listingoptions_types_parents p ON p.id = t.parent_id
             WHERE LOWER(t.type) = ANY($1::text[])
            """,
            [types],
        )
        taxonomy_by_type = {
            r["needle"]: TypeTaxonomy(r["sizing_types"], r["gender"]) for r in rows
        }

    # goat_code and region_code are SCHEME level, denormalised onto every size row
    # of the scheme, so the first non-null of each is the whole answer. The size
    # list comes from the same grouped scan rather than a second query.
    scheme_by_name: dict[str, SizingScheme] = {}
    if schemes:
        rows = await conn.execute_query_dict(
            """
            SELECT sizing_scheme,
                   (array_agg(goat_code)   FILTER (WHERE goat_code   IS NOT NULL))[1] AS goat_code,
                   (array_agg(region_code) FILTER (WHERE region_code IS NOT NULL))[1] AS region_code,
                   array_agg(size ORDER BY "order") AS sizes
              FROM listingoptions_sizing_schemes
             WHERE sizing_scheme = ANY($1::text[])
             GROUP BY sizing_scheme
            """,
            [schemes],
        )
        scheme_by_name = {
            r["sizing_scheme"]: SizingScheme(
                r["goat_code"], r["region_code"],
                tuple(s for s in (r["sizes"] or []) if s),
            )
            for r in rows
        }

    return GoatLookups(
        field_templates=field_templates or {},
        field_defs_by_name={
            fd.get("name"): fd for fd in (field_definitions or ()) if fd.get("name")
        },
        column_sources=sources,
        require_brand_mapping=bool(platform_settings.get("require_brand_mapping")),
        require_color_mapping=bool(platform_settings.get("require_color_mapping")),
        brand_known=frozenset(brand_known),
        brand_by_name=brand_by_name,
        color_by_name=color_by_name,
        taxonomy_by_type=taxonomy_by_type,
        scheme_by_name=scheme_by_name,
    )


def build_row(
    product_id: str,
    form_data: Mapping[str, Any],
    lookups: GoatLookups,
) -> GoatRow:
    """One sheet row. No I/O - every lookup was resolved by load_lookups.

    Raises GoatBuildError naming the offending value, because that string becomes
    error_display and "Failed to build the row" is not actionable.
    """

    def value(column: str) -> str:
        local_name = lookups.column_sources.get(column)
        if not local_name:
            return ""
        return clean_cell(
            build_field_value(
                lookups.field_templates,
                PLATFORM_ID,
                local_name,
                lookups.field_defs_by_name.get(local_name),
                form_data,
            )
        )

    brand_raw = value("brand")
    brand = brand_raw
    if brand_raw:
        mapped = lookups.brand_by_name.get(brand_raw.lower())
        if mapped:
            brand = mapped
        elif lookups.require_brand_mapping:
            known = brand_raw.lower() in lookups.brand_known
            raise GoatBuildError(
                f"GOAT: no brand mapping for {brand_raw!r}"
                + ("" if known else " (brand not in listing options)")
            )

    color_raw = value("main_color")
    main_color = color_raw
    if color_raw:
        mapped = lookups.color_by_name.get(color_raw.lower())
        if mapped:
            main_color = mapped
        elif lookups.require_color_mapping:
            raise GoatBuildError(f"GOAT: no colour mapping for {color_raw!r}")

    product_type_raw = clean_cell(form_data.get("product_type"))
    taxonomy = lookups.taxonomy_by_type.get(product_type_raw.lower())
    if product_type_raw and taxonomy is None:
        raise GoatBuildError(f"GOAT: product type {product_type_raw!r} is not in listing options")

    scheme_raw = clean_cell(form_data.get("SIZING_SCHEME"))
    scheme = lookups.scheme_by_name.get(scheme_raw)
    if scheme_raw and scheme is None:
        raise GoatBuildError(f"GOAT: sizing scheme {scheme_raw!r} has no record")
    if not scheme_raw or not (scheme and clean_cell(scheme.goat_code)):
        # Size Range is a required GOAT column and a blank one is not a usable
        # row. Fail with the scheme named so the gap is obvious.
        raise GoatBuildError(
            f"GOAT: no GOAT code on sizing scheme {scheme_raw or '(none set)'!r}"
        )

    # An X scheme has no GOAT chart: Size Unit becomes X too, and the Sizes
    # column - blank for every other code - carries this product's own sizes,
    # ordered by the scheme rather than by however child_size_overrides happens
    # to iterate.
    goat_code = clean_cell(scheme.goat_code)
    size_unit = clean_cell(scheme.region_code)
    sizes_cell = ""
    if goat_code.upper() == GOAT_CODE_NO_CHART:
        size_unit = GOAT_CODE_NO_CHART
        sizes_cell = ", ".join(_ordered_sizes(form_data, scheme))

    gender_raw = clean_cell(taxonomy.gender if taxonomy else "")
    return GoatRow(
        goat_sku="",
        style_code=value("style_code"),
        custom_sku=clean_cell(product_id),
        image=f"{GCS_BASE_URL}/{quote(clean_cell(product_id))}/{IMAGE_VARIANT}",
        name=value("name"),
        brand=brand,
        season="",
        product_type=clean_cell(taxonomy.sizing_type if taxonomy else ""),
        main_color=main_color,
        colorway=value("colorway"),
        gender=GOAT_GENDER.get(gender_raw, GENDER_FALLBACK),
        composition=value("composition"),
        care_instructions="",
        country_of_manufacture=value("country_of_manufacture"),
        retail_price=value("retail_price"),
        size_range=goat_code,
        size_unit=size_unit,
        sizes=sizes_cell,
    )


def _ordered_sizes(form_data: Mapping[str, Any], scheme: SizingScheme) -> list[str]:
    """This listing's distinct child sizes, in the scheme's display order.

    child_size_overrides is {child_sku: size}, so several children share a size
    and dict order follows SKU rather than anything meaningful. Sizes the scheme
    does not know about are kept, appended in first-seen order, rather than
    dropped: a size GOAT is being offered must appear in the cell even if the
    scheme is out of date.
    """
    overrides = form_data.get("child_size_overrides") or {}
    if not isinstance(overrides, dict):
        return []
    seen: list[str] = []
    for value in overrides.values():
        size = clean_cell(value)
        if size and size not in seen:
            seen.append(size)
    rank = {size: i for i, size in enumerate(scheme.sizes)}
    return sorted(seen, key=lambda s: (rank.get(s, len(rank)), seen.index(s)))


def header_index(header: Sequence[str]) -> dict[str, int]:
    """Column name to position, from the sheet's OWN header row.

    Raises if either read-back column is missing, which is the signal that the
    template drifted or someone renamed a column.
    """
    index = {str(name).strip(): i for i, name in enumerate(header)}
    for required in (COL_GOAT_SKU, COL_CUSTOM_SKU):
        if required not in index:
            raise GoatBuildError(f"GOAT: sheet header has no {required!r} column")
    return index


def validate_goat_sku(raw: Any) -> str | None:
    """Clean an externally-supplied GOAT SKU, or None if it is not usable.

    This is untrusted input: it comes from a sheet the GOAT team can edit, and it
    lands in append-only jsonb and in a live Shopify metafield. The allowlist
    also keeps `=`, `+` and `@` out, which pre-empts formula injection when the
    value is later exported to CSV.
    """
    value = clean_cell(raw)
    if not value:
        return None
    if len(value) > MAX_GOAT_SKU_LENGTH:
        logger.warning("GOAT: ignoring over-long GOAT SKU (%d chars)", len(value))
        return None
    if not _GOAT_SKU_ALLOWED.match(value):
        logger.warning("GOAT: ignoring GOAT SKU with disallowed characters")
        return None
    return value


def as_bool(value: Any) -> bool:
    """A checkbox cell as a bool.

    UNFORMATTED_VALUE gives a real bool for a ticked checkbox, but a never-touched
    cell can come back absent, empty, or (if anything reads it formatted) as the
    string "TRUE". Accept all of those rather than trusting one shape.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().upper() in {"TRUE", "YES", "1"}


@dataclass(frozen=True, slots=True)
class ReadbackRow:
    """One row as GOAT left it. `outcome` is what the dashboard counts."""

    custom_sku: str
    style_code: str
    goat_sku: str | None
    denied: bool
    stv: bool

    @property
    def outcome(self) -> str:
        # Denied wins over a SKU: if GOAT ticked Denied, the row is rejected even
        # if a SKU was typed and then reconsidered.
        if self.denied:
            return OUTCOME_DENIED
        if self.goat_sku:
            return OUTCOME_APPROVED
        return OUTCOME_PENDING


def parse_readback_row(row: Sequence[Any], index: Mapping[str, int]) -> ReadbackRow | None:
    """One sheet row -> ReadbackRow, or None when there is nothing to act on.

    `index` comes from header_index(), so an inserted or reordered column cannot
    shift what is read. Rows are ragged - Sheets truncates trailing empties - so
    every access is bounds-checked.
    """

    def cell(name: str) -> Any:
        i = index.get(name)
        return row[i] if i is not None and i < len(row) else ""

    custom_sku = clean_cell(cell(COL_CUSTOM_SKU))
    if not custom_sku:
        return None
    return ReadbackRow(
        custom_sku=custom_sku,
        style_code=clean_cell(cell(COL_STYLE_CODE)),
        goat_sku=validate_goat_sku(cell(COL_GOAT_SKU)),
        denied=as_bool(cell(COL_DENIED)),
        stv=as_bool(cell(COL_STV)),
    )


@dataclass(frozen=True, slots=True)
class MasterRow:
    """One line of the Master Sheet dashboard."""

    tab_title: str
    tab_id: int
    submitted: int
    approved: int
    denied: int

    @property
    def pending(self) -> int:
        return max(0, self.submitted - self.approved - self.denied)

    @property
    def status(self) -> str:
        return STATUS_DONE if self.submitted and self.pending == 0 else STATUS_PENDING

    def as_cells(self, spreadsheet_id: str) -> list[Any]:
        """HYPERLINK needs valueInputOption=USER_ENTERED to stay a live formula.

        The batch tabs are written RAW so nothing is coerced; the dashboard is the
        one place a formula is wanted, so it uses a different option on its own
        call.
        """
        url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={self.tab_id}"
        title = self.tab_title.replace('"', '""')
        return [
            f'=HYPERLINK("{url}","{title}")',
            self.status,
            self.submitted,
            self.approved,
            self.denied,
            self.pending,
        ]


def master_grid(rows: Sequence[MasterRow], spreadsheet_id: str) -> list[list[Any]]:
    """Header plus every row, newest tab first. Written in ONE values.update."""
    ordered = sorted(rows, key=lambda r: r.tab_id, reverse=True)
    return [list(MASTER_HEADERS)] + [r.as_cells(spreadsheet_id) for r in ordered]
