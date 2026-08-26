"""Build and post eBay item specifics to SellerCloud.

Nothing here talks to eBay. SellerCloud owns the eBay channel, so the integration ends at
its catalog import:

    POST /rest/api/Catalog/Imports/EbaySpecifics

THE FILE IS LONG, NOT WIDE. Fetched from the API's own template endpoint
(GET .../EbaySpecifics/Template?fileFormat=0), which returns base64 of:

    ProductID<TAB>SpecificName<TAB>SpecificValue<TAB>SpecificType<TAB>Action\r\n

So there is one ROW per (product, aspect), not one COLUMN per aspect. That matters because
`sellercloud_ebay_import_template.xls` -- the file the per-aspect `sellercloud_field`
setting was derived from -- is wide, one column per aspect name. The two are different
import paths into the same data. `sellercloud_field` still supplies the right string; it
lands in SpecificName rather than in a header.

FileContents is base64. The swagger types it as a bare string with no `format: byte`, but
the template endpoint returns base64 for the same field, so the encoding is symmetric.

SpecificType and Action are columns of the file, not fields of the API model, and the
swagger documents no values for either. Both are left EMPTY on the first pass: Metadata
carries DeleteExistingSpecifics for replace semantics, which is the only behaviour we
actually need, and guessing an enum here writes bad data to a live catalog. There is no
SellerCloud sandbox -- both configs point at the same live account -- so these stay blank
until a real response tells us otherwise.
"""

from __future__ import annotations

import asyncio
import base64
import logging
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

from models.db_models import Listing
from services.ebay_aspect_service import ebay_aspect_service
from services.sellercloud_internal_service import sellercloud_internal_service
from services.listing_options_service import listing_options_service
from services.sellercloud_service import sellercloud_service
from tortoise import Tortoise

logger = logging.getLogger(__name__)

IMPORT_ENDPOINT = "/Catalog/Imports/EbaySpecifics"
TEMPLATE_ENDPOINT = "/Catalog/Imports/EbaySpecifics/Template"

# --- step 1: catalog info -----------------------------------------------------------
CATALOG_EXPORT_ENDPOINT = "/Catalog/Exports/Custom"
CATALOG_IMPORT_ENDPOINT = "/Catalog/Imports/Custom"
LAUNCH_ENDPOINT = "/Catalog/Actions/LaunchOnChannel"

# The eBay catalog fields this owns, in file order. ProductID is the key and is not one of
# them: it is absent from GET /Catalog/Imports/Custom/Templates/Fields for that reason, and
# the export confirms it as the first header column.
#
# Spellings verified against that same field list. eBayCategory1 carries a capital B, and
# DescriptionTemplateId a lowercase d in "Id" -- the catalog GRID spells the same field
# DescriptionTemplateID, which is not the name either the export or the import uses.
CATALOG_COLUMNS = (
    "StartPrice",
    "BuyItNowPrice",
    "DescriptionTemplateId",
    "eBaySellerProfileID_Shipping",
    "eBayCategory1",
    # The title eBay ends up showing. NOT eBayTopTitle: that is a resolved read-only value
    # the catalog grid reports, and it is absent from the importable column list. It falls
    # back through eBayTitle (empty on every product checked) to TopTitle, which is what
    # actually carries the text.
    "TopTitle",
    # Capital E AND capital B, unlike eBayCategory1 and eBaySellerProfileID_Shipping right
    # above it. The catalog grid spells the same field eBayItemCondition with a lowercase
    # e. Both spellings verified against GET /Catalog/Imports/Custom/Templates/Fields; the
    # import only accepts this one.
    "EBayItemCondition",
)
CATALOG_KEY = "ProductID"
DESCRIPTION_TEMPLATE = "Long Description"

# eBay condition 1000 = New. Read off products that are already listed and live --
# DNT-MJNS-0035/L and ALD-MHDS-0057/L both carry 1000 -- rather than guessed from eBay's
# published list. Categories like 260956 refuse the launch outright without it:
# "Category id 260956 requires condition specified. Condition is not specified".
ITEM_CONDITION = "1000"

# Columns the diff SETS rather than merely fills. Everything else keeps the never-overwrite
# rule. These three are one derived group: BuyItNowPrice is SitePrice / (1 - ebay_discount),
# StartPrice equals it, and the shipping profile is its band. Whatever else has written
# them -- the Pricehub repricer, an earlier run of this script -- does not survive, because
# a price that is not the eBay price is the wrong price on an eBay listing.
ENFORCED_COLUMNS = ("StartPrice", "BuyItNowPrice", "eBaySellerProfileID_Shipping")

# eBay seller shipping profiles, banded on price. Deliberately a function rather than an
# enum: the bands are the logic, and an enum would name the ids without saying when each
# applies.
def shipping_profile_id(price: Decimal) -> str:
    if price < 50:
        return "331669932021"
    if price <= 100:          # 50 and 100 both sit in the middle band
        return "300065418021"
    return "116834426021"


def _is_unset(column: str, value: Any) -> bool:
    """Whether SellerCloud is holding nothing for this column.

    Not one sentinel. The same probe returned '' for the description template, '0' for the
    shipping profile and '0.00' for a price, so "empty" has to be asked per column type
    rather than tested with a single falsy check.
    """
    text = ("" if value is None else str(value)).strip()
    if not text:
        return True
    if column in ("StartPrice", "BuyItNowPrice", "eBaySellerProfileID_Shipping",
                  "eBayCategory1", "EBayItemCondition"):
        try:
            return Decimal(text) == 0
        except (InvalidOperation, ValueError):
            return False
    return False

# Format enum on the request body. 0 = TAB_Delimited, 1 = CSV, 2 = Excel.
FORMAT_TAB = 0

# Export jobs are queued; 5 SKUs measured at ~51s.
POLL_INTERVAL = 5

# The template's own header and line ending, reproduced exactly.
COLUMNS = ("ProductID", "SpecificName", "SpecificValue", "SpecificType", "Action")
LINE_END = "\r\n"

# A tab or a newline inside a value would silently shift every later column on the row.
# Collapsed to spaces rather than quoted: the format has no quoting rules we can rely on.
_ILLEGAL = {"\t": " ", "\r": " ", "\n": " "}


def sanitize(value: Any) -> str:
    """One cell, safe for a tab-delimited row."""
    text = "" if value is None else str(value)
    for bad, good in _ILLEGAL.items():
        text = text.replace(bad, good)
    return text.strip()


def render_tsv(rows: List[Tuple[str, str, str, str, str]]) -> str:
    """The full file, header included."""
    lines = ["\t".join(COLUMNS)]
    lines.extend("\t".join(sanitize(cell) for cell in row) for row in rows)
    return LINE_END.join(lines) + LINE_END


class EbayService:
    PLATFORM_ID = "ebay"

    @staticmethod
    async def resolve_specifics(
        listing: Listing,
    ) -> Tuple[List[Tuple[str, str]], List[str], List[str]]:
        """Every (SpecificName, SpecificValue) this listing sends, plus what could not resolve.

        The value chain per aspect, matching what the listing form renders:

            listings.data[aspect]  ??  this category's default  ??  the aspect-wide default

        A `mapped_field` aspect is not in that chain at all: its value is whatever the
        brand/colour/size mapping already holds, which is the same value the submit gate
        made the operator supply. Reading it from listings.data instead would let the two
        disagree.
        """
        data = listing.data or {}
        product_type = data.get("product_type")
        if not product_type:
            return [], [], ["listing has no product_type"]

        category_id = await ebay_aspect_service.resolve_listing_category(
            product_type, data.get("ebay_category_id")
        )
        if not category_id:
            return [], [], [f"no eBay category for type {product_type!r}"]

        detail = await ebay_aspect_service.get_category_aspects(category_id)
        if not detail:
            return [], [], [f"eBay category {category_id} is not in the loaded tree"]

        pairs: List[Tuple[str, str]] = []
        # Aspects the size mapping answers. Held back rather than resolved here: their
        # value differs per child, and this function does not know the child.
        size_names: List[str] = []
        problems: List[str] = []

        for aspect in detail["aspects"]:
            settings = aspect["settings"]
            name = aspect["aspect_name"]
            required = aspect["ebay"]["is_required"]
            # Same rule as the form: eBay's required aspects are on whether or not the
            # operator ticked them.
            if not (settings["enabled"] or required):
                continue

            specific_name = settings["sellercloud_field_effective"] or name
            resolved_by = aspect.get("resolved_by")

            if resolved_by == "size":
                size_names.append(specific_name)
                continue

            if resolved_by:
                value = await EbayService._value_from_mapping(resolved_by, data)
            else:
                # A `mapped_field` aspect reads THE FIELD IT IS MAPPED TO, not its own
                # name. Color is mapped to brand_color and Style to style_name, and neither
                # key exists under the aspect's name -- so reading data["Color"] found
                # nothing and the aspect was silently dropped from the file. eBay then
                # refused the listing with "The item specific Color is missing".
                #
                # resolve_mapping returns None for these deliberately: brand_color needs no
                # mapping TABLE. That is not the same as needing no value, which is the
                # distinction this missed.
                source_key = name
                if settings.get("source") == "mapped_field" and settings.get("mapped_field"):
                    source_key = settings["mapped_field"]
                value = data.get(source_key)
                if value in (None, "", []):
                    value = aspect.get("category_default")
                if value in (None, "", []):
                    value = settings.get("default_value")

            if isinstance(value, list):
                # eBay MULTI aspects. SellerCloud takes one value per row, so a multi-value
                # aspect becomes several rows under the same SpecificName.
                for item in value:
                    if item not in (None, ""):
                        pairs.append((specific_name, item))
                continue

            if value in (None, "", []):
                if required:
                    problems.append(f"{name} is required by eBay and has no value")
                continue

            pairs.append((specific_name, value))

        return pairs, size_names, problems

    @staticmethod
    async def _value_from_mapping(resolved_by: str, data: Dict[str, Any]) -> Optional[str]:
        """The eBay value a brand or colour mapping already holds for this listing.

        Size is not here: it varies per child, so it is resolved in build_rows where the
        child SKU is known. Both are the same value the submit gate already made the
        operator supply, which is why they are read from the mapping rather than from
        listings.data -- the two could otherwise disagree.
        """
        if resolved_by == "brand":
            brand = data.get("brand_name")
            return await listing_options_service.get_platform_brand(brand, "ebay") if brand else None
        if resolved_by == "color":
            color = data.get("standard_color")
            return await listing_options_service.get_platform_color(color, "ebay") if color else None
        return None

    @staticmethod
    async def build_rows(
        listing: Listing,
        children: Optional[List[Dict[str, Any]]] = None,
    ) -> Tuple[List[Tuple[str, str, str, str, str]], List[str]]:
        """This listing's rows, one block per CHILD SKU.

        ProductID is the child, not the parent. Confirmed against a real SellerCloud eBay
        specifics export, where every ProductID carries the size suffix
        ("WRL-XBTM-0009/S", "PRP-XBTM-0105/34"). A parent SKU would address the matrix
        product, which for a multi-variant item is inactive and not what eBay lists.

        Children come from get_product_children, which is active-only by default -- the
        same list submit_listing_to_sellercloud writes to, so a disabled variant can never
        pick up specifics here either.

        The listing-level specifics repeat on every child. That is what the wide export
        shows too: each child row carries the full set, differing only in the per-child
        ones. Size and Size Type are exactly those, and they are resolved per child from
        the eBay size mapping.
        """
        pairs, size_names, problems = await EbayService.resolve_specifics(listing)
        parent_id = listing.product_id
        if not parent_id:
            problems.append("listing has no product_id")
            return [], problems

        data = listing.data or {}
        # `children` is passed in by callers that already hold the whole run's children --
        # one bulk read beats one SellerCloud round trip per listing. Same shape
        # get_product_children returns: [{"id", "size", ...}], active only.
        if children is None:
            try:
                children_data = await sellercloud_service.get_product_children(parent_id)
            except Exception as e:  # noqa: BLE001 - reported per listing, never fatal
                problems.append(f"could not fetch children: {type(e).__name__}: {e}")
                return [], problems
            children = children_data.get("children") or []

        overrides = data.get("child_size_overrides") or {}

        targets: List[Tuple[str, Optional[str]]] = []
        for child in children:
            child_id = child.get("id")
            if not child_id:
                continue
            # Same rule as the SellerCloud write path: a single-SKU product uses the parent
            # itself, but only when the listing actually mapped a size for it.
            if child_id == parent_id and child_id not in overrides:
                continue
            targets.append((child_id, overrides.get(child_id, child.get("size"))))

        if not targets:
            problems.append(f"no active children for {parent_id}")
            return [], problems

        # One lookup for every child size, not one per child.
        size_map: Dict[str, str] = {}
        if size_names:
            sizes = sorted({size for _cid, size in targets if size})
            scheme = data.get("SIZING_SCHEME")
            if sizes and scheme:
                size_map = await listing_options_service.get_mapped_platform_sizes(
                    scheme,
                    sizes,
                    "ebay",
                    await EbayService._sizing_type(data.get("product_type")),
                )
            elif sizes:
                problems.append("listing has no SIZING_SCHEME, sizes cannot be mapped")

        rows: List[Tuple[str, str, str, str, str]] = []
        for child_id, size in targets:
            for name, value in pairs:
                rows.append((child_id, name, value, "", ""))
            if not size_names:
                continue
            mapped = size_map.get(size) if size else None
            if not mapped:
                problems.append(f"{child_id}: size {size!r} is not mapped for eBay")
                continue
            size_type, ebay_size = EbayService.split_size_value(mapped)
            for specific_name in size_names:
                # One mapping row feeds both aspects: "Regular L" is Size Type "Regular"
                # and Size "L". Which of the two this aspect wants is decided by its name,
                # since that is the only thing distinguishing them.
                value = size_type if "type" in specific_name.lower() else ebay_size
                if value:
                    rows.append((child_id, specific_name, value, "", ""))
        # SpecificType and Action stay empty. See the module docstring.
        return rows, problems

    @staticmethod
    def split_size_value(platform_value: str) -> Tuple[str, str]:
        """"Regular L" -> ("Regular", "L").

        A Size Type containing spaces is stored with underscores ("Big_&_Tall 28"), so the
        split is on the FIRST space only and the underscores come back out afterwards.
        That encoding exists precisely so this split stays unambiguous.
        """
        head, _, tail = platform_value.partition(" ")
        return head.replace("_", " "), tail

    @staticmethod
    async def _sizing_type(product_type: Optional[str]) -> Optional[str]:
        """The type's sizing_types, which scopes the size mapping lookup.

        Required, not optional: eBay's size rows are stored scoped (sizing_type
        "Men's Tops"), and get_mapped_platform_sizes matches on `= $4 OR IS NULL`. Passing
        None there matches neither, so every size would silently come back unmapped.
        """
        if not product_type:
            return None
        conn = Tortoise.get_connection("default")
        rows = await conn.execute_query_dict(
            "SELECT sizing_types FROM listingoptions_types WHERE type = $1 LIMIT 1",
            [product_type],
        )
        return rows[0]["sizing_types"] if rows else None

    @staticmethod
    async def import_specifics(
        tsv: str,
        delete_existing: bool = False,
        format_product: Optional[int] = None,
    ) -> Dict[str, Any]:
        """POST the file to SellerCloud."""
        metadata: Dict[str, Any] = {"DeleteExistingSpecifics": bool(delete_existing)}
        if format_product is not None:
            metadata["FormatProduct"] = format_product

        payload = {
            "Metadata": metadata,
            "FileContents": base64.b64encode(tsv.encode("utf-8")).decode("ascii"),
            # LEADING DOT REQUIRED. SellerCloud rejects "txt" with "The provided file
            # extension 'txt' is not supported" and then lists ".txt" among the supported
            # ones. Matches the working payload in PhotoManagementNew.
            "FileExtension": ".txt",
            "Format": FORMAT_TAB,
        }

        response = await sellercloud_service._make_request("POST", IMPORT_ENDPOINT, data=payload)
        body: Any
        try:
            body = response.json()
        except Exception:  # noqa: BLE001 - the body is for diagnostics either way
            body = response.text

        # QueuedJobResponse: {ID, QueuedJobLink, Message}. The ID is a real SellerCloud
        # queued job, pollable through the get_job_status / is_job_complete pair that
        # already exists for exports -- so this import is visible and followable rather
        # than fire and forget.
        job_id = body.get("ID") if isinstance(body, dict) else None
        logger.info(
            "eBay specifics import returned %s, queued job %s", response.status_code, job_id
        )
        return {
            "sent": True,
            "status_code": response.status_code,
            "response": body,
            "job_id": job_id,
            "message": body.get("Message") if isinstance(body, dict) else None,
        }

    # ------------------------------------------------------------------ step 1: catalog
    @staticmethod
    async def export_catalog_fields(
        skus: List[str], poll_seconds: int = 180
    ) -> Tuple[Dict[str, Dict[str, str]], str]:
        """Current CATALOG_COLUMNS per SKU, via a custom export.

        Exported rather than read from the catalog grid because the grid, for all its 200
        columns, carries neither StartPrice nor eBaySellerProfileID_Shipping -- and
        GET /Catalog/{id}/Prices 404s on a child SKU. This is the only way to see all five.

        Knowing every current value is also what lets the import write a field back
        UNCHANGED instead of leaving its cell blank, which sidesteps having to know whether
        a blank cell in a custom import means "skip" or "clear".

        DisplayName is sent even though swagger marks only OriginalName required: without
        it the export 500s with "Display names cannot contain null entries."

        Measured at ~51s for 5 SKUs, so this is the slow step of a submit.
        """
        body = {
            "Columns": [{"OriginalName": c, "DisplayName": c} for c in CATALOG_COLUMNS],
            "ProductIds": list(skus),
            "FileFormat": FORMAT_TAB,
        }
        queued = await sellercloud_service.post(CATALOG_EXPORT_ENDPOINT, data=body)
        link = queued.get("QueuedJobLink") or ""
        if "id=" not in link:
            raise RuntimeError(f"{CATALOG_EXPORT_ENDPOINT} returned no job link: {queued}")
        job_id = link.split("id=")[1].split("&")[0]

        waited = 0
        while waited < poll_seconds:
            await asyncio.sleep(POLL_INTERVAL)
            waited += POLL_INTERVAL
            if await sellercloud_service.is_job_complete(job_id):
                break
        else:
            raise TimeoutError(f"catalog export job {job_id} did not finish in {poll_seconds}s")

        raw = await sellercloud_service.get_job_output_file(job_id)
        return EbayService.parse_catalog_export(raw.decode("utf-8", "replace")), job_id

    @staticmethod
    def parse_catalog_export(text: str) -> Dict[str, Dict[str, str]]:
        """Export rows keyed by ProductID.

        Parsed by HEADER NAME, never by position: SellerCloud inserts ProductName after the
        key without being asked, so column order is its choice and not ours.
        """
        lines = [ln for ln in text.replace("\r\n", "\n").split("\n") if ln.strip()]
        if not lines:
            return {}
        header = lines[0].split("\t")
        out: Dict[str, Dict[str, str]] = {}
        for line in lines[1:]:
            cells = line.split("\t")
            row = dict(zip(header, cells))
            key = (row.get(CATALOG_KEY) or "").strip()
            if key:
                out[key] = row
        return out

    @staticmethod
    async def desired_catalog_values(
        listing: Listing,
        child_skus: List[str],
        discount: Decimal,
        prices: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Tuple[Dict[str, Dict[str, str]], List[str]]:
        """What each child's catalog fields SHOULD read, before any diff."""
        problems: List[str] = []
        data = listing.data or {}
        product_type = data.get("product_type")

        category = await ebay_aspect_service.resolve_listing_category(
            product_type, data.get("ebay_category_id")
        )
        if not category:
            # An error, not a skip: publishing a product with no eBay category puts it live
            # under nothing, and its specifics were resolved for a category it will not use.
            return {}, [f"no eBay category for type {product_type!r}"]

        # Same reasoning as build_rows: a caller working through many listings reads the
        # grid once for every child in the run and passes the map down.
        if prices is None:
            prices = await sellercloud_internal_service.get_catalog_grid_rows(child_skus)
        wanted: Dict[str, Dict[str, str]] = {}
        for sku in child_skus:
            site_price = (prices.get(sku) or {}).get("SitePrice")
            if site_price in (None, "", 0):
                problems.append(f"{sku}: no SitePrice in SellerCloud")
                continue
            # SitePrice, not the listing's list_price. batch_value_service:12: "SitePrice is
            # what the storefront charges." Rounded to whole dollars.
            price = (Decimal(str(site_price)) / (Decimal("1") - discount)).quantize(Decimal("1"))
            wanted[sku] = {
                "StartPrice": str(price),
                "BuyItNowPrice": str(price),
                "DescriptionTemplateId": DESCRIPTION_TEMPLATE,
                "eBaySellerProfileID_Shipping": shipping_profile_id(price),
                "eBayCategory1": str(category),
                "EBayItemCondition": ITEM_CONDITION,
            }
            # The listing's own title, identical on every child: an eBay variation listing
            # shows one title, so a per-child one is a SellerCloud storage detail rather
            # than something eBay wants. Omitted entirely when the listing has none, so an
            # empty title can never be written over a populated field.
            title = (data.get("title") or "").strip()
            if title:
                wanted[sku]["TopTitle"] = title
        return wanted, problems

    @staticmethod
    def diff_catalog_rows(
        current: Dict[str, Dict[str, str]], wanted: Dict[str, Dict[str, str]]
    ) -> List[Dict[str, str]]:
        """Import rows for the children that need one, and only those.

        Per column: a value SellerCloud already holds is kept and written back unchanged; the
        desired value is used only where the field is empty. So this never overwrites a live
        price, and never blanks a cell. A child with nothing to change contributes no row.
        """
        rows: List[Dict[str, str]] = []
        for sku, target in wanted.items():
            have = current.get(sku, {})
            target = dict(target)

            # All three follow the computed eBay price. desired_catalog_values already set
            # them from SitePrice / (1 - ebay_discount); this only re-derives the band so
            # the three can never drift apart.
            computed = target.get("BuyItNowPrice")
            if computed is not None:
                try:
                    target["StartPrice"] = computed
                    target["eBaySellerProfileID_Shipping"] = shipping_profile_id(
                        Decimal(computed))
                except (InvalidOperation, ValueError):
                    pass

            row = {CATALOG_KEY: sku}
            changed = False
            for column in CATALOG_COLUMNS:
                existing = have.get(column, "")
                wanted_value = target.get(column)
                # A column this listing has no value for is carried through untouched, not
                # blanked: `wanted` omits a key rather than offering an empty one.
                fill = _is_unset(column, existing) and column in target
                # ENFORCED, not merely filled. These two are not independent facts about a
                # product, they are functions of BuyItNowPrice -- StartPrice must equal it
                # and the shipping profile must be its band. A value that contradicts that
                # is wrong no matter who wrote it, and never-overwrite would preserve the
                # contradiction forever. It preserved 1,270 of them once already.
                if (not fill and column in ENFORCED_COLUMNS and wanted_value is not None
                        and str(existing).strip() != str(wanted_value)):
                    try:
                        same = Decimal(str(existing).strip() or 0) == Decimal(wanted_value)
                    except (InvalidOperation, ValueError):
                        same = False
                    if not same:
                        fill = True
                if fill:
                    row[column] = target[column]
                    changed = True
                else:
                    row[column] = str(existing).strip()
            if changed:
                rows.append(row)
        return rows

    @staticmethod
    def render_catalog_tsv(rows: List[Dict[str, str]]) -> str:
        header = (CATALOG_KEY,) + CATALOG_COLUMNS
        lines = ["\t".join(header)]
        lines.extend("\t".join(sanitize(r.get(c, "")) for c in header) for r in rows)
        return LINE_END.join(lines) + LINE_END

    @staticmethod
    async def import_catalog_info(tsv: str) -> Dict[str, Any]:
        """POST the catalog file. Updates only; never creates.

        CompanyIdForNewProduct and UpdateFromCompanyId are omitted deliberately -- nothing is
        created, so no company applies. The flag really is DoNotUpdateProducts, not
        DoNotUpdateExistingProducts as the article's prose suggests.
        """
        payload = {
            "Metadata": {"CreateProductIfDoesntExist": False, "DoNotUpdateProducts": False},
            "FileContents": base64.b64encode(tsv.encode("utf-8")).decode("ascii"),
            "FileExtension": ".txt",
            "Format": FORMAT_TAB,
        }
        response = await sellercloud_service._make_request(
            "POST", CATALOG_IMPORT_ENDPOINT, data=payload
        )
        body = response.json() if response.content else {}
        job_id = body.get("ID") if isinstance(body, dict) else None
        logger.info("catalog info import returned %s, job %s", response.status_code, job_id)
        return {"status_code": response.status_code, "response": body, "job_id": job_id}

    # ------------------------------------------------------------------ step 3: publish
    @staticmethod
    async def publish_to_channel(child_skus: List[str], channel: str = "1") -> Dict[str, Any]:
        """Launch the children on a sales channel.

        Goes through sellercloud_internal_service, whose base URL is the delta API. HTTP 200
        is NOT success here: a permission failure answers 200 with Success=false, so the
        body is what decides. The queued job id is only available inside Notification.Message.
        """
        result = await sellercloud_internal_service.post(
            LAUNCH_ENDPOINT, data={"selectedChannel": channel, "productIds": list(child_skus)}
        )
        ok = bool(result.get("Success"))
        message = ((result.get("Notification") or {}).get("Message") or "")
        job_id = None
        if "id=" in message:
            job_id = message.split("id=")[1].split("'")[0].split("&")[0].strip()
        return {"ok": ok, "job_id": job_id, "message": message, "response": result}

    @staticmethod
    async def fetch_template(file_format: int = FORMAT_TAB) -> str:
        """SellerCloud's own template, decoded. The source of truth for COLUMNS above."""
        response = await sellercloud_service._make_request(
            "GET", TEMPLATE_ENDPOINT, params={"fileFormat": file_format}
        )
        return base64.b64decode(response.text.strip().strip('"')).decode("utf-8")


ebay_service = EbayService()
