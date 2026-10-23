"""GOAT Sneakers row building. One row per PARENT listing, 12 fixed columns.

The second GOAT-family platform. The flow is goat's and goat_poller runs both; only
the sheet layout is different, and this module holds that layout plus the SellerCloud
quantity export its Qty column needs. Everything shared
(the lookups, the brand gate, the gender map, the cell cleaning, the read-back) lives
in goat_service and is used from there under this platform's own id.

What the Sneakers layout drops from goat's 20 columns, and why it matters here:

    STV                          so the 1nventory sync never adds the gsync tag
    Main Color                   so there is no colour mapping to require
    Size Range, Size Unit        so a sizing scheme needs no GOAT code
    Season, Product Type, Composition, Care Instructions, Country of Manufacture

and what it changes:

    Sizes    ALWAYS filled with the listing's own sizes that have stock, comma
             separated. goat fills it only for an X scheme. Sizes are sent exactly as
             listed: no size mapping, no US conversion.
    Price    goat calls the same column Retail Price.

and adds:

    Qty      the product's total available quantity, one number: the sum over the
             sizes in Sizes. Exported from SellerCloud when the batch is created
             (load_quantities), not stored on the listing. A size with none is left
             out of Sizes, and a product with none at all fails its row.

Columns A..D are four of the five goat reads back (Denied, SKU (GOAT), Style Code,
Custom SKU). The read-back finds them by header name and reads a missing STV as
unticked, which is what lets it and the 1nventory sync run unchanged.
"""

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Awaitable, Callable, Final, Iterable, Mapping

from exceptions.goat_exceptions import GoatBuildError
from services import goat_service
from services.goat_service import GoatLookups, SizingScheme, clean_cell
from services.sellercloud_service import sellercloud_service

PLATFORM_ID: Final = "goat_sneakers"
LABEL: Final = "GOAT Sneakers"

# Column order IS the sheet. Verified against the live template tab
# ("PT Sheet Template - Goat Sneakers", gid 0) on 2026-10-14, after Qty was added back.
# STV removed 2026-10-23 at the user's request.
SHEET_HEADERS: Final[tuple[str, ...]] = (
    "Denied",
    "SKU (GOAT)",
    "Style Code",
    "Custom SKU",
    "Image",
    "Name",
    "Brand",
    "Colorway (Brand Color)",
    "Gender",
    "Price",
    "Sizes",
    "Qty",
)

# The SellerCloud custom export column read for Qty: the sellable quantity, which
# SellerCloud's General (Excel) export calls Qty. Verified on prod 2026-10-14 to equal
# AggregateQty on every row sampled. NOT "AvailableQty": that name exports blank, which
# is what SellerCloud does with any column it does not know.
QTY_COLUMN: Final = "InventoryAvailableQty"

# Which local listing field feeds each mapped column by default. Re-pointable from the
# template editor (GOAT_SNEAKERS_FIELDS in ManageTemplates.jsx), exactly as goat's are.
DEFAULT_SOURCES: Final[Mapping[str, str]] = MappingProxyType(
    {
        "style_code": "manufacturer_sku",
        "name": "title",
        "brand": "brand_name",
        "colorway": "brand_color",
        "price": "list_price",
    }
)

_NO_SCHEME: Final = SizingScheme(goat_code=None, region_code=None)


@dataclass(frozen=True, slots=True)
class GoatSneakersRow:
    """One sheet row. Field order matches SHEET_HEADERS exactly."""

    denied: bool = False
    goat_sku: str = ""
    style_code: str = ""
    custom_sku: str = ""
    image: str = ""
    name: str = ""
    brand: str = ""
    colorway: str = ""
    gender: str = ""
    price: str = ""
    sizes: str = ""
    qty: str = ""

    def as_cells(self) -> list[Any]:
        return goat_service.row_cells(self)


def _numeric_first(size: str) -> tuple[int, float]:
    try:
        return (0, float(size))
    except ValueError:
        return (1, 0.0)


def listing_sizes(form_data: Mapping[str, Any], lookups: GoatLookups) -> list[str]:
    """This listing's distinct sizes, as listed, in a sensible order.

    The scheme's display order when the listing has a scheme we know. Without one the
    scheme is not a gate here (nothing is mapped through it), so the sizes fall back to
    numeric order: child_size_overrides iterates by child SKU, which would put 10 and
    10.5 ahead of 7.5.
    """
    scheme = lookups.scheme_by_name.get(clean_cell(form_data.get("SIZING_SCHEME")))
    if scheme and scheme.sizes:
        return goat_service.ordered_sizes(form_data, scheme)
    return sorted(goat_service.ordered_sizes(form_data, _NO_SCHEME), key=_numeric_first)


def listing_child_skus(form_data: Mapping[str, Any]) -> list[str]:
    """The child SKUs behind this listing's sizes: child_size_overrides' keys with a size."""
    overrides = form_data.get("child_size_overrides") or {}
    if not isinstance(overrides, dict):
        return []
    return [str(sku) for sku, size in overrides.items() if clean_cell(size)]


def parse_qty(raw: Any) -> int | None:
    """An exported quantity cell, or None when it is blank or not a number.

    Negative (oversold) counts as 0: there is nothing to offer GOAT.
    """
    text = clean_cell(raw)
    if not text:
        return None
    try:
        return max(0, int(float(text)))
    except ValueError:
        return None


def size_quantities(
    form_data: Mapping[str, Any], sizes: Iterable[str], lookups: GoatLookups
) -> list[int]:
    """Available quantity per size, in `sizes` order. Children sharing a size add up.

    A child with no exported quantity is an error, never a 0: SellerCloud leaves an
    unknown SKU out of the export, and a 0 there would tell GOAT the size is sold out.
    """
    overrides = form_data.get("child_size_overrides") or {}
    by_size: dict[str, int] = {}
    missing: list[str] = []
    for sku in listing_child_skus(form_data):
        qty = lookups.quantities.get(sku)
        if qty is None:
            missing.append(sku)
            continue
        size = clean_cell(overrides[sku])
        by_size[size] = by_size.get(size, 0) + qty
    if missing:
        more = f" and {len(missing) - 1} more" if len(missing) > 1 else ""
        raise GoatBuildError(f"{lookups.label}: no SellerCloud quantity for {missing[0]}{more}")
    return [by_size.get(size, 0) for size in sizes]


async def load_quantities(
    form_datas: Iterable[Mapping[str, Any]],
    *,
    heartbeat: Callable[[], Awaitable[None]] | None = None,
) -> tuple[dict[str, int], list[str]]:
    """Available quantity per child SKU across a batch, from one SellerCloud export.

    Returns (quantities, export job ids). A SKU missing from the result fails only its
    own row, in size_quantities. Raises when the export as a whole cannot be trusted
    (it failed, timed out, or came back with the column blank on every row), so the
    caller puts the batch back rather than failing rows that were never tried.
    """
    skus = sorted({sku for form_data in form_datas for sku in listing_child_skus(form_data)})
    if not skus:
        return {}, []
    rows, job_ids = await sellercloud_service.export_custom_columns(
        skus, [QTY_COLUMN], heartbeat=heartbeat
    )
    quantities = {}
    for sku, row in rows.items():
        qty = parse_qty(row.get(QTY_COLUMN))
        if qty is not None:
            quantities[sku] = qty
    if rows and not quantities:
        raise RuntimeError(
            f"SellerCloud export {', '.join(job_ids)} has no {QTY_COLUMN} on any row"
        )
    return quantities, job_ids


def build_row(
    product_id: str,
    form_data: Mapping[str, Any],
    lookups: GoatLookups,
) -> GoatSneakersRow:
    """One sheet row. No I/O - every lookup was resolved by goat_service.load_lookups.

    Raises GoatBuildError naming the offending value, as goat_service.build_row does.
    The quantities must already be in lookups (load_quantities).
    """
    brand = goat_service.resolve_brand(form_data, lookups)
    taxonomy = goat_service.resolve_taxonomy(form_data, lookups)

    sizes = listing_sizes(form_data, lookups)
    if not sizes:
        # The one column this layout exists for. A blank one is not a usable row.
        raise GoatBuildError(f"{lookups.label}: no sizes on the listing")
    # Only what can be sold: a size with no stock is left out of Sizes, and Qty is the
    # total of the sizes that are left.
    in_stock = [
        (size, qty)
        for size, qty in zip(sizes, size_quantities(form_data, sizes, lookups))
        if qty > 0
    ]
    if not in_stock:
        raise GoatBuildError(f"{lookups.label}: every size is out of stock")

    return GoatSneakersRow(
        goat_sku="",
        style_code=goat_service.field_value("style_code", form_data, lookups),
        custom_sku=clean_cell(product_id),
        image=goat_service.image_url(product_id),
        name=goat_service.field_value("name", form_data, lookups),
        brand=brand,
        colorway=goat_service.field_value("colorway", form_data, lookups),
        gender=goat_service.goat_gender(taxonomy),
        price=goat_service.field_value("price", form_data, lookups),
        sizes=", ".join(size for size, _ in in_stock),
        qty=str(sum(qty for _, qty in in_stock)),
    )
