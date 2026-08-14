"""Merchandise value of a set of products, read live from SellerCloud.

A product is worth the sum, over its ACTIVE variants, of physical quantity on hand times
the price the website sells it at:

    value(parent) = sum over active children of (AggregatePhysicalQty x SitePrice)

Two field choices worth stating, both verified against production on 2026-08-14:

  * AggregatePhysicalQty, not AggregateQty. AggregateQty read 0 on rows where
    AggregatePhysicalQty was 2, so it is not the on-hand number.
  * SitePrice, not ListPrice. SitePrice is what the storefront charges - oneinventory
    _service prices Shopify variants off it - and submit rewrites ListPrice, so ListPrice
    can differ from what was there when the batch was made. SitePrice and WebsitePrice
    were identical on every production row sampled.

"Active" means child_products.is_active in the products DB, matching the default of
product_resolver.child_skus_for. The SellerCloud grid is deliberately asked for every SKU
regardless of its own ActiveStatus, so a disagreement between the two registries shows up
as a real number rather than as a silently dropped row.
"""

import logging
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Iterable, List, Tuple

from tortoise import connections

from services.sellercloud_internal_service import sellercloud_internal_service

logger = logging.getLogger(__name__)

_CENTS = Decimal("0.01")


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal(0)
    try:
        return Decimal(str(value))
    except Exception:  # noqa: BLE001
        return Decimal(0)


async def _active_children_by_parent(parent_skus: List[str]) -> Dict[str, List[str]]:
    """Active child SKUs per parent, in one query.

    product_resolver.child_skus_for is the single-parent form of this. Calling it in a
    loop is N round trips for what one ANY() scan of idx_child_products_parent answers.
    """
    out: Dict[str, List[str]] = {p: [] for p in parent_skus}
    if not parent_skus:
        return out

    conn = connections.get("product_db")
    rows = await conn.execute_query_dict(
        "SELECT sku, parent_sku FROM child_products "
        "WHERE parent_sku = ANY($1::text[]) AND is_active",
        [parent_skus],
    )
    for row in rows:
        out.setdefault(row["parent_sku"], []).append(row["sku"])
    return out


def aggregate_values(
    children_by_parent: Dict[str, List[str]],
    grid_rows: Dict[str, Dict[str, Any]],
) -> Tuple[Decimal, Dict[str, Dict[str, Any]]]:
    """(total_value, per-product breakdown) from already-fetched inputs. Pure, no I/O.

    Split out from compute_product_values so backfill_batch_values.py, which talks to the
    databases over raw asyncpg rather than Tortoise, arrives at exactly the same numbers as
    the live batch-creation path instead of reimplementing the arithmetic.

    The breakdown is what gets stored in batches.product_values:

        {"<parent_sku>": {"value": 680.0, "qty": 2, "children": 4, "priced": 4}}

    `children` is how many active variants we asked SellerCloud about and `priced` how many
    came back with a non-zero SitePrice, so a zero value stays diagnosable after the fact:
    children == 0 means the parent has no active variants registered, children > priced
    means SellerCloud had no price rather than no stock.
    """
    # SellerCloud has returned SKUs with the exact casing we sent on every row sampled, but
    # get_children_pricing already folds case for the same lookup and a case mismatch here
    # would silently value a product at 0. Cheap enough to be certain.
    by_sku = {sku.lower(): row for sku, row in grid_rows.items()}

    breakdown: Dict[str, Dict[str, Any]] = {}
    total = Decimal(0)

    for parent in sorted(children_by_parent):
        children = children_by_parent[parent]
        value = Decimal(0)
        qty = 0
        priced = 0

        for child in children:
            row = by_sku.get(child.lower())
            if not row:
                continue
            child_qty = _to_int(row.get("AggregatePhysicalQty"))
            price = _to_decimal(row.get("SitePrice"))
            if price > 0:
                priced += 1
            qty += child_qty
            value += price * child_qty

        value = value.quantize(_CENTS, rounding=ROUND_HALF_UP)
        breakdown[parent] = {
            "value": float(value),
            "qty": qty,
            "children": len(children),
            "priced": priced,
        }
        total += value

    return total.quantize(_CENTS, rounding=ROUND_HALF_UP), breakdown


async def compute_product_values(
    parent_skus: Iterable[str],
) -> Tuple[Decimal, Dict[str, Dict[str, Any]]]:
    """(total_value, per-product breakdown) for a set of parent SKUs.

    Raises whatever the SellerCloud call raises. Callers that must not fail on a SellerCloud
    outage are responsible for catching it.
    """
    parents = sorted({p for p in parent_skus if p})
    if not parents:
        return Decimal(0), {}

    children_by_parent = await _active_children_by_parent(parents)
    all_children = [sku for skus in children_by_parent.values() for sku in skus]

    grid_rows = await sellercloud_internal_service.get_catalog_grid_rows(all_children)

    unregistered = [p for p in parents if not children_by_parent.get(p)]
    if unregistered:
        logger.warning(
            "Valuing %d products: %d have no active children in the products DB and are "
            "valued at 0 (%s%s)",
            len(parents),
            len(unregistered),
            ", ".join(unregistered[:5]),
            f" +{len(unregistered) - 5} more" if len(unregistered) > 5 else "",
        )

    return aggregate_values(children_by_parent, grid_rows)
