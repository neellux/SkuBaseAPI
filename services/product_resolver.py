"""Parent-SKU resolution against the products database.

A SKU's parent is looked up, never derived. String-splitting on "/" is deliberately NOT a
resolution path: a split can invent a parent that was never registered. That is not a
hypothetical - SkuBase derived parents by chopping the last "/" segment for years, which
assumes a SKU is exactly PARENT/SIZE. ESSX SKUs are ESSX/BRAND/SEASON/STYLE/COLOR with the
size appended, so the chop ate the colour and 94% of them silently produced blank listings
pointing at a parent that does not exist.

Resolution order for any SKU:
  1. child_products.sku -> parent_sku   (parent_sku has a FK to parent_products, so a hit
                                         is guaranteed registered)
  2. the SKU is itself a parent_products.sku   (accessory-style SKUs like 75LE-3010 that
                                                sit directly on the variant, and the
                                                single-size products where sku == parent_sku)
  3. the same two, case-insensitively, for whatever is left over
  4. unresolved

The child lookup wins over the parent lookup. ~1,590 SKUs are registered as both, almost
all of them self-parented single-size products where the two answers agree; only a handful
genuinely differ, and for those the child row is the specific answer.

is_active is deliberately not filtered. A deactivated parent must still resolve, because
the submit path has to be able to write to it.

Callers that must not proceed on a guess use resolve_parent/resolve_parents_strict, which
raise. Callers where a miss is a normal outcome (search, pollers) use resolve_parents,
which omits misses.
"""

from __future__ import annotations

import logging
from typing import Iterable

from tortoise import connections

logger = logging.getLogger(__name__)

_BATCH = 5000


class SkuResolutionError(Exception):
    """One or more SKUs are not registered in the products database."""

    def __init__(self, unresolved: list[str]):
        self.unresolved = unresolved
        shown = ", ".join(unresolved[:10])
        if len(unresolved) > 10:
            shown += f", ... (+{len(unresolved) - 10} more)"
        super().__init__(
            f"{len(unresolved)} SKU(s) not registered in the products database: {shown}"
        )


async def resolve_parents(skus: Iterable[str]) -> dict[str, str]:
    """Map each resolvable SKU to its registered parent SKU.

    SKUs that resolve to nothing are omitted from the result.
    """
    conn = connections.get("product_db")
    unique = sorted({s for s in skus if s})
    mapping: dict[str, str] = {}

    for i in range(0, len(unique), _BATCH):
        batch = unique[i:i + _BATCH]

        child_rows = await conn.execute_query_dict(
            "SELECT sku, parent_sku FROM child_products "
            "WHERE sku = ANY($1::text[]) AND parent_sku IS NOT NULL",
            [batch],
        )
        child_map = {row["sku"]: row["parent_sku"] for row in child_rows}

        # The whole batch, not just the unresolved remainder: it is the same single
        # indexed query either way, and covering everything is what lets us see a SKU
        # that is registered as both a child and a parent.
        parent_rows = await conn.execute_query_dict(
            "SELECT sku FROM parent_products WHERE sku = ANY($1::text[])",
            [batch],
        )
        parent_set = {row["sku"] for row in parent_rows}

        for sku in batch:
            if sku in child_map:
                mapping[sku] = child_map[sku]
                if sku in parent_set and child_map[sku] != sku:
                    logger.warning(
                        "SKU %s is registered as both a child of %s and a parent in its "
                        "own right; resolving to the child's parent",
                        sku,
                        child_map[sku],
                    )
            elif sku in parent_set:
                mapping[sku] = sku

        residue = [s for s in batch if s not in mapping]
        if residue:
            mapping.update(await _resolve_case_insensitive(conn, residue))

    return mapping


async def _resolve_case_insensitive(conn, skus: list[str]) -> dict[str, str]:
    """Last-resort pass for SKUs that differ from the products DB only in case.

    Runs on the leftovers only, so the common path stays at two exact-match queries. Every
    hit is logged: case drift between SellerCloud and the products DB is a data problem
    worth seeing, it is just not worth failing a whole batch over.
    """
    lowered = {s.lower(): s for s in skus}
    keys = sorted(lowered)
    mapping: dict[str, str] = {}

    child_rows = await conn.execute_query_dict(
        "SELECT sku, parent_sku FROM child_products "
        "WHERE lower(sku) = ANY($1::text[]) AND parent_sku IS NOT NULL",
        [keys],
    )
    for row in child_rows:
        original = lowered.get(row["sku"].lower())
        if original:
            mapping[original] = row["parent_sku"]

    remaining = [lowered[k] for k in keys if lowered[k] not in mapping]
    if remaining:
        parent_rows = await conn.execute_query_dict(
            "SELECT sku FROM parent_products WHERE lower(sku) = ANY($1::text[])",
            [sorted({s.lower() for s in remaining})],
        )
        for row in parent_rows:
            original = lowered.get(row["sku"].lower())
            if original:
                mapping.setdefault(original, row["sku"])

    for sku, parent in mapping.items():
        logger.warning(
            "SKU %s resolved to parent %s only after a case-insensitive match; "
            "the products DB and the source disagree on case",
            sku,
            parent,
        )

    return mapping


async def resolve_parents_strict(skus: Iterable[str]) -> dict[str, str]:
    """resolve_parents, but raise SkuResolutionError naming every SKU that did not resolve."""
    requested = [s for s in skus if s]
    mapping = await resolve_parents(requested)

    unresolved = sorted({s for s in requested if s not in mapping})
    if unresolved:
        raise SkuResolutionError(unresolved)

    return mapping


async def resolve_parent(sku: str) -> str:
    """Registered parent for a single SKU. Raises SkuResolutionError if it does not resolve."""
    if not sku:
        raise SkuResolutionError([""])

    mapping = await resolve_parents([sku])
    if sku not in mapping:
        raise SkuResolutionError([sku])

    return mapping[sku]


async def child_skus_for(parent_sku: str, include_inactive: bool = False) -> list[str]:
    """The SKUs registered under a parent.

    These are exactly the product IDs SellerCloud uses for that product's variants, which
    is what lets callers match catalog items by membership instead of by chopping an ID
    apart. Index-backed by idx_child_products_parent.
    """
    if not parent_sku:
        return []

    conn = connections.get("product_db")
    sql = "SELECT sku FROM child_products WHERE parent_sku = $1"
    if not include_inactive:
        sql += " AND is_active"

    rows = await conn.execute_query_dict(sql, [parent_sku])
    return [row["sku"] for row in rows]
