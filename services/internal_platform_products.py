"""Parent-SKU resolution for the STS pipeline.

The pipeline's rule is: a product may only be listed on STS if SkuBase knows about it,
i.e. one of its Shopify variant SKUs resolves to a registered parent in parent_products.
Products with no registered parent are NOT listed - they cannot be normalized safely and
would escape the destination ownership guard.

The resolution itself now lives in services/product_resolver.py, which is shared with
batch creation, the SellerCloud reads and the pollers. resolve_registered_parents is kept
as the name this pipeline uses; it is the shared lenient resolver, which omits SKUs that
do not resolve. Note the shared resolver adds a case-insensitive last-resort pass, so a
Shopify variant SKU that differs from the products DB only in case now resolves (and logs
a warning) where it was previously rejected - it is a product SkuBase does know about.

This is the I/O layer. The pure rules module stays free of DB access; the pollers call
resolve_registered_parents() once per cycle and pass the results in.

REASSIGNED SKUs
---------------
A "merge" repoints child_products.parent_sku at the new parent and sets is_primary=false,
while the SKU STRING keeps its original OLDPARENT/SIZE shape. resolve_parents therefore
returns the NEW parent for a reassigned SKU, which for this pipeline is a mislink: the
1nventory product has not changed, but it starts reporting a parent it is not, and
internal_platform_state is keyed on exactly that value.

Measured in prod 2026-08-03: 24 state rows were built entirely on reassigned SKUs, 9 live on
Shop The Sample, and three rows had their source_product_gid overwritten by a different
garment's product. So resolution here filters them out - see load_reassigned below.
"""

from __future__ import annotations

import logging
from typing import Iterable, Mapping, Sequence

from tortoise import connections

from services.product_resolver import resolve_parents as resolve_registered_parents

logger = logging.getLogger(__name__)

__all__ = ["resolve_registered_parents", "load_reassigned", "product_parent"]

# Matches product_resolver._BATCH. Same shape of query against the same table sizes, so
# there is no reason for the two to disagree.
_BATCH = 5000


async def load_reassigned(skus: Iterable[str]) -> dict[str, str]:
    """Map each reassigned (secondary) SKU to the primary SKU it now defers to.

    Reads the `secondary_skus` materialized view, which is the codebase's canonical
    "has this SKU been reassigned" signal - the same source product_service's search
    fallback, the export endpoints and SecondaryInventoryTransferPoller all use. Deriving
    it here from child_products.is_primary instead would be a second definition that can
    disagree with those; the matview also resolves multi-hop chains A->B->C, which a bare
    is_primary check cannot.

    Deliberately NOT added to product_resolver.py. That module is shared with batch
    creation and every SellerCloud path, which must keep writing to secondaries.

    SKUs that were never reassigned are omitted, so `sku in reassigned` is the test.
    """
    conn = connections.get("product_db")
    unique = sorted({s for s in skus if s})
    mapping: dict[str, str] = {}

    for i in range(0, len(unique), _BATCH):
        batch = unique[i:i + _BATCH]
        rows = await conn.execute_query_dict(
            "SELECT secondary_sku, current_primary_sku FROM secondary_skus "
            "WHERE secondary_sku = ANY($1::text[])",
            [batch],
        )
        for row in rows:
            mapping[row["secondary_sku"]] = row["current_primary_sku"]

    return mapping


def product_parent(variant_skus: Sequence[str | None],
                   registered: Mapping[str, str],
                   reassigned: Mapping[str, str] | None = None) -> str | None:
    """Registered parent for a product, or None if no PRIMARY variant SKU resolves.

    None means: do not list. Takes the parent from the first variant SKU that resolves;
    a well-formed product's variants all share one parent, so order does not matter.

    Reassigned SKUs are skipped rather than resolved. Resolving one returns the parent of
    the product the SKU was merged INTO, which is a different garment - that is how
    1nventory product 10142326358316 (MULTICOLOR AMARINO, one SKU) came to own the state
    row for RHD-MOTW-0038 (BLACK AMARINO, five SKUs) and overwrite its source_product_gid.

    `reassigned` defaults to None so existing callers keep the old behaviour; the STS
    pollers always pass it.
    """
    for sku in variant_skus:
        if not sku or sku not in registered:
            continue
        if reassigned and sku in reassigned:
            continue
        return registered[sku]
    return None


def reassigned_skus_on(variant_skus: Sequence[str | None],
                       reassigned: Mapping[str, str]) -> list[str]:
    """The product's variant SKUs that have been reassigned away, in variant order.

    Used to tell "this product is unregistered" (SkuBase never knew it) apart from "this
    product's SKUs were merged elsewhere" - two very different outcomes that both make
    product_parent return None.
    """
    return [s for s in variant_skus if s and s in reassigned]
