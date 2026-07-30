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
"""

from __future__ import annotations

import logging
from typing import Sequence

from services.product_resolver import resolve_parents as resolve_registered_parents

logger = logging.getLogger(__name__)

__all__ = ["resolve_registered_parents", "product_parent"]


def product_parent(variant_skus: Sequence[str | None],
                   registered: dict[str, str]) -> str | None:
    """Registered parent for a product, or None if no variant SKU resolves.

    None means: do not list. Takes the parent from the first variant SKU that resolves;
    a well-formed product's variants all share one parent, so order does not matter.
    """
    for sku in variant_skus:
        if sku and sku in registered:
            return registered[sku]
    return None
