"""Typed Shopify product / inventory operations for the consignment pipeline.

Sits on shopify_client (transport). This layer changes with every phase; the transport
below it should not.

Two prohibitions enforced here rather than left to discipline:

1. NO `productUpdate { tags }`. That field is replace-mode. Measured against 250 live
   destination products, a replace would destroy tags on 196 of them (78%), including
   `stock:low`, `arrival:new`, and the correct live category tags SHOE/JACKET/ACC.
   Tag writes go through tagsAdd / scoped tagsRemove only.

2. NO `ignoreCompareQuantity: true`. Syncio writes inventory concurrently and cannot be
   paused, so a compare-and-set mismatch means Syncio raced us. The response is
   skip-and-flag, never force.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, AsyncIterator, Mapping, Sequence

from services.internal_platform_rules import money
from services.shopify_client import ShopifyClient, ShopifySemanticError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Aliased-mutation cost model
# ---------------------------------------------------------------------------
# Shopify charges a GraphQL document its STATICALLY COMPUTED requested cost, which for an
# aliased mutation is the sum of its aliases. Two independent limits follow, and they do
# not trade against each other:
#
#   MAX_QUERY_COST   a hard per-document cap. Over it, the request is rejected BEFORE
#                    execution (MAX_COST_EXCEEDED) no matter how much bucket is banked.
#   the bucket       4,000 capacity restoring at 200/s, measured live 2026-07-29. This
#                    limits requests per second, not document size.
#
# Measured 2026-07-29 against the live stores: productVariantsBulkUpdate costs 10 points
# for one variant and 30 for a hundred; tagsAdd, tagsRemove and productUpdate are 10 flat;
# 80 products in one reprice document came to 848 requested points and 120 was rejected
# at 1,272.
#
# These functions exist so batch size is DERIVED from the work rather than guessed. The
# same numbers were previously inline in two methods here and implied by two magic chunk
# constants in the destination poller - four copies that had to agree and none of which
# referenced each other. A product with a deep size run costs three times a single-variant
# one, so a fixed product count is either unsafe for the first or wasteful for the second.

MAX_QUERY_COST = 1000

# Hard ceiling on `nodes(ids:)`. A separate limit from MAX_QUERY_COST and unrelated to it:
# this one is validated on the input array before costing happens, and over it the request
# is rejected with "The input array size of N is greater than the maximum allowed of 250"
# no matter how cheap the query would have been.
NODES_MAX_IDS = 250

# The budget we pack to, and the ONLY place safety margin lives. The cost functions below
# model what Shopify charges and nothing else; a fudge factor inside them (reprice_cost
# carried a +1) hides the margin in two places at once and makes neither number mean what
# it says. Measured against the live distribution, Shopify's actual charge runs ~2% above
# the documented formula - 10.60 per product observed against 10.39 predicted, the gap
# being the `userErrors { field message code }` selection each alias returns. A 5% budget
# margin covers that with room left, and 91 products still fit one reprice document.
DOC_BUDGET = 950


def reprice_cost(variant_count: int) -> int:
    """Requested cost of one aliased productVariantsBulkUpdate: Shopify's formula, exact.

    Deliberately NOT rounded up. Under-packing costs requests; over-packing costs a
    rejected document. The margin for the difference belongs in DOC_BUDGET, where it is
    one visible tunable number rather than a silent per-item tax.
    """
    return 10 + variant_count // 5


def normalize_cost(product_update: Any, add: Sequence[str],
                   remove: Sequence[str]) -> int:
    """Requested cost of one product's normalization: 10 per alias it contributes.

    `product_update` is truthy when the product needs a productUpdate at all - vendor,
    title, or both. It is deliberately ONE argument rather than one per field: Shopify
    charges a mutation 10 points flat however many fields it sets, so vendor and title
    share a single alias and adding a third field later would not change this number
    either.

    Costed from what the product ACTUALLY needs, not the worst case. Most products need
    one or two aliases, so assuming three - as a fixed chunk of 28 did - left roughly 40%
    of every document unused.
    """
    return 10 * (bool(product_update) + bool(add) + bool(remove))


def batches_by_cost(items: Sequence[Any], cost_of: Any,
                    budget: int = DOC_BUDGET) -> list[list[Any]]:
    """Split items into groups that each fit one document.

    An item whose own cost exceeds the budget still gets its own group rather than being
    dropped: rejection by Shopify is a far better failure than silent omission, and it
    surfaces as one identifiable product instead of a vanished write.
    """
    groups: list[list[Any]] = []
    current: list[Any] = []
    cost = 0
    for item in items:
        c = cost_of(item)
        if current and cost + c > budget:
            groups.append(current)
            current, cost = [], 0
        current.append(item)
        cost += c
    if current:
        groups.append(current)
    return groups


# ---------------------------------------------------------------------------
# Read shapes
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class Variant:
    gid: str
    sku: str | None
    price: Decimal | None
    compare_at: Decimal | None
    inventory_quantity: int
    inventory_item_gid: str | None = None


@dataclass(frozen=True, slots=True)
class Product:
    gid: str
    title: str
    vendor: str | None
    product_type: str | None
    status: str
    tags: tuple[str, ...]
    updated_at: str | None
    variants: tuple[Variant, ...]
    syncio_source_gid: str | None
    image_url: str | None = None
    handle: str | None = None
    # None when the product is not live on the storefront (DRAFT, or unpublished from the
    # Online Store channel). Callers should treat None as "not visible to shoppers" rather
    # than "unknown".
    online_store_url: str | None = None

    @property
    def total_inventory(self) -> int:
        return sum(v.inventory_quantity for v in self.variants)

    @property
    def variant_prices(self) -> tuple[Decimal | None, ...]:
        return tuple(v.price for v in self.variants)

    @property
    def variant_compare_at(self) -> tuple[Decimal | None, ...]:
        return tuple(v.compare_at for v in self.variants)

    @property
    def variant_inventory(self) -> tuple[int, ...]:
        """Per-variant stock, parallel to variant_prices. Pricing filters on this."""
        return tuple(v.inventory_quantity for v in self.variants)

    @property
    def variant_skus(self) -> tuple[str | None, ...]:
        return tuple(v.sku for v in self.variants)


# featuredImage and the syncio metafield are both selected as SINGULAR object fields, not
# connections. `images(first: 1)` would cost ~10 points per product where this costs 1;
# measured, adding featuredImage took a 250-product page from 57 to 68 actual points, and
# returned an image for 250 of 250 products.
#
# The syncio metafield is likewise singular: 1 point per product instead of ~10, and it
# works inside bulk operations.
#
# Note we filter by `tag:`, never by the metafield. Shopify requires filtering to be
# enabled on the metafield DEFINITION, which Syncio owns, and a query filtering on a
# non-filterable metafield SILENTLY RETURNS UNFILTERED RESULTS rather than erroring.
# Here that would mean treating the entire catalog as matched.
#
# handle and onlineStoreUrl are both SCALARS, so unlike featuredImage they are free:
# measured on the live store 2026-08-13, a 250-product page costs requestedQueryCost 255 /
# actualQueryCost 68 with and without them. That matters because this fragment drives the
# source scan's 58-page sweep every five minutes.
#
# onlineStoreUrl is null whenever the product is not live on the storefront, DRAFT
# included. That null is the signal, not a gap: it is Shopify's own answer to "can a
# shopper see this", which is exactly what the product-page links need.
_PRODUCT_FIELDS = """
      id handle onlineStoreUrl title vendor productType status tags updatedAt
      featuredImage { url }
      metafield(namespace: "syncio", key: "source_product_id") { value }
      variants(first: 100) {
        nodes { id sku price compareAtPrice inventoryQuantity inventoryItem { id } }
      }
"""

PRODUCTS_PAGE = f"""
query($first: Int!, $after: String, $q: String) {{
  products(first: $first, after: $after, query: $q) {{
    nodes {{{_PRODUCT_FIELDS}}}
    pageInfo {{ hasNextPage endCursor }}
  }}
}}
"""

PRODUCT_BY_ID = f"""
query($id: ID!) {{
  product(id: $id) {{{_PRODUCT_FIELDS}}}
}}
"""

# Bulk re-read of a known, small set. The manual submit uses this instead of re-scanning
# the catalog: it already knows WHICH products it wants from stored state and only needs
# to confirm they are still sellable, so this is one cheap query rather than 58 pages.
PRODUCTS_BY_IDS = f"""
query($ids: [ID!]!) {{
  nodes(ids: $ids) {{
    ... on Product {{{_PRODUCT_FIELDS}}}
  }}
}}
"""


def parse_product(node: Mapping[str, Any]) -> Product:
    """GraphQL shape -> typed value. dict[str, Any] must not escape this module."""
    variants = tuple(
        Variant(
            gid=v["id"],
            sku=v.get("sku"),
            price=money(v.get("price")),
            compare_at=money(v.get("compareAtPrice")),
            inventory_quantity=int(v.get("inventoryQuantity") or 0),
            inventory_item_gid=(v.get("inventoryItem") or {}).get("id"),
        )
        for v in (node.get("variants") or {}).get("nodes") or []
    )
    return Product(
        gid=node["id"],
        title=node.get("title") or "",
        vendor=node.get("vendor"),
        product_type=node.get("productType"),
        status=node.get("status") or "",
        tags=tuple(node.get("tags") or ()),
        updated_at=node.get("updatedAt"),
        variants=variants,
        syncio_source_gid=(node.get("metafield") or {}).get("value"),
        image_url=(node.get("featuredImage") or {}).get("url"),
        handle=node.get("handle"),
        online_store_url=node.get("onlineStoreUrl"),
    )


def escape_query_value(value: str) -> str:
    """Escape a value going into Shopify's `query:` filter mini-language.

    GraphQL variables do NOT protect this: the string is parsed by Shopify after
    substitution. An unescaped value like `A OR id:*` widens the result set, and in the
    delist path the scanned set is the delete set.
    """
    return value.replace("\\", "\\\\").replace('"', '\\"')


class ShopifyAdmin:
    """Typed operations against one store."""

    def __init__(self, client: ShopifyClient) -> None:
        self.client = client

    @property
    def store_id(self) -> str:
        return self.client.store.store_id

    # -- reads -------------------------------------------------------------

    async def products_by_tag(
        self, tag: str, *, updated_after: str | None = None, page_size: int = 250
    ) -> AsyncIterator[Product]:
        """Page products carrying `tag`, optionally only those changed recently.

        An empty tag means "every product", which is what the source scan wants. Building
        `tag:""` instead would match NOTHING - and because a zero-result scan is
        indistinguishable from "everything stopped qualifying", it would look like a mass
        drop-out rather than a broken query.

        The updated_at watermark should carry ~15 minutes of overlap: Shopify's
        updated_at search index is eventually consistent, and our own writes bump it.
        Duplicate yields are absorbed by desired-state diffing.
        """
        clauses = []
        if tag:
            clauses.append(f'tag:"{escape_query_value(tag)}"')
        q = " AND ".join(clauses) if clauses else None
        if updated_after:
            clause = f'updated_at:>"{escape_query_value(updated_after)}"'
            q = f"{q} AND {clause}" if q else clause
        async for node in self.client.paginate(
            PRODUCTS_PAGE, {"q": q},
            connection_path=["products"],
            operation=f"products.by_tag[{self.store_id}]",
            page_size=page_size,
        ):
            yield parse_product(node)

    async def products_by_ids(self, gids: Sequence[str]) -> list[Product]:
        """Fetch a known set of products, however many. Chunked at NODES_MAX_IDS.

        `nodes(ids:)` rejects more than 250 ids outright - "The input array size of 398 is
        greater than the maximum allowed of 250" - and it is a hard API limit, not a cost
        ceiling, so no budget arithmetic avoids it. Chunking lives HERE rather than in the
        callers because the limit belongs to the query: manual_submit hit this the first
        time max_products_in_flight was set to 0 and all 398 ready products became one
        batch, and a caller that has to remember a transport limit will forget it.

        Sequential, not concurrent. Four reads cost ~316 points against a 4,000 bucket
        restoring at 200/s, so the governor never paces them and the wall clock is about a
        second - not worth the extra failure modes concurrency brings to a read path.

        Nodes that came back null - deleted since we stored them - are dropped rather
        than raising: a product vanishing between the scan and the submit is a normal
        race, and the caller simply has one fewer candidate.
        """
        if not gids:
            return []
        ids = list(gids)
        out: list[Product] = []
        for i in range(0, len(ids), NODES_MAX_IDS):
            data = await self.client.execute(
                PRODUCTS_BY_IDS, {"ids": ids[i:i + NODES_MAX_IDS]},
                operation=f"products.by_ids[{self.store_id}]",
            )
            out.extend(parse_product(n) for n in (data.get("nodes") or []) if n)
        return out

    async def get_product(self, gid: str) -> Product | None:
        data = await self.client.execute(
            PRODUCT_BY_ID, {"id": gid}, operation=f"product.get[{self.store_id}]"
        )
        node = data.get("product")
        return parse_product(node) if node else None

    # -- tag writes --------------------------------------------------------

    async def add_tags(self, gid: str, tags: Sequence[str]) -> None:
        """Additive and atomic. Matches Matrixify MERGE semantics."""
        if not tags:
            return
        await self.client.execute(
            """
            mutation($id: ID!, $tags: [String!]!) {
              tagsAdd(id: $id, tags: $tags) { node { id } userErrors { field message } }
            }
            """,
            {"id": gid, "tags": list(tags)},
            operation=f"tagsAdd[{self.store_id}]",
            mutation_name="tagsAdd",
            is_write=True,
        )

    async def remove_tags(self, gid: str, tags: Sequence[str]) -> None:
        """Remove ONLY tags this automation owns. Never a blanket replace."""
        if not tags:
            return
        await self.client.execute(
            """
            mutation($id: ID!, $tags: [String!]!) {
              tagsRemove(id: $id, tags: $tags) { node { id } userErrors { field message } }
            }
            """,
            {"id": gid, "tags": list(tags)},
            operation=f"tagsRemove[{self.store_id}]",
            mutation_name="tagsRemove",
            is_write=True,
        )

    # -- product writes ----------------------------------------------------

    async def update_vendor(self, gid: str, vendor: str) -> None:
        """Vendor only.

        Deliberately narrow. productUpdate can also set `tags`, and that field is
        replace-mode; keeping this method single-purpose means no caller can reach it.
        productSet is likewise avoided: its list fields delete entries absent from the
        input, which on a Syncio-managed product is a foot-gun.
        """
        await self.client.execute(
            """
            mutation($product: ProductUpdateInput!) {
              productUpdate(product: $product) {
                product { id vendor }
                userErrors { field message }
              }
            }
            """,
            {"product": {"id": gid, "vendor": vendor}},
            operation=f"productUpdate.vendor[{self.store_id}]",
            mutation_name="productUpdate",
            is_write=True,
        )

    async def set_variant_prices(
        self, product_gid: str, variants: Sequence[tuple[str, Decimal, Decimal | None]]
    ) -> None:
        """Uniform price across a product's variants, in one mutation.

        allowPartialUpdates stays false: the pricing rule is uniform-per-product, so a
        partial write would leave mixed pricing that no ledger row can describe. All or
        nothing means a failed normalize is retried as a whole.
        """
        if not variants:
            return
        payload = [
            {
                "id": vid,
                "price": str(price),
                **({"compareAtPrice": str(compare)} if compare is not None else {}),
            }
            for vid, price, compare in variants
        ]
        await self.client.execute(
            """
            mutation($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
              productVariantsBulkUpdate(
                productId: $productId, variants: $variants, allowPartialUpdates: false
              ) {
                productVariants { id price compareAtPrice }
                userErrors { field message code }
              }
            }
            """,
            {"productId": product_gid, "variants": payload},
            operation=f"productVariantsBulkUpdate[{self.store_id}]",
            mutation_name="productVariantsBulkUpdate",
            is_write=True,
        )

    # -- inventory (BLOCKED: needs read_locations + write_inventory) --------

    async def locations(self) -> list[tuple[str, str]]:
        data = await self.client.execute(
            "{ locations(first: 50, includeInactive: true) { nodes { id name } } }",
            operation=f"locations[{self.store_id}]",
        )
        return [(n["id"], n["name"]) for n in data["locations"]["nodes"]]

    async def resolve_location_gid(self, name: str) -> str:
        """Name -> GID, asserting exactly one match.

        Called once at startup, never per cycle. Matching a location by display name at
        write time is the single most dangerous thing this pipeline could do: rename the
        location and suddenly no location matches, so every location is a
        'non-Lakewood' location and the whole catalog gets zeroed.
        """
        matches = [(gid, n) for gid, n in await self.locations() if n == name]
        if len(matches) != 1:
            raise ValueError(
                f"location {name!r} resolved to {len(matches)} matches on "
                f"{self.store_id}; refusing to proceed"
            )
        return matches[0][0]

    async def zero_inventory_at(
        self, *, inventory_item_gid: str, location_gid: str,
        compare_quantity: int, idempotency_key: str,
    ) -> bool:
        """Set a location's available quantity to 0, compare-and-set.

        Returns False if Syncio raced us (CAS mismatch) so the caller can skip and flag.
        `@idempotent` is optional in 2026-01 and MANDATORY from 2026-04; written now so
        the version bump is not a forced code change.
        """
        try:
            await self.client.execute(
                """
                mutation($input: InventorySetQuantitiesInput!, $key: String!) {
                  inventorySetQuantities(input: $input) @idempotent(key: $key) {
                    inventoryAdjustmentGroup { reason changes { name delta } }
                    userErrors { code field message }
                  }
                }
                """,
                {
                    "input": {
                        "name": "available",
                        "reason": "correction",
                        "referenceDocumentUri":
                            f"skubase://internal-platform/{self.store_id}/location",
                        # ignoreCompareQuantity is deliberately absent. Do not add it.
                        "quantities": [{
                            "inventoryItemId": inventory_item_gid,
                            "locationId": location_gid,
                            "quantity": 0,
                            "compareQuantity": compare_quantity,
                        }],
                    },
                    "key": idempotency_key,
                },
                operation=f"inventorySetQuantities[{self.store_id}]",
                mutation_name="inventorySetQuantities",
                is_write=True,
            )
            return True
        except ShopifySemanticError as exc:
            if any("compare" in str(e.get("code", "")).lower()
                   or "compare" in str(e.get("message", "")).lower()
                   for e in exc.user_errors):
                logger.warning(
                    "%s: inventory CAS mismatch on %s, skipping (Syncio raced us)",
                    self.store_id, inventory_item_gid,
                )
                return False
            raise

    async def set_variant_prices_bulk(
        self, items: Sequence[tuple[str, Sequence[tuple[str, Decimal, Decimal | None]]]],
        max_cost: int = DOC_BUDGET,
    ) -> dict[str, str | None]:
        """Price many products per request. Returns {product_gid: error or None}.

        Split by reprice_cost, so how many products travel together depends on how many
        variants they carry: 95 single-variant parents fill a document, 47 with a hundred
        variants each fill the same budget. See the cost model at the top of this module
        for the measurements behind both numbers.

        Errors are attributed PER ALIAS. A single failing product must not fail its
        batch-mates, which is why this returns a map rather than raising: the caller marks
        exactly the products Shopify complained about.
        """
        results: dict[str, str | None] = {}

        for batch in batches_by_cost(
            list(items), lambda it: reprice_cost(len(it[1])), max_cost
        ):
            aliases, variables, defs = [], {}, []
            for n, (gid, variants) in enumerate(batch):
                defs.append(f"$p{n}: ID!")
                defs.append(f"$v{n}: [ProductVariantsBulkInput!]!")
                variables[f"p{n}"] = gid
                variables[f"v{n}"] = [
                    {"id": vid, "price": str(price),
                     **({"compareAtPrice": str(cmp)} if cmp is not None else {})}
                    for vid, price, cmp in variants
                ]
                aliases.append(
                    f"b{n}: productVariantsBulkUpdate("
                    f"productId: $p{n}, variants: $v{n}, allowPartialUpdates: false) "
                    "{ userErrors { field message code } }"
                )
            data = await self.client.execute(
                "mutation(" + ", ".join(defs) + ") { " + " ".join(aliases) + " }",
                variables,
                operation=f"productVariantsBulkUpdate.bulk[{self.store_id}]",
                is_write=True,
            )
            for n, (gid, _) in enumerate(batch):
                errs = (data.get(f"b{n}") or {}).get("userErrors") or []
                results[gid] = (
                    "; ".join(f"{e.get('field') or '?'}: {e.get('message')}" for e in errs)
                    if errs else None
                )
        return results

    async def apply_normalizations_bulk(
        self,
        items: Sequence[
            tuple[str, str | None, str | None, Sequence[str], Sequence[str]]],
        max_cost: int = DOC_BUDGET,
    ) -> dict[str, str | None]:
        """Vendor, title and tag corrections for many products per request.

        items is (product_gid, vendor or None, title or None, tags to add, tags to
        remove). Split by normalize_cost, which charges 10 points per alias the product
        ACTUALLY needs, so a document holds 28 products that each need all three
        corrections but 85 that need only one.

        Vendor and title share ONE productUpdate alias. Shopify charges a mutation 10
        points flat regardless of how many fields it sets, so correcting both costs
        exactly what correcting either alone costs - which is why title normalization
        added no measurable load when it was introduced.

        add and remove are issued in the same document with no ordering between them.
        That is safe because desired_tags builds them disjoint by construction - every
        added tag is in the desired set and every removed tag is not - verified against
        151 live tag writes with zero overlap. The ordering that DOES matter in this
        pipeline is untag-before-delete on the delist path, which is a different thing.

        Still tagsAdd/tagsRemove, never productUpdate{tags}: that field is replace-mode
        and would destroy tags this automation does not own.
        """
        results: dict[str, str | None] = {}

        for batch in batches_by_cost(
            list(items),
            lambda it: normalize_cost(it[1] or it[2], it[3], it[4]), max_cost
        ):
            defs, aliases, variables = [], [], {}
            for n, (gid, vendor, title, add, remove) in enumerate(batch):
                # $id{n} is declared ONLY when a tag mutation will reference it.
                # productUpdate carries the id inside its input object instead, so a
                # product needing only a vendor or title correction never uses $id{n} -
                # and GraphQL rejects the WHOLE DOCUMENT for one unused variable:
                # "Variable $id0 is declared by anonymous mutation but not used".
                #
                # Latent since this method was written, but unreachable in practice while
                # vendor was the only productUpdate field: vendor-only products are rare
                # (4 against 184 tag changes on the first pass) and never landed in a
                # batch alone. Title normalization made them common - 592 products needed
                # a title and nothing else - and 293 normalizations failed on the first
                # production cycle after it shipped.
                if add or remove:
                    defs.append(f"$id{n}: ID!")
                    variables[f"id{n}"] = gid
                if vendor or title:
                    fields = {"id": gid}
                    if vendor:
                        fields["vendor"] = vendor
                    if title:
                        fields["title"] = title
                    defs.append(f"$vp{n}: ProductUpdateInput!")
                    variables[f"vp{n}"] = fields
                    aliases.append(
                        f"v{n}: productUpdate(product: $vp{n}) "
                        "{ userErrors { field message } }"
                    )
                if add:
                    defs.append(f"$at{n}: [String!]!")
                    variables[f"at{n}"] = list(add)
                    aliases.append(
                        f"a{n}: tagsAdd(id: $id{n}, tags: $at{n}) "
                        "{ userErrors { field message } }"
                    )
                if remove:
                    defs.append(f"$rt{n}: [String!]!")
                    variables[f"rt{n}"] = list(remove)
                    aliases.append(
                        f"r{n}: tagsRemove(id: $id{n}, tags: $rt{n}) "
                        "{ userErrors { field message } }"
                    )
            if aliases:
                data = await self.client.execute(
                    "mutation(" + ", ".join(defs) + ") { " + " ".join(aliases) + " }",
                    variables,
                    operation=f"normalize.bulk[{self.store_id}]",
                    is_write=True,
                )
                for n, (gid, vendor, title, add, remove) in enumerate(batch):
                    errs = []
                    for prefix, present in (("v", vendor or title), ("a", add),
                                            ("r", remove)):
                        if not present:
                            continue
                        errs += (data.get(f"{prefix}{n}") or {}).get("userErrors") or []
                    results[gid] = (
                        "; ".join(f"{e.get('field') or '?'}: {e.get('message')}"
                                  for e in errs)
                        if errs else None
                    )
        return results

    # -- listing creation (the 1nventory submission platform) --------------
    #
    # These serve services/onenventory_service.py, which creates products from a SkuBase
    # listing. Everything above this line belongs to the consignment pipeline and edits
    # products someone else created; this section is the only place that creates one.
    #
    # Three details below are not guesses. They were established by introspection and live
    # runs against api_version 2026-01 and each one silently breaks the flow if changed:
    #
    #   - Publishable exposes availablePublicationsCount and resourcePublicationsCount,
    #     both PLURAL. The singular spellings are rejected BEFORE execution, so a create
    #     succeeds and the publish never runs, leaving an ACTIVE but invisible product.
    #   - productCreate does not accept variants. They go through
    #     productVariantsBulkCreate with REMOVE_STANDALONE_VARIANT, which drops the
    #     default variant Shopify auto-creates.
    #   - productCreate sets status but does NOT put the product on a sales channel.
    #     Without publish_to_online_store the product exists, is active, and no shopper
    #     can see it.

    async def find_product_by_variant_sku(
        self, skus: Sequence[str]
    ) -> dict[str, Any] | None:
        """First product owning any of these variant SKUs, or None.

        The identity check before a create. Deliberately NOT by handle: the AppScript's
        handle is built from a CUT title (brand stripped, trimmed at " size" and "$", run
        through a find-and-replace sheet), so a handle rebuilt from SkuBase form data
        misses the ~14,400 products already on the store and duplicates every one of them.
        A variant SKU is exact and survives retitling.
        """
        if not skus:
            return None

        # ONE query with an OR of every child SKU, not one per SKU. The loop version paid
        # its maximum cost precisely when the answer was "not on Shopify" - it had to try
        # every SKU to conclude nothing - which is the common case in a bulk backfill.
        query = " OR ".join(f"sku:{escape_query_value(s)}" for s in skus)
        data = await self.client.execute(
            """
            query($q: String!) {
              products(first: 5, query: $q) {
                nodes {
                  id handle title status
                  variants(first: 100) { nodes { id sku } }
                }
              }
            }
            """,
            {"q": query},
            operation=f"product.findBySku[{self.store_id}]",
        )
        nodes = (data.get("products") or {}).get("nodes") or []
        return nodes[0] if nodes else None

    async def create_product(
        self, product: Mapping[str, Any], media: Sequence[Mapping[str, Any]] = ()
    ) -> dict[str, Any]:
        """productCreate. Returns the created product node.

        `product` carries tags on CREATE only, which is safe: there is nothing to
        destroy on a product that did not exist a moment ago. Never pass tags to
        productUpdate - see the prohibition at the top of this module.
        """
        data = await self.client.execute(
            """
            mutation($product: ProductCreateInput!, $media: [CreateMediaInput!]) {
              productCreate(product: $product, media: $media) {
                product { id handle title status }
                userErrors { field message }
              }
            }
            """,
            {"product": dict(product), "media": [dict(m) for m in media]},
            operation=f"productCreate[{self.store_id}]",
            mutation_name="productCreate",
            is_write=True,
        )
        return (data.get("productCreate") or {}).get("product") or {}

    async def create_variants(
        self, product_gid: str, variants: Sequence[Mapping[str, Any]],
        *, strategy: str = "PRESERVE_STANDALONE_VARIANT",
    ) -> list[dict[str, Any]]:
        """productVariantsBulkCreate. The strategy is the CALLER's decision, per path.

        There is no single safe value, and assuming there was took the create path down
        for four days on 2026-08-28.

        REMOVE_STANDALONE_VARIANT - "Deletes the existing standalone variant when the
        product has only a single default ("Default Title") or custom variant."
        Required on CREATE. _create passes productOptions, so productCreate auto-generates
        a variant for the FIRST size before we add any. That placeholder carries a real
        option value, which makes it a *custom* standalone, and it must go or the real
        variant for that size collides with it as ['variants','0']: The variant 'XS'
        already exists.

        PRESERVE_STANDALONE_VARIANT - keeps it. Required on UPDATE, where a product down to
        one variant holds a real, stock-bearing size. Passing REMOVE there deleted
        DNT-MJNS-0035/XL and the 11 units on it.

        DEFAULT is wrong for CREATE specifically: it removes only the "Default Title"
        placeholder and explicitly PRESERVES the standalone custom variant - which is the
        one create always produces.

        The default here is the conservative one: a caller that does not think about it
        never destroys a variant, it only risks a collision it will see immediately.
        """
        if not variants:
            return []
        data = await self.client.execute(
            """
            mutation($productId: ID!, $variants: [ProductVariantsBulkInput!]!,
                     $strategy: ProductVariantsBulkCreateStrategy) {
              productVariantsBulkCreate(productId: $productId, variants: $variants,
                                        strategy: $strategy) {
                productVariants { id sku price compareAtPrice }
                userErrors { field message }
              }
            }
            """,
            {"productId": product_gid, "variants": [dict(v) for v in variants],
             "strategy": strategy},
            operation=f"productVariantsBulkCreate[{self.store_id}]",
            mutation_name="productVariantsBulkCreate",
            is_write=True,
        )
        return (data.get("productVariantsBulkCreate") or {}).get("productVariants") or []

    async def update_variants(
        self, product_gid: str, variants: Sequence[Mapping[str, Any]]
    ) -> list[dict[str, Any]]:
        """productVariantsBulkUpdate for variants that already exist.

        Callers pass inventoryItem fields (cost, weight, requiresShipping). Inventory
        QUANTITY is never among them: the app holds no write_inventory scope, so an
        attempt would fail at the credential rather than silently zero live stock.

        allowPartialUpdates=true here, and false in set_variant_prices above. The two are
        not in tension: pricing carries a cross-variant invariant (uniform price per
        product), so a partial write there leaves mixed pricing no ledger row can describe.
        The fields THIS method writes - cost, weight, requiresShipping - are independent
        per variant, so there is no state a partial application can corrupt.

        What it buys: one unusable variant id no longer discards the refresh for every
        other variant. DNT-MJNS-0035 failed outright on three consecutive submissions over
        a single bad id and refreshed nothing. The userErrors array still reports the row
        that could not be applied, so a partial write is still visible rather than silent.
        """
        if not variants:
            return []
        data = await self.client.execute(
            """
            mutation($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
              productVariantsBulkUpdate(productId: $productId, variants: $variants,
                                        allowPartialUpdates: true) {
                productVariants {
                  id sku
                  inventoryItem { measurement { weight { value unit } } }
                }
                userErrors { field message }
              }
            }
            """,
            {"productId": product_gid, "variants": [dict(v) for v in variants]},
            operation=f"productVariantsBulkUpdate[{self.store_id}]",
            mutation_name="productVariantsBulkUpdate",
            is_write=True,
        )
        return (data.get("productVariantsBulkUpdate") or {}).get("productVariants") or []

    async def update_product_details(
        self, gid: str, fields: Mapping[str, Any]
    ) -> None:
        """productUpdate for descriptive fields on an existing product.

        `fields` is filtered against an allowlist rather than trusted, because
        productUpdate accepts `tags` and that field is replace-mode. Measured against 250
        live products, a replace would destroy tags on 78% of them. Here the specific
        casualty would be SHOPTHESAMPLE: strip it mid-flight and the consignment pipeline
        deletes the product from Shop The Sample. Tags go through add_tags only.
        """
        allowed = {"title", "descriptionHtml", "vendor", "productType", "category",
                   "handle", "status"}
        rejected = set(fields) - allowed
        if rejected:
            raise ValueError(
                f"update_product_details refuses {sorted(rejected)}; "
                "tags are replace-mode and must go through add_tags"
            )
        payload = {k: v for k, v in fields.items() if v not in (None, "")}
        if not payload:
            return
        await self.client.execute(
            """
            mutation($product: ProductUpdateInput!) {
              productUpdate(product: $product) {
                product { id }
                userErrors { field message }
              }
            }
            """,
            {"product": {"id": gid, **payload}},
            operation=f"productUpdate.details[{self.store_id}]",
            mutation_name="productUpdate",
            is_write=True,
        )

    async def publish_to_online_store(self, gid: str) -> tuple[int, int]:
        """publishablePublish onto the Online Store publication.

        Returns (on, available). productCreate leaves a product on NO sales channel, so
        without this it is active and invisible. Resolved by name rather than hardcoding
        the id, so it survives a store reconfiguration.
        """
        data = await self.client.execute(
            "query { publications(first: 50) { nodes { id name } } }",
            {},
            operation=f"publications[{self.store_id}]",
        )
        nodes = (data.get("publications") or {}).get("nodes") or []
        online = next((p for p in nodes if p["name"] == "Online Store"), None)
        if not online:
            logger.warning("%s: no 'Online Store' publication; product left unpublished",
                           self.store_id)
            return (0, len(nodes))

        data = await self.client.execute(
            """
            mutation($id: ID!, $input: [PublicationInput!]!) {
              publishablePublish(id: $id, input: $input) {
                publishable {
                  availablePublicationsCount { count }
                  resourcePublicationsCount { count }
                }
                userErrors { field message }
              }
            }
            """,
            {"id": gid, "input": [{"publicationId": online["id"]}]},
            operation=f"publishablePublish[{self.store_id}]",
            mutation_name="publishablePublish",
            is_write=True,
        )
        state = (data.get("publishablePublish") or {}).get("publishable") or {}
        return (
            (state.get("resourcePublicationsCount") or {}).get("count", 0),
            (state.get("availablePublicationsCount") or {}).get("count", 0),
        )

    # -- delete (BLOCKED on Phase 5; irreversible) -------------------------

    async def delete_product(self, gid: str) -> bool:
        """productDelete. IRREVERSIBLE - no undelete API exists.

        Callers must have already: untagged the source and re-read to confirm, passed
        all nine ownership guards, and committed the pre-image to the ledger.

        Returns False when the product was already gone. Shopify signals that as HTTP
        200 with deletedProductId null and a "Product does not exist" userError, NOT a
        404, so a naive handler records a false failure on an idempotent retry.
        """
        try:
            data = await self.client.execute(
                """
                mutation($input: ProductDeleteInput!) {
                  productDelete(input: $input) {
                    deletedProductId
                    userErrors { field message }
                  }
                }
                """,
                {"input": {"id": gid}},
                operation=f"productDelete[{self.store_id}]",
                mutation_name="productDelete",
                is_write=True,
            )
            return bool((data.get("productDelete") or {}).get("deletedProductId"))
        except ShopifySemanticError as exc:
            if any("does not exist" in str(e.get("message", "")).lower()
                   for e in exc.user_errors):
                logger.info("%s: product %s already gone, treating as success",
                            self.store_id, gid)
                return False
            raise
