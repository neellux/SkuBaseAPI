"""The 1inventory submission platform: create and update products on the source Shopify store.

Real time, like SellerCloud. No batching and no poller of its own; the two existing
submission dispatch paths call submit_listing() directly.

Replaces the create path of 1nventory-shopify-appscript/, which round-tripped a SellerCloud
export through Matrixify sheets and a human import, then pushed the Shopify ids back on a
LATER run. Nothing here modifies that AppScript; it simply stops being used.

Plan: docs/plans/2026-08-11-feat-1nventory-realtime-platform-plan.md

FOUR THINGS THIS DELIBERATELY NEVER DOES
----------------------------------------
1. Write an inventory quantity or location, on any path. The AppScript sets qty 0 on
   create because a Matrixify row has a column for it; a GraphQL create has no such
   obligation and a new variant is already at zero. The app holds no write_inventory
   scope, so this is enforced by the credential rather than by discipline.
2. Replace the tag set. Tags go through tagsAdd on update. A productUpdate{tags} would
   strip SHOPTHESAMPLE mid-flight and the consignment pipeline would then delete the
   product from Shop The Sample.
3. Delete a variant. A resubmit whose listing dropped a size must not remove a variant
   holding live stock, so updates are additive only.
4. Create a product with no images. Belt and braces on requires_images, which is an
   operator-editable setting: a product that goes live looking broken is worse than a
   failed submission, and unlike a failure nobody gets told.

SHARED STORE
------------
internal_platform_source_poller tags and untags products on this same store every five
minutes. Everything above is what keeps the two writers from fighting.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Mapping, Sequence

import httpx
from tortoise import connections

from models.db_models import AppSettings
from services.sellercloud_service import GENDER_MAPPING, sellercloud_service
from services.shopify_admin import ShopifyAdmin
from services.shopify_client import (
    ShopifyError,
    ShopifySemanticError,
    get_shopify_client,
)
from services.template_render import render_template, resolve_field_template

logger = logging.getLogger(__name__)

PLATFORM_ID = "1nventory"
STORE = "high-end-merchandise"
GCS_ROOT = "https://storage.googleapis.com/lux_products"

# Present on every product, from the AppScript's DEFAULT_TAGS.
DEFAULT_TAGS: tuple[str, ...] = ("couponcollection", "channelenable-all", "shop375")

# parent_products.company_code for EssxNYC, confirmed against SellerCloud's own /Companies
# list on 2026-08-12 (249 = "EssxNYC"; 250 = "ShopEvergreene (1NVENTORY)", the store this
# platform writes to, which is a different company entirely).
#
# The company code is the ONLY signal used. The "ESSX/" SKU prefix looks like it should
# work and does not: across active parents, 5,883 of company 249 carry the prefix but 2,420
# do not, and 544 prefixed SKUs sit under company 182. Either test alone mislabels
# thousands of products.
ESSX_COMPANY_CODE = 249
ESSX_TAG = "ESSX SKU"

# Wholesale Type -> category tag, transcribed from the AppScript's Tags sheet (col A/B).
# The wholesale vocabulary is closed: exactly these five values across 232 of 233 types,
# verified in production 2026-08-11.
CATEGORY_TAGS: Mapping[str, str] = {
    "Accessories": "main-accessories",
    "Tops": "main-clothing",
    "Bottoms": "main-clothing",
    "Outwear": "main-clothing",
    "Footwear": "main-shoes",
}

# listingoptions_types_parents.gender -> gender tag, from the same sheet (col C/D).
#
# Boys, Girls and "Does Not Apply" are absent ON PURPOSE. The Tags sheet has no row for
# them, so the AppScript emits no gender tag for them today either; 32 of 233 types are
# affected. Adding entries here would be a behaviour change, not a bug fix.
GENDER_TAGS: Mapping[str, tuple[str, ...]] = {
    "Mens": ("gender-mens",),
    "Womens": ("gender-womens",),
    "Unisex": ("gender-mens", "gender-womens"),
}

# Hardcoded rather than settings: a setting implies someone might reasonably change it.
# published_scope is the exception and lives in platform_settings.
PRODUCT_STATUS = "ACTIVE"
# EssxNYC products are created for review rather than for sale. A DRAFT is invisible to
# shoppers whatever its channel associations say, so this is the safe half of the ESSX
# handling; the other half is that an ESSX product already on 1nventory is never touched.
DRAFT_STATUS = "DRAFT"
OPTION_NAME = "Size"
INVENTORY_POLICY = "DENY"

# Used when app_settings.field_templates has no "1nventory" key yet. The description is
# byte-for-byte the SellerCloud template, which is not a coincidence: the AppScript reads
# the SellerCloud LongDescription straight out of the export, so 1nventory has always
# displayed SellerCloud's rendering. The title is SellerCloud's minus the leading
# {brand_name}, because the brand moves to vendor.
DEFAULT_TEMPLATES: Mapping[str, str] = {
    "title": "{brand_color/standard_color} {style_name}",
    "description": (
        "<p><strong>{brand_name}</strong></p><p>{GENDER}{style_name}</p><p><br></p>"
        "<p><strong>Highlights</strong></p><p>{description}</p><p><br></p>"
        "<p><strong>Material Composition:</strong></p><p>{material}</p><p><br></p>"
        "<p><strong>Manufacturer SKU:</strong> {manufacturer_sku}</p>"
        "<p><strong>Seller SKU:</strong> {ID}</p><p><br></p>"
        "<p>Made in {country_of_origin}</p>"
    ),
}


class OneInventorySubmitError(Exception):
    """Operator-facing failure. str() is short enough for a snackbar; .detail is the rest."""

    def __init__(self, message: str, *, stage: str, detail: str | None = None) -> None:
        super().__init__(message)
        self.stage = stage
        self.detail = detail or message

    def display(self) -> str:
        return str(self)


def _material_transform(value: Any) -> str:
    """One div per line, Main: renamed to Shell:. Same transform SellerCloud applies.

    Load-bearing for multi-line materials: without it "Shell: 100% Polyester\\nLining: ..."
    collapses onto one line.
    """
    lines = [ln.strip() for ln in str(value).split("\n") if ln.strip()]
    return "".join(f"<div>{ln.replace('Main:', 'Shell:')}</div>" for ln in lines)


VALUE_TRANSFORMS = {
    "GENDER": lambda v: GENDER_MAPPING.get(v, v),
    "material": _material_transform,
}


def build_handle(brand: str, rendered_title: str, parent_sku: str) -> str:
    """brand + the RENDERED (brand-less) title + parent SKU.

    Verified against the live catalog: PRP-MTPS-0043 has handle
    purple-brand-black-textured-sky-high-tee-prp-mtps-0043, exactly "Purple Brand" +
    "Black Textured Sky High Tee" + "PRP-MTPS-0043". The title fed in must be the rendered
    one, not the form's, or the brand appears twice.

    Sanitised here rather than left to Shopify so the handle we log is the handle that
    gets saved. "Orange/Off White" is why: an unsanitised slash reads as a path segment.

    Cosmetic only. Identity resolves by variant SKU, never by handle.
    """
    raw = f"{brand}-{rendered_title}:{parent_sku}".lower().replace(" ", "-")
    cleaned = "".join(c if c.isalnum() or c == "-" else "-" for c in raw)
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned.strip("-")


class OneInventoryService:

    # -- reference data ----------------------------------------------------

    async def _load_templates(self) -> dict[str, str]:
        settings = await AppSettings.first()
        stored = settings.field_templates if settings else None
        return {
            key: resolve_field_template(stored, PLATFORM_ID, key) or default
            for key, default in DEFAULT_TEMPLATES.items()
        }

    async def _load_taxonomy(self, product_type: str | None) -> dict[str, Any]:
        """gender, wholesale category, Shopify category GID and item weight, in one query.

        Deliberately NOT internal_platform_type_map.load_taxonomy: that belongs to the
        consignment pipeline, loads the whole 233-row taxonomy for a whole-catalog
        classification pass, and routes gender through Shop The Sample's vocabulary.

        The product type is read against TWO platform lists here, for two different jobs:
        'wholesale' gives the main-* tag and never gates, '1nventory' gives the Shopify
        structured category GID and does gate (require_type_mapping).
        """
        if not product_type:
            return {}
        conn = connections.get("default")
        rows = await conn.execute_query_dict(
            """
            SELECT p.gender,
                   t.item_weight_oz,
                   wholesale.platform_value           AS wholesale_value,
                   onenv.platform_meta ->> 'category_id' AS taxonomy_gid
              FROM listingoptions_types t
              LEFT JOIN listingoptions_types_parents p ON p.id = t.parent_id
              LEFT JOIN listingoptions_types_default_list wholesale
                     ON wholesale.primary_id = t.id
                    AND wholesale.platform_id = 'wholesale'
                    AND wholesale.primary_table_column = 'type'
              LEFT JOIN listingoptions_types_default_list onenv
                     ON onenv.primary_id = t.id
                    AND onenv.platform_id = $2
                    AND onenv.primary_table_column = 'type'
             WHERE LOWER(t.type) = LOWER($1)
             LIMIT 1
            """,
            [product_type, PLATFORM_ID],
        )
        if not rows:
            return {}
        row = rows[0]
        weight = row.get("item_weight_oz")
        return {
            "gender": row.get("gender"),
            "wholesale": row.get("wholesale_value"),
            "taxonomy_gid": row.get("taxonomy_gid"),
            "item_weight_oz": float(weight) if weight is not None else None,
        }

    async def _is_essx(self, parent_sku: str) -> bool:
        """True when the parent belongs to EssxNYC, read from parent_products.company_code.

        The products database, not SkuBase's own. Never inferred from the SKU string.
        """
        conn = connections.get("product_db")
        rows = await conn.execute_query_dict(
            "SELECT company_code FROM parent_products WHERE sku = $1 LIMIT 1",
            [parent_sku],
        )
        return bool(rows) and rows[0]["company_code"] == ESSX_COMPANY_CODE

    async def _load_size_order(self, sizing_scheme: str | None) -> dict[str, int]:
        """size -> position, so the variant rail is not alphabetical.

        Without this the storefront shows L, M, S, XL, XS, XXL. Note "order" is a reserved
        word and has to be quoted.
        """
        if not sizing_scheme:
            return {}
        conn = connections.get("default")
        rows = await conn.execute_query_dict(
            'SELECT size, "order" FROM listingoptions_sizing_schemes '
            "WHERE sizing_scheme = $1",
            [sizing_scheme],
        )
        return {r["size"]: r["order"] for r in rows}

    async def _gallery_images(self, parent_sku: str, limit: int = 8) -> list[str]:
        """Gallery images only. Washtags are excluded deliberately.

        sellercloud_service.get_product_images appends washtag URLs, which is right for
        SellerCloud and wrong for a storefront carousel, so the URLs are rebuilt here
        rather than filtered afterwards. _fullsize is tried first because that is the
        variant the existing 1nventory catalog uses.
        """
        # All slots probed CONCURRENTLY. Serially this was up to 16 round trips of pure
        # latency per product (8 slots x 2 suffixes) and the largest single cost in the
        # bulk path after the SellerCloud reads.
        #
        # The trade is that a serial loop could stop at the first gap, while this probes
        # every slot. That is the safer behaviour anyway: it finds image 5 when image 4 is
        # missing, where the serial version stopped and shipped a product with three
        # photos.
        async def probe(index: int) -> str | None:
            async with httpx.AsyncClient(timeout=15) as client:
                for suffix in ("fullsize", "1500"):
                    url = f"{GCS_ROOT}/{parent_sku}/{index}_{suffix}.jpg"
                    try:
                        if (await client.head(url)).status_code == 200:
                            return url
                    except httpx.HTTPError:
                        continue
            return None

        results = await asyncio.gather(*(probe(i) for i in range(1, limit + 1)))
        return [url for url in results if url]

    # -- assembly ----------------------------------------------------------

    def build_tags(
        self, gender: str | None, wholesale: str | None, is_essx: bool = False
    ) -> list[str]:
        """Three constants plus the derived pair, and ESSX SKU for EssxNYC parents.

        An unresolved tag is OMITTED.

        Never raises. This inverts the STS behaviour, where an underivable gender skips
        the product entirely on the grounds that a half-tagged product lands in the wrong
        collections. For a listing an operator explicitly submitted, refusing to create it
        over a missing tag is the wrong trade, and it matches the AppScript, which guards
        both lookups with a length check and carries on.
        """
        tags = list(DEFAULT_TAGS)
        tags.extend(GENDER_TAGS.get(gender or "", ()))
        category = CATEGORY_TAGS.get(wholesale or "")
        if category:
            tags.append(category)
        if is_essx:
            tags.append(ESSX_TAG)
        return tags

    def build_variants(
        self,
        child_size_overrides: Mapping[str, str],
        prices: Mapping[str, Mapping[str, Any]],
        size_order: Mapping[str, int],
        list_price: Any,
        weight_oz: float | None,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """(variants, size_values) in sizing-scheme order.

        Both AppScript price guards carry over. A zero SitePrice raises rather than
        creating a product priced at 0 - the AppScript throws "Price is zero!" and skips,
        so this state occurs in practice.
        """
        ordered = sorted(
            child_size_overrides.items(),
            key=lambda kv: (size_order.get(kv[1], 10**6), kv[1]),
        )

        variants: list[dict[str, Any]] = []
        size_values: list[str] = []
        for child_sku, size in ordered:
            site_price = (prices.get(child_sku) or {}).get("SitePrice")
            site_cost = (prices.get(child_sku) or {}).get("SiteCost")

            if site_price in (None, 0, 0.0):
                raise OneInventorySubmitError(
                    f"No price for {child_sku}",
                    stage="pricing",
                    detail=(f"SellerCloud SitePrice for {child_sku} is {site_price!r}; "
                            "refusing to create a product priced at 0"),
                )

            # compare-at from the FORM, not from the SellerCloud read. Submit rewrites
            # ListPrice on the children, so a parallel read could return the pre-submit
            # value. Falls back to the price itself when there is no higher was-price,
            # so no product shows a discount against zero.
            compare_at = list_price if (list_price or 0) > 0 else site_price

            inventory_item: dict[str, Any] = {
                "sku": child_sku,
                "cost": str(site_cost or 0),
                "requiresShipping": True,
                "tracked": True,
            }
            # OUNCES, not pounds. SellerCloud splits weight into PackageWeightLbs and
            # PackageWeightOz; a 15 oz garment is Lbs=0, Oz=15, so anything copying the
            # pounds field alone rounds it to zero. That is why 61% of the live catalog
            # has no weight.
            if weight_oz:
                inventory_item["measurement"] = {
                    "weight": {"value": float(weight_oz), "unit": "OUNCES"}
                }

            variants.append({
                "optionValues": [{"optionName": OPTION_NAME, "name": size}],
                "inventoryItem": inventory_item,
                "price": str(site_price),
                "compareAtPrice": str(compare_at),
                "barcode": child_sku,
                "taxable": True,
                "inventoryPolicy": INVENTORY_POLICY,
            })
            size_values.append(size)

        return variants, size_values

    # -- identity ----------------------------------------------------------

    @staticmethod
    def _is_missing_product(exc: ShopifySemanticError) -> bool:
        """Shopify reports a write against a DELETED product as a userError, not a 404.

        HTTP 200, `userErrors: [{field: ["id"], message: "Product does not exist"}]`, which
        shopify_client turns into ShopifySemanticError. Matching on exc.user_errors rather
        than str(exc) because multiple userErrors are joined with "; " and the structured
        list is the only reliable discriminator.

        THE FIELD PATH IS THE DISCRIMINATOR, NOT THE MESSAGE. An earlier version of this
        matched "does not exist" anywhere in the message and cost a production submission:
        _update's update_variants call failed with

            ['variants', '0', 'id']: Product variant does not exist

        on DNT-MTPS-0106, which is a stale VARIANT id on a product that exists perfectly
        well. The loose predicate read that as a missing product, fell through to _create,
        and Shopify rejected the create with "Handle ... already in use" - correctly, since
        the product was there the whole time.

        Only field == ["id"] is the product itself. shopify_admin.delete_product can afford
        the loose check because productDelete has exactly one possible subject; _update
        makes four calls and any of them can raise.
        """
        return any(
            tuple(e.get("field") or ()) == ("id",)
            and "does not exist" in str(e.get("message", "")).lower()
            for e in exc.user_errors
        )

    async def _mirror_lookup(self, parent_sku: str) -> str | None:
        """internal_platform_state.source_product_gid, a free hit when it is there.

        Never trusted on its own. The mirror only covers parents that resolve in
        parent_products (unregistered and reassigned SKUs are dropped by design) and it is
        written only when the consignment source poller completes a full scan, which it
        does not do on TEST. A false "not there" would create a duplicate, so the caller
        always confirms against Shopify before creating.
        """
        conn = connections.get("default")
        rows = await conn.execute_query_dict(
            "SELECT source_product_gid FROM internal_platform_state "
            "WHERE parent_sku = $1 AND source_product_gid IS NOT NULL LIMIT 1",
            [parent_sku],
        )
        return rows[0]["source_product_gid"] if rows else None

    # -- entry point -------------------------------------------------------

    async def submit_listing(
        self,
        *,
        listing_id: str,
        product_id: str,
        form_data: Mapping[str, Any],
        set_category: bool = True,
    ) -> dict[str, Any]:
        """Create or additively update the product, publish it, and write ids back.

        set_category=False omits Shopify's structured `category` entirely, for callers that
        are deliberately deferring it. Used by the bulk backfill, which runs while almost no
        product type has a 1nventory mapping yet: setting the field for the handful that do
        and leaving it blank for the rest would make the catalog inconsistent in a way that
        is harder to find later than simply not setting it at all. The submit path from the
        UI always passes True, and `require_type_mapping` guarantees a value is there.

        Returns {product_gid, variant_gids, created, published, writeback}. Raises
        OneInventorySubmitError for anything an operator should read.
        """
        data = dict(form_data or {})
        brand = (data.get("brand_name") or "").strip()
        product_type = data.get("product_type")
        child_size_overrides: dict[str, str] = data.get("child_size_overrides") or {}

        if not child_size_overrides:
            raise OneInventorySubmitError(
                "Sizes need to be mapped before submission", stage="validate"
            )

        stage = "reference_data"
        templates = await self._load_templates()
        taxonomy = await self._load_taxonomy(product_type)
        size_order = await self._load_size_order(data.get("SIZING_SCHEME"))

        stage = "render"
        title, _ = render_template(
            templates["title"], data,
            value_transforms=VALUE_TRANSFORMS, collapse_whitespace=True,
        )
        description, _ = render_template(
            templates["description"], data, value_transforms=VALUE_TRANSFORMS
        )
        if not title:
            raise OneInventorySubmitError(
                "Cannot build a title for this listing", stage=stage,
                detail=f"title template {templates['title']!r} rendered empty",
            )

        stage = "images"
        images = await self._gallery_images(product_id)
        if not images:
            # requires_images normally parks the submission long before here, but that is
            # an operator-editable setting. A live product with no images is worse than a
            # failed submission.
            raise OneInventorySubmitError(
                "No images found for this product", stage=stage,
                detail=f"no gallery images on GCS for {product_id}",
            )

        stage = "pricing"
        child_skus = sorted(child_size_overrides)
        prices = await sellercloud_service.get_children_pricing(child_skus)

        weight_oz = data.get("shipping_weight") or taxonomy.get("item_weight_oz")
        variants, size_values = self.build_variants(
            child_size_overrides, prices, size_order,
            data.get("list_price"), weight_oz,
        )

        is_essx = await self._is_essx(product_id)
        tags = self.build_tags(
            taxonomy.get("gender"), taxonomy.get("wholesale"), is_essx=is_essx
        )

        stage = "identity"
        client = await get_shopify_client(STORE)
        admin = ShopifyAdmin(client)

        # Shopify's SKU search index is eventually consistent in BOTH directions: it can
        # still return a product that was deleted minutes ago, and it can miss one that was
        # created moments ago. The mirror covers the second case; a live fetch covers the
        # first. Neither is trusted alone.
        existing = await admin.find_product_by_variant_sku(child_skus)
        if existing is None:
            mirror_gid = await self._mirror_lookup(product_id)
            if mirror_gid:
                # The search missed. Before letting a stored gid force the update branch,
                # confirm it still resolves - internal_platform_state keeps a gid forever
                # and nothing prunes it when the product is deleted, so an unverified seed
                # is how a dead product keeps failing every resubmit.
                confirmed = await admin.get_product(mirror_gid)
                if confirmed:
                    existing = {
                        "id": confirmed.gid,
                        "handle": confirmed.handle,
                        "variants": {
                            "nodes": [
                                {"id": v.gid, "sku": v.sku}
                                for v in confirmed.variants if v.sku
                            ]
                        },
                    }
                else:
                    logger.warning(
                        "1inventory: %s has a stale mirror gid (%s); the product no "
                        "longer exists on Shopify, treating as a create",
                        product_id, mirror_gid,
                    )

        product_fields = {
            "title": title,
            "descriptionHtml": description,
            "vendor": brand,
            # Raw Lux type. The AppScript's "DO NOT USE! " strip has no equivalent: zero of
            # the 233 listing-options types carry that prefix.
            "productType": product_type or "",
            # Shopify's STRUCTURED category, a separate field from productType above.
            # None is filtered out by both _create and update_product_details, so an
            # omitted category leaves the field untouched rather than clearing it.
            "category": taxonomy.get("taxonomy_gid") if set_category else None,
        }

        # EssxNYC products are handled differently on BOTH branches. An existing one is
        # left exactly as it is, and a new one is created as a draft for a human to review
        # before it can reach a shopper.
        if existing and is_essx:
            stage = "skip"
            result = {
                "product_gid": existing["id"],
                "handle": existing.get("handle"),
                "variant_gids": {
                    n["sku"]: n["id"]
                    for n in (existing.get("variants") or {}).get("nodes") or []
                    if n.get("sku")
                },
                "variant_prices": {},
                "created": False,
                "skipped": "essx_exists",
            }
            logger.info(
                "1inventory: %s is ESSX and already on 1nventory (%s); leaving it "
                "untouched", product_id, existing["id"],
            )
        elif existing:
            stage = "update"
            try:
                result = await self._update(
                    admin, existing, product_fields, tags, variants
                )
            except ShopifySemanticError as exc:
                if not self._is_missing_product(exc):
                    raise
                # The identity check was reading a tombstone. The operator asked for this
                # product to be listed and it genuinely is not there, so create it rather
                # than failing - a resubmit would only hit the same stale index again.
                logger.warning(
                    "1inventory: %s resolved to %s but Shopify says it does not exist; "
                    "falling back to create", product_id, existing.get("id"),
                )
                stage = "create"
                result = await self._create(
                    admin, product_fields, tags, variants, size_values, images,
                    handle=build_handle(brand, title, product_id),
                    status=DRAFT_STATUS if is_essx else PRODUCT_STATUS,
                )
        else:
            stage = "create"
            result = await self._create(
                admin, product_fields, tags, variants, size_values, images,
                handle=build_handle(brand, title, product_id),
                status=DRAFT_STATUS if is_essx else PRODUCT_STATUS,
            )

        # Publishing an ESSX draft is deliberate, and matches what the Shopify admin does
        # when a human creates a product: the channel association exists but a DRAFT is
        # invisible to shoppers, so flipping it to ACTIVE later is one click rather than
        # two. Skipped entirely on the leave-alone branch, where nothing may change.
        if result.get("skipped"):
            result["published"] = None
        else:
            stage = "publish"
            on, available = await admin.publish_to_online_store(result["product_gid"])
            result["published"] = {"on": on, "available": available}

        stage = "writeback"
        result["writeback"] = await self._write_back(result)

        # Read the counts back off `result`, NOT from locals. The leave-alone branch never
        # runs the publish, so `on`/`available` are unbound there and referencing them
        # raised UnboundLocalError AFTER the product and the write-back had both already
        # succeeded - a submission recorded as failed for work that was actually done.
        published = result.get("published") or {}
        logger.info(
            "1nventory: listing %s -> %s (created=%s, %s%d variants, published %s/%s)",
            listing_id, result["product_gid"], result["created"],
            f"{result['skipped']}, " if result.get("skipped") else "",
            len(result["variant_gids"]),
            published.get("on", "-"), published.get("available", "-"),
        )
        return result

    async def _create(
        self,
        admin: ShopifyAdmin,
        product_fields: Mapping[str, Any],
        tags: Sequence[str],
        variants: Sequence[Mapping[str, Any]],
        size_values: Sequence[str],
        images: Sequence[str],
        *,
        handle: str,
        status: str = PRODUCT_STATUS,
    ) -> dict[str, Any]:
        product_input = {
            **{k: v for k, v in product_fields.items() if v},
            "handle": handle,
            "status": status,
            # Safe on CREATE only: there is nothing to destroy on a product that did not
            # exist. productUpdate{tags} is replace-mode and is refused by shopify_admin.
            "tags": list(tags),
            "productOptions": [
                {"name": OPTION_NAME, "values": [{"name": s} for s in size_values]}
            ],
        }
        media = [{"originalSource": u, "mediaContentType": "IMAGE"} for u in images]

        created = await admin.create_product(product_input, media)
        # REMOVE, not preserve: productCreate above was given productOptions, so Shopify
        # has already auto-generated a variant for the first size. It is a *custom*
        # standalone (it carries a real option value), and unless it is removed the real
        # variant for that size collides with it.
        made = await admin.create_variants(
            created["id"], variants, strategy="REMOVE_STANDALONE_VARIANT"
        )
        return {
            "product_gid": created["id"],
            "handle": created.get("handle"),
            "variant_gids": {v["sku"]: v["id"] for v in made if v.get("sku")},
            "variant_prices": {v["sku"]: v.get("price") for v in made if v.get("sku")},
            "created": True,
            "status": status,
        }

    async def _update(
        self,
        admin: ShopifyAdmin,
        existing: Mapping[str, Any],
        product_fields: Mapping[str, Any],
        tags: Sequence[str],
        variants: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        """Additive. Adds missing variants, refreshes descriptive fields, removes nothing."""
        gid = existing["id"]

        await admin.update_product_details(gid, product_fields)
        await admin.add_tags(gid, tags)

        by_sku = {v["inventoryItem"]["sku"]: v for v in variants}

        # VARIANT IDS COME FROM A DIRECT FETCH, NEVER FROM `existing`.
        #
        # `existing` comes from find_product_by_variant_sku, which reads Shopify's search
        # index, so its ids are only as fresh as that index. The fetch below costs one read
        # and removes the doubt.
        live = await admin.get_product(gid)
        present = {v.sku: v.gid for v in (live.variants if live else ()) if v.sku}

        missing = [v for sku, v in by_sku.items() if sku not in present]
        # PRESERVE on the update path: a product down to a single variant holds a real
        # size with real stock, and REMOVE would delete it.
        made = (
            await admin.create_variants(
                gid, missing, strategy="PRESERVE_STANDALONE_VARIANT"
            )
            if missing else []
        )

        if made:
            # THIS RE-READ IS THE ACTUAL FIX, and it is not defensive - it is required.
            #
            # create_variants runs productVariantsBulkCreate with REMOVE_STANDALONE_VARIANT,
            # which DELETES the product's existing variant and rebuilds the set. Its
            # docstring claims "It never removes a variant"; that is false, and it cost
            # three production submissions. Proof from Shopify's own timestamps: the
            # submission for DNT-MJNS-0035 started at 13:38:55 on 2026-08-28 and its L
            # variant reports createdAt 13:38:58 - our own call, three seconds in. Same
            # shape on DNT-MTPS-0106, whose four variants all carry createdAt 15:15:22
            # against a submission that started at 15:15:19.
            #
            # So the ids captured before the create are dead by the time update_variants
            # runs, and writing them fails as
            #     ['variants', '0', 'id']: Product variant does not exist
            # on a product that is perfectly healthy. Re-read, never assume they survived.
            live = await admin.get_product(gid)
            present = {v.sku: v.gid for v in (live.variants if live else ()) if v.sku}

        # Existing variants get their inventoryItem refreshed (cost, weight) but NOT their
        # price: an additive update must not fight the consignment pipeline for price
        # control, and it must never touch quantity.
        refresh = [
            {"id": vid, "inventoryItem": by_sku[sku]["inventoryItem"]}
            for sku, vid in present.items() if sku in by_sku
        ]
        await admin.update_variants(gid, refresh)

        variant_gids = {**present, **{v["sku"]: v["id"] for v in made if v.get("sku")}}
        return {
            "product_gid": gid,
            "handle": existing.get("handle"),
            "variant_gids": variant_gids,
            "variant_prices": {
                sku: (by_sku[sku]["price"] if sku in by_sku else None)
                for sku in variant_gids
            },
            "created": False,
        }

    async def _write_back(self, result: Mapping[str, Any]) -> dict[str, Any]:
        """Push the Shopify ids onto the SellerCloud children.

        Never raises. The product already exists on Shopify at this point, so a failure
        here is a partial success, not a reason to fail the submission and invite a
        resubmit that would have to reconcile. The ids are on the submission row either
        way, so a resubmit resumes rather than duplicating.
        """
        product_numeric = str(result["product_gid"]).rsplit("/", 1)[-1]
        report: dict[str, Any] = {"ok": [], "failed": []}

        for sku, variant_gid in (result.get("variant_gids") or {}).items():
            variant_numeric = str(variant_gid).rsplit("/", 1)[-1]
            price = (result.get("variant_prices") or {}).get(sku)
            try:
                outcome = await sellercloud_service.set_website_ids(
                    sku, product_numeric, variant_numeric, price
                )
                (report["ok"] if outcome["ok"] else report["failed"]).append(
                    {"sku": sku, **outcome}
                )
            except Exception as exc:                                    # noqa: BLE001
                logger.warning("1nventory: write-back failed for %s: %s", sku, exc)
                report["failed"].append({"sku": sku, "error": str(exc)[:300]})

        if report["failed"]:
            logger.warning(
                "1nventory: %d/%d children did not take the website ids",
                len(report["failed"]),
                len(report["failed"]) + len(report["ok"]),
            )
        return report


    # -- the single entry point both dispatch paths use --------------------

    async def run_submission(
        self, submission: Any, listing: Any, *, set_category: bool = True
    ) -> None:
        """Submit, then move the submission row. Never raises.

        THE reason this exists rather than living in the callers: there are two dispatch
        paths into this platform and they must produce identical rows.

          - routes/listing_routes.py::_run_submissions_background, inline, when the
            listing's images are already uploaded
          - services/submission_poller.py::_submit_to_platform, when the row was parked
            QUEUED because requires_images held it back until photo_upload_poller flipped
            upload_status to 'uploaded'

        The second is the NORMAL path for a freshly photographed listing, not an edge
        case, so any behaviour implemented in only one of them is broken for most submits.

        external_id is written BEFORE the write-back is judged, so a partial success still
        records the Shopify ids and a resubmit resumes instead of duplicating.
        """
        from utils.submission_steps import record_step

        try:
            result = await self.submit_listing(
                listing_id=str(listing.id),
                product_id=listing.product_id,
                form_data=listing.data or {},
                set_category=set_category,
            )
        except OneInventorySubmitError as exc:
            logger.error("1inventory submission failed at %s: %s", exc.stage, exc.detail)
            submission.status = "failed"
            submission.error = exc.detail
            submission.error_display = exc.display()
            await submission.save(
                update_fields=["status", "error", "error_display", "updated_at"]
            )
            await record_step(submission.id, "failed", stage=exc.stage,
                              reason=str(exc)[:300])
            return
        except ShopifyError as exc:
            # Shopify rejected the write for a reason it named. Surfacing that instead of
            # the blanket "Failed to submit" is the difference between an operator seeing
            # "Product does not exist" and seeing nothing actionable at all - the full
            # traceback still goes to submission.error either way.
            import traceback
            logger.error("1inventory submission failed on Shopify: %s", exc, exc_info=True)
            submission.status = "failed"
            submission.error = traceback.format_exc()
            submission.error_display = f"Shopify rejected this: {str(exc)[:180]}"
            await submission.save(
                update_fields=["status", "error", "error_display", "updated_at"]
            )
            await record_step(submission.id, "failed", stage="shopify",
                              reason=str(exc)[:300])
            return
        except Exception as exc:                                        # noqa: BLE001
            import traceback
            logger.error("1inventory submission failed: %s", exc, exc_info=True)
            submission.status = "failed"
            submission.error = traceback.format_exc()
            submission.error_display = "Failed to submit"
            await submission.save(
                update_fields=["status", "error", "error_display", "updated_at"]
            )
            await record_step(submission.id, "failed", stage="submit",
                              reason=str(exc)[:300])
            return

        submission.status = "success"
        submission.external_id = {
            "product_gid": result["product_gid"],
            "handle": result.get("handle"),
            "variant_gids": result.get("variant_gids") or {},
        }
        await submission.save(
            update_fields=["status", "external_id", "updated_at"]
        )

        writeback = result.get("writeback") or {}
        await record_step(
            submission.id, "listed",
            meta={
                "created": result.get("created"),
                "status": result.get("status"),
                "skipped": result.get("skipped"),
                "published": result.get("published"),
                "writeback_ok": len(writeback.get("ok") or []),
                "writeback_failed": len(writeback.get("failed") or []),
            },
        )


oneinventory_service = OneInventoryService()
