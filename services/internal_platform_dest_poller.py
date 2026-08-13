"""Destination-side reconciler for internal platforms (hourly).

Scans the destination store for products Syncio has delivered, brings them to the
desired state, cleans up phantom inventory, and repricing is a pass of the same loop
rather than a separate service.

Split from the source poller deliberately. The source store is the live operational
catalog; the destination is a consignment mirror. Different blast radii deserve
different gates and caps, and BasePoller carries exactly one interval so "split cadence
in one poller" has no clean expression anyway.

Desired-state reconciliation, NOT ledger-lookup skipping. Skipping a product because a
successful normalize row exists can never self-heal: Syncio overwrites our writes (that
is the entire reason a repricer was ever needed), and nothing suggests it spares vendor
and tags. A product Syncio reverts would silently fall out of its collections forever.
So every cycle reads current state and writes only the diff.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Mapping

from config import config
from models.db_models import (
    InternalPlatformAction as Act,
    InternalPlatformSkipReason as Skip,
    InternalPlatformStatus as St,
)
from services import internal_platform_ledger as ledger
from services.base_poller import BasePoller
from services.internal_platform_products import (
    load_reassigned,
    resolve_registered_parents,
)
from services.internal_platform_type_map import (
    TypeTaxonomy,
    check_taxonomy_health,
    load_taxonomy,
)
from services.internal_platform_rules import (
    PriceOutcome,
    PricingResult,
    DEFAULT_PRICING,
    SafetyCaps,
    assess_ownership,
    compute_price,
    desired_tags,
)
from services.shopify_admin import (
    Product,
    ShopifyAdmin,
    batches_by_cost,
    normalize_cost,
    reprice_cost,
)
from services.shopify_client import (
    ShopifyError,
    ShopifyScopeError,
    ShopifyTransientError,
    enable_writes,
    get_shopify_client,
    writes_enabled,
)

logger = logging.getLogger(__name__)

# Shopify's updated_at search index is eventually consistent, and our own writes bump
# updated_at. Overlap generously and let desired-state diffing absorb the duplicates.
WATERMARK_OVERLAP_MINUTES = 15


@dataclass(slots=True)
class PlannedWrite:
    parent_sku: str
    dest_gid: str
    source_gid: str | None
    kind: str                      # vendor | title | tags | price
    detail: str
    variant_count: int = 0
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CycleReport:
    scanned: int = 0
    ours: int = 0
    not_ours: int = 0
    unchanged: int = 0
    planned: list[PlannedWrite] = field(default_factory=list)
    skipped: list[tuple[str, Skip, str]] = field(default_factory=list)
    executed: int = 0
    failed: int = 0
    aborted: str | None = None
    # Delivered products re-read as already matching everything we would write. Their
    # normalization is finished, which is what promotes them out of pending_normalization.
    converged: list[str] = field(default_factory=list)
    normalized: int = 0

    @property
    def variant_writes(self) -> int:
        return sum(p.variant_count for p in self.planned)

    def summary(self) -> str:
        return (
            f"scanned={self.scanned} ours={self.ours} not_ours={self.not_ours} "
            f"unchanged={self.unchanged} planned={len(self.planned)} "
            f"skipped={len(self.skipped)} executed={self.executed} failed={self.failed} "
            f"normalized={self.normalized}"
            + (f" ABORTED={self.aborted}" if self.aborted else "")
        )


def desired_state_hash(vendor: str | None, tags: tuple[str, ...],
                       price: Decimal | None, compare_at: Decimal | None) -> str:
    """Stable fingerprint of what we intend a product to look like."""
    raw = f"{vendor}|{','.join(sorted(t.upper() for t in tags))}|{price}|{compare_at}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


class InternalPlatformDestPoller(BasePoller):

    def __init__(self) -> None:
        super().__init__("internal_platform_dest_poller", name="InternalPlatformDestPoller")
        cfg = config.get("internal_platform_dest_poller", {})

        self.platform_id: str = cfg.get("platform_id", "shopthesample")
        # Both default FALSE. base_poller's cfg.get("enabled", True) fails OPEN, which
        # is the wrong direction when config prod.toml has no section yet and the TEST
        # config's credentials point at the live stores.
        self.enabled = bool(cfg.get("enabled", False))
        self.execute: bool = bool(cfg.get("execute", False))
        self.page_size: int = int(cfg.get("page_size", 250))
        self.full_scan: bool = bool(cfg.get("full_scan", False))

        self.caps = SafetyCaps(
            max_actions_per_cycle=int(cfg.get("max_actions_per_cycle", 250)),
            max_variant_writes_per_cycle=int(cfg.get("max_variant_writes_per_cycle", 600)),
            max_deletes_per_cycle=int(cfg.get("max_deletes_per_cycle", 0)),
            max_sold_out_deletes_per_cycle=int(cfg.get("max_sold_out_deletes_per_cycle", 0)),
            max_pct_of_footprint_changed=float(cfg.get("max_pct_of_footprint_changed", 10.0)),
            max_units_zeroed_per_cycle=int(cfg.get("max_units_zeroed_per_cycle", 400)),
        )
        self._last_watermark: str | None = None
        self._scope_warned = False

    async def run_once(self) -> CycleReport:
        """Manual trigger. Lets an hourly poller be exercised without waiting."""
        return await self._reconcile()

    async def _poll_cycle(self) -> None:
        await self._reconcile()

    # -- main --------------------------------------------------------------

    async def _reconcile(self) -> CycleReport:
        report = CycleReport()

        platform = await ledger.get_platform(self.platform_id)
        if platform is None:
            logger.warning("%s: platform %s not found", self.name, self.platform_id)
            report.aborted = "platform-missing"
            return report
        if not platform.enabled:
            logger.info("%s: platform %s disabled, skipping", self.name, self.platform_id)
            report.aborted = "platform-disabled"
            return report

        released = await ledger.recover_stale_inflight(self.platform_id)
        if released:
            logger.info("%s: released %d stale claims", self.name, released)
        orphaned = await ledger.close_orphaned_audit_rows(self.platform_id)
        if orphaned:
            logger.warning("%s: closed %d audit row(s) orphaned by an interrupted batch",
                           self.name, orphaned)

        state_map = await ledger.load_state_map(self.platform_id)
        # Category tags come from the listing-options mappings, not a hardcoded table,
        # so the destination's vocabulary can be edited without a deploy.
        taxonomy = await load_taxonomy(self.platform_id)

        # A thin taxonomy is less destructive here than on the source - _plan_product
        # skips a product whose category or gender does not resolve, so the failure mode
        # is "normalizes nothing" rather than "delists everything". It still aborts,
        # because a cycle that silently corrects nothing while reporting success is how
        # a broken mapping table stays unnoticed for a day.
        thin = check_taxonomy_health(taxonomy)
        if thin:
            logger.error("%s: %s", self.name, thin)
            report.aborted = "taxonomy-too-thin"
            return report

        client = await get_shopify_client(platform.dest_store)
        admin = ShopifyAdmin(client)

        watermark = None if self.full_scan else self._watermark()
        logger.info(
            "%s: cycle start execute=%s watermark=%s footprint=%d",
            self.name, self.execute, watermark or "FULL", len(state_map),
        )

        # ---- plan (no writes) --------------------------------------------
        #
        # Buffered rather than planned inside the loop, because ownership now needs a
        # products-DB lookup and one query for the whole page set beats one per product.
        # Same shape the source poller uses.
        try:
            scanned_products = []
            async for product in admin.products_by_tag(
                platform.trigger_tag, updated_after=watermark, page_size=self.page_size
            ):
                report.scanned += 1
                scanned_products.append(product)
        except ShopifyScopeError as exc:
            # Latch: without this the first enabled pass emits one identical failure
            # per product instead of a single actionable line.
            if not self._scope_warned:
                logger.error("%s: missing scope, aborting cycle: %s", self.name, exc)
                self._scope_warned = True
            report.aborted = "scope"
            return report
        except ShopifyTransientError as exc:
            logger.warning("%s: transient failure mid-scan, aborting cycle: %s", self.name, exc)
            report.aborted = "transient"
            return report

        # Ownership is catalog membership, resolved from the products DB exactly as the
        # source poller resolves it. Loaded once for the whole scan.
        all_skus = {s for p in scanned_products for s in p.variant_skus if s}
        registered = await resolve_registered_parents(all_skus)

        # Fail-closed, and for a sharper reason than the source poller's: an unreadable
        # matview read as "nothing is reassigned" would let a merged SKU resolve to its
        # NEW parent and relink a live destination product onto a different garment.
        try:
            reassigned = await load_reassigned(all_skus)
        except Exception as exc:                                     # noqa: BLE001
            logger.error("%s: could not read secondary_skus, aborting before any write: %s",
                         self.name, exc)
            report.aborted = "reassigned-read-failed"
            return report

        # Resolving nothing from a non-empty scan means the catalog lookup is broken, not
        # that we suddenly own nothing. Every product would fall out as "not ours" and the
        # cycle would correct nothing while reporting success - the same silent-no-op shape
        # check_taxonomy_health exists to catch, so it aborts the same way.
        if scanned_products and not registered:
            logger.error("%s: %d products scanned but ZERO SKUs resolved to a registered "
                         "parent; aborting rather than treating the catalog as empty",
                         self.name, len(scanned_products))
            report.aborted = "no-registered-parents"
            return report

        # Destination storefront addressing, refreshed for every owned product in ONE
        # write, mirroring refresh_product_facts on the source side.
        #
        # Deliberately not threaded through the four places that set dest_product_gid
        # (ledger.release, ledger.claim_dest_gid, the normalize path here). Those fire on
        # different transitions, so a handle written by only some of them would be
        # populated or stale depending on which path last touched the row. This runs every
        # cycle over the products the scan actually returned, which is the only place that
        # sees the current handle for all of them.
        #
        # Outside the execute gate: it corrects our own state, it writes nothing to
        # Shopify, and the same reasoning already applies to refresh_product_facts.
        # Gated on assess_ownership, the same four-way check _plan_product uses, not on
        # bare parent resolution. A destination product that merely resolves to a
        # registered parent is not necessarily ours; recording its handle against that
        # parent would point the product page at Shop The Sample's own listing.
        dest_facts: dict[str, tuple[str | None, bool]] = {}
        for product in scanned_products:
            own = assess_ownership(
                product.variant_skus, product.tags,
                platform.trigger_tag, product.syncio_source_gid,
                registered, reassigned,
            )
            if own.ours and own.parent_sku:
                dest_facts[own.parent_sku] = (
                    product.handle, product.online_store_url is not None
                )
        if dest_facts:
            n = await ledger.refresh_dest_facts(self.platform_id, dest_facts)
            logger.info("%s: refreshed destination handles on %d row(s)", self.name, n)

        for product in scanned_products:
            self._plan_product(platform, product, state_map, taxonomy,
                               registered, reassigned, report)

        # An incomplete scan is indistinguishable from "everything changed", so caps are
        # evaluated on the FULL planned set before the first write.
        footprint = await ledger.footprint(self.platform_id)
        breach = self._check_caps(report, footprint)
        if breach:
            logger.error("%s: CAP BREACH, zero writes this cycle: %s", self.name, breach)
            report.aborted = breach
            return report

        # ---- execute ------------------------------------------------------
        if not self.execute:
            logger.info("%s: dry-run, %s", self.name, report.summary())
            self._advance_watermark()
            return report

        if not writes_enabled():
            enable_writes(f"{self.name} execute=true")

        # Price is 79% of a typical backlog and is the only kind that batches cleanly:
        # Two independent passes over disjoint sets, not one interleaved loop: they issue
        # different mutations, are costed by different formulas, and are recorded as
        # different ledger actions. Keeping `reprice` and `normalize` distinct in the
        # audit trail is what made the compounding-markup incident traceable, and is
        # worth more than the round trips merging them into one document would save -
        # which is nothing in wall-clock terms, since the shared point budget is what
        # paces us either way.
        #
        # Both are batched, and both size their batches from the work rather than a
        # constant: ~91 products per reprice document at the live variant spread, 31 to
        # 95 per normalize document depending on how many corrections each product needs.
        price_writes = [p for p in report.planned if p.kind == "price"]
        norm_writes = [p for p in report.planned
                       if p.kind in ("vendor", "title", "tags")]

        if price_writes:
            await self._apply_prices_batched(admin, price_writes, report)
        if norm_writes:
            await self._apply_normalizations_batched(admin, norm_writes, report)

        # Products this cycle re-read as already correct. A product written THIS cycle is
        # deliberately not here - it converges on the next pass, once we have seen the
        # result rather than only the response, and the 15-minute watermark overlap
        # guarantees a product we just wrote is rescanned within three cycles.
        if report.converged:
            report.normalized = await ledger.mark_normalized_many(
                self.platform_id, report.converged)
            if report.normalized:
                logger.info("%s: %d product(s) finished normalizing", self.name,
                            report.normalized)

        self._advance_watermark()
        logger.info("%s: cycle done, %s", self.name, report.summary())
        return report

    # -- planning ----------------------------------------------------------

    def _plan_product(self, platform: Any, product: Product,
                      state_map: dict[str, Any], taxonomy: TypeTaxonomy,
                      registered: Mapping[str, str], reassigned: Mapping[str, str],
                      report: CycleReport) -> None:
        own = assess_ownership(
            product.variant_skus, product.tags,
            platform.trigger_tag, product.syncio_source_gid,
            registered, reassigned,
        )
        if not own.ours:
            report.not_ours += 1
            return
        report.ours += 1
        parent_sku = own.parent_sku
        assert parent_sku is not None

        # The target price comes from the SOURCE, never from the destination product.
        #
        # This previously called compute_price on `product` - the DESTINATION - which
        # applied the 10% markup to a price that had already been marked up. Every cycle
        # compounded: 542.70 -> 597 -> 657 -> 723 -> 796 -> 876 on LRP-XTPS-0019 over
        # five passes, on the live storefront. A repricer that reads its own output is a
        # feedback loop, and the markup rule only means anything measured from 1nventory.
        #
        # sts_price and source_compare_at are written by the source scan from the
        # 1nventory product, so they are fixed points: re-running cannot move them.
        state = state_map.get(parent_sku)
        if state is None or state.sts_price is None:
            # Never scanned, or does not currently qualify. Repricing it would mean
            # inventing a target from the destination, which is the bug above.
            report.skipped.append((
                parent_sku, Skip.NOT_OURS,
                "no source-derived price on the state row; skipping until a scan sets one",
            ))
            return

        pricing = PricingResult(
            PriceOutcome.PRICED, state.sts_price, state.source_compare_at,
            f"source-derived target {state.sts_price} / compare-at {state.source_compare_at}",
        )

        vendor_now = product.vendor
        from services.internal_platform_taxonomy import normalize_title, normalize_vendor
        vendor_want = normalize_vendor(product.vendor)
        if vendor_want is None:
            report.skipped.append((parent_sku, Skip.NOT_OURS, "product has no vendor"))
            return

        # Category and gender must BOTH resolve, else we would write a partial tag set
        # and put the product in the wrong collections. Checked before desired_tags is
        # built, and from the same taxonomy that builds it - previously the guard read
        # the database mapping while the tags were written from a hardcoded map, so the
        # tag written was not the tag that had been checked.
        if taxonomy.category_for(product.product_type) is None:
            report.skipped.append((
                parent_sku, Skip.UNMAPPED_PRODUCT_TYPE,
                f"{product.product_type!r} has no Shop The Sample type mapping "
                "in listing options"))
            return
        if taxonomy.gender_for(product.product_type) is None:
            report.skipped.append((
                parent_sku, Skip.UNDERIVABLE_GENDER,
                f"{product.product_type!r} has no usable gender in listing options"))
            return

        # A product Syncio has just delivered still wears 1nventory's tags - SPO, BFCM,
        # MEM2025 - which mean nothing on this storefront and put it in the wrong
        # collections. Those get cleared down to exactly our three. Once the product has
        # converged to `listed` the tag set is no longer ours alone to define, and the
        # additive rule takes over so Shop The Sample's own tags are never stripped.
        first_normalization = state.current_status == "pending_normalization"
        tag_diff = desired_tags(product.product_type, product.tags,
                                platform.trigger_tag, taxonomy=taxonomy,
                                replace=first_normalization)

        wrote_anything = False

        if vendor_want != vendor_now:
            report.planned.append(PlannedWrite(
                parent_sku, product.gid, product.syncio_source_gid, "vendor",
                f"{vendor_now!r} -> {vendor_want!r}",
                payload={"vendor": vendor_want, "before": {"vendor": vendor_now}},
            ))
            wrote_anything = True

        # Kept a SEPARATE planned write from vendor even though both are fields of the
        # same productUpdate mutation, so the plan and the dry-run report say which of the
        # two actually changed. They are merged back into one alias at write time, where
        # productUpdate costs 10 points whether it carries one field or both - so the
        # clarity is free.
        title_now = product.title
        title_want = normalize_title(title_now)
        if title_want is not None and title_want != title_now:
            report.planned.append(PlannedWrite(
                parent_sku, product.gid, product.syncio_source_gid, "title",
                f"{title_now!r} -> {title_want!r}",
                payload={"title": title_want, "before": {"title": title_now}},
            ))
            wrote_anything = True

        if tag_diff.add or tag_diff.remove:
            report.planned.append(PlannedWrite(
                parent_sku, product.gid, product.syncio_source_gid, "tags",
                f"+{list(tag_diff.add)} -{list(tag_diff.remove)}",
                payload={"add": list(tag_diff.add), "remove": list(tag_diff.remove),
                         "before": {"tags": list(product.tags)}},
            ))
            wrote_anything = True

        # Quantized Decimal comparison. Shopify returns Money as strings, so "42.00"
        # vs "42.0" on a naive compare marks everything as drifted and silently turns
        # skip-unchanged into a 100% write rate.
        drifted = [
            v for v in product.variants
            if v.price != pricing.price or v.compare_at != pricing.compare_at
        ]
        if drifted:
            report.planned.append(PlannedWrite(
                parent_sku, product.gid, product.syncio_source_gid, "price",
                f"{len(drifted)}/{len(product.variants)} variants -> "
                f"{pricing.price}/{pricing.compare_at}",
                variant_count=len(drifted),
                payload={
                    "price": str(pricing.price),
                    "compare_at": str(pricing.compare_at) if pricing.compare_at else None,
                    "variant_gids": [v.gid for v in drifted],
                    "before": {v.gid: {"price": str(v.price),
                                       "compare_at": str(v.compare_at)} for v in drifted},
                },
            ))
            wrote_anything = True

        if not wrote_anything:
            report.unchanged += 1
            # Nothing left to write means normalization is done. Only a row still sitting
            # at pending_normalization is promoted; the filter is here rather than in the
            # UPDATE so a full scan does not ship 1,300 already-listed SKUs every cycle.
            if state.current_status == "pending_normalization":
                report.converged.append(parent_sku)

    def _check_caps(self, report: CycleReport, footprint: int) -> str | None:
        from services.internal_platform_rules import check_caps
        return check_caps(
            action_count=len(report.planned),
            delete_count=0,          # this poller never deletes
            sold_out_delete_count=0,
            variant_writes=report.variant_writes,
            units_zeroed=0,
            footprint=footprint,
            caps=self.caps,
        )

    # -- execution ---------------------------------------------------------

    async def _apply_prices_batched(self, admin: ShopifyAdmin,
                                    planned: list[PlannedWrite],
                                    report: CycleReport) -> None:
        """Reprice many products per round trip, ledger included.

        The sequential path costs ~4.2s per action, and only ~0.3s of that is Shopify -
        the rest is round trips to a remote database ~0.57s away. Batching only the
        Shopify call would have optimised the part that was never slow, so the ledger
        writes are batched with it, and then merged: the whole batch is two statements,
        claim+record and finish+release, with the mutation between them.

        Batch size is DERIVED from the variants being written, not a fixed product count.
        A parent with a deep size run costs three times a single-variant one, so a
        constant is either unsafe for the first or wasteful for the second. Sizing each
        batch to exactly one document also makes the crash window exactly one Shopify
        request, which is the smallest it can be while still batching at all.
        """
        groups = batches_by_cost(
            planned, lambda p: reprice_cost(len(p.payload["variant_gids"])))
        done = 0
        for group in groups:
            by_sku = {p.parent_sku: p for p in group}

            ids = await ledger.claim_and_record(
                self.platform_id, Act.REPRICE,
                [(p.parent_sku, p.source_gid, p.dest_gid, p.payload)
                 for p in group],
            )
            if not ids:
                continue
            group = [by_sku[sku] for sku in ids]
            closed = False

            try:
                items = []
                for p in group:
                    price = Decimal(p.payload["price"])
                    compare = (Decimal(p.payload["compare_at"])
                               if p.payload["compare_at"] else None)
                    items.append((p.dest_gid,
                                  [(gid, price, compare) for gid in p.payload["variant_gids"]]))

                errors = await admin.set_variant_prices_bulk(items)

                ok_ids, bad_ids, dest_gids = [], [], {}
                for p in group:
                    err = errors.get(p.dest_gid)
                    if err:
                        bad_ids.append(ids[p.parent_sku])
                        report.failed += 1
                        logger.warning("%s: reprice failed on %s: %s",
                                       self.name, p.parent_sku, err)
                    else:
                        ok_ids.append(ids[p.parent_sku])
                        dest_gids[p.parent_sku] = p.dest_gid
                        report.executed += 1

                await ledger.finish_and_release(
                    self.platform_id, release_skus=[p.parent_sku for p in group],
                    ok_ids=ok_ids, bad_ids=bad_ids, dest_gids=dest_gids,
                    error="see per-product warning in the log",
                )
                closed = True
            except ShopifyError as exc:
                # Whole-request failure: throttling, scope, transport. The products
                # requeue on the next cycle regardless - the state row is what decides
                # that - so these rows are closed rather than left pending forever, with
                # an error that says the outcome is unknown instead of implying we know
                # the mutation did not land.
                logger.warning("%s: reprice batch failed: %s", self.name, exc)
                report.failed += len(group)
                await ledger.finish_and_release(
                    self.platform_id, release_skus=[p.parent_sku for p in group],
                    bad_ids=list(ids.values()),
                    error=f"batch request failed, outcome unknown: {exc}",
                )
                closed = True
            finally:
                if not closed:
                    await ledger.release_many(
                        self.platform_id, [p.parent_sku for p in group])

            done += len(group)
            logger.info("%s: repriced %d/%d (%d products in this document)",
                        self.name, done, len(planned), len(group))

    async def _apply_normalizations_batched(self, admin: ShopifyAdmin,
                                            planned: list[PlannedWrite],
                                            report: CycleReport) -> None:
        """Vendor and tag corrections, batched the same way prices are.

        Vendor and tags for one product are merged into a SINGLE normalize action here.
        The sequential path recorded them as two ledger rows, which described the
        pipeline's internal loop rather than what happened to the product - one product
        normalized once is the truer audit row, and it is what the batch can attribute an
        error to.

        Sized by normalize_cost, which charges only for the aliases a product actually
        needs. The fixed chunk this replaces was 28, derived from the worst case of three
        mutations per product - but a product needing one correction costs 10 points, not
        30, so a document that could have carried 85 carried 28.
        """
        merged: dict[str, dict[str, Any]] = {}
        for p in planned:
            e = merged.setdefault(p.parent_sku, {
                "dest_gid": p.dest_gid, "source_gid": p.source_gid,
                "vendor": None, "title": None, "add": [], "remove": [],
                "payload": {},
            })
            # vendor and title collapse into ONE productUpdate alias; tags are their own
            # mutations. Three planned writes for a product become at most three aliases,
            # never four.
            if p.kind == "vendor":
                e["vendor"] = p.payload["vendor"]
                e["payload"]["vendor"] = p.payload
            elif p.kind == "title":
                e["title"] = p.payload["title"]
                e["payload"]["title"] = p.payload
            else:
                e["add"] = list(p.payload["add"])
                e["remove"] = list(p.payload["remove"])
                e["payload"]["tags"] = p.payload

        skus = list(merged)
        groups = batches_by_cost(
            skus, lambda s: normalize_cost(
                merged[s]["vendor"] or merged[s]["title"],
                merged[s]["add"], merged[s]["remove"]))
        done = 0
        for group_skus in groups:
            ids = await ledger.claim_and_record(
                self.platform_id, Act.NORMALIZE,
                [(sku, merged[sku]["source_gid"], merged[sku]["dest_gid"],
                  merged[sku]["payload"]) for sku in group_skus],
            )
            if not ids:
                continue
            claimed = list(ids)
            closed = False

            try:
                errors = await admin.apply_normalizations_bulk([
                    (merged[sku]["dest_gid"], merged[sku]["vendor"],
                     merged[sku]["title"], merged[sku]["add"], merged[sku]["remove"])
                    for sku in claimed
                ])

                ok_ids, bad_ids, dest_gids = [], [], {}
                for sku in claimed:
                    err = errors.get(merged[sku]["dest_gid"])
                    if err:
                        bad_ids.append(ids[sku])
                        report.failed += 1
                        logger.warning("%s: normalize failed on %s: %s",
                                       self.name, sku, err)
                    else:
                        ok_ids.append(ids[sku])
                        dest_gids[sku] = merged[sku]["dest_gid"]
                        report.executed += 1

                await ledger.finish_and_release(
                    self.platform_id, release_skus=claimed,
                    ok_ids=ok_ids, bad_ids=bad_ids, dest_gids=dest_gids,
                    error="see per-product warning in the log",
                )
                closed = True
            except ShopifyError as exc:
                logger.warning("%s: normalize batch failed: %s", self.name, exc)
                report.failed += len(claimed)
                await ledger.finish_and_release(
                    self.platform_id, release_skus=claimed,
                    bad_ids=list(ids.values()),
                    error=f"batch request failed, outcome unknown: {exc}",
                )
                closed = True
            finally:
                if not closed:
                    await ledger.release_many(self.platform_id, claimed)

            done += len(group_skus)
            logger.info("%s: normalized %d/%d (%d products in this document)",
                        self.name, done, len(skus), len(group_skus))

    async def _apply(self, platform: Any, admin: ShopifyAdmin, planned: PlannedWrite,
                     state_map: dict[str, Any], report: CycleReport) -> None:
        action = Act.NORMALIZE if planned.kind in ("vendor", "tags") else Act.REPRICE

        state = await ledger.claim(self.platform_id, planned.parent_sku, action)
        if state is None:
            logger.debug("%s: %s already claimed, skipping", self.name, planned.parent_sku)
            return

        row = await ledger.record(
            platform_id=self.platform_id,
            parent_sku=planned.parent_sku,
            action=action,
            status=St.PENDING,
            source_gid=planned.source_gid,
            dest_gid=planned.dest_gid,
            payload=planned.payload,
        )

        try:
            if planned.kind == "vendor":
                await admin.update_vendor(planned.dest_gid, planned.payload["vendor"])
            elif planned.kind == "tags":
                await admin.add_tags(planned.dest_gid, planned.payload["add"])
                await admin.remove_tags(planned.dest_gid, planned.payload["remove"])
            else:
                price = Decimal(planned.payload["price"])
                compare = (Decimal(planned.payload["compare_at"])
                           if planned.payload["compare_at"] else None)
                await admin.set_variant_prices(
                    planned.dest_gid,
                    [(gid, price, compare) for gid in planned.payload["variant_gids"]],
                )
            await ledger.finish(row, status=St.SUCCESS, result={"ok": True})
            state.dest_product_gid = planned.dest_gid
            await state.save(update_fields=["dest_product_gid", "updated_at"])
            report.executed += 1
        except ShopifyError as exc:
            await ledger.finish(row, status=St.FAILED, error=str(exc),
                                result={"detail": exc.detail})
            report.failed += 1
            logger.warning("%s: %s failed on %s: %s",
                           self.name, planned.kind, planned.parent_sku, exc)
        finally:
            await ledger.release(state)

    # -- watermark ---------------------------------------------------------

    def _watermark(self) -> str | None:
        if self._last_watermark is None:
            return None
        return self._last_watermark

    def _advance_watermark(self) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=WATERMARK_OVERLAP_MINUTES)
        self._last_watermark = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")


internal_platform_dest_poller = InternalPlatformDestPoller()
