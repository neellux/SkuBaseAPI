"""Source-side reconciler for internal platforms.

Computes the qualifying set from the source catalog, tags what should be listed,
untags what should not, and deletes the corresponding destination product.

TWO CADENCES, deliberately:

  scan pass    every `interval_seconds` (default 300). Full catalog sweep: refreshes
               state and variant counts, and tags what newly qualifies. Measured cost
               on 1nventory is ~3,300 points and ~60s for 14,410 products / 38,229
               variants, against a bucket that restores 200 points/s - 5.5% of a
               5-minute budget, so the cadence is bounded by wall clock, not quota.

  delist pass  daily. Adds the untag/delete half. Kept daily because `delist_soak_cycles`
               counts CYCLES: running the delist evaluation every 5 minutes would shrink
               a two-day soak to ten minutes and multiply the per-cycle delete caps by
               288. Nothing about the sell-out case needs sub-daily latency.

One case does need sub-daily latency, and it runs on the SCAN pass instead: a product
tagged on 1nventory but not yet delivered by Syncio, which sells out or drifts out of the
price band inside Syncio's 1-to-3-day window. The daily path cannot reach it in time -
two soak days plus a human Delist click is longer than the window it is racing - and by
the time it fires the product is already live on Shop The Sample at zero stock or at the
wrong price. Untagging before delivery is a source-side tagsRemove with no destination
product to delete, so it carries none of the delist path's irreversibility and gets its
own flag (`auto_untag_awaiting_sync`), its own clock (`ineligible_since`, in minutes) and
its own cap. See pre_delivery_untag_due() in internal_platform_rules.

BLOCKED: the source store currently grants read_products only. Every write here raises
ShopifyScopeError until write_products is granted and the app reinstalled. The planning
half runs fine today and produces a reviewable dry-run report.

Writes to the LIVE 1nventory operational catalog, so its caps are tighter than the
destination poller's and deletes are gated separately from execute.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from config import config
from models.db_models import (
    InternalPlatformAction as Act,
    InternalPlatformSkipReason as Skip,
    InternalPlatformStatus as St,
)
from services import internal_platform_ledger as ledger
from services.internal_platform_type_map import check_taxonomy_health, load_taxonomy
from services.internal_platform_products import (
    load_reassigned,
    product_parent,
    reassigned_skus_on,
    resolve_registered_parents,
)
from services.internal_platform_rules import (
    DEFAULT_PRICING,
    SKIPPED,
    check_submit_cooldown,
    compute_price,
    pricing_basis,
    check_reconcile_cap,
    check_syncio_capacity,
    derive_scan_status,
    fit_to_capacity,
    Allowlists,
    SafetyCaps,
    SourceProduct,
    check_candidate_set,
    check_caps,
    delist_cause,
    is_awaiting_sync,
    is_delist_candidate,
    is_tagged,
    plan_scheduled_actions,
    pre_delivery_untag_due,
    qualifies,
)
from services.shopify_admin import Product, ShopifyAdmin
from services.shopify_client import (
    ShopifyError,
    ShopifyScopeError,
    enable_writes,
    get_shopify_client,
    writes_enabled,
)

logger = logging.getLogger(__name__)


def to_source_product(p: Product, parent_sku: str | None) -> SourceProduct:
    return SourceProduct(
        gid=p.gid,
        parent_sku=parent_sku,
        vendor=p.vendor,
        product_type=p.product_type,
        tags=p.tags,
        total_inventory=p.total_inventory,
        variant_prices=p.variant_prices,
        variant_compare_at=p.variant_compare_at,
        variant_inventory=p.variant_inventory,
        updated_at=p.updated_at,
    )


@dataclass(slots=True)
class SourceReport:
    scanned: int = 0
    unregistered: int = 0        # no registered parent in the products DB -> not listed
    # Every variant SKU merged onto another parent. Counted apart from `unregistered`
    # because the causes are opposite: SkuBase has never heard of an unregistered product,
    # and knows a reassigned one too well.
    reassigned: int = 0
    queued_reassigned: int = 0   # orphaned rows moved to pending_delisting
    reconciled: int = 0          # rows whose stored stock the scan corrected
    zeroed: int = 0              # of those, rows whose SKUs are gone from Shopify entirely
    qualifying: int = 0
    to_tag: list[tuple[str, str, str]] = field(default_factory=list)      # sku, gid, why
    to_delist: list[tuple[str, str, str]] = field(default_factory=list)   # sku, gid, cause
    # Tagged, awaiting Syncio, stopped qualifying, soak elapsed: untag before delivery.
    # (sku, gid, cause, skus) - the variant SKUs ride along because the destination check
    # runs after the scan loop, by which point the Shopify product is out of scope.
    to_untag_awaiting: list[tuple[str, str, str, tuple[str, ...]]] = field(
        default_factory=list)
    soaking: int = 0
    # Awaiting Syncio and failing, but the untag soak has not elapsed yet.
    pre_delivery_soaking: int = 0
    # Tagged, out of stock, and deliberately NOT delisted because stock is transient.
    # Counted so a large number is visible rather than looking like nothing happened.
    stock_held: int = 0
    held_back: int = 0           # trimmed to fit Syncio's remaining variant budget
    variants_submitted: int = 0
    dry_run: bool = False
    gate_message: str = ""
    tagged: int = 0
    untagged: int = 0
    deleted: int = 0
    # Untagged before Syncio delivered. Counted apart from `deleted` because no
    # productDelete happened - reporting these as deletions would make a reversible
    # source-side action indistinguishable from the irreversible one.
    pre_delivery_untagged: int = 0
    # Found on the destination after all, so handed back to the reviewed delist path
    # instead of being untagged.
    pre_delivery_arrived: int = 0
    failed: int = 0
    aborted: str | None = None

    def summary(self) -> str:
        return (
            f"scanned={self.scanned} unregistered={self.unregistered} "
            f"reassigned={self.reassigned} queued_reassigned={self.queued_reassigned} "
            f"reconciled={self.reconciled} zeroed={self.zeroed} "
            f"qualifying={self.qualifying} "
            f"to_tag={len(self.to_tag)} to_delist={len(self.to_delist)} "
            f"to_untag_awaiting={len(self.to_untag_awaiting)} "
            f"soaking={self.soaking} pre_delivery_soaking={self.pre_delivery_soaking} "
            f"stock_held={self.stock_held} "
            f"held_back={self.held_back} "
            f"variants={self.variants_submitted} tagged={self.tagged} untagged={self.untagged} "
            f"deleted={self.deleted} "
            f"pre_delivery_untagged={self.pre_delivery_untagged} "
            f"pre_delivery_arrived={self.pre_delivery_arrived} failed={self.failed}"
            + (f" ABORTED={self.aborted}" if self.aborted else "")
        )


class InternalPlatformSourcePoller:

    def __init__(self) -> None:
        cfg = config.get("internal_platform_source_poller", {})
        self.name = "InternalPlatformSourcePoller"
        self.platform_id: str = cfg.get("platform_id", "shopthesample")

        self.enabled: bool = bool(cfg.get("enabled", False))
        self.execute: bool = bool(cfg.get("execute", False))
        # Gates the SCHEDULED pass only. The manual button ignores this and is gated by
        # `execute` instead, so tagging can be driven by hand while the cron stays off.
        self.auto_submit: bool = bool(cfg.get("auto_submit", False))
        # Same shape for delisting. While false the scheduled pass marks products
        # pending_delisting and stops - the irreversible half only ever runs from the
        # Delist button.
        self.auto_delist: bool = bool(cfg.get("auto_delist", False))
        # Pace successive batches: a new one waits for Syncio to drain, or for this many
        # hours since the last successful submit, whichever comes first. 0 disables.
        self.submit_cooldown_hours: int = int(cfg.get("submit_cooldown_hours", 24))
        # Syncio's rough daily variant throughput. Tagging past it does not make anything
        # arrive sooner, it just builds an invisible queue. 0 disables the gate.
        self.max_products_in_flight: int = int(cfg.get("max_products_in_flight", 500))
        # Deletes are irreversible, so they get their own flag. One boolean should not
        # simultaneously enable tagging and permanent catalog destruction.
        self.execute_deletes: bool = bool(cfg.get("execute_deletes", False))

        # Full catalog sweep cadence. Scan + tag only; see the module docstring.
        self.interval_seconds: int = int(cfg.get("interval_seconds", 300))
        self.daily_hour: int = int(cfg.get("daily_hour", 5))
        self.daily_minute: int = int(cfg.get("daily_minute", 0))
        self.timezone: str = cfg.get("timezone", "America/New_York")
        self.page_size: int = int(cfg.get("page_size", 250))

        # A product must fail qualification on this many consecutive DELIST passes
        # before it is delisted. Defeats every transient-read failure mode for the
        # price of one day's latency. Only the daily pass bumps this counter, so the
        # unit stays "days" no matter how often the scan pass runs.
        self.delist_soak_cycles: int = int(cfg.get("delist_soak_cycles", 2))

        # Whether selling out is grounds for delisting. FALSE by default and in config:
        # stock is transient, but a delist is not - it untags on 1nventory AND deletes the
        # Shop The Sample product outright, so the listing has to be rebuilt from nothing
        # when the item restocks. 583 of the 1,363 live products were at zero stock when
        # this was measured, so the flag decides the fate of 43% of the live footprint.
        self.delist_on_no_inventory: bool = bool(
            cfg.get("delist_on_no_inventory", False)
        )

        # Untag a product Syncio has not delivered yet, once it stops qualifying. Its own
        # flag rather than a mode of auto_delist, because the two authorise different
        # things: auto_delist permits a productDelete on Shop The Sample, this permits one
        # tagsRemove on 1nventory against a product that has no destination product at all.
        # Folding them together would mean an operator could only get the reversible
        # behaviour by also arming the irreversible one.
        #
        # Note this ignores delist_on_no_inventory, deliberately. That flag is false
        # because tearing down a LIVE listing over transient stock costs a rebuild; before
        # delivery there is nothing to tear down. See pre_delivery_untag_due().
        self.auto_untag_awaiting_sync: bool = bool(
            cfg.get("auto_untag_awaiting_sync", False)
        )
        # How long a tagged product must fail qualification before it is untagged, in
        # MINUTES against a stored timestamp rather than in cycles against a counter: this
        # runs on the five-minute scan, and a timestamp survives a restart, a missed cycle
        # and a change to interval_seconds. Sized to ride out a momentary stock dip during
        # a transfer or a mid-bulk-edit price while still acting a day or more ahead of
        # Syncio. 0 = untag on the first failing scan.
        self.awaiting_sync_untag_soak_minutes: int = int(
            cfg.get("awaiting_sync_untag_soak_minutes", 60)
        )

        # Correct stored stock against what the scan actually saw, including zeroing rows
        # whose SKUs are gone from Shopify. ON by default: without it a row whose parent key
        # stopped resolving freezes forever, which is how nine phantom units survived on the
        # Products tab from merge day until 2026-08-03.
        self.reconcile_stock: bool = bool(cfg.get("reconcile_stock", True))

        # Move orphaned reassigned rows to pending_delisting. OFF by default and shipped off
        # deliberately: pending_delisting is the queue the Delist button drains, and
        # execute_deletes is TRUE in production. Turn this on only after a cycle has run with
        # it off and the mispaired rows have been confirmed to have healed their
        # source_product_gid - see the plan's landing order. While off, reassigned products
        # are still detected, counted and kept out of tagging; they are simply not queued.
        self.queue_reassigned_delists: bool = bool(
            cfg.get("queue_reassigned_delists", False)
        )

        self.caps = SafetyCaps(
            max_actions_per_cycle=int(cfg.get("max_tags_per_cycle", 150)),
            max_deletes_per_cycle=int(cfg.get("max_deletes_per_cycle", 0)),
            max_sold_out_deletes_per_cycle=int(cfg.get("max_sold_out_deletes_per_cycle", 20)),
            max_pct_of_footprint_changed=float(cfg.get("max_pct_of_footprint_changed", 10.0)),
            min_candidate_set_size=int(cfg.get("min_candidate_set_size", 50)),
            max_candidate_set_shrink_pct=float(cfg.get("max_candidate_set_shrink_pct", 50.0)),
            max_rows_zeroed_per_cycle=int(cfg.get("max_rows_zeroed_per_cycle", 50)),
            max_pre_delivery_untags_per_cycle=int(
                cfg.get("max_pre_delivery_untags_per_cycle", 25)),
        )

        self.allowlists = Allowlists(
            vendors=frozenset(v.upper() for v in cfg.get("allow_vendors", [])),
            product_types=frozenset(cfg.get("allow_product_types", [])),
            strict=bool(cfg.get("allowlist_strict", False)),
        )

        self._scheduler: AsyncIOScheduler | None = None
        self._previous_candidate_size: int | None = None

    async def start(self) -> None:
        if not self.enabled:
            logger.info("%s: disabled in config, skipping start", self.name)
            return
        self._scheduler = AsyncIOScheduler(timezone=self.timezone)
        # A sweep takes ~60s against a 300s interval, so overlap is unlikely - but
        # max_instances=1 + coalesce means a slow sweep delays the next one rather than
        # stacking two concurrent full-catalog scans on one cost bucket.
        self._scheduler.add_job(
            self._scan_pass,
            IntervalTrigger(seconds=self.interval_seconds),
            id="internal_platform_source_scan",
            max_instances=1,
            coalesce=True,
        )
        self._scheduler.add_job(
            self._delist_pass,
            CronTrigger(hour=self.daily_hour, minute=self.daily_minute,
                        timezone=self.timezone),
            id="internal_platform_source_delist",
            max_instances=1,
            coalesce=True,
        )
        self._scheduler.start()
        logger.info(
            "%s: scan every %ds, delist daily at %02d:%02d %s (execute=%s, deletes=%s)",
            self.name, self.interval_seconds, self.daily_hour, self.daily_minute,
            self.timezone, self.execute, self.execute_deletes,
        )

    async def stop(self) -> None:
        if self._scheduler:
            self._scheduler.shutdown(wait=False)
            self._scheduler = None

    async def run_once(self) -> SourceReport:
        """Manual trigger, so a cron is testable without waiting for its hour."""
        return await self._cycle(delists=True)

    async def _scan_pass(self) -> SourceReport:
        """The 5-minute sweep: refresh state, tag what newly qualifies. Never delists."""
        return await self._cycle(delists=False)

    async def _delist_pass(self) -> SourceReport:
        """The daily pass: everything the scan does, plus untag/delete."""
        return await self._cycle(delists=True)

    async def manual_submit(self) -> SourceReport:
        """Tag the next batch on demand, from stored state rather than a fresh scan.

        The scheduled pass discovers what qualifies; this only has to ACT on it. Rows
        already sitting at `ready_for_listing` are the answer, so instead of re-reading
        14,410 products to rediscover five, it takes the five it wants and re-reads only
        those. Roughly two seconds against the ~140 the full cycle took, which is the
        difference between a button and a button that looks broken.

        The re-read is not optional. A ready_for_listing row is up to one scan-interval
        stale and the product may have sold out in between, so every candidate is checked
        against qualifies() again on fresh Shopify data before it is tagged. What this
        skips is DISCOVERY, never verification.

        Gates are unchanged: `execute` (checked by the route), the Syncio capacity ceiling
        and the submit cooldown all still apply. The candidate-set guard does not, because
        it exists to catch a truncated catalog scan and there is no catalog scan here.
        """
        report = SourceReport()
        report.dry_run = not self.execute

        platform = await ledger.get_platform(self.platform_id)
        if platform is None:
            report.aborted = "platform-missing"
            report.gate_message = f"Platform {self.platform_id} not found"
            return report
        if not platform.enabled:
            report.aborted = "platform-disabled"
            report.gate_message = f"Platform {self.platform_id} is disabled"
            return report

        in_flight = await ledger.products_in_flight(self.platform_id)
        capacity = check_syncio_capacity(in_flight, self.max_products_in_flight)
        report.gate_message = capacity.message
        if capacity.blocked:
            report.aborted = "syncio-capacity"
            return report

        cooldown = check_submit_cooldown(
            in_flight, await ledger.last_submit_at(self.platform_id),
            datetime.now(timezone.utc), self.submit_cooldown_hours,
        )
        if not cooldown.allowed:
            report.gate_message = cooldown.message
            report.aborted = "submit-cooldown"
            return report

        # Take a bounded slice, ordered the same way the Products tab is, so "the next
        # five" means the five a human would predict from the screen.
        budget = capacity.remaining if self.max_products_in_flight > 0 else 0
        candidates = await ledger.ready_for_listing(self.platform_id, limit=budget or None)
        report.qualifying = len(candidates)
        if not candidates:
            report.gate_message = "Nothing is ready for listing"
            return report

        await ledger.recover_stale_inflight(self.platform_id)
        admin = ShopifyAdmin(await get_shopify_client(platform.source_store))
        taxonomy = await load_taxonomy(self.platform_id)

        thin = check_taxonomy_health(taxonomy)
        if thin:
            logger.error("%s: %s", self.name, thin)
            report.aborted = "taxonomy-too-thin"
            report.gate_message = thin
            return report

        gids = [r.source_product_gid for r in candidates if r.source_product_gid]
        fresh = {p.gid: p for p in await admin.products_by_ids(gids)}
        report.scanned = len(fresh)

        # A row can have been merged away between the scan that marked it ready and this
        # press. Tagging it would hand Syncio a product whose SKUs now belong to another
        # parent, so the same check the scan makes is repeated here on fresh data. Fail
        # closed for the same reason it does there.
        try:
            reassigned = await load_reassigned(
                {s for p in fresh.values() for s in p.variant_skus if s}
            )
        except Exception as exc:                                     # noqa: BLE001
            logger.error("%s: could not read secondary_skus, refusing to tag: %s",
                         self.name, exc)
            report.aborted = "reassigned-read-failed"
            report.gate_message = f"secondary_skus unreadable: {exc}"
            return report

        by_gid = {r.source_product_gid: r for r in candidates}
        variant_counts: dict[str, int] = {}
        for gid, product in fresh.items():
            row = by_gid.get(gid)
            if row is None:
                continue
            sp = to_source_product(product, row.parent_sku)
            if is_tagged(sp, platform.trigger_tag):
                continue        # already tagged between the scan and now
            skus = [s for s in product.variant_skus if s]
            moved = reassigned_skus_on(product.variant_skus, reassigned)
            if skus and len(moved) == len(skus):
                # Every SKU merged away since the scan. Correct the row so the tab stops
                # offering it, and do not tag.
                await ledger.apply_scan_statuses(
                    self.platform_id,
                    {row.parent_sku: (SKIPPED, Skip.REASSIGNED_SKU.value,
                                      len(product.variants))},
                )
                report.reassigned += 1
                continue
            verdict = qualifies(sp, self.allowlists, DEFAULT_PRICING, taxonomy=taxonomy)
            if not verdict.qualified:
                # Went stale - usually sold out. Correct the row so the tab agrees.
                await ledger.apply_scan_statuses(
                    self.platform_id,
                    {row.parent_sku: (SKIPPED, verdict.rejected_by, len(product.variants))},
                )
                continue
            variant_counts[row.parent_sku] = len(product.variants)
            report.to_tag.append((row.parent_sku, gid, verdict.reason or ""))

        if not report.to_tag:
            report.gate_message = "Nothing still qualifies; the tab has been updated"
            return report

        sized = [(sku, variant_counts.get(sku, 1)) for sku, _, _ in report.to_tag]
        fits, used, held = fit_to_capacity(sized, budget or len(sized))
        if held:
            keep = set(fits)
            report.to_tag = [r for r in report.to_tag if r[0] in keep]
            report.held_back = held
        report.variants_submitted = used

        footprint = await ledger.footprint(self.platform_id)
        breach = check_caps(
            action_count=len(report.to_tag), delete_count=0, sold_out_delete_count=0,
            variant_writes=0, units_zeroed=0, footprint=footprint, caps=self.caps,
        )
        if breach:
            logger.error("%s: CAP BREACH, zero writes: %s", self.name, breach)
            report.aborted = breach
            return report

        if not self.execute:
            logger.info("%s: dry-run, %s", self.name, report.summary())
            return report

        if not writes_enabled():
            enable_writes(f"{self.name} execute=true")
        await self._apply_tags(platform, admin, report)
        logger.info("%s: manual submit done, %s", self.name, report.summary())
        return report

    async def manual_delist(self) -> SourceReport:
        """Execute every queued delist on demand. Mirrors manual_submit().

        Operates on rows the scan already moved to `pending_delisting`, so the set has
        soaked and has been visible on the Products tab. Reuses _apply_delists rather
        than reimplementing it - that function carries the untag-confirm-guard-preimage
        ordering that a second implementation would inevitably get wrong.
        """
        report = SourceReport()
        platform = await ledger.get_platform(self.platform_id)
        if platform is None:
            report.aborted = "platform-missing"
            report.gate_message = f"Platform {self.platform_id} not found"
            return report
        if not platform.enabled:
            report.aborted = "platform-disabled"
            report.gate_message = f"Platform {self.platform_id} is disabled"
            return report

        rows = await ledger.pending_delists(self.platform_id)
        report.to_delist = [
            (r.parent_sku, r.source_product_gid or "", r.skip_reason or "unknown")
            for r in rows
            if r.source_product_gid
        ]
        missing_gid = len(rows) - len(report.to_delist)
        if missing_gid:
            # No source GID means nothing to untag, and untagging is the step that stops
            # Syncio recreating the product. Skip rather than guess.
            logger.warning("%s: %d pending delists have no source_product_gid, skipping",
                           self.name, missing_gid)

        if not report.to_delist:
            report.gate_message = "Nothing is queued for delisting"
            return report

        await ledger.recover_stale_inflight(self.platform_id)
        client = await get_shopify_client(platform.source_store)
        admin = ShopifyAdmin(client)

        if not writes_enabled():
            enable_writes(f"{self.name} manual delist")

        await self._apply_delists(platform, admin, report, report.to_delist)
        logger.info("%s: manual delist done, %s", self.name, report.summary())
        return report

    # -- cycle -------------------------------------------------------------

    # `manual` used to be a parameter here. It is gone: manual_submit() was rewritten to
    # work from stored state and no longer routes through this method, so no caller ever
    # passed it, and a dead parameter that appears to unlock the write path is worse than
    # no parameter at all - it was half of why auto_delist looked reachable.
    async def _cycle(self, delists: bool = True) -> SourceReport:
        report = SourceReport()
        report.dry_run = not self.execute

        platform = await ledger.get_platform(self.platform_id)
        if platform is None:
            report.aborted = "platform-missing"
            report.gate_message = f"Platform {self.platform_id} not found"
            return report
        if not platform.enabled:
            report.aborted = "platform-disabled"
            report.gate_message = (
                f"Platform {self.platform_id} is disabled; enable it on the "
                "internal_platforms row before submitting"
            )
            return report

        # Allowlist gate DISABLED for now (2026-07-24): selection is min-price +
        # discount-band only, so an empty allowlist is expected and must not block.
        # Re-enable together with the allowlist filters in internal_platform_rules.qualifies.
        # if not self.allowlists.usable:
        #     # Fails CLOSED. The opposite default would make a missing allowlist turn
        #     # every listed product into a delist candidate.
        #     logger.error("%s: allowlist empty, refusing to run", self.name)
        #     report.aborted = "empty-allowlist"
        #     return report

        await ledger.recover_stale_inflight(self.platform_id)
        state_map = await ledger.load_state_map(self.platform_id)
        # Category AND gender, from the listing-options mappings. One load per cycle;
        # the pure rules take it as an argument so they stay database-free.
        taxonomy = await load_taxonomy(self.platform_id)

        # BEFORE the Shopify scan, not after. A thin taxonomy makes every product read as
        # unmapped_product_type, and on a delist pass that starts the whole tagged
        # catalog soaking toward deletion. Aborting here also means no product facts are
        # written, so a cycle that cannot be trusted leaves the database untouched.
        thin = check_taxonomy_health(taxonomy)
        if thin:
            logger.error("%s: %s", self.name, thin)
            report.aborted = "taxonomy-too-thin"
            report.gate_message = thin
            return report

        client = await get_shopify_client(platform.source_store)
        admin = ShopifyAdmin(client)

        candidates: list[tuple[SourceProduct, Product]] = []
        try:
            scanned_products = []
            async for product in admin.products_by_tag("", page_size=self.page_size):
                report.scanned += 1
                scanned_products.append(product)
        except ShopifyScopeError as exc:
            logger.error("%s: missing scope: %s", self.name, exc)
            report.aborted = "scope"
            return report
        except ShopifyError as exc:
            # A partial scan is indistinguishable from a mass drop-out, so abort rather
            # than act on what we got.
            logger.warning("%s: scan incomplete, aborting: %s", self.name, exc)
            report.aborted = "incomplete-scan"
            return report

        # No completeness assertion follows, deliberately. A short scan cannot cause a
        # wrong action here: every write below is keyed on a product this scan returned,
        # and absence is never read as signal. See the note above check_candidate_set.

        # Resolve parents from the products DB. A product with no registered parent is
        # NOT listed: SkuBase does not know it, it cannot be normalized safely, and it
        # would escape the destination ownership guard. String-splitting is deliberately
        # not a resolution path - it would invent unregistered parents.
        all_skus = {s for p in scanned_products for s in p.variant_skus if s}
        registered = await resolve_registered_parents(all_skus)

        # Which of those SKUs have been merged onto another parent. Loaded BEFORE any write,
        # and fail-closed: an unreadable matview must never be read as "nothing is
        # reassigned", because that is precisely the state in which the mislink happens.
        try:
            reassigned = await load_reassigned(all_skus)
        except Exception as exc:                                     # noqa: BLE001
            logger.error("%s: could not read secondary_skus, aborting before any write: %s",
                         self.name, exc)
            report.aborted = "reassigned-read-failed"
            report.gate_message = f"secondary_skus unreadable: {exc}"
            return report

        # sku -> live inventoryQuantity, for the reconciliation pass below. Built from the
        # products this scan returned, so it is only trustworthy on a cycle that completed -
        # which the ShopifyError/ShopifyScopeError aborts above have already established.
        live_stock: dict[str, int] = {}
        for product in scanned_products:
            for variant in product.variants:
                if variant.sku:
                    live_stock[variant.sku] = variant.inventory_quantity

        # Source products whose every resolvable SKU has been reassigned away. Keyed by
        # source gid because that is the only handle left: they no longer resolve to a
        # parent, so they cannot be found in state_map by key.
        reassigned_gids: dict[str, list[str]] = {}

        variant_counts: dict[str, int] = {}
        facts: dict[str, ledger.ProductFacts] = {}
        for product in scanned_products:
            parent = product_parent(product.variant_skus, registered, reassigned)
            if parent is None:
                moved = reassigned_skus_on(product.variant_skus, reassigned)
                if moved:
                    # SkuBase knows these SKUs; they have just stopped describing this
                    # product. Do NOT fall through to the unregistered branch - that would
                    # bury a merge under a count that means "never heard of it".
                    report.reassigned += 1
                    reassigned_gids[product.gid] = moved
                else:
                    report.unregistered += 1
                continue
            # Refreshed every scan, not written once at tag time, so a product whose size
            # run changed on 1nventory is not budgeted at its old weight forever.
            variant_counts[parent] = len(product.variants)

            # Everything the Products tab shows. Free: the scan already read all of it off
            # this product, and compute_price is the same call qualification makes below.
            # The SAME two numbers compute_price priced from, not a second computation.
            # Storing an independent min() here is how the row came to show a cheapest
            # price the engine never used - "$164 -> $220" is not a 10% markup of $164.
            base, compare = pricing_basis(
                product.variant_prices, product.variant_compare_at,
                product.variant_inventory,
            )
            pricing = compute_price(product.variant_prices, product.variant_compare_at,
                                    DEFAULT_PRICING, product.variant_inventory)
            # Size comes from the trailing segment of a PARENT/SIZE variant SKU. This is
            # display only - it labels the size rail on the Products tab and nothing keys
            # off it. Parent RESOLUTION deliberately does not work this way; it goes
            # through the products DB below. Falls back to None so a non-conforming
            # variant shows no size rather than a wrong one.
            variant_rows = tuple(
                {
                    "sku": v.sku,
                    "size": (v.sku.rpartition("/")[2] if v.sku and "/" in v.sku else None),
                    "price": str(v.price) if v.price is not None else None,
                    "compare_at": str(v.compare_at) if v.compare_at is not None else None,
                    "inventory": v.inventory_quantity,
                }
                for v in product.variants
            )
            want = ledger.ProductFacts(
                source_gid=product.gid,
                title=product.title,
                image_url=product.image_url,
                product_type=product.product_type,
                inventory=product.total_inventory,
                variant_count=len(product.variants),
                source_price=base,
                source_compare_at=compare,
                sts_price=pricing.price if pricing.ok else None,
                variants=variant_rows,
            )
            existing = state_map.get(parent)
            if existing is None or want.differs_from(existing):
                facts[parent] = want

            candidates.append((to_source_product(product, parent), product))

        # NOTHING has been written yet, and nothing will be until every gate below has
        # passed. refresh_product_facts used to run HERE, before check_candidate_set, so
        # an aborted cycle still left 13,757 rows rewritten - a cycle we had just decided
        # not to trust.
        qualifying = [
            (sp, p) for sp, p in candidates
            if qualifies(sp, self.allowlists, DEFAULT_PRICING, taxonomy=taxonomy).qualified
        ]
        report.qualifying = len(qualifying)

        breach = check_candidate_set(len(qualifying), self._previous_candidate_size,
                                     self.caps)
        if breach:
            logger.error("%s: candidate-set guard tripped: %s", self.name, breach)
            report.aborted = breach
            return report

        # Everything the loop decides, accumulated and written together after it. The
        # loop itself performs NO I/O, so it cannot leave the database half-updated.
        desired_status: dict[str, tuple[str, str | None, int]] = {}
        to_bump: list[str] = []      # soak counter +1
        to_clear: list[str] = []     # soak counter reset to 0
        to_start_inelig: list[str] = []   # pre-delivery untag clock: start
        to_clear_inelig: list[str] = []   # pre-delivery untag clock: stop

        # One timestamp for the whole cycle. Reading the clock per product would let two
        # products scanned a minute apart resolve the same soak differently.
        now = datetime.now(timezone.utc)

        for sp, product in candidates:
            verdict = qualifies(sp, self.allowlists, DEFAULT_PRICING, taxonomy=taxonomy)
            tagged = is_tagged(sp, platform.trigger_tag)
            parent = sp.parent_sku or ""
            state = state_map.get(parent)
            soak_reached = False
            queued_for_delist = False

            # A tagged product that failed ONLY on stock is not a delist candidate while
            # delist_on_no_inventory is false, and must be treated exactly like a
            # qualifying one: no strike, and any strike it already carries is cleared.
            # Leaving a stale strike would mean a product that sold out, restocked, and
            # sold out again reached the soak threshold across two unrelated events.
            delistable = is_delist_candidate(
                verdict, delist_on_no_inventory=self.delist_on_no_inventory)

            if verdict.qualified and not tagged:
                report.to_tag.append((parent or "?", sp.gid, verdict.reason or ""))
            elif tagged and delistable:
                # Only the delist pass bumps strikes, so the soak keeps counting days
                # rather than five-minute windows. A scan still records the row's status
                # below, it just does not advance the counter.
                if delists:
                    if state is not None:
                        # Predicted, not written. The counter decides an irreversible
                        # action, so it is advanced once at the end of the cycle rather
                        # than per product mid-loop, where a crash left half the catalog
                        # one strike closer to deletion than the other half.
                        strikes = (state.delist_strikes or 0) + 1
                        to_bump.append(parent)
                        soak_reached = strikes >= self.delist_soak_cycles
                        if not soak_reached:
                            report.soaking += 1
                    else:
                        soak_reached = True
                    if soak_reached:
                        queued_for_delist = True
                        report.to_delist.append(
                            (parent or "?", sp.gid,
                             delist_cause(sp, self.allowlists, DEFAULT_PRICING, taxonomy=taxonomy))
                        )
            elif tagged and not delistable:
                if verdict.rejected_by == "no_inventory":
                    report.stock_held += 1
                if state is not None and state.delist_strikes:
                    to_clear.append(parent)

            # Pre-delivery untag, and the clock that drives it. A SEPARATE check rather
            # than another arm of the chain above, which routes on `delistable`: a
            # sold-out product lands in its third branch and a price failure in its
            # second, so one new arm could only ever catch one of the two cases this
            # exists for. Both are in scope here - see pre_delivery_untag_due().
            #
            # The clock is maintained for every TAGGED row, not only the ones awaiting
            # Syncio, so "how long has this been failing" is also readable for products
            # already live. It costs nothing extra: only genuine changes are collected.
            #
            # queued_for_delist excludes anything the branch above already queued. On the
            # DAILY pass both can select the same product - it is tagged, failing, past
            # its soak AND undelivered - and _apply_delists would then run twice on one
            # parent: untag, mark_delisted, then untag again on a product that no longer
            # carries the tag, settling a second time and double-counting the cycle.
            # The delist queue wins because it is the reviewed one.
            if parent and tagged and state is not None and not queued_for_delist:
                if verdict.qualified:
                    if state.ineligible_since is not None:
                        to_clear_inelig.append(parent)
                else:
                    if state.ineligible_since is None:
                        to_start_inelig.append(parent)
                    if pre_delivery_untag_due(
                        verdict=verdict,
                        listed_at=state.listed_at,
                        dest_product_gid=state.dest_product_gid,
                        ineligible_since=state.ineligible_since,
                        now=now,
                        soak_minutes=self.awaiting_sync_untag_soak_minutes,
                    ):
                        report.to_untag_awaiting.append((
                            parent, sp.gid, verdict.rejected_by or "unknown",
                            tuple(s for s in product.variant_skus if s),
                        ))
                    elif is_awaiting_sync(state.listed_at, state.dest_product_gid):
                        report.pre_delivery_soaking += 1

            if not parent:
                continue
            derived = derive_scan_status(
                tagged=tagged, qualified=verdict.qualified,
                rejected_by=verdict.rejected_by, soak_reached=soak_reached,
                current_status=state.current_status if state is not None else None,
            )
            if derived is None:
                continue        # tagged: owned by the tag/normalize/delist paths
            status, reason = derived
            want = (status, reason, len(product.variants))
            if state is None or (
                state.current_status, state.skip_reason, state.variant_count
            ) != want:
                desired_status[parent] = want

        # ---- the ONLY writes in this cycle, all past every gate --------------
        # Product facts first: they are what the Products tab renders, and a status
        # referring to a row whose facts have not landed reads as stale rather than wrong.
        if facts:
            n = await ledger.refresh_product_facts(self.platform_id, facts)
            logger.info("%s: refreshed product facts on %d rows", self.name, n)

        if to_bump or to_clear:
            n = await ledger.apply_delist_strikes(self.platform_id, to_bump, to_clear)
            logger.info("%s: soak counters - %d bumped, %d cleared (%d rows)",
                        self.name, len(to_bump), len(to_clear), n)

        if to_start_inelig or to_clear_inelig:
            n = await ledger.apply_ineligible_since(
                self.platform_id, to_start_inelig, to_clear_inelig)
            logger.info("%s: ineligibility clocks - %d started, %d cleared (%d rows)",
                        self.name, len(to_start_inelig), len(to_clear_inelig), n)

        if desired_status:
            n = await ledger.apply_scan_statuses(self.platform_id, desired_status)
            logger.info("%s: wrote %d status rows", self.name, n)

        # Every parent a scanned product resolved to this cycle. A row holding one of these
        # keys has a living owner and must never be treated as orphaned - see the guard in
        # both blocks below.
        claimed = {sp.parent_sku for sp, _ in candidates if sp.parent_sku}

        if self.reconcile_stock:
            await self._reconcile_stock(report, state_map, claimed, live_stock)

        if reassigned_gids:
            await self._flag_reassigned(report, state_map, claimed, reassigned_gids,
                                        reassigned)

        self._previous_candidate_size = len(qualifying)

        # Syncio capacity, BEFORE the blast-radius caps. Order matters: the gate trims
        # to_tag down to what Syncio can absorb, and the caps must judge what will
        # actually be written rather than the untrimmed plan.
        #
        # Capping first deadlocks the pipeline permanently. Measured on the first real
        # run: 1,085 products qualified and 492 needed tagging, against
        # max_actions_per_cycle=150. The pass aborted on 492 before the gate could cut it
        # to a handful - and would have aborted identically on every subsequent cycle,
        # because nothing gets tagged, so the backlog never shrinks. A backlog larger
        # than one cycle's cap is the NORMAL first-run state, not an anomaly.
        in_flight = await ledger.products_in_flight(self.platform_id)
        capacity = check_syncio_capacity(in_flight, self.max_products_in_flight)
        report.gate_message = capacity.message
        if capacity.blocked:
            logger.info("%s: %s", self.name, capacity.message)
            report.aborted = "syncio-capacity"
            return report

        if report.to_tag:
            sized = [(sku, variant_counts.get(sku, 1)) for sku, _, _ in report.to_tag]
            if self.max_products_in_flight > 0:
                fits, used, held = fit_to_capacity(sized, capacity.remaining)
                if held:
                    keep = set(fits)
                    report.to_tag = [r for r in report.to_tag if r[0] in keep]
                    report.held_back = held
                    logger.info("%s: trimmed to %d products (%d variants); %d held back "
                                "for the next pass", self.name, len(fits), used, held)
            else:
                # Gate disabled (ceiling 0 = submit everything). The trim is skipped, so
                # the variant total has to be summed here or the report claims 0 variants
                # after tagging hundreds.
                used = sum(v for _, v in sized)
            report.variants_submitted = used

        # Pace successive batches. Independent of the ceiling above, which limits the SIZE
        # of one batch and says nothing about how often batches may go out.
        cooldown = check_submit_cooldown(
            in_flight, await ledger.last_submit_at(self.platform_id),
            datetime.now(timezone.utc), self.submit_cooldown_hours,
        )
        if not cooldown.allowed and report.to_tag:
            report.gate_message = cooldown.message
            report.aborted = "submit-cooldown"
            logger.info("%s: %s", self.name, cooldown.message)
            return report

        # Delete caps deliberately NOT applied (2026-07-28, explicit instruction). They
        # existed to catch a broken qualification filter mass-delisting the catalog - the
        # shape of the SPO mapping-wipe incident. What replaces them is review rather than
        # arithmetic: nothing is deleted unless it already sits in pending_delisting,
        # which needs consecutive non-qualifying passes and is listed on the Products tab
        # before anyone presses Delist. The candidate-set guard above still aborts on an
        # incomplete source read, so a partial scan cannot flood that queue in the first
        # place.
        #
        # The pre-delivery untag cap IS applied, and is the exception to the paragraph
        # above for a reason: that path has no pending_delisting queue and no button in
        # front of it, so review cannot be what bounds it. Its trigger set is also every
        # rejection reason, taxonomy gaps included, which means one type mapping deleted
        # from listing options can make a whole product type fail qualification at once.
        footprint = await ledger.footprint(self.platform_id)
        breach = check_caps(
            action_count=len(report.to_tag),
            delete_count=0, sold_out_delete_count=0,
            variant_writes=0, units_zeroed=0,
            footprint=footprint, caps=self.caps,
            pre_delivery_untag_count=len(report.to_untag_awaiting),
        )
        if breach:
            logger.error("%s: CAP BREACH, zero writes: %s", self.name, breach)
            report.aborted = breach
            return report

        # auto_submit and auto_delist are INDEPENDENT. They did not used to be: this
        # returned early on `not auto_submit`, before the delist branch below, so the
        # daily pass never reached it and auto_delist did nothing whatever it was set to.
        # Nothing announced that - the pass logged "planning only" and looked correct.
        allowed = plan_scheduled_actions(
            auto_submit=self.auto_submit, auto_delist=self.auto_delist,
            execute_deletes=self.execute_deletes, delists=delists,
            auto_untag_awaiting_sync=self.auto_untag_awaiting_sync,
        )
        may_tag, may_delist = allowed.tag, allowed.delist

        if not allowed.any:
            logger.info("%s: auto_submit=%s auto_delist=%s execute_deletes=%s "
                        "auto_untag_awaiting_sync=%s - planning only, the buttons execute. "
                        "%s", self.name, self.auto_submit, self.auto_delist,
                        self.execute_deletes, self.auto_untag_awaiting_sync,
                        report.summary())
            return report

        # `execute` gates every write from this pass, scheduled or not.
        if not self.execute:
            logger.info("%s: dry-run, %s", self.name, report.summary())
            return report

        if not writes_enabled():
            enable_writes(f"{self.name} execute=true")

        if may_tag:
            await self._apply_tags(platform, admin, report)
        elif report.to_tag:
            logger.info("%s: %d products qualify; the Submit button tags them "
                        "(auto_submit=false)", self.name, len(report.to_tag))

        if may_delist:
            await self._apply_delists(platform, admin, report, report.to_delist)
        elif report.to_delist:
            logger.info("%s: %d products queued as pending_delisting; the Delist button "
                        "executes them (auto_delist=%s, execute_deletes=%s)",
                        self.name, len(report.to_delist),
                        self.auto_delist, self.execute_deletes)

        if allowed.untag_awaiting and report.to_untag_awaiting:
            confirmed = await self._confirm_undelivered(
                platform, report, report.to_untag_awaiting)
            if confirmed:
                await self._apply_delists(platform, admin, report, confirmed,
                                          pre_delivery=True)
        elif report.to_untag_awaiting:
            logger.info("%s: %d products awaiting Syncio no longer qualify; not untagging "
                        "(auto_untag_awaiting_sync=false)",
                        self.name, len(report.to_untag_awaiting))

        logger.info("%s: %s pass done, %s",
                    self.name, "delist" if delists else "scan", report.summary())
        return report

    # -- reconciliation ----------------------------------------------------

    async def _reconcile_stock(self, report: SourceReport, state_map: dict[str, Any],
                               claimed: set[str], live_stock: dict[str, int]) -> None:
        """Correct stored stock on rows this scan could not key.

        The scan writes by RESOLVED parent, so a row whose key stopped resolving - after a
        merge, a rename, a child_products cleanup - is no longer a possible write target and
        freezes at whatever it held that day. Nothing sweeps for it: the design note above
        check_candidate_set explains why absence from a scan is never signal, which is right
        for a truncated page and wrong for a key that will never resolve again.

        So this is the one place absence IS signal, and it is scoped as narrowly as the
        problem allows:

          - only rows NOT claimed by a scanned product this cycle (rows the scan just wrote
            are correct by construction)
          - a SKU still on Shopify takes its live quantity, whichever product now carries it
          - a SKU absent from a COMPLETE catalog sweep is set to 0
          - stock columns ONLY. current_status, skip_reason and dest_product_gid are not
            touched, so a bad read cannot cascade into the delete queue

        Reached only on a cycle that completed: the ShopifyError and ShopifyScopeError paths
        in _cycle return before this. Runs regardless of `execute`, matching
        refresh_product_facts - `execute` gates STOREFRONT writes, not state corrections.
        """
        updates: dict[str, tuple[int, list[dict]]] = {}
        rows_with_missing_skus = 0

        for parent_sku, row in state_map.items():
            if parent_sku in claimed:
                continue
            stored = row.variants or []
            if not stored:
                continue

            corrected: list[dict] = []
            changed = False
            saw_missing = False
            for variant in stored:
                sku = variant.get("sku")
                live = live_stock.get(sku) if sku else None
                if live is None:
                    live = 0
                    if sku:
                        saw_missing = True
                if variant.get("inventory") != live:
                    changed = True
                corrected.append({**variant, "inventory": live})

            total = sum(v["inventory"] for v in corrected)
            if not changed and row.inventory == total:
                continue
            if saw_missing:
                rows_with_missing_skus += 1
            updates[parent_sku] = (total, corrected)

        if not updates:
            return

        breach = check_reconcile_cap(rows_with_missing_skus, self.caps)
        if breach:
            # Deliberately ALL-OR-NOTHING. Zeroing the first 50 and stopping would leave the
            # catalog half-corrected on exactly the cycle we decided not to trust.
            logger.error("%s: RECONCILE CAP BREACH, zero stock writes: %s (%d rows wanted "
                         "correction)", self.name, breach, len(updates))
            report.gate_message = breach
            return

        report.reconciled = await ledger.reconcile_stock(self.platform_id, updates)
        report.zeroed = rows_with_missing_skus
        logger.info("%s: reconciled stock on %d row(s); %d had SKUs absent from Shopify "
                    "and were zeroed", self.name, report.reconciled, report.zeroed)

    async def _flag_reassigned(self, report: SourceReport, state_map: dict[str, Any],
                               claimed: set[str], reassigned_gids: dict[str, list[str]],
                               reassigned: dict[str, str]) -> None:
        """Mark ORPHANED rows of reassigned products, and queue them only if configured.

        The claimed-key guard is the whole safety of this method. A reassigned product's gid
        can appear on two rows - the row it wrongly took over, and the row it left behind -
        and state_map was loaded at the START of the cycle, before any healing write. Queuing
        every row on that gid would therefore queue the SURVIVOR.

        Concretely, before this change RHD-MOTW-0038 and RHD-MOTW-0040 both carried gid
        10142326358316. RHD-MOTW-0038 is a live BLACK AMARINO with 4 units on 1nventory and 3
        on Shop The Sample; its rightful source product (10104636113196) reclaims the key on
        this very cycle. Queueing it would delist a selling product. So: a row is queued only
        when nothing claims its parent_sku.
        """
        by_gid: dict[str, list[Any]] = {}
        for row in state_map.values():
            if row.source_product_gid:
                by_gid.setdefault(row.source_product_gid, []).append(row)

        items: dict[str, str] = {}
        for gid, moved in reassigned_gids.items():
            for row in by_gid.get(gid, []):
                if row.parent_sku in claimed:
                    # Reclaimed this cycle by its rightful product. Not an orphan.
                    continue
                detail = "; ".join(
                    f"{sku} -> {reassigned.get(sku, '?')}" for sku in moved[:5]
                )
                if len(moved) > 5:
                    detail += f" (+{len(moved) - 5} more)"
                items[row.parent_sku] = f"reassigned: {detail}"

        if not items:
            return

        n = await ledger.flag_reassigned(self.platform_id, items,
                                         queue=self.queue_reassigned_delists)
        if self.queue_reassigned_delists:
            report.queued_reassigned = n
            logger.info("%s: queued %d orphaned row(s) as pending_delisting/reassigned_sku: "
                        "%s", self.name, n, sorted(items)[:10])
        else:
            logger.info("%s: flagged %d orphaned row(s) as reassigned_sku and kept them out "
                        "of tagging; queue_reassigned_delists=false so current_status was "
                        "not changed: %s", self.name, n, sorted(items)[:10])

    async def _apply_tags(self, platform: Any, admin: ShopifyAdmin,
                          report: SourceReport) -> None:
        for parent_sku, gid, why in report.to_tag:
            state = await ledger.claim(self.platform_id, parent_sku, Act.LIST)
            if state is None:
                continue
            row = await ledger.record(
                platform_id=self.platform_id, parent_sku=parent_sku, action=Act.LIST,
                status=St.PENDING, source_gid=gid,
                payload={"tag": platform.trigger_tag, "why": why},
            )
            try:
                await admin.add_tags(gid, [platform.trigger_tag])
                await ledger.finish(row, status=St.SUCCESS, result={"ok": True})
                await ledger.mark_listed(state, gid)
                report.tagged += 1
            except ShopifyError as exc:
                await ledger.finish(row, status=St.FAILED, error=str(exc))
                report.failed += 1
            finally:
                await ledger.release(state)

    async def _confirm_undelivered(
        self, platform: Any, report: SourceReport,
        items: list[tuple[str, str, str, tuple[str, ...]]],
    ) -> list[tuple[str, str, str]]:
        """Drop anything Syncio has already delivered. Returns what is safe to untag.

        THE hazard on this path. `dest_product_gid IS NULL` means "we have not SEEN it on
        Shop The Sample", not "it is not there": the destination poller runs on its own
        five-minute cycle behind a watermark, so Syncio can have created the product
        minutes before this scan decided to untag it.

        Untagging then is not merely premature, it is unrecoverable in one specific way.
        The destination poller only ever finds products through
        products_by_tag(trigger_tag). If the trigger tag does not survive on the delivered
        product, nothing in this pipeline will ever look at it again: it stays on STS
        forever, uncorrected, with no state row claiming it and no sweep that can see it.

        So ask the destination directly, by variant SKU. find_product_by_variant_sku is
        the same identity check used before a create - exact, and unaffected by retitling.

        Fails CLOSED. An unreadable destination reads as "cannot confirm this is
        undelivered", never as "it is not there"; the candidate simply waits for the next
        cycle, and its soak clock keeps running underneath it.
        """
        dest_client = await get_shopify_client(platform.dest_store)
        dest_admin = ShopifyAdmin(dest_client)

        confirmed: list[tuple[str, str, str]] = []
        for parent_sku, source_gid, cause, skus in items:
            if not skus:
                logger.warning("%s: %s has no variant SKUs to check against the "
                               "destination; refusing to untag", self.name, parent_sku)
                continue
            try:
                # Three is enough. The check asks "does ANY variant of this product exist
                # on the destination", and Syncio delivers a product whole, so a product
                # present under none of its first three SKUs is not present.
                node = await dest_admin.find_product_by_variant_sku(skus[:3])
            except Exception as exc:                                     # noqa: BLE001
                logger.error("%s: destination lookup failed for %s, refusing to untag: %s",
                             self.name, parent_sku, exc)
                continue
            if node is None:
                confirmed.append((parent_sku, source_gid, cause))
                continue

            # Delivered after all. Record the GID so the row stops satisfying
            # is_awaiting_sync(): this is now an ordinary live product, and removing it
            # has to go through the soak, the ownership guards and the Delist button like
            # any other. Nothing is untagged here.
            await ledger.adopt_dest_gid(self.platform_id, parent_sku, node["id"])
            report.pre_delivery_arrived += 1
            logger.info("%s: %s is already on the destination as %s; adopted it and left "
                        "the source tagged", self.name, parent_sku, node["id"])
        return confirmed

    async def _apply_delists(self, platform: Any, admin: ShopifyAdmin,
                             report: SourceReport,
                             items: list[tuple[str, str, str]],
                             *, pre_delivery: bool = False) -> None:
        """Untag source, confirm, then delete destination. Ordering is mandatory.

        Deleting while the source is still tagged causes Syncio to recreate the
        product, which is verified behaviour, not a theory.

        `items` is (parent_sku, source_gid, cause). Passed in rather than read off the
        report so the manual path can supply the pending_delisting set from the database
        while the scheduled path supplies what it just computed - one implementation of
        the ordering either way.

        `pre_delivery` marks the third caller: the scan pass untagging a product Syncio
        has not delivered. Reused rather than given its own routine on purpose - the
        claim/record/untag/re-read/settle sequence is the part that must not be
        reimplemented, and reuse is also what makes the delivery race safe, because a
        product that arrived between _confirm_undelivered and this claim is caught by the
        re-read below instead of being untagged on a stale reading.
        """
        dest_client = await get_shopify_client(platform.dest_store)
        dest_admin = ShopifyAdmin(dest_client)

        for parent_sku, source_gid, cause in items:
            state = await ledger.claim(self.platform_id, parent_sku, Act.UNTAG)
            if state is None:
                continue
            try:
                if pre_delivery and state.dest_product_gid:
                    # claim() re-reads the row, so this is fresher than the destination
                    # check that queued this item. Syncio delivered in between: not our
                    # case any more. Release it untouched and let the reviewed delist path
                    # have it. This is what keeps auto_untag_awaiting_sync's promise that
                    # it can never reach a productDelete - the flag authorises one
                    # tagsRemove against a product with no destination product, and the
                    # moment that stops being true it stops acting.
                    logger.info("%s: %s was delivered between the destination check and "
                                "the untag; leaving it to the delist path", self.name,
                                parent_sku)
                    report.pre_delivery_arrived += 1
                    continue
                untag_row = await ledger.record(
                    platform_id=self.platform_id, parent_sku=parent_sku,
                    action=Act.UNTAG, status=St.PENDING, source_gid=source_gid,
                    payload={"tag": platform.trigger_tag, "cause": cause,
                             "pre_delivery": pre_delivery},
                )
                await admin.remove_tags(source_gid, [platform.trigger_tag])

                # Re-read and assert. Do not assume the untag landed.
                fresh = await admin.get_product(source_gid)
                if fresh and any(t.upper() == platform.trigger_tag.upper()
                                 for t in fresh.tags):
                    await ledger.finish(untag_row, status=St.FAILED,
                                        error="tag still present after removal")
                    report.failed += 1
                    continue
                await ledger.finish(untag_row, status=St.SUCCESS, result={"ok": True})
                report.untagged += 1

                # From here the source is untagged and that cannot be walked back, so
                # every remaining exit MUST settle the row. Three of them used to fall
                # through to `continue` with no mark_delisted and no audit row, which
                # left the product untagged, the row stuck at pending_delisting, and
                # report.deleted at 0 - a delist that half-happened looked like one that
                # never started.
                dest_gid = state.dest_product_gid
                if not dest_gid:
                    # Never delivered by Syncio. Untagging IS the whole delist.
                    await ledger.mark_delisted(state)
                    # Counted apart on the pre-delivery path. No productDelete happened
                    # and none was possible, so reporting it as a deletion would make the
                    # reversible action indistinguishable from the irreversible one in
                    # every log line and every cycle count.
                    if pre_delivery:
                        report.pre_delivery_untagged += 1
                    else:
                        report.deleted += 1
                    continue

                dest_product = await dest_admin.get_product(dest_gid)
                if dest_product is None:
                    # Already gone - deleted by hand, removed in Syncio's own UI, or
                    # cleaned up by an earlier run of this method.
                    #
                    # NOT because untagging the source causes Syncio to remove it. Five
                    # products vanished after an untag on 2026-07-29 and that inference
                    # was recorded here; it was wrong, and a human had deleted them.
                    # Assume the opposite: the destination product SURVIVES an untag, so
                    # the productDelete below is the only thing that removes it and this
                    # branch is a genuine edge case rather than the normal path.
                    #
                    # Still recorded as success: the outcome we wanted is the state we
                    # are in, and the alternative was dropping the row on the floor.
                    await ledger.record(
                        platform_id=self.platform_id, parent_sku=parent_sku,
                        action=Act.DELETE, status=St.SUCCESS,
                        source_gid=source_gid, dest_gid=dest_gid,
                        payload={"cause": cause},
                        result={"already_absent": True,
                                "note": "destination product did not exist when we came "
                                        "to delete it; removed by something outside this "
                                        "pipeline"},
                    )
                    await ledger.mark_delisted(state)
                    report.deleted += 1
                    continue

                if not await self._delete_guards_pass(platform, dest_product, state):
                    # The ONE case that must not be marked delisted: a destination
                    # product that still exists but no longer looks like ours. The source
                    # is untagged, so this is a genuine orphan and needs a human. A FAILED
                    # delete row is written so ledger.orphaned_delists() can find it -
                    # previously this exited with no row at all, making the orphan sweep
                    # structurally incapable of seeing the orphans it exists to find.
                    detail = (f"delete guards failed on {dest_gid}; source is untagged "
                              f"but the destination product was left in place")
                    logger.error("%s: %s (%s)", self.name, detail, parent_sku)
                    await ledger.record(
                        platform_id=self.platform_id, parent_sku=parent_sku,
                        action=Act.DELETE, status=St.FAILED,
                        source_gid=source_gid, dest_gid=dest_gid,
                        payload={"cause": cause}, error=detail,
                    )
                    state.last_error = detail[:2000]
                    await state.save(update_fields=["last_error", "updated_at"])
                    report.failed += 1
                    continue

                # Pre-image committed BEFORE the mutation. This is the only
                # reconstruction material a delete will ever have.
                del_row = await ledger.record_pre_image(
                    platform_id=self.platform_id, parent_sku=parent_sku,
                    action=Act.DELETE, dest_gid=dest_gid, source_gid=source_gid,
                    before={
                        "title": dest_product.title,
                        "vendor": dest_product.vendor,
                        "product_type": dest_product.product_type,
                        "status": dest_product.status,
                        "tags": list(dest_product.tags),
                        "syncio_source_gid": dest_product.syncio_source_gid,
                        "variants": [
                            {"gid": v.gid, "sku": v.sku, "price": str(v.price),
                             "compare_at": str(v.compare_at),
                             "inventory": v.inventory_quantity}
                            for v in dest_product.variants
                        ],
                        "cause": cause,
                    },
                )
                await dest_admin.delete_product(dest_gid)
                await ledger.finish(del_row, status=St.SUCCESS, result={"deleted": True})
                await ledger.mark_delisted(state)
                report.deleted += 1
            except ShopifyError as exc:
                report.failed += 1
                logger.warning("%s: delist failed for %s: %s", self.name, parent_sku, exc)
            finally:
                await ledger.release(state)

    async def _delete_guards_pass(self, platform: Any, product: Product,
                                  state: Any) -> bool:
        """All must hold. Nine checks for one irreversible action is proportionate.

        Async now: ownership resolves the parent through the products DB rather than by
        splitting the SKU string. Resolved per product rather than per cycle because the
        destination product is only fetched inside the delist loop - two small queries
        against a path that already makes several Shopify round trips per delete, and
        deletes are a daily batch rather than a hot loop.
        """
        from services.internal_platform_products import (
            load_reassigned,
            resolve_registered_parents,
        )
        from services.internal_platform_rules import assess_ownership

        skus = {s for s in product.variant_skus if s}
        try:
            registered = await resolve_registered_parents(skus)
            reassigned = await load_reassigned(skus)
        except Exception as exc:                                     # noqa: BLE001
            # Fail CLOSED. This gates a delete, so an unreadable catalog must read as
            # "cannot confirm this is ours", never as "nothing is reassigned".
            logger.error("%s: parent resolution failed for %s, refusing delete: %s",
                         self.name, state.parent_sku, exc)
            return False

        own = assess_ownership(product.variant_skus, product.tags,
                               platform.trigger_tag, product.syncio_source_gid,
                               registered, reassigned)
        # The destination product should still look like ours, and its Syncio pairing
        # must still point where the ledger says it does.
        if product.syncio_source_gid != state.source_product_gid:
            return False
        if not product.syncio_source_gid:
            return False
        if any(s and s.strip().lower().startswith("i") and s.strip()[1:].isdigit()
               for s in product.variant_skus):
            return False
        return own.parent_sku == state.parent_sku or own.ours


internal_platform_source_poller = InternalPlatformSourcePoller()
