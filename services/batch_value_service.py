"""What a batch is worth, and when that gets recomputed.

Two things live here: the arithmetic (read live from SellerCloud, used by
BatchService._snapshot_value when a batch is created) and the nightly poller at the bottom
of the file that re-prices the products in open batches which have not been submitted yet.
A product's value freezes while its listing is submitted; everything still pending tracks
current stock and price. "While", not "on the day": recompute_listing_submitted() clears
listings.submitted again if a platform later reports a failure, so a product can re-enter
the pending set and pick up a later day's price. That is the intended behaviour - an
unfinished listing is worth what it is worth now.


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

import hashlib
import logging
import struct
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from tortoise import connections

from config import config
from models.db_models import Batch
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


# One grid call's worth of SKUs. Matches CATALOG_GRID_PAGE_SIZE, and is also the unit of
# quarantine below: a bad read is contained to the parents whose children were in it.
GRID_CHUNK = 200

# Below this share of a chunk's SKUs coming back, the chunk is treated as a failed read
# rather than as news about stock. Production returns 100% of requested rows (measured
# across 13.5k SKUs), and the missing-SKU case is separately visible as children > priced,
# so half is a floor that only a broken response can cross.
MIN_CHUNK_COVERAGE = 0.5

# Consecutive failed chunks that mean the problem is SellerCloud, not the data.
MAX_CONSECUTIVE_CHUNK_FAILURES = 3


class GridReadFailure(Exception):
    """The SellerCloud grid did not return enough to trust anything computed from it."""


@dataclass(frozen=True)
class Valuation:
    """Result of valuing several parent sets at once.

    `dropped` is the part that matters: parents whose price could not be read reliably and
    which must therefore keep whatever value they already had. They are NOT valued at 0 -
    see the quarantine rationale in _fetch_grid_rows.
    """

    values: Dict[Any, Tuple[Decimal, Dict[str, Dict[str, Any]]]] = field(default_factory=dict)
    dropped: Dict[Any, List[str]] = field(default_factory=dict)
    requested: int = 0
    returned: int = 0
    chunks: int = 0
    quarantined: int = 0


async def _fetch_grid_rows(
    children: List[str],
    chunk_size: int = GRID_CHUNK,
    min_chunk_coverage: float = MIN_CHUNK_COVERAGE,
    max_consecutive_failures: int = MAX_CONSECUTIVE_CHUNK_FAILURES,
) -> Tuple[Dict[str, Dict[str, Any]], Set[str], int, int]:
    """(grid rows, untrusted SKUs, chunks tried, chunks quarantined). One chunk at a time.

    The reason this exists rather than a bare get_catalog_grid_rows call:
    **a failed SellerCloud query is indistinguishable from an empty one.**
    sellercloud_internal_service.post() returns the parsed body for any 2xx without
    inspecting `Success`, so a soft failure arrives as `Data.Grid = []`, the paging loop
    breaks, and the SKUs are simply absent from the result. aggregate_values then reads
    "no row" as qty 0 x price 0 and prices the product at nothing.

    For a single batch at creation that is one recoverable wrong number. Run nightly over
    every open batch it would zero the entire work queue in one cycle and stamp
    value_computed_at over the real figures, with nothing left to restore them from. So a
    chunk that comes back empty (or nearly so) is quarantined instead of believed, and
    every parent with a child in it keeps yesterday's value.
    """
    out: Dict[str, Dict[str, Any]] = {}
    unfetched: Set[str] = set()
    unique = list(dict.fromkeys(sku for sku in children if sku))
    chunks = quarantined = consecutive = 0

    for start in range(0, len(unique), chunk_size):
        chunk = unique[start : start + chunk_size]
        chunks += 1
        try:
            rows = await sellercloud_internal_service.get_catalog_grid_rows(chunk)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "Catalog grid chunk %d (%d SKUs) failed, quarantining it: %s",
                chunks, len(chunk), e,
            )
            rows = {}

        if len(rows) < len(chunk) * min_chunk_coverage:
            logger.warning(
                "Catalog grid chunk %d returned %d/%d SKUs, below the %.0f%% floor; "
                "treating it as a failed read rather than as zero stock",
                chunks, len(rows), len(chunk), min_chunk_coverage * 100,
            )
            unfetched.update(chunk)
            quarantined += 1
            consecutive += 1
            if consecutive >= max_consecutive_failures:
                raise GridReadFailure(
                    f"{consecutive} consecutive catalog grid chunks came back empty or "
                    f"near-empty; SellerCloud is not answering usefully"
                )
            continue

        consecutive = 0
        out.update(rows)

    return out, unfetched, chunks, quarantined


async def compute_values_for_sets(
    parent_sets: Dict[Any, List[str]],
    chunk_size: int = GRID_CHUNK,
) -> Valuation:
    """Value several parent sets against ONE children query and ONE grid pass.

    A parent appearing in several sets - the same product sitting in two open batches - is
    looked up and priced once. backfill_batch_values.py already chunks across batch
    boundaries for that reason; the nightly refresh needs the same trick, so it lives here
    instead of being written a third time.

    A parent with any child in a quarantined chunk is left out of its set's breakdown and
    reported in `dropped`, because a product nobody could read a price for is not a product
    worth nothing.
    """
    parents = sorted({p for skus in parent_sets.values() for p in skus if p})
    if not parents:
        return Valuation(values={key: (Decimal(0), {}) for key in parent_sets})

    children_by_parent = await _active_children_by_parent(parents)
    all_children = [sku for skus in children_by_parent.values() for sku in skus]

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

    grid_rows, unfetched, chunks, quarantined = await _fetch_grid_rows(
        all_children, chunk_size=chunk_size
    )

    values: Dict[Any, Tuple[Decimal, Dict[str, Dict[str, Any]]]] = {}
    dropped: Dict[Any, List[str]] = {}
    for key, skus in parent_sets.items():
        scoped: Dict[str, List[str]] = {}
        skipped: List[str] = []
        for parent in (p for p in skus if p):
            kids = children_by_parent.get(parent, [])
            if unfetched.intersection(kids):
                skipped.append(parent)
            else:
                scoped[parent] = kids
        values[key] = aggregate_values(scoped, grid_rows)
        if skipped:
            dropped[key] = sorted(skipped)

    return Valuation(
        values=values,
        dropped=dropped,
        requested=len(set(all_children)),
        returned=len(grid_rows),
        chunks=chunks,
        quarantined=quarantined,
    )


async def compute_product_values(
    parent_skus: Iterable[str],
) -> Tuple[Decimal, Dict[str, Dict[str, Any]]]:
    """(total_value, per-product breakdown) for a set of parent SKUs.

    The single-set form of compute_values_for_sets, kept because batch creation values
    exactly one batch. Routing it through the bulk function means the creation snapshot and
    the nightly refresh cannot drift on how a product is priced.

    Raises rather than returning a partial answer if any product's price could not be read:
    _snapshot_value catches it and leaves value_computed_at NULL, which is what its
    docstring has always promised and what the backfill selects on. Writing a zero instead
    would look exactly like a batch of worthless stock.
    """
    valuation = await compute_values_for_sets({None: list(parent_skus)})
    if valuation.dropped.get(None):
        raise GridReadFailure(
            f"{len(valuation.dropped[None])} product(s) had no trustworthy grid data: "
            f"{', '.join(valuation.dropped[None][:5])}"
        )
    return valuation.values[None]


def is_suspicious_zero(
    previous: Optional[Dict[str, Any]],
    refreshed: Dict[str, Any],
) -> bool:
    """True when a refresh zeroes a product in the shape a bad read produces, not a sale.

    `children > 0, priced == 0` on a product that HAD a price yesterday means SellerCloud
    returned rows carrying no SitePrice. Genuine sell-through looks different: qty falls to
    0 while priced stays > 0, because the price is still on the product. Distinguishing the
    two is worth the extra condition - one is news, the other is a bad night at SellerCloud.
    """
    if not isinstance(previous, dict) or not isinstance(refreshed, dict):
        return False
    if refreshed.get("children") and not refreshed.get("priced"):
        return bool(previous.get("priced"))
    return False


def merge_product_values(
    existing: Optional[Dict[str, Any]],
    refreshed: Dict[str, Dict[str, Any]],
    keep: Iterable[str],
) -> Tuple[Decimal, Dict[str, Dict[str, Any]], List[str]]:
    """Overlay refreshed entries onto frozen ones. Pure, no I/O.

    Returns (total_value, merged breakdown, dropped keys).

    This is the freeze rule, and it is a pure function so it can be tested without a
    database or SellerCloud: an entry the caller re-priced wins, every other entry survives
    exactly as it was, and the total is the sum over what is left - never a number carried
    over from a previous run.

    `keep` is the batch's current parent SKUs. An entry outside it belongs to a listing
    that was deleted or moved to another batch, and because the total is now a sum rather
    than a value written whole, leaving it in place would inflate the batch forever. Those
    keys are returned rather than logged here so this stays pure.

    A missing or non-numeric `value` contributes 0 and keeps its place, mirroring the
    jsonb_typeof guard in product_queue_service: one junk entry must not raise and take a
    whole nightly cycle down with it.
    """
    if existing is not None and not isinstance(existing, dict):
        # Never coerce. A JSON *string* here - the shape a raw asyncpg read hands back for
        # jsonb - would quietly become {} and wipe every frozen entry on the batch. The
        # caller isolates this per batch, so raising costs one batch, not the cycle.
        raise TypeError(
            f"product_values must be a mapping, got {type(existing).__name__}"
        )

    keep_set = {sku for sku in keep if sku}
    merged: Dict[str, Dict[str, Any]] = {}
    dropped: List[str] = []

    for sku, entry in (existing or {}).items():
        if sku in keep_set:
            merged[sku] = entry
        else:
            dropped.append(sku)

    merged.update(refreshed)

    total = Decimal(0)
    for entry in merged.values():
        if isinstance(entry, dict):
            total += _to_decimal(entry.get("value"))

    # Sorted so a stored breakdown reads the same way aggregate_values writes one, and so
    # two runs that changed nothing produce an identical jsonb.
    merged = {sku: merged[sku] for sku in sorted(merged)}
    return total.quantize(_CENTS, rounding=ROUND_HALF_UP), merged, sorted(dropped)


# ---------------------------------------------------------------------------
# The nightly refresh
# ---------------------------------------------------------------------------

# One worker at a time across every process. Today's deploy is a single uvicorn unit, so
# this is insurance against a second instance (or a restart straddling the fire time)
# valuing the same batches twice, not a requirement.
_CYCLE_LOCK_KEY = struct.unpack(
    ">q", hashlib.sha256(b"batch_value_refresh_poller").digest()[:8]
)[0]


class BatchValueRefreshPoller:
    """Re-prices the products in open batches that have not been submitted yet.

    One cycle a day at a fixed wall-clock time, via APScheduler's AsyncIOScheduler and a
    CronTrigger, so DST transitions and missed fires are the library's problem - the same
    shape as DailyImageImportPoller and SecondaryInventoryTransferPoller.

    Scheduled after the 04:00 inventory transfer and the 05:00 delist pass, both of which
    move the quantities being read here.

    A cycle never writes half a batch's value: the SellerCloud read is all or nothing (see
    get_catalog_grid_rows, which has no rate limiter and no 429 handling on purpose), so an
    outage leaves every batch exactly as it was and the next night retries. Only the
    per-batch writes are individually guarded, and there is deliberately no transaction
    around the run - an interrupted cycle keeps what it already refreshed, the same
    property backfill_batch_values.py relies on.
    """

    def __init__(self) -> None:
        self.name = self.__class__.__name__
        cfg = config.get("batch_value_refresh_poller", {})
        # Defaults OFF, unlike the other daily pollers which default enabled=True.
        # Production's config is injected from a CI secret, and a section missing there
        # must not silently start a job that reads SellerCloud every night.
        self.enabled: bool = bool(cfg.get("enabled", False))
        self._schedule_hour: int = int(cfg.get("daily_hour", 6))
        self._schedule_minute: int = int(cfg.get("daily_minute", 0))
        self._schedule_tz: ZoneInfo = ZoneInfo(cfg.get("timezone", "America/New_York"))
        # 0 = no cap. At ~76 open batches a cycle is a handful of grid calls, so this is a
        # brake for an unexpected growth spurt, not a routine setting.
        self.max_batches_per_cycle: int = int(cfg.get("max_batches_per_cycle", 0))
        # A tripwire, not a cap: the pending set is bounded by how fast operators create
        # batches, so a sudden jump means something unflagged listings.submitted.
        self.warn_batches_per_cycle: int = int(cfg.get("warn_batches_per_cycle", 300))
        self._scheduler: Optional[AsyncIOScheduler] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        if not self.enabled:
            logger.info(f"{self.name}: disabled in config, skipping start")
            return
        if self._scheduler and self._scheduler.running:
            logger.info(f"{self.name}: already running")
            return

        self._scheduler = AsyncIOScheduler(timezone=self._schedule_tz)
        trigger = CronTrigger(
            hour=self._schedule_hour,
            minute=self._schedule_minute,
            timezone=self._schedule_tz,
        )
        self._scheduler.add_job(
            self._poll_cycle,
            trigger=trigger,
            id="batch_value_refresh_daily",
            name=self.name,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
        self._scheduler.start()
        next_run = self._scheduler.get_job("batch_value_refresh_daily").next_run_time
        logger.info(
            f"{self.name}: scheduled daily at "
            f"{self._schedule_hour:02d}:{self._schedule_minute:02d} "
            f"{self._schedule_tz.key}; next run at {next_run.isoformat(timespec='seconds')}"
        )

    async def stop(self) -> None:
        if not self._scheduler:
            return
        logger.info(f"{self.name}: stopping...")
        self._scheduler.shutdown(wait=False)
        self._scheduler = None
        logger.info(f"{self.name}: stopped")

    async def run_once(self, dry_run: bool = False) -> dict:
        """Trigger a cycle manually. dry_run reports what would change and writes nothing."""
        return await self._poll_cycle(dry_run=dry_run)

    # ------------------------------------------------------------------
    # Main cycle
    # ------------------------------------------------------------------

    async def _poll_cycle(self, dry_run: bool = False) -> dict:
        pool = connections.get("default")._pool
        async with pool.acquire() as raw_conn:
            if not await raw_conn.fetchval("SELECT pg_try_advisory_lock($1)", _CYCLE_LOCK_KEY):
                logger.info(f"{self.name}: another worker holds the cycle lock, skipping")
                return {"skipped": "locked"}
            try:
                return await self._run_cycle(dry_run)
            finally:
                try:
                    await raw_conn.fetchval(
                        "SELECT pg_advisory_unlock($1)", _CYCLE_LOCK_KEY
                    )
                except Exception as unlock_err:  # noqa: BLE001
                    logger.warning(f"{self.name}: failed to release cycle lock: {unlock_err}")

    async def _run_cycle(self, dry_run: bool) -> dict:
        # Deferred: product_queue_service imports batch_service, which imports this module,
        # so a top-level import here is circular and breaks app startup. Same reason as the
        # deferred imports in batch_service._enqueue_verification. Do not hoist it.
        from services import product_queue_service
        from services.batch_service import OPEN_BATCH_STATUSES

        started = time.monotonic()
        targets = await product_queue_service.get_refresh_targets()
        if self.max_batches_per_cycle:
            targets = targets[: self.max_batches_per_cycle]
        if not targets:
            return {"batches": 0, "products": 0, "updated": 0, "failed": 0}

        batch_ids = [t["batch_id"] for t in targets]
        if len(batch_ids) > self.warn_batches_per_cycle:
            # Not a cap - a tripwire. The pending set is bounded by how fast operators
            # create batches, so a jump this size means something reset listings.submitted.
            logger.warning(
                f"{self.name}: {len(batch_ids)} open batches with pending work, over the "
                f"expected {self.warn_batches_per_cycle}; check nothing has unflagged "
                f"submitted listings"
            )

        all_parents = await product_queue_service.get_all_parents(batch_ids)

        parent_sets: Dict[Any, List[str]] = {}
        never_valued: Set[int] = set()
        for target in targets:
            batch_id = target["batch_id"]
            if target["value_computed_at"] is None:
                # Nothing frozen worth preserving - its creation snapshot never ran. Value
                # it whole: a pending-only breakdown would stamp value_computed_at and hide
                # the batch from backfill_batch_values.py's unvalued sweep forever, leaving
                # its submitted products at no value for good.
                parent_sets[batch_id] = all_parents.get(batch_id) or target["parents"]
                never_valued.add(batch_id)
            else:
                parent_sets[batch_id] = target["parents"]

        valuation = await compute_values_for_sets(parent_sets)

        now = datetime.now(timezone.utc)
        # values(), not model instances: nothing here should be in a position to call
        # save() on a batch row the update_batch_counts() trigger owns.
        current = {
            row["id"]: row
            for row in await Batch.filter(id__in=batch_ids).values(
                "id", "total_value", "product_values"
            )
        }

        updated = failed = skipped = suppressed = 0
        before_total = after_total = Decimal(0)

        for batch_id, (_scoped_total, refreshed) in valuation.values.items():
            row = current.get(batch_id)
            if row is None:
                continue
            try:
                existing = row["product_values"] or {}
                dropped_by_grid = valuation.dropped.get(batch_id, [])

                # A zero that looks like a failed read rather than a sale keeps yesterday's
                # number, so one bad night at SellerCloud cannot empty the work queue.
                held: List[str] = []
                for sku, entry in list(refreshed.items()):
                    if is_suspicious_zero(existing.get(sku), entry):
                        refreshed.pop(sku)
                        held.append(sku)
                if held:
                    suppressed += len(held)
                    logger.warning(
                        f"{self.name}: batch {batch_id} kept the previous value for "
                        f"{len(held)} product(s) that came back priced 0 after being "
                        f"priced before: {', '.join(sorted(held)[:5])}"
                    )

                if not refreshed and (dropped_by_grid or held):
                    # Nothing on this batch could be priced. Writing here would restamp
                    # value_computed_at on numbers that were not recomputed, and the
                    # staleness monitor would stop being able to see the problem.
                    skipped += 1
                    logger.warning(
                        f"{self.name}: batch {batch_id} had no product it could price; "
                        f"left untouched"
                    )
                    continue

                if batch_id in never_valued and (dropped_by_grid or held):
                    # This batch has no baseline, so a partial breakdown here would be
                    # written AND stamped, and "never valued" would stop being true of a
                    # batch that still is. Leave it entirely for tomorrow.
                    skipped += 1
                    logger.warning(
                        f"{self.name}: batch {batch_id} has never been valued and "
                        f"{len(dropped_by_grid) + len(held)} of its products could not be "
                        f"priced; leaving it uncomputed rather than writing a partial value"
                    )
                    continue

                total, merged, dropped = merge_product_values(
                    existing, refreshed, keep=all_parents.get(batch_id) or list(refreshed)
                )
                if dropped:
                    logger.warning(
                        f"{self.name}: batch {batch_id} dropped {len(dropped)} "
                        f"product_values entr{'y' if len(dropped) == 1 else 'ies'} for "
                        f"products no longer on the batch: {', '.join(dropped[:5])}"
                    )

                before_total += _to_decimal(row["total_value"])
                after_total += total

                if dry_run:
                    continue
                # Guarded on status too: a batch that completed while this cycle was
                # talking to SellerCloud should keep the value it finished with.
                written = await Batch.filter(
                    id=batch_id, status__in=OPEN_BATCH_STATUSES
                ).update(
                    total_value=total, product_values=merged, value_computed_at=now
                )
                if written:
                    updated += 1
                else:
                    skipped += 1
                    logger.debug(
                        f"{self.name}: batch {batch_id} closed mid-cycle, left alone"
                    )
            except Exception:  # noqa: BLE001
                failed += 1
                logger.exception(
                    f"{self.name}: failed to refresh value for batch {batch_id}"
                )

        report = {
            "batches": len(valuation.values),
            "products": sum(len(skus) for skus in parent_sets.values()),
            "skus": f"{valuation.returned}/{valuation.requested}",
            "quarantined_chunks": f"{valuation.quarantined}/{valuation.chunks}",
            "unpriceable": sum(len(v) for v in valuation.dropped.values()),
            "suppressed_zeroes": suppressed,
            "updated": updated,
            "skipped": skipped,
            "failed": failed,
            "before_total": float(before_total),
            "after_total": float(after_total),
            "elapsed_s": round(time.monotonic() - started, 1),
            "dry_run": dry_run,
        }
        logger.info(
            f"{self.name}: {'would refresh' if dry_run else 'refreshed'} "
            f"{report['products']} pending products across {report['batches']} open "
            f"batches in {report['elapsed_s']}s "
            f"({report['skus']} SKUs from the grid); value "
            f"{float(before_total):,.2f} -> {float(after_total):,.2f}"
            + (f"; {report['unpriceable']} unpriceable" if report["unpriceable"] else "")
            + (f"; {suppressed} zero(es) suppressed" if suppressed else "")
            + (f"; {skipped} batch(es) skipped" if skipped else "")
            + (f"; {failed} failed" if failed else "")
        )
        return report


batch_value_refresh_poller = BatchValueRefreshPoller()
