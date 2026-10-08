"""Decide what would change if a platform's export were taken as the truth.

The decisions are pure functions over plain dicts and sets, in the same split as
platform_delist_service: one read pass gathers state, then nothing below touches the
database. That is what makes the flowchart in the plan table-testable and what makes
deriving the plan twice on unchanged data provably identical.

This module plans. It never writes. Applying a plan is U6.

Three things here are load-bearing and were wrong in the plan's first draft:

  Believed listed is presence OR a latest successful submission, not presence alone.
  spo_poller makes no ExternalListingService.record call, so an SPO success writes no
  presence row; a presence-only read silently under-delists.

  An open delist is handled three ways, by what the file says (R12a). Listed in full:
  close the delist and record presence as manual, because a delisted product can be
  resubmitted so the export is the better evidence. Listed partially on a platform that
  cannot post single sizes: leave it alone. Absent: leave it alone. The middle case is
  not a detail - without it the 746 partial SPO parents would be closed and re-delisted
  on every single run, forever.

  A parent being delisted whole plans no mark-listed rows. R12 read on its own would
  re-mark its in-file sizes while R14 delists the parent again.
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

from tortoise import connections

logger = logging.getLogger(__name__)

# What the planner decided for one parent.
SKIP_UNKNOWN_PARENT = "skip_unknown_parent"
NOTHING = "nothing"
MARK_LISTED = "mark_listed"
RELIST = "relist"
DELIST_PARENT = "delist_parent"
DELIST_SIZES = "delist_sizes"
# Stale presence whose product is gone from the products database. Counted with the
# delists, because that is what it means to an operator, but applied differently: there is
# no product for platform_delist_service to load, and an entry it could write would be
# unreachable (the listing history 404s without a product) and would let Restore recreate
# presence for a phantom. So the rows are removed and recorded in the import's revert
# record instead. Safe: these parents have no listings and no submission rows, so the
# presence trigger finds nothing to recompute and no batch can reopen.
CLEAR_PRESENCE = "clear_presence"
REFUSED = "refused"

# Refusal reasons. The planner predicts these; platform_delist_service is the authority
# and U6 re-checks under the per-product lock before writing.
IN_FLIGHT = "A submission is in flight"
AWAITING_ACTION = "Awaiting action on the platform"
EXCLUDED = "Excluded from this platform"
# Both of these mean "no row in child_products", but they point OPPOSITE ways and the
# preview shows them side by side, so each has to name its own direction and consequence.
# Sharing one string printed the same sentence twice with two counts; naming only the cause
# ("not in the products database" / "products that no longer exist") still read as the same
# fact twice. So each label says where it came from and what cannot happen to it.
#
# Short enough for the inline line; REASON_DETAIL below carries the explanation into the
# info popover, so the line stays scannable and the difference is one click away.
UNKNOWN_PARENT = "Unknown to SkuBase"
PRODUCT_GONE = "Deleted products"

# Why each group cannot be acted on. Shown in the preview's info popover beside its
# examples, because the short labels alone could not say which way each one points.
REASON_DETAIL = {
    UNKNOWN_PARENT: (
        "The platform lists these SKUs but SkuBase has no record of them, so there is "
        "nothing to mark listed. Usually a SKU retired or renamed here while the "
        "platform kept the old one."
    ),
    PRODUCT_GONE: (
        "SkuBase records these as listed on the platform, but the product itself is gone, "
        "so there is no product to record a delist against. Stale presence rows pointing "
        "at nothing."
    ),
    IN_FLIGHT: "A submission is still in flight, so the import leaves the product alone.",
    AWAITING_ACTION: (
        "The platform accepted a submission and still owes it a manual step, so the "
        "import leaves the product alone."
    ),
    EXCLUDED: "The platform is excluded for this product, so it is never delisted here.",
}

# A latest submission in one of these states means the platform has work in hand, so the
# import must not touch the parent (R16/AE7). 'queued' is deliberately NOT here: it is
# the status a manual-fallback platform's rows are created in and is what R17 repairs.
IN_FLIGHT_STATUSES = frozenset({"pending", "processing"})


@dataclass
class ParentPlan:
    """One parent's verdict. sizes carries whichever set the outcome refers to."""

    parent_sku: str
    outcome: str
    sizes: list[str] = field(default_factory=list)
    reason: Optional[str] = None
    delist_id: Optional[str] = None

    @property
    def writes(self) -> bool:
        return self.outcome in (
            MARK_LISTED,
            RELIST,
            DELIST_PARENT,
            DELIST_SIZES,
            CLEAR_PRESENCE,
        )


@dataclass
class Repair:
    """A stale submission row the file contradicts."""

    listing_id: str
    parent_sku: str
    previous_status: str
    outcome: str  # 'submitted' (R17, from queued) or 'reviewed' (R18, from failed)


@dataclass
class ReconcilePlan:
    platform_id: str
    parents: list[ParentPlan] = field(default_factory=list)
    repairs: list[Repair] = field(default_factory=list)
    skipped_rows: list[dict[str, Any]] = field(default_factory=list)
    believed_listed: int = 0

    def by_outcome(self, outcome: str) -> list[ParentPlan]:
        return [p for p in self.parents if p.outcome == outcome]

    @property
    def refusals(self) -> list[ParentPlan]:
        return self.by_outcome(REFUSED)

    def counts(self) -> dict[str, Any]:
        """The numbers the preview leads with (R7) and the apply checks against (U6)."""
        listed = self.by_outcome(MARK_LISTED) + self.by_outcome(RELIST)
        cleared = self.by_outcome(CLEAR_PRESENCE)
        delisted = (
            self.by_outcome(DELIST_PARENT) + self.by_outcome(DELIST_SIZES) + cleared
        )
        return {
            "listed_parents": len(listed),
            "listed_sizes": sum(len(p.sizes) for p in listed),
            "delisted_parents": len(delisted),
            "delisted_sizes": sum(len(p.sizes) for p in delisted),
            # Of the delists, the ones applied by removing stale presence because the
            # product is gone. They get no platform_delists entry, so Restore cannot bring
            # them back; the import's revert record is their only way home.
            "cleared_parents": len(cleared),
            "cleared_rows": sum(len(p.sizes) for p in cleared),
            "repairs": len(self.repairs),
            "skipped_rows": len(self.skipped_rows),
            "refusals": len(self.refusals),
            "believed_listed": self.believed_listed,
            # The share is what makes a delist count mean anything on a later run. On the
            # first SPO run the raw number is enormous by design.
            "delist_share": (
                round(100.0 * len(delisted) / self.believed_listed, 1)
                if self.believed_listed
                else 0.0
            ),
        }


def believes_listed(
    *, has_presence: bool, latest_status: Optional[str], has_open_delist: bool
) -> bool:
    """The catalog's own definition of "listed on this platform".

    An open delist overrides both signals: an operator took it down by hand.
    """
    if has_open_delist:
        return False
    return has_presence or latest_status == "success"


def refusal_for(
    *, latest_status: Optional[str], excluded: bool, in_products_db: bool
) -> Optional[str]:
    """Why this parent cannot be acted on, or None (R11, R16)."""
    if not in_products_db:
        return UNKNOWN_PARENT
    if excluded:
        return EXCLUDED
    if latest_status in IN_FLIGHT_STATUSES:
        return IN_FLIGHT
    if latest_status == "awaiting_action":
        return AWAITING_ACTION
    return None


def plan_parent(
    *,
    parent_sku: str,
    file_skus: set[str],
    active_children: set[str],
    presence_skus: set[str],
    has_parent_level_presence: bool = False,
    latest_status: Optional[str] = None,
    open_delist_id: Optional[str] = None,
    allow_partial: bool = False,
    excluded: bool = False,
    in_products_db: bool = True,
) -> ParentPlan:
    """One parent's verdict: the plan's first flowchart, in one pure function.

    file_skus and active_children are this parent's sets only, already narrowed by the
    caller, so this function never scans the whole export.
    """
    if not in_products_db:
        # R4. The import never creates a product.
        return ParentPlan(parent_sku, SKIP_UNKNOWN_PARENT, reason=UNKNOWN_PARENT)

    in_file = file_skus & active_children
    missing = active_children - file_skus
    has_open_delist = open_delist_id is not None
    # A parent-level row covers every size, so it counts as presence on its own.
    has_presence = bool(presence_skus) or has_parent_level_presence

    # Sizes the file asserts that we have no presence row for. A parent-level presence
    # row covers every size, matching ExternalListingService.coverage_for_parent, so a
    # parent carrying one needs no child rows written beside it (R12).
    unrecorded = set() if has_parent_level_presence else (in_file - presence_skus)

    if not in_file:
        # The file has nothing for this parent.
        if not believes_listed(
            has_presence=has_presence,
            latest_status=latest_status,
            has_open_delist=has_open_delist,
        ):
            # Already not listed, including the already-delisted case. This is what makes
            # a second run of the same file a no-op.
            return ParentPlan(parent_sku, NOTHING)
        reason = refusal_for(
            latest_status=latest_status, excluded=excluded, in_products_db=True
        )
        if reason:
            return ParentPlan(parent_sku, REFUSED, reason=reason)
        return ParentPlan(parent_sku, DELIST_PARENT, sizes=sorted(active_children))

    if not missing:
        # The file has every active size.
        if has_open_delist:
            # R12a. The export says the platform has it in full, and a delisted product
            # can simply be resubmitted, so the export is the better evidence.
            reason = refusal_for(
                latest_status=latest_status, excluded=excluded, in_products_db=True
            )
            if reason:
                return ParentPlan(parent_sku, REFUSED, reason=reason)
            return ParentPlan(
                parent_sku,
                RELIST,
                sizes=sorted(in_file),
                delist_id=open_delist_id,
            )
        if not unrecorded:
            return ParentPlan(parent_sku, NOTHING)
        return ParentPlan(parent_sku, MARK_LISTED, sizes=sorted(unrecorded))

    # The file has some but not all active sizes.
    if allow_partial:
        # The platform can post a single size, so record exactly what is true.
        reason = refusal_for(
            latest_status=latest_status, excluded=excluded, in_products_db=True
        )
        if reason:
            return ParentPlan(parent_sku, REFUSED, reason=reason)
        return ParentPlan(parent_sku, DELIST_SIZES, sizes=sorted(missing))

    # The platform cannot post a single size, so a parent missing sizes is not listed.
    if has_open_delist:
        # Already delisted, and it must STAY delisted. Closing it here would make the
        # parent believed-listed again and the branch above would re-delist it on the
        # next run: a permanent thrash across 746 SPO parents. This is the asymmetry
        # with R12a's full-listing case.
        return ParentPlan(parent_sku, NOTHING)
    if not believes_listed(
        has_presence=has_presence,
        latest_status=latest_status,
        has_open_delist=False,
    ):
        return ParentPlan(parent_sku, NOTHING)
    reason = refusal_for(
        latest_status=latest_status, excluded=excluded, in_products_db=True
    )
    if reason:
        return ParentPlan(parent_sku, REFUSED, reason=reason)
    return ParentPlan(parent_sku, DELIST_PARENT, sizes=sorted(active_children))


def plan_repair(
    *,
    listing_id: str,
    parent_sku: str,
    latest_status: str,
    parent_in_file: bool,
    active_children: set[str],
    file_skus: set[str],
) -> Optional[Repair]:
    """A stale submission row the file contradicts (R17, R18).

    'queued' is the status a manual-fallback platform's rows are created in
    (listing_routes sets it when manual_fallback or photography is pending), so it is
    what R17 repairs. 'processing' is left alone for the same reason R16 refuses a
    delist: the platform has work in hand.
    """
    if not parent_in_file:
        return None
    if latest_status == "failed":
        # R18: the same action and meaning the dashboard's Mark as Reviewed already has.
        return Repair(listing_id, parent_sku, latest_status, "reviewed")
    if latest_status == "queued":
        # R17 needs every active size present, or the parent is not fully listed.
        if active_children and not (active_children - file_skus):
            return Repair(listing_id, parent_sku, latest_status, "submitted")
    return None


def build_plan(
    *,
    platform_id: str,
    file_skus: Iterable[str],
    skipped_rows: list[dict[str, Any]],
    active_by_parent: dict[str, set[str]],
    presence_by_parent: dict[str, set[str]],
    parent_level_presence: set[str],
    latest_by_parent: dict[str, str],
    latest_submissions: list[dict[str, Any]],
    open_delists: dict[str, str],
    parent_by_sku: dict[str, str],
    allow_partial: bool,
    excluded_parents: Optional[set[str]] = None,
) -> ReconcilePlan:
    """Assemble the whole plan from one read pass. Pure: every argument is already read.

    Deriving this twice on unchanged inputs gives an identical result, which is the
    third success criterion.
    """
    excluded_parents = excluded_parents or set()
    file_set = set(file_skus)

    # Group the file by parent using the resolved map, never by splitting the sku.
    # A sku the products database does not know has no parent, so it is a skip (R4).
    file_by_parent: dict[str, set[str]] = {}
    unresolved: list[str] = []
    for sku in sorted(file_set):
        parent = parent_by_sku.get(sku)
        if parent is None:
            unresolved.append(sku)
            continue
        file_by_parent.setdefault(parent, set()).add(sku)

    believed = {
        parent
        for parent in set(presence_by_parent) | set(latest_by_parent) | parent_level_presence
        if believes_listed(
            has_presence=bool(presence_by_parent.get(parent)) or parent in parent_level_presence,
            latest_status=latest_by_parent.get(parent),
            has_open_delist=parent in open_delists,
        )
    }

    plan = ReconcilePlan(platform_id=platform_id, believed_listed=len(believed))
    plan.skipped_rows = list(skipped_rows)
    for sku in unresolved:
        plan.skipped_rows.append({"value": sku, "reason": UNKNOWN_PARENT})

    # Every parent either side of the diff. A parent in neither set cannot change.
    for parent in sorted(set(file_by_parent) | believed | set(open_delists)):
        in_db = parent in active_by_parent
        if not in_db:
            if parent in file_by_parent:
                # Its skus resolved to this parent but the parent has no child_products
                # rows, so there is nothing to act on. The skus are already counted as
                # skips above, so only record the parent verdict here.
                plan.parents.append(
                    ParentPlan(parent, SKIP_UNKNOWN_PARENT, reason=UNKNOWN_PARENT)
                )
            elif parent in believed:
                # Believed listed, absent from the file, and the product itself is gone
                # from the products database: 19 such parents on SPO in prod, holding 70
                # stale SPO presence rows between them. They still have to come off, so
                # they join the delists rather than being merely reported.
                stale = sorted(presence_by_parent.get(parent, set()))
                plan.parents.append(
                    ParentPlan(
                        parent, CLEAR_PRESENCE, sizes=stale, reason=PRODUCT_GONE
                    )
                )
            continue
        plan.parents.append(
            plan_parent(
                parent_sku=parent,
                file_skus=file_by_parent.get(parent, set()),
                active_children=active_by_parent.get(parent, set()),
                presence_skus=presence_by_parent.get(parent, set()),
                has_parent_level_presence=parent in parent_level_presence,
                latest_status=latest_by_parent.get(parent),
                open_delist_id=open_delists.get(parent),
                allow_partial=allow_partial,
                excluded=parent in excluded_parents,
            )
        )

    for row in latest_submissions:
        parent = row["parent_sku"]
        repair = plan_repair(
            listing_id=row["listing_id"],
            parent_sku=parent,
            latest_status=row["status"],
            parent_in_file=parent in file_by_parent,
            active_children=active_by_parent.get(parent, set()),
            file_skus=file_by_parent.get(parent, set()),
        )
        if repair:
            plan.repairs.append(repair)

    return plan


# --- the read pass -----------------------------------------------------------------
#
# One pass per platform, gathering everything build_plan needs. Everything above this
# line is pure; everything below reads and nothing writes.


async def _allow_partial(platform_id: str) -> bool:
    """The platform's allow_partial_submit setting, defaulting to off.

    Off is the safe default: it means a parent missing sizes is delisted whole, which is
    right for every platform that cannot post a single size. The setting only exists on
    databases that have the delist migration.
    """
    rows = await connections.get("default").execute_query_dict(
        "SELECT COALESCE(platform_settings, '{}'::jsonb) AS s FROM app_settings "
        "ORDER BY id LIMIT 1"
    )
    if not rows:
        return False
    settings = rows[0]["s"]
    if isinstance(settings, str):
        import json

        settings = json.loads(settings or "{}")
    return bool((settings.get(platform_id) or {}).get("allow_partial_submit", False))


# Ranked so a parent with several listings reads by its most informative row: a success
# anywhere means listed, and otherwise the most blocking state wins so refusals are
# predicted rather than discovered at apply time.
_STATUS_RANK = {
    "failed": 0,
    "queued": 1,
    "awaiting_action": 2,
    "pending": 3,
    "processing": 4,
    "success": 5,
}


async def read_state(
    *, platform_id: str, file_skus: Iterable[str]
) -> dict[str, Any]:
    """Gather the platform's current state. Read-only.

    open_platform_delists() may not exist yet on a database without the delist
    migration, so the read is guarded: an unmigrated database simply has no open
    delists, which is factually what it contains.
    """
    sk = connections.get("default")
    pr = connections.get("product_db")

    presence_by_parent: dict[str, set[str]] = {}
    parent_level_presence: set[str] = set()
    for r in await sk.execute_query_dict(
        "SELECT parent_sku, sku, level FROM external_listing_ids WHERE platform_id = $1",
        [platform_id],
    ):
        if r["level"] == "parent" or r["sku"] is None:
            parent_level_presence.add(r["parent_sku"])
        else:
            presence_by_parent.setdefault(r["parent_sku"], set()).add(r["sku"])

    latest_submissions = await sk.execute_query_dict(
        """
        SELECT DISTINCT ON (s.listing_id)
               s.listing_id::text AS listing_id, s.status, l.product_id AS parent_sku
        FROM listing_submissions s
        JOIN listings l ON l.id = s.listing_id
        WHERE s.platform_id = $1
        ORDER BY s.listing_id, s.attempt_number DESC
        """,
        [platform_id],
    )
    latest_by_parent: dict[str, str] = {}
    for r in latest_submissions:
        parent, status = r["parent_sku"], r["status"]
        if parent not in latest_by_parent or _STATUS_RANK.get(
            status, 0
        ) > _STATUS_RANK.get(latest_by_parent[parent], 0):
            latest_by_parent[parent] = status

    open_delists: dict[str, str] = {}
    migrated = await sk.execute_query_dict(
        "SELECT to_regclass('public.platform_delists') IS NOT NULL AS ok"
    )
    if migrated and migrated[0]["ok"]:
        for r in await sk.execute_query_dict(
            "SELECT parent_sku, id::text AS id FROM open_platform_delists() "
            "WHERE platform_id = $1",
            [platform_id],
        ):
            open_delists[r["parent_sku"]] = r["id"]
    else:
        logger.warning(
            "platform_delists is absent, so %s reconcile sees no open delists",
            platform_id,
        )

    # Resolve every file sku to its parent from child_products. NEVER by splitting on
    # "/": a child sku is not reliably "<parent>/<size>" (1.7% of active children are
    # not, including 1,579 whose child sku IS the parent sku), and a wrong parent means a
    # real product reads as absent from the file and gets delisted. A sku with no row
    # here is one the products database does not know, which is R4's skip.
    file_sku_list = sorted(set(file_skus))
    parent_by_sku: dict[str, str] = {}
    if file_sku_list:
        for r in await pr.execute_query_dict(
            "SELECT sku, parent_sku FROM child_products WHERE sku = ANY($1::text[])",
            [file_sku_list],
        ):
            parent_by_sku[r["sku"]] = r["parent_sku"]

    interesting = sorted(
        set(presence_by_parent)
        | parent_level_presence
        | set(latest_by_parent)
        | set(parent_by_sku.values())
        | set(open_delists)
    )
    active_by_parent: dict[str, set[str]] = {}
    if interesting:
        for r in await pr.execute_query_dict(
            "SELECT parent_sku, sku FROM child_products "
            "WHERE is_active = true AND parent_sku = ANY($1::text[])",
            [interesting],
        ):
            active_by_parent.setdefault(r["parent_sku"], set()).add(r["sku"])
        # A parent in child_products with no active child is still KNOWN. Without this
        # it would be reported as absent from the products database (R4) when it is
        # simply out of stock, and 19 prod parents whose product really is gone would be
        # indistinguishable from it.
        for r in await pr.execute_query_dict(
            "SELECT DISTINCT parent_sku FROM child_products WHERE parent_sku = ANY($1::text[])",
            [interesting],
        ):
            active_by_parent.setdefault(r["parent_sku"], set())

    return {
        "presence_by_parent": presence_by_parent,
        "parent_level_presence": parent_level_presence,
        "latest_by_parent": latest_by_parent,
        "latest_submissions": [dict(r) for r in latest_submissions],
        "open_delists": open_delists,
        "active_by_parent": active_by_parent,
        "parent_by_sku": parent_by_sku,
        "allow_partial": await _allow_partial(platform_id),
    }
