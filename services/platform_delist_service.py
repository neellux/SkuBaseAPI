"""A product's listing history per platform, and the manual delist that undoes "listed".

A person takes a listing down on a platform by hand. SkuBase would still treat the product
as listed there (presence rows in external_listing_ids, a successful attempt), so the catalog
would never offer it again and the submit gate would block a relist. A delist records the
takedown as a platform_delists entry and removes the presence rows it contradicts. It never
calls a platform. Restore undoes one entry exactly, until the next attempt to that platform.

Plan: docs/plans/2026-09-30-1031-feat-product-listing-history-delist-plan.md (U3).
Schema and the one definition of an OPEN delist: migrations/add_platform_delists.sql.
Completion and the required write order: migrations/delist_aware_listing_completion.sql.

The decisions are pure functions over plain dicts shaped like the SQL rows below, so they
can be table-tested; API/tests/ cannot import route modules. The routes (U4) only map
DelistRefused to an HTTPException and add user names.


GET HISTORY PAYLOAD (get_history / build_history)

    {
      "parent_sku": "ALD-MSNK-0003",
      "sizes": [                                  # the columns of the matrix, in order
        {"sku": "ALD-MSNK-0003/9", "size": "9", "active": true},
        ...
        # then, sorted by sku, any INACTIVE child that still has a presence row or is in an
        # open delist on a shown platform, with "active": false, so a row that says
        # "listed" is never invisible. Only active sizes count toward Listed vs Partially.
      ],
      "platforms": [                              # app_settings.platforms, in that order
        {
          "platform_id": "ebay",
          "label": "eBay",
          "read_only": false,                     # true for sellercloud only
          "status": "listed",                     # see ROW STATUS
          "excluded": false,                      # decorates ANY status, see ROW STATUS
          "excluded_reasons": [],                 # ["brand", "company", "product type"]
          "listed_at": <datetime> | null,         # KTD9, see LISTED DATE
          "listed_at_recorded": false,            # true: presence only, show "Recorded as listed"
          "allow_partial_submit": true,           # R8: false means a pick selects every size
          "can_delist": true,
          "lock_reason": null | "Waiting on eBay images",   # short, snackbar-safe
          "lock_code": null | "awaiting_action",            # see REFUSAL CODES
          "sizes": {"ALD-MSNK-0003/9": "listed", ...},      # every sku in top-level sizes
                                                  # "listed" | "delisted" | "none"
          "open_delists": [<delist event>, ...],  # open entries only, oldest first
          "timeline": [<event>, ...]              # newest first
        }
      ]
    }

    Internal platforms (Shop The Sample) never appear: they are not in app_settings.platforms.

    Events. Every event has "kind", "at" and "user_id", so the route can name users with
    add_user_data(events, ["user_id"], ["name"]) -> "user_name".

      attempt   {"kind": "attempt", "at": created_at, "user_id": submitted_by,
                 "submission_id", "listing_id", "batch_id", "batch_comment",
                 "attempt_number", "status", "platform_status", "reviewed": bool,
                 "accepted": bool (is_accepted; closes a delist),
                 "error": first line, <= 160 chars | null,
                 "listed_at": KTD9 date if it shows as listed else null, "completed_at"}
      delist    {"kind": "delist", "at": created_at, "user_id": created_by,
                 "delist_id", "comment", "child_skus": [..] | null (whole product),
                 "sizes": ["M", ..] | null, "state": "open" | "relisted" | "restored",
                 "can_restore": bool, "restore_lock_reason": str | null}
      restore   {"kind": "restore", "at": restored_at, "user_id": restored_by,
                 "delist_id", "child_skus", "sizes"}

    delist() and restore() return the entry: {"id", "platform_id", "parent_sku",
    "child_skus", "comment", "created_by", "created_at", "restored_at", "restored_by"}.


ROW STATUS (R4, the catalog's vocabulary plus two), first match wins:

    in_progress       a listing's latest attempt is queued, pending, processing or
                      awaiting_action (eBay is live then, but a person owes it images)
    failed            an open delist, and an attempt created after the newest open entry
                      finished without the platform accepting it
    partially_listed  some active sizes listed (or only inactive ones)
    listed            every active size listed
    failed            nothing listed, no open delist, and the latest attempt was not
                      accepted
    excluded          nothing else to say, and the platform is excluded
    none              "Not listed", including every size taken down by an open delist

    The status does NOT carry exclusion; `excluded` does, beside whatever the status is
    (user decision 2026-10-06). Exclusion stops new submissions and never removes what is
    already live, so a platform can be excluded AND listed. Until 2026-10-06 excluded was
    checked first and masked the real state of exactly those products. The catalog grid
    still merges the two (catalog_service.exclusions_for), because a grid cell has room
    for one answer; a detail view separates them, as the listing view already does.

    A delisted size is not listed: there is no "delisted" row status. The size keeps
    the state "delisted" in `sizes` so the service can refuse a second delist of it.

    "Accepted" is is_accepted, the Python mirror of platform_delist_closing_attempt:
    success that is not a GOAT denial (a reviewed failure counts), or awaiting_action.
    Anything else that finished (failed, a denial) counts as failed here.

SIZE STATE: delisted if an open entry covers it (child_skus NULL covers all); else listed
if a parent row or its child row exists, or, when the platform has no rows at all, an
accepted attempt exists and no open entry removed rows (a platform that never recorded
presence); else none.

LISTED DATE (KTD9): the latest accepted attempt's `listed` step `at`, then its completed_at,
then its updated_at. With no accepted attempt on any listing of the parent, the earliest
first_seen_at of the presence rows, flagged listed_at_recorded (R5).

REFUSAL CODES (DelistRefused.code, also lock_code), with the HTTP status the route uses:

    read_only 400, unknown_platform 400, excluded 409, in_flight 409, awaiting_action 409,
    not_listed 409, already_delisted 409, no_sizes 400, unknown_size 400,
    comment_too_long 400, not_found 404, already_restored 409, resubmitted 409


WRITE ORDER. The presence trigger on external_listing_ids is a non-deferred AFTER row
trigger that recomputes completion, so what exists at each statement matters (KTD3):

    delist   lock, re-read, INSERT the entry with its pre-image, DELETE the rows, then
             INSERT split child rows and record them on the entry.
    restore  lock, re-read, mark the entry restored, DELETE the rows it inserted, re-insert
             its pre-image, recompute every listing of the parent with p_allow_unflag false
             (a re-insert that conflicts inserts nothing and fires no trigger).

Both run in one transaction under the per-product submit advisory lock (KTD5) and clear the
catalog summary cache after commit (KTD11).
"""

import asyncio
import json
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from tortoise import connections
from tortoise.transactions import in_transaction

from services import catalog_service
from services.catalog_product_cache import Product
from services.external_listing_service import NEVER_GATED, ExternalListingService

logger = logging.getLogger(__name__)

# Size states and row statuses. "none" and the four shared with the catalog use its ids,
# so the UI reuses its labels.
LISTED = "listed"
DELISTED = "delisted"
NONE = "none"
PARTIALLY_LISTED = "partially_listed"
IN_PROGRESS = "in_progress"
FAILED = "failed"
EXCLUDED = "excluded"

# attempt_outcome's third value, beside IN_PROGRESS and FAILED.
ACCEPTED = "accepted"

# Delist entry states in the timeline.
OPEN = "open"
RELISTED = "relisted"
RESTORED = "restored"

# Refusal codes. EXCLUDED doubles as one.
READ_ONLY = "read_only"
UNKNOWN_PLATFORM = "unknown_platform"
IN_FLIGHT = "in_flight"
AWAITING_ACTION = "awaiting_action"
NOT_LISTED = "not_listed"
ALREADY_DELISTED = "already_delisted"
NO_SIZES = "no_sizes"
UNKNOWN_SIZE = "unknown_size"
COMMENT_TOO_LONG = "comment_too_long"
NOT_FOUND = "not_found"
ALREADY_RESTORED = "already_restored"
RESUBMITTED = "resubmitted"

# Same limit as the flag note, and the table's CHECK.
COMMENT_MAX_LENGTH = 500
ERROR_MAX_LENGTH = 160

# R12: a latest attempt in one of these, on any listing of the parent, locks the platform.
# The same set that blocks a sibling submit. awaiting_action gets its own reason text.
_IN_FLIGHT_STATUSES = tuple(
    s for s in ExternalListingService.SIBLING_IN_FLIGHT_STATUSES if s != "awaiting_action"
)

# Must match PLATFORM_LABELS in UI/src/utils/catalogFilters.js.
PLATFORM_LABELS = {
    "sellercloud": "SellerCloud",
    "grailed": "Grailed",
    "spo": "SPO",
    "ebay": "eBay",
    "1nventory": "1nventory",
    "goat": "GOAT",
}

# The key listing_routes takes before creating submission rows. Delist and Restore take the
# same one, so neither can interleave with a submit of the same product (KTD5).
SUBMIT_LOCK_SQL = "SELECT pg_advisory_xact_lock(hashtext('listing_submit'), hashtext($1))"

_EPOCH = datetime.min.replace(tzinfo=timezone.utc)


class DelistRefused(Exception):
    """A delist or Restore the service will not do.

    `message` is short enough to show as a snackbar; the route raises
    HTTPException(status_code=status_code, detail=message). Diagnostics are logged here.
    """

    def __init__(self, message: str, code: str, status_code: int = 409):
        super().__init__(message)
        self.message = message
        self.code = code
        self.status_code = status_code


@dataclass(frozen=True)
class ProductFacts:
    """The product side, from the products DB: exclusion inputs and the children.

    `active` is [{"sku", "size"}] in size order; `inactive` maps sku -> size.
    """

    parent_sku: str
    brand: Optional[str]
    product_type: Optional[str]
    company_code: Optional[int]
    active: List[Dict[str, str]]
    inactive: Dict[str, str]


@dataclass(frozen=True)
class PlatformFacts:
    """Everything the decisions need about one platform of one product."""

    platform_id: str
    states: Dict[str, str]
    active_skus: List[str]
    open_entries: List[Dict[str, Any]]
    in_flight: bool
    awaiting: bool
    failed_since_delist: bool
    latest_failed: bool
    accepted_any: bool
    listed_active: int
    listed_other: bool

    @property
    def anything_listed(self) -> bool:
        return self.listed_active > 0 or self.listed_other


@dataclass(frozen=True)
class RowPlan:
    remove_ids: List[str]
    insert_skus: List[str]


@dataclass(frozen=True)
class DelistPlan:
    scope: Optional[List[str]]  # None: the whole product
    remove_ids: List[str]
    insert_skus: List[str]


# ---------------------------------------------------------------------------
# Small pure helpers
# ---------------------------------------------------------------------------


def platform_label(platform_id: str) -> str:
    return PLATFORM_LABELS.get(platform_id, str(platform_id or "").upper())


def normalize_comment(comment: Optional[str]) -> Optional[str]:
    """The comment as stored: trimmed, and blank stored as NULL (R9).

    Shaped like ListingService.normalize_flag_note, except that the comment is optional.
    """
    cleaned = (comment or "").strip()
    if not cleaned:
        return None
    if len(cleaned) > COMMENT_MAX_LENGTH:
        raise DelistRefused(
            f"Comment is over {COMMENT_MAX_LENGTH} characters", COMMENT_TOO_LONG, 400
        )
    return cleaned


def is_accepted(status: Optional[str], platform_status: Optional[str], reviewed_at: Any) -> bool:
    """The platform took this attempt: success that is not a GOAT denial, or eBay's
    awaiting_action. A reviewed failure counts, because reviewing resolves it
    (mark_import_reviewed) and the catalog reads it as listed. The Python mirror of
    platform_delist_closing_attempt; keep the two identical. reviewed_at stays in the
    signature to match the SQL function."""
    return (status == "success" and platform_status != "denied") or status == "awaiting_action"


def attempt_outcome(attempt: Dict[str, Any]) -> str:
    """IN_PROGRESS, ACCEPTED or FAILED, for display. awaiting_action is in progress here,
    as in the catalog, even though it is also accepted."""
    status = attempt.get("status")
    if status in _IN_FLIGHT_STATUSES or status == "awaiting_action":
        return IN_PROGRESS
    if is_accepted(status, attempt.get("platform_status"), attempt.get("reviewed_at")):
        return ACCEPTED
    return FAILED


def _as_json(value: Any) -> Any:
    """Raw reads hand JSONB back as text."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return None
    return value


def _parse_ts(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def listed_step_at(steps: Any) -> Optional[datetime]:
    """The `at` of the first `listed` step in platform_meta.steps (submission_steps.py)."""
    steps = _as_json(steps)
    if not isinstance(steps, list):
        return None
    for step in steps:
        if isinstance(step, dict) and step.get("step") == "listed":
            parsed = _parse_ts(step.get("at"))
            if parsed:
                return parsed
    return None


def attempt_listed_at(attempt: Dict[str, Any]) -> Optional[datetime]:
    """When the platform accepted this attempt (KTD9, R6). submitted_at is empty on prod."""
    return (
        listed_step_at(attempt.get("steps"))
        or attempt.get("completed_at")
        or attempt.get("updated_at")
    )


def _latest_key(attempt: Dict[str, Any]) -> Tuple[datetime, int]:
    """The catalog's "latest": newest listing, then highest attempt number."""
    return (attempt.get("listing_created_at") or _EPOCH, attempt.get("attempt_number") or 0)


def listed_date(
    attempts: Sequence[Dict[str, Any]], rows: Sequence[Dict[str, Any]]
) -> Tuple[Optional[datetime], bool]:
    """(date, recorded). recorded is True when no listing of the parent has an accepted
    attempt and the date is the presence rows' first_seen_at (R5): a backfill date, not a
    listing date. Decided from attempts, never from `source`, which the upsert keeps."""
    accepted = [
        a
        for a in attempts
        if is_accepted(a.get("status"), a.get("platform_status"), a.get("reviewed_at"))
    ]
    if accepted:
        return attempt_listed_at(max(accepted, key=_latest_key)), False
    seen = [r["first_seen_at"] for r in rows if r.get("first_seen_at")]
    if seen:
        return min(seen), True
    return None, False


def short_error(text: Optional[str]) -> Optional[str]:
    """First non-empty line, capped: tracebacks land in `error` and the timeline has one
    line per attempt."""
    if not text:
        return None
    line = next((ln.strip() for ln in str(text).splitlines() if ln.strip()), "")
    if not line:
        return None
    if len(line) > ERROR_MAX_LENGTH:
        return line[: ERROR_MAX_LENGTH - 3].rstrip() + "..."
    return line


def _size_label(sku: str) -> str:
    return sku.rsplit("/", 1)[-1] if "/" in sku else sku


# ---------------------------------------------------------------------------
# Sizes
# ---------------------------------------------------------------------------


# Mirrors SIZE_ORDER in UI/src/components/ImportDetailDialog.jsx.
_CLOTHING_ORDER = {
    label: i
    for i, label in enumerate(
        ("XXXS", "XXS", "XS", "S", "M", "L", "XL", "XXL", "XXXL", "2XL", "3XL", "4XL", "5XL", "6XL")
    )
}
_NUMBER = re.compile(r"(\d+(?:\.\d+)?)")


def _natural_key(label: str) -> Tuple[Tuple[int, float, str], ...]:
    """Numbers compare as numbers, so 6.5 sorts before 10, as localeCompare numeric."""
    return tuple(
        (0, float(part), "") if _NUMBER.fullmatch(part) else (1, 0.0, part.lower())
        for part in _NUMBER.split(label)
        if part
    )


def order_children(
    children: Iterable[Dict[str, Any]], rank: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Children in the parent's sizing-scheme order (ProductService.apply_size_sort). Most
    parents have no scheme, so then the UI's order: clothing sizes S, M, L first, then
    labels with numbers compared numerically."""

    def key(child: Dict[str, Any]) -> Tuple[Any, ...]:
        size = child.get("size") or ""
        order = rank.get(size)
        return (
            float("inf") if order is None else float(order),
            _CLOTHING_ORDER.get(size.upper(), len(_CLOTHING_ORDER)),
            _natural_key(size),
            child.get("sku") or "",
        )

    return sorted(children, key=key)


def size_list(
    product: ProductFacts,
    rows: Iterable[Dict[str, Any]],
    entries: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """The size columns: active children in size order, then inactive or unknown child
    SKUs that still have a presence row or are in an open delist, sorted by sku."""
    out = [{"sku": c["sku"], "size": c["size"], "active": True} for c in product.active]
    active = {c["sku"] for c in product.active}
    stray: Set[str] = {
        r["sku"] for r in rows if r.get("level") == "child" and r["sku"] not in active
    }
    for e in entries:
        if e.get("is_open") and e.get("restored_at") is None:
            stray.update(s for s in (e.get("child_skus") or []) if s not in active)
    for sku in sorted(stray):
        out.append(
            {"sku": sku, "size": product.inactive.get(sku) or _size_label(sku), "active": False}
        )
    return out


def _accepted_fallback(
    rows: Sequence[Dict[str, Any]], open_entries: Sequence[Dict[str, Any]], accepted_any: bool
) -> bool:
    """An accepted attempt stands in for presence: no rows, and no open entry removed any."""
    return not rows and accepted_any and not any(e.get("removed_count") for e in open_entries)


def size_states(
    skus: Sequence[str],
    rows: Sequence[Dict[str, Any]],
    open_entries: Sequence[Dict[str, Any]],
    accepted_any: bool,
) -> Dict[str, str]:
    """listed, delisted or none per sku, for one platform.

    The accepted-attempt fallback covers a platform that never recorded presence for this
    parent. It is off once an open entry removed rows: an empty table then means "taken
    down", and a size that never had a row must not start reading as listed.
    """
    has_parent_row = any(r.get("level") == "parent" for r in rows)
    child_rows = {r["sku"] for r in rows if r.get("level") == "child"}
    whole = any(e.get("child_skus") is None for e in open_entries)
    delisted = {s for e in open_entries for s in (e.get("child_skus") or [])}
    fallback = _accepted_fallback(rows, open_entries, accepted_any)
    states = {}
    for sku in skus:
        if whole or sku in delisted:
            states[sku] = DELISTED
        elif has_parent_row or sku in child_rows or fallback:
            states[sku] = LISTED
        else:
            states[sku] = NONE
    return states


# ---------------------------------------------------------------------------
# One platform's facts, status and refusals
# ---------------------------------------------------------------------------


def _latest_per_listing(attempts: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    latest: Dict[Any, Dict[str, Any]] = {}
    for a in attempts:
        current = latest.get(a.get("listing_id"))
        if current is None or (a.get("attempt_number") or 0, a["created_at"]) > (
            current.get("attempt_number") or 0,
            current["created_at"],
        ):
            latest[a.get("listing_id")] = a
    return list(latest.values())


def platform_facts(
    platform_id: str,
    sizes: Sequence[Dict[str, Any]],
    attempts: Iterable[Dict[str, Any]],
    rows: Iterable[Dict[str, Any]],
    entries: Iterable[Dict[str, Any]],
) -> PlatformFacts:
    """Facts for one platform. attempts span every listing of the parent."""
    attempts = [a for a in attempts if a.get("platform_id") == platform_id]
    rows = [r for r in rows if r.get("platform_id") == platform_id]
    entries = [e for e in entries if e.get("platform_id") == platform_id]
    open_entries = sorted(
        (e for e in entries if e.get("is_open") and e.get("restored_at") is None),
        key=lambda e: e["created_at"],
    )

    latest = _latest_per_listing(attempts)
    in_flight = any(a.get("status") in _IN_FLIGHT_STATUSES for a in latest)
    awaiting = any(a.get("status") == "awaiting_action" for a in latest)
    accepted_any = any(
        is_accepted(a.get("status"), a.get("platform_status"), a.get("reviewed_at"))
        for a in attempts
    )

    failed_since_delist = False
    if open_entries:
        newest = open_entries[-1]["created_at"]
        failed_since_delist = any(
            a["created_at"] > newest and attempt_outcome(a) == FAILED for a in attempts
        )
    latest_failed = bool(attempts) and attempt_outcome(max(attempts, key=_latest_key)) == FAILED

    skus = [s["sku"] for s in sizes]
    active_skus = [s["sku"] for s in sizes if s.get("active")]
    states = size_states(skus, rows, open_entries, accepted_any)
    listed_active = sum(1 for s in active_skus if states[s] == LISTED)
    if skus:
        listed_other = any(states[s["sku"]] == LISTED for s in sizes if not s.get("active"))
    else:
        # No children to show: presence or an accepted attempt stands for the product.
        whole_open = any(e.get("child_skus") is None for e in open_entries)
        listed_other = not whole_open and (
            bool(rows) or _accepted_fallback(rows, open_entries, accepted_any)
        )
    return PlatformFacts(
        platform_id=platform_id,
        states=states,
        active_skus=active_skus,
        open_entries=open_entries,
        in_flight=in_flight,
        awaiting=awaiting,
        failed_since_delist=failed_since_delist,
        latest_failed=latest_failed,
        accepted_any=accepted_any,
        listed_active=listed_active,
        listed_other=listed_other,
    )


def row_status(
    *,
    excluded: bool,
    in_flight: bool,
    delist_open: bool,
    failed_since_delist: bool,
    listed_active: int,
    total_active: int,
    listed_other: bool,
    latest_failed: bool,
) -> str:
    """Precedence: In progress, Failed, Listed, Partially listed, Excluded, Not listed.

    A delisted size is simply not listed (user decision 2026-09-30), so an open delist
    never has a status of its own: it lowers the listed count, and while one is open only
    an attempt made after it can read Failed.

    Excluded sits near the BOTTOM, not the top (user decision 2026-10-06). A platform can
    be excluded AND genuinely listed, because exclusion stops new submissions and never
    removes what is already live. Short-circuiting on it masked the real state of every
    such product: 95-HLS-1004 is listed on grailed and spo while excluded from both, and
    read "Excluded" with green listed size chips beside it. So exclusion is the status
    only when there is nothing else to say, and the `excluded` flag on the payload rides
    alongside whatever the status turns out to be."""
    if in_flight:
        return IN_PROGRESS
    if delist_open and failed_since_delist:
        return FAILED
    if total_active and listed_active >= total_active:
        return LISTED
    if listed_active or listed_other:
        return PARTIALLY_LISTED if total_active else LISTED
    # An open delist suppresses a stale failure, as before; it does not suppress the
    # exclusion, which is the most informative thing left to say about the platform.
    if not delist_open and latest_failed:
        return FAILED
    return EXCLUDED if excluded else NONE


def row_status_for(facts: PlatformFacts, excluded: bool) -> str:
    return row_status(
        excluded=excluded,
        in_flight=facts.in_flight or facts.awaiting,
        delist_open=bool(facts.open_entries),
        failed_since_delist=facts.failed_since_delist,
        listed_active=facts.listed_active,
        total_active=len(facts.active_skus),
        listed_other=facts.listed_other,
        latest_failed=facts.latest_failed,
    )


def _read_only(platform_id: str) -> DelistRefused:
    return DelistRefused(f"{platform_label(platform_id)} cannot be delisted", READ_ONLY, 400)


def delist_refusal(
    platform_id: str, *, enabled: bool, excluded: bool, facts: PlatformFacts
) -> Optional[DelistRefused]:
    """Why this platform cannot be delisted right now (R12, R13), or None. Returned, not
    raised, because the history shows it as the column's lock reason."""
    label = platform_label(platform_id)
    if platform_id in NEVER_GATED:
        return _read_only(platform_id)
    if not enabled:
        return DelistRefused("Unknown platform", UNKNOWN_PLATFORM, 400)
    if excluded:
        return DelistRefused(f"{label} is excluded for this product", EXCLUDED)
    if facts.in_flight:
        return DelistRefused(f"A submission to {label} is in progress", IN_FLIGHT)
    if facts.awaiting:
        message = (
            "Waiting on eBay images" if platform_id == "ebay" else f"{label} is awaiting action"
        )
        return DelistRefused(message, AWAITING_ACTION)
    if not facts.anything_listed:
        if facts.open_entries:
            return DelistRefused("Already delisted", ALREADY_DELISTED)
        return DelistRefused(f"Not listed on {label}", NOT_LISTED)
    return None


def expand_selection(
    selected: Optional[Sequence[str]], active_skus: Sequence[str], allow_partial: bool
) -> Optional[List[str]]:
    """R8. None stays None (the whole product). With allow_partial_submit off, any pick is
    every active size. With it on, the picks, deduplicated, in size order."""
    if selected is None:
        return None
    if not allow_partial:
        return list(active_skus)
    picked = set(selected)
    ordered = [s for s in active_skus if s in picked]
    extra = []
    for s in selected:
        if s not in active_skus and s not in extra:
            extra.append(s)
    return ordered + extra


def delist_scope(
    expanded: Optional[Sequence[str]], active_skus: Sequence[str]
) -> Optional[List[str]]:
    """What the entry stores: None when the selection covers every active size, so a
    whole-product delist also removes rows for SKUs no longer among the children."""
    if expanded is None:
        return None
    if active_skus and set(active_skus) <= set(expanded):
        return None
    return list(expanded)


def plan_rows(
    rows: Sequence[Dict[str, Any]],
    scope: Optional[Sequence[str]],
    active_skus: Sequence[str],
    open_covered: Set[str],
) -> RowPlan:
    """KTD2. Whole product: remove every row for the platform and parent. Per size: remove
    those sizes' child rows, and replace a parent row with child rows for the remaining
    active sizes, so "a row means listed" stays true. Sizes that already have a child row,
    or that another open entry covers, get none."""
    if scope is None:
        return RowPlan(remove_ids=[r["id"] for r in rows], insert_skus=[])
    scoped = set(scope)
    remove = [r["id"] for r in rows if r.get("level") == "child" and r["sku"] in scoped]
    parent_rows = [r for r in rows if r.get("level") == "parent"]
    insert: List[str] = []
    if parent_rows:
        remove += [r["id"] for r in parent_rows]
        have = {r["sku"] for r in rows if r.get("level") == "child" and r["sku"] not in scoped}
        insert = [
            s for s in active_skus if s not in scoped and s not in open_covered and s not in have
        ]
    return RowPlan(remove_ids=remove, insert_skus=insert)


def plan_delist(
    platform_id: str,
    selected: Optional[Sequence[str]],
    *,
    enabled: bool,
    allow_partial: bool,
    excluded: bool,
    size_list: Sequence[Dict[str, Any]],
    facts: PlatformFacts,
    rows: Sequence[Dict[str, Any]],
) -> DelistPlan:
    """Validate a delist and plan its rows, or raise DelistRefused."""
    refused = delist_refusal(platform_id, enabled=enabled, excluded=excluded, facts=facts)
    if refused:
        raise refused
    if selected is not None:
        if not selected:
            raise DelistRefused("Pick at least one size", NO_SIZES, 400)
        unknown = [s for s in selected if s not in facts.states]
        if unknown:
            logger.info("delist %s: unknown sizes %s", platform_id, unknown)
            raise DelistRefused("Unknown size", UNKNOWN_SIZE, 400)

    active = [s["sku"] for s in size_list if s.get("active")]
    scope = delist_scope(expand_selection(selected, active, allow_partial), active)
    if scope is not None:
        if any(facts.states.get(s) == DELISTED for s in scope):
            raise DelistRefused("Already delisted", ALREADY_DELISTED)
        if any(facts.states.get(s) != LISTED for s in scope):
            raise DelistRefused("Only listed sizes can be delisted", NOT_LISTED)

    open_covered = {s for e in facts.open_entries for s in (e.get("child_skus") or [])}
    row_plan = plan_rows(rows, scope, active, open_covered)
    return DelistPlan(scope=scope, remove_ids=row_plan.remove_ids, insert_skus=row_plan.insert_skus)


# ---------------------------------------------------------------------------
# Restore eligibility (R18)
# ---------------------------------------------------------------------------


def attempted_since(entry: Dict[str, Any], attempts: Iterable[Dict[str, Any]]) -> bool:
    """Any attempt, of any status, on any listing of the parent, to the entry's platform,
    created after it. The SQL form is the EXISTS in _RESTORE_READ_SQL."""
    return any(
        a.get("platform_id") == entry["platform_id"] and a["created_at"] > entry["created_at"]
        for a in attempts
    )


def restore_refusal(*, restored: bool, attempted_since: bool) -> Optional[DelistRefused]:
    if restored:
        return DelistRefused("Already restored", ALREADY_RESTORED)
    if attempted_since:
        return DelistRefused("Already resubmitted since this delist", RESUBMITTED)
    return None


# ---------------------------------------------------------------------------
# The history payload
# ---------------------------------------------------------------------------

_KIND_RANK = {"attempt": 0, "delist": 1, "restore": 2}


def _timeline(
    attempts: Sequence[Dict[str, Any]],
    entries: Sequence[Dict[str, Any]],
    labels: Dict[str, str],
) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    for a in attempts:
        accepted = is_accepted(a.get("status"), a.get("platform_status"), a.get("reviewed_at"))
        events.append(
            {
                "kind": "attempt",
                "at": a["created_at"],
                "user_id": a.get("submitted_by"),
                "submission_id": a.get("id"),
                "listing_id": a.get("listing_id"),
                "batch_id": a.get("batch_id"),
                "batch_comment": a.get("batch_comment"),
                "attempt_number": a.get("attempt_number"),
                "status": a.get("status"),
                "platform_status": a.get("platform_status"),
                "reviewed": a.get("reviewed_at") is not None,
                "accepted": accepted,
                "error": short_error(a.get("error")),
                "listed_at": attempt_listed_at(a) if accepted else None,
                "completed_at": a.get("completed_at"),
            }
        )
    for e in entries:
        restored = e.get("restored_at") is not None
        refusal = restore_refusal(restored=restored, attempted_since=attempted_since(e, attempts))
        child_skus = e.get("child_skus")
        sizes = [labels.get(s) or _size_label(s) for s in child_skus] if child_skus else None
        if restored:
            state = RESTORED
        elif e.get("is_open"):
            state = OPEN
        else:
            state = RELISTED
        events.append(
            {
                "kind": "delist",
                "at": e["created_at"],
                "user_id": e.get("created_by"),
                "delist_id": e["id"],
                "comment": e.get("comment"),
                "child_skus": child_skus,
                "sizes": sizes,
                "state": state,
                "can_restore": refusal is None,
                "restore_lock_reason": refusal.message if refusal else None,
            }
        )
        if restored:
            events.append(
                {
                    "kind": "restore",
                    "at": e["restored_at"],
                    "user_id": e.get("restored_by"),
                    "delist_id": e["id"],
                    "child_skus": child_skus,
                    "sizes": sizes,
                }
            )
    events.sort(key=lambda ev: (ev["at"], _KIND_RANK[ev["kind"]]), reverse=True)
    return events


def platform_view(
    platform_id: str,
    *,
    settings: Dict[str, Any],
    excluded_reasons: Sequence[str],
    size_list: Sequence[Dict[str, Any]],
    attempts: Sequence[Dict[str, Any]],
    rows: Sequence[Dict[str, Any]],
    entries: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    """One platform's row of the history payload (module docstring)."""
    attempts = [a for a in attempts if a.get("platform_id") == platform_id]
    rows = [r for r in rows if r.get("platform_id") == platform_id]
    entries = [e for e in entries if e.get("platform_id") == platform_id]
    facts = platform_facts(platform_id, size_list, attempts, rows, entries)
    excluded = bool(excluded_reasons)
    listed_at, recorded = listed_date(attempts, rows)
    refusal = delist_refusal(platform_id, enabled=True, excluded=excluded, facts=facts)
    labels = {s["sku"]: s["size"] for s in size_list}
    timeline = _timeline(attempts, entries, labels)
    open_ids = [e["id"] for e in facts.open_entries]
    by_id = {ev["delist_id"]: ev for ev in timeline if ev["kind"] == "delist"}
    return {
        "platform_id": platform_id,
        "label": platform_label(platform_id),
        "read_only": platform_id in NEVER_GATED,
        "status": row_status_for(facts, excluded),
        # The fact, separate from the status. A platform can be excluded and listed at the
        # same time, so the status cannot carry this and the UI must not re-derive it from
        # excluded_reasons (the API decides what can be decided).
        "excluded": excluded,
        "excluded_reasons": sorted(excluded_reasons),
        "listed_at": listed_at,
        "listed_at_recorded": recorded,
        "allow_partial_submit": bool((settings or {}).get("allow_partial_submit", False)),
        "can_delist": refusal is None,
        "lock_reason": refusal.message if refusal else None,
        "lock_code": refusal.code if refusal else None,
        "sizes": facts.states,
        "open_delists": [dict(by_id[i]) for i in open_ids if i in by_id],
        "timeline": timeline,
    }


def build_history(
    parent_sku: str,
    *,
    platforms: Sequence[str],
    settings: Dict[str, Any],
    product: ProductFacts,
    exclusions: Dict[str, Sequence[str]],
    attempts: Sequence[Dict[str, Any]],
    rows: Sequence[Dict[str, Any]],
    entries: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    """The whole payload from what get_history read. Pure."""
    shown = list(platforms)
    shown_set = set(shown)
    sizes = size_list(
        product,
        [r for r in rows if r.get("platform_id") in shown_set],
        [e for e in entries if e.get("platform_id") in shown_set],
    )
    return {
        "parent_sku": parent_sku,
        "sizes": sizes,
        "platforms": [
            platform_view(
                p,
                settings=settings.get(p) or {},
                excluded_reasons=exclusions.get(p) or [],
                size_list=sizes,
                attempts=attempts,
                rows=rows,
                entries=entries,
            )
            for p in shown
        ],
    }


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------

# Every attempt on every listing of the parent. $2 NULL means every platform.
_ATTEMPTS_SQL = """
SELECT s.id, s.listing_id::text AS listing_id, l.batch_id, b.comment AS batch_comment,
       l.created_at AS listing_created_at, s.platform_id, s.status, s.platform_status,
       s.reviewed_at, s.attempt_number, s.created_at, s.updated_at, s.completed_at,
       s.submitted_by, COALESCE(NULLIF(s.error_display, ''), s.error) AS error,
       s.platform_meta -> 'steps' AS steps
  FROM listings l
  JOIN listing_submissions s ON s.listing_id = l.id
  LEFT JOIN batches b ON b.id = l.batch_id
 WHERE l.product_id = $1
   AND ($2::text IS NULL OR s.platform_id = $2::text)
 ORDER BY s.created_at, s.id
"""

_PRESENCE_SQL = """
SELECT e.id::text AS id, e.platform_id, e.level, e.sku, e.first_seen_at
  FROM external_listing_ids e
 WHERE e.parent_sku = $1
   AND ($2::text IS NULL OR e.platform_id = $2::text)
 ORDER BY e.platform_id, e.level DESC, e.sku
"""

# is_open comes from open_platform_delists(), the only definition of an open delist.
_ENTRIES_SQL = """
SELECT d.id::text AS id, d.platform_id, d.parent_sku, d.child_skus, d.comment,
       d.created_by, d.created_at, d.restored_at, d.restored_by,
       jsonb_array_length(d.removed_rows) AS removed_count,
       d.id IN (SELECT o.id FROM open_platform_delists() o WHERE o.parent_sku = $1) AS is_open
  FROM platform_delists d
 WHERE d.parent_sku = $1
   AND ($2::text IS NULL OR d.platform_id = $2::text)
 ORDER BY d.created_at
"""


async def _read_state(
    conn: Any, parent_sku: str, platform_id: Optional[str], lock_rows: bool = False
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """(attempts, presence rows, entries) for the parent, optionally one platform. With
    lock_rows the presence rows are locked until the transaction ends."""
    attempts = await conn.execute_query_dict(_ATTEMPTS_SQL, [parent_sku, platform_id])
    presence_sql = _PRESENCE_SQL + (" FOR UPDATE" if lock_rows else "")
    rows = await conn.execute_query_dict(presence_sql, [parent_sku, platform_id])
    entries = await conn.execute_query_dict(_ENTRIES_SQL, [parent_sku, platform_id])
    return list(attempts), list(rows), list(entries)


async def _size_rank(scheme: Optional[str]) -> Dict[str, Any]:
    """size -> order within the parent's sizing scheme. Best effort: a failure only costs
    the order, as in ProductService.apply_size_sort."""
    if not scheme:
        return {}
    try:
        rows = await connections.get("default").execute_query_dict(
            'SELECT size, "order" FROM listingoptions_sizing_schemes WHERE sizing_scheme = $1',
            [scheme],
        )
    except Exception:
        logger.warning("size order unavailable for scheme %s", scheme, exc_info=True)
        return {}
    return {r["size"]: r["order"] for r in rows if r.get("order") is not None}


async def _load_product(parent_sku: str) -> Optional[ProductFacts]:
    """The parent and its children, from the products DB. None if the parent is unknown."""
    conn = connections.get("product_db")
    parents = await conn.execute_query_dict(
        "SELECT sku, brand, product_type, company_code, sizing_scheme "
        "FROM parent_products WHERE sku = $1",
        [parent_sku],
    )
    if not parents:
        return None
    parent = parents[0]
    children = await conn.execute_query_dict(
        "SELECT sku, size, is_active FROM child_products WHERE parent_sku = $1",
        [parent_sku],
    )
    ordered = order_children(children, await _size_rank(parent.get("sizing_scheme")))
    return ProductFacts(
        parent_sku=parent_sku,
        brand=parent.get("brand"),
        product_type=parent.get("product_type"),
        company_code=parent.get("company_code"),
        active=[{"sku": c["sku"], "size": c["size"]} for c in ordered if c["is_active"]],
        inactive={c["sku"]: c["size"] for c in ordered if not c["is_active"]},
    )


async def _load_platform_config() -> Tuple[List[str], Dict[str, Any]]:
    """(app_settings.platforms, app_settings.platform_settings)."""
    rows = await connections.get("default").execute_query_dict(
        "SELECT platforms, platform_settings FROM app_settings ORDER BY id LIMIT 1"
    )
    if not rows:
        return [], {}
    platforms = _as_json(rows[0].get("platforms")) or []
    settings = _as_json(rows[0].get("platform_settings")) or {}
    return [str(p) for p in platforms], settings


async def _exclusions(product: ProductFacts, platforms: Iterable[str]) -> Dict[str, List[str]]:
    """{platform: reasons}, by the catalog's rules (catalog_service.exclusions_for)."""
    index = catalog_service.rule_index(await catalog_service.exclusion_rules())
    item = Product(
        sku=product.parent_sku,
        title="",
        mpn=None,
        brand=product.brand,
        product_type=product.product_type,
        company_code=product.company_code,
        created_at=None,
        haystack="",
    )
    return catalog_service.exclusions_for(item, index, set(platforms))


def _not_found() -> DelistRefused:
    return DelistRefused("Product not found", NOT_FOUND, 404)


async def get_history(parent_sku: str) -> Dict[str, Any]:
    """The history payload for a PARENT sku (the route resolves a child first).

    The three read groups are independent and read-only, so they run together: each
    query outside a transaction takes its own pooled connection. Never do this with a
    transaction's conn (delist, restore), which runs one statement at a time. An
    unknown product still answers 404 before any other read's error, as it did when
    the reads ran one after another."""

    async def read_state():
        return await _read_state(connections.get("default"), parent_sku, None)

    product, config, state = await asyncio.gather(
        _load_product(parent_sku), _load_platform_config(), read_state(), return_exceptions=True
    )
    if isinstance(product, BaseException):
        raise product
    if product is None:
        raise _not_found()
    for result in (config, state):
        if isinstance(result, BaseException):
            raise result
    platforms, settings = config
    attempts, rows, entries = state
    exclusions = await _exclusions(product, platforms)
    return build_history(
        parent_sku,
        platforms=platforms,
        settings=settings,
        product=product,
        exclusions=exclusions,
        attempts=attempts,
        rows=rows,
        entries=entries,
    )


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------

# The pre-image is taken in the same statement that creates the entry, straight from the
# rows (locked by the read), so it is exactly what Restore puts back. clock_timestamp(), not
# now(): the entry is created after the lock wait, and "created after the entry" compares
# against attempts whose created_at is the wall clock at insert.
_INSERT_ENTRY_SQL = """
INSERT INTO platform_delists
       (platform_id, parent_sku, child_skus, comment, created_by, created_at, removed_rows)
SELECT $1, $2, $3::text[], $4, $5, clock_timestamp(),
       COALESCE((SELECT jsonb_agg(to_jsonb(e) ORDER BY e.level DESC, e.sku)
                   FROM external_listing_ids e
                  WHERE e.id = ANY($6::text[]::uuid[])), '[]'::jsonb)
RETURNING id::text AS id, created_at
"""

_DELETE_ROWS_SQL = "DELETE FROM external_listing_ids WHERE id = ANY($1::text[]::uuid[])"

# KTD2 split: child rows for the remaining sizes, recorded on the entry in the same
# statement. DO NOTHING on a row that already exists, so only rows this entry created are
# recorded and later deleted by Restore. first_seen_at is the parent row's: the sizes were
# listed since then, and the presence-only listed date must not jump to today.
_SPLIT_SQL = """
WITH ins AS (
    INSERT INTO external_listing_ids AS e
           (platform_id, level, sku, parent_sku, external_id, external_meta, source,
            first_seen_at)
    SELECT $1, 'child', s.sku, $2, NULL, $4::jsonb, 'manual', COALESCE($6::timestamptz, now())
      FROM unnest($3::text[]) AS s(sku)
    ON CONFLICT (platform_id, level, sku) DO NOTHING
    RETURNING e.*
)
UPDATE platform_delists d
   SET inserted_rows = (SELECT COALESCE(jsonb_agg(to_jsonb(ins) ORDER BY ins.sku), '[]'::jsonb)
                          FROM ins)
 WHERE d.id = $5::uuid
RETURNING jsonb_array_length(d.inserted_rows) AS inserted
"""


def _entry_out(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": str(row["id"]),
        "platform_id": row["platform_id"],
        "parent_sku": row["parent_sku"],
        "child_skus": row.get("child_skus"),
        "comment": row.get("comment"),
        "created_by": row.get("created_by"),
        "created_at": row.get("created_at"),
        "restored_at": row.get("restored_at"),
        "restored_by": row.get("restored_by"),
    }


def _clear_summary_cache() -> None:
    """KTD11: the catalog's counts would otherwise lag the change by up to a minute."""
    catalog_service._summary_cache.clear()


async def delist(
    parent_sku: str,
    platform_id: str,
    child_skus: Optional[Sequence[str]],
    comment: Optional[str],
    user_id: str,
) -> Dict[str, Any]:
    """Record that `platform_id` no longer lists `child_skus` of the parent (None: all).

    One transaction under the submit lock. Raises DelistRefused. Never calls a platform.
    """
    comment = normalize_comment(comment)
    if platform_id in NEVER_GATED:
        raise _read_only(platform_id)
    product = await _load_product(parent_sku)
    if product is None:
        raise _not_found()
    platforms, settings = await _load_platform_config()
    enabled = platform_id in platforms
    if not enabled:
        raise DelistRefused("Unknown platform", UNKNOWN_PLATFORM, 400)
    excluded = bool((await _exclusions(product, [platform_id])).get(platform_id))
    allow_partial = bool((settings.get(platform_id) or {}).get("allow_partial_submit", False))

    async with in_transaction("default") as conn:
        await conn.execute_query(SUBMIT_LOCK_SQL, [parent_sku])
        attempts, rows, entries = await _read_state(conn, parent_sku, platform_id, lock_rows=True)
        sizes = size_list(product, rows, entries)
        facts = platform_facts(platform_id, sizes, attempts, rows, entries)
        try:
            plan = plan_delist(
                platform_id,
                child_skus,
                enabled=enabled,
                allow_partial=allow_partial,
                excluded=excluded,
                size_list=sizes,
                facts=facts,
                rows=rows,
            )
        except DelistRefused as refused:
            logger.info(
                "delist refused: %s %s sizes=%s by %s: %s",
                platform_id,
                parent_sku,
                child_skus,
                user_id,
                refused.code,
            )
            raise

        created = (
            await conn.execute_query_dict(
                _INSERT_ENTRY_SQL,
                [platform_id, parent_sku, plan.scope, comment, user_id, plan.remove_ids],
            )
        )[0]
        entry_id = str(created["id"])
        if plan.remove_ids:
            await conn.execute_query(_DELETE_ROWS_SQL, [plan.remove_ids])
        inserted = 0
        if plan.insert_skus:
            parent_row = next((r for r in rows if r.get("level") == "parent"), None)
            meta = {"delist_id": entry_id, "split_from": parent_row["id"] if parent_row else None}
            result = await conn.execute_query_dict(
                _SPLIT_SQL,
                [
                    platform_id,
                    parent_sku,
                    plan.insert_skus,
                    json.dumps(meta),
                    entry_id,
                    parent_row.get("first_seen_at") if parent_row else None,
                ],
            )
            inserted = result[0]["inserted"] if result else 0

    _clear_summary_cache()
    logger.info(
        "delist %s: %s %s sizes=%s by %s, removed %d rows, inserted %d",
        entry_id,
        platform_id,
        parent_sku,
        plan.scope,
        user_id,
        len(plan.remove_ids),
        inserted,
    )
    return _entry_out(
        {
            "id": entry_id,
            "platform_id": platform_id,
            "parent_sku": parent_sku,
            "child_skus": plan.scope,
            "comment": comment,
            "created_by": user_id,
            "created_at": created.get("created_at"),
        }
    )


_RESTORE_READ_SQL = """
SELECT d.id::text AS id, d.platform_id, d.parent_sku, d.child_skus, d.comment,
       d.created_by, d.created_at, d.restored_at, d.restored_by,
       EXISTS (
           SELECT 1
             FROM listings l
             JOIN listing_submissions s ON s.listing_id = l.id
            WHERE l.product_id = d.parent_sku
              AND s.platform_id = d.platform_id
              AND s.created_at > d.created_at
       ) AS attempted_since
  FROM platform_delists d
 WHERE d.id = $1::uuid
   FOR UPDATE OF d
"""

# Marked FIRST, so every presence trigger below already sees this entry closed.
_MARK_RESTORED_SQL = """
UPDATE platform_delists
   SET restored_at = clock_timestamp(), restored_by = $2
 WHERE id = $1::uuid AND restored_at IS NULL
RETURNING id::text AS id, platform_id, parent_sku, child_skus, comment, created_by,
          created_at, restored_at, restored_by
"""

_DELETE_INSERTED_SQL = """
DELETE FROM external_listing_ids e
 USING platform_delists d
 WHERE d.id = $1::uuid
   AND e.platform_id = d.platform_id
   AND e.parent_sku = d.parent_sku
   AND e.id IN (SELECT (r ->> 'id')::uuid FROM jsonb_array_elements(d.inserted_rows) r)
"""

# Exactly the pre-image, id and timestamps included. A row re-added meanwhile (a relist
# capture, a backfill) is kept; it only takes the earlier first_seen_at, so that still
# means "first seen".
#
# Except a row that an EARLIER entry's split created, where that entry is already restored.
# Stacked per-size entries: A splits the parent row into child rows, B removes one of
# them. Restoring A deletes A's rows and puts the parent row back; restoring B afterwards
# must not resurrect A's child row next to it. Restored in either order, the rows end up
# exactly as before A.
_REINSERT_SQL = """
INSERT INTO external_listing_ids
SELECT r.*
  FROM platform_delists d,
       jsonb_populate_recordset(NULL::external_listing_ids, d.removed_rows) r
 WHERE d.id = $1::uuid
   AND NOT EXISTS (
       SELECT 1
         FROM platform_delists o
        WHERE o.platform_id = d.platform_id
          AND o.parent_sku = d.parent_sku
          AND o.restored_at IS NOT NULL
          AND o.inserted_rows @> jsonb_build_array(jsonb_build_object('id', r.id))
   )
ON CONFLICT (platform_id, level, sku) DO UPDATE
   SET first_seen_at = LEAST(external_listing_ids.first_seen_at, EXCLUDED.first_seen_at)
"""

# p_allow_unflag false: a restore can only satisfy platforms. Id order, as the trigger in
# delist_aware_listing_completion.sql, so concurrent recomputes lock listings alike.
# The inner ORDER BY sorts before the volatile call is evaluated, and a volatile target list
# keeps the subquery from being flattened or its column dropped.
_RECOMPUTE_SQL = """
SELECT count(*) AS recomputed
  FROM (SELECT recompute_listing_submitted(l.id, false)
          FROM listings l
         WHERE l.product_id = $1
         ORDER BY l.id) r
"""


async def restore(delist_id: str, user_id: str) -> Dict[str, Any]:
    """Undo one delist entry exactly (R18). Raises DelistRefused."""
    try:
        delist_id = str(uuid.UUID(str(delist_id)))
    except ValueError:
        raise DelistRefused("Delist not found", NOT_FOUND, 404)

    async with in_transaction("default") as conn:
        found = await conn.execute_query_dict(
            "SELECT parent_sku FROM platform_delists WHERE id = $1::uuid", [delist_id]
        )
        if not found:
            raise DelistRefused("Delist not found", NOT_FOUND, 404)
        parent_sku = found[0]["parent_sku"]
        await conn.execute_query(SUBMIT_LOCK_SQL, [parent_sku])

        entry = (await conn.execute_query_dict(_RESTORE_READ_SQL, [delist_id]))[0]
        refused = restore_refusal(
            restored=entry.get("restored_at") is not None,
            attempted_since=bool(entry.get("attempted_since")),
        )
        if refused:
            logger.info("restore refused: %s by %s: %s", delist_id, user_id, refused.code)
            raise refused

        marked = (await conn.execute_query_dict(_MARK_RESTORED_SQL, [delist_id, user_id]))[0]
        await conn.execute_query(_DELETE_INSERTED_SQL, [delist_id])
        await conn.execute_query(_REINSERT_SQL, [delist_id])
        await conn.execute_query(_RECOMPUTE_SQL, [parent_sku])

    _clear_summary_cache()
    logger.info("restore %s: %s %s by %s", delist_id, marked["platform_id"], parent_sku, user_id)
    return _entry_out(marked)
