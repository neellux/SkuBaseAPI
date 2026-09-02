"""The global work queue: pending products across open batches, most valuable first.

The batch list answers "which batch next". This answers "which product next", across
every open batch at once, ordered by the merchandise value on the batch - taken at
creation and refreshed nightly for products that are still pending
(batch_value_service.BatchValueRefreshPoller). Nothing here writes.

Value is read from batches.product_values, the per-parent-SKU breakdown that
batch_value_service.aggregate_values stores:

    {"<parent_sku>": {"value": 680.0, "qty": 2, "children": 4, "priced": 4}}

There is no per-listing value column, so the ordering key is a jsonb extraction
correlated across two tables (b.product_values -> l.product_id). Tortoise has no
expression for that and no window function for the rank the queue strip needs, so
this module talks raw SQL over the default connection, the way batch_value_service
already does against product_db.

Cheap at current volume: 322 queue members out of 4,087 listings, 9.5 ms for page 1
measured against production on 2026-09-01 (hash join over 76 open batches, top-N
heapsort). If listings grows an order of magnitude the escape hatch is an expression
index or a denormalised listings.value column, neither of which is needed yet.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from tortoise import connections

from services.batch_service import OPEN_BATCH_STATUSES
from services.product_resolver import resolve_parents

logger = logging.getLogger(__name__)


# The one definition of pending work, shared by every query below and by the nightly
# value refresh in batch_value_service. A second copy of this predicate anywhere is a bug
# waiting to happen: what counts as pending work has to mean the same thing to the table,
# the strip, the arrows and the job that re-prices it.
PENDING_ROWS = """
FROM listings l
JOIN batches b ON b.id = l.batch_id
WHERE NOT l.submitted
  AND b.status = ANY($1::text[])
"""


# created_at is the LISTING's, because it is both the tie-break and the age the row
# shows. The date-range filter deliberately reads the BATCH's instead (see
# build_filters), so that switching between the two views keeps a date filter meaning
# what it meant on the batch cards.
QUEUE_SELECT = f"""
SELECT
  l.id                      AS listing_id,
  l.product_id,
  l.batch_id,
  l.created_at,
  l.data ->> 'title'        AS title,
  l.data ->> 'brand_name'   AS brand_name,
  l.data ->> 'product_type' AS product_type,
  b.priority,
  -- The batch's assignee, not listings.assigned_to. It is what the batch card shows
  -- and what the batch list's assignee filter means, so the view toggle stays
  -- coherent when a filter carries across it.
  b.assigned_to,
  -- jsonb_typeof guard rather than a bare ::numeric cast. A product missing from
  -- product_values, or holding anything non-numeric, has to become NULL and sort
  -- last; a cast would raise and take the whole page down with it.
  CASE WHEN jsonb_typeof(b.product_values -> l.product_id -> 'value') = 'number'
       THEN (b.product_values -> l.product_id ->> 'value')::numeric END AS value,
  (b.product_values -> l.product_id ->> 'qty')::int      AS qty,
  (b.product_values -> l.product_id ->> 'children')::int AS children,
  (b.product_values -> l.product_id ->> 'priced')::int   AS priced
{PENDING_ROWS}"""

# The single source of truth for order. The page query and the rank window both read
# it, the way BATCH_SORTS and _sorts_after share one tuple in batch_service: an
# ordering that disagrees with the cursor walking it silently skips or repeats rows.
#
# NULLS LAST is load-bearing. batches.total_value is NOT NULL precisely so a plain
# DESC works there; a per-product value can genuinely be absent, and Postgres puts
# NULLs first on DESC, which would park every unvalued product at the top of the queue.
#
# The tie-break runs oldest-first, deliberately unlike BATCH_SORTS which is
# newest-first: 55 pending products are valued at exactly 0, so the tail needs a
# deterministic order, and a work queue should age its backlog out rather than bury
# it. listing_id last keeps offset pagination stable across a tie.
QUEUE_ORDER = "value DESC NULLS LAST, created_at ASC, listing_id"


def build_filters(
    params: List[Any],
    assigned_to: Optional[List[str]] = None,
    priority: Optional[List[str]] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    search_exact: Optional[str] = None,
    search_like: Optional[str] = None,
) -> str:
    """Extra WHERE fragment for the queue, appending its parameters to `params`.

    Pure and separated from the I/O so the placeholder numbering can be tested without
    a database, the way batch_value_service splits aggregate_values out from the
    fetch. `params` is mutated in order; placeholders are numbered from its current
    length, so the caller must have already put the open-batch statuses in as $1.

    assigned_to, priority and the date range all filter the BATCH, matching
    BatchService.get_all_batches, so a filter carried across the view toggle keeps
    meaning what it meant on the cards.

    Search is the one filter that does not copy the batch version. There it resolves a
    term to a parent SKU and then selects the batches CONTAINING a matching listing;
    here it has to match the listing itself, so the caller resolves first (see
    resolve_search) and passes exactly one of search_exact / search_like.
    """
    clauses: List[str] = []

    def placeholder(value: Any) -> str:
        params.append(value)
        return f"${len(params)}"

    if assigned_to:
        clauses.append(f"AND b.assigned_to = ANY({placeholder(list(assigned_to))}::text[])")

    if priority:
        clauses.append(f"AND b.priority = ANY({placeholder(list(priority))}::text[])")

    if date_from:
        clauses.append(f"AND b.created_at >= {placeholder(date_from)}")

    if date_to:
        # Exclusive upper bound a day out, so a date_to of the 5th includes the whole
        # of the 5th. Same arithmetic as BatchService.get_all_batches.
        clauses.append(f"AND b.created_at < {placeholder(date_to + timedelta(days=1))}")

    # ILIKE with no wildcards is an exact case-insensitive match, so both search modes
    # share one operator and differ only in the value bound to it.
    if search_exact:
        clauses.append(f"AND l.product_id ILIKE {placeholder(search_exact)}")
    elif search_like:
        clauses.append(f"AND l.product_id ILIKE {placeholder(f'%{search_like}%')}")

    return "\n  ".join(clauses)


async def resolve_search(search: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """(exact, like) for a raw search term.

    listings.product_id holds the parent, so a fully typed child SKU has to be resolved
    before it can match; chopping it would invent a parent, and an ESSX parent contains
    slashes of its own. Resolve on the raw term rather than the upper-cased one so an
    exact hit takes the exact path. A miss is normal (partial input, a typo), so fall
    back to a substring match and keep search forgiving. Mirrors the resolution half of
    BatchService.get_all_batches.
    """
    raw = (search or "").strip()
    if not raw:
        return None, None

    try:
        resolved = await resolve_parents([raw])
    except Exception:  # noqa: BLE001
        # A SellerCloud or products-DB hiccup must not take search down with it; the
        # substring fallback still answers most of what operators type.
        logger.warning("Search resolution failed for %r, falling back to substring", raw)
        return None, raw.upper()

    if raw in resolved:
        return resolved[raw], None
    return None, raw.upper()


def _row_out(row: Dict[str, Any]) -> Dict[str, Any]:
    """One raw row shaped for the response model.

    asyncpg hands back a UUID object for listing_id and pydantic will not coerce that
    to str on its own, so it is converted here rather than at every call site.
    """
    out = dict(row)
    out["listing_id"] = str(out["listing_id"])
    return out


async def get_queue_page(
    page: int = 1,
    page_size: int = 20,
    assigned_to: Optional[List[str]] = None,
    priority: Optional[List[str]] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    search: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """One page of the queue, most valuable first.

    Returns a bare list rather than a (rows, total) pair: the table drives itself off
    useInfiniteList, which infers "there is more" from a full page, and the header
    strip gets its counts from get_queue_summary instead. Same shape as
    GET /listings/batches.
    """
    params: List[Any] = [list(OPEN_BATCH_STATUSES)]
    exact, like = await resolve_search(search)
    where = build_filters(
        params,
        assigned_to=assigned_to,
        priority=priority,
        date_from=date_from,
        date_to=date_to,
        search_exact=exact,
        search_like=like,
    )

    limit = f"${len(params) + 1}"
    params.append(page_size)
    offset = f"${len(params) + 1}"
    params.append((page - 1) * page_size)

    sql = f"""
WITH queue AS (
{QUEUE_SELECT}
  {where}
)
SELECT * FROM queue
ORDER BY {QUEUE_ORDER}
LIMIT {limit} OFFSET {offset}
"""
    conn = connections.get("default")
    rows = await conn.execute_query_dict(sql, params)
    return [_row_out(row) for row in rows]


async def get_queue_around(
    listing_id: str,
    before: int = 2,
    after: int = 6,
) -> Tuple[List[Dict[str, Any]], Optional[int], int]:
    """(rows, position, total) for the window around one listing.

    Feeds the queue strip, the arrows and the position readout in one request.

    Deliberately takes no filters. Table filters narrow the table only, never the walk,
    so an operator who has filtered to one assignee still advances to the most valuable
    product on the floor.

    `position` is the anchor's rank, or None when the anchor is no longer in the queue
    (it was just submitted, or a finished listing was opened to review it). In that
    case the COALESCE below makes the window the front of the line, which is exactly
    where the operator is about to go. Keying on the listing id rather than
    reconstructing an ordering tuple is what buys that: no _sorts_after style OR
    expansion, and the not-in-queue case falls out for free.
    """
    params: List[Any] = [list(OPEN_BATCH_STATUSES), listing_id, before, after]

    sql = f"""
WITH queue AS (
{QUEUE_SELECT}
),
ranked AS (
  SELECT *,
         row_number() OVER (ORDER BY {QUEUE_ORDER}) AS rank,
         count(*)     OVER ()                       AS total
  FROM queue
),
anchor AS (
  SELECT rank FROM ranked WHERE listing_id = $2::uuid
)
SELECT r.* FROM ranked r
WHERE r.rank BETWEEN COALESCE((SELECT rank FROM anchor), 1) - $3
                 AND COALESCE((SELECT rank FROM anchor), 1) + $4
ORDER BY r.rank
"""
    conn = connections.get("default")
    rows = await conn.execute_query_dict(sql, params)

    if not rows:
        return [], None, 0

    total = int(rows[0]["total"])
    position = None
    out: List[Dict[str, Any]] = []
    for row in rows:
        shaped = _row_out(row)
        shaped.pop("total", None)
        shaped["rank"] = int(shaped["rank"])
        if shaped["listing_id"] == str(listing_id):
            position = shaped["rank"]
        out.append(shaped)

    return out, position, total


async def get_queue_summary(
    assigned_to: Optional[List[str]] = None,
    priority: Optional[List[str]] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    search: Optional[str] = None,
) -> Dict[str, Any]:
    """Counts and total value behind the header strip, under the same filters.

    unvalued and zero_valued are separate and are never summed into one number. A
    product with no entry in product_values and a product SellerCloud priced at 0 are
    different conditions with different fixes, and the row treatment distinguishes
    them.

    total_value double-counts a product sitting in two open batches (DNT-MBTM-0061 is
    in 810 and 814). Both are real work and both belong in the queue, so this is a
    known imprecision in the sum, not in the count.
    """
    params: List[Any] = [list(OPEN_BATCH_STATUSES)]
    exact, like = await resolve_search(search)
    where = build_filters(
        params,
        assigned_to=assigned_to,
        priority=priority,
        date_from=date_from,
        date_to=date_to,
        search_exact=exact,
        search_like=like,
    )

    sql = f"""
WITH queue AS (
{QUEUE_SELECT}
  {where}
)
SELECT count(*)                              AS count,
       COALESCE(sum(value), 0)               AS total_value,
       count(*) FILTER (WHERE value IS NULL) AS unvalued,
       count(*) FILTER (WHERE value = 0)     AS zero_valued
FROM queue
"""
    conn = connections.get("default")
    rows = await conn.execute_query_dict(sql, params)
    return dict(rows[0]) if rows else {
        "count": 0,
        "total_value": 0,
        "unvalued": 0,
        "zero_valued": 0,
    }


# ---------------------------------------------------------------------------
# The nightly value refresh's worklist
#
# Lives here rather than in batch_value_service so that the products re-priced every
# night and the products this queue shows are selected by the same PENDING_ROWS, not by
# two predicates that agree today.
# ---------------------------------------------------------------------------

REFRESH_TARGETS = f"""
SELECT l.batch_id,
       b.value_computed_at,
       array_agg(DISTINCT l.product_id) AS parents
{PENDING_ROWS}
  AND l.product_id IS NOT NULL
GROUP BY l.batch_id, b.value_computed_at
ORDER BY l.batch_id
"""


async def get_refresh_targets() -> List[Dict[str, Any]]:
    """Open batches holding pending work, each with its distinct pending parent SKUs.

    A parent is pending if ANY of its listings in that batch is unsubmitted, which is what
    array_agg over the filtered rows gives - the right answer for the few batches holding
    duplicate listings of one parent.
    """
    conn = connections.get("default")
    rows = await conn.execute_query_dict(REFRESH_TARGETS, [list(OPEN_BATCH_STATUSES)])
    return [
        {
            "batch_id": row["batch_id"],
            "value_computed_at": row["value_computed_at"],
            "parents": list(row["parents"] or []),
        }
        for row in rows
    ]


async def get_all_parents(batch_ids: List[int]) -> Dict[int, List[str]]:
    """Every parent SKU on each batch, submitted or not.

    Two callers, neither of them the queue: a batch whose creation snapshot failed has
    nothing frozen worth preserving and must be valued whole, and the refresher needs to
    know which product_values entries no longer belong to the batch at all. A plain GROUP
    BY over listings - no join, no status filter - because the caller has already chosen
    the batches.
    """
    if not batch_ids:
        return {}
    conn = connections.get("default")
    rows = await conn.execute_query_dict(
        "SELECT batch_id, array_agg(DISTINCT product_id) AS parents "
        "FROM listings WHERE batch_id = ANY($1::int[]) AND product_id IS NOT NULL "
        "GROUP BY batch_id",
        [list(batch_ids)],
    )
    return {row["batch_id"]: list(row["parents"] or []) for row in rows}
