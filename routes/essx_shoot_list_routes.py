"""The ESSX Store shoot list page.

GET /essx_shoot_list/products   one page of parents from essx_shoot_list, by value
GET /essx_shoot_list/summary    counts per state for the header and the state toggle

Query parameters only. One row per PARENT: photography shoots parents, and every size of
a parent shares one state (services/essx_shoot_list_service.py). Read-only. Registered
with the auth service under view_batches, like the catalog GETs.
"""

import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from tortoise import connections

from services.essx_shoot_list_service import HAS_IMAGES, IN_BATCH, SHOT, TO_SHOOT

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/essx_shoot_list", tags=["essx_shoot_list"])

STATES = (TO_SHOOT, SHOT, IN_BATCH, HAS_IMAGES)

# Whitelist: the request's sort value picks a clause, never text from the request.
SORTS = {
    "value_desc": "g.value DESC, g.parent",
    # Units across the three ESSX warehouses (negatives as 0), the same qty the value uses.
    "qty_desc": "g.total_qty DESC, g.value DESC, g.parent",
    "session_newest": "g.session_started_at DESC NULLS LAST, g.value DESC, g.parent",
    "newest": "g.first_seen_at DESC, g.value DESC, g.parent",
    "sku": "g.parent",
}

# Parents, not rows. A size kept after it left the store (it carries a session id) keeps
# its old as_of; `at_store` says whether any size of the parent was in the latest refresh.
GROUPED_CTE = """
WITH latest AS (SELECT max(as_of) AS as_of, max(source_job_id) AS job FROM essx_shoot_list),
g AS (
    SELECT COALESCE(t.parent_sku, t.sku) AS parent,
           bool_or(t.parent_sku IS NOT NULL) AS known_parent,
           (array_agg(t.product_name ORDER BY t.sku))[1] AS product_name,
           (array_agg(t.brand ORDER BY t.sku))[1] AS brand,
           (array_agg(t.product_type ORDER BY t.sku))[1] AS product_type,
           (array_agg(t.company ORDER BY t.sku))[1] AS company,
           (array_agg(t.state ORDER BY t.as_of DESC, t.sku))[1] AS state,
           (array_agg(t.session_id::text ORDER BY t.session_started_at DESC NULLS LAST))[1]
               AS session_id,
           max(t.session_started_at) AS session_started_at,
           max(t.photo_batch_id) AS photo_batch_id,
           COALESCE(sum(t.qty_essx_store) FILTER (WHERE t.as_of = l.as_of), 0)::int AS store_qty,
           COALESCE(sum(GREATEST(t.qty_total, 0)) FILTER (WHERE t.as_of = l.as_of), 0)::int
               AS total_qty,
           COALESCE(sum(t.value), 0) AS value,
           count(*)::int AS size_count,
           min(t.first_seen_at) AS first_seen_at,
           bool_or(t.as_of = l.as_of) AS at_store,
           json_agg(json_build_object(
               'sku', t.sku, 'store', t.qty_essx_store, 'total', t.qty_total,
               'list_price', t.list_price, 'value', t.value, 'at_store', t.as_of = l.as_of
           ) ORDER BY t.sku) AS sizes,
           lower(string_agg(concat_ws(' ', t.sku, t.parent_sku, t.product_name, t.brand,
                                      t.product_type, t.session_id::text), ' ')) AS haystack
      FROM essx_shoot_list t CROSS JOIN latest l
     GROUP BY 1
)
"""


class ShootListSize(BaseModel):
    sku: str
    size: str
    store: int
    total: int
    list_price: Optional[float] = None
    value: float
    at_store: bool


class ShootListRow(BaseModel):
    parent: str
    known_parent: bool
    title: Optional[str] = None
    brand: Optional[str] = None
    product_type: Optional[str] = None
    company: Optional[str] = None
    state: str
    session_id: Optional[str] = None
    session_started_at: Optional[datetime] = None
    photo_batch_id: Optional[int] = None
    store_qty: int
    total_qty: int
    value: float
    size_count: int
    first_seen_at: datetime
    at_store: bool
    sizes: List[ShootListSize]


class ShootListStateCount(BaseModel):
    parents: int = 0
    skus: int = 0
    value: float = 0


class ShootListSummary(BaseModel):
    count: int
    total_value: float
    states: Dict[str, ShootListStateCount]
    refreshed_at: Optional[datetime] = None
    source_job_id: Optional[int] = None


def _where(
    search: Optional[str], states: List[str], has_session: Optional[bool]
) -> Tuple[str, List[Any]]:
    """WHERE clause over the grouped CTE `g`, with $n parameters."""
    clauses: List[str] = []
    params: List[Any] = []
    if search:
        escaped = search.lower().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        params.append(f"%{escaped}%")
        clauses.append(f"g.haystack LIKE ${len(params)}")
    if states:
        params.append(states)
        clauses.append(f"g.state = ANY(${len(params)}::text[])")
    if has_session is True:
        clauses.append("g.session_id IS NOT NULL")
    elif has_session is False:
        clauses.append("g.session_id IS NULL")
    return (" WHERE " + " AND ".join(clauses)) if clauses else "", params


def _check_states(state: Optional[List[str]]) -> List[str]:
    states = [s for s in (state or []) if s]
    if any(s not in STATES for s in states):
        raise HTTPException(status_code=400, detail="Unknown shoot list state")
    return states


_SIZE_SUFFIX = re.compile(r"\s+SIZE\s+\S.*$", re.IGNORECASE)


def clean_title(name: Optional[str], sku: Optional[str] = None) -> Optional[str]:
    """A size's SellerCloud ProductName made into a title, for parents SkuBase has no title
    for. Drops a leading copy of the SKU and the "SIZE 48 $1755" tail."""
    if not name:
        return None
    text = name.strip()
    if sku and text.lower().startswith(sku.lower()):
        text = text[len(sku):].strip()
    return _SIZE_SUFFIX.sub("", text).strip() or name


class PageLookups(BaseModel):
    """What the products and listing-options tables add to one page of rows."""

    titles: Dict[str, str] = {}
    schemes: Dict[str, str] = {}  # parent -> sizing scheme
    child_sizes: Dict[str, str] = {}  # lowercased child sku -> child_products.size
    size_orders: Dict[str, Dict[str, int]] = {}  # scheme -> size -> "order"


async def page_lookups(parents: List[str], child_skus: List[str]) -> PageLookups:
    """Titles, sizing schemes and child sizes (products database), and each scheme's size
    order (listingoptions_sizing_schemes, the order ListingView sorts its sizes by).
    Read-only."""
    out = PageLookups()
    product_db = connections.get("product_db")
    if parents:
        for r in await product_db.execute_query_dict(
            "SELECT sku, title, sizing_scheme FROM parent_products WHERE sku = ANY($1::text[])",
            [parents],
        ):
            if r["title"]:
                out.titles[r["sku"]] = r["title"]
            if r["sizing_scheme"]:
                out.schemes[r["sku"]] = r["sizing_scheme"]
    if child_skus:
        for r in await product_db.execute_query_dict(
            "SELECT lower(sku) AS sku, size FROM child_products WHERE lower(sku) = ANY($1::text[])",
            [[c.lower() for c in child_skus]],
        ):
            if r["size"]:
                out.child_sizes[r["sku"]] = r["size"]
    if parents:
        # The listing's scheme first: it is the one ListingView sorts by.
        for r in await connections.get("default").execute_query_dict(
            "SELECT DISTINCT ON (product_id) product_id, data->>'SIZING_SCHEME' AS scheme "
            "FROM listings WHERE product_id = ANY($1::text[]) "
            "AND COALESCE(data->>'SIZING_SCHEME', '') <> '' "
            "ORDER BY product_id, created_at DESC",
            [parents],
        ):
            out.schemes[r["product_id"]] = r["scheme"]
    schemes = sorted(set(out.schemes.values()))
    if schemes:
        for r in await connections.get("default").execute_query_dict(
            'SELECT sizing_scheme, size, "order" FROM listingoptions_sizing_schemes '
            "WHERE sizing_scheme = ANY($1::text[])",
            [schemes],
        ):
            out.size_orders.setdefault(r["sizing_scheme"], {})[r["size"]] = r["order"]
    return out


# Letter sizes in wear order, for a parent with no sizing scheme to read the order from.
LETTER_SIZES = ["XXXS", "XXS", "XS", "S", "M", "L", "XL", "XXL", "XXXL", "XXXXL", "OS", "O/S"]
_LETTER_RANK = {s: i for i, s in enumerate(LETTER_SIZES)}


def _natural_key(size: str) -> Tuple[int, float]:
    """Numbers ascending, then letter sizes in wear order, then anything else."""
    try:
        return (0, float(size.replace(",", ".")))
    except ValueError:
        pass
    rank = _LETTER_RANK.get(size.strip().upper())
    return (1, float(rank)) if rank is not None else (2, 0.0)


def order_sizes(sizes: List["ShootListSize"], order: Dict[str, int]) -> List["ShootListSize"]:
    """By the sizing scheme's order, as ListingView sorts its sizes. A size the scheme does
    not list goes after the ones it does. With no scheme at all, a natural order instead.
    Ties keep the order they came in."""
    if not order:
        return [s for _, s in sorted(enumerate(sizes), key=lambda i: (_natural_key(i[1].size), i[0]))]
    folded = {k.lower(): v for k, v in order.items()}

    def key(item):
        index, size = item
        rank = order.get(size.size, folded.get(size.size.lower()))
        return (rank is None, rank if rank is not None else 0, index)

    return [s for _, s in sorted(enumerate(sizes), key=key)]


def size_label(sku: str, parent: str, known_parent: bool) -> str:
    """The part of a child SKU after its parent, else the whole SKU."""
    if known_parent and sku.lower().startswith(parent.lower() + "/"):
        return sku[len(parent) + 1:]
    return sku


def _row(r: Dict[str, Any], lookups: PageLookups) -> ShootListRow:
    import json

    sizes = r["sizes"]
    if isinstance(sizes, str):
        sizes = json.loads(sizes)
    first_sku = sizes[0]["sku"] if sizes else None
    titles = lookups.titles
    scheme = lookups.schemes.get(r["parent"])
    return ShootListRow(
        parent=r["parent"],
        known_parent=bool(r["known_parent"]),
        title=titles.get(r["parent"]) or clean_title(r["product_name"], first_sku),
        brand=r["brand"],
        product_type=r["product_type"],
        company=r["company"],
        state=r["state"],
        session_id=r["session_id"],
        session_started_at=r["session_started_at"],
        photo_batch_id=r["photo_batch_id"],
        store_qty=r["store_qty"],
        total_qty=r["total_qty"],
        value=float(r["value"] or 0),
        size_count=r["size_count"],
        first_seen_at=r["first_seen_at"],
        at_store=bool(r["at_store"]),
        sizes=order_sizes([
            ShootListSize(
                sku=s["sku"],
                # The child's own size, as ListingView shows it; the SKU's tail otherwise.
                size=lookups.child_sizes.get(s["sku"].lower())
                or size_label(s["sku"], r["parent"], bool(r["known_parent"])),
                store=s["store"] or 0,
                total=s["total"] or 0,
                list_price=s["list_price"],
                value=float(s["value"] or 0),
                at_store=bool(s["at_store"]),
            )
            for s in sizes or []
        ], lookups.size_orders.get(scheme, {}) if scheme else {}),
    )


@router.get("/products", response_model=List[ShootListRow])
async def get_shoot_list(
    search: Optional[str] = Query(None, max_length=100, description="SKU, title, brand or session id"),
    state: Optional[List[str]] = Query(
        None, description="to_shoot, shot, in_batch or has_images (repeat the key for several)"
    ),
    has_session: Optional[bool] = Query(None, description="Only parents with (true) or without (false) a session"),
    sort: str = Query("value_desc", description=", ".join(SORTS)),
    page: int = Query(1, ge=1, le=1000),
    page_size: int = Query(50, ge=1, le=100),
):
    if sort not in SORTS:
        raise HTTPException(status_code=400, detail="Unknown sort")
    where, params = _where((search or "").strip() or None, _check_states(state), has_session)
    params += [page_size, (page - 1) * page_size]
    sql = (
        f"{GROUPED_CTE} SELECT g.* FROM g{where} ORDER BY {SORTS[sort]} "
        f"LIMIT ${len(params) - 1} OFFSET ${len(params)}"
    )
    rows = await connections.get("default").execute_query_dict(sql, params)
    import json

    child_skus = [
        s["sku"]
        for r in rows
        for s in (json.loads(r["sizes"]) if isinstance(r["sizes"], str) else r["sizes"] or [])
    ]
    lookups = await page_lookups([r["parent"] for r in rows if r["known_parent"]], child_skus)
    return [_row(r, lookups) for r in rows]


@router.get("/summary", response_model=ShootListSummary)
async def get_shoot_list_summary(
    search: Optional[str] = Query(None, max_length=100),
    state: Optional[List[str]] = Query(None),
    has_session: Optional[bool] = Query(None),
):
    """Counts per state ignore the state filter, so the toggle can show them all;
    `count` and `total_value` honour it."""
    states = _check_states(state)
    where, params = _where((search or "").strip() or None, [], has_session)
    conn = connections.get("default")
    rows = await conn.execute_query_dict(
        f"{GROUPED_CTE} SELECT g.state, count(*)::int AS parents, "
        f"sum(g.size_count)::int AS skus, COALESCE(sum(g.value), 0) AS value "
        f"FROM g{where} GROUP BY g.state",
        params,
    )
    latest = (await conn.execute_query_dict(
        "SELECT max(as_of) AS as_of, max(source_job_id) AS job FROM essx_shoot_list"
    ))[0]
    by_state = {s: ShootListStateCount() for s in STATES}
    for r in rows:
        by_state[r["state"]] = ShootListStateCount(
            parents=r["parents"], skus=r["skus"], value=float(r["value"] or 0)
        )
    chosen = [s for s in STATES if not states or s in states]
    return ShootListSummary(
        count=sum(by_state[s].parents for s in chosen),
        total_value=sum(by_state[s].value for s in chosen),
        states=by_state,
        refreshed_at=latest["as_of"],
        source_job_id=latest["job"],
    )
