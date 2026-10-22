"""The ESSX Store shoot list page.

GET /essx_shoot_list/products   one page of parents from essx_shoot_list, by value
GET /essx_shoot_list/summary    counts per state for the header and the state toggle

Query parameters only. One row per PARENT: photography shoots parents, and every size of
a parent shares one state (services/essx_shoot_list_service.py). Read-only, and reads ONE
table: essx_shoot_list in the photography database, where photography keeps the shoot
details and the daily refresh stores the titles, MPNs, sizes and size order. Registered with the
auth service under view_batches, like the catalog GETs.
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from tortoise import connections

from services.essx_shoot_list_service import HAS_IMAGES, IN_BATCH, QUEUE_DB, SHOT, TO_SHOOT

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

# Parents, not rows. A size kept after it left the store (photography is tracking its
# shoot) keeps its old as_of; `at_store` says whether any size of the parent was in the
# latest refresh. Photography writes the same photo columns on every row of a parent, so
# any row's will do; the freshest is taken.
GROUPED_CTE = """
WITH latest AS (SELECT max(as_of) AS as_of FROM essx_shoot_list),
g AS (
    SELECT COALESCE(t.parent_sku, t.sku) AS parent,
           bool_or(t.parent_sku IS NOT NULL) AS known_parent,
           (array_agg(t.title ORDER BY t.as_of DESC, t.sku))[1] AS title,
           (array_agg(t.mpn ORDER BY t.as_of DESC, t.sku))[1] AS mpn,
           (array_agg(t.product_name ORDER BY t.sku))[1] AS product_name,
           (array_agg(t.brand ORDER BY t.sku))[1] AS brand,
           (array_agg(t.product_type ORDER BY t.sku))[1] AS product_type,
           (array_agg(t.company ORDER BY t.sku))[1] AS company,
           (array_agg(t.state ORDER BY t.as_of DESC, t.sku))[1] AS state,
           (array_agg(t.session_id::text ORDER BY t.as_of DESC, t.sku))[1] AS session_id,
           -- Text, not uuid[]: array_agg of arrays of different lengths is an error.
           (array_agg(array_to_string(t.session_ids, ' ') ORDER BY t.as_of DESC, t.sku))[1]
               AS session_ids,
           max(t.session_started_at) AS session_started_at,
           max(t.last_session_started_at) AS last_session_started_at,
           max(t.photo_batch_id) AS photo_batch_id,
           (array_agg(t.photo_batch_status ORDER BY t.as_of DESC, t.sku))[1] AS photo_batch_status,
           COALESCE(sum(t.qty_essx_store) FILTER (WHERE t.as_of = l.as_of), 0)::int AS store_qty,
           COALESCE(sum(GREATEST(t.qty_total, 0)) FILTER (WHERE t.as_of = l.as_of), 0)::int
               AS total_qty,
           COALESCE(sum(t.value), 0) AS value,
           count(*)::int AS size_count,
           min(t.first_seen_at) AS first_seen_at,
           bool_or(t.as_of = l.as_of) AS at_store,
           json_agg(json_build_object(
               'sku', t.sku, 'size', t.size, 'store', t.qty_essx_store, 'total', t.qty_total,
               'list_price', t.list_price, 'value', t.value, 'at_store', t.as_of = l.as_of
           ) ORDER BY t.size_rank NULLS LAST, t.sku) AS sizes,
           lower(string_agg(concat_ws(' ', t.sku, t.parent_sku, t.mpn, t.title, t.product_name,
                                      t.brand, t.product_type,
                                      array_to_string(t.session_ids, ' ')), ' ')) AS haystack
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
    mpn: Optional[str] = None
    brand: Optional[str] = None
    product_type: Optional[str] = None
    company: Optional[str] = None
    state: str
    session_id: Optional[str] = None
    session_ids: List[str] = []
    session_started_at: Optional[datetime] = None
    last_session_started_at: Optional[datetime] = None
    photo_batch_id: Optional[int] = None
    photo_batch_status: Optional[str] = None
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


def _row(r: Dict[str, Any]) -> ShootListRow:
    sizes = r["sizes"]
    if isinstance(sizes, str):
        sizes = json.loads(sizes)
    return ShootListRow(
        parent=r["parent"],
        known_parent=bool(r["known_parent"]),
        title=r["title"] or r["product_name"],
        mpn=r["mpn"],
        brand=r["brand"],
        product_type=r["product_type"],
        company=r["company"],
        state=r["state"],
        session_id=r["session_id"],
        session_ids=(r["session_ids"] or "").split(),
        session_started_at=r["session_started_at"],
        last_session_started_at=r["last_session_started_at"],
        photo_batch_id=r["photo_batch_id"],
        photo_batch_status=r["photo_batch_status"],
        store_qty=r["store_qty"],
        total_qty=r["total_qty"],
        value=float(r["value"] or 0),
        size_count=r["size_count"],
        first_seen_at=r["first_seen_at"],
        at_store=bool(r["at_store"]),
        # Already in sizing-scheme order: the refresh stores size and size_rank.
        sizes=[
            ShootListSize(
                sku=s["sku"],
                size=s["size"] or s["sku"],
                store=s["store"] or 0,
                total=s["total"] or 0,
                list_price=s["list_price"],
                value=float(s["value"] or 0),
                at_store=bool(s["at_store"]),
            )
            for s in sizes or []
        ],
    )


@router.get("/products", response_model=List[ShootListRow])
async def get_shoot_list(
    search: Optional[str] = Query(None, max_length=100, description="SKU, MPN, title, brand or session id"),
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
    rows = await connections.get(QUEUE_DB).execute_query_dict(sql, params)
    return [_row(r) for r in rows]


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
    conn = connections.get(QUEUE_DB)
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
