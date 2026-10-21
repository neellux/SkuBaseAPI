"""The ESSX Store shoot list: stock at the Essx Store warehouse that has never been shot.

Filled once a day from SellerCloud scheduled task 339, an inventory report with one row
per child SKU and a physical quantity column per ESSX warehouse (Essx Annex, Essx NJ
Warehouse, Essx Store) plus their Total. Read on 2026-10-20: 7,583 rows, Total equal to
the three warehouses summed on every row, negatives included. The report has Cost but no
ListPrice, so the price comes from the catalog grid.

A SKU is on the list (state `to_shoot`) when it has Essx Store qty > 0, its parent has no
photography images, and photography has no pending session or open batch for it. Images,
sessions and batches are all keyed on the PARENT in photography, so every size of a
parent shares its state. Value = max(Total, 0) x ListPrice; the list is read by value.

Sessions are tracked here rather than pushed by photography:

    to_shoot    on the list; no session
    shot        a session started within session_days and not in any batch yet. After
                session_days with still no batch the id is cleared and the SKU is back
                to to_shoot.
    in_batch    the parent is in a photography batch that is not completed
    has_images  photography images arrived; only a row that carries a session id gets here

A row with a session id is kept when it drops off the list (shot, sold, moved), so the
page can show what was shot. A row without one is deleted. Nothing outside this table is
ever written.
"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from tortoise import connections
from tortoise.transactions import in_transaction

from services.product_value_service import parse_number

logger = logging.getLogger(__name__)

TO_SHOOT = "to_shoot"
SHOT = "shot"
IN_BATCH = "in_batch"
HAS_IMAGES = "has_images"

STORE_COLUMN = "Essx Store"
ANNEX_COLUMN = "Essx Annex"
NJ_COLUMN = "Essx NJ Warehouse"
TOTAL_COLUMN = "Total"
REQUIRED_COLUMNS = ("ProductID", STORE_COLUMN, TOTAL_COLUMN)


class ReportRejected(Exception):
    """The task output is missing a column or does not look whole enough to trust."""


@dataclass(frozen=True)
class ReportRow:
    sku: str
    product_name: Optional[str]
    brand: Optional[str]
    product_type: Optional[str]
    company: Optional[str]
    product_status: Optional[str]
    annex: int
    nj: int
    store: int
    total: int


@dataclass
class Session:
    id: str
    started: datetime
    batched: bool


@dataclass
class ParentFacts:
    """Everything photography says about one parent, read once per cycle."""

    has_images: bool = False
    open_batch_id: Optional[int] = None
    open_batch_session: Optional[str] = None
    sessions: List[Session] = field(default_factory=list)  # newest first


@dataclass(frozen=True)
class Decision:
    state: str
    session_id: Optional[str]
    session_started_at: Optional[datetime]
    photo_batch_id: Optional[int]


# -- pure ---------------------------------------------------------------------


def _text(value: Any) -> Optional[str]:
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return None


def _qty(value: Any) -> int:
    number = parse_number(value if isinstance(value, str) else None)
    return int(number) if number is not None else 0


def parse_inventory_report(data: bytes) -> List[ReportRow]:
    """The task 339 xlsx, read by header name. Raises ReportRejected on a missing column."""
    import pandas as pd

    df = pd.read_excel(io.BytesIO(data), dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ReportRejected(f"task output is missing {missing}; header was {list(df.columns)}")

    rows: List[ReportRow] = []
    seen: set = set()
    for rec in df.to_dict("records"):
        sku = _text(rec.get("ProductID"))
        if not sku or sku.lower() in seen:
            continue
        seen.add(sku.lower())
        rows.append(ReportRow(
            sku=sku,
            product_name=_text(rec.get("ProductName")),
            brand=_text(rec.get("Brand")),
            product_type=_text(rec.get("ProductType")),
            company=_text(rec.get("Company")),
            product_status=_text(rec.get("ProductStatus")),
            annex=_qty(rec.get(ANNEX_COLUMN)),
            nj=_qty(rec.get(NJ_COLUMN)),
            store=_qty(rec.get(STORE_COLUMN)),
            total=_qty(rec.get(TOTAL_COLUMN)),
        ))
    return rows


def row_value(total: int, list_price: Optional[Decimal]) -> Decimal:
    """max(Total, 0) x ListPrice. A negative total counts as nothing, never as a credit."""
    if list_price is None:
        return Decimal(0)
    return (Decimal(max(total, 0)) * list_price).quantize(Decimal("0.01"))


def decide(
    facts: Optional[ParentFacts],
    *,
    now: datetime,
    session_days: int,
    existing_session: Optional[Tuple[str, Optional[datetime]]] = None,
    first_seen_at: Optional[datetime] = None,
) -> Optional[Decision]:
    """One parent's state, or None when the row should not exist.

    `facts` None means the SKU has no parent in child_products: photography cannot be
    checked, so it stays on the list. `existing_session` is what the row carries today.
    """
    if facts is None:
        return Decision(TO_SHOOT, None, None, None)

    if facts.has_images:
        # Only a row that was shot from this list is worth keeping once images exist.
        if existing_session and existing_session[0]:
            return Decision(HAS_IMAGES, existing_session[0], existing_session[1], None)
        recent = next(
            (s for s in facts.sessions
             if first_seen_at is not None and s.started >= first_seen_at - timedelta(days=1)),
            None,
        )
        if recent:
            return Decision(HAS_IMAGES, recent.id, recent.started, None)
        return None

    if facts.open_batch_id is not None:
        session_id = facts.open_batch_session
        started = next((s.started for s in facts.sessions if s.id == session_id), None)
        if session_id is None and facts.sessions:
            session_id, started = facts.sessions[0].id, facts.sessions[0].started
        return Decision(IN_BATCH, session_id, started, facts.open_batch_id)

    cutoff = now - timedelta(days=session_days)
    pending = next((s for s in facts.sessions if not s.batched and s.started >= cutoff), None)
    if pending:
        return Decision(SHOT, pending.id, pending.started, None)

    return Decision(TO_SHOOT, None, None, None)


def batch_session_for(session_map: Any, parent: str) -> Optional[str]:
    """The newest session id photography recorded for `parent` in a batch, if any."""
    if not isinstance(session_map, Mapping):
        return None
    ids = session_map.get(parent)
    if isinstance(ids, str):
        return ids or None
    if isinstance(ids, Sequence) and ids:
        return str(ids[-1])
    return None


def lower_map(skus: Iterable[str]) -> Dict[str, str]:
    return {s.lower(): s for s in skus}


# -- reads --------------------------------------------------------------------


async def load_parents(skus: Sequence[str]) -> Dict[str, str]:
    """lowercased child sku -> parent sku, for the skus child_products knows."""
    rows = await connections.get("product_db").execute_query_dict(
        "SELECT lower(sku) AS sku, parent_sku FROM child_products "
        "WHERE lower(sku) = ANY($1::text[]) AND parent_sku IS NOT NULL",
        [[s.lower() for s in skus]],
    )
    return {r["sku"]: r["parent_sku"] for r in rows}


async def load_facts(parents: Sequence[str]) -> Dict[str, ParentFacts]:
    """Images from the catalog mirror, sessions and open batches from photography."""
    facts = {p: ParentFacts() for p in parents}
    if not parents:
        return facts
    wanted = list(parents)

    for r in await connections.get("default").execute_query_dict(
        "SELECT parent_sku FROM catalog_product_images WHERE parent_sku = ANY($1::text[])",
        [wanted],
    ):
        facts[r["parent_sku"]].has_images = True

    photo = connections.get("photography_db")
    for r in await photo.execute_query_dict(
        "SELECT s.id::text AS id, s.product_id, s.start_time, "
        "       EXISTS (SELECT 1 FROM image i "
        "                WHERE i.session_id = s.id AND i.batch_id IS NOT NULL) AS batched "
        "  FROM session s "
        " WHERE s.product_id = ANY($1::text[]) AND NOT COALESCE(s.hidden, false) "
        "   AND s.start_time IS NOT NULL "
        " ORDER BY s.start_time DESC",
        [wanted],
    ):
        if r["product_id"] in facts:
            facts[r["product_id"]].sessions.append(
                Session(r["id"], _aware(r["start_time"]), bool(r["batched"]))
            )

    for r in await photo.execute_query_dict(
        "SELECT id, product_ids, session_id_map_all FROM batch "
        " WHERE status IS DISTINCT FROM 'completed' AND product_ids && $1::text[] "
        " ORDER BY id DESC",
        [wanted],
    ):
        session_map = r["session_id_map_all"]
        if isinstance(session_map, str):
            import json

            session_map = json.loads(session_map or "{}")
        for parent in r["product_ids"] or []:
            item = facts.get(parent)
            if item is not None and item.open_batch_id is None:
                item.open_batch_id = r["id"]
                item.open_batch_session = batch_session_for(session_map, parent)
    return facts


async def load_list_prices(skus: Sequence[str]) -> Tuple[Dict[str, Decimal], set]:
    """lowercased sku -> ListPrice from the guarded grid reader, plus untrusted skus."""
    from services.batch_value_service import _fetch_grid_rows

    rows, untrusted, _chunks, _quarantined = await _fetch_grid_rows(list(skus))
    prices: Dict[str, Decimal] = {}
    for sku, row in rows.items():
        price = parse_number(str(row.get("ListPrice")) if row.get("ListPrice") is not None else None)
        if price is not None:
            prices[sku.lower()] = price
    return prices, {s.lower() for s in untrusted}


def _aware(value: Any) -> datetime:
    if isinstance(value, datetime) and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


# -- the refresh --------------------------------------------------------------


UPSERT_SQL = """
INSERT INTO essx_shoot_list (
    sku, parent_sku, product_name, brand, product_type, company, product_status,
    qty_essx_annex, qty_essx_nj, qty_essx_store, qty_total,
    list_price, value, parent_value, state, session_id, session_started_at,
    photo_batch_id, source_job_id, as_of
)
SELECT u.sku, u.parent_sku, u.product_name, u.brand, u.product_type, u.company,
       u.product_status, u.annex, u.nj, u.store, u.total, u.list_price, u.value,
       u.parent_value, u.state, u.session_id, u.session_started_at, u.photo_batch_id,
       $19, $20
  FROM unnest(
       $1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[],
       $8::int[], $9::int[], $10::int[], $11::int[],
       $12::numeric[], $13::numeric[], $14::numeric[], $15::text[], $16::uuid[],
       $17::timestamptz[], $18::int[]
  ) AS u(sku, parent_sku, product_name, brand, product_type, company, product_status,
         annex, nj, store, total, list_price, value, parent_value, state, session_id,
         session_started_at, photo_batch_id)
ON CONFLICT (sku) DO UPDATE SET
    parent_sku = EXCLUDED.parent_sku, product_name = EXCLUDED.product_name,
    brand = EXCLUDED.brand, product_type = EXCLUDED.product_type,
    company = EXCLUDED.company, product_status = EXCLUDED.product_status,
    qty_essx_annex = EXCLUDED.qty_essx_annex, qty_essx_nj = EXCLUDED.qty_essx_nj,
    qty_essx_store = EXCLUDED.qty_essx_store, qty_total = EXCLUDED.qty_total,
    list_price = EXCLUDED.list_price, value = EXCLUDED.value,
    parent_value = EXCLUDED.parent_value, state = EXCLUDED.state,
    session_id = EXCLUDED.session_id, session_started_at = EXCLUDED.session_started_at,
    photo_batch_id = EXCLUDED.photo_batch_id, source_job_id = EXCLUDED.source_job_id,
    as_of = EXCLUDED.as_of
"""

UPDATE_KEPT_SQL = """
UPDATE essx_shoot_list t
   SET state = u.state, session_id = u.session_id,
       session_started_at = u.session_started_at, photo_batch_id = u.photo_batch_id
  FROM unnest($1::text[], $2::text[], $3::uuid[], $4::timestamptz[], $5::int[])
       AS u(sku, state, session_id, session_started_at, photo_batch_id)
 WHERE t.sku = u.sku
"""


async def load_existing() -> Dict[str, Dict[str, Any]]:
    rows = await connections.get("default").execute_query_dict(
        "SELECT sku, parent_sku, session_id::text AS session_id, session_started_at, "
        "first_seen_at, list_price, as_of FROM essx_shoot_list"
    )
    return {r["sku"]: r for r in rows}


async def refresh(
    *,
    task_id: int,
    session_days: int,
    max_age_hours: float,
    min_candidate_ratio: float,
    task_timezone: str = "America/New_York",
    execute: bool = True,
    now: Optional[datetime] = None,
    report: Optional[Tuple[str, datetime, bytes]] = None,
    return_plan: bool = False,
) -> Dict[str, Any]:
    """One refresh. Raises only on a bug; a stale or short report skips with a reason.

    `execute=False` computes everything and writes nothing (the script's dry run).
    `report` lets a caller pass (job id, completed at, bytes) it already fetched.
    `return_plan` (dry run only) puts the computed writes on summary["plan"].
    """
    now = now or datetime.now(timezone.utc)
    summary: Dict[str, Any] = {"execute": execute}

    if report is None:
        from services.sellercloud_service import sellercloud_service

        report = await sellercloud_service.get_latest_task_output(
            task_id, max_age_hours=max_age_hours, task_timezone=task_timezone
        )
    if report is None:
        summary["skipped"] = f"task {task_id} has no completed run in the last {max_age_hours}h"
        logger.warning(f"essx_shoot_list: {summary['skipped']}")
        return summary
    job_id, completed_at, data = report
    summary.update(job_id=job_id, report_age_h=round((now - completed_at).total_seconds() / 3600, 1))

    rows = parse_inventory_report(data)
    candidates = [r for r in rows if r.store > 0]
    summary.update(report_rows=len(rows), candidates=len(candidates))

    try:
        existing = await load_existing()
    except Exception as exc:  # noqa: BLE001
        # A dry run before the migration still answers "what would the list be".
        if execute or "does not exist" not in str(exc):
            raise
        summary["table_missing"] = True
        existing = {}
    last_as_of = max((e["as_of"] for e in existing.values()), default=None)
    last_count = sum(1 for e in existing.values() if e["as_of"] == last_as_of)
    if not candidates or (last_count and len(candidates) < min_candidate_ratio * last_count):
        summary["skipped"] = (
            f"{len(candidates)} SKUs at {STORE_COLUMN}, against {last_count} last time"
        )
        logger.error(f"essx_shoot_list: skipped, {summary['skipped']}")
        return summary

    # By lowercased SKU throughout: SellerCloud's spelling of a SKU is not guaranteed to
    # match the one stored yesterday, and the primary key is case-sensitive.
    existing_lc = {sku.lower(): e for sku, e in existing.items()}
    candidate_lc = {r.sku.lower() for r in candidates}
    kept = [e for sku, e in existing_lc.items() if sku not in candidate_lc]
    parent_of = await load_parents([r.sku for r in candidates])
    parents = sorted(
        set(parent_of.values()) | {e["parent_sku"] for e in kept if e["parent_sku"]}
    )
    facts = await load_facts(parents)
    prices, untrusted = await load_list_prices([r.sku for r in candidates])
    summary["priced"] = len(prices)

    def existing_session(e: Optional[Mapping[str, Any]]):
        if e and e.get("session_id"):
            return e["session_id"], e.get("session_started_at")
        return None

    upserts: List[Dict[str, Any]] = []
    deletes: List[str] = []
    for r in candidates:
        e = existing_lc.get(r.sku.lower())
        parent = parent_of.get(r.sku.lower())
        d = decide(
            facts.get(parent) if parent else None, now=now, session_days=session_days,
            existing_session=existing_session(e),
            first_seen_at=e["first_seen_at"] if e else None,
        )
        if d is None:
            if e:
                deletes.append(e["sku"])
            continue
        price = prices.get(r.sku.lower())
        if price is None and r.sku.lower() in untrusted and e:
            price = e.get("list_price")
        upserts.append({"row": r, "sku": e["sku"] if e else r.sku, "parent": parent,
                        "decision": d, "price": price, "value": row_value(r.total, price)})

    parent_values: Dict[str, Decimal] = {}
    for u in upserts:
        key = u["parent"] or u["row"].sku
        parent_values[key] = parent_values.get(key, Decimal(0)) + u["value"]

    updates: List[Tuple[str, Decision]] = []
    for e in kept:
        d = decide(
            facts.get(e["parent_sku"]) if e["parent_sku"] else None, now=now,
            session_days=session_days, existing_session=existing_session(e),
            first_seen_at=e["first_seen_at"],
        )
        if d is None or d.session_id is None:
            deletes.append(e["sku"])
        else:
            updates.append((e["sku"], d))

    states: Dict[str, int] = {}
    for u in upserts:
        states[u["decision"].state] = states.get(u["decision"].state, 0) + 1
    summary.update(
        states=states,
        to_shoot_parents=len({u["parent"] or u["row"].sku for u in upserts
                              if u["decision"].state == TO_SHOOT}),
        kept_off_list=len(updates),
        deleted=len(deletes),
        inserted=sum(1 for u in upserts if u["row"].sku.lower() not in existing_lc),
    )
    summary["top"] = [
        (u["row"].sku, str(u["value"]), u["decision"].state)
        for u in sorted(upserts, key=lambda u: u["value"], reverse=True)[:20]
    ]

    plan = {"upserts": upserts, "parent_values": parent_values, "updates": updates,
            "deletes": deletes, "job_id": job_id, "now": now}
    if not execute:
        if return_plan:
            summary["plan"] = plan
        logger.info(f"essx_shoot_list: dry run {summary}")
        return summary

    async with in_transaction("default") as conn:
        await apply_plan(conn, plan)

    logger.info(f"essx_shoot_list: refreshed {summary}")
    return summary


async def apply_plan(conn: Any, plan: Mapping[str, Any]) -> None:
    """Write one refresh. `conn` is anything with execute_query(sql, params), so the TEST
    loader can run a plan computed from prod data against the TEST database."""
    upserts, parent_values = plan["upserts"], plan["parent_values"]
    updates, deletes = plan["updates"], plan["deletes"]
    if upserts:
        await conn.execute_query(UPSERT_SQL, [
            [u["sku"] for u in upserts],
            [u["parent"] for u in upserts],
            [u["row"].product_name for u in upserts],
            [u["row"].brand for u in upserts],
            [u["row"].product_type for u in upserts],
            [u["row"].company for u in upserts],
            [u["row"].product_status for u in upserts],
            [u["row"].annex for u in upserts],
            [u["row"].nj for u in upserts],
            [u["row"].store for u in upserts],
            [u["row"].total for u in upserts],
            [u["price"] for u in upserts],
            [u["value"] for u in upserts],
            [parent_values[u["parent"] or u["row"].sku] for u in upserts],
            [u["decision"].state for u in upserts],
            [u["decision"].session_id for u in upserts],
            [u["decision"].session_started_at for u in upserts],
            [u["decision"].photo_batch_id for u in upserts],
            int(plan["job_id"]),
            plan["now"],
        ])
    if updates:
        await conn.execute_query(UPDATE_KEPT_SQL, [
            [sku for sku, _ in updates],
            [d.state for _, d in updates],
            [d.session_id for _, d in updates],
            [d.session_started_at for _, d in updates],
            [d.photo_batch_id for _, d in updates],
        ])
    if deletes:
        await conn.execute_query(
            "DELETE FROM essx_shoot_list WHERE sku = ANY($1::text[])", [deletes]
        )
