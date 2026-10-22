"""The ESSX Store photo queue: stock at the Essx Store warehouse that has never been shot.

The table, essx_shoot_list, lives in the PHOTOGRAPHY database (lux_photography), reached
here through the photography_db connection. Two apps write it, each only its own columns:

    stock   this module, once a day       qtys, ListPrice, value, and the page's title,
                                          MPN, size and size order
    photo   PhotoManagementNew            state, session ids, photo batch, last session
            services/photo_queue.py, on
            every scan, batch, hide or
            disable and once a minute

so SkuBase reads no photography table but this one. A row inserted here takes the photo
defaults (to_shoot, no sessions) and photography fills them in within a minute. The photo
rules live with photography's decide() and in SkuBase/docs/essx_shoot_list.md.

The stock comes from SellerCloud scheduled task 339, an inventory report with one row per
child SKU and a physical quantity column per ESSX warehouse (Essx Annex, Essx NJ
Warehouse, Essx Store) plus their Total. Read on 2026-10-20: 7,583 rows, Total equal to
the three warehouses summed on every row, negatives included. The report has Cost but no
ListPrice, so the price comes from the catalog grid. Value = max(Total, 0) x ListPrice.

A SKU at the Essx Store is on the list while its parent has no images (no
catalog_product_images row, SkuBase's mirror of photography's productimages) or while its
row carries session ids, which means photography is tracking a shoot of it. A row that is
no longer listed is kept only while it is past to_shoot and carries session ids, so the
page can show what was shot; any other row is deleted. Nothing outside this table is ever
written.
"""

from __future__ import annotations

import io
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from tortoise import connections
from tortoise.transactions import in_transaction

from services.product_value_service import parse_number

logger = logging.getLogger(__name__)

# The photo states photography writes (PhotoManagementNew services/photo_queue.py).
TO_SHOOT = "to_shoot"
SHOT = "shot"
IN_BATCH = "in_batch"
HAS_IMAGES = "has_images"

# The connection the table is on: the photography database.
QUEUE_DB = "photography_db"

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


def size_label(sku: str, parent: Optional[str]) -> str:
    """The part of a child SKU after its parent, else the whole SKU."""
    if parent and sku.lower().startswith(parent.lower() + "/"):
        return sku[len(parent) + 1:]
    return sku


# Letter sizes in wear order, for a parent with no sizing scheme to read the order from.
LETTER_SIZES = ["XXXS", "XXS", "XS", "S", "M", "L", "XL", "XXL", "XXXL", "XXXXL", "OS", "O/S"]
_LETTER_RANK = {s: i for i, s in enumerate(LETTER_SIZES)}

# size_rank bands. A rank is compared only within one parent, which has one scheme, so a
# rank stays right whichever day it was written.
_LETTER_BAND = 1_000_000
_OTHER_BAND = 2_000_000
UNKNOWN_SIZE_RANK = 9_000_000


def size_rank(size: str, order: Mapping[str, int]) -> int:
    """Sort key for a size within its parent, the order ListingView shows sizes in.

    With a sizing scheme: the scheme's "order", and a size the scheme does not list after
    every size it does. Without one: numbers ascending, then letter sizes in wear order,
    then anything else. Ties fall back to the SKU.
    """
    if order:
        rank = order.get(size)
        if rank is None:
            rank = {k.lower(): v for k, v in order.items()}.get(size.lower())
        return int(rank) if rank is not None else UNKNOWN_SIZE_RANK
    try:
        number = float(size.replace(",", "."))
        return int(round(min(max(number, 0.0), 9_999.99) * 100))
    except ValueError:
        pass
    letter = _LETTER_RANK.get(size.strip().upper())
    return _LETTER_BAND + letter if letter is not None else _OTHER_BAND


# -- reads --------------------------------------------------------------------


async def load_parents(skus: Sequence[str]) -> Dict[str, str]:
    """lowercased child sku -> parent sku, for the skus child_products knows."""
    rows = await connections.get("product_db").execute_query_dict(
        "SELECT lower(sku) AS sku, parent_sku FROM child_products "
        "WHERE lower(sku) = ANY($1::text[]) AND parent_sku IS NOT NULL",
        [[s.lower() for s in skus]],
    )
    return {r["sku"]: r["parent_sku"] for r in rows}


async def load_imaged_parents(parents: Sequence[str]) -> set:
    """The parents that already have photography images, from SkuBase's own mirror of
    productimages (catalog_sync_poller, every 15 minutes): the catalog's "No images" test."""
    if not parents:
        return set()
    rows = await connections.get("default").execute_query_dict(
        "SELECT parent_sku FROM catalog_product_images WHERE parent_sku = ANY($1::text[])",
        [list(parents)],
    )
    return {r["parent_sku"] for r in rows}


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


@dataclass
class PageLookups:
    """What the products and listing-options tables add to the page's rows."""

    titles: Dict[str, str] = field(default_factory=dict)
    mpns: Dict[str, str] = field(default_factory=dict)  # parent -> parent_products.mpn
    schemes: Dict[str, str] = field(default_factory=dict)  # parent -> sizing scheme
    child_sizes: Dict[str, str] = field(default_factory=dict)  # lowercased child sku -> size
    size_orders: Dict[str, Dict[str, int]] = field(default_factory=dict)  # scheme -> size -> order


async def page_lookups(parents: Sequence[str], child_skus: Sequence[str]) -> PageLookups:
    """Titles, MPNs, sizing schemes and child sizes (products database), and each scheme's size
    order (listingoptions_sizing_schemes, the order ListingView sorts its sizes by).
    Read-only, and read at refresh so the page reads nothing but the queue table."""
    out = PageLookups()
    product_db = connections.get("product_db")
    parents = list(parents)
    if parents:
        for r in await product_db.execute_query_dict(
            "SELECT sku, title, mpn, sizing_scheme FROM parent_products WHERE sku = ANY($1::text[])",
            [parents],
        ):
            if r["title"]:
                out.titles[r["sku"]] = r["title"]
            if r["mpn"] and r["mpn"].strip():
                out.mpns[r["sku"]] = r["mpn"].strip()
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
            if r["order"] is not None:
                out.size_orders.setdefault(r["sizing_scheme"], {})[r["size"]] = int(r["order"])
    return out


async def load_existing() -> Dict[str, Dict[str, Any]]:
    rows = await connections.get(QUEUE_DB).execute_query_dict(
        "SELECT sku, parent_sku, state, cardinality(session_ids)::int AS sessions, "
        "list_price, as_of FROM essx_shoot_list"
    )
    return {r["sku"]: r for r in rows}


# -- the refresh --------------------------------------------------------------


# Stock and page columns only. A new row takes the photo columns' defaults, and an existing
# row's photo columns are photography's: never touched here.
UPSERT_SQL = """
INSERT INTO essx_shoot_list (
    sku, parent_sku, product_name, brand, product_type, company, product_status,
    qty_essx_annex, qty_essx_nj, qty_essx_store, qty_total,
    list_price, value, parent_value, title, mpn, size, size_rank, source_job_id, as_of
)
SELECT u.sku, u.parent_sku, u.product_name, u.brand, u.product_type, u.company,
       u.product_status, u.annex, u.nj, u.store, u.total, u.list_price, u.value,
       u.parent_value, u.title, u.mpn, u.size, u.size_rank, $19, $20
  FROM unnest(
       $1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[],
       $8::int[], $9::int[], $10::int[], $11::int[],
       $12::numeric[], $13::numeric[], $14::numeric[], $15::text[], $16::text[],
       $17::text[], $18::int[]
  ) AS u(sku, parent_sku, product_name, brand, product_type, company, product_status,
         annex, nj, store, total, list_price, value, parent_value, title, mpn, size,
         size_rank)
ON CONFLICT (sku) DO UPDATE SET
    parent_sku = EXCLUDED.parent_sku, product_name = EXCLUDED.product_name,
    brand = EXCLUDED.brand, product_type = EXCLUDED.product_type,
    company = EXCLUDED.company, product_status = EXCLUDED.product_status,
    qty_essx_annex = EXCLUDED.qty_essx_annex, qty_essx_nj = EXCLUDED.qty_essx_nj,
    qty_essx_store = EXCLUDED.qty_essx_store, qty_total = EXCLUDED.qty_total,
    list_price = EXCLUDED.list_price, value = EXCLUDED.value,
    parent_value = EXCLUDED.parent_value, title = EXCLUDED.title, mpn = EXCLUDED.mpn,
    size = EXCLUDED.size,
    size_rank = EXCLUDED.size_rank, source_job_id = EXCLUDED.source_job_id,
    as_of = EXCLUDED.as_of
"""

# A row no longer listed goes unless photography is tracking a shoot of it. Judged on the
# photo columns as they are at delete time, not as they were read: photography may have
# added a session since.
DELETE_SQL = """
DELETE FROM essx_shoot_list
 WHERE sku = ANY($1::text[])
   AND (state = 'to_shoot' OR cardinality(session_ids) = 0)
RETURNING sku
"""


async def refresh(
    *,
    task_id: int,
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
    at_store = [r for r in rows if r.store > 0]
    summary.update(report_rows=len(rows), at_store=len(at_store))

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

    # By lowercased SKU throughout: SellerCloud's spelling of a SKU is not guaranteed to
    # match the one stored yesterday, and the primary key is case-sensitive.
    existing_lc = {sku.lower(): e for sku, e in existing.items()}
    parent_of = await load_parents([r.sku for r in at_store])
    imaged = await load_imaged_parents(sorted(set(parent_of.values())))

    listed: List[Tuple[ReportRow, Optional[Dict[str, Any]], Optional[str]]] = []
    for r in at_store:
        e = existing_lc.get(r.sku.lower())
        parent = parent_of.get(r.sku.lower())
        # Already photographed, and photography is not tracking a shoot of it from here.
        if parent and parent in imaged and not (e and e["sessions"]):
            continue
        listed.append((r, e, parent))
    summary["listed"] = len(listed)

    if not listed or (last_count and len(listed) < min_candidate_ratio * last_count):
        summary["skipped"] = f"{len(listed)} SKUs to list, against {last_count} last time"
        logger.error(f"essx_shoot_list: skipped, {summary['skipped']}")
        return summary

    listed_lc = {r.sku.lower() for r, _, _ in listed}
    gone = [e["sku"] for sku, e in existing_lc.items() if sku not in listed_lc]

    prices, untrusted = await load_list_prices([r.sku for r, _, _ in listed])
    summary["priced"] = len(prices)
    lookups = await page_lookups(
        sorted({p for _, _, p in listed if p}), [r.sku for r, _, _ in listed]
    )

    upserts: List[Dict[str, Any]] = []
    for r, e, parent in listed:
        price = prices.get(r.sku.lower())
        if price is None and r.sku.lower() in untrusted and e:
            price = e.get("list_price")
        scheme = lookups.schemes.get(parent) if parent else None
        size = lookups.child_sizes.get(r.sku.lower()) or size_label(r.sku, parent)
        upserts.append({
            "row": r,
            "sku": e["sku"] if e else r.sku,
            "parent": parent,
            "price": price,
            "value": row_value(r.total, price),
            "title": (lookups.titles.get(parent) if parent else None)
            or clean_title(r.product_name, r.sku),
            "mpn": lookups.mpns.get(parent) if parent else None,
            "size": size,
            "size_rank": size_rank(size, lookups.size_orders.get(scheme, {}) if scheme else {}),
            "state": e["state"] if e else TO_SHOOT,
        })

    parent_values: Dict[str, Decimal] = {}
    for u in upserts:
        key = u["parent"] or u["row"].sku
        parent_values[key] = parent_values.get(key, Decimal(0)) + u["value"]

    states: Dict[str, int] = {}
    for u in upserts:
        states[u["state"]] = states.get(u["state"], 0) + 1
    summary.update(
        states=states,
        listed_parents=len({u["parent"] or u["row"].sku for u in upserts}),
        inserted=sum(1 for u in upserts if u["row"].sku.lower() not in existing_lc),
        not_listed=len(gone),
    )
    summary["top"] = [
        (u["row"].sku, str(u["value"]), u["state"])
        for u in sorted(upserts, key=lambda u: u["value"], reverse=True)[:20]
    ]

    plan = {"upserts": upserts, "parent_values": parent_values, "gone": gone,
            "job_id": job_id, "now": now}
    if not execute:
        if return_plan:
            summary["plan"] = plan
        logger.info(f"essx_shoot_list: dry run {summary}")
        return summary

    async with in_transaction(QUEUE_DB) as conn:
        deleted = await apply_plan(conn, plan)
    summary["deleted"] = len(deleted)
    summary["kept_off_list"] = len(gone) - len(deleted)

    logger.info(f"essx_shoot_list: refreshed {summary}")
    return summary


async def apply_plan(conn: Any, plan: Mapping[str, Any]) -> List[str]:
    """Write one refresh; returns the deleted SKUs. `conn` is anything with
    execute_query(sql, params) and execute_query_dict(sql, params), so the TEST loader can
    run a plan computed from prod data against the TEST database."""
    upserts, parent_values = plan["upserts"], plan["parent_values"]
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
            [u["title"] for u in upserts],
            [u["mpn"] for u in upserts],
            [u["size"] for u in upserts],
            [u["size_rank"] for u in upserts],
            int(plan["job_id"]),
            plan["now"],
        ])
    if not plan["gone"]:
        return []
    rows = await conn.execute_query_dict(DELETE_SQL, [list(plan["gone"])])
    return [r["sku"] for r in rows]
