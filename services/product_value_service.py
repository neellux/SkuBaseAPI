"""Latest merchandise value per parent product: parent_product_values.

Written once a day from the ExportCustomInfo job daily_sellercloud_sync_poller already runs
over every active child SKU, so valuing the whole catalog costs no extra SellerCloud export.
Phase B0 of the plan verified the columns on 2026-09-15: FieldNames AggregatePhysicalQty and
SitePrice matched the Kind 13 grid on 50 of 50 SKUs (PhysicalQty exported blank, which is
what an unknown name looks like). SellerCloud also inserts ProductName after ProductID
without being asked, so columns are only ever read by header name.

Read by the batch value snapshot and the nightly batch refresh when
[batch_value] source = "table", and by the catalog browser.

value(parent) = sum over active children of (AggregatePhysicalQty x SitePrice), through the
same aggregate_values batch_value_service has always used.

Imports no batch or listing service.
"""

import csv
import io
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional, Tuple

from tortoise import connections
from tortoise.transactions import in_transaction

logger = logging.getLogger(__name__)

QTY_FIELD = "AggregatePhysicalQty"
PRICE_FIELD = "SitePrice"
EXPORT_FIELD_NAMES = ["ProductID", "UPC", QTY_FIELD, PRICE_FIELD]

# Guards. A failed SellerCloud export can look like an empty one, and an unknown field name
# exports a blank column, so a write needs positive evidence the export is whole.
MIN_RETURNED_RATIO = 0.9
MAX_BLANK_RATIO = 0.1
# Never let one run delete more than this share of stored values.
MAX_DELETE_RATIO = 0.05
# A value older than this is not trusted by readers: the parent keeps its previous batch entry.
MAX_VALUE_AGE_HOURS = 36
UPSERT_CHUNK = 5000


class MissingExportColumn(Exception):
    """The export header lacks a required column. Never fall back to a column position:
    with value columns appended, the old last-column fallback would read prices as UPCs."""


class ExportRejected(Exception):
    """The export parsed but does not look whole enough to trust."""


@dataclass(frozen=True)
class CustomExport:
    upcs: Dict[str, str]
    # sku -> {QTY_FIELD: int, PRICE_FIELD: Decimal}; only rows where both parsed.
    rows: Dict[str, Dict[str, Any]]
    returned: int
    blank_qty: int
    blank_price: int


def parse_number(text: Optional[str]) -> Optional[Decimal]:
    """Export cells are text. "3.0" and "1,234" are numbers; blank or junk is None."""
    cleaned = (text or "").strip().replace(",", "")
    if not cleaned:
        return None
    try:
        number = Decimal(cleaned)
    except InvalidOperation:
        return None
    return number if number.is_finite() else None


def parse_custom_export_tsv(data: bytes) -> CustomExport:
    """Parse the ExportCustomInfo TSV by header name. Pure."""
    text = data.decode("utf-8-sig", "replace")
    reader = csv.reader(io.StringIO(text), delimiter="\t", quoting=csv.QUOTE_NONE)
    header = [name.strip() for name in next(reader, [])]
    missing = [name for name in EXPORT_FIELD_NAMES if name not in header]
    if missing:
        raise MissingExportColumn(f"custom export is missing {missing}; header was {header}")

    sku_i = header.index("ProductID")
    upc_i = header.index("UPC")
    qty_i = header.index(QTY_FIELD)
    price_i = header.index(PRICE_FIELD)
    width = max(sku_i, upc_i, qty_i, price_i)

    upcs: Dict[str, str] = {}
    rows: Dict[str, Dict[str, Any]] = {}
    returned = blank_qty = blank_price = 0
    for parts in reader:
        if len(parts) <= width:
            continue
        sku = parts[sku_i].strip()
        if not sku:
            continue
        returned += 1
        upc = parts[upc_i].strip()
        if upc:
            upcs[sku] = upc
        qty = parse_number(parts[qty_i])
        price = parse_number(parts[price_i])
        if qty is None:
            blank_qty += 1
        if price is None:
            blank_price += 1
        if qty is not None and price is not None:
            rows[sku] = {QTY_FIELD: int(qty), PRICE_FIELD: price}

    return CustomExport(
        upcs=upcs, rows=rows, returned=returned, blank_qty=blank_qty, blank_price=blank_price
    )


def check_export(export: CustomExport, requested: int) -> Optional[str]:
    """Why this export cannot be trusted, or None. Reads parse counts, never aggregates."""
    if export.returned == 0:
        return "the export returned no rows"
    if requested and export.returned < MIN_RETURNED_RATIO * requested:
        return f"the export returned {export.returned} of {requested} requested SKUs"
    if export.blank_qty > MAX_BLANK_RATIO * export.returned:
        return f"{export.blank_qty} of {export.returned} rows have no {QTY_FIELD}"
    if export.blank_price > MAX_BLANK_RATIO * export.returned:
        return f"{export.blank_price} of {export.returned} rows have no {PRICE_FIELD}"
    return None


def build_parent_values(
    children_by_parent: Dict[str, List[str]],
    export_rows: Dict[str, Dict[str, Any]],
) -> List[Tuple[str, Decimal, int, int, int, int]]:
    """(parent, value, qty, children, priced, exported) for every parent. Pure.

    `exported` counts active children the export had a trustworthy row for; readers treat
    exported < children as a partial read and keep the previous value.
    """
    # Deferred: batch_value_service is heavier, and this keeps the module importable alone.
    from services.batch_value_service import aggregate_values

    _total, breakdown = aggregate_values(children_by_parent, export_rows)
    out = []
    for parent, kids in children_by_parent.items():
        entry = breakdown.get(parent) or {}
        out.append(
            (
                parent,
                Decimal(str(entry.get("value") or 0)),
                int(entry.get("qty") or 0),
                len(kids),
                int(entry.get("priced") or 0),
                sum(1 for kid in kids if kid in export_rows),
            )
        )
    return out


UPSERT_SQL = """
INSERT INTO parent_product_values (parent_sku, value, qty, children, priced, exported, as_of)
SELECT u.sku, u.value, u.qty, u.children, u.priced, u.exported, $7
  FROM unnest($1::text[], $2::numeric[], $3::int[], $4::int[], $5::int[], $6::int[])
       AS u(sku, value, qty, children, priced, exported)
ON CONFLICT (parent_sku) DO UPDATE
   SET value = EXCLUDED.value, qty = EXCLUDED.qty, children = EXCLUDED.children,
       priced = EXCLUDED.priced, exported = EXCLUDED.exported, as_of = EXCLUDED.as_of
"""


async def write_parent_values(export: CustomExport, requested: int, as_of: datetime) -> Dict[str, Any]:
    """Value every active parent from one export and store it. Raises ExportRejected (and
    writes nothing) when the export fails its guards."""
    reason = check_export(export, requested)
    if reason:
        logger.error(f"value export rejected: {reason}")
        raise ExportRejected(reason)

    started = time.monotonic()
    product_db = connections.get("product_db")
    parents = await product_db.execute_query_dict(
        "SELECT sku FROM parent_products WHERE is_active"
    )
    children = await product_db.execute_query_dict(
        "SELECT parent_sku, sku FROM child_products WHERE is_active AND parent_sku IS NOT NULL"
    )
    children_by_parent: Dict[str, List[str]] = {row["sku"]: [] for row in parents}
    for row in children:
        if row["parent_sku"] in children_by_parent:
            children_by_parent[row["parent_sku"]].append(row["sku"])

    values = build_parent_values(children_by_parent, export.rows)

    async with in_transaction("default") as conn:
        for i in range(0, len(values), UPSERT_CHUNK):
            chunk = values[i : i + UPSERT_CHUNK]
            await conn.execute_query(
                UPSERT_SQL,
                [
                    [v[0] for v in chunk],
                    [v[1] for v in chunk],
                    [v[2] for v in chunk],
                    [v[3] for v in chunk],
                    [v[4] for v in chunk],
                    [v[5] for v in chunk],
                    as_of,
                ],
            )
        counts = (
            await conn.execute_query_dict(
                "SELECT count(*)::int AS total, count(*) FILTER (WHERE as_of < $1)::int AS stale "
                "FROM parent_product_values",
                [as_of],
            )
        )[0]
        deleted = 0
        if counts["stale"]:
            if counts["stale"] > MAX_DELETE_RATIO * counts["total"]:
                logger.error(
                    f"value write kept {counts['stale']} stale rows of {counts['total']}: "
                    f"deleting them would remove more than {MAX_DELETE_RATIO:.0%}"
                )
            else:
                await conn.execute_query("DELETE FROM parent_product_values WHERE as_of < $1", [as_of])
                deleted = counts["stale"]

    total_value = sum((v[1] for v in values), Decimal(0))
    summary = {
        "parents": len(values),
        "total_value": float(total_value),
        "zero_valued": sum(1 for v in values if v[1] == 0),
        "unpriced": sum(1 for v in values if v[3] and not v[4]),
        "partial": sum(1 for v in values if v[5] < v[3]),
        "deleted": deleted,
        "export_rows": export.returned,
        "elapsed_s": round(time.monotonic() - started, 1),
    }
    logger.info(f"Wrote parent values: {summary}")
    return summary


async def read_parent_values(
    parents: Iterable[str], max_age_hours: float = MAX_VALUE_AGE_HOURS
) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
    """(entries in the batches.product_values shape, dropped parents).

    A parent is dropped, and keeps whatever its batch had, when it has no row, when the row
    is older than max_age_hours, or when the export covered only some of its children.
    """
    wanted = sorted({p for p in parents if p})
    if not wanted:
        return {}, []
    rows = await connections.get("default").execute_query_dict(
        "SELECT parent_sku, value, qty, children, priced, exported, as_of "
        "FROM parent_product_values WHERE parent_sku = ANY($1::text[])",
        [wanted],
    )
    by_parent = {row["parent_sku"]: row for row in rows}
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)

    values: Dict[str, Dict[str, Any]] = {}
    dropped: List[str] = []
    for parent in wanted:
        row = by_parent.get(parent)
        if row is None or row["as_of"] < cutoff or row["exported"] < row["children"]:
            dropped.append(parent)
            continue
        values[parent] = {
            "value": float(row["value"]),
            "qty": row["qty"],
            "children": row["children"],
            "priced": row["priced"],
        }
    return values, dropped


async def latest_as_of() -> Optional[datetime]:
    rows = await connections.get("default").execute_query_dict(
        "SELECT max(as_of) AS as_of FROM parent_product_values"
    )
    return rows[0]["as_of"] if rows else None
