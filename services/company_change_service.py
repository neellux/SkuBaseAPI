"""ESSX products moving to nymilan, caught off the daily SellerCloud export.

oneinventory_service creates every EssxNYC (company 249) product on 1nventory as a DRAFT
tagged "ESSX SKU", and leaves one that already exists alone. Operators move products in
SellerCloud from 249 to nymilan (182), usually one size at a time. Nothing in SkuBase
hears about that, so the 1nventory product would stay a draft forever.

One cycle, riding on the ExportCustomInfo job daily_sellercloud_sync_poller already runs
over every active child (which now carries CompanyID and PURCHASING_COMPANY):

  1. Classify every company-249 parent by its children's CompanyID. ONE active child at
     182 means the whole parent has moved.
  2. Shopify: every product tagged "ESSX SKU" whose parent moved goes ACTIVE, is
     published to the Online Store, and loses the tag.
  3. SellerCloud: one bulk import moves the parent's remaining 249 children to 182 and
     sets PURCHASING_COMPANY = "ESSX" on exactly those children. Children an operator
     already moved are never written.

NO SKUBASE DATABASE WRITES, by decision (2026-10-20). parent_products.company_code stays
249 as the original company, and the state lives where the work is: a 249 child under a
moved parent is the "still to import" marker, and the "ESSX SKU" tag is the "still to
flip" marker. Each half is retried by the next cycle until its marker is gone, so the two
are independent and neither waits on the other.

Parents that already had a size at 182 when this shipped are a baseline and are only
reported, never acted on (BASELINE_MIXED_PARENTS).
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set

from tortoise import connections

from services.oneinventory_service import ESSX_COMPANY_CODE, ESSX_TAG, STORE
from services.product_value_service import MAX_BLANK_RATIO, CustomExport, check_export

logger = logging.getLogger(__name__)

FROM_COMPANY = ESSX_COMPANY_CODE  # 249, EssxNYC
TO_COMPANY = 182  # nymilan
# PURCHASING_COMPANY holds company NAMES (ESSX, LUX, OAKWOOD), not ids: 36,192 of the
# 38,732 children of 249 parents already read "ESSX" on 2026-10-20.
PURCHASING_VALUE = "ESSX"
SOURCE = "company_change_sync"

# Every company-249 parent that already had a child at 182 on 2026-10-20 (export job
# 4318218 over all 38,732 children of 249 parents). None of those 72 children carried
# PURCHASING_COMPANY "ESSX" (55 LUX, 17 blank), and most were sizes added weeks after the
# parent, so they read as stock LUX bought rather than ESSX stock that moved. Excluded by
# decision: only moves made after this shipped count. Reported every cycle so they stay
# visible. The same 30 parents were mixed in the 2026-09-09 grid snapshot.
BASELINE_MIXED_PARENTS: frozenset = frozenset({
    "424-MTPS-0028", "424-MTPS-0035", "ADR-MBTM-0005", "ADS-MSNK-2029", "AHL-MTPS-0014",
    "ART-MTPS-0001", "ASO-MTPS-0006", "AVV-WTPS-0004", "AWG-WBTM-0001", "CDP-MSNK-0003",
    "DBD-MBTM-0002", "GRD-MSNK-0003", "HBS-MACC-0009", "HOO-MSNK-2012", "KRO-MBTM-0003",
    "KRO-MJNS-0001", "KRO-MOTW-0002", "KRO-MTPS-0002", "MMM-MSNK-0002", "MMM-MSNK-0003",
    "MRE-MFTW-0008", "MRE-MFTW-0009", "MRE-XFTW-0007", "MRE-XTPS-0012", "OLY-XBTM-0014",
    "OLY-XOTW-0009", "OLY-XTSH-0005", "PRO-MHDS-0001", "ROA-MTPS-0003", "WIN-MHDS-0001",
})

# Verdicts. MOVED needs the import (a child is still at 249); MOVED_ALL has every active
# child at 182 already and only its Shopify product can still need work.
MOVED = "moved"
MOVED_ALL = "moved_all"
UNCHANGED = "unchanged"
BASELINE = "baseline"
INCOMPLETE = "incomplete"
OTHER = "other"

PURCHASING_COLUMN = "PURCHASING_COMPANY"
IMPORT_HEADER = ("ProductID", "CompanyID", PURCHASING_COLUMN)


class CompanySyncError(Exception):
    """A Shopify write was accepted but the product did not end up where it should."""


@dataclass
class Settings:
    enabled: bool = True
    execute: bool = False
    confirm_with_grid: bool = True
    max_moved_parents_per_cycle: int = 300
    max_shopify_writes_per_cycle: int = 100
    max_seconds: int = 300
    circuit_breaker_consecutive_failures: int = 3
    import_wait_seconds: int = 300

    @classmethod
    def from_config(cls, cfg: Mapping[str, Any] | None) -> "Settings":
        cfg = cfg or {}
        base = cls()
        return cls(**{
            name: type(getattr(base, name))(cfg.get(name, getattr(base, name)))
            for name in base.__dataclass_fields__
        })


# -- pure ---------------------------------------------------------------------


def check_company_export(export: CustomExport, requested: int) -> Optional[str]:
    """Why this export cannot be used to move anything, or None."""
    reason = check_export(export, requested)
    if reason:
        return reason
    if not export.company_column:
        return "the export has no CompanyID column"
    if export.blank_company > MAX_BLANK_RATIO * export.returned:
        return f"{export.blank_company} of {export.returned} rows have no CompanyID"
    return None


def lower_keys(mapping: Mapping[str, Any]) -> Dict[str, Any]:
    """SellerCloud spells SKUs its own way; every lookup here folds case."""
    return {k.lower(): v for k, v in mapping.items()}


def classify_parent(
    parent: str, children: Sequence[str], companies: Mapping[str, int]
) -> str:
    """One parent's verdict from its ACTIVE children. `companies` is keyed lowercase."""
    if parent in BASELINE_MIXED_PARENTS:
        return BASELINE
    if not children:
        return UNCHANGED
    seen = [companies.get(c.lower()) for c in children]
    known = {c for c in seen if c is not None}
    if TO_COMPANY not in known:
        return UNCHANGED
    if known - {FROM_COMPANY, TO_COMPANY}:
        return OTHER
    if None in seen:
        return INCOMPLETE
    return MOVED if FROM_COMPANY in known else MOVED_ALL


def counts_as_moved(parent: str, companies: Iterable[Any]) -> bool:
    """The submit-time version of the rule, for one parent's live child companies.

    One child at 182 is enough, unless the parent is in the baseline.
    """
    if parent in BASELINE_MIXED_PARENTS:
        return False
    for value in companies:
        try:
            if int(value) == TO_COMPANY:
                return True
        except (TypeError, ValueError):
            continue
    return False


def plan_product(product: Any) -> str:
    """What the flip would do to one Shopify product."""
    if product is None:
        return "not_found"
    status = (product.status or "").upper()
    tagged = any(t.lower() == ESSX_TAG.lower() for t in product.tags)
    if status == "ARCHIVED":
        return "archived"
    if status == "DRAFT":
        return "flip" if tagged else "draft_untagged"
    if status == "ACTIVE":
        return "untag" if tagged else "none"
    return f"status_{status.lower() or 'unknown'}"


def build_import_rows(
    children: Sequence[str], companies: Mapping[str, int]
) -> List[tuple]:
    """(sku, 182, "ESSX") for every child still at 249. Never a blank cell.

    A blank PURCHASING_COMPANY cell WIPES the column (measured on TEST-MSNK-0005/8,
    import 4318241), so every row carries the value.
    """
    return [
        (sku, TO_COMPANY, PURCHASING_VALUE)
        for sku in children
        if companies.get(sku.lower()) == FROM_COMPANY
    ]


def render_import_tsv(rows: Sequence[tuple], header: Sequence[str] = IMPORT_HEADER) -> bytes:
    lines = ["\t".join(header)]
    lines.extend("\t".join(str(v) for v in row) for row in rows)
    return ("\n".join(lines) + "\n").encode("utf-8")


def map_products_to_parents(
    products: Sequence[Any], parent_of_child: Mapping[str, str]
) -> tuple[Dict[str, List[Any]], List[tuple]]:
    """Tagged products grouped by parent, via variant SKU -> child_products.parent_sku.

    Never by cutting the SKU: ESSX SKUs carry slashes of their own. Returns
    ({parent: [products]}, [(product, reason)] for products that map to no parent or to
    more than one).
    """
    by_parent: Dict[str, List[Any]] = {}
    unmapped: List[tuple] = []
    for product in products:
        parents = {
            parent_of_child[sku.lower()]
            for sku in product.variant_skus
            if sku and sku.lower() in parent_of_child
        }
        if len(parents) == 1:
            by_parent.setdefault(parents.pop(), []).append(product)
        else:
            unmapped.append((product, "no_parent" if not parents else "several_parents"))
    return by_parent, unmapped


# -- Shopify ------------------------------------------------------------------


async def activate_essx_draft(admin: Any, product: Any) -> str:
    """Make one ESSX product live and drop its tag. Returns the outcome.

    Status first, tag last: the tag is what marks the product for the next cycle, so a
    failure part way leaves it marked and the next cycle finishes the job.
    """
    action = plan_product(product)
    if action not in ("flip", "untag"):
        return action
    gid = product.gid
    if action == "flip":
        await admin.update_product_details(gid, {"status": "ACTIVE"})
        await admin.publish_to_online_store(gid)
    elif product.online_store_url is None:
        await admin.publish_to_online_store(gid)
    await admin.remove_tags(gid, [ESSX_TAG])

    live = await admin.get_product(gid)
    if live is None:
        return "not_found"
    if (live.status or "").upper() != "ACTIVE":
        raise CompanySyncError(f"{gid} is {live.status} after the flip")
    if any(t.lower() == ESSX_TAG.lower() for t in live.tags):
        raise CompanySyncError(f"{gid} still carries {ESSX_TAG!r}")
    return "flipped" if action == "flip" else "untagged"


def _product_image(product: Any) -> Dict[str, Any]:
    return {
        "gid": product.gid,
        "title": product.title,
        "status": product.status,
        "tags": list(product.tags),
        "online_store_url": product.online_store_url,
        "variant_skus": [s for s in product.variant_skus if s],
    }


# -- the cycle ----------------------------------------------------------------


@dataclass
class ParentWork:
    parent: str
    verdict: str
    children: List[str]
    # Active and inactive. The verdict reads active children only (the export has no
    # others), but "flip all sizes" includes an inactive size still at 249, whose company
    # comes from the grid.
    all_children: List[str] = field(default_factory=list)
    companies: Dict[str, Optional[int]] = field(default_factory=dict)
    purchasing: Dict[str, Optional[str]] = field(default_factory=dict)
    products: List[Any] = field(default_factory=list)
    import_rows: List[tuple] = field(default_factory=list)
    outcomes: List[str] = field(default_factory=list)
    note: str = ""


@dataclass
class ParentRows:
    """Company-249 parents from the products database. Read-only."""

    active: Dict[str, List[str]]  # parent -> active children
    every: Dict[str, List[str]]  # parent -> active and inactive children
    parent_of: Dict[str, str]  # lowercased child -> parent, every child


async def load_parents() -> ParentRows:
    """Inactive children are included so a tagged product whose only matching variant is
    an inactive size still finds its parent, and so "all sizes" means all of them."""
    conn = connections.get("product_db")
    rows = await conn.execute_query_dict(
        "SELECT c.sku, c.parent_sku, c.is_active "
        "FROM child_products c JOIN parent_products p ON p.sku = c.parent_sku "
        "WHERE p.company_code = $1 AND p.is_active",
        [FROM_COMPANY],
    )
    out = ParentRows(active={}, every={}, parent_of={})
    for r in rows:
        out.parent_of[r["sku"].lower()] = r["parent_sku"]
        out.active.setdefault(r["parent_sku"], [])
        out.every.setdefault(r["parent_sku"], []).append(r["sku"])
        if r["is_active"]:
            out.active[r["parent_sku"]].append(r["sku"])
    return out


async def _tagged_products(admin: Any) -> List[Any]:
    return [p async for p in admin.products_by_tag(ESSX_TAG)]


async def _grid_companies(children: Sequence[str]) -> tuple[Dict[str, int], Set[str]]:
    """Live CompanyID per child from the grid, plus the SKUs it could not vouch for."""
    from services.batch_value_service import _fetch_grid_rows

    rows, untrusted, _chunks, _quarantined = await _fetch_grid_rows(list(children))
    out: Dict[str, int] = {}
    for sku, row in rows.items():
        try:
            out[sku.lower()] = int(row.get("CompanyID"))
        except (TypeError, ValueError):
            continue
    return out, {s.lower() for s in untrusted}


async def _send_import(rows: Sequence[tuple], wait_seconds: int) -> Dict[str, Any]:
    from services.sellercloud_service import sellercloud_service

    job_id = await sellercloud_service.create_custom_import(render_import_tsv(rows))
    logger.info(f"{SOURCE}: import {job_id} moving {len(rows)} child(ren) to {TO_COMPANY}")
    result: Dict[str, Any] = {"job_id": job_id, "rows": len(rows), "finished": False}
    deadline = time.monotonic() + wait_seconds
    while time.monotonic() < deadline:
        try:
            if await sellercloud_service.is_job_complete(job_id):
                result["finished"] = True
                break
        except RuntimeError as exc:
            result["error"] = str(exc)[:300]
            logger.error(f"{SOURCE}: import {job_id} failed: {exc}")
            break
        await asyncio.sleep(10)
    if not result["finished"] and "error" not in result:
        logger.warning(
            f"{SOURCE}: import {job_id} not finished after {wait_seconds}s; the next "
            f"export shows whether it landed"
        )
    return result


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str))


def _write_report(path: Path, work: Sequence[ParentWork], unmapped: Sequence[tuple],
                  import_job: Optional[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["parent", "verdict", "child_companies", "products",
                    "status_before", "import_rows", "outcome", "import_job", "note"])
        for item in work:
            counts: Dict[str, int] = {}
            for value in item.companies.values():
                key = str(value) if value is not None else "absent"
                counts[key] = counts.get(key, 0) + 1
            w.writerow([
                item.parent,
                item.verdict,
                ",".join(f"{k}:{v}" for k, v in sorted(counts.items())),
                ",".join(p.gid.rsplit("/", 1)[-1] for p in item.products),
                ",".join(p.status for p in item.products),
                len(item.import_rows),
                ";".join(item.outcomes),
                import_job if item.import_rows else "",
                item.note,
            ])
        for product, reason in unmapped:
            w.writerow(["", reason, "", product.gid.rsplit("/", 1)[-1], product.status,
                        0, "", "", ",".join(s for s in product.variant_skus if s)[:300]])


async def run_company_sync(
    export: CustomExport,
    requested: int,
    *,
    settings: Settings,
    reports_dir: str,
    allowlist: Optional[Set[str]] = None,
    max_moved_override: Optional[int] = None,
) -> Dict[str, Any]:
    """One cycle. Never raises; the summary says what happened and why."""
    started = time.monotonic()
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    summary: Dict[str, Any] = {"execute": settings.execute}
    try:
        return await _run(export, requested, settings, Path(reports_dir), ts,
                          allowlist, max_moved_override, summary, started)
    except Exception as exc:  # noqa: BLE001
        logger.exception(f"{SOURCE}: cycle failed")
        summary["error"] = str(exc)[:300]
        return summary


async def _run(export, requested, settings, reports_dir, ts, allowlist,
               max_moved_override, summary, started) -> Dict[str, Any]:
    reason = check_company_export(export, requested)
    if reason:
        logger.error(f"{SOURCE}: skipped, export not usable: {reason}")
        summary["skipped"] = reason
        return summary

    companies = lower_keys(export.companies)
    purchasing = lower_keys(export.purchasing)
    parents = await load_parents()
    active, parent_of = parents.active, parents.parent_of

    work: Dict[str, ParentWork] = {}
    for parent, children in active.items():
        if allowlist is not None and parent not in allowlist:
            continue
        verdict = classify_parent(parent, children, companies)
        if verdict == UNCHANGED:
            continue
        if verdict == BASELINE and not any(
            companies.get(c.lower()) == TO_COMPANY for c in children
        ):
            continue
        work[parent] = ParentWork(
            parent=parent, verdict=verdict, children=list(children),
            all_children=list(parents.every.get(parent) or children),
            companies={c: companies.get(c.lower()) for c in children},
            # "" = known blank. Only children in the export are known; an inactive size's
            # PURCHASING_COMPANY is never read, so it stays out (and a rollback knows it
            # must not write that column for it).
            purchasing={
                c: purchasing.get(c.lower(), "") for c in children if c.lower() in companies
            },
        )

    # Shopify side: every product still carrying the ESSX tag, mapped back to its parent.
    from services.shopify_client import (
        ShopifyError,
        enable_writes,
        get_shopify_client,
        writes_allowed_by_config,
        writes_enabled,
    )
    from services.shopify_admin import ShopifyAdmin

    admin = ShopifyAdmin(await get_shopify_client(STORE))
    unmapped: List[tuple] = []
    sweep_ok = True
    try:
        tagged = await _tagged_products(admin)
        by_parent, unmapped = map_products_to_parents(tagged, parent_of)
        summary["tagged_products"] = len(tagged)
    except ShopifyError as exc:
        logger.error(f"{SOURCE}: ESSX tag sweep failed, no Shopify work this cycle: {exc}")
        by_parent, sweep_ok = {}, False
        summary["sweep_error"] = str(exc)[:300]
    for parent, products in by_parent.items():
        if parent in work:
            work[parent].products = products

    # A MOVED_ALL parent with no tagged product left is finished: nothing to report.
    for parent in [p for p, w in work.items() if w.verdict == MOVED_ALL and not w.products]:
        del work[parent]

    actionable = [w for w in work.values() if w.verdict in (MOVED, MOVED_ALL)]
    summary["moved_parents"] = len(actionable)
    for verdict in (MOVED, MOVED_ALL, BASELINE, INCOMPLETE, OTHER):
        summary[verdict] = sum(1 for w in work.values() if w.verdict == verdict)

    gate = (
        max_moved_override if max_moved_override is not None
        else settings.max_moved_parents_per_cycle
    )
    report_only = not settings.execute
    if len(actionable) > gate:
        logger.error(
            f"{SOURCE}: {len(actionable)} moved parents is over the cap of {gate}; "
            f"report only this cycle"
        )
        summary["gated"] = f"{len(actionable)} moved parents > {gate}"
        report_only = True
    if settings.execute and not writes_allowed_by_config():
        logger.warning(f"{SOURCE}: execute=true but [shopify] allow_writes is false; report only")
        summary["gated"] = "shopify allow_writes is false"
        report_only = True

    # Second source before anything is written: the grid must agree child by child. It
    # also reads the inactive sizes, which the export does not carry.
    if actionable and settings.confirm_with_grid:
        grid, untrusted = await _grid_companies(
            [c for w in actionable for c in w.all_children]
        )
        for item in actionable:
            if any(c.lower() in untrusted for c in item.children):
                item.note = "grid_unconfirmed"
            elif classify_parent(item.parent, item.children, grid) != item.verdict:
                item.note = "grid_disagrees"
            else:
                item.companies = {
                    c: grid.get(c.lower()) for c in item.all_children
                    if c in item.children or grid.get(c.lower()) is not None
                }
        actionable = [w for w in actionable if not w.note]

    for item in actionable:
        if len(item.products) > 1:
            item.note = "duplicate_products"
        item.import_rows = build_import_rows(
            sorted(item.companies), {c.lower(): v for c, v in item.companies.items()}
        )

    reports = reports_dir / "company_changes"
    before_path = reports / f"company_change_before_{ts}.json"
    _write_json(before_path, {
        "taken_at": ts,
        "execute": settings.execute and not report_only,
        "parents": {
            w.parent: {
                "verdict": w.verdict,
                "note": w.note,
                # PURCHASING_COMPANY None = never read (an inactive size), "" = blank.
                "children": {
                    c: {"CompanyID": w.companies.get(c), PURCHASING_COLUMN: w.purchasing.get(c)}
                    for c in sorted(w.companies)
                },
                "products": [_product_image(p) for p in w.products],
            }
            for w in actionable
        },
    })
    summary["before_image"] = str(before_path)

    import_job: Optional[str] = None
    if not report_only:
        if not writes_enabled():
            enable_writes(f"{SOURCE} execute=true")

        writes = consecutive = 0
        deadline = started + settings.max_seconds
        for item in actionable:
            if item.note == "duplicate_products" or not sweep_ok:
                continue
            for product in item.products:
                if writes >= settings.max_shopify_writes_per_cycle:
                    item.outcomes.append("deferred_cap")
                    continue
                if time.monotonic() > deadline:
                    item.outcomes.append("deferred_time")
                    continue
                if consecutive >= settings.circuit_breaker_consecutive_failures:
                    item.outcomes.append("deferred_breaker")
                    continue
                try:
                    outcome = await activate_essx_draft(admin, product)
                    if outcome in ("flipped", "untagged"):
                        writes += 1
                    consecutive = 0
                except Exception as exc:  # noqa: BLE001
                    consecutive += 1
                    outcome = f"failed: {str(exc)[:150]}"
                    logger.error(f"{SOURCE}: {item.parent} {product.gid} flip failed: {exc}")
                item.outcomes.append(outcome)
        summary["shopify_writes"] = writes

        rows = [row for item in actionable for row in item.import_rows]
        if rows:
            try:
                result = await _send_import(rows, settings.import_wait_seconds)
                import_job = result["job_id"]
                summary["import"] = result
            except Exception as exc:  # noqa: BLE001
                logger.exception(f"{SOURCE}: import of {len(rows)} rows failed")
                summary["import"] = {"error": str(exc)[:300], "rows": len(rows)}
    else:
        for item in actionable:
            item.outcomes.extend(f"would_{plan_product(p)}" for p in item.products)

    ordered = sorted(work.values(), key=lambda w: (w.verdict, w.parent))
    report_path = reports / f"company_changes_{ts}.tsv"
    _write_report(report_path, ordered, unmapped, import_job)
    summary["report"] = str(report_path)
    summary["import_rows"] = sum(len(w.import_rows) for w in actionable)
    summary["report_only"] = report_only
    summary["elapsed_s"] = round(time.monotonic() - started, 1)
    logger.info(f"{SOURCE}: {summary}")
    return summary
