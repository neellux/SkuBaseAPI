"""
Background poller that walks the `secondary_skus` materialized view and moves
any residual SellerCloud inventory from each secondary SKU to its current
primary SKU.

Safety gates (per the plan):
  1. Never transfer a SKU with ReservedQty > 0 — a reserved unit is tied to
     an open order; moving it would break fulfilment. Logged as failed with
     metadata.skip_reason='reserved_qty'.
  2. Skip SKUs with no physical inventory silently (no DB row).

Each non-silent outcome is logged as a single row in
`sellercloud_sync_operations` with `operation='transfer_inventory'`.
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from tortoise import connections

from config import config
from services.sellercloud_internal_service import sellercloud_internal_service
from services.sellercloud_sync_logger import complete_operation, create_operation

logger = logging.getLogger(__name__)

GRID_ENDPOINT = "/Manage/ManageEntity/GetGridData"
SOURCE_TAG = "secondary_sync_poller"


class SecondaryInventoryTransferPoller:
    """Runs one full cycle per day at a fixed wall-clock time
    (default 04:00 America/New_York) via APScheduler's AsyncIOScheduler
    and a CronTrigger — so DST transitions, missed fires, and misaligned
    system clocks are handled by the library.
    """

    def __init__(self) -> None:
        self.name = self.__class__.__name__
        cfg = config.get("secondary_inventory_transfer_poller", {})
        self.enabled: bool = cfg.get("enabled", True)
        self.per_cycle_limit: int = cfg.get("per_cycle_limit", 0)
        self.prefilter_batch_size: int = cfg.get("prefilter_batch_size", 100)
        self._schedule_hour: int = int(cfg.get("daily_hour", 4))
        self._schedule_minute: int = int(cfg.get("daily_minute", 0))
        self._schedule_tz: ZoneInfo = ZoneInfo(cfg.get("timezone", "America/New_York"))
        self._scheduler: Optional[AsyncIOScheduler] = None

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
            id="secondary_inventory_transfer_daily",
            name=self.name,
            max_instances=1,
            coalesce=True,          # collapse missed runs into one
            misfire_grace_time=3600, # 1h catch-up window
        )
        self._scheduler.start()

        next_run = self._scheduler.get_job("secondary_inventory_transfer_daily").next_run_time
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

    async def _poll_cycle(self) -> None:
        cycle_id = str(uuid.uuid4())

        pairs = await self._fetch_secondary_mapping()
        if self.per_cycle_limit:
            pairs = pairs[: self.per_cycle_limit]
        if not pairs:
            logger.info(f"{self.name}: no secondary SKUs to process")
            return

        logger.info(f"{self.name}: cycle {cycle_id} starting — {len(pairs)} secondary SKUs")

        classified = await self._classify_via_grid(pairs)

        transfer_count = 0
        reserved_count = 0
        error_count = 0

        for entry in classified:
            action = entry["action"]
            try:
                if action == "skip_reserved":
                    await self._log_reserved_skip(cycle_id, entry)
                    reserved_count += 1
                elif action == "transfer":
                    await self._process_transfer(cycle_id, entry)
                    transfer_count += 1
            except Exception:
                error_count += 1
                logger.exception(
                    f"{self.name}: unexpected error processing "
                    f"{entry.get('secondary_sku')} -> {entry.get('current_primary_sku')}"
                )

        logger.info(
            f"{self.name}: cycle {cycle_id} done — total={len(pairs)}, "
            f"transfers={transfer_count}, reserved_skip={reserved_count}, "
            f"errors={error_count}"
        )

    # ------------------------------------------------------------------
    # DB
    # ------------------------------------------------------------------
    @staticmethod
    async def _fetch_secondary_mapping() -> List[Dict[str, str]]:
        conn = connections.get("product_db")
        rows = await conn.execute_query_dict(
            "SELECT secondary_sku, current_primary_sku "
            "FROM secondary_skus ORDER BY secondary_sku"
        )
        return rows

    # ------------------------------------------------------------------
    # Grid prefilter
    # ------------------------------------------------------------------
    async def _classify_via_grid(
        self, pairs: List[Dict[str, str]]
    ) -> List[Dict[str, Any]]:
        """Returns one entry per pair with an 'action' field:
        'transfer' | 'skip_reserved' | 'none' (no DB row to write)."""
        by_sku = {p["secondary_sku"]: p["current_primary_sku"] for p in pairs}
        sku_list = list(by_sku.keys())

        grid_rows: Dict[str, Dict[str, Any]] = {}
        for i in range(0, len(sku_list), self.prefilter_batch_size):
            batch = sku_list[i : i + self.prefilter_batch_size]
            try:
                rows = await self._grid_batch(batch)
            except Exception as e:
                logger.error(
                    f"{self.name}: grid prefilter batch {i // self.prefilter_batch_size + 1} "
                    f"failed ({len(batch)} SKUs): {e}"
                )
                continue
            for row in rows:
                sku = row.get("ID") or row.get("ProductMasterSKU")
                if isinstance(sku, str) and sku in by_sku:
                    grid_rows[sku] = row

        classified: List[Dict[str, Any]] = []
        for sku, primary in by_sku.items():
            row = grid_rows.get(sku)
            if not row:
                classified.append({
                    "secondary_sku": sku,
                    "current_primary_sku": primary,
                    "action": "none",
                })
                continue

            physical = _to_int(row.get("AggregatePhysicalQty"))
            reserved = _to_int(row.get("ReservedQty"))

            if reserved > 0:
                classified.append({
                    "secondary_sku": sku,
                    "current_primary_sku": primary,
                    "action": "skip_reserved",
                    "physical_qty": physical,
                    "reserved_qty": reserved,
                })
            elif physical > 0:
                classified.append({
                    "secondary_sku": sku,
                    "current_primary_sku": primary,
                    "action": "transfer",
                    "physical_qty": physical,
                    "reserved_qty": reserved,
                })
            else:
                classified.append({
                    "secondary_sku": sku,
                    "current_primary_sku": primary,
                    "action": "none",
                })

        return classified

    async def _grid_batch(self, skus: List[str]) -> List[Dict[str, Any]]:
        """Single GetGridData call with paging to pull every row for the
        batch. No qty filter — we want physical + reserved even when sellable is 0."""
        collected: List[Dict[str, Any]] = []
        page = 1
        per_page = 200
        while True:
            payload = _build_grid_payload(skus, page=page, per_page=per_page)
            body = await sellercloud_internal_service.post(GRID_ENDPOINT, data=payload)
            data = body.get("Data") or {}
            rows = data.get("Grid") or []
            collected.extend(rows)
            total = data.get("TotalResults")
            if isinstance(total, int):
                if len(collected) >= total or not rows:
                    break
            elif len(rows) < per_page:
                break
            page += 1
            if page > 50:
                logger.warning(f"{self.name}: grid paging bailed at page {page}")
                break
        return collected

    # ------------------------------------------------------------------
    # Logging + transfer
    # ------------------------------------------------------------------
    async def _log_reserved_skip(self, cycle_id: str, entry: Dict[str, Any]) -> None:
        from_sku = entry["secondary_sku"]
        to_sku = entry["current_primary_sku"]
        physical = entry.get("physical_qty", 0)
        reserved = entry.get("reserved_qty", 0)

        metadata = {
            "cycle_id": cycle_id,
            "physical_qty": physical,
            "reserved_qty": reserved,
            "skip_reason": "reserved_qty",
            "checked_at": _utc_now(),
        }

        op_id = await create_operation(
            operation="transfer_inventory",
            source_sku=from_sku,
            target_sku=to_sku,
            value=str(physical),
            source=SOURCE_TAG,
            metadata=metadata,
        )
        await complete_operation(
            operation_id=op_id,
            status="failed",
            completed_steps=0,
            total_steps=0,
            error_message="Skipped: reserved qty > 0",
        )
        logger.info(
            f"{self.name}: skipped {from_sku} -> {to_sku} "
            f"(reserved={reserved}, physical={physical})"
        )

    async def _process_transfer(self, cycle_id: str, entry: Dict[str, Any]) -> None:
        from_sku = entry["secondary_sku"]
        to_sku = entry["current_primary_sku"]
        physical = entry.get("physical_qty", 0)
        reserved = entry.get("reserved_qty", 0)

        op_id = await create_operation(
            operation="transfer_inventory",
            source_sku=from_sku,
            target_sku=to_sku,
            value=str(physical),
            source=SOURCE_TAG,
            metadata={
                "cycle_id": cycle_id,
                "physical_qty": physical,
                "reserved_qty": reserved,
                "started_at": _utc_now(),
            },
        )

        try:
            result = await sellercloud_internal_service.transfer_all_inventory(
                from_sku=from_sku, to_sku=to_sku
            )
        except Exception as exc:
            logger.exception(f"{self.name}: transfer crashed {from_sku} -> {to_sku}")
            await complete_operation(
                operation_id=op_id,
                status="failed",
                completed_steps=0,
                total_steps=0,
                error_message=f"{type(exc).__name__}: {exc}",
            )
            return

        status, total_steps, completed_steps, transferred_qty, error_msg = _classify_transfer_result(result)

        # Rebuild metadata with the transfer result merged in. We write it back
        # via create_operation-equivalent UPDATE because complete_operation
        # doesn't update metadata. Do this via raw SQL for a single UPDATE.
        await _merge_metadata_and_finalize(
            op_id=op_id,
            status=status,
            completed_steps=completed_steps,
            total_steps=total_steps,
            error_message=error_msg,
            value=str(transferred_qty if transferred_qty else physical),
            extra_metadata={
                "completed_at": _utc_now(),
                "summary": result.get("summary"),
                "warehouses": result.get("warehouses"),
                "no_inventory": bool(result.get("no_inventory")),
                "partial": bool(result.get("partial")),
                "transfer_error": result.get("error"),
            },
        )

        logger.info(
            f"{self.name}: transferred {from_sku} -> {to_sku} "
            f"status={status} qty={transferred_qty}/{physical}"
        )


# ----------------------------------------------------------------------
# Module-level helpers
# ----------------------------------------------------------------------

def _to_int(v: Any) -> int:
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, (int, float)):
        return int(v)
    return 0


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_grid_payload(skus: List[str], page: int, per_page: int) -> Dict[str, Any]:
    return {
        "Kind": 13,
        "SelectedFilters": [
            {"FilterId": "txtGlobalSearch", "FilterPropertyName": "GlobalSearchKeyWord", "FilterSelectedValues": None},
            {"FilterId": "txtSKU", "FilterPropertyName": "SKU", "FilterSelectedValues": skus},
            {"FilterId": "lstCompanies", "FilterPropertyName": "CompanyID", "FilterSelectedValues": None},
            {"FilterId": "txtUPCList", "FilterPropertyName": "UPC", "FilterSelectedValues": None},
            {"FilterId": "ddlManufacturers", "FilterPropertyName": "ManufacturerNames", "FilterSelectedValues": None},
            {"FilterId": "ddlActiveStatus", "FilterPropertyName": "ActiveStatus", "FilterSelectedValues": ["-1"]},
            # Only return SKUs with AggregateQty >= 1 (sellable). Oversold
            # (-1) and empty (0) SKUs are excluded server-side so the poller
            # never sees them.
            {"FilterId": "vqAggregateQtyRange", "FilterSelectedValues": ["1", None]},
        ],
        "PageNumber": page,
        "ResultsPerPage": per_page,
        "SortColumn": "ID",
        "SortDirection": True,
        "IncludeTotals": True,
        "UtcOffset": 330,
        "GlobalSearchKeyWord": "",
        "Key": None,
        "SavedViewID": None,
    }


def _classify_transfer_result(
    result: Dict[str, Any],
) -> tuple[str, int, int, int, Optional[str]]:
    """Maps transfer_all_inventory output onto (status, total_steps,
    completed_steps, transferred_qty, error_message)."""
    summary = result.get("summary") or {}
    transferred = _to_int(summary.get("transferred_qty"))
    failed_qty = _to_int(summary.get("failed_qty"))
    warehouses = result.get("warehouses") or []
    total_steps = len(warehouses)
    completed_steps = sum(1 for w in warehouses if w.get("status") == "completed")

    if result.get("no_inventory"):
        return ("completed", 0, 0, 0, None)
    if result.get("success") and not result.get("partial"):
        return ("completed", total_steps, completed_steps, transferred, None)
    if result.get("success") and result.get("partial"):
        first_err = next(
            (w.get("error") for w in warehouses if w.get("status") in ("failed", "partial") and w.get("error")),
            None,
        )
        return ("partial_failure", total_steps, completed_steps, transferred, first_err)

    # success=False
    err = result.get("error")
    if not err:
        err = next(
            (w.get("error") for w in warehouses if w.get("error")),
            None,
        ) or f"Transfer failed ({failed_qty} qty failed)"
    return ("failed", total_steps, completed_steps, transferred, err)


async def _merge_metadata_and_finalize(
    *,
    op_id: int,
    status: str,
    completed_steps: int,
    total_steps: int,
    error_message: Optional[str],
    value: str,
    extra_metadata: Dict[str, Any],
) -> None:
    """Finalize an operation row while merging fresh fields into metadata.
    sellercloud_sync_logger.complete_operation doesn't update metadata, so we
    do this single UPDATE ourselves to keep the row self-contained."""
    import json as _json

    conn = connections.get("product_db")
    await conn.execute_query(
        """
        UPDATE sellercloud_sync_operations
        SET status = $2,
            completed_steps = $3,
            total_steps = $4,
            error_message = $5,
            value = $6,
            completed_at = CURRENT_TIMESTAMP,
            metadata = COALESCE(metadata, '{}'::jsonb) || $7::jsonb
        WHERE id = $1
        """,
        [op_id, status, completed_steps, total_steps, error_message, value, _json.dumps(extra_metadata)],
    )


secondary_inventory_transfer_poller = SecondaryInventoryTransferPoller()
