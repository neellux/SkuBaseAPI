"""Daily UPC/alias reconciliation poller.

Runs on a cron (default 04:30 America/New_York). Each cycle:
  1. Pull active child SKUs + DB UPC/keyword state.
  2. Pull SellerCloud alias state via ExportStandardInfo Kind=2 (50k SKU batches).
  3. Pull SellerCloud UPC state via ExportCustomInfo (job + poll + download).
  4. Reconcile (see services/daily_sellercloud_sync_service.py).
  5. Apply safety gates (allowlist, per-cycle caps, % drift guard, runtime).
  6. Write XLSX + rollback CSV to reports_dir.
  7. If execute=true: dispatch surviving actions to SellerCloud and log each
     to sellercloud_sync_operations.

DB is truth; SC gets updated. Scope: only SKUs with is_active=TRUE.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

import httpx
import orjson
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from tortoise import connections

from config import config
from services.sellercloud_internal_service import (
    SellercloudPermanentError,
    sellercloud_internal_service,
)
from services.sellercloud_sync_logger import complete_operation, create_operation
from services.daily_sellercloud_sync_service import (
    Action,
    State,
    build_db_state,
    build_sc_state,
    order_per_sku,
    reconcile,
    write_actions_xlsx,
    write_rollback_csv,
)

logger = logging.getLogger(__name__)

SOURCE_TAG = "daily_sellercloud_sync_poller"

# SellerCloud server-side limits — not ops knobs.
ALIAS_EXPORT_BATCH_SIZE = 50_000
JOB_POLL_INTERVAL = 10.0
JOB_POLL_TIMEOUT = 1800.0  # 30 min for the biggest custom export


class DailySellercloudSyncPoller:
    """Daily reconciler for SellerCloud aliases vs DB truth.

    Mirrors SecondaryInventoryTransferPoller's APScheduler + cron pattern.
    """

    def __init__(self) -> None:
        self.name = self.__class__.__name__
        cfg = config.get("daily_sellercloud_sync_poller", {})
        self.enabled: bool = cfg.get("enabled", True)
        self._schedule_hour: int = int(cfg.get("daily_hour", 4))
        self._schedule_minute: int = int(cfg.get("daily_minute", 30))
        self._schedule_tz: ZoneInfo = ZoneInfo(cfg.get("timezone", "America/New_York"))

        # Execution gate + safety caps
        self.execute: bool = bool(cfg.get("execute", False))
        self.max_actions_per_cycle: int = int(cfg.get("max_actions_per_cycle", 500))
        self.max_deletes_per_cycle: int = int(cfg.get("max_deletes_per_cycle", 50))
        self.max_pct_of_total_aliases_changed: float = float(
            cfg.get("max_pct_of_total_aliases_changed", 5.0)
        )
        self.sc_concurrency: int = int(cfg.get("sc_concurrency", 1))
        self.max_runtime_seconds: int = int(cfg.get("max_runtime_seconds", 900))
        self.cutover_allowlist_path: str = cfg.get("cutover_allowlist_path", "") or ""
        self.circuit_breaker_consecutive_failures: int = int(
            cfg.get("circuit_breaker_consecutive_failures", 3)
        )
        self.write_rollback_csv_flag: bool = bool(cfg.get("write_rollback_csv", True))

        # Where reports go
        default_reports_dir = str(Path(__file__).resolve().parent.parent / "logs" / "daily_sellercloud_sync")
        self.reports_dir: str = cfg.get("reports_dir", default_reports_dir)
        self.reports_retention: int = int(cfg.get("reports_retention_count", 30))

        self._scheduler: Optional[AsyncIOScheduler] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        if not self.enabled:
            logger.info(f"{self.name}: disabled in config, skipping start")
            return
        if self._scheduler and self._scheduler.running:
            logger.info(f"{self.name}: already running")
            return

        Path(self.reports_dir).mkdir(parents=True, exist_ok=True)

        self._scheduler = AsyncIOScheduler(timezone=self._schedule_tz)
        trigger = CronTrigger(
            hour=self._schedule_hour,
            minute=self._schedule_minute,
            timezone=self._schedule_tz,
        )
        self._scheduler.add_job(
            self._poll_cycle,
            trigger=trigger,
            id="daily_sellercloud_sync_daily",
            name=self.name,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
        self._scheduler.start()
        next_run = self._scheduler.get_job("daily_sellercloud_sync_daily").next_run_time
        logger.info(
            f"{self.name}: scheduled daily at "
            f"{self._schedule_hour:02d}:{self._schedule_minute:02d} "
            f"{self._schedule_tz.key}; next run at {next_run.isoformat(timespec='seconds')}; "
            f"execute={self.execute}; reports_dir={self.reports_dir}"
        )

    async def stop(self) -> None:
        if not self._scheduler:
            return
        logger.info(f"{self.name}: stopping...")
        self._scheduler.shutdown(wait=False)
        self._scheduler = None
        logger.info(f"{self.name}: stopped")

    async def run_once(self) -> dict:
        """Trigger a cycle manually (e.g. from an ops endpoint or shell)."""
        return await self._poll_cycle()

    # ------------------------------------------------------------------
    # Main cycle
    # ------------------------------------------------------------------

    async def _poll_cycle(self) -> dict:
        cycle_id = str(uuid.uuid4())
        cycle_started = time.perf_counter()
        cycle_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        deadline = cycle_started + self.max_runtime_seconds
        logger.info(f"{self.name}: cycle {cycle_id} starting; execute={self.execute}")

        try:
            # 1. DB state
            db_state = await self._load_db_state()
            logger.info(f"{self.name}: DB state for {len(db_state)} active SKUs")

            if not db_state:
                logger.warning(f"{self.name}: no active SKUs in DB; aborting cycle")
                return {"cycle_id": cycle_id, "skipped": "no_active_skus"}

            all_skus = sorted(db_state.keys())

            # 2. SC alias state (batched ExportStandardInfo)
            sc_aliases = await self._fetch_sc_aliases(all_skus, deadline)
            logger.info(f"{self.name}: SC aliases pulled for {len(sc_aliases)} SKUs")

            # 3. SC primary UPC state (one ExportCustomInfo job)
            sc_primaries = await self._fetch_sc_primaries(all_skus, deadline)
            logger.info(f"{self.name}: SC primary UPCs pulled for {len(sc_primaries)} SKUs")

            sc_state = build_sc_state(sc_aliases, sc_primaries)

            # 4. Reconcile
            result = reconcile(db_state, sc_state)
            actions = order_per_sku(result.actions)
            logger.info(
                f"{self.name}: reconciled — {len(actions)} actions, "
                f"{len(result.db_conflicts)} db conflicts"
            )

            # 5. Apply safety gates
            allowlist = self._load_allowlist()
            gated_reason = self._check_gates(actions, db_state, allowlist)

            # 6. Write artifacts (always, even when gated)
            xlsx_path = Path(self.reports_dir) / f"daily_sellercloud_sync_{cycle_ts}.xlsx"
            write_actions_xlsx(actions, result.db_conflicts, xlsx_path)
            rollback_path = xlsx_path.with_suffix(".rollback.csv")
            if self.write_rollback_csv_flag:
                write_rollback_csv(actions, rollback_path)
            self._rotate_reports()
            self._update_latest_symlink(xlsx_path)
            logger.info(f"{self.name}: wrote {xlsx_path}")

            # 7. Filter to executable actions
            if allowlist is not None:
                actions = [a for a in actions if a.sku in allowlist]

            executed = 0
            failed = 0
            blocked = 0
            skipped = 0

            if gated_reason:
                logger.warning(f"{self.name}: cycle gated by {gated_reason}; not executing")
            elif not self.execute:
                logger.info(f"{self.name}: dry-run mode (execute=false); not executing")
            else:
                # 8. Execute, per-SKU serial ordering, semaphore-bounded across SKUs
                logger.info(
                    f"{self.name}: executing {len(actions)} actions "
                    f"with sc_concurrency={self.sc_concurrency}"
                )
                executed, failed, blocked, skipped = await self._execute(
                    actions, cycle_id, deadline
                )

            elapsed = time.perf_counter() - cycle_started
            summary = {
                "cycle_id": cycle_id,
                "elapsed_seconds": round(elapsed, 2),
                "active_skus": len(db_state),
                "total_actions": len(result.actions),
                "db_conflicts": len(result.db_conflicts),
                "gated_reason": gated_reason,
                "execute": self.execute,
                "executed": executed,
                "failed": failed,
                "blocked": blocked,
                "skipped": skipped,
                "xlsx": str(xlsx_path),
            }
            logger.info(f"{self.name}: cycle {cycle_id} done — {summary}")
            return summary

        except Exception:
            logger.exception(f"{self.name}: cycle {cycle_id} crashed")
            raise

    # ------------------------------------------------------------------
    # DB
    # ------------------------------------------------------------------

    async def _load_db_state(self) -> dict[str, State]:
        conn = connections.get("product_db")
        upc_rows = await conn.execute_query_dict(
            """
            SELECT u.child_sku, u.upc, u.is_primary_upc
            FROM child_upcs u
            INNER JOIN child_products p ON p.sku = u.child_sku
            WHERE p.is_active = TRUE
            """
        )
        kw_rows = await conn.execute_query_dict(
            "SELECT sku, keywords FROM child_products WHERE is_active = TRUE"
        )
        return build_db_state(upc_rows, kw_rows)

    # ------------------------------------------------------------------
    # SellerCloud exports
    # ------------------------------------------------------------------

    async def _fetch_sc_aliases(
        self, skus: list[str], deadline: float
    ) -> dict[str, set[str]]:
        """Run ExportStandardInfo Kind=2 in 50k-SKU batches and merge results."""
        merged: dict[str, set[str]] = {}
        for i in range(0, len(skus), ALIAS_EXPORT_BATCH_SIZE):
            self._enforce_deadline(deadline, "during alias export")
            batch = skus[i : i + ALIAS_EXPORT_BATCH_SIZE]
            batch_num = i // ALIAS_EXPORT_BATCH_SIZE + 1
            logger.info(
                f"{self.name}: alias export batch {batch_num} "
                f"({len(batch)} SKUs)"
            )
            tsv_bytes = await self._export_aliases_batch(batch)
            self._merge_alias_tsv(tsv_bytes, merged)
        return merged

    async def _export_aliases_batch(self, skus: list[str]) -> bytes:
        payload = {"StandardExportKind": "2", "ProductIds": skus}
        await sellercloud_internal_service._ensure_authenticated()
        client = await sellercloud_internal_service._get_client()
        resp = await client.post(
            f"{sellercloud_internal_service.base_url}/Product/ExportProductCatalogInfo/ExportStandardInfo",
            content=orjson.dumps(payload),
            headers={"Content-Type": "application/json; charset=UTF-8"},
            timeout=httpx.Timeout(600.0),
        )
        resp.raise_for_status()
        body = orjson.loads(resp.content)
        downloads = body.get("FileDownloads") or []
        if not downloads:
            msg = (body.get("Notification") or {}).get("Message") or str(body)[:200]
            raise RuntimeError(f"ExportStandardInfo returned no FileDownloads: {msg}")
        first = downloads[0]
        file_id = first.get("FileID") or first.get("FileId")
        pretty = first.get("PrettyFileName") or "ProductAlias.txt"
        return await self._download_file(file_id, pretty)

    def _merge_alias_tsv(self, tsv_bytes: bytes, into: dict[str, set[str]]) -> None:
        text = tsv_bytes.decode("utf-8", "replace")
        lines = text.splitlines()
        if not lines:
            return
        # Header is "ProductID\tAlias"
        for line in lines[1:]:
            parts = line.split("\t", 1)
            if len(parts) != 2:
                continue
            sku, alias = parts[0].strip(), parts[1].strip()
            if sku and alias:
                into.setdefault(sku, set()).add(alias)

    async def _fetch_sc_primaries(
        self, skus: list[str], deadline: float
    ) -> dict[str, str]:
        """Run ExportCustomInfo with all SKUs in one queued job, parse UPC column."""
        await sellercloud_internal_service._ensure_authenticated()
        client = await sellercloud_internal_service._get_client()
        base_url = sellercloud_internal_service.base_url

        payload = {
            "FieldNames": ["ProductID", "UPC"],
            "DisplayNames": ["", ""],
            "FileFormat": "0",
            "SortBy": "",
            "ProductIds": skus,
        }
        resp = await client.post(
            f"{base_url}/Product/ExportProductCatalogInfo/ExportCustomInfo",
            content=orjson.dumps(payload),
            headers={"Content-Type": "application/json; charset=UTF-8"},
            timeout=httpx.Timeout(600.0),
        )
        resp.raise_for_status()
        kickoff = orjson.loads(resp.content)

        msg = (kickoff.get("Notification") or {}).get("Message") or ""
        m = re.search(r"queued-job-details\.aspx\?id=(\d+)", msg)
        if not m:
            raise RuntimeError(f"ExportCustomInfo did not return a job id: {kickoff}")
        job_id = m.group(1)
        logger.info(f"{self.name}: ExportCustomInfo job {job_id} queued")

        await self._poll_job(client, base_url, job_id, deadline)

        file_id = await self._resolve_job_output_file(client, base_url, job_id)
        pretty = f"{job_id}.txt"
        tsv_bytes = await self._download_file(file_id, pretty)
        return self._parse_upc_tsv(tsv_bytes)

    def _parse_upc_tsv(self, tsv_bytes: bytes) -> dict[str, str]:
        # Note: SellerCloud's saved column preference may add ProductName as
        # column 2. Detect column count and pick the last column as UPC.
        text = tsv_bytes.decode("utf-8", "replace")
        lines = text.splitlines()
        if not lines:
            return {}
        header = lines[0].split("\t")
        try:
            upc_idx = header.index("UPC")
        except ValueError:
            upc_idx = len(header) - 1
        out: dict[str, str] = {}
        for line in lines[1:]:
            parts = line.split("\t")
            if len(parts) <= upc_idx:
                continue
            sku, upc = parts[0].strip(), parts[upc_idx].strip()
            if sku and upc:
                out[sku] = upc
        return out

    async def _poll_job(
        self, client: httpx.AsyncClient, base_url: str, job_id: str, deadline: float
    ) -> None:
        while True:
            self._enforce_deadline(deadline, f"polling job {job_id}")
            resp = await client.post(
                f"{base_url}/QueuedJob/Details/Load",
                content=orjson.dumps({
                    "EntityKind": 10,
                    "EntityId": str(job_id),
                    "PathName": "/queued-jobs/queued-job-details.aspx",
                    "QueryString": f"?id={job_id}",
                }),
                headers={"Content-Type": "application/json; charset=UTF-8"},
                timeout=httpx.Timeout(60.0),
            )
            resp.raise_for_status()
            body = orjson.loads(resp.content)
            basic = ((body.get("Data") or {}).get("Content") or {}).get("Basic") or {}
            status = basic.get("Status")
            if status == 3:  # Completed
                return
            if status in (4, 5, 6, 7):
                err = basic.get("ErrorMessage") or "<no error>"
                raise RuntimeError(f"job {job_id} failed (Status={status}): {err}")
            await asyncio.sleep(JOB_POLL_INTERVAL)

    async def _resolve_job_output_file(
        self, client: httpx.AsyncClient, base_url: str, job_id: str
    ) -> str:
        resp = await client.post(
            f"{base_url}/QueuedJob/Details/ProcessAction",
            content=orjson.dumps({
                "Parameters": {
                    "EntityKind": 10,
                    "EntityId": str(job_id),
                    "PathName": "/queued-jobs/queued-job-details.aspx",
                    "QueryString": f"?id={job_id}",
                    "SelectedGridItemIds": None,
                },
                "ActionKind": 1,
            }),
            headers={"Content-Type": "application/json; charset=UTF-8"},
            timeout=httpx.Timeout(60.0),
        )
        resp.raise_for_status()
        body = orjson.loads(resp.content)
        downloads = body.get("FileDownloads") or []
        if downloads and (downloads[0].get("FileID") or downloads[0].get("FileId")):
            return downloads[0].get("FileID") or downloads[0].get("FileId")
        raise RuntimeError(f"ProcessAction returned no FileID for job {job_id}: {body}")

    async def _download_file(self, file_id: str, pretty_name: str) -> bytes:
        client = await sellercloud_internal_service._get_client()
        resp = await client.get(
            f"{sellercloud_internal_service.base_url}/Files/Download",
            params={"FileID": file_id, "PrettyFileName": pretty_name},
            timeout=httpx.Timeout(600.0),
        )
        resp.raise_for_status()
        return resp.content

    # ------------------------------------------------------------------
    # Safety gates
    # ------------------------------------------------------------------

    def _load_allowlist(self) -> Optional[set[str]]:
        if not self.cutover_allowlist_path:
            return None
        path = Path(self.cutover_allowlist_path)
        if not path.exists():
            logger.warning(f"{self.name}: cutover_allowlist_path={path} not found; treating as empty")
            return set()
        skus = {ln.strip() for ln in path.read_text().splitlines() if ln.strip()}
        logger.info(f"{self.name}: allowlist has {len(skus)} SKUs")
        return skus

    def _check_gates(
        self,
        actions: list[Action],
        db_state: dict[str, State],
        allowlist: Optional[set[str]],
    ) -> Optional[str]:
        applicable = actions
        if allowlist is not None:
            applicable = [a for a in actions if a.sku in allowlist]

        if len(applicable) > self.max_actions_per_cycle:
            return (
                f"max_actions_per_cycle={self.max_actions_per_cycle} exceeded "
                f"({len(applicable)} applicable actions)"
            )

        deletes = sum(1 for a in applicable if a.action == "delete_alias")
        if deletes > self.max_deletes_per_cycle:
            return (
                f"max_deletes_per_cycle={self.max_deletes_per_cycle} exceeded "
                f"({deletes} deletes)"
            )

        # % drift guard: count of alias-like changes (add/delete) vs total db aliases
        total_aliases = sum(len(s.aliases) for s in db_state.values()) or 1
        alias_changes = sum(
            1 for a in applicable if a.action in ("add_alias", "delete_alias")
        )
        pct = (alias_changes / total_aliases) * 100
        if pct > self.max_pct_of_total_aliases_changed:
            return (
                f"% drift {pct:.2f}% > max_pct_of_total_aliases_changed="
                f"{self.max_pct_of_total_aliases_changed}%"
            )

        return None

    def _enforce_deadline(self, deadline: float, what: str) -> None:
        if time.perf_counter() > deadline:
            raise TimeoutError(
                f"{self.name}: max_runtime_seconds={self.max_runtime_seconds}s exceeded {what}"
            )

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    async def _execute(
        self,
        actions: list[Action],
        cycle_id: str,
        deadline: float,
    ) -> tuple[int, int, int, int]:
        """Per-SKU serial; across-SKU bounded by Semaphore(sc_concurrency)."""
        by_sku: dict[str, list[Action]] = {}
        for a in actions:
            by_sku.setdefault(a.sku, []).append(a)

        consecutive_failures = 0
        cb_threshold = self.circuit_breaker_consecutive_failures
        cb_lock = asyncio.Lock()
        cb_tripped = asyncio.Event()

        sem = asyncio.Semaphore(max(self.sc_concurrency, 1))
        executed = failed = blocked = skipped = 0
        counts_lock = asyncio.Lock()

        async def process_sku(sku: str, sku_actions: list[Action]) -> None:
            nonlocal executed, failed, blocked, skipped, consecutive_failures
            async with sem:
                primary_failed = False
                for action in sku_actions:
                    if cb_tripped.is_set():
                        async with counts_lock:
                            skipped += 1
                        continue
                    try:
                        self._enforce_deadline(deadline, f"executing on SKU {sku}")
                    except TimeoutError:
                        cb_tripped.set()
                        async with counts_lock:
                            skipped += 1
                        continue

                    if primary_failed and action.action == "delete_alias":
                        await self._log_action(cycle_id, action, "blocked",
                                               "upstream primary action failed")
                        async with counts_lock:
                            blocked += 1
                        continue

                    op_id = await self._log_start(cycle_id, action)
                    try:
                        await self._dispatch(action)
                    except SellercloudPermanentError as e:
                        await complete_operation(
                            op_id, "failed", 0, 1,
                            error_message=f"SellercloudPermanentError: {e}",
                        )
                        async with counts_lock:
                            failed += 1
                        if action.action in ("set_primary", "clear_primary"):
                            primary_failed = True
                        async with cb_lock:
                            consecutive_failures += 1
                            if consecutive_failures >= cb_threshold:
                                logger.error(
                                    f"{self.name}: circuit breaker tripped after "
                                    f"{consecutive_failures} consecutive failures"
                                )
                                cb_tripped.set()
                    except Exception as e:
                        await complete_operation(
                            op_id, "failed", 0, 1,
                            error_message=f"{type(e).__name__}: {e}",
                        )
                        async with counts_lock:
                            failed += 1
                        if action.action in ("set_primary", "clear_primary"):
                            primary_failed = True
                        async with cb_lock:
                            consecutive_failures += 1
                            if consecutive_failures >= cb_threshold:
                                cb_tripped.set()
                    else:
                        await complete_operation(op_id, "completed", 1, 1)
                        async with counts_lock:
                            executed += 1
                        async with cb_lock:
                            consecutive_failures = 0

        await asyncio.gather(
            *(process_sku(sku, acts) for sku, acts in by_sku.items()),
            return_exceptions=False,
        )
        return executed, failed, blocked, skipped

    async def _dispatch(self, action: Action) -> None:
        svc = sellercloud_internal_service
        if action.action == "add_alias":
            await svc.sync_add_alias(action.sku, action.value, is_primary=False)
        elif action.action == "delete_alias":
            await svc.sync_delete_alias(action.sku, action.value)
        elif action.action == "set_primary":
            await svc.sync_change_primary(action.sku, action.value, old_primary=None)
        elif action.action == "clear_primary":
            from services.sellercloud_service import sellercloud_service
            result = await sellercloud_service.update_product_upc(action.sku, "")
            if not result.get("success"):
                raise RuntimeError(f"update_product_upc failed: {result}")
        else:
            raise ValueError(f"Unknown action: {action.action}")

    async def _log_start(self, cycle_id: str, action: Action) -> int:
        return await create_operation(
            operation=action.action,
            target_sku=action.sku,
            value=action.value,
            source=SOURCE_TAG,
            metadata={
                "cycle_id": cycle_id,
                "action": action.action,
                "db_role": action.db_role,
                "sc_role": action.sc_role,
            },
        )

    async def _log_action(
        self, cycle_id: str, action: Action, status: str, message: str | None = None
    ) -> None:
        """Log a non-executed action (e.g. blocked) as a terminal entry."""
        op_id = await self._log_start(cycle_id, action)
        await complete_operation(op_id, status, 0, 1, error_message=message)

    # ------------------------------------------------------------------
    # Report rotation
    # ------------------------------------------------------------------

    def _rotate_reports(self) -> None:
        try:
            reports = sorted(
                Path(self.reports_dir).glob("daily_sellercloud_sync_*.xlsx"),
                key=lambda p: p.name,
                reverse=True,
            )
            for old in reports[self.reports_retention:]:
                old.unlink(missing_ok=True)
                rb = old.with_suffix(".rollback.csv")
                if rb.exists():
                    rb.unlink(missing_ok=True)
        except Exception:
            logger.exception(f"{self.name}: report rotation failed (non-fatal)")

    def _update_latest_symlink(self, xlsx_path: Path) -> None:
        link = Path(self.reports_dir) / "latest.xlsx"
        try:
            if link.is_symlink() or link.exists():
                link.unlink()
            os.symlink(xlsx_path.name, link)
        except OSError:
            # Symlink failures aren't fatal — Windows or odd filesystems.
            logger.warning(f"{self.name}: could not update latest.xlsx symlink (non-fatal)")


daily_sellercloud_sync_poller = DailySellercloudSyncPoller()
