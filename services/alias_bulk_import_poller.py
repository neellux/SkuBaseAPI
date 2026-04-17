"""
Alias bulk import background poller.

Claims one pending job at a time from alias_bulk_import_jobs, processes each
item by dispatching to the appropriate _bulk_process_* method on ProductService,
and appends per-item results to the job row so the UI can poll for progress.

Follows the SellercloudSyncPoller pattern (BasePoller subclass).
"""

import logging
import traceback

from services import alias_bulk_import_job_service
from services.base_poller import BasePoller

logger = logging.getLogger(__name__)


class AliasBulkImportPoller(BasePoller):

    def __init__(self) -> None:
        super().__init__(config_section="alias_bulk_import_poller", name="AliasBulkImportPoller")

    async def _poll_cycle(self) -> None:
        job = await alias_bulk_import_job_service.claim_next_job()
        if not job:
            return

        job_id = job["id"]
        items = job.get("items") or []
        logger.info(f"{self.name}: claimed job id={job_id} with {len(items)} items")

        try:
            await self._process_job(job_id, items)
        except Exception as e:
            logger.exception(f"{self.name}: failed to process job id={job_id}")
            await alias_bulk_import_job_service.mark_failed(
                job_id, f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            )
            return

        await alias_bulk_import_job_service.mark_completed(job_id)
        logger.info(f"{self.name}: completed job id={job_id}")

    async def _process_job(self, job_id: int, items: list[dict]) -> None:
        # Local import to avoid circular dependency (product_service imports nothing from here)
        from services.product_service import ProductService

        conn = await ProductService._get_connection()

        # Group items by classification (same ordering as synchronous processing)
        swap_items = [i for i in items if (i.get("classification") or "").startswith("swap_")]
        noop_items = [i for i in items if i.get("classification") == "noop"]
        delete_items = [i for i in items if (i.get("classification") or "").startswith("delete_")]
        other_items = [
            i for i in items
            if i not in swap_items and i not in noop_items and i not in delete_items
        ]

        # Process in order: noops → swaps → adds/promotes → deletes
        processing_order = noop_items + swap_items + other_items + delete_items

        for item in processing_order:
            if self._shutdown_event.is_set():
                logger.info(f"{self.name}: shutdown requested mid-job id={job_id}, stopping")
                break

            try:
                result = await self._dispatch_item(conn, item)
            except Exception as e:
                logger.exception(f"{self.name}: unexpected error on item {item.get('row')}")
                result = {
                    "row": item.get("row"),
                    "sku": item.get("sku"),
                    "value": item.get("value"),
                    "action": item.get("action"),
                    "classification": item.get("classification"),
                    "success": False,
                    "error": f"Unexpected error: {e}",
                }

            await alias_bulk_import_job_service.append_result(
                job_id, result, success=bool(result.get("success"))
            )

    async def _dispatch_item(self, conn, item: dict) -> dict:
        """Dispatch a single item to the appropriate ProductService bulk processor."""
        from services.product_service import ProductService

        classification = item.get("classification") or ""

        if classification == "noop":
            return {
                "row": item["row"], "sku": item["sku"], "value": item["value"],
                "action": item["action"], "classification": "noop", "success": True,
            }

        if classification.startswith("swap_"):
            return await ProductService._bulk_process_swap(conn, item)

        if classification.startswith("delete_"):
            return await ProductService._bulk_process_delete(conn, item)

        # Add/promote flows dispatch on action
        if item["action"] == "Primary":
            return await ProductService._bulk_process_primary(conn, item)
        if item["action"] == "Secondary":
            return await ProductService._bulk_process_secondary(conn, item)
        if item["action"] == "Keyword":
            return await ProductService._bulk_process_keyword(conn, item)

        return {
            "row": item.get("row"), "sku": item.get("sku"), "value": item.get("value"),
            "action": item.get("action"), "classification": classification,
            "success": False, "error": f"Unknown action: {item.get('action')}",
        }


alias_bulk_import_poller = AliasBulkImportPoller()
