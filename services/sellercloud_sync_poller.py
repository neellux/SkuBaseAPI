import logging
import traceback

from config import config
from services import sellercloud_sync_queue
from services.base_poller import BasePoller
from services.sellercloud_internal_service import sellercloud_internal_service

logger = logging.getLogger(__name__)


class SellercloudSyncPoller(BasePoller):

    def __init__(self) -> None:
        super().__init__(config_section="sellercloud_sync_poller", name="SellercloudSyncPoller")
        cfg = config.get("sellercloud_sync_poller", {})
        self.batch_size: int = cfg.get("batch_size", 100)

    async def _poll_cycle(self) -> None:
        records = await sellercloud_sync_queue.claim_batch(self.batch_size)
        if not records:
            return

        logger.info(f"{self.name}: processing {len(records)} pending alias syncs")

        for rec in records:
            if self._shutdown_event.is_set():
                logger.info(f"{self.name}: shutdown requested mid-batch, stopping")
                break
            await self._process_one(rec)

    async def _process_one(self, rec: dict) -> None:
        sync_id = rec["id"]
        sync_type = rec["type"]
        sku = rec["sku"]
        value = rec["value"]

        try:
            if sync_type == "add_primary_upc":
                await sellercloud_internal_service.sync_add_alias(sku, value, is_primary=True)
            elif sync_type in ("add_secondary_upc", "add_keyword"):
                await sellercloud_internal_service.sync_add_alias(sku, value, is_primary=False)
            elif sync_type == "change_primary_upc":
                await sellercloud_internal_service.sync_change_primary(
                    sku, value, rec.get("old_primary_upc")
                )
            elif sync_type in ("delete_upc", "delete_keyword"):
                await sellercloud_internal_service.sync_delete_alias(sku, value)
            else:
                raise ValueError(f"Unknown sync type: {sync_type}")

            await sellercloud_sync_queue.mark_synced(sync_id)
            logger.info(f"{self.name}: synced id={sync_id} {sync_type} {sku}/{value}")

        except Exception as e:
            logger.exception(
                f"{self.name}: failed sync id={sync_id} {sync_type} {sku}/{value}"
            )
            await sellercloud_sync_queue.mark_failed(
                sync_id,
                f"{type(e).__name__}: {e}\n{traceback.format_exc()}",
                (rec.get("retry_count") or 0) + 1,
            )


sellercloud_sync_poller = SellercloudSyncPoller()
