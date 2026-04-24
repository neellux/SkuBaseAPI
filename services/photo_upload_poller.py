import logging

from config import config
from services.base_poller import BasePoller
from tortoise import connections

logger = logging.getLogger(__name__)

ELIGIBLE_SOURCES = {"upload", "manual"}


class PhotoUploadPoller(BasePoller):

    def __init__(self) -> None:
        super().__init__(config_section="photo_upload_poller", name="PhotoUploadPoller")
        cfg = config.get("photo_upload_poller", {})
        self.batch_size: int = cfg.get("batch_size", 100)

    async def _poll_cycle(self) -> None:
        photo_conn = connections.get("photography_db")
        rows = await photo_conn.execute_query_dict(
            """
            SELECT batch_id, product_id, image_source
            FROM productimages
            ORDER BY updated_at DESC
            LIMIT $1
            """,
            [self.batch_size],
        )

        pairs: set[tuple[int, str]] = set()
        for row in rows:
            batch_id = row.get("batch_id")
            product_id = row.get("product_id")
            source = row.get("image_source")
            if batch_id is None or not product_id or source not in ELIGIBLE_SOURCES:
                continue
            pairs.add((batch_id, product_id))

        if not pairs:
            return

        batch_ids = [p[0] for p in pairs]
        product_ids = [p[1] for p in pairs]

        default_conn = connections.get("default")
        affected, _ = await default_conn.execute_query(
            """
            UPDATE listings AS l
            SET upload_status = 'uploaded', updated_at = NOW()
            FROM batches AS b,
                 unnest($1::int[], $2::text[]) AS p(photography_batch_id, product_id)
            WHERE l.batch_id = b.id
              AND b.photography_batch_id = p.photography_batch_id
              AND l.product_id = p.product_id
              AND l.upload_status = 'pending'
            """,
            [batch_ids, product_ids],
        )

        if affected:
            logger.info(f"{self.name}: flipped {affected} listings to uploaded")


photo_upload_poller = PhotoUploadPoller()
