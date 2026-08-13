"""Keeps listings.upload_status in step with where a product sits in the photo cycle.

The photography app writes one productimages row per (product, batch, source), so a
product accumulates rows and the NEWEST one says where it is in the cycle:

    batch_creation  shot, the editor has not delivered for this shoot  -> pending
    upload/manual   edited files delivered                             -> uploaded

upload_status gates submissions: listing_routes parks a submission QUEUED while a
platform has requires_images and the listing is still pending, and submission_poller
only releases queued rows whose listing has reached uploaded.

WHY THIS READS THE NEWEST ROW RATHER THAN LOOKING FOR AN 'upload' ROW

The original version flipped pending -> uploaded whenever it saw an upload/manual row
and never wrote the flag back, which made it a ONE-WAY LATCH. A product re-shot after
its first edit kept upload_status = 'uploaded' forever, so the gate was open for a shoot
nobody had edited yet, and a submit or resubmit published the raw capture. That is how
raw studio shots (hands, camera, studio floor) reached the 1nventory storefront.

Reading the newest row instead makes the flag track the cycle in both directions. It
also fixes a subtler bug in the old query: it took its rows from a LIMIT window, so a
product whose stale upload row happened to fall inside the window was flipped to
uploaded even when a newer batch_creation row existed. The window is now used only to
find candidates; the verdict comes from a query over all of that product's rows.

WHY THIS IS SCOPED PER PRODUCT, NOT PER BATCH

The old UPDATE joined batches on photography_batch_id, so it only touched listings whose
SkuBase batch was linked to the photography batch that changed. GCS blobs live under
/{product_id}/ and every listing for a product resolves to the same images, so a re-shoot
in a NEW photography batch has to reset the OLDER listing too - and under the batch join
it never would, because the new rows carry the new batch id.

Both directions use the same scope deliberately. An asymmetric fix would reset a listing
the forward flip could never reach again and strand it in queued forever.
"""
import logging

from config import config
from services.base_poller import BasePoller
from tortoise import connections

logger = logging.getLogger(__name__)

# Sources that publish EDITED images. Mirrors image_service.IMAGE_SOURCES; 'manual' counts
# because a manual upload is a deliberate human delivery, not a camera dump.
READY_SOURCES = {"upload", "manual"}

# The source the photography app writes when a batch is shot. Only this resets the flag:
# an unrecognised source leaves upload_status alone rather than holding a listing on a
# row nobody understands.
SHOT_SOURCE = "batch_creation"


class PhotoUploadPoller(BasePoller):

    def __init__(self) -> None:
        super().__init__(config_section="photo_upload_poller", name="PhotoUploadPoller")
        cfg = config.get("photo_upload_poller", {})
        self.batch_size: int = cfg.get("batch_size", 100)

    async def _poll_cycle(self) -> None:
        photo_conn = connections.get("photography_db")

        # Candidates only. Any product whose photo state changed recently is worth
        # re-evaluating; what its state actually IS comes from the next query.
        recent = await photo_conn.execute_query_dict(
            """
            SELECT product_id
            FROM productimages
            ORDER BY updated_at DESC
            LIMIT $1
            """,
            [self.batch_size],
        )
        product_ids = list({r["product_id"] for r in recent if r.get("product_id")})
        if not product_ids:
            return

        # The newest row per candidate, across ALL of its rows.
        verdicts = await photo_conn.execute_query_dict(
            """
            SELECT DISTINCT ON (product_id) product_id, image_source
            FROM productimages
            WHERE product_id = ANY($1::text[])
            ORDER BY product_id, updated_at DESC
            """,
            [product_ids],
        )

        ready = [v["product_id"] for v in verdicts if v["image_source"] in READY_SOURCES]
        shot = [v["product_id"] for v in verdicts if v["image_source"] == SHOT_SOURCE]

        default_conn = connections.get("default")

        if ready:
            affected, _ = await default_conn.execute_query(
                """
                UPDATE listings
                SET upload_status = 'uploaded', updated_at = NOW()
                WHERE product_id = ANY($1::text[])
                  AND upload_status = 'pending'
                """,
                [ready],
            )
            if affected:
                logger.info(f"{self.name}: flipped {affected} listings to uploaded")

        if shot:
            # Reset. Safe for work already done: upload_status gates the CREATION of a
            # submission, so an existing success row is untouched and only the next
            # submit is held.
            affected, _ = await default_conn.execute_query(
                """
                UPDATE listings
                SET upload_status = 'pending', updated_at = NOW()
                WHERE product_id = ANY($1::text[])
                  AND upload_status = 'uploaded'
                """,
                [shot],
            )
            if affected:
                logger.info(
                    f"{self.name}: reset {affected} listings to pending "
                    f"(re-shot, awaiting edited images)"
                )


photo_upload_poller = PhotoUploadPoller()
