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

        # The newest row per candidate, across ALL of its rows, plus whether an edit has
        # been delivered for THAT ROW'S BATCH.
        #
        # The second half is not optional. PhotoManagementNew writes washtags onto the
        # batch_creation row (gcs_product_uploader.py: it looks up the batch_creation row
        # and save()s washtag_data onto it), and save() moves updated_at. So a washtag
        # upload that lands after the edited photos makes batch_creation the newest row
        # for a product whose photography is finished. Measured on production: 11 of the
        # 198 products whose newest row is batch_creation are in exactly that state, and
        # resetting them would gate 12 listings whose images are fine.
        #
        # Asking "is there an upload/manual row for the same batch" separates the two:
        # a genuine re-shoot opens a NEW batch that has no edit yet, while a washtag write
        # bumps a batch that already has one. IS NOT DISTINCT FROM, not =, because manual
        # rows carry batch_id NULL and NULL = NULL is never true.
        verdicts = await photo_conn.execute_query_dict(
            """
            WITH newest AS (
                SELECT DISTINCT ON (product_id) product_id, image_source, batch_id
                FROM productimages
                WHERE product_id = ANY($1::text[])
                ORDER BY product_id, updated_at DESC
            )
            SELECT n.product_id, n.image_source,
                   EXISTS (
                       SELECT 1 FROM productimages e
                       WHERE e.product_id = n.product_id
                         AND e.image_source = ANY($2::text[])
                         AND e.batch_id IS NOT DISTINCT FROM n.batch_id
                   ) AS edited_for_batch,
                   EXISTS (
                       SELECT 1 FROM productimages e
                       WHERE e.product_id = n.product_id
                         AND e.image_source = ANY($2::text[])
                   ) AS ever_edited
            FROM newest n
            """,
            [product_ids, list(READY_SOURCES)],
        )

        ready, shot = [], []
        for v in verdicts:
            if v["image_source"] in READY_SOURCES:
                ready.append(v["product_id"])
            elif v["image_source"] == SHOT_SOURCE:
                # Reset ONLY the latch case this poller exists to fix: an edit was
                # delivered at some point, and a LATER shoot has not been edited yet.
                #
                # Both guards are load-bearing, and each was added after production data
                # contradicted the simpler rule:
                #
                # edited_for_batch - a washtag upload save()s the batch_creation row and
                #   moves its updated_at, so "newest row is batch_creation" is true for 11
                #   products whose photography is finished.
                #
                # ever_edited - a product can reach GCS with edited images and have NO
                #   upload/manual row at all (AMR-MJNS-0136, AMR-WBTM-0041, AMR-WOTW-0036
                #   are batch_creation-only yet hold 3000x3000 edits). Those were never
                #   latched, so resetting them would gate a product whose photos are fine
                #   and that nothing will ever un-gate.
                if v["ever_edited"] and not v["edited_for_batch"]:
                    shot.append(v["product_id"])
                # Otherwise LEAVE IT ALONE - deliberately neither bucket.
                #
                # Not `ready`: a product whose only rows are batch_creation has never had
                # an edit delivered, and releasing it would open the images gate on raw
                # studio captures. PRP-MTPS-0042 is exactly that shape, and an earlier
                # draft of this function would have flipped it to 'uploaded'.
                #
                # Not `shot`: it was never latched, so there is nothing to undo, and
                # gating it would strand a listing nothing will ever un-gate.

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
