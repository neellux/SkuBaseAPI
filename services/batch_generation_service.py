"""One attempt at one batch_generation_jobs row: relink a draft, or generate a listing.

Called only by GenerationPoller, which owns claiming, timeouts, retries and failure
classification. Everything here either finishes the job (inserting or relinking the listing
and completing the row in one short transaction) or raises.

Model and SellerCloud calls happen before any transaction opens. Holding a pooled connection
through a 5-18s model call, three per batch across many batches, would exhaust the pool.
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any, Optional, Tuple

from tortoise.transactions import in_transaction

from exceptions.batch_generation_exceptions import (
    IMAGE_STORE_UNAVAILABLE,
    NO_IMAGES,
    PermanentGenerationError,
    TransientGenerationError,
)
from models.api_models import CreateListingRequest
from models.db_models import Listing
from services import generation_queue as queue
from services.generation_queue import ClaimedJob
from utils import gcs_probe

logger = logging.getLogger(__name__)

# The default template and its mapped options are the same for every product. Batch
# creation used to fetch them once per batch; background jobs share one copy for this long.
TEMPLATE_CACHE_SECONDS = 300

_template_lock = asyncio.Lock()
_template_cache: dict = {"at": 0.0, "template": None, "mapped_options": None}


async def cached_template_and_options() -> Tuple[Any, Optional[dict]]:
    # Deferred: listing_service imports this module's neighbours, and the template service
    # is only needed once per cache period.
    from services.listing_service import ListingService
    from services.template_service import TemplateService

    async with _template_lock:
        now = time.monotonic()
        cached = _template_cache["template"]
        if cached is not None and now - _template_cache["at"] < TEMPLATE_CACHE_SECONDS:
            return cached, _template_cache["mapped_options"]

        template = await TemplateService.get_template_by_id("default")
        mapped_options = None
        if template and template.field_definitions:
            mapped_options = await ListingService._load_mapped_options(
                template.field_definitions
            )
        # A missing template is not cached: build_listing_fields raises on it with
        # require_ai, and the retry should look again.
        if template is not None:
            _template_cache.update(at=now, template=template, mapped_options=mapped_options)
        return template, mapped_options


async def require_primary_image(parent_sku: str) -> None:
    """Raise unless the product's primary photograph exists.

    A 404 is proof, so it fails the job for good. Anything else means the store did not
    answer, and the attempt is retried rather than blaming the product.
    """
    result = await gcs_probe.probe_one(parent_sku)
    if result is gcs_probe.Probe.ABSENT:
        raise PermanentGenerationError(
            NO_IMAGES, detail=f"{gcs_probe.PRIMARY_IMAGE} returned 404 for {parent_sku}"
        )
    if result is gcs_probe.Probe.UNKNOWN:
        raise TransientGenerationError(
            IMAGE_STORE_UNAVAILABLE, detail=f"Image probe for {parent_sku} got no answer"
        )


def _kick_ai_search() -> None:
    try:
        from services.ai_search_poller import ai_search_poller

        ai_search_poller.kick()
    except Exception:  # noqa: BLE001
        logger.exception("Could not kick the AI search poller; its interval will pick the job up")


async def _relink_draft(job: ClaimedJob) -> Optional[Listing]:
    """Join an existing unbatched draft for the parent to this batch, with no AI call.

    The same draft lookup batch creation used to do up front. Returns None when there is no
    draft, or when another batch took it first, in which case the caller generates.
    """
    from services import ai_search_queue
    from services.listing_service import ListingService

    draft = await ListingService.get_draft_listing_by_product_id(job.product_id)
    if not draft:
        return None

    async with in_transaction("default") as conn:
        await queue.lock_lease(conn, job)
        moved = (
            await Listing.filter(id=draft.id, batch_id=None, submitted=False)
            .using_db(conn)
            .update(batch_id=job.batch_id, updated_at=datetime.now(timezone.utc))
        )
        if not moved:
            return None
        # Inside the transaction, so the listing never appears ready without its AI search
        # job. The queue's own WHERE skips a draft that is already verified.
        await ai_search_queue.enqueue_for_listings(
            [(draft.id, draft.info_product_id or draft.product_id)], conn=conn
        )
        await queue.complete(conn, job, str(draft.id), outcome="relinked")

    logger.info(f"Job {job.id}: relinked draft {draft.id} for {job.product_id} to batch {job.batch_id}")
    return draft


async def run_job(job: ClaimedJob) -> Listing:
    """Finish one job or raise. LeaseLost means another attempt owns it now."""
    from services import ai_search_queue
    from services.listing_service import ListingService

    relinked = await _relink_draft(job)
    if relinked is not None:
        _kick_ai_search()
        return relinked

    await require_primary_image(job.product_id)

    template, mapped_options = await cached_template_and_options()
    fields = await ListingService.build_listing_fields(
        CreateListingRequest(
            product_id=job.product_id,
            info_product_id=job.info_product_id,
            data={},
        ),
        sellercloud_template=template,
        mapped_options=mapped_options,
        ai_search_inline=False,
        require_ai=True,
    )

    async with in_transaction("default") as conn:
        lease = await queue.lock_lease(conn, job)
        listing = await ListingService.persist_listing(
            fields,
            created_by=job.created_by,
            batch_id=job.batch_id,
            # The batch's assignee now, not when the job was queued.
            assigned_to=lease.get("assigned_to"),
            using_db=conn,
        )
        await ai_search_queue.enqueue_for_listings(
            [(listing.id, listing.info_product_id or listing.product_id)], conn=conn
        )
        await queue.complete(conn, job, str(listing.id), outcome="generated")

    _kick_ai_search()
    return listing
