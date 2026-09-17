"""One attempt at one batch_generation_jobs row: relink a draft, copy a listing that already
went out, or generate a listing.

Called only by GenerationPoller, which owns claiming, timeouts, retries and failure
classification. Everything here either finishes the job (inserting, copying or relinking the
listing and completing the row in one short transaction) or raises.

Model and SellerCloud calls happen before any transaction opens. Holding a pooled connection
through a 5-18s model call, three per batch across many batches, would exhaust the pool.
"""

import asyncio
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import orjson
from tortoise import connections
from tortoise.transactions import in_transaction

from exceptions.batch_generation_exceptions import (
    GENERIC,
    IMAGE_STORE_UNAVAILABLE,
    NO_IMAGES,
    AIGenerationError,
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


# One read decides the copy path: where the batch came from, the product's newest listing that
# went out, and whether eBay is required for it.
#
# "Went out" is submitted OR any successful submission row. A resubmit adds a pending row and
# recompute_listing_submitted unflags the listing until that row lands (and for good if it
# fails on a platform without manual_fallback), so `submitted` alone would skip the listing
# an operator is resubmitting and copy an older one.
#
# listing_required_platforms is the rule completion uses: enabled platforms minus brand, type
# and company exclusions. A copy has the source's data and company, so the answer is the
# copy's too, and a database error raises instead of quietly skipping eBay.
COPY_LOOKUP_SQL = """
SELECT b.photography_batch_id IS NOT NULL AS from_photography,
       src.id AS source_id,
       src.data,
       src.ai_response,
       'ebay' = ANY(listing_required_platforms(src.id)) AS ebay_required
  FROM batches b
  LEFT JOIN LATERAL (
      SELECT l.id, l.data, l.ai_response
        FROM listings l
       WHERE l.product_id = $2
         AND (l.submitted OR EXISTS (
                SELECT 1 FROM listing_submissions s
                 WHERE s.listing_id = l.id AND s.status = 'success'))
       ORDER BY l.created_at DESC
       LIMIT 1
  ) src ON true
 WHERE b.id = $1
"""

# The copy itself: one statement, so the row is copied as it stands inside the transaction
# and no JSONB column round-trips through Python. EVERY column a copy carries is listed
# here: a column added to the Listing model must be added here too, or copies leave it at
# its default.
#
# Not copied, on purpose: submitted, submitted_at, submitted_by and error (the copy has not
# been submitted), required_platforms (NULL, so the platforms the product is not on yet are
# required; recompute_listing_submitted may freeze it just after), batch_id, created_by and
# assigned_to (this batch's).
#
# data takes the eBay answers, which only ever name keys that were empty in the source, and
# ai_response gains them so the form shows them as AI suggestions. original_data is carried
# as it is: a source without a creation baseline gives a copy without one, rather than an
# operator-edited state posing as it. The four flag columns move together, so
# listings_flag_consistent holds. ai_search is carried, so a finished verdict is reused.
#
# $1 new id, $2 job info_product_id, $3 assignee, $4 eBay answers (jsonb object),
# $5 upload_status or NULL to inherit, $6 created_by, $7 batch_id, $8 source id.
COPY_INSERT_SQL = """
INSERT INTO listings (
    id, product_id, info_product_id, company_code, assigned_to,
    data, original_data, ai_response, ai_description, original_description,
    original_title, title_auto_update, upload_status,
    flagged, flag_note, flagged_by, flagged_at,
    ai_search, copied_from_id, created_by, batch_id)
SELECT $1::uuid, s.product_id, COALESCE($2, s.info_product_id), s.company_code, $3,
       s.data || $4::jsonb,
       s.original_data,
       CASE WHEN $4::jsonb = '{}'::jsonb THEN s.ai_response
            WHEN jsonb_typeof(s.ai_response) = 'object' THEN s.ai_response || $4::jsonb
            ELSE $4::jsonb END,
       s.ai_description, s.original_description,
       s.original_title, s.title_auto_update, COALESCE($5, s.upload_status),
       s.flagged, s.flag_note, s.flagged_by, s.flagged_at,
       s.ai_search, s.id, $6, $7
  FROM listings s
 WHERE s.id = $8::uuid
RETURNING id, product_id, info_product_id
"""

# The photo verdict mapped to upload_status. "leave" maps to nothing, so the copy keeps its
# source's value: that is the case the photo poller leaves alone too.
_UPLOAD_STATUS_FOR_VERDICT = {"ready": "uploaded", "shot": "pending"}


async def _missing_ebay_aspects(
    data: Dict[str, Any], source_ai_response: Any
) -> Tuple[List[Any], Dict[str, List[Any]]]:
    """The AI-tagged eBay aspects this listing has no value for, as AI fields.

    Filled means what the submit path would send (EbayService.aspect_value_from_listing):
    the listing's value, else the category default, else the aspect default. Two more are
    never asked for: a `mapped_field` aspect, whose value submit reads from the mapped field
    (an answer under the aspect's name would never be read), and an aspect the source's AI
    already answered, which an operator has since cleared on purpose.
    """
    from services.ebay_aspect_service import ebay_aspect_service
    from services.ebay_service import EbayService
    from services.listing_service import ai_fields_from_rows

    product_type = data.get("product_type")
    category_id = await ebay_aspect_service.resolve_listing_category(
        product_type, data.get("ebay_category_id")
    )
    if not category_id:
        return [], {}
    rows = await ebay_aspect_service.get_ai_aspects_for_category(product_type, category_id)
    if not rows:
        return [], {}
    detail = await ebay_aspect_service.get_category_aspects(category_id) or {}
    aspects = {a["aspect_name"]: a for a in detail.get("aspects", [])}
    answered = source_ai_response if isinstance(source_ai_response, dict) else {}

    missing = []
    for row in rows:
        name = row["aspect_name"]
        aspect = aspects.get(name)
        if aspect is None or name in answered:
            continue
        if (aspect.get("settings") or {}).get("source") == "mapped_field":
            continue
        if EbayService.aspect_value_from_listing(aspect, data) in EbayService.EMPTY_VALUES:
            missing.append(row)
    return ai_fields_from_rows(missing)


async def _ask_ai_for_ebay_aspects(
    product_id: str, data: Dict[str, Any], fields: List[Any], options: Dict[str, List[Any]]
) -> Dict[str, Any]:
    """One aspects-only model call for the missing aspects; the non-empty answers asked for.

    No description field is passed, so no caption call runs. product_data carries only what
    the prompt and the image lookup read (the image lookup reads GCS and the photography
    database, not SellerCloud), and leaves out empty values, which would reach the prompt as
    the text "None".
    """
    from services.ai_service import AIService
    from services.ebay_service import EbayService

    product_data = {
        key: value
        for key, value in {
            "ID": product_id,
            "ProductName": data.get("title"),
            "ProductType": data.get("product_type"),
        }.items()
        if value not in EbayService.EMPTY_VALUES
    }
    content = await AIService.generate_ai_content(
        product_data, fields, options, require_ai=True
    )
    aspects = (content or {}).get("aspects") or {}
    if not isinstance(aspects, dict):
        # The type only: model text in the detail could match a rate-limit marker ("429")
        # and pause every job.
        raise AIGenerationError(
            f"eBay aspects answer was {type(aspects).__name__}, not an object"
        )
    asked = {field.name for field in fields}
    return {
        name: value
        for name, value in aspects.items()
        if name in asked and value not in EbayService.EMPTY_VALUES
    }


async def _copy_upload_status(product_id: str) -> Optional[str]:
    """upload_status for a copy, from the photo poller's own verdict; None to inherit.

    A read failure is the image store not answering, so it retries under
    IMAGE_STORE_UNAVAILABLE, which also trips the breaker: a photography outage pauses the
    queue instead of burning every copy job's attempts in a few minutes.
    """
    from services.photo_upload_poller import classify_photo_row, fetch_photo_rows

    try:
        rows = await fetch_photo_rows(connections.get("photography_db"), [product_id])
    except Exception as e:  # noqa: BLE001
        raise TransientGenerationError(
            IMAGE_STORE_UNAVAILABLE, detail=f"Photography lookup for {product_id} failed: {e}"
        ) from e
    if not rows:
        return None
    row = rows[0]
    verdict = classify_photo_row(row["image_source"], row["ever_edited"], row["edited_for_batch"])
    return _UPLOAD_STATUS_FOR_VERDICT.get(verdict)


async def _copy_submitted(job: ClaimedJob) -> Optional[str]:
    """Copy the product's newest listing that went out into this batch, with no SellerCloud call.

    Returns the new listing's id, or None to fall through to generation: in a batch from
    photography (a re-shoot should be described from its new photographs) or when the
    product has no listing that went out.

    The only model call is for eBay aspects the copy is missing, and only when eBay is
    required for the product. A copy that presence already covers on every required platform
    is completed in the same transaction, so it is never left open to flip on its own later.
    """
    from services import ai_search_queue
    from services.ebay_aspect_service import decode_json

    rows = await connections.get("default").execute_query_dict(
        COPY_LOOKUP_SQL, [job.batch_id, job.product_id]
    )
    lookup = rows[0] if rows else None
    if lookup is None or lookup["from_photography"] or lookup["source_id"] is None:
        return None
    source_id = str(lookup["source_id"])
    # Raw reads return JSONB as text.
    data = decode_json(lookup["data"], {}) or {}
    source_ai_response = decode_json(lookup["ai_response"], None)

    await require_primary_image(job.product_id)

    asked: List[str] = []
    answers: Dict[str, Any] = {}
    if lookup["ebay_required"]:
        fields, options = await _missing_ebay_aspects(data, source_ai_response)
        asked = [field.name for field in fields]
        if fields:
            answers = await _ask_ai_for_ebay_aspects(job.product_id, data, fields, options)

    # Read last, just before the transaction, so the verdict is as fresh as it can be.
    upload_status = await _copy_upload_status(job.product_id)

    new_id = str(uuid.uuid4())
    async with in_transaction("default") as conn:
        lease = await queue.lock_lease(conn, job)
        inserted = await conn.execute_query_dict(
            COPY_INSERT_SQL,
            [
                new_id,
                job.info_product_id,
                # The batch's assignee now, not when the job was queued.
                lease.get("assigned_to"),
                orjson.dumps(answers).decode(),
                upload_status,
                job.created_by,
                job.batch_id,
                source_id,
            ],
        )
        if not inserted:
            # Deleted since the lookup. The next attempt decides again.
            raise TransientGenerationError(
                GENERIC,
                detail=f"Copy source {source_id} for {job.product_id} was deleted",
                trips_breaker=False,
            )
        copy = inserted[0]
        # Nothing recomputes completion on an INSERT. Without this, a copy that presence
        # already covers everywhere would sit open until some later presence write for the
        # product flipped it.
        await conn.execute_query("SELECT recompute_listing_submitted($1::uuid)", [new_id])
        await ai_search_queue.enqueue_for_listings(
            [(new_id, copy["info_product_id"] or copy["product_id"])], conn=conn
        )
        await queue.complete(conn, job, new_id, outcome="copied")

    logger.info(
        f"Job {job.id}: copied listing {source_id} into {new_id} for {job.product_id} "
        f"(batch {job.batch_id}); eBay asked={asked} answered={sorted(answers)}"
    )
    return new_id


async def run_job(job: ClaimedJob) -> Union[Listing, str]:
    """Finish one job or raise. LeaseLost means another attempt owns it now.

    Relink an unbatched draft, else copy a listing that already went out, else generate.
    The poller does not read the return value.
    """
    from services import ai_search_queue
    from services.listing_service import ListingService

    relinked = await _relink_draft(job)
    if relinked is not None:
        _kick_ai_search()
        return relinked

    copied = await _copy_submitted(job)
    if copied is not None:
        _kick_ai_search()
        return copied

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
