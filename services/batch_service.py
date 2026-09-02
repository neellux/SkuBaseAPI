import logging
import asyncio
from typing import List, Optional
from datetime import datetime, timedelta, timezone
from fastapi import HTTPException
from tortoise import transactions
from tortoise.expressions import Q, Subquery
from models.db_models import Batch, Listing, AppSettings
from exceptions.batch_exceptions import BatchCreationError
from models.api_models import (
    CreateBatchRequest,
    CreateListingRequest,
    BatchResponse,
    BatchListResponse,
    UpdateBatchRequest,
)
from services.batch_value_service import compute_product_values
from services.listing_service import ListingService
from services.product_resolver import (
    SkuResolutionError,
    resolve_parents,
    resolve_parents_strict,
)
from services.template_service import TemplateService
from utils.load_app_data import app_users
from config import config

logger = logging.getLogger(__name__)

# A batch is "open" while it still has unsubmitted listings. status is maintained
# by the update_batch_counts() trigger on listings, so this is equivalent to
# submitted_listings < total_listings without needing to count anything.
OPEN_BATCH_STATUSES = ("new", "in_progress")

# Orderings the batch list offers. Both end in -id so offset pagination stays stable for
# batches sharing a created_at (they are created inside one transaction, so ties are
# common) - without it the same batch can appear on two pages or on neither.
#
# value_desc is the default. total_value is NOT NULL precisely so this ordering works:
# Postgres puts NULLs first on DESC, which would have parked every unvalued batch at the
# top of the list.
BATCH_SORTS = {
    "value_desc": ("-total_value", "-created_at", "-id"),
    "created_desc": ("-created_at", "-id"),
}
DEFAULT_BATCH_SORT = "value_desc"

# Above this many products, the AI web/tag search is queued for the poller
# instead of running inline with the aspects call.
#
# A search is ~18s and runs 3-wide, so 12 products is 4 waves, ~72s, on top of
# the SellerCloud prefill and the aspects model. That fits inside the 5-minute
# timeout PhotoManagementNew now allows with room to spare, while 23 (the
# largest batch in the last 60 days) would not leave much. Batches are median 6,
# so most run inline and only the unusually large ones wait for the poller.
INLINE_AI_SEARCH_MAX = 12


def _sorts_after(order_by: tuple[str, ...], row: Batch) -> Q:
    """Rows that sort strictly after `row` under `order_by`, every field of which is DESC.

    Lexicographic tuple comparison, expanded into ORs because Tortoise has no row-value
    constructor. For ("-a", "-b", "-c") this builds

        a < row.a  OR  (a = row.a AND b < row.b)  OR  (a = row.a AND b = row.b AND c < row.c)

    Derived from the ordering tuple rather than hand-written so the cursor cannot drift out
    of step with BATCH_SORTS: a walk whose comparison disagrees with its ORDER BY silently
    skips or repeats rows, which is the bug this shape exists to prevent.
    """
    names = [field.lstrip("-") for field in order_by]
    clauses = []
    for i, name in enumerate(names):
        terms = {earlier: getattr(row, earlier) for earlier in names[:i]}
        terms[f"{name}__lt"] = getattr(row, name)
        clauses.append(Q(**terms))
    return Q(*clauses, join_type=Q.OR)


class BatchService:

    @staticmethod
    async def create_batch(request: CreateBatchRequest, created_by: str) -> BatchResponse:
        settings = await AppSettings.first()
        max_batches = 50
        if settings and settings.app_variables:
            for var in settings.app_variables:
                if var.get("id") == "max_batches":
                    max_batches = var.get("value", 50)
                    break

        if len(request.product_ids) > max_batches:
            raise HTTPException(
                status_code=400,
                detail=f"Batch size ({len(request.product_ids)}) exceeds maximum allowed ({max_batches})",
            )

        if request.photography_batch_id is not None:
            existing = (
                await Batch.filter(photography_batch_id=request.photography_batch_id)
                .order_by("created_at")
                .first()
            )
            if existing:
                logger.info(
                    f"Idempotent create_batch: photography_batch_id={request.photography_batch_id} "
                    f"already mapped to batch_id={existing.id}; returning existing batch"
                )
                return await BatchService._to_response(existing, include_listings=True)

        # Resolve every SKU to its registered parent up front, in one round trip, before
        # the template pre-fetch and before the transaction opens. Parents are looked up
        # in the products DB, never derived by chopping the SKU: a chop invents a parent
        # that was never registered, which is how ESSX products (whose parent SKU itself
        # contains "/") silently produced blank listings for months. Failing here means a
        # bad batch is rejected in milliseconds instead of after a rollback.
        try:
            parents = await resolve_parents_strict(request.product_ids)
        except SkuResolutionError as e:
            logger.error(
                "Batch creation rejected: %d of %d products are not registered in the "
                "products database",
                len(e.unresolved),
                len(request.product_ids),
                extra={"unresolved_skus": e.unresolved},
            )
            raise BatchCreationError(
                f"{len(e.unresolved)} of {len(request.product_ids)} products not found: "
                + ", ".join(e.unresolved[:5])
                + (f" (+{len(e.unresolved) - 5} more)" if len(e.unresolved) > 5 else ""),
                [
                    {
                        "product_id": sku,
                        "error_type": "ProductNotFound",
                        "error_message": f"Product {sku} not found",
                    }
                    for sku in e.unresolved
                ],
            ) from e

        # Small batches get their AI search inline, alongside the aspects call,
        # so the suggestions are already there when the operator opens the first
        # listing. Large ones would push the request past the caller's timeout,
        # so they fall back to the queue.
        ai_search_inline = len(request.product_ids) <= INLINE_AI_SEARCH_MAX
        logger.info(
            f"Batch of {len(request.product_ids)}: AI search will run "
            f"{'inline with aspects' if ai_search_inline else 'via the queue'}"
        )

        logger.info("Pre-fetching default template for batch processing")
        sellercloud_template = await TemplateService.get_template_by_id("default")

        mapped_options = None
        if sellercloud_template and sellercloud_template.field_definitions:
            logger.info("Pre-loading mapped options for batch processing")
            mapped_options = await ListingService._load_mapped_options(
                sellercloud_template.field_definitions
            )

        try:
            async with transactions.in_transaction("default"):
                logger.info(
                    f"Starting batch creation transaction for {len(request.product_ids)} products"
                )

                batch = await Batch.create(
                    comment=request.comment or "",
                    assigned_to=request.assigned_to,
                    priority=request.priority,
                    created_by=created_by,
                    total_listings=len(request.product_ids),
                    photography_batch_id=request.photography_batch_id,
                )
                logger.debug(f"Created batch {batch.id} (not yet committed)")

                semaphore = asyncio.Semaphore(3)
                product_failures = []

                async def process_product(full_product_id: str):
                    async with semaphore:
                        try:
                            parent_product_id = parents[full_product_id]
                            existing_listing = await ListingService.get_draft_listing_by_product_id(
                                parent_product_id
                            )

                            if existing_listing:
                                existing_listing.batch = batch
                                await existing_listing.save()
                                logger.info(
                                    f"Linked existing listing {existing_listing.id} for product {full_product_id} to batch {batch.id}"
                                )
                                return existing_listing
                            else:
                                create_request = CreateListingRequest(
                                    product_id=parent_product_id,
                                    info_product_id=full_product_id,
                                    assigned_to=request.assigned_to,
                                    data={},
                                )

                                listing_response = await ListingService.create_listing(
                                    create_request,
                                    created_by,
                                    sellercloud_template=sellercloud_template,
                                    mapped_options=mapped_options,
                                    ai_search_inline=ai_search_inline,
                                )

                                listing = await Listing.get(id=listing_response.id)
                                listing.batch = batch
                                await listing.save()

                                logger.info(
                                    f"Created new listing for product {full_product_id} in batch {batch.id}"
                                )
                                return listing

                        except Exception as e:
                            logger.warning(
                                f"Error creating/linking listing for product {full_product_id}: {e}",
                                exc_info=True,
                            )
                            raise

                results = await asyncio.gather(
                    *[process_product(pid) for pid in request.product_ids], return_exceptions=True
                )

                successful_listings = []
                for i, result in enumerate(results):
                    product_id = request.product_ids[i]

                    if isinstance(result, Exception):
                        product_failures.append(
                            {
                                "product_id": product_id,
                                "error_type": type(result).__name__,
                                "error_message": str(result),
                            }
                        )
                    elif result is None:
                        product_failures.append(
                            {
                                "product_id": product_id,
                                "error_type": "ProcessingError",
                                "error_message": "Failed to create or link listing (returned None)",
                            }
                        )
                    else:
                        successful_listings.append(result)

                if product_failures:
                    error_msg = (
                        f"Failed to create batch: {len(product_failures)} of "
                        f"{len(request.product_ids)} products failed processing"
                    )
                    logger.error(
                        "Batch creation failed - rolling back",
                        extra={
                            "total_products": len(request.product_ids),
                            "succeeded_count": len(successful_listings),
                            "failed_count": len(product_failures),
                            "failure_details": product_failures,
                        },
                    )
                    raise BatchCreationError(error_msg, product_failures)

                batch.total_listings = len(successful_listings)
                await batch.save()

                logger.info(
                    f"Batch {batch.id} created successfully with {len(successful_listings)} listings"
                )

            await BatchService._snapshot_value(batch, sorted(set(parents.values())))
            await BatchService._enqueue_verification(successful_listings)

            return await BatchService._to_response(batch, include_listings=True)

        except BatchCreationError:
            raise
        except Exception as e:
            logger.error(f"Unexpected error creating batch: {e}", exc_info=True)
            raise

    @staticmethod
    async def _snapshot_value(batch: Batch, parent_skus: List[str]) -> None:
        """Take the batch's opening merchandise value. Never raises.

        The creation half of the story: from here on
        batch_value_service.BatchValueRefreshPoller re-reads this nightly for whichever of
        the batch's products are still unsubmitted, so this snapshot is what the batch is
        worth on day one, not for good.

        Called AFTER create_batch's transaction commits, not inside it. The photography
        service posts to /api/create_batch with no pre-validation, so a SellerCloud outage
        must not roll back a batch that is otherwise complete - it leaves
        value_computed_at NULL, which is exactly what backfill_batch_values.py picks up on
        its next run.

        Saves with update_fields because update_batch_counts() rewrites total_listings,
        submitted_listings and status on this row as each listing is inserted. A bare
        save() would write the stale in-memory copies straight back over the trigger's
        work.
        """
        try:
            total, breakdown = await compute_product_values(parent_skus)
        except Exception:  # noqa: BLE001
            logger.exception(
                f"Batch {batch.id} created, but its value snapshot failed; left uncomputed "
                f"for the nightly refresh (or backfill_batch_values.py) to pick up"
            )
            return

        batch.total_value = total
        batch.product_values = breakdown
        batch.value_computed_at = datetime.now(timezone.utc)
        await batch.save(
            update_fields=["total_value", "product_values", "value_computed_at"]
        )
        logger.info(f"Batch {batch.id} valued at {total} across {len(breakdown)} products")

    @staticmethod
    async def _enqueue_verification(listings: List[Listing]) -> None:
        """Queue AI search for the batch's listings. Never raises.

        Same contract and the same placement as _snapshot_value above, for the
        same reason: the photography service posts to /api/create_batch with no
        pre-validation behind a 120s timeout, and a batch that is otherwise
        complete must not roll back because a queue insert failed. A missed
        enqueue leaves ai_search NULL, which the operator fixes with the
        re-run button and backfill_ai_search.py sweeps up.

        Verification itself is NOT run here. A grounded call takes 5-47s, so
        adding a third inline model call to the two already running per listing
        would blow the caller's timeout. This is one INSERT, milliseconds.

        The relink path above (an existing draft joined to this batch, which
        skips AI) is enqueued too. The queue's SQL decides what actually runs:
        an already-verified draft is skipped, an unverified or failed one is
        picked up, which makes every batch a free backfill for what it touches.
        """
        if not listings:
            return
        try:
            from services import ai_search_queue

            queued = await ai_search_queue.enqueue_for_listings(
                [(listing.id, listing.info_product_id or listing.product_id) for listing in listings]
            )
            if queued:
                # Start now instead of waiting out the poller's interval. The
                # search itself takes 17-42s; without this it also waits up to
                # another 30s first, which is most of the gap between creating a
                # listing and finding its suggestions already there.
                #
                # Not run inline with the aspects call for the same reason it is
                # not run inline at all: at concurrency 3 a 23-listing batch
                # would add ~216s to a request PhotoManagementNew abandons at
                # 120s. Kicking is the cheap half of that win, without the risk.
                from services.ai_search_poller import ai_search_poller

                ai_search_poller.kick()
                logger.info(f"Queued AI search for {queued} listing(s)")
        except Exception:  # noqa: BLE001
            logger.exception(
                "Batch created, but queueing AI search failed; listings keep their "
                "existing ai_search and can be re-run from the listing view"
            )

    @staticmethod
    async def get_batch_by_id(
        batch_id: int, include_listings: bool = True
    ) -> Optional[BatchResponse]:
        try:
            batch = await Batch.get_or_none(id=batch_id)
            if not batch:
                return None

            return await BatchService._to_response(batch, include_listings=include_listings)

        except Exception as e:
            logger.error(f"Error fetching batch {batch_id}: {e}")
            raise

    @staticmethod
    async def get_all_batches(
        assigned_to: Optional[List[str]] = None,
        priority: Optional[List[str]] = None,
        status: Optional[List[str]] = None,
        date_from: Optional[object] = None,
        date_to: Optional[object] = None,
        search: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
        sort: str = DEFAULT_BATCH_SORT,
    ) -> tuple[List[BatchListResponse], int]:
        try:
            query = Batch.all()

            if assigned_to and len(assigned_to) > 0:
                query = query.filter(assigned_to__in=assigned_to)

            if priority and len(priority) > 0:
                query = query.filter(priority__in=priority)

            if status and len(status) > 0:
                expanded_statuses = []
                for s in status:
                    if s == "pending":
                        expanded_statuses.extend(OPEN_BATCH_STATUSES)
                    else:
                        expanded_statuses.append(s)
                expanded_statuses = list(dict.fromkeys(expanded_statuses))
                query = query.filter(status__in=expanded_statuses)

            if date_from:
                query = query.filter(created_at__gte=date_from)

            if date_to:
                date_to_end = date_to + timedelta(days=1)
                query = query.filter(created_at__lt=date_to_end)

            if search:
                raw_search = search.strip()
                # listings.product_id holds the parent, so a fully typed child SKU has to
                # be resolved before it can match - chopping it would invent a parent.
                # Resolve on the raw term (not the upper-cased one) so an exact hit takes
                # the exact-match path rather than the case-insensitive fallback. A miss
                # is normal here (partial input, a typo), so fall back to the substring
                # match and keep search forgiving.
                resolved = await resolve_parents([raw_search])
                if raw_search in resolved:
                    matching_batch_ids = Listing.filter(
                        product_id__iexact=resolved[raw_search]
                    ).values("batch_id")
                else:
                    matching_batch_ids = Listing.filter(
                        product_id__icontains=raw_search.upper()
                    ).values("batch_id")
                query = query.filter(id__in=Subquery(matching_batch_ids))

            total = await query.count()

            order_by = BATCH_SORTS.get(sort, BATCH_SORTS[DEFAULT_BATCH_SORT])
            batches = (
                await query.offset((page - 1) * page_size)
                .limit(page_size)
                .order_by(*order_by)
            )

            response_batches = []
            for batch in batches:
                response_batches.append(await BatchService._to_list_response(batch))

            return response_batches, total

        except Exception as e:
            logger.error(f"Error fetching batches: {e}")
            raise

    @staticmethod
    async def get_next_open_batch(batch_id: int) -> tuple[Optional[BatchResponse], bool]:
        """Next open batch after `batch_id`, in the same order the batch list uses.

        That order is BATCH_SORTS[DEFAULT_BATCH_SORT] - currently total_value DESC,
        so the arrow walks from the most valuable open batch down. Both the cursor
        and the ORDER BY come from that one tuple (see _sorts_after), so changing
        the list default moves this walk with it.

        The comparison is over the whole ordering tuple, not just its first field:
        total_value ties are common (35 production batches value at 0) and the
        created_at/id tail is what stops the walk from sticking or looping inside
        a tie.

        The batch view opens in a new tab (BatchList hands the id to window.open)
        and carries no sort state, so it cannot mirror a per-user sort choice -
        it follows the default.

        When the cursor is already past the last open batch the walk wraps to the
        first one, so the caller only dead-ends when there genuinely is no other
        open batch. Returns (batch, wrapped); a null batch means none is
        left, while an unknown batch_id raises 404.

        Carries the listings, i.e. the same payload as GET /listings/batch/detail,
        so the batch view can render the next batch the moment the arrow is used
        instead of showing a spinner while it fetches what the caller already
        asked for.
        """
        current = await Batch.get_or_none(id=batch_id)
        if not current:
            raise HTTPException(status_code=404, detail="Batch not found")

        try:
            open_batches = Batch.filter(status__in=OPEN_BATCH_STATUSES).exclude(id=batch_id)
            order_by = BATCH_SORTS[DEFAULT_BATCH_SORT]

            next_batch = (
                await open_batches.filter(_sorts_after(order_by, current))
                .order_by(*order_by)
                .first()
            )
            wrapped = False

            if not next_batch:
                next_batch = await open_batches.order_by(*order_by).first()
                wrapped = next_batch is not None

            if not next_batch:
                return None, False

            return (
                await BatchService._to_response(next_batch, include_listings=True),
                wrapped,
            )

        except Exception as e:
            logger.error(f"Error fetching next open batch after {batch_id}: {e}")
            raise

    @staticmethod
    async def update_batch(
        batch_id: int, update_data: "UpdateBatchRequest"
    ) -> Optional[BatchListResponse]:
        try:
            batch = await Batch.get_or_none(id=batch_id)
            if not batch:
                return None

            update_dict = update_data.model_dump(exclude_unset=True)
            for field, value in update_dict.items():
                setattr(batch, field, value)

            # Only the fields the request actually set. A bare save() writes back every
            # column from an instance read moments earlier, which means editing a comment
            # can revert the trigger's counts and undo the night's value refresh
            # (batch_value_service.BatchValueRefreshPoller) with numbers that were already
            # stale when this request started.
            await batch.save(update_fields=list(update_dict) + ["updated_at"])
            logger.info(f"Updated batch {batch_id}")

            return await BatchService._to_list_response(batch)

        except Exception as e:
            logger.error(f"Error updating batch {batch_id}: {e}")
            raise

    @staticmethod
    async def delete_batch(batch_id: int) -> bool:
        try:
            batch = await Batch.get_or_none(id=batch_id)
            if not batch:
                return False

            await batch.delete()
            logger.info(f"Deleted batch {batch_id}")
            return True

        except Exception as e:
            logger.error(f"Error deleting batch {batch_id}: {e}")
            raise

    @staticmethod
    async def get_filter_options() -> dict:
        try:
            users = [
                {"id": user_id, "name": user_data["name"]}
                for user_id, user_data in app_users.items()
                if any(
                    role.startswith(config["auth"]["short_name"] + "_")
                    for role in user_data["roles"]
                )
                and not any(role.endswith("_dev") for role in user_data["roles"])
            ]

            users.sort(key=lambda x: x["name"])

            return {
                "users": users,
                "priorities": ["low", "medium", "high"],
                "statuses": ["pending", "new", "in_progress", "completed"],
            }

        except Exception as e:
            logger.error(f"Error fetching filter options: {e}")
            raise

    @staticmethod
    async def _to_response(batch: Batch, include_listings: bool = True) -> BatchResponse:
        listings = []
        if include_listings:
            listing_models = await batch.listings.all()
            for listing in listing_models:
                listings.append(await ListingService._to_response(listing))

        return BatchResponse(
            id=batch.id,
            comment=batch.comment,
            assigned_to=batch.assigned_to,
            priority=batch.priority,
            status=batch.status,
            created_by=batch.created_by,
            total_listings=batch.total_listings,
            submitted_listings=batch.submitted_listings,
            progress_percentage=batch.progress_percentage,
            total_value=batch.total_value,
            value_computed_at=batch.value_computed_at,
            product_values=batch.product_values or {},
            created_at=batch.created_at,
            updated_at=batch.updated_at,
            listings=listings,
        )

    @staticmethod
    async def _to_list_response(batch: Batch) -> BatchListResponse:
        return BatchListResponse(
            id=batch.id,
            comment=batch.comment,
            assigned_to=batch.assigned_to,
            priority=batch.priority,
            status=batch.status,
            total_listings=batch.total_listings,
            submitted_listings=batch.submitted_listings,
            progress_percentage=batch.progress_percentage,
            total_value=batch.total_value,
            value_computed_at=batch.value_computed_at,
            product_values=batch.product_values or {},
            created_at=batch.created_at,
        )
