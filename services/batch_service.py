import logging
import asyncio
from typing import List, Optional
from datetime import datetime, timedelta
from fastapi import HTTPException
from tortoise import transactions
from models.db_models import Batch, Listing, AppSettings
from exceptions.batch_exceptions import BatchCreationError
from models.api_models import (
    CreateBatchRequest,
    CreateListingRequest,
    BatchResponse,
    BatchListResponse,
    UpdateBatchRequest,
)
from services.listing_service import ListingService
from services.template_service import TemplateService
from utils.product_utils import get_parent_product_id
from utils.load_app_data import app_users
from config import config

logger = logging.getLogger(__name__)


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

                semaphore = asyncio.Semaphore(10)
                product_failures = []

                async def process_product(full_product_id: str):
                    async with semaphore:
                        try:
                            parent_product_id = get_parent_product_id(full_product_id)
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

                if not successful_listings:
                    error_msg = f"Failed to create batch: all {len(request.product_ids)} products failed processing"
                    logger.error(
                        f"Batch creation failed - no successful listings",
                        extra={
                            "total_products": len(request.product_ids),
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

            return await BatchService._to_response(batch, include_listings=True)

        except BatchCreationError:
            raise
        except Exception as e:
            logger.error(f"Unexpected error creating batch: {e}", exc_info=True)
            raise

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
                        expanded_statuses.extend(["new", "in_progress"])
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
                normalized_search = search.strip().upper()
                parent_search_id = (
                    "/".join(normalized_search.split("/")[:-1])
                    if "/" in normalized_search
                    else normalized_search
                )
                query = query.filter(listings__product_id__icontains=parent_search_id)

            total = await query.count()

            batches = (
                await query.offset((page - 1) * page_size).limit(page_size).order_by("-created_at")
            )

            response_batches = []
            for batch in batches:
                response_batches.append(await BatchService._to_list_response(batch))

            return response_batches, total

        except Exception as e:
            logger.error(f"Error fetching batches: {e}")
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

            await batch.save()
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
            created_at=batch.created_at,
        )
