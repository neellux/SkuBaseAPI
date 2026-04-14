import asyncio
import logging
import os
import tempfile
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any

from config import config
from models.db_models import (
    Listing,
    ListingSubmission,
    SubmissionStatus,
)
from services.base_poller import BasePoller
from services.spo_service import spo_service, TERMINAL_STATUSES
from services.template_service import TemplateService
from tortoise.transactions import in_transaction

logger = logging.getLogger(__name__)


class SpoPoller(BasePoller):

    def __init__(self) -> None:
        super().__init__(config_section="spo_poller", name="SpoPoller")
        cfg = config.get("spo_poller", {})
        self.max_polls_per_submission: int = cfg.get("max_polls_per_submission", 40)
        self.max_batch_size: int = cfg.get("max_batch_size", 200)
        self.stale_timeout_minutes: int = cfg.get("stale_processing_timeout_minutes", 1440)

    async def _poll_cycle(self) -> None:
        await self._recover_stale_processing()
        await self._resume_products_complete()
        await self._batch_upload_pending()
        await self._check_processing()

    async def _recover_stale_processing(self) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=self.stale_timeout_minutes)
        stale = await ListingSubmission.filter(
            platform_id="spo",
            status=SubmissionStatus.PROCESSING,
            updated_at__lt=cutoff,
        ).all()

        for sub in stale:
            logger.warning(f"{self.name}: stale processing submission {sub.id}, marking failed")
            sub.status = SubmissionStatus.FAILED
            sub.error_display = "Import timed out after 24 hours"
            await sub.save()

    async def _resume_products_complete(self) -> None:
        stuck = await ListingSubmission.filter(
            platform_id="spo",
            status=SubmissionStatus.PROCESSING,
            platform_status="products_complete",
        ).all()

        if not stuck:
            return

        logger.info(
            f"{self.name}: resuming offer upload for {len(stuck)} submissions at products_complete"
        )
        for sub in stuck:
            try:
                await self._upload_offers_for_submission(sub)
            except Exception:
                logger.exception(
                    f"{self.name}: failed to resume offer upload for submission {sub.id}"
                )
                sub.status = SubmissionStatus.FAILED
                sub.error = traceback.format_exc()
                sub.error_display = "Failed to upload offers"
                await sub.save()

    async def _batch_upload_pending(self) -> None:
        submission_ids: list[int] = []
        async with in_transaction("default") as conn:
            pending = await (
                ListingSubmission.filter(
                    platform_id="spo",
                    status=SubmissionStatus.PENDING,
                )
                .select_for_update(skip_locked=True)
                .using_db(conn)
                .limit(self.max_batch_size)
            )

            if not pending:
                return

            submission_ids = [s.id for s in pending]
            await (
                ListingSubmission.filter(id__in=submission_ids)
                .using_db(conn)
                .update(
                    status=SubmissionStatus.PROCESSING,
                    platform_status="products_uploading",
                )
            )

        logger.info(f"{self.name}: batch uploading {len(submission_ids)} SPO submissions")

        all_products: list[dict[str, Any]] = []
        template = await TemplateService.get_template_by_id("default")
        field_definitions = template.field_definitions if template else []

        submissions = await ListingSubmission.filter(id__in=submission_ids).prefetch_related(
            "listing"
        )
        for sub in submissions:
            listing = sub.listing
            if not listing:
                sub.status = SubmissionStatus.FAILED
                sub.error_display = "Listing not found"
                await sub.save()
                continue
            try:
                products = await spo_service.build_product_rows(
                    listing, listing.data or {}, field_definitions
                )
                all_products.extend(products)
            except Exception:
                logger.exception(
                    f"{self.name}: failed to build product rows for submission {sub.id}"
                )
                sub.status = SubmissionStatus.FAILED
                sub.error = traceback.format_exc()
                sub.error_display = "Failed to build product data"
                await sub.save()
                submission_ids.remove(sub.id)

        if not all_products or not submission_ids:
            return

        tmp_dir = tempfile.mkdtemp(prefix="spo_")
        xlsx_path = os.path.join(
            tmp_dir, f"spo_products_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        )

        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None, spo_service.generate_product_xlsx, all_products, xlsx_path
            )
            import_id = await spo_service.upload_products(xlsx_path)

            await ListingSubmission.filter(id__in=submission_ids).update(
                platform_meta={"product_import_id": import_id},
                platform_status="products_processing",
            )
            logger.info(f"{self.name}: P41 upload successful, import_id={import_id}")

        except Exception as e:
            logger.exception(f"{self.name}: P41 upload failed")
            await ListingSubmission.filter(id__in=submission_ids).update(
                status=SubmissionStatus.FAILED,
                error=traceback.format_exc(),
                error_display="Failed to submit to SPO",
            )
        finally:
            try:
                os.remove(xlsx_path)
                os.rmdir(tmp_dir)
            except OSError:
                pass

    async def _check_processing(self) -> None:
        processing = await ListingSubmission.filter(
            platform_id="spo",
            status=SubmissionStatus.PROCESSING,
        ).all()

        if not processing:
            return

        product_groups: dict[int, list[ListingSubmission]] = {}
        offer_groups: dict[int, list[ListingSubmission]] = {}

        for sub in processing:
            meta = sub.platform_meta or {}
            offer_import_id = meta.get("offer_import_id")
            product_import_id = meta.get("product_import_id")

            if sub.platform_status == "offers_processing" and offer_import_id:
                offer_groups.setdefault(offer_import_id, []).append(sub)
            elif product_import_id:
                product_groups.setdefault(product_import_id, []).append(sub)

        for import_id, subs in product_groups.items():
            try:
                status_data = await spo_service.check_import_status(import_id)
                current_status = (status_data.get("import_status") or "UNKNOWN").upper()

                has_transform_errors = status_data.get("has_transformation_error_report", False)
                lines_read = status_data.get("transform_lines_read", 0)
                lines_in_error = status_data.get("transform_lines_in_error", 0)

                if has_transform_errors and lines_in_error > 0:
                    await self._handle_transformation_errors(import_id, subs)

                elif current_status == "COMPLETE":
                    await self._handle_products_complete(import_id, subs)

                elif current_status in TERMINAL_STATUSES:
                    error_msg = f"Product import {current_status.lower()}"
                    for sub in subs:
                        sub.status = SubmissionStatus.FAILED
                        sub.error_display = error_msg
                        await sub.save()
            except Exception:
                logger.exception(f"{self.name}: error checking P42 for import {import_id}")

        for import_id, subs in offer_groups.items():
            try:
                status_data = await spo_service.check_offer_status(import_id)
                current_status = (status_data.get("import_status") or "UNKNOWN").upper()

                if current_status == "COMPLETE":
                    await self._handle_offers_complete(import_id, subs)
                elif current_status in TERMINAL_STATUSES:
                    error_msg = f"Offer import {current_status.lower()}"
                    for sub in subs:
                        sub.status = SubmissionStatus.FAILED
                        sub.error_display = error_msg
                        await sub.save()
            except Exception:
                logger.exception(f"{self.name}: error checking OF02 for import {import_id}")

    async def _handle_transformation_errors(
        self, import_id: int, submissions: list[ListingSubmission]
    ) -> None:
        errors = await spo_service.get_transformation_error_report(import_id)
        failed_skus = {e["sku"]: e["error"] for e in errors}

        if not failed_skus:
            logger.warning(
                f"{self.name}: P47 flagged but no parseable errors for import {import_id}"
            )
            return

        for sub in submissions:
            listing = await Listing.get_or_none(id=sub.listing_id)
            if not listing:
                sub.status = SubmissionStatus.FAILED
                sub.error_display = "Listing not found"
                await sub.save()
                continue

            child_skus = list((listing.data or {}).get("child_size_overrides", {}).keys())
            sub_failed = {sku: failed_skus[sku] for sku in child_skus if sku in failed_skus}

            if sub_failed:
                error_parts = [f"{sku}: {err}" for sku, err in sub_failed.items()]
                sub.status = SubmissionStatus.FAILED
                sub.error_display = f"{', '.join(error_parts)}"[:500]
                await sub.save()
                logger.info(f"{self.name}: submission {sub.id} failed with transformation errors")

    async def _handle_products_complete(
        self, import_id: int, submissions: list[ListingSubmission]
    ) -> None:
        errors = await spo_service.get_error_report(import_id)
        failed_skus = {e["sku"]: e["error"] for e in errors}

        successful_subs: list[ListingSubmission] = []
        for sub in submissions:
            listing = await Listing.get_or_none(id=sub.listing_id)
            if not listing:
                sub.status = SubmissionStatus.FAILED
                sub.error_display = "Listing not found"
                await sub.save()
                continue

            child_skus = list((listing.data or {}).get("child_size_overrides", {}).keys())
            sub_failed = {sku: failed_skus[sku] for sku in child_skus if sku in failed_skus}

            if sub_failed:
                error_parts = [f"{sku} ({err})" for sku, err in sub_failed.items()]
                sub.status = SubmissionStatus.FAILED
                sub.error_display = f"Failed SKUs: {', '.join(error_parts)}"[:500]
                await sub.save()
            else:
                sub.platform_status = "products_complete"
                await sub.save()
                successful_subs.append(sub)

        for sub in successful_subs:
            try:
                await self._upload_offers_for_submission(sub)
            except Exception:
                logger.exception(f"{self.name}: failed to upload offers for submission {sub.id}")
                sub.status = SubmissionStatus.FAILED
                sub.error = traceback.format_exc()
                sub.error_display = "Failed to upload offers"
                await sub.save()

    async def _upload_offers_for_submission(self, sub: ListingSubmission) -> None:
        listing = await Listing.get_or_none(id=sub.listing_id)
        if not listing:
            raise ValueError("Listing not found")

        offers = spo_service.build_offer_rows(listing.data or {})
        if not offers:
            sub.status = SubmissionStatus.SUCCESS
            sub.platform_status = "listed"
            await sub.save()
            return

        tmp_dir = tempfile.mkdtemp(prefix="spo_offers_")
        csv_path = os.path.join(
            tmp_dir, f"spo_offers_{sub.id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )

        try:
            spo_service.generate_offer_csv(offers, csv_path)
            offer_import_id = await spo_service.upload_offers(csv_path)

            meta = sub.platform_meta or {}
            meta["offer_import_id"] = offer_import_id
            sub.platform_meta = meta
            sub.platform_status = "offers_processing"
            await sub.save()
        finally:
            try:
                os.remove(csv_path)
                os.rmdir(tmp_dir)
            except OSError:
                pass

    async def _handle_offers_complete(
        self, import_id: int, submissions: list[ListingSubmission]
    ) -> None:
        errors = await spo_service.get_offer_error_report(import_id)
        failed_skus = {e["sku"]: e["error"] for e in errors}

        for sub in submissions:
            listing = await Listing.get_or_none(id=sub.listing_id)
            if not listing:
                sub.status = SubmissionStatus.FAILED
                sub.error_display = "Listing not found"
                await sub.save()
                continue

            child_skus = list((listing.data or {}).get("child_size_overrides", {}).keys())
            sub_failed = {sku: failed_skus[sku] for sku in child_skus if sku in failed_skus}

            if sub_failed:
                error_parts = [f"{sku} ({err})" for sku, err in sub_failed.items()]
                sub.status = SubmissionStatus.FAILED
                sub.error_display = f"Offer failed: {', '.join(error_parts)}"[:500]
                await sub.save()
            else:
                sub.status = SubmissionStatus.SUCCESS
                sub.platform_status = "listed"
                await sub.save()
                logger.info(f"{self.name}: submission {sub.id} successfully listed on SPO")


spo_poller = SpoPoller()
