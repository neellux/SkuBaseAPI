import asyncio
import logging
import os
import tempfile
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any

from config import config
from models.db_models import (
    AppSettings,
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
        self.stale_timeout_minutes: int = cfg.get("stale_processing_timeout_minutes", 1440)

    async def _get_spo_settings(self) -> dict[str, Any]:
        settings = await AppSettings.first()
        if not settings:
            return {}
        return (settings.platform_settings or {}).get("spo") or {}

    @staticmethod
    def _parse_min_batch_size(spo_settings: dict[str, Any]) -> int:
        value = spo_settings.get("min_batch_size", 200)
        try:
            return max(1, int(value))
        except (TypeError, ValueError):
            return 200

    async def _poll_cycle(self) -> None:
        await self._recover_stale_processing()
        await self._resume_products_complete()
        await self._batch_upload_pending(force=False)
        await self._check_processing()

    async def _recover_stale_processing(self) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=self.stale_timeout_minutes)
        stale = await ListingSubmission.filter(
            platform_id="spo",
            status=SubmissionStatus.PROCESSING,
            updated_at__lt=cutoff,
        ).all()

        timeout_hours = self.stale_timeout_minutes / 60
        if timeout_hours >= 24 and timeout_hours % 24 == 0:
            timeout_label = f"{int(timeout_hours // 24)} days"
        else:
            timeout_label = f"{timeout_hours:g} hours"

        for sub in stale:
            logger.warning(f"{self.name}: stale processing submission {sub.id}, marking failed")
            sub.status = SubmissionStatus.FAILED
            sub.error_display = f"Import timed out after {timeout_label}"
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

    async def _batch_upload_pending(self, force: bool = False) -> dict[str, Any]:
        spo_settings = await self._get_spo_settings()
        manual_fallback = bool(spo_settings.get("manual_fallback"))
        min_batch_size = self._parse_min_batch_size(spo_settings)

        submission_ids: list[int] = []
        async with in_transaction("default") as conn:
            # One locked fetch: skip_locked means a concurrent caller (other
            # poll cycle or manual_flush) cannot grab the same rows. We hold
            # the lock for the duration of this transaction and only flip the
            # rows to PROCESSING if we actually commit to uploading them.
            pending = await (
                ListingSubmission.filter(
                    platform_id="spo",
                    status=SubmissionStatus.PENDING,
                )
                .select_for_update(skip_locked=True)
                .using_db(conn)
            )

            if not pending:
                return {"submission_count": 0, "product_import_id": None}

            # Manual flush ignores both gates. The scheduled poll cycle only
            # applies the min-batch gate when manual_fallback is enabled for
            # SPO; otherwise it flushes whatever is pending the same as before.
            if not force and manual_fallback and len(pending) < min_batch_size:
                logger.debug(
                    "%s: %d pending below min_batch_size %d, skipping auto batch",
                    self.name,
                    len(pending),
                    min_batch_size,
                )
                return {"submission_count": 0, "product_import_id": None}

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
            return {"submission_count": 0, "product_import_id": None}

        tmp_dir = tempfile.mkdtemp(prefix="spo_")
        xlsx_path = os.path.join(
            tmp_dir, f"spo_products_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        )

        import_id: int | None = None
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

        except Exception:
            logger.exception(f"{self.name}: P41 upload failed")
            await ListingSubmission.filter(id__in=submission_ids).update(
                status=SubmissionStatus.FAILED,
                error=traceback.format_exc(),
                error_display="Failed to submit to SPO",
            )
            return {"submission_count": 0, "product_import_id": None}
        finally:
            try:
                os.remove(xlsx_path)
                os.rmdir(tmp_dir)
            except OSError:
                pass

        return {"submission_count": len(submission_ids), "product_import_id": import_id}

    async def manual_flush(self) -> dict[str, Any]:
        return await self._batch_upload_pending(force=True)

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
                sub.platform_meta = {**(sub.platform_meta or {}), "sku_errors": sub_failed}
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
                sub.platform_meta = {**(sub.platform_meta or {}), "sku_errors": sub_failed}
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

        result = await spo_service.submit_offers(offers)

        skipped = result.get("skipped") or []
        if skipped:
            child_skus = set((listing.data or {}).get("child_size_overrides", {}).keys())
            sub_skipped = [s for s in skipped if s.get("sku") in child_skus]
            if sub_skipped:
                logger.info(
                    f"{self.name}: submission {sub.id} had {len(sub_skipped)} "
                    f"already-on-sheet SKUs: {sub_skipped}"
                )

        sub.status = SubmissionStatus.SUCCESS
        sub.platform_status = "listed"
        await sub.save()
        logger.info(f"{self.name}: submission {sub.id} successfully listed on SPO")

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
                sub.platform_meta = {**(sub.platform_meta or {}), "sku_errors": sub_failed}
                await sub.save()
            else:
                sub.status = SubmissionStatus.SUCCESS
                sub.platform_status = "listed"
                await sub.save()
                logger.info(f"{self.name}: submission {sub.id} successfully listed on SPO")


spo_poller = SpoPoller()
