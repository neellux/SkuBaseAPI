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
    ListingSubmission,
    SubmissionStatus,
)
from services.base_poller import BasePoller
from services.spo_service import spo_service, TERMINAL_STATUSES
from services.template_service import TemplateService
from tortoise.transactions import in_transaction
from utils.submission_steps import record_step

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
            await sub.save(update_fields=["status", "error_display", "updated_at"])
            await record_step(
                sub.id,
                "failed",
                stage=sub.platform_status or "unknown",
                reason=f"import timed out after {timeout_label}",
            )

    async def _resume_products_complete(self) -> None:
        # UNREACHABLE today: nothing sets platform_status "products_complete".
        # _handle_products_complete goes straight to "listed" because offers are
        # sent up-front in _batch_upload_pending. Kept for the case where the
        # offer step is moved back after the product import completes.
        stuck = await ListingSubmission.filter(
            platform_id="spo",
            status=SubmissionStatus.PROCESSING,
            platform_status="products_complete",
        ).prefetch_related("listing")

        if not stuck:
            return

        logger.info(
            f"{self.name}: resuming offer upload for {len(stuck)} submissions at products_complete"
        )
        for sub in stuck:
            try:
                await self._upload_offers_for_submission(sub)
            except Exception as e:
                logger.exception(
                    f"{self.name}: failed to resume offer upload for submission {sub.id}"
                )
                sub.status = SubmissionStatus.FAILED
                sub.error = traceback.format_exc()
                sub.error_display = "Failed to upload offers"
                await sub.save(
                    update_fields=["status", "error", "error_display", "updated_at"]
                )
                await record_step(
                    sub.id, "offers_failed", stage="resume", reason=str(e)[:300]
                )

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

        # Recorded outside the transaction above: record_step uses its own
        # connection and would block on the rows that transaction has locked.
        await record_step(submission_ids, "products_uploading", batch_size=len(submission_ids))

        template = await TemplateService.get_template_by_id("default")
        field_definitions = template.field_definitions if template else []

        submissions = await ListingSubmission.filter(id__in=submission_ids).prefetch_related(
            "listing"
        )
        submission_products: dict[int, list[dict[str, Any]]] = {}
        for sub in submissions:
            listing = sub.listing
            if not listing:
                sub.status = SubmissionStatus.FAILED
                sub.error_display = "Listing not found"
                await sub.save(update_fields=["status", "error_display", "updated_at"])
                await record_step(sub.id, "failed", stage="build_products", reason="listing not found")
                submission_ids.remove(sub.id)
                continue
            try:
                products = await spo_service.build_product_rows(
                    listing, listing.data or {}, field_definitions
                )
            except Exception as e:
                logger.exception(
                    f"{self.name}: failed to build product rows for submission {sub.id}"
                )
                sub.status = SubmissionStatus.FAILED
                sub.error = traceback.format_exc()
                sub.error_display = "Failed to build product data"
                await sub.save(
                    update_fields=["status", "error", "error_display", "updated_at"]
                )
                await record_step(
                    sub.id, "failed", stage="build_products", reason=str(e)[:300]
                )
                submission_ids.remove(sub.id)
                continue

            if not products:
                # Product rows and offer rows both derive from
                # data.child_size_overrides, so a listing with no child SKUs
                # sends nothing to either destination. Such a submission used to
                # ride along in the batch and get marked `listed` once P42 came
                # back COMPLETE, reporting success for an upload it was never
                # part of.
                logger.warning(
                    f"{self.name}: submission {sub.id} has no child SKUs, failing it"
                )
                sub.status = SubmissionStatus.FAILED
                sub.error_display = "No child SKUs to submit"
                await sub.save(update_fields=["status", "error_display", "updated_at"])
                await record_step(
                    sub.id,
                    "failed",
                    stage="build_products",
                    reason="no child SKUs on listing (child_size_overrides empty)",
                )
                submission_ids.remove(sub.id)
                continue

            submission_products[sub.id] = products

        # Submit offers to AppScript FIRST so they land on the sheet. Only after
        # AppScript succeeds for a submission do we include its products in the
        # P41 upload — failed AppScript submissions are dropped from this batch.
        for sub in submissions:
            if sub.id not in submission_ids:
                continue
            listing = sub.listing
            if not listing:
                continue
            try:
                offers = spo_service.build_offer_rows(listing.data or {})
            except Exception as e:
                logger.exception(
                    f"{self.name}: failed to build offer rows for submission {sub.id}"
                )
                await ListingSubmission.filter(id=sub.id).update(
                    status=SubmissionStatus.FAILED,
                    error=traceback.format_exc(),
                    error_display="Failed to build offer data",
                )
                await record_step(
                    sub.id, "offers_failed", stage="build_offers", reason=str(e)[:300]
                )
                submission_ids.remove(sub.id)
                submission_products.pop(sub.id, None)
                continue

            if not offers:
                # Unreachable while the product-row build above gates on the
                # same data.child_size_overrides. Kept as a failure rather than
                # a skip so that if the two ever diverge, the result is a visible
                # failed submission and not one marked `listed` with nothing on
                # the sheet.
                logger.error(
                    f"{self.name}: submission {sub.id} built product rows but no "
                    f"offer rows, failing it"
                )
                await ListingSubmission.filter(id=sub.id).update(
                    status=SubmissionStatus.FAILED,
                    error_display="No offers to submit",
                )
                await record_step(
                    sub.id,
                    "failed",
                    stage="build_offers",
                    reason="no offer rows built despite product rows existing",
                )
                submission_ids.remove(sub.id)
                submission_products.pop(sub.id, None)
                continue

            try:
                result = await spo_service.submit_offers(offers)
            except Exception as e:
                logger.exception(
                    f"{self.name}: AppScript offer submission failed for submission {sub.id}"
                )
                await ListingSubmission.filter(id=sub.id).update(
                    status=SubmissionStatus.FAILED,
                    error=traceback.format_exc(),
                    error_display="Failed to submit offers to SPO AppScript after 3 attempts",
                )
                await record_step(
                    sub.id, "offers_failed", stage="appscript", reason=str(e)[:300]
                )
                submission_ids.remove(sub.id)
                submission_products.pop(sub.id, None)
                continue

            # Recorded outside the try: the sheet write has already happened, so
            # a failure persisting the step must not be reported as an AppScript
            # failure and trigger a resubmit that duplicates the sheet rows.
            await record_step(
                sub.id,
                "offers_submitted",
                skus=[o["sku"] for o in offers],
                added=result.get("addedCount"),
                skipped=result.get("skippedCount"),
            )

        all_products: list[dict[str, Any]] = []
        for sub_id in submission_ids:
            all_products.extend(submission_products.get(sub_id, []))

        if not all_products or not submission_ids:
            # The claim transaction already flipped these rows to PROCESSING /
            # products_uploading. Returning without touching them stranded them:
            # _check_processing only polls rows that carry a product_import_id, so
            # nothing would move them until the stale sweep (7 days in prod).
            # Nothing was sent to SPO, so requeueing is safe and matches the
            # requeue-on-unknown-outcome convention in grailed_poller.
            if submission_ids:
                logger.warning(
                    f"{self.name}: no products to upload, requeueing "
                    f"{len(submission_ids)} claimed submissions"
                )
                await ListingSubmission.filter(id__in=submission_ids).update(
                    status=SubmissionStatus.PENDING,
                    platform_status=None,
                )
                await record_step(
                    submission_ids,
                    "requeued",
                    stage="products_uploading",
                    reason="batch produced no product rows; nothing was sent to SPO",
                )
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

            # Merged, not replaced: a wholesale platform_meta write here would
            # discard the offer step recorded above. Recorded before the status
            # flip so a failure here leaves the rows at products_uploading and
            # the except below marks them failed consistently.
            await record_step(
                submission_ids,
                "products_uploaded",
                meta={
                    "product_import_id": import_id,
                    # When this batch was actually uploaded to SPO; the
                    # submissions dashboard shows it as the import's created time.
                    "uploaded_at": datetime.now(timezone.utc).isoformat(),
                    # The exact file name sent to SPO, so the dashboard can be
                    # matched against the SPO-side import list.
                    "file_name": os.path.basename(xlsx_path),
                },
                product_import_id=import_id,
                file_name=os.path.basename(xlsx_path),
                product_rows=len(all_products),
            )
            await ListingSubmission.filter(id__in=submission_ids).update(
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
            await record_step(
                submission_ids, "failed", stage="products_upload", reason=str(e)[:300]
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
        # Prefetched so the completion handlers below read sub.listing instead of
        # issuing one Listing query per submission. An import carries up to a few
        # hundred submissions against a remote database (~0.5s per round trip),
        # so the per-row form dominated this path. Same pattern as
        # _batch_upload_pending and grailed_poller._submit_chunk.
        processing = await ListingSubmission.filter(
            platform_id="spo",
            status=SubmissionStatus.PROCESSING,
        ).prefetch_related("listing")

        if not processing:
            return

        product_groups: dict[int, list[ListingSubmission]] = {}
        offer_groups: dict[int, list[ListingSubmission]] = {}

        for sub in processing:
            meta = sub.platform_meta or {}
            offer_import_id = meta.get("offer_import_id")
            product_import_id = meta.get("product_import_id")

            # The offers_processing branch is dead while offers go via AppScript:
            # nothing sets that platform_status and no offer_import_id is ever
            # stored. See the note above spo_service.upload_offers().
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
                    # Batched: every submission in this import gets the identical
                    # message, so one statement replaces one per row. Both status
                    # triggers are FOR EACH ROW and still fire per submission.
                    error_msg = f"Product import {current_status.lower()}"
                    sub_ids = [s.id for s in subs]
                    await ListingSubmission.filter(id__in=sub_ids).update(
                        status=SubmissionStatus.FAILED,
                        error_display=error_msg,
                    )
                    await record_step(
                        sub_ids,
                        "failed",
                        stage="products_processing",
                        reason=error_msg,
                        product_import_id=import_id,
                    )
            except Exception:
                logger.exception(f"{self.name}: error checking P42 for import {import_id}")

        for import_id, subs in offer_groups.items():
            try:
                status_data = await spo_service.check_offer_status(import_id)
                current_status = (status_data.get("import_status") or "UNKNOWN").upper()

                if current_status == "COMPLETE":
                    await self._handle_offers_complete(import_id, subs)
                elif current_status in TERMINAL_STATUSES:
                    # Batched for the same reason as the product branch above.
                    error_msg = f"Offer import {current_status.lower()}"
                    sub_ids = [s.id for s in subs]
                    await ListingSubmission.filter(id__in=sub_ids).update(
                        status=SubmissionStatus.FAILED,
                        error_display=error_msg,
                    )
                    await record_step(
                        sub_ids,
                        "failed",
                        stage="offers_processing",
                        reason=error_msg,
                        offer_import_id=import_id,
                    )
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
            listing = sub.listing
            if not listing:
                sub.status = SubmissionStatus.FAILED
                sub.error_display = "Listing not found"
                await sub.save(update_fields=["status", "error_display", "updated_at"])
                await record_step(
                    sub.id, "failed", stage="transformation", reason="listing not found"
                )
                continue

            child_skus = list((listing.data or {}).get("child_size_overrides", {}).keys())
            sub_failed = {sku: failed_skus[sku] for sku in child_skus if sku in failed_skus}

            if sub_failed:
                error_parts = [f"{sku}: {err}" for sku, err in sub_failed.items()]
                sub.status = SubmissionStatus.FAILED
                sub.error_display = f"{', '.join(error_parts)}"[:500]
                await sub.save(update_fields=["status", "error_display", "updated_at"])
                await record_step(
                    sub.id,
                    "failed",
                    meta={"sku_errors": sub_failed},
                    stage="transformation",
                    product_import_id=import_id,
                    sku_errors=sub_failed,
                )
                logger.info(f"{self.name}: submission {sub.id} failed with transformation errors")

    async def _handle_products_complete(
        self, import_id: int, submissions: list[ListingSubmission]
    ) -> None:
        errors = await spo_service.get_error_report(import_id)
        failed_skus = {e["sku"]: e["error"] for e in errors}

        listed_ids: list[int] = []
        for sub in submissions:
            listing = sub.listing
            if not listing:
                sub.status = SubmissionStatus.FAILED
                sub.error_display = "Listing not found"
                await sub.save(update_fields=["status", "error_display", "updated_at"])
                await record_step(
                    sub.id, "failed", stage="products_complete", reason="listing not found"
                )
                continue

            child_skus = list((listing.data or {}).get("child_size_overrides", {}).keys())
            sub_failed = {sku: failed_skus[sku] for sku in child_skus if sku in failed_skus}

            if not child_skus:
                # An empty child list makes sub_failed empty too, which used to
                # fall through to `listed` even though the import contained no
                # rows for this submission. The batch build now rejects these up
                # front; this covers rows already at products_processing from a
                # prior deploy.
                logger.warning(
                    f"{self.name}: submission {sub.id} completed with no child SKUs, failing it"
                )
                sub.status = SubmissionStatus.FAILED
                sub.error_display = "No child SKUs to submit"
                await sub.save(update_fields=["status", "error_display", "updated_at"])
                await record_step(
                    sub.id,
                    "failed",
                    stage="products_complete",
                    reason="no child SKUs on listing (child_size_overrides empty)",
                    product_import_id=import_id,
                )
            elif sub_failed:
                error_parts = [f"{sku} ({err})" for sku, err in sub_failed.items()]
                sub.status = SubmissionStatus.FAILED
                sub.error_display = f"Failed SKUs: {', '.join(error_parts)}"[:500]
                await sub.save(update_fields=["status", "error_display", "updated_at"])
                await record_step(
                    sub.id,
                    "failed",
                    meta={"sku_errors": sub_failed},
                    stage="products_complete",
                    product_import_id=import_id,
                    sku_errors=sub_failed,
                )
            else:
                listed_ids.append(sub.id)

        # Batched: an import carries up to a few hundred submissions and the
        # database is remote (~0.5s per round trip), so a statement per row would
        # dominate this handler. Both status triggers are FOR EACH ROW, so they
        # still fire once per submission. The failure branches above stay per-row
        # because each writes a different error message.
        if listed_ids:
            await ListingSubmission.filter(id__in=listed_ids).update(
                status=SubmissionStatus.SUCCESS,
                platform_status="listed",
            )
            await record_step(listed_ids, "listed", product_import_id=import_id)
            logger.info(
                f"{self.name}: {len(listed_ids)} submissions successfully listed on SPO"
            )

    async def _upload_offers_for_submission(self, sub: ListingSubmission) -> None:
        listing = sub.listing
        if not listing:
            raise ValueError("Listing not found")

        offers = spo_service.build_offer_rows(listing.data or {})
        if not offers:
            # A listing with no child SKUs has nothing to put on the sheet, so
            # this is a failure, not a listing. It used to be marked `listed`.
            logger.warning(
                f"{self.name}: submission {sub.id} has no child SKUs, failing it"
            )
            sub.status = SubmissionStatus.FAILED
            sub.error_display = "No child SKUs to submit"
            await sub.save(update_fields=["status", "error_display", "updated_at"])
            await record_step(
                sub.id,
                "failed",
                stage="resume_offers",
                reason="no child SKUs on listing (child_size_overrides empty)",
            )
            return

        result = await spo_service.submit_offers(offers)

        skipped = result.get("skipped") or []
        sub_skipped: list[dict[str, Any]] = []
        if skipped:
            child_skus = set((listing.data or {}).get("child_size_overrides", {}).keys())
            sub_skipped = [s for s in skipped if s.get("sku") in child_skus]
            if sub_skipped:
                logger.info(
                    f"{self.name}: submission {sub.id} had {len(sub_skipped)} "
                    f"already-on-sheet SKUs: {sub_skipped}"
                )

        await record_step(
            sub.id,
            "offers_submitted",
            skus=[o["sku"] for o in offers],
            added=result.get("addedCount"),
            skipped=result.get("skippedCount"),
            already_on_sheet=[s.get("sku") for s in sub_skipped] or None,
        )

        sub.status = SubmissionStatus.SUCCESS
        sub.platform_status = "listed"
        await sub.save(update_fields=["status", "platform_status", "updated_at"])
        await record_step(sub.id, "listed")
        logger.info(f"{self.name}: submission {sub.id} successfully listed on SPO")

    async def _handle_offers_complete(
        self, import_id: int, submissions: list[ListingSubmission]
    ) -> None:
        """UNREACHABLE today: reached only from the "offers_processing" branch of
        _check_processing, and nothing sets that platform_status. Offers go via
        AppScript, which returns no import id. See the note above
        spo_service.upload_offers()."""
        errors = await spo_service.get_offer_error_report(import_id)
        failed_skus = {e["sku"]: e["error"] for e in errors}

        for sub in submissions:
            listing = sub.listing
            if not listing:
                sub.status = SubmissionStatus.FAILED
                sub.error_display = "Listing not found"
                await sub.save(update_fields=["status", "error_display", "updated_at"])
                await record_step(
                    sub.id, "failed", stage="offers_complete", reason="listing not found"
                )
                continue

            child_skus = list((listing.data or {}).get("child_size_overrides", {}).keys())
            sub_failed = {sku: failed_skus[sku] for sku in child_skus if sku in failed_skus}

            if sub_failed:
                error_parts = [f"{sku} ({err})" for sku, err in sub_failed.items()]
                sub.status = SubmissionStatus.FAILED
                sub.error_display = f"Offer failed: {', '.join(error_parts)}"[:500]
                await sub.save(update_fields=["status", "error_display", "updated_at"])
                await record_step(
                    sub.id,
                    "failed",
                    meta={"sku_errors": sub_failed},
                    stage="offers_complete",
                    offer_import_id=import_id,
                    sku_errors=sub_failed,
                )
            else:
                sub.status = SubmissionStatus.SUCCESS
                sub.platform_status = "listed"
                await sub.save(
                    update_fields=["status", "platform_status", "updated_at"]
                )
                await record_step(sub.id, "listed", offer_import_id=import_id)
                logger.info(f"{self.name}: submission {sub.id} successfully listed on SPO")


spo_poller = SpoPoller()
