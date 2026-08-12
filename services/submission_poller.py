import asyncio
import logging
import traceback
from datetime import datetime, timedelta, timezone

from config import config
from exceptions.submission_exceptions import SellerCloudSubmitError
from models.db_models import (
    AppSettings,
    Listing,
    ListingSubmission,
    SubmissionStatus,
)
from services.base_poller import BasePoller
from services.oneinventory_service import oneinventory_service
from services.sellercloud_service import sellercloud_service
from services.template_service import TemplateService
from tortoise.transactions import in_transaction
from utils.submission_steps import record_step

logger = logging.getLogger(__name__)

STALE_PENDING_MINUTES = 10


class SubmissionPoller(BasePoller):

    def __init__(self) -> None:
        super().__init__(config_section="submission_poller", name="SubmissionPoller")
        cfg = config.get("submission_poller", {})
        self.max_concurrent: int = cfg.get("max_concurrent", 1)
        self._semaphore: asyncio.Semaphore = asyncio.Semaphore(self.max_concurrent)

    async def _poll_cycle(self) -> None:
        await self._recover_stale_submissions()
        await self._process_queued_submissions()

    async def _recover_stale_submissions(self) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=STALE_PENDING_MINUTES)
        stale = await ListingSubmission.filter(
            status=SubmissionStatus.PENDING,
            updated_at__lt=cutoff,
        ).all()

        if not stale:
            return

        # Manual-fallback platforms (e.g. SPO) legitimately park rows in
        # PENDING while waiting for a batch flush, so they must not be
        # swept as stale.
        settings = await AppSettings.first()
        ps_all = settings.platform_settings if settings else {}
        stale = [
            sub
            for sub in stale
            if not ps_all.get(sub.platform_id, {}).get("manual_fallback", False)
        ]

        if not stale:
            return

        logger.warning(f"{self.name}: recovering {len(stale)} stale pending submissions")
        for sub in stale:
            sub.status = SubmissionStatus.FAILED
            sub.error_display = "Submission timed out - please retry"
            sub.error = (
                f"Stale pending submission recovered at {datetime.now(timezone.utc).isoformat()}"
            )
            await sub.save(
                update_fields=["status", "error", "error_display", "updated_at"]
            )
        await record_step(
            [s.id for s in stale],
            "failed",
            stage="pending",
            reason=f"no progress for {STALE_PENDING_MINUTES} minutes",
        )

    async def _process_queued_submissions(self) -> None:
        claimed_subs: list[ListingSubmission] = []
        async with in_transaction("default") as conn:
            queued = await (
                ListingSubmission.filter(
                    status=SubmissionStatus.QUEUED,
                    listing__upload_status="uploaded",
                )
                .select_for_update(skip_locked=True)
                .using_db(conn)
            )

            if not queued:
                return

            logger.info(f"{self.name}: transitioning {len(queued)} queued submissions to pending")
            for sub in queued:
                sub.status = SubmissionStatus.PENDING
                await sub.save(using_db=conn)
                claimed_subs.append(sub)

        # Recorded after the transaction commits: record_step uses its own
        # connection and would block on the rows locked above.
        await record_step([s.id for s in claimed_subs], "pending")

        settings = await AppSettings.first()
        ps_all = settings.platform_settings if settings else {}

        # Fetched once per cycle rather than once per submission: the template is
        # the same for every row, and against a remote database the per-submission
        # form cost a round trip each. spo_poller and grailed_poller already fetch
        # it once per batch.
        template = await TemplateService.get_template_by_id("default")
        field_definitions = template.field_definitions if template else []

        async def _submit(sub):
            async with self._semaphore:
                await self._submit_to_platform(sub, field_definitions)

        tasks = [
            _submit(sub)
            for sub in claimed_subs
            if not ps_all.get(sub.platform_id, {}).get("manual_fallback", False)
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _submit_to_platform(
        self, submission: ListingSubmission, field_definitions: list | None = None
    ) -> None:
        listing = await Listing.get_or_none(id=submission.listing_id)
        if not listing:
            submission.status = SubmissionStatus.FAILED
            submission.error_display = "Failed to submit"
            submission.error = "Listing not found"
            await submission.save(
                update_fields=["status", "error", "error_display", "updated_at"]
            )
            await record_step(
                submission.id, "failed", stage="submit", reason="listing not found"
            )
            return

        form_data = listing.data or {}
        # Only fetched here when the caller did not supply it (direct callers other
        # than the poll cycle).
        if field_definitions is None:
            template = await TemplateService.get_template_by_id("default")
            field_definitions = template.field_definitions if template else []

        try:
            if submission.platform_id == "sellercloud":
                await sellercloud_service.submit_listing_to_sellercloud(
                    product_id=listing.product_id,
                    form_data=form_data,
                    field_definitions=field_definitions,
                )
                submission.status = SubmissionStatus.SUCCESS
                await submission.save(update_fields=["status", "updated_at"])
                await record_step(submission.id, "listed")
            elif submission.platform_id == "1nventory":
                # This is the PRIMARY path for 1inventory, not a fallback. Its
                # requires_images setting parks the row QUEUED until photo_upload_poller
                # flips upload_status to 'uploaded', which is the normal flow for a
                # freshly photographed listing. Without this branch the `else` below
                # would fail every one of them as "Unknown platform".
                #
                # run_submission owns the status/step/external_id writes, so a row from
                # here is indistinguishable from one submitted inline by listing_routes.
                await oneinventory_service.run_submission(submission, listing)
            elif submission.platform_id in ("grailed", "spo", "ebay"):
                # manual_fallback batch platforms handled by their own pollers
                # (grailed_poller / spo_poller); nothing to do per-listing.
                #
                # ebay is listed here before it has a poller on purpose. It is disabled
                # (absent from app_settings.platforms) so no rows should exist, but the
                # `else` below hard-fails any unrecognised platform, which would turn an
                # accidental enable into a wall of "Unknown platform: ebay" failures
                # instead of rows parked harmlessly in queued.
                pass
            else:
                logger.warning(
                    f"{self.name}: unknown platform '{submission.platform_id}', skipping"
                )
                submission.status = SubmissionStatus.FAILED
                submission.error_display = f"Unknown platform: {submission.platform_id}"
                await submission.save(
                    update_fields=["status", "error_display", "updated_at"]
                )
                await record_step(
                    submission.id,
                    "failed",
                    stage="submit",
                    reason=f"unknown platform: {submission.platform_id}",
                )
        except Exception as e:
            logger.exception(
                f"{self.name}: submission failed for {submission.listing_id} on {submission.platform_id}"
            )
            # Same per-SKU/per-stage detail as the route path in listing_routes,
            # so a submission looks identical whichever path submitted it.
            sc_error = e if isinstance(e, SellerCloudSubmitError) else None
            submission = await ListingSubmission.get(id=submission.id)
            if submission.status not in ("success", "failed"):
                submission.status = SubmissionStatus.FAILED
                submission.error = traceback.format_exc()
                submission.error_display = (
                    sc_error.display() if sc_error else "Failed to submit"
                )
                await submission.save(
                    update_fields=["status", "error", "error_display", "updated_at"]
                )
                if sc_error:
                    await record_step(
                        submission.id,
                        "failed",
                        meta=(
                            {"sku_errors": sc_error.sku_errors}
                            if sc_error.sku_errors
                            else None
                        ),
                        stage=sc_error.stage,
                        sku_errors=sc_error.sku_errors or None,
                        succeeded_skus=sc_error.succeeded or None,
                    )
                else:
                    await record_step(
                        submission.id, "failed", stage="submit", reason=str(e)[:300]
                    )


def _log_task_exception(task: asyncio.Task) -> None:
    if not task.cancelled() and task.exception():
        logger.error("Background task failed", exc_info=task.exception())


submission_poller = SubmissionPoller()
