import asyncio
import logging
import traceback
from datetime import datetime, timedelta, timezone

from config import config
from models.db_models import (
    AppSettings,
    Listing,
    ListingSubmission,
    SubmissionStatus,
)
from services.base_poller import BasePoller
from services.grailed_service import grailed_service
from services.sellercloud_service import sellercloud_service
from services.template_service import TemplateService
from tortoise.transactions import in_transaction

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

        logger.warning(f"{self.name}: recovering {len(stale)} stale pending submissions")
        for sub in stale:
            sub.status = SubmissionStatus.FAILED
            sub.error_display = "Submission timed out - please retry"
            sub.error = (
                f"Stale pending submission recovered at {datetime.now(timezone.utc).isoformat()}"
            )
            await sub.save()

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

        settings = await AppSettings.first()
        ps_all = settings.platform_settings if settings else {}

        async def _submit(sub):
            async with self._semaphore:
                await self._submit_to_platform(sub)

        tasks = [
            _submit(sub)
            for sub in claimed_subs
            if not ps_all.get(sub.platform_id, {}).get("manual_fallback", False)
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _submit_to_platform(self, submission: ListingSubmission) -> None:
        listing = await Listing.get_or_none(id=submission.listing_id)
        if not listing:
            submission.status = SubmissionStatus.FAILED
            submission.error_display = "Failed to submit"
            submission.error = "Listing not found"
            await submission.save()
            return

        template = await TemplateService.get_template_by_id("default")
        form_data = listing.data or {}
        field_definitions = template.field_definitions if template else []

        try:
            if submission.platform_id == "sellercloud":
                await sellercloud_service.submit_listing_to_sellercloud(
                    product_id=listing.product_id,
                    form_data=form_data,
                    field_definitions=field_definitions,
                )
                submission.status = SubmissionStatus.SUCCESS
                await submission.save()
            elif submission.platform_id == "grailed":
                await grailed_service.submit_listing(
                    listing=listing,
                    form_data=form_data,
                    field_definitions=field_definitions,
                    submission=submission,
                )
            elif submission.platform_id == "spo":
                pass
            else:
                logger.warning(
                    f"{self.name}: unknown platform '{submission.platform_id}', skipping"
                )
                submission.status = SubmissionStatus.FAILED
                submission.error_display = f"Unknown platform: {submission.platform_id}"
                await submission.save()
        except Exception:
            logger.exception(
                f"{self.name}: submission failed for {submission.listing_id} on {submission.platform_id}"
            )
            submission = await ListingSubmission.get(id=submission.id)
            if submission.status not in ("success", "failed"):
                submission.status = SubmissionStatus.FAILED
                submission.error = traceback.format_exc()
                submission.error_display = "Failed to submit"
                await submission.save()


def _log_task_exception(task: asyncio.Task) -> None:
    if not task.cancelled() and task.exception():
        logger.error("Background task failed", exc_info=task.exception())


submission_poller = SubmissionPoller()
