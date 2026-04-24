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
from services.listing_service import ListingService
from services.sellercloud_service import sellercloud_service
from services.template_service import TemplateService
from tortoise import connections
from tortoise.transactions import in_transaction

logger = logging.getLogger(__name__)

STALE_PENDING_MINUTES = 10


class SubmissionPoller(BasePoller):

    def __init__(self) -> None:
        super().__init__(config_section="submission_poller", name="SubmissionPoller")
        cfg = config.get("submission_poller", {})
        self.max_auto_submit_per_cycle: int = cfg.get("max_auto_submit_per_cycle", 50)
        self.max_concurrent: int = cfg.get("max_concurrent", 1)
        self._semaphore: asyncio.Semaphore = asyncio.Semaphore(self.max_concurrent)

    async def _poll_cycle(self) -> None:
        await self._recover_stale_submissions()
        await self._process_queued_submissions()
        await self._auto_submit_new()

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
            if not ps_all.get(sub.platform_id, {}).get("batch_submit", False)
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _auto_submit_new(self) -> None:
        settings = await AppSettings.first()
        if not settings:
            return
        platform_settings = settings.platform_settings or {}

        for platform_id, ps in platform_settings.items():
            if not ps.get("auto_submit", False):
                continue

            conn = connections.get("default")
            rows = await conn.execute_query_dict(
                """
                SELECT l.id FROM listings l
                WHERE l.upload_status = 'uploaded'
                AND NOT EXISTS (
                    SELECT 1 FROM listing_submissions ls
                    WHERE ls.listing_id = l.id
                    AND ls.platform_id = $1
                    AND ls.status IN ('queued', 'pending', 'processing', 'success')
                )
                LIMIT $2
                """,
                [platform_id, self.max_auto_submit_per_cycle],
            )

            if not rows:
                continue

            logger.info(f"{self.name}: auto-submitting {len(rows)} listings to {platform_id}")

            submissions: list[ListingSubmission] = []
            for row in rows:
                listing_id = row["id"]
                try:
                    latest = await (
                        ListingSubmission.filter(
                            listing_id=listing_id,
                            platform_id=platform_id,
                        )
                        .order_by("-attempt_number")
                        .first()
                    )
                    attempt_number = (latest.attempt_number + 1) if latest else 1

                    submission = await ListingSubmission.create(
                        listing_id=listing_id,
                        platform_id=platform_id,
                        status=SubmissionStatus.PENDING,
                        attempt_number=attempt_number,
                    )
                    if not ps.get("batch_submit", False):
                        submissions.append(submission)
                except Exception:
                    logger.exception(
                        f"{self.name}: failed to create auto-submit for listing {listing_id} on {platform_id}"
                    )

            async def _submit(sub):
                async with self._semaphore:
                    await self._submit_to_platform(sub)

            if submissions:
                await asyncio.gather(
                    *[_submit(sub) for sub in submissions],
                    return_exceptions=True,
                )

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
