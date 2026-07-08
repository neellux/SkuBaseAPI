import json
import logging
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
from services.grailed_service import grailed_service
from services.template_service import TemplateService
from tortoise.transactions import in_transaction

logger = logging.getLogger(__name__)


class GrailedPoller(BasePoller):
    """Batches parked Grailed submissions and flushes them in chunks.

    Grailed becomes a manual_fallback platform: submissions park in PENDING and
    this poller flushes them to the AppScript ``addListings`` endpoint. Unlike
    SPO the endpoint is synchronous, so a submission goes
    PENDING -> PROCESSING -> SUCCESS/FAILED within a single cycle; there is no
    long-running import to poll.

    ``min_batch_size`` (from ``platform_settings.grailed``, default 100) is both
    the auto-flush threshold and the per-call chunk size: a scheduled cycle only
    flushes once at least that many are pending, and then sends them in chunks of
    that size (250 pending -> 100 + 100 + 50). A manual flush ignores the
    threshold and flushes whatever is pending, still chunked at that size.
    """

    def __init__(self) -> None:
        super().__init__(config_section="grailed_poller", name="GrailedPoller")
        cfg = config.get("grailed_poller", {})
        self.stale_timeout_minutes: int = cfg.get("stale_processing_timeout_minutes", 60)

    async def _get_grailed_settings(self) -> dict[str, Any]:
        settings = await AppSettings.first()
        if not settings:
            return {}
        return (settings.platform_settings or {}).get("grailed") or {}

    @staticmethod
    def _parse_batch_size(grailed_settings: dict[str, Any]) -> int:
        value = grailed_settings.get("min_batch_size", 100)
        try:
            return max(1, int(value))
        except (TypeError, ValueError):
            return 100

    async def _poll_cycle(self) -> None:
        await self._recover_stale_processing()
        await self._batch_upload_pending(force=False)

    async def _recover_stale_processing(self) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=self.stale_timeout_minutes)
        stale = await ListingSubmission.filter(
            platform_id="grailed",
            status=SubmissionStatus.PROCESSING,
            updated_at__lt=cutoff,
        ).all()

        for sub in stale:
            # A row stuck in PROCESSING means a flush was interrupted after we
            # committed to sending it. The AppScript call may have created the
            # listing, so we fail (not requeue) to avoid double-listing and leave
            # it for manual review.
            logger.warning(f"{self.name}: stale processing submission {sub.id}, marking failed")
            sub.status = SubmissionStatus.FAILED
            sub.error_display = "Batch interrupted - verify on Grailed before resubmitting"
            await sub.save()

    async def _batch_upload_pending(self, force: bool = False) -> dict[str, Any]:
        grailed_settings = await self._get_grailed_settings()
        manual_fallback = bool(grailed_settings.get("manual_fallback"))
        batch_size = self._parse_batch_size(grailed_settings)

        submission_ids: list[int] = []
        async with in_transaction("default") as conn:
            # One locked fetch: skip_locked means a concurrent caller (other poll
            # cycle or manual_flush) cannot grab the same rows. We only flip them
            # to PROCESSING if we actually commit to flushing them.
            pending = await (
                ListingSubmission.filter(
                    platform_id="grailed",
                    status=SubmissionStatus.PENDING,
                )
                .select_for_update(skip_locked=True)
                .using_db(conn)
            )

            if not pending:
                return {"submission_count": 0, "batch_count": 0}

            # Manual flush ignores the threshold. A scheduled cycle only applies
            # the min-batch gate when manual_fallback is enabled for Grailed.
            if not force and manual_fallback and len(pending) < batch_size:
                logger.debug(
                    "%s: %d pending below min_batch_size %d, skipping auto batch",
                    self.name,
                    len(pending),
                    batch_size,
                )
                return {"submission_count": 0, "batch_count": 0}

            submission_ids = [s.id for s in pending]
            await (
                ListingSubmission.filter(id__in=submission_ids)
                .using_db(conn)
                .update(status=SubmissionStatus.PROCESSING)
            )

        logger.info(
            f"{self.name}: batch submitting {len(submission_ids)} Grailed submissions "
            f"in chunks of {batch_size}"
        )

        template = await TemplateService.get_template_by_id("default")
        field_definitions = template.field_definitions if template else []

        total_success = 0
        chunk_count = 0
        for start in range(0, len(submission_ids), batch_size):
            chunk_ids = submission_ids[start : start + batch_size]
            chunk_count += 1
            total_success += await self._submit_chunk(chunk_ids, field_definitions)

        return {"submission_count": total_success, "batch_count": chunk_count}

    async def _submit_chunk(
        self, chunk_ids: list[int], field_definitions: list[dict[str, Any]]
    ) -> int:
        submissions = await ListingSubmission.filter(id__in=chunk_ids).prefetch_related("listing")
        subs_by_id = {s.id: s for s in submissions}

        all_products: list[dict[str, Any]] = []
        sku_to_sub: dict[str, int] = {}
        active_ids: list[int] = []

        for sub in submissions:
            listing = sub.listing
            if not listing:
                sub.status = SubmissionStatus.FAILED
                sub.error_display = "Listing not found"
                await sub.save()
                continue
            try:
                products = await grailed_service.build_csv_rows(
                    listing, listing.data or {}, field_definitions
                )
                if not products:
                    raise ValueError("No children found to submit to Grailed")
            except Exception:
                # A build error fails only this submission; the rest of the chunk
                # still goes out.
                logger.exception(
                    f"{self.name}: failed to build product rows for submission {sub.id}"
                )
                sub.status = SubmissionStatus.FAILED
                sub.error = traceback.format_exc()
                sub.error_display = "Failed to build product data"
                await sub.save()
                continue

            active_ids.append(sub.id)
            for product in products:
                sku = product.get("sku")
                if sku:
                    sku_to_sub[sku] = sub.id
            all_products.extend(products)

        if not all_products or not active_ids:
            return 0

        # A synthetic batch id groups this chunk's submissions into one "import"
        # row on the Submissions Dashboard (grailed has no platform-side import id,
        # so we reuse the same product_import_id field SPO uses). min(active_ids)
        # is unique per chunk (id sets are disjoint) and stable.
        batch_id = min(active_ids)
        batch_meta = {
            "product_import_id": batch_id,
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
        }

        try:
            response_data = await grailed_service.submit_batch(all_products)
        except Exception:
            # submit_batch already retried the AppScript in-call; reaching here
            # means no definitive response. The outcome is unknown, but addListings
            # dedups by SKU, so we requeue (not fail) rather than lose the batch:
            # the next poll cycle retries, and a request that actually landed
            # becomes an update, not a duplicate.
            logger.exception(
                f"{self.name}: Grailed AppScript unreachable, requeueing {len(active_ids)} submissions"
            )
            await ListingSubmission.filter(id__in=active_ids).update(
                status=SubmissionStatus.PENDING,
                error_display="Grailed AppScript unreachable, will retry",
            )
            return 0

        if not response_data.get("success"):
            error_msg = response_data.get("error", "Unknown error from Grailed AppScript")
            await ListingSubmission.filter(id__in=active_ids).update(
                status=SubmissionStatus.FAILED,
                error=error_msg,
                error_display=str(error_msg)[:500],
                platform_meta=batch_meta,
            )
            logger.error(f"{self.name}: Grailed AppScript returned error: {error_msg}")
            return 0

        # AppScript returns success=true even on PARTIAL failure (failed > 0), so
        # attribute per SKU rather than trusting the top-level flag:
        #   added_references:   ["<sku>_<MMDDYYYY>", ...]   (newly added)
        #   updated_references: [{"sku": ...}, ...]          (already on sheet -> done)
        #   failures:           [{"sku": ..., "error": ...}] (per-row failure)
        # A submission fails iff any of its child SKUs is in `failures`; otherwise
        # it succeeded (added and/or updated both count as listed on Grailed).
        ref_by_sku: dict[str, str] = {}
        for ref in response_data.get("added_references", []) or []:
            if isinstance(ref, str):
                ref_by_sku[ref.rsplit("_", 1)[0] if "_" in ref else ref] = ref

        error_by_sku: dict[str, str] = {}
        for failure in response_data.get("failures", []) or []:
            if isinstance(failure, dict) and failure.get("sku"):
                error_by_sku[failure["sku"]] = failure.get("error", "Unknown error")

        succeeded = 0
        for sub_id in active_ids:
            sub = subs_by_id[sub_id]
            sub_skus = [sku for sku, sid in sku_to_sub.items() if sid == sub_id]

            failed_skus = {sku: error_by_sku[sku] for sku in sub_skus if sku in error_by_sku}
            if failed_skus:
                sub.status = SubmissionStatus.FAILED
                sub.error = json.dumps(failed_skus)
                sub.error_display = ", ".join(f"{s}: {e}" for s, e in failed_skus.items())[:500]
                sub.platform_meta = {**batch_meta, "sku_errors": failed_skus}
                await sub.save()
                continue

            sub_refs = [ref_by_sku[sku] for sku in sub_skus if sku in ref_by_sku]
            sub.status = SubmissionStatus.SUCCESS
            sub.error_display = None  # clear any "will retry" note from a prior cycle
            sub.platform_meta = dict(batch_meta)
            if sub_refs:
                sub.external_id = sub_refs
            await sub.save()
            succeeded += 1

        logger.info(
            f"{self.name}: Grailed batch done, added={response_data.get('added', 0)}, "
            f"updated={response_data.get('updated', 0)}, failed={response_data.get('failed', 0)}, "
            f"submissions_succeeded={succeeded}/{len(active_ids)}"
        )
        return succeeded

    async def manual_flush(self) -> dict[str, Any]:
        return await self._batch_upload_pending(force=True)


grailed_poller = GrailedPoller()
