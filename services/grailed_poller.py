"""Grailed submissions: parked as PENDING, flushed in chunks to the AppScript addListings,
then, with results tracking on, settled from Grailed's own result files in Drive.

Results tracking ([grailed_poller] results_enabled; docs/grailed_integration.md). The
AppScript only writes rows to a sheet. Grailed's processor reads that sheet about every two
hours and writes, per run, a success file and an error file to the Grailed processing Drive
folder. With tracking on, a submission the AppScript accepted stays `processing` with
platform_status `sent` ("Sent to Grailed") until _check_results reads those files and
services/grailed_results.py decides it. There is no timeout. With tracking off an accepted
submission is success at once, as before.

Presence. A successful submission records a Grailed parent row plus one child row per sku
sent (grailed_service.presence_rows): at accept with tracking off, at the decided success
with tracking on (a failure writes none, so it can be fixed and resubmitted). Each child
row's id is the real Grailed listing id once a result file reports one, and otherwise the
sheet key, "<child sku>_<MMDDYYYY>", parsed from added_references
(utils/grailed_keys.sheet_keys_by_sku). A child refreshed in place keeps its stored id. The
Listing history tab shows and edits these per size. See docs/external_listing_ids.md.
"""

import asyncio
import json
import logging
import traceback
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

from config import config
from exceptions.grailed_exceptions import (
    GrailedDrivePermanentError,
    GrailedDriveTransientError,
)
from models.db_models import (
    AppSettings,
    GrailedResultRun,
    Listing,
    ListingSubmission,
    SubmissionStatus,
)
from services import grailed_results
from services import grailed_results_parser as parser
from services.base_poller import BasePoller
from services.external_listing_service import ExternalListingService
from services.grailed_drive import FOLDER_MIME, grailed_drive
from services.grailed_service import GrailedStage, grailed_service, presence_rows
from services.scheduled_flush import run_scheduled_check, settle_stamp, stamp_flush
from services.template_service import TemplateService
from tortoise import Tortoise
from tortoise.transactions import in_transaction
from utils.grailed_keys import sheet_keys_by_sku
from utils.submission_steps import first_step_at, last_step, merge_meta, record_step

logger = logging.getLogger(__name__)

# Grailed's day folders are named by the Eastern date; the run stamps in the file names are
# UTC (a file's Drive createdTime lands about 6 minutes after its stamp).
EASTERN = ZoneInfo("America/New_York")

# A run whose other file has not appeared after this long is read with that file missing.
RUN_PAIR_WAIT = timedelta(minutes=60)

# Runs read per check. A catch-up after downtime continues next cycle (each waiting row's
# cursor says where), so one check never holds up the flush for long.
RUNS_PER_CHECK = 24

# Top-level platform_meta key: the run_at of the last run a waiting row has seen, written
# for every row still waiting after a check. Its evidence (grailed_results) is written only
# when it changes, so its last_run_at alone would have the row re-read runs it has seen.
CURSOR_KEY = "grailed_results_cursor"


@dataclass
class _RunFiles:
    """The two files of one processor run, as found in Drive."""

    run_key: str
    date_folder: str
    success: Optional[dict[str, Any]] = None
    errors: Optional[dict[str, Any]] = None

    @property
    def run_at(self) -> datetime:
        created = [
            datetime.fromisoformat(f["createdTime"].replace("Z", "+00:00"))
            for f in (self.success, self.errors)
            if f and f.get("createdTime")
        ]
        return min(created) if created else parser.run_stamp_utc(self.run_key)


def _submission_view(sub: Any) -> Optional[grailed_results.SubmissionView]:
    meta = sub.platform_meta or {}
    # From the attempt's first `sending`: a resend after an interrupted flush whose first
    # call landed still counts from there.
    first_sending = first_step_at(meta, "sending")
    sent = meta.get("sent_skus") or []
    if not first_sending or not sent:
        return None
    external = sub.external_id if isinstance(sub.external_id, dict) else {}
    references = sheet_keys_by_sku(external.get("references"))
    return grailed_results.SubmissionView(
        id=sub.id,
        sent_skus=frozenset(sent),
        attempt_number=int(getattr(sub, "attempt_number", 1) or 1),
        already_on_sheet=frozenset(meta.get("updated_references") or []),
        first_sending_at=first_sending,
        references=references,
    )


def _sku_error_fields(sku_errors: dict[str, str]) -> tuple[str, str]:
    """(error, error_display) for per-size failures: the JSON map, and "sku: message, ..."
    cut to the column's 500 characters, which is what the dashboard and the pill show."""
    return json.dumps(sku_errors), ", ".join(f"{s}: {e}" for s, e in sku_errors.items())[:500]


def _stored_cursor(meta: dict[str, Any]) -> Optional[datetime]:
    value = meta.get(CURSOR_KEY)
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _evidence_key(ev: grailed_results.Evidence) -> dict[str, Any]:
    """The evidence without its run cursor: what is worth writing back."""
    data = ev.to_json()
    data.pop("last_run_at", None)
    return data


def _grailed_presence_rows(
    sub: Any, sub_skus: list[str], keys_by_sku: dict[str, str]
) -> list[dict[str, Any]]:
    """grailed_service.presence_rows for one submission of the flush.

    The parent sku comes off the prefetched listing, never from splitting a child
    sku: product_resolver forbids that, and a reassigned child still carries its
    original parent's prefix. No listing means no parent, so no rows.
    """
    listing = getattr(sub, "listing", None)
    parent_sku = getattr(listing, "product_id", None) if listing else None
    return presence_rows(parent_sku, sub_skus, keys_by_sku, sub.id)


class GrailedPoller(BasePoller):
    """Batches parked Grailed submissions and flushes them in chunks.

    Grailed becomes a manual_fallback platform: submissions park in PENDING and
    this poller flushes them to the AppScript ``addListings`` endpoint. The call
    is synchronous, but it only writes the sheet: with results tracking on, an
    accepted submission waits in PROCESSING/`sent` until _check_results reads
    Grailed's own result files; with it off, it is SUCCESS within the cycle.

    ``min_batch_size`` (from ``platform_settings.grailed``, default 100) is both
    the auto-flush threshold and the per-call chunk size: a scheduled cycle only
    flushes once at least that many are pending, and then sends them in chunks of
    that size (250 pending -> 100 + 100 + 50). A manual flush ignores the
    threshold and flushes whatever is pending, still chunked at that size.
    """

    def __init__(self) -> None:
        super().__init__(config_section="grailed_poller", name="GrailedPoller")
        cfg = config.get("grailed_poller", {})
        # A row still `submitting` this long was abandoned by a flush that died. It is
        # requeued (see _requeue_interrupted), so the cost of the timeout being too
        # short is a duplicate AppScript call that dedups, not a wrong status. A row
        # the AppScript accepted (`sent`) is waiting on Grailed and is never swept.
        self.stale_timeout_minutes: int = cfg.get("stale_processing_timeout_minutes", 60)
        # Results tracking (docs/grailed_integration.md). Both switches fail closed:
        # with results_enabled false an accepted row is success at once, as before.
        self.results_enabled: bool = bool(cfg.get("results_enabled", False))
        self.results_dry_run: bool = bool(cfg.get("results_dry_run", True))
        self.results_root: str = cfg.get("results_root_folder_id", "") or ""
        self.results_interval = timedelta(minutes=int(cfg.get("results_interval_minutes", 15)))
        self.results_grace = timedelta(minutes=int(cfg.get("results_grace_minutes", 30)))
        self.drive = grailed_drive
        self._last_results_check: Optional[datetime] = None
        # Dry run writes no evidence or cursor, so it keeps the evidence of every row of the
        # last check here, its last_run_at serving as the cursor (lost on restart, which only
        # means re-reading runs), and logs each would-be decision once.
        self._dry_run_evidence: dict[int, grailed_results.Evidence] = {}
        self._dry_run_logged: set[tuple[int, str]] = set()
        # Files seen in each day's Other folder, so a new one is logged once.
        self._other_counts: dict[str, int] = {}

    async def stop(self) -> None:
        await super().stop()
        await self.drive.close()

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
        await run_scheduled_check(
            "grailed", count_ready=self._count_pending, flush=self._scheduled_flush
        )
        await self._batch_upload_pending(force=False)
        await self._check_results()

    @staticmethod
    async def _count_pending() -> int:
        return await ListingSubmission.filter(
            platform_id="grailed", status=SubmissionStatus.PENDING
        ).count()

    async def _scheduled_flush(self) -> int:
        # force=True, exactly as Submit Pending, still chunked at min_batch_size.
        result = await self._batch_upload_pending(force=True)
        return int(result.get("submission_count") or 0)

    async def _recover_stale_processing(self) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=self.stale_timeout_minutes)
        stale = await ListingSubmission.filter(
            platform_id="grailed",
            status=SubmissionStatus.PROCESSING,
            platform_status=GrailedStage.SUBMITTING,
            updated_at__lt=cutoff,
        ).all()

        if stale:
            logger.warning(
                f"{self.name}: {len(stale)} stale processing submission(s), requeueing"
            )
            await self._requeue_interrupted(stale, cause="no progress in "
                                            f"{self.stale_timeout_minutes} minutes")

    async def _requeue_interrupted(
        self, subs: list[ListingSubmission], *, cause: str
    ) -> None:
        """Hand interrupted rows back to PENDING, saying how far each one got.

        Requeued, not failed, in both cases. These used to be failed with "verify
        on Grailed before resubmitting" on the theory that the AppScript call may
        have landed, but submit_batch already relies on addListings deduping by
        SKU to retry a call with no definitive answer, and the same property makes
        a resend of an interrupted chunk an update rather than a duplicate. A
        failed row cost an operator a manual check and a resubmit for nothing.

        The `sending` step (recorded immediately before the AppScript call) is what
        says whether the call was made; `submitting` alone means the chunk never
        got that far. A last step of `sent` means the call was made and accepted but
        the save that moves the row on did not happen.
        """
        # Only rows still being sent. A row the AppScript accepted waits on Grailed's
        # result file in `sent` and must never be resent from here.
        subs = [s for s in subs if s.platform_status == GrailedStage.SUBMITTING]
        made_call = [s.id for s in subs if last_step(s) in ("sending", "sent")]
        not_sent = [s.id for s in subs if last_step(s) not in ("sending", "sent")]
        for ids, reason in (
            (made_call, f"interrupted after the AppScript call was made ({cause}); "
                        "addListings dedups by SKU so a resend updates in place"),
            (not_sent, f"interrupted before the AppScript call ({cause}); nothing was sent"),
        ):
            if not ids:
                continue
            await ListingSubmission.filter(
                id__in=ids,
                status=SubmissionStatus.PROCESSING,
                platform_status=GrailedStage.SUBMITTING,
            ).update(
                status=SubmissionStatus.PENDING,
                platform_status=None,
                error_display="Grailed batch interrupted, will retry",
            )
            await record_step(ids, "requeued", stage="submitting", reason=reason)

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
                # platform_status kept in step with SPO's, so the dashboard's
                # granular column is populated for Grailed too rather than null.
                .update(
                    status=SubmissionStatus.PROCESSING,
                    platform_status=GrailedStage.SUBMITTING,
                )
            )

        # The flush clock (services/scheduled_flush.py): stamped once the claim has committed,
        # and put back in the finally if every claimed row ends in pending again, as it does
        # when the template fetch fails or the AppScript is unreachable for every chunk.
        stamp = await stamp_flush("grailed")
        try:
            return await self._flush_claimed(submission_ids, batch_size)
        finally:
            await settle_stamp(stamp, submission_ids)

    async def _flush_claimed(
        self, submission_ids: list[int], batch_size: int
    ) -> dict[str, Any]:
        """Send rows _batch_upload_pending has already claimed, in chunks of batch_size."""
        logger.info(
            f"{self.name}: batch submitting {len(submission_ids)} Grailed submissions "
            f"in chunks of {batch_size}"
        )

        # Recorded outside the transaction above: record_step uses its own
        # connection and would block on the rows that transaction has locked.
        await record_step(
            submission_ids,
            "submitting",
            batch_size=len(submission_ids),
            chunk_size=batch_size,
        )

        try:
            template = await TemplateService.get_template_by_id("default")
        except Exception:
            # Nothing has been sent; every claimed row goes back.
            logger.exception(f"{self.name}: template fetch failed, requeueing batch")
            subs = await ListingSubmission.filter(id__in=submission_ids)
            await self._requeue_interrupted(subs, cause="template fetch failed")
            raise
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
        """One chunk, isolated: a crash in this chunk must not abandon the next.

        An unexpected exception (a DB write failing mid-attribution, say) used to
        propagate out of the chunk loop, so chunks two onward were never sent and
        every row of theirs sat in PROCESSING until the stale sweep. The rows of
        THIS chunk that are still PROCESSING are requeued the same way the sweep
        would, and the loop carries on.
        """
        try:
            return await self._run_chunk(chunk_ids, field_definitions)
        except Exception as e:
            logger.exception(f"{self.name}: chunk of {len(chunk_ids)} aborted")
            live = await ListingSubmission.filter(
                id__in=chunk_ids,
                status=SubmissionStatus.PROCESSING,
                platform_status=GrailedStage.SUBMITTING,
            )
            await self._requeue_interrupted(
                live, cause=f"{type(e).__name__}: {str(e)[:200]}"
            )
            return 0

    async def _run_chunk(
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
                await sub.save(update_fields=["status", "error_display", "updated_at"])
                await record_step(
                    sub.id, "failed", stage="build_products", reason="listing not found"
                )
                continue
            try:
                products = await grailed_service.build_csv_rows(
                    listing, listing.data or {}, field_definitions
                )
                if not products:
                    raise ValueError("No children found to submit to Grailed")
            except Exception as e:
                # A build error fails only this submission; the rest of the chunk
                # still goes out.
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
                continue

            active_ids.append(sub.id)
            for product in products:
                sku = product.get("sku")
                if sku:
                    sku_to_sub[sku] = sub.id
            all_products.extend(products)

        if not all_products or not active_ids:
            return 0

        # The commitment marker, written by nothing else: a row whose last step is
        # `sending` got as far as the call. _requeue_interrupted reads it.
        await record_step(
            active_ids, "sending", rows=len(all_products), submissions=len(active_ids)
        )

        try:
            response_data = await grailed_service.submit_batch(all_products)
        except Exception:
            # submit_batch already retried the AppScript in-call; reaching here
            # means no definitive response. The outcome is unknown, but addListings
            # dedups by SKU, so we requeue (not fail) rather than lose the batch:
            # the next poll cycle retries, and a request that actually landed
            # becomes an update, not a duplicate. No batch number is consumed.
            logger.exception(
                f"{self.name}: Grailed AppScript unreachable, requeueing {len(active_ids)} submissions"
            )
            await ListingSubmission.filter(id__in=active_ids).update(
                status=SubmissionStatus.PENDING,
                platform_status=None,
                error_display="Grailed AppScript unreachable, will retry",
            )
            await record_step(
                active_ids,
                "requeued",
                stage="appscript",
                reason="AppScript unreachable, no definitive response",
            )
            return 0

        # Definitive response -> assign this batch its number. product_import_id
        # stays the internal routing key (min(active_ids), unique per chunk);
        # batch_number is the user-facing sequential 1,2,3... the dashboard shows
        # zero-padded (000001). Pulled only now so a requeued chunk leaves no gap.
        batch_meta = {
            "product_import_id": min(active_ids),
            "batch_number": await self._next_batch_number(),
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
        }

        if not response_data.get("success"):
            error_msg = response_data.get("error", "Unknown error from Grailed AppScript")
            await ListingSubmission.filter(id__in=active_ids).update(
                status=SubmissionStatus.FAILED,
                error=error_msg,
                error_display=str(error_msg)[:500],
            )
            # Merged via record_step rather than assigned: a wholesale
            # platform_meta write discards the step history on the row.
            await record_step(
                active_ids,
                "failed",
                meta=batch_meta,
                stage="appscript",
                reason=str(error_msg)[:300],
                batch_number=batch_meta["batch_number"],
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
        ref_by_sku = sheet_keys_by_sku(response_data.get("added_references"))

        error_by_sku: dict[str, str] = {}
        for failure in response_data.get("failures", []) or []:
            if isinstance(failure, dict) and failure.get("sku"):
                error_by_sku[failure["sku"]] = failure.get("error", "Unknown error")

        # SKUs that were already on the sheet and got refreshed in place (no new
        # row). AppScript returns these as [{"sku": ...}]; we persist them per
        # submission so "added vs updated" is stored, not just derivable.
        updated_sku_set: set[str] = set()
        for entry in response_data.get("updated_references", []) or []:
            if isinstance(entry, dict) and entry.get("sku"):
                updated_sku_set.add(entry["sku"])
            elif isinstance(entry, str):
                updated_sku_set.add(entry)

        succeeded = 0
        for sub_id in active_ids:
            sub = subs_by_id[sub_id]
            sub_skus = [sku for sku, sid in sku_to_sub.items() if sid == sub_id]
            sub_updated = [sku for sku in sub_skus if sku in updated_sku_set]

            failed_skus = {sku: error_by_sku[sku] for sku in sub_skus if sku in error_by_sku}
            if failed_skus:
                sub.status = SubmissionStatus.FAILED
                sub.error, sub.error_display = _sku_error_fields(failed_skus)
                meta = {**batch_meta, "sku_errors": failed_skus}
                if sub_updated:
                    meta["updated_references"] = sub_updated
                await sub.save(
                    update_fields=["status", "error", "error_display", "updated_at"]
                )
                await record_step(
                    sub.id,
                    "failed",
                    meta=meta,
                    stage="appscript",
                    sku_errors=failed_skus,
                    batch_number=batch_meta["batch_number"],
                )
                continue

            sub_refs = [ref_by_sku[sku] for sku in sub_skus if sku in ref_by_sku]
            meta = {**batch_meta, "sent_skus": sub_skus}
            if sub_updated:
                meta["updated_references"] = sub_updated
            # The step before the save: it carries sent_skus, which the results check needs
            # to decide a row in `sent`. A failure between the two then leaves the row
            # `submitting`, which the chunk handler or the stale sweep requeues (the resend
            # updates in place); the other order could strand a row in `sent` for good.
            await record_step(
                sub.id,
                "sent",
                meta=meta,
                batch_number=batch_meta["batch_number"],
                references=sub_refs or None,
                updated_references=sub_updated or None,
            )
            # One object shape for Grailed external_id: the sheet keys now, and the
            # Grailed listing ids once a result file reports them (item_ids).
            sub.external_id = {"references": sub_refs}
            sub.error_display = None  # clear any "will retry" note from a prior cycle
            if self.results_enabled:
                # Accepted onto the sheet, not yet on Grailed. Stays in progress until
                # the results check reads Grailed's own result file; the presence row is
                # written then, on success only, so a failure can be fixed and resubmitted.
                sub.platform_status = GrailedStage.SENT
            else:
                sub.status = SubmissionStatus.SUCCESS
                sub.platform_status = GrailedStage.LISTED
            # status and platform_status in one save, so a row is never success-and-sent.
            await sub.save(
                update_fields=[
                    "status",
                    "platform_status",
                    "error_display",
                    "external_id",
                    "updated_at",
                ]
            )
            if self.results_enabled:
                succeeded += 1
                continue

            await record_step(sub.id, "listed", batch_number=batch_meta["batch_number"])
            # Presence, recorded beside the reference list rather than derived
            # from it later. It is sub_skus and not sub_refs because sub_refs
            # holds only added_references: a submission whose children were ALL
            # already on the sheet has no references, and that is exactly the
            # parent this feature exists to remember.
            #
            # The parent row is what gates: a success means every child landed
            # (a partial failure fails the whole submission, above), so the
            # parent is fully covered and cannot read as partial.
            await ExternalListingService.record(
                "grailed",
                _grailed_presence_rows(sub, sub_skus, ref_by_sku),
            )
            succeeded += 1

        logger.info(
            f"{self.name}: Grailed batch done, added={response_data.get('added', 0)}, "
            f"updated={response_data.get('updated', 0)}, failed={response_data.get('failed', 0)}, "
            f"submissions_succeeded={succeeded}/{len(active_ids)}"
        )
        return succeeded

    # ------------------------------------------------------------- results check

    async def _check_results(self) -> None:
        """Settle submissions waiting in `sent` from Grailed's processor runs in Drive.

        Gated by results_enabled and results_interval. A transient Drive error leaves the
        gate where it was, so the next cycle retries; a permanent one (the root not shared,
        say) is logged once per interval. A check that stopped at RUNS_PER_CHECK continues next
        cycle. Never raises: the flush above must not depend on Drive.
        """
        if not self.results_enabled:
            return
        now = datetime.now(timezone.utc)
        if self._last_results_check and now - self._last_results_check < self.results_interval:
            return
        if not self.results_root:
            logger.error(f"{self.name}: results_enabled but results_root_folder_id is empty")
            self._last_results_check = now
            return
        try:
            waiting = await ListingSubmission.filter(
                platform_id="grailed",
                status=SubmissionStatus.PROCESSING,
                platform_status=GrailedStage.SENT,
            )
            more = await self._settle_from_results(waiting, now) if waiting else False
            self._last_results_check = None if more else now
        except GrailedDriveTransientError as e:
            logger.warning(f"{self.name}: Grailed Drive unavailable, will retry: {e}")
        except GrailedDrivePermanentError as e:
            logger.error(f"{self.name}: Grailed Drive refused the results check: {e}")
            self._last_results_check = now
        except Exception:
            logger.exception(f"{self.name}: Grailed results check failed")
            self._last_results_check = now

    async def _settle_from_results(self, waiting: list[ListingSubmission], now: datetime) -> bool:
        """Apply new runs to the waiting rows. True when RUNS_PER_CHECK stopped it early.

        Each row reads from its own cursor: the last run it has seen, else its first
        `sending`. A row that first sent before runs already applied to the others (one
        requeued after a call that landed, then resent) still gets those runs, whose
        one-time creates may be all it ever gets. apply_run ignores a run at or before a
        row's last_run_at, so a run read again for another row changes nothing for it.
        """
        views: dict[int, grailed_results.SubmissionView] = {}
        evidence: dict[int, grailed_results.Evidence] = {}
        for sub in waiting:
            view = _submission_view(sub)
            if view is None:
                # Parked in `sent` without the facts to decide it (no sent sizes or no
                # `sending` step). Nothing a result file can settle; say so, once a check.
                logger.warning(f"{self.name}: submission {sub.id} waits in sent but has no "
                               "sent_skus or sending step; cannot match it to results")
                continue
            views[sub.id] = view
            if self.results_dry_run:
                # Never the stored evidence: dry run may not rely on what it may not write.
                evidence[sub.id] = self._dry_run_evidence.get(sub.id) or grailed_results.Evidence()
                continue
            meta = sub.platform_meta or {}
            ev = grailed_results.Evidence.from_json(meta.get("grailed_results"))
            cursor = _stored_cursor(meta)
            if cursor and (ev.last_run_at is None or cursor > ev.last_run_at):
                # Runs up to the cursor were applied and changed nothing worth writing.
                ev.last_run_at = cursor
            evidence[sub.id] = ev
        if self.results_dry_run:
            self._dry_run_evidence = {i: e for i, e in self._dry_run_evidence.items() if i in views}
            self._dry_run_logged = {k for k in self._dry_run_logged if k[0] in views}
        if not views:
            return False
        subs = {s.id: s for s in waiting}

        cursors = {
            i: max(evidence[i].last_run_at or v.first_sending_at, v.first_sending_at)
            for i, v in views.items()
        }
        since = min(cursors.values())
        runs = sorted(
            (r for r in await self._discover_runs(since, now) if r.run_at > since),
            key=lambda r: r.run_at,
        )
        more = len(runs) > RUNS_PER_CHECK
        runs = runs[:RUNS_PER_CHECK]

        # The ledger is a record of the runs read, not a filter: a run is read again for a
        # row whose cursor is behind it, and recorded once.
        ledgered: set[str] = set()
        if not self.results_dry_run and runs:
            ledgered = set(
                await GrailedResultRun.filter(
                    run_key__in=[r.run_key for r in runs]
                ).values_list("run_key", flat=True)
            )

        last_at: Optional[datetime] = None
        for files in runs:
            if not views:
                break
            run, detail = await self._read_run(files)
            if run is not None:
                await self._apply_run(run, views, evidence, subs)
            last_at = files.run_at
            if not self.results_dry_run and files.run_key not in ledgered:
                await self._ledger(files, run, detail)
                ledgered.add(files.run_key)

        if self.results_dry_run:
            # Every row of this check, decided ones too: a decided row stays in `sent` in a
            # dry run, and left at its decision run it would hold `since` back next check.
            if last_at is not None:
                for i, ev in evidence.items():
                    if ev.last_run_at is None or ev.last_run_at < last_at:
                        evidence[i] = replace(ev, last_run_at=last_at)
            self._dry_run_evidence = dict(evidence)
        elif last_at is not None:
            # Every row still waiting has now seen every run read. A row whose cursor was
            # already past the last run read keeps its own.
            behind = [i for i in views if cursors[i] < last_at]
            if behind:
                await merge_meta(behind, {CURSOR_KEY: last_at.isoformat()})
        return more

    @staticmethod
    async def _ledger(
        files: _RunFiles, run: Optional[grailed_results.Run], detail: Optional[str]
    ) -> None:
        """One grailed_result_runs row: processed, partial (the error file could not be
        used, so its errors were read as unknown) or unreadable (the run was dropped)."""
        if run is None:
            status = "unreadable"
        elif detail:
            status = "partial"
        else:
            status = "processed"
        await GrailedResultRun.create(
            run_key=files.run_key,
            run_at=files.run_at,
            success_file_id=(files.success or {}).get("id"),
            errors_file_id=(files.errors or {}).get("id"),
            success_rows=len(run.successes) if run is not None and files.success else None,
            error_rows=(
                len(run.errors or ())
                if status == "processed" and files.errors
                else None
            ),
            date_folder=files.date_folder,
            status=status,
            detail=detail,
        )

    async def _discover_runs(self, since: datetime, now: datetime) -> list[_RunFiles]:
        """Every run under the root from one Eastern day before `since`, found by listing:
        year, month and day folder names are parsed, never computed, so a new month or year
        is picked up with no config change. A run with only one of its files is left for a
        later check until RUN_PAIR_WAIT has passed, then read with the other one missing."""
        start: date = since.astimezone(EASTERN).date() - timedelta(days=1)
        found: dict[str, _RunFiles] = {}
        for year_folder in await self._folders(self.results_root):
            year = parser.parse_year_folder(year_folder["name"])
            if year is None or year < start.year:
                continue
            for month_folder in await self._folders(year_folder["id"]):
                month = parser.parse_month_folder(month_folder["name"])
                if month is None or (year, month) < (start.year, start.month):
                    continue
                for day_folder in await self._folders(month_folder["id"]):
                    day = parser.parse_date_folder(day_folder["name"])
                    if day is None or day < start:
                        continue
                    await self._collect_day(day_folder, found)

        ready = []
        for files in found.values():
            if files.success and files.errors:
                ready.append(files)
            elif now - parser.run_stamp_utc(files.run_key) >= RUN_PAIR_WAIT:
                ready.append(files)
        return ready

    async def _folders(self, folder_id: str) -> list[dict[str, Any]]:
        return [f for f in await self.drive.list_children(folder_id)
                if f.get("mimeType") == FOLDER_MIME]

    async def _collect_day(self, day_folder: dict[str, Any], found: dict[str, _RunFiles]) -> None:
        for sub in await self._folders(day_folder["id"]):
            name = (sub.get("name") or "").strip().lower()
            if name == "other":
                count = len(await self.drive.list_children(sub["id"]))
                if count and self._other_counts.get(sub["id"]) != count:
                    logger.info(f"{self.name}: {count} file(s) in Other under "
                                f"{day_folder['name']}; Other is not read")
                self._other_counts[sub["id"]] = count
                continue
            if name not in ("success", "errors"):
                continue
            for f in await self.drive.list_children(sub["id"]):
                parsed = parser.parse_run_name(f.get("name") or "")
                if not parsed:
                    continue
                run_key, kind = parsed
                files = found.setdefault(run_key, _RunFiles(run_key, day_folder["name"]))
                if kind == "successes":
                    files.success = f
                else:
                    files.errors = f

    async def _read_run(
        self, files: _RunFiles
    ) -> tuple[Optional[grailed_results.Run], Optional[str]]:
        """(run, None) for a run read whole. (run, reason) when only the error file cannot be
        used: its errors are unknown (R11), and the success file's one-time events still
        count. (None, reason) when the success file cannot be used, which drops the run. A
        transient Drive error propagates, so the run is read again next check."""
        async def fetch(f):
            if not f:
                return None
            try:
                return await self.drive.read_file(f["id"], f.get("mimeType") or "")
            except GrailedDrivePermanentError as e:
                return e

        fetched = await asyncio.gather(fetch(files.success), fetch(files.errors))
        parsed: dict[str, Optional[parser.ParsedFile]] = {"successes": None, "errors": None}
        partial: Optional[str] = None
        for (kind, f), got in zip((("successes", files.success), ("errors", files.errors)), fetched):
            if not f:
                continue
            try:
                if isinstance(got, GrailedDrivePermanentError):
                    raise got
                data, fmt = got
                result = parser.parse_run_file(data, fmt, kind, f.get("name") or "")
            except (GrailedDrivePermanentError, parser.ResultParseError) as e:
                reason = f"{kind}: {e}"[:500]
                if kind == "successes":
                    return None, reason
                logger.warning(f"{self.name}: run {files.run_key} error file unusable, its "
                               f"errors read as unknown: {e}")
                partial = reason
                continue
            if result.rows and result.unknown_count == len(result.rows) and kind == "successes":
                # Every message unrecognized: most likely Grailed changed its wording.
                logger.error(f"{self.name}: run {files.run_key} has {len(result.rows)} rows "
                             "and no recognized message; Grailed's wording may have changed")
                return None, "no recognized message in the success file"
            if result.unknown_count:
                logger.warning(f"{self.name}: run {files.run_key} {kind}: "
                               f"{result.unknown_count} unrecognized message(s)")
            parsed[kind] = result
        successes = parsed["successes"]
        errors = parsed["errors"]
        return (
            grailed_results.Run(
                run_key=files.run_key,
                run_at=files.run_at,
                successes=tuple(successes.rows) if successes else (),
                # Missing, unusable or empty: unknown, decides nothing and clears nothing (R11).
                errors=None if errors is None or errors.empty else tuple(errors.rows),
            ),
            partial,
        )

    async def _apply_run(
        self,
        run: grailed_results.Run,
        views: dict[int, grailed_results.SubmissionView],
        evidence: dict[int, grailed_results.Evidence],
        subs: dict[int, ListingSubmission],
    ) -> None:
        by_sku_success: dict[str, list] = defaultdict(list)
        by_sku_error: dict[str, list] = defaultdict(list)
        for row in run.successes:
            by_sku_success[row.sku].append(row)
        for row in run.errors or ():
            by_sku_error[row.sku].append(row)

        for sub_id in list(views):
            view = views[sub_id]
            # Only this submission's sizes, so each apply reads a handful of rows.
            mine = grailed_results.Run(
                run_key=run.run_key,
                run_at=run.run_at,
                successes=tuple(r for sku in view.sent_skus for r in by_sku_success.get(sku, ())),
                errors=None if run.errors is None else tuple(
                    r for sku in view.sent_skus for r in by_sku_error.get(sku, ())
                ),
            )
            before = evidence[sub_id]
            after, decision = grailed_results.apply_run(before, view, mine, self.results_grace)
            evidence[sub_id] = after
            if decision is not None:
                await self._write_decision(subs[sub_id], view, after, decision)
                del views[sub_id]
            elif not self.results_dry_run and _evidence_key(after) != _evidence_key(before):
                await merge_meta(sub_id, {"grailed_results": after.to_json()})

    async def _write_decision(
        self,
        sub: ListingSubmission,
        view: grailed_results.SubmissionView,
        ev: grailed_results.Evidence,
        decision: grailed_results.Decision,
    ) -> None:
        if self.results_dry_run:
            if (sub.id, decision.outcome) not in self._dry_run_logged:
                self._dry_run_logged.add((sub.id, decision.outcome))
                logger.info(
                    f"{self.name}: DRY RUN submission {sub.id} would be {decision.outcome} at "
                    f"run {decision.run_key}: ids={decision.ids} errors={decision.sku_errors}"
                )
            return

        external = dict(sub.external_id) if isinstance(sub.external_id, dict) else {
            "references": list(sub.external_id or [])
        }
        if decision.ids:
            external["item_ids"] = decision.ids
        meta = {"grailed_results": ev.to_json()}

        if decision.outcome == grailed_results.SUCCESS:
            moved = await ListingSubmission.filter(
                id=sub.id, status=SubmissionStatus.PROCESSING, platform_status=GrailedStage.SENT
            ).update(
                status=SubmissionStatus.SUCCESS,
                platform_status=GrailedStage.LISTED,
                error_display=None,
                external_id=external,
            )
            if not moved:
                return
            await record_step(
                sub.id, "listed", meta=meta, at=decision.listed_at,
                run_key=decision.run_key, grailed_ids=decision.ids or None,
            )
            # Presence on success only. Each child's id is its Grailed listing id when a
            # result reported one, else the sheet key sent for it; a size with neither
            # sends NULL and keeps whatever id is stored.
            ids_by_sku = {**view.references, **decision.ids}
            listing = await Listing.filter(id=sub.listing_id).only("id", "product_id").first()
            await ExternalListingService.record(
                "grailed",
                presence_rows(
                    listing.product_id if listing else None,
                    sorted(view.sent_skus), ids_by_sku, sub.id,
                ),
            )
            logger.info(f"{self.name}: submission {sub.id} listed on Grailed at run "
                        f"{decision.run_key} ({len(decision.ids)} id(s))")
            return

        error, error_display = _sku_error_fields(decision.sku_errors)
        moved = await ListingSubmission.filter(
            id=sub.id, status=SubmissionStatus.PROCESSING, platform_status=GrailedStage.SENT
        ).update(
            status=SubmissionStatus.FAILED,
            error=error,
            error_display=error_display,
            external_id=external,
        )
        if not moved:
            return
        await record_step(
            sub.id, "failed", meta={**meta, "sku_errors": decision.sku_errors},
            stage="grailed_processing", run_key=decision.run_key,
            sku_errors=decision.sku_errors,
        )
        logger.info(f"{self.name}: submission {sub.id} rejected by Grailed at run "
                    f"{decision.run_key}: {error_display}")

    @staticmethod
    async def _next_batch_number() -> int:
        """Next sequential grailed batch number (1, 2, 3, ...) from a DB sequence,
        so concurrent flushes never collide."""
        conn = Tortoise.get_connection("default")
        rows = await conn.execute_query_dict("SELECT nextval('grailed_batch_seq') AS n")
        return int(rows[0]["n"])

    async def manual_flush(self) -> dict[str, Any]:
        return await self._batch_upload_pending(force=True)


grailed_poller = GrailedPoller()
