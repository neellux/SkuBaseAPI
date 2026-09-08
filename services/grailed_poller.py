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
from services.external_listing_service import ExternalListingService
from services.grailed_service import grailed_service
from services.template_service import TemplateService
from tortoise import Tortoise
from tortoise.transactions import in_transaction
from utils.submission_steps import last_step, record_step

logger = logging.getLogger(__name__)


def _grailed_presence_rows(sub: Any, sub_skus: list[str]) -> list[dict[str, Any]]:
    """A parent row plus one child row per sku that went to the sheet.

    The parent sku comes off the prefetched listing, never from splitting a child
    sku: product_resolver forbids that, and a reassigned child still carries its
    original parent's prefix. Without a listing there is no parent to assert, so
    the child rows are dropped too rather than guessed at.
    """
    listing = getattr(sub, "listing", None)
    parent_sku = getattr(listing, "product_id", None) if listing else None
    if not parent_sku:
        return []
    rows: list[dict[str, Any]] = [
        {"level": "parent", "sku": parent_sku, "parent_sku": parent_sku,
         "external_meta": {"submission_id": sub.id}}
    ]
    rows.extend(
        {"level": "child", "sku": sku, "parent_sku": parent_sku}
        for sku in sub_skus
        if sku
    )
    return rows


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
        # A row in PROCESSING this long was abandoned by a flush that died. It is
        # requeued (see _requeue_interrupted), so the cost of the timeout being too
        # short is a duplicate AppScript call that dedups, not a wrong status.
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
        got that far.
        """
        made_call = [s.id for s in subs if last_step(s) == "sending"]
        not_sent = [s.id for s in subs if last_step(s) != "sending"]
        for ids, reason in (
            (made_call, f"interrupted after the AppScript call was made ({cause}); "
                        "addListings dedups by SKU so a resend updates in place"),
            (not_sent, f"interrupted before the AppScript call ({cause}); nothing was sent"),
        ):
            if not ids:
                continue
            await ListingSubmission.filter(
                id__in=ids, status=SubmissionStatus.PROCESSING
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
                    platform_status="submitting",
                )
            )

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
                id__in=chunk_ids, status=SubmissionStatus.PROCESSING
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
        ref_by_sku: dict[str, str] = {}
        for ref in response_data.get("added_references", []) or []:
            if isinstance(ref, str):
                ref_by_sku[ref.rsplit("_", 1)[0] if "_" in ref else ref] = ref

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
                sub.error = json.dumps(failed_skus)
                sub.error_display = ", ".join(f"{s}: {e}" for s, e in failed_skus.items())[:500]
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
            sub.status = SubmissionStatus.SUCCESS
            sub.platform_status = "listed"
            sub.error_display = None  # clear any "will retry" note from a prior cycle
            meta = dict(batch_meta)
            if sub_updated:
                meta["updated_references"] = sub_updated
            if sub_refs:
                sub.external_id = sub_refs
            await sub.save(
                update_fields=[
                    "status",
                    "platform_status",
                    "error_display",
                    "external_id",
                    "updated_at",
                ]
            )
            await record_step(
                sub.id,
                "listed",
                meta=meta,
                batch_number=batch_meta["batch_number"],
                references=sub_refs or None,
                updated_references=sub_updated or None,
            )
            # Presence, recorded beside the reference list rather than derived
            # from it later. Two reasons it is sub_skus and not sub_refs:
            #
            #   1. sub_refs holds only added_references. A submission whose
            #      children were ALL already on the sheet succeeds with
            #      external_id left NULL by the `if sub_refs:` above, and that is
            #      exactly the parent this feature exists to remember.
            #   2. a Grailed reference is "<sku>_<MMDDYYYY>", our own sku plus a
            #      batch date. It is a sheet row key, not a Grailed id, so there
            #      is nothing to store in external_id and it stays NULL.
            #
            # The parent row is what gates: a success means every child landed
            # (a partial failure fails the whole submission, above), so the
            # parent is fully covered and cannot read as partial.
            await ExternalListingService.record(
                "grailed",
                _grailed_presence_rows(sub, sub_skus),
            )
            succeeded += 1

        logger.info(
            f"{self.name}: Grailed batch done, added={response_data.get('added', 0)}, "
            f"updated={response_data.get('updated', 0)}, failed={response_data.get('failed', 0)}, "
            f"submissions_succeeded={succeeded}/{len(active_ids)}"
        )
        return succeeded

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
