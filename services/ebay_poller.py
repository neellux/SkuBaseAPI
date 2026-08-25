"""Batch eBay item specifics into one SellerCloud import.

Mirrors grailed_poller: claim the pending rows under a lock, flip them to PROCESSING,
build one file for the whole batch, post it, then mark each row.

Unlike Grailed and SPO this has NO scheduled cycle yet. It is driven only by
POST /submissions/create_batch?platform=ebay, so an import happens when a person asks for
one rather than on a timer. Clicking submit sends, matching what that button already means
for SPO and Grailed; neither of those has a preview either.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from tortoise import Tortoise
from tortoise.transactions import in_transaction

from decimal import Decimal

from models.db_models import AppSettings, ListingSubmission, SubmissionStatus
from services.ebay_service import ebay_service, render_tsv
from utils.submission_steps import record_step

logger = logging.getLogger(__name__)


class EbayPoller:
    name = "ebay_poller"
    PLATFORM_ID = "ebay"

    async def recover_stale_processing(self, stale_minutes: int = 30) -> dict[str, int]:
        """Un-strand rows a killed flush left in PROCESSING.

        A crash between claiming the rows and posting leaves them PROCESSING with no
        `submitting` step, because that step is written immediately before the POST and by
        nothing else. Those provably never reached SellerCloud, so they go back to PENDING.

        A row that DOES carry the step got as far as the call, and whether SellerCloud
        received it is unknowable from here, so it is failed for review instead of retried.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=stale_minutes)
        stale = await ListingSubmission.filter(
            platform_id=self.PLATFORM_ID,
            status=SubmissionStatus.PROCESSING,
            updated_at__lt=cutoff,
        ).all()

        requeued = failed = 0
        for sub in stale:
            steps = (sub.platform_meta or {}).get("steps") or []
            if any(st.get("step") == "submitting" for st in steps):
                sub.status = SubmissionStatus.FAILED
                sub.error_display = "Import interrupted - check SellerCloud before resubmitting"
                await sub.save(update_fields=["status", "error_display", "updated_at"])
                await record_step(
                    sub.id, "failed", stage="stale_processing",
                    reason="interrupted after the import was sent; may already be in SellerCloud",
                )
                failed += 1
            else:
                sub.status = SubmissionStatus.PENDING
                sub.platform_status = None
                await sub.save(update_fields=["status", "platform_status", "updated_at"])
                await record_step(
                    sub.id, "pending", stage="stale_processing",
                    reason="interrupted before the import was sent; safe to retry",
                )
                requeued += 1

        if requeued or failed:
            logger.info("%s: recovered %d stale row(s): %d requeued, %d failed",
                        self.name, len(stale), requeued, failed)
        return {"requeued": requeued, "failed": failed}

    @staticmethod
    async def _next_batch_number() -> int:
        """Next eBay batch id, from a sequence so concurrent flushes cannot collide.

        Same mechanism as grailed_batch_seq. It exists because the batch needs an id BEFORE
        any SellerCloud call: the export alone takes about a minute, and the queued job ids
        that eventually come back belong to individual steps, not to the batch.
        """
        conn = Tortoise.get_connection("default")
        rows = await conn.execute_query_dict("SELECT nextval('ebay_batch_seq') AS n")
        return int(rows[0]["n"])

    async def begin_batch(self) -> tuple[int | None, list[int]]:
        """Claim the pending rows and stamp them with an import id. Database only.

        Deliberately separate from the work: this is fast enough to run inside the request,
        so the dashboard has an import row the moment Submit is clicked rather than a minute
        later. run_batch then does the four SellerCloud round trips in the background.
        """
        async with in_transaction("default") as conn:
            pending = await (
                ListingSubmission.filter(
                    platform_id=self.PLATFORM_ID, status=SubmissionStatus.PENDING
                )
                .select_for_update(skip_locked=True)
                .using_db(conn)
            )
            if not pending:
                return None, []
            submission_ids = [s.id for s in pending]
            await (
                ListingSubmission.filter(id__in=submission_ids)
                .using_db(conn)
                .update(
                    status=SubmissionStatus.PROCESSING,
                    platform_status="submitting",
                )
            )

        import_id = await self._next_batch_number()
        # product_import_id up front, so the import appears immediately. The per-step
        # SellerCloud job ids are added to ebay_jobs as each one returns.
        await record_step(
            submission_ids,
            "queued_batch",
            meta={"product_import_id": import_id},
            submissions=len(submission_ids),
        )
        logger.info("%s: batch %s claimed %d submission(s)",
                    self.name, import_id, len(submission_ids))
        return import_id, submission_ids

    async def run_batch(self, import_id: int, submission_ids: list[int]) -> dict[str, Any]:
        """The four SellerCloud round trips, for rows begin_batch already claimed."""
        try:
            return await self._submit_batch(submission_ids)
        except Exception:
            logger.exception("%s: batch %s failed", self.name, import_id)
            raise

    async def manual_flush(self) -> dict[str, Any]:
        """Claim and run in one call. Used where a caller wants to block on the result."""
        import_id, submission_ids = await self.begin_batch()
        if not submission_ids:
            return {"submission_count": 0, "rows": 0, "sent": False}
        return await self.run_batch(import_id, submission_ids)

    @staticmethod
    async def _stage(
        submission_ids: list[int], step: str, jobs: dict[str, Any], **details: Any
    ) -> None:
        """Record one stage, carrying every job id collected so far.

        The whole map is rewritten each time, not merged in SQL: record_step's `meta` does a
        shallow `platform_meta || $2`, so a nested key like ebay_jobs would be REPLACED by a
        partial one rather than merged. Passing the accumulated dict keeps it complete.

        Written twice on purpose, the way spo_poller does it: into `meta` for the top level
        the dashboard reads, and onto the step entry so the history says which ids belonged
        to which stage rather than only where things ended up.
        """
        await record_step(
            submission_ids,
            step,
            meta={"ebay_jobs": dict(jobs)} if jobs else None,
            **{k: v for k, v in details.items() if v is not None},
        )

    async def _submit_batch(self, submission_ids: list[int]) -> dict[str, Any]:
        """One import file for these submissions."""
        submissions = await ListingSubmission.filter(
            id__in=submission_ids
        ).prefetch_related("listing")

        settings = await AppSettings.first()
        ebay_settings = ((settings.platform_settings if settings else None) or {}).get("ebay") or {}
        discount = Decimal(str(ebay_settings.get("ebay_discount", 0.18)))

        rows: list[tuple[str, str, str, str, str]] = []
        per_submission: dict[int, int] = {}
        blocked: dict[int, list[str]] = {}
        # Catalog targets are per LISTING, not per batch: each listing has its own category
        # and its own SitePrice. Accumulated here so one export covers the whole batch.
        wanted: dict[str, dict[str, str]] = {}

        for sub in submissions:
            listing = sub.listing
            if not listing:
                blocked[sub.id] = ["submission has no listing"]
                continue
            built, problems = await ebay_service.build_rows(listing)
            if problems:
                blocked[sub.id] = problems
            if built:
                rows.extend(built)
                per_submission[sub.id] = len(built)
                listing_skus = sorted({r[0] for r in built})
                targets, catalog_problems = await ebay_service.desired_catalog_values(
                    listing, listing_skus, discount
                )
                if catalog_problems:
                    blocked.setdefault(sub.id, []).extend(catalog_problems)
                wanted.update(targets)

        tsv = render_tsv(rows)
        logger.info(
            "%s: %d submission(s), %d specifics row(s), %d blocked",
            self.name,
            len(submissions),
            len(rows),
            len(blocked),
        )

        if not rows:
            # Nothing resolved. Hand the rows back rather than leaving them stranded in
            # PROCESSING with no import behind them, and say why on each one.
            await ListingSubmission.filter(id__in=submission_ids).update(
                status=SubmissionStatus.PENDING, platform_status=None
            )
            return {"submission_count": 0, "rows": 0, "sent": False, "blocked": blocked}

        # Recorded IMMEDIATELY before the first write, and written by nothing else. That
        # makes the step a commitment marker: a row in PROCESSING without it provably never
        # reached SellerCloud, which is what lets recover_stale_processing requeue it safely
        # rather than failing everything for manual review.
        await record_step(submission_ids, "submitting", rows=len(rows))
        jobs: dict[str, Any] = {}
        try:
            # --- step 1: catalog info, export then diff then import ------------------
            catalog_skus = sorted({r[0] for r in rows})
            current, export_job = await ebay_service.export_catalog_fields(catalog_skus)
            jobs["export"] = export_job
            await self._stage(submission_ids, "catalog_exported", jobs,
                              job=export_job, skus=len(catalog_skus))
            catalog_rows = ebay_service.diff_catalog_rows(current, wanted)
            if catalog_rows:
                cat = await ebay_service.import_catalog_info(
                    ebay_service.render_catalog_tsv(catalog_rows)
                )
                jobs["catalog"] = cat.get("job_id")
                await self._stage(submission_ids, "catalog_imported", jobs,
                                  rows=len(catalog_rows), job=cat.get("job_id"))
            else:
                # Everything already correct. A normal outcome, not a failure: sending a
                # file that changes nothing would still queue a job and still take a minute.
                await self._stage(submission_ids, "catalog_unchanged", jobs,
                                  skus=len(catalog_skus))

            # --- step 2: specifics ----------------------------------------------------
            result = await ebay_service.import_specifics(tsv)
        except Exception as exc:  # noqa: BLE001 - recorded on the rows, not swallowed
            # The POST itself blew up, so whether SellerCloud received the file is unknown.
            # Failed rather than requeued, for the reason Grailed fails a stale batch: a
            # retry that double-imports is worse than one that needs a human.
            logger.exception("%s: eBay specifics POST failed", self.name)
            # SellerCloud puts the real cause in the response BODY ("The provided file
            # extension 'txt' is not supported...") while raise_for_status only carries the
            # status line. Without this the step reads "500" and says nothing actionable.
            detail = getattr(getattr(exc, "response", None), "text", "") or str(exc)
            await ListingSubmission.filter(id__in=submission_ids).update(
                status=SubmissionStatus.FAILED,
                error_display="eBay specifics import failed to send",
            )
            await self._stage(
                submission_ids, "failed", jobs, stage="import",
                reason=f"{type(exc).__name__}: {detail}"[:600],
            )
            raise

        ok = 200 <= int(result.get("status_code", 0)) < 300
        job_id = result.get("job_id")
        jobs["specifics"] = job_id
        sent_ids = [sid for sid in submission_ids if sid in per_submission]
        if ok:
            await self._stage(submission_ids, "specifics_imported", jobs,
                              job=job_id, rows=len(rows))

        # --- step 3: publish ---------------------------------------------------------
        if ok:
            published = await ebay_service.publish_to_channel(catalog_skus)
            jobs["publish"] = published.get("job_id")
            if not published.get("ok"):
                # 200 with Success=false. Treated as the failure it is.
                ok = False
                await ListingSubmission.filter(id__in=sent_ids).update(
                    status=SubmissionStatus.FAILED,
                    error_display="eBay publish to channel refused",
                )
                await self._stage(sent_ids, "failed", jobs, stage="publish",
                                  reason=str(published.get("message"))[:400])
                return {"submission_count": 0, "rows": len(rows), "sent": True, "ok": False,
                        "jobs": jobs, "response": published.get("response"), "blocked": blocked}
        if ok:
            # product_import_id is what the dashboard's import view keys on -- the same key
            # SPO writes -- so storing SellerCloud's queued job id here is what makes an
            # eBay import appear there at all.
            #
            # Left in PROCESSING, not SUCCESS: SellerCloud accepting the file means it
            # QUEUED a job, not that the specifics landed. get_job_status can settle that
            # later; claiming success now would be a claim we cannot support.
            # The terminal stage. product_import_id is NOT rewritten here: begin_batch
            # already stamped the batch's own id before any SellerCloud call, and the
            # dashboard has been keying on it since. Overwriting it with a job id now would
            # move an import row that operators have already been watching.
            await record_step(
                sent_ids,
                "published",
                meta={"ebay_jobs": dict(jobs),
                      "published_at": datetime.now(timezone.utc).isoformat()},
                job=jobs.get("publish"),
                rows=len(rows),
                message=str(result.get("message") or "")[:200],
            )
        else:
            await ListingSubmission.filter(id__in=sent_ids).update(
                status=SubmissionStatus.FAILED,
                error_display="eBay specifics import rejected",
            )
            await record_step(
                sent_ids, "failed", stage="import", reason=str(result.get("response"))[:400]
            )

        return {
            "submission_count": len(sent_ids),
            "rows": len(rows),
            "sent": True,
            "ok": ok,
            "job_id": jobs.get("publish") or job_id,
            "jobs": jobs,
            "status_code": result.get("status_code"),
            "response": result.get("response"),
            "blocked": blocked,
        }


ebay_poller = EbayPoller()
