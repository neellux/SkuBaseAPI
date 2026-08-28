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

from collections import defaultdict

from tortoise import Tortoise, connections
from tortoise.transactions import in_transaction

from decimal import Decimal

from models.db_models import AppSettings, ListingSubmission, SubmissionStatus
from services.base_poller import BasePoller
from services.ebay_service import ebay_service, render_tsv, weight_oz
from services.sellercloud_internal_service import sellercloud_internal_service
from services.sellercloud_service import sellercloud_service
from utils.submission_steps import record_step

logger = logging.getLogger(__name__)

# platform_status markers. `submitting` is written immediately before the SellerCloud POST
# and is the commitment marker recover_stale_processing keys on; the two below mark the
# stages after it.
PUBLISHED_STAGE = "published"
AWAITING_IMAGES_STAGE = "awaiting_images"


class EbayPoller(BasePoller):
    name = "ebay_poller"
    PLATFORM_ID = "ebay"

    def __init__(self) -> None:
        super().__init__("ebay_poller", name="ebay_poller")

    async def _poll_cycle(self) -> None:
        """Advance rows the submit path cannot.

        Submitting is driven by POST /submissions/create_batch, not by this cycle. What the
        cycle owns is what happens AFTER SellerCloud accepts: publish jobs finish minutes to
        hours later, eBay item ids appear gradually as it processes them, and neither event
        notifies anything. Measured on the 2026-08-25 run, item ids went 222 -> 797 -> 1,074
        over about an hour.
        """
        await self.recover_stale_processing()
        await self.collect_item_ids()

    async def recover_stale_processing(self, stale_minutes: int = 30) -> dict[str, int]:
        """Un-strand rows a killed flush left in PROCESSING.

        A crash between claiming the rows and posting leaves them PROCESSING with no
        `submitting` step, because that step is written immediately before the POST and by
        nothing else. Those provably never reached SellerCloud, so they go back to PENDING.

        A row whose LAST step is `submitting` got as far as the call and no further, and
        whether SellerCloud received it is unknowable from here, so it is failed for review
        instead of retried.

        THE LAST STEP, NOT ANY STEP. Asking `any(step == "submitting")` was wrong and did
        real damage: the flow passes THROUGH `submitting` on its way to `catalog_exported`,
        `specifics_imported` and `published`, and a published row sits in PROCESSING on
        purpose (SellerCloud accepting a file means it queued a job, not that the work
        landed). So every successfully published row still carried a `submitting` step, and
        the first cycle after this poller was enabled failed five of them on prod with
        "Import interrupted". A row that got past `submitting` is not interrupted, it is
        waiting, and the stage marker is what says which.
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
            names = [st.get("step") for st in steps]
            last = names[-1] if names else None

            if last != "submitting" and "submitting" in names:
                # Got past the call and was answered, so a later stage owns this row.
                # collect_item_ids picks up a published one; anything else stopped
                # somewhere worth looking at rather than silently retrying or failing.
                continue

            if last == "submitting":
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


    # ---------------------------------------------------------------- item ids
    async def collect_item_ids(self) -> dict[str, int]:
        """Read eBay item ids for published imports and park them for the image upload.

        Publishing does not finish an eBay listing: the images are attached by uploading a
        File Exchange file by hand. This is the stage that makes that visible, moving a row
        from PROCESSING to AWAITING_ACTION once there is something to upload.
        """
        rows = await ListingSubmission.filter(
            platform_id=self.PLATFORM_ID,
            status=SubmissionStatus.PROCESSING,
            platform_status=PUBLISHED_STAGE,
        ).prefetch_related("listing")
        if not rows:
            return {"imports": 0, "submissions": 0}

        by_import: dict[Any, list[ListingSubmission]] = defaultdict(list)
        for sub in rows:
            by_import[(sub.platform_meta or {}).get("product_import_id")].append(sub)

        settled = 0
        for import_id, subs in by_import.items():
            try:
                settled += await self._settle_import(import_id, subs)
            except Exception:  # noqa: BLE001 - one bad import must not stall the cycle
                logger.exception(
                    "%s: import %s failed to settle", self.name, import_id
                )
        return {"imports": len(by_import), "submissions": settled}

    async def _settle_import(
        self, import_id: Any, subs: list[ListingSubmission]
    ) -> int:
        """One import: wait for its publish jobs, read item ids, park the rows."""
        publish_jobs = {
            str(job)
            for sub in subs
            if (job := ((sub.platform_meta or {}).get("ebay_jobs") or {}).get("publish"))
        }
        for job_id in publish_jobs:
            if not await sellercloud_service.is_job_complete(job_id):
                logger.debug(
                    "%s: import %s waiting on publish job %s", self.name, import_id, job_id
                )
                return 0

        # eBay's own words per child, merged across EVERY publish job before anything is
        # written. record_step's `meta` does a top-level `platform_meta || $2`, so writing
        # publish_errors once per job would have the second job REPLACE the first job's
        # errors rather than merge with them. One publish call per batch today, but a
        # parent's children can straddle a chunk boundary the moment publishing is chunked.
        publish_errors: dict[str, str] = {}
        for job_id in publish_jobs:
            publish_errors.update(await self._publish_errors(job_id))

        children = await self._children_by_parent(
            [sub.listing.product_id for sub in subs if sub.listing]
        )
        all_children = [sku for skus in children.values() for sku in skus]
        grid = (
            await sellercloud_internal_service.get_catalog_grid_rows(all_children)
            if all_children
            else {}
        )

        settled = 0
        for sub in subs:
            parent = sub.listing.product_id if sub.listing else None
            kids = children.get(parent, [])
            item_ids = {
                sku: str(item_id)
                for sku in kids
                if (item_id := (grid.get(sku) or {}).get("ebayItemID"))
            }
            errors = {sku: publish_errors[sku] for sku in kids if sku in publish_errors}

            sub.external_id = {"item_ids": item_ids}
            # A submission that listed NOTHING is failed, not awaiting images. Every child
            # was refused -- an invalid return policy, no quantity, a category eBay would
            # not take -- so there is no live listing to attach a photo to, and parking it
            # in awaiting_action showed "Images needed" on a product that never reached
            # eBay. It also inflated the coverage denominator with children the file can
            # never contain, so "12 of 40" undercounted a complete upload.
            if item_ids:
                sub.status = SubmissionStatus.AWAITING_ACTION
                sub.platform_status = AWAITING_IMAGES_STAGE
            else:
                sub.status = SubmissionStatus.FAILED
                sub.platform_status = None
                # eBay's own words where the publish job gave them, rather than a generic
                # line: "You've provided an invalid return policy" is actionable, "publish
                # failed" is not.
                sub.error_display = (
                    next(iter(errors.values()), "")[:200] or "No eBay listing was created"
                )
            # platform_meta deliberately absent from update_fields: record_step below owns
            # that column, and this instance is carrying a stale copy of it.
            await sub.save(
                update_fields=["status", "platform_status", "external_id",
                               "error_display", "updated_at"]
            )
            # Per submission, not per import: the errors differ per row, and record_step
            # writes one `meta` to every id it is handed. Only rows that actually have
            # errors pay for a call.
            await record_step(
                [sub.id],
                "item_ids_read",
                meta={"publish_errors": errors} if errors else None,
                listed=len(item_ids),
                children=len(kids),
            )
            settled += 1

        logger.info(
            "%s: import %s settled, %d submission(s), %d/%d children listed",
            self.name, import_id, settled,
            sum(len((s.external_id or {}).get("item_ids") or {}) for s in subs),
            len(all_children),
        )
        return settled

    @staticmethod
    async def _publish_errors(job_id: str) -> dict[str, str]:
        """{child SKU: eBay's message} from a publish job's output file.

        SellerCloud 500s with "There is no output file for job #N" often enough that a
        missing file is a normal outcome, not an error. The reason degrades to nothing
        rather than taking the import down with it.
        """
        try:
            raw = await sellercloud_service.get_job_output_file(job_id)
        except Exception as exc:  # noqa: BLE001 - a missing output file is expected
            logger.info("%s: no output file for publish job %s (%s)",
                        EbayPoller.name, job_id, type(exc).__name__)
            return {}

        text = raw.decode("utf-8", "replace").replace("\r\n", "\n")
        lines = [line for line in text.split("\n") if line.strip()]
        if not lines:
            return {}
        header = lines[0].split("\t")
        out: dict[str, str] = {}
        for line in lines[1:]:
            row = dict(zip(header, line.split("\t")))
            sku = (row.get("ProductID") or "").strip()
            message = (row.get("ErrorMessage") or "").strip()
            if sku and message:
                out[sku] = message[:600]
        return out

    @staticmethod
    async def _children_by_parent(parents: list[str]) -> dict[str, list[str]]:
        """Active child SKUs per parent, one query for the whole import.

        The products registry rather than a SellerCloud catalog search per parent: it is the
        same list get_product_children filters its search against, and one query beats one
        round trip per listing.
        """
        wanted = [p for p in parents if p]
        if not wanted:
            return {}
        rows = await connections.get("product_db").execute_query_dict(
            "SELECT sku, parent_sku FROM child_products "
            "WHERE parent_sku = ANY($1::text[]) AND is_active",
            [wanted],
        )
        out: dict[str, list[str]] = defaultdict(list)
        for row in rows:
            out[row["parent_sku"]].append(row["sku"])
        return out

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
        # Child SKU -> the type's weight in ounces, and -> the submission that owns it, so
        # a fault found after the export can be reported on the right row.
        fallback_oz: dict[str, Decimal] = {}
        sku_owner: dict[str, int] = {}

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
                # The type's weight, for the shipping band, used only where the catalog
                # export has none of its own. The field is "Item weight: OZ", mapped from
                # listingoptions_types.item_weight_oz, so it is already in ounces.
                type_oz = weight_oz(None, (listing.data or {}).get("shipping_weight"))
                if type_oz is not None:
                    for sku in listing_skus:
                        fallback_oz[sku] = type_oz
                sku_owner.update({sku: sub.id for sku in listing_skus})

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
            # A self-contradictory price pair, or a band that cannot be decided, makes a
            # WRONG listing rather than a refused one -- it goes live at the wrong price or
            # on the wrong shipping profile, and nothing downstream will ever flag it.
            #
            # The whole batch stops, rather than the faulted children being dropped and the
            # rest going. Partial would mean a submission publishing some of its variations
            # and silently not others, and the fault is nearly always something systemic
            # (a repricer mid-run, a type with no weight) that will affect the next child
            # too. Measured across all 15,568 children: zero have no weight, and the price
            # pair only diverges when something else has written one of them.
            faults = ebay_service.catalog_faults(current, wanted, fallback_oz)
            if faults:
                for sku, reason in sorted(faults.items()):
                    owner = sku_owner.get(sku)
                    if owner is not None:
                        blocked.setdefault(owner, []).append(f"{sku}: {reason}")
                logger.warning("%s: %d catalog fault(s), nothing imported: %s",
                               self.name, len(faults), sorted(faults.items())[:5])
                await ListingSubmission.filter(id__in=submission_ids).update(
                    status=SubmissionStatus.FAILED,
                    error_display=f"{len(faults)} product(s) have a price or weight fault",
                )
                await self._stage(
                    submission_ids, "failed", jobs, stage="catalog",
                    reason="; ".join(f"{sku}: {why}"
                                     for sku, why in sorted(faults.items()))[:600],
                )
                return {"submission_count": 0, "rows": len(rows), "sent": True,
                        "ok": False, "jobs": jobs, "blocked": blocked}
            catalog_rows = ebay_service.diff_catalog_rows(current, wanted, fallback_oz)
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
            # The stage marker the poll cycle keys on. A queryset update, not an instance
            # save: record_step wrote platform_meta through its own connection, and an ORM
            # save carrying this instance's stale copy of that column would overwrite the
            # steps just recorded. Written after the step so the cycle can never observe
            # the marker without the history behind it.
            await ListingSubmission.filter(id__in=sent_ids).update(
                platform_status=PUBLISHED_STAGE
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
