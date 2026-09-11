"""Batch eBay item specifics into one SellerCloud import.

Mirrors grailed_poller: claim the pending rows under a lock, flip them to PROCESSING,
build one file for the whole batch, post it, then mark each row.

Unlike Grailed and SPO this has NO scheduled cycle yet. It is driven only by
POST /submissions/create_batch?platform=ebay, so an import happens when a person asks for
one rather than on a timer. Clicking submit sends, matching what that button already means
for SPO and Grailed; neither of those has a preview either.
"""

from __future__ import annotations

import json
import logging

import httpx
from datetime import datetime, timedelta, timezone
from typing import Any

from collections import defaultdict

from tortoise import Tortoise, connections
from tortoise.transactions import in_transaction

from decimal import Decimal

from models.db_models import AppSettings, ListingSubmission, SubmissionStatus
from services.base_poller import BasePoller
from services.ebay_service import (
    IMPORT_WAIT_SECONDS,
    REVISE_CHUNK,
    ebay_service,
    render_tsv,
    weight_oz,
)
from services.external_listing_service import ExternalListingService
from services.sellercloud_internal_service import sellercloud_internal_service
from services.sellercloud_service import sellercloud_service
from utils.submission_steps import last_step, record_step

logger = logging.getLogger(__name__)

# platform_status markers. `submitting` is written immediately before the SellerCloud POST
# and is the commitment marker recover_stale_processing keys on; the two below mark the
# stages after it.
PUBLISHED_STAGE = "published"
AWAITING_IMAGES_STAGE = "awaiting_images"
# Terminal step for a submission whose every child was revised: it closes as SUCCESS in the
# batch itself and never passes through `published`.
REVISED_STAGE = "revised"

# Steps a row passes through between `submitting` and `published`. A row stale at one
# of these was abandoned mid-batch; nothing downstream reads them.
MID_BATCH_STEPS = frozenset(
    {"catalog_exported", "catalog_imported", "catalog_unchanged", "specifics_imported",
     "listing_ids_read", "revise_sent"}
)


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

            if last == PUBLISHED_STAGE:
                # Answered by SellerCloud and waiting on its publish job.
                # collect_item_ids owns this row for as long as that takes.
                continue

            if last in MID_BATCH_STEPS:
                # Past the first SellerCloud write but never published: the batch
                # died between stages (a crash, or publish_to_channel raising
                # before it was guarded). The catalog and specifics files it sent
                # are in SellerCloud, so this cannot be retried blind, and no
                # later stage will ever pick it up: row 13862 sat like this from
                # 3 September with `specifics_imported` as its last step.
                sub.status = SubmissionStatus.FAILED
                sub.platform_status = None
                sub.error_display = (
                    f"Import interrupted after {last.replace('_', ' ')} - "
                    "check SellerCloud before resubmitting"
                )
                await sub.save(
                    update_fields=["status", "platform_status", "error_display", "updated_at"]
                )
                await record_step(
                    sub.id, "failed", stage="stale_processing",
                    reason=f"interrupted after {last}; files already sent to SellerCloud",
                )
                failed += 1
                continue

            if last != "submitting" and "submitting" in names:
                # Some later step this sweep does not know. Left alone but said out
                # loud, rather than silently skipped forever.
                logger.warning(
                    "%s: submission %s stale in processing at unknown step %r",
                    self.name, sub.id, last,
                )
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
            submission_ids = await self._one_per_parent([s.id for s in pending], conn)
            if not submission_ids:
                return None, []
            await (
                ListingSubmission.filter(id__in=submission_ids)
                .using_db(conn)
                .update(
                    status=SubmissionStatus.PROCESSING,
                    platform_status="submitting",
                    # The stale sweep keys on updated_at, and a queryset update does not
                    # touch an auto_now field, so without this a row that sat pending for a
                    # day looks a day stale the moment it is claimed.
                    updated_at=datetime.now(timezone.utc),
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

    @staticmethod
    async def _one_per_parent(pending_ids: list[int], conn: Any) -> list[int]:
        """The pending ids to claim: at most one per parent, none for a parent in flight.

        Two listings for the same parent each pass the submit route's check on their own
        rows. Claimed together, both would send the same children; claimed in consecutive
        batches, the second batch's export can run before the first batch's publish job has
        issued item ids, and both would launch. Oldest first, and the rest stay pending for a
        later batch, where the export will show what the first one listed.

        A row with no listing is still claimed: _submit_batch fails it with the reason.
        """
        rows = await conn.execute_query_dict(
            """
            SELECT ls.id, l.product_id,
                   EXISTS (
                       SELECT 1
                       FROM listings l2
                       JOIN listing_submissions s2 ON s2.listing_id = l2.id
                       WHERE l2.product_id = l.product_id
                         AND s2.platform_id = 'ebay'
                         AND s2.status IN ('processing', 'awaiting_action')
                   ) AS parent_busy
            FROM listing_submissions ls
            LEFT JOIN listings l ON l.id = ls.listing_id
            WHERE ls.id = ANY($1::int[])
            ORDER BY ls.created_at, ls.id
            """,
            [pending_ids],
        )
        claimed: list[int] = []
        seen: set[str] = set()
        for row in rows:
            parent = row["product_id"]
            if parent is not None:
                if row["parent_busy"] or parent in seen:
                    continue
                seen.add(parent)
            claimed.append(row["id"])
        if len(claimed) < len(pending_ids):
            logger.info("%s: left %d pending row(s) for a later batch, one per parent",
                        EbayPoller.name, len(pending_ids) - len(claimed))
        return claimed

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
        results: dict[str, tuple[str, str]] = {}
        for job_id in publish_jobs:
            results.update(await self._publish_results(job_id))
        publish_errors = {sku: msg for sku, (_id, msg) in results.items() if msg}

        children = await self._children_by_parent(
            [sub.listing.product_id for sub in subs if sub.listing]
        )
        # The children each submission actually LAUNCHED. Revised children are not in the
        # publish job, so counting them here would send a batch that listed cleanly to the
        # catalog grid looking for ids it already has.
        launched_by_sub = {sub.id: self._launched_children(sub, children) for sub in subs}
        all_children = sorted({sku for kids in launched_by_sub.values() for sku in kids})

        # The job's own output is the source of item ids. The catalog grid is only read
        # when that output is missing -- SellerCloud 500s "There is no output file" often
        # enough that dropping the fallback would turn a routine gap into a batch marked
        # entirely failed. When the output is there, this saves a bulk grid read of every
        # child in the import AND is correct sooner, the grid lagging a publish by up to
        # an hour.
        job_item_ids = {sku: item for sku, (item, _msg) in results.items() if item}
        grid = {}
        if not job_item_ids and all_children:
            logger.info("%s: import %s has no item ids from its publish job(s), "
                        "falling back to the catalog grid", self.name, import_id)
            grid = await sellercloud_internal_service.get_catalog_grid_rows(all_children)

        settled = 0
        for sub in subs:
            parent = sub.listing.product_id if sub.listing else None
            kids = launched_by_sub[sub.id]
            launched = {
                sku: str(item_id)
                for sku in kids
                if (item_id := job_item_ids.get(sku)
                    or (grid.get(sku) or {}).get("ebayItemID"))
            }
            # Ids for children this attempt revised come from the batch's own export, as
            # recorded when the split was made; nothing later would find them otherwise.
            revised = ((sub.platform_meta or {}).get("ebay_split") or {}).get("revised") or {}
            item_ids = {**revised, **launched}
            errors = {sku: publish_errors[sku] for sku in kids if sku in publish_errors}

            sub.external_id = {
                "item_ids": item_ids,
                "revised": sorted(revised),
                "launched": sorted(launched),
            }
            # A submission that listed NOTHING is failed, not awaiting images. Every child
            # was refused -- an invalid return policy, no quantity, a category eBay would
            # not take -- so there is no live listing to attach a photo to, and parking it
            # in awaiting_action showed "Images needed" on a product that never reached
            # eBay. It also inflated the coverage denominator with children the file can
            # never contain, so "12 of 40" undercounted a complete upload.
            # Images are owed for LAUNCHED children only: a revised child already carries its
            # pictures on eBay, and revision profile 1145 does not resend them. So when every
            # launch was refused the submission fails even if its revised children are live:
            # the new sizes are what it was sent for, and FAILED is what lets it go again.
            if launched:
                sub.status = SubmissionStatus.AWAITING_ACTION
                sub.platform_status = AWAITING_IMAGES_STAGE
            else:
                sub.status = SubmissionStatus.FAILED
                sub.platform_status = None
                # eBay's own words where the publish job gave them, rather than a generic
                # line: "You've provided an invalid return policy" is actionable, "publish
                # failed" is not.
                fallback = ("No eBay listing was created for the new sizes" if revised
                            else "No eBay listing was created")
                sub.error_display = next(iter(errors.values()), "")[:200] or fallback
                # The per-child map, so the technical column carries every reason and
                # not only the first one the display line shows.
                sub.error = json.dumps(errors) if errors else "No eBay listing was created"
            # platform_meta deliberately absent from update_fields: record_step below owns
            # that column, and this instance is carrying a stale copy of it.
            await sub.save(
                update_fields=["status", "platform_status", "external_id",
                               "error", "error_display", "updated_at"]
            )
            # Per submission, not per import: the errors differ per row, and record_step
            # writes one `meta` to every id it is handed. Only rows that actually have
            # errors pay for a call.
            await record_step(
                [sub.id],
                "item_ids_read",
                # sku_errors, the key _build_import_detail reads and the dashboard renders
                # per child. Written as publish_errors until now, which nothing read: eBay's
                # own reasons were recorded on every row of import 2 and shown on none of
                # them. publish_errors is kept alongside because mark_images_uploaded reads
                # it when it closes an import.
                meta={"sku_errors": errors, "publish_errors": errors} if errors else None,
                listed=len(launched),
                revised=len(revised),
                children=len(kids) + len(revised),
            )
            if not launched:
                # Every other platform ends a failed row on a `failed` step; without
                # this one an eBay failure ended on `item_ids_read` and read as still
                # in progress to anything walking the timeline.
                await record_step(
                    [sub.id], "failed", stage="publish",
                    reason=sub.error_display[:300],
                    sku_errors=errors or None,
                )
            # eBay is the one platform that issues real per-child ids and nothing at
            # parent level, which is why the presence table carries parent_sku on
            # child rows: a parent-only lookup would never see an eBay listing.
            #
            # Guarded on item_ids because the write above is unconditional and stores
            # {"item_ids": {}} on a total failure. Asserting presence from that would
            # claim a parent is on eBay when every child was refused.
            #
            # No parent row: eBay lists per child, and a parent row would read as full
            # coverage when only some children published. The gap belongs on the pill.
            if item_ids and parent:
                await ExternalListingService.record(
                    "ebay",
                    [
                        {"level": "child", "sku": sku, "parent_sku": parent,
                         "external_id": item_id}
                        for sku, item_id in item_ids.items()
                    ],
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
    def _launched_children(
        sub: ListingSubmission, children: dict[str, list[str]]
    ) -> list[str]:
        """Children this submission sent to LaunchOnChannel.

        From the split _revise_listed recorded, where there is one. A row published before
        revise existed has none, and every active child of its parent was launched.
        """
        split = (sub.platform_meta or {}).get("ebay_split")
        if split is not None:
            return list(split.get("launch") or [])
        parent = sub.listing.product_id if sub.listing else None
        return children.get(parent, [])

    @staticmethod
    async def _publish_results(job_id: str) -> dict[str, tuple[str, str]]:
        """{child SKU: (eBay item id, eBay's message)} from a publish job's output file.

        The output carries BOTH columns, and it is the authority for both. Item ids were
        read from the catalog grid instead, which lags a publish by up to an hour: 14 of
        import 2's 44 products had listed cleanly -- eBay returned Success and a real item
        number for every child -- and were recorded as having listed nothing because the
        grid had not caught up. The job knows immediately.

        SellerCloud 500s with "There is no output file for job #N" often enough that a
        missing file is a normal outcome, not an error. It degrades to nothing rather than
        taking the import down, and the caller falls back to the grid.
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
        out: dict[str, tuple[str, str]] = {}
        for line in lines[1:]:
            row = dict(zip(header, line.split("\t")))
            sku = (row.get("ProductID") or "").strip()
            if not sku:
                continue
            item_id = (row.get("EBayItemID") or "").strip()
            # eBay's stack trace is appended to its own sentence and is the same 400
            # characters on every row. Only the sentence is useful to an operator.
            message = (row.get("ErrorMessage") or "").strip().split(" at eBay.Service")[0]
            out[sku] = (item_id, message.strip()[:600])
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

    @staticmethod
    async def _still_processing(ids: list[int]) -> list[int]:
        """The ids still PROCESSING, in the order given.

        The stale sweep runs alongside a batch. A row it has failed must not be revised or
        launched after the fact: it would go live with no item ids recorded and no image row,
        and the next export would be the first anyone knew of it.
        """
        if not ids:
            return []
        live = set(
            await ListingSubmission.filter(
                id__in=ids, status=SubmissionStatus.PROCESSING
            ).values_list("id", flat=True)
        )
        return [sid for sid in ids if sid in live]

    @staticmethod
    async def _heartbeat(ids: list[int]) -> None:
        """Refresh updated_at on rows still PROCESSING, so the sweep sees a working batch."""
        if ids:
            await ListingSubmission.filter(
                id__in=ids, status=SubmissionStatus.PROCESSING
            ).update(updated_at=datetime.now(timezone.utc))

    @staticmethod
    def _pack_by_submission(
        groups: list[tuple[int, list[str]]], limit: int | None = None
    ) -> list[list[tuple[int, list[str]]]]:
        """Revise calls of up to `limit` children, never splitting one submission across two.

        So a refused or timed-out call fails only the submissions it carried. A submission
        larger than the limit gets a call of its own rather than being split.
        """
        limit = limit or REVISE_CHUNK
        packs: list[list[tuple[int, list[str]]]] = []
        current: list[tuple[int, list[str]]] = []
        size = 0
        for sid, skus in groups:
            if current and size + len(skus) > limit:
                packs.append(current)
                current, size = [], 0
            current.append((sid, skus))
            size += len(skus)
        if current:
            packs.append(current)
        return packs

    async def _fail_rows(
        self, ids: list[int], jobs: dict[str, Any], stage: str, display: str, reason: str
    ) -> None:
        """FAILED for review, for rows still PROCESSING only.

        Guarded on PROCESSING so a row the stale sweep already failed, or one this batch
        closed as revised, is never overwritten by a later stage of the same batch.
        """
        if not ids:
            return
        await ListingSubmission.filter(
            id__in=ids, status=SubmissionStatus.PROCESSING
        ).update(status=SubmissionStatus.FAILED, error_display=display[:200])
        await self._stage(ids, "failed", jobs, stage=stage, reason=reason[:600])

    async def _revise_listed(
        self,
        sent_ids: list[int],
        catalog_skus: list[str],
        current: dict[str, dict[str, str]],
        sku_owner: dict[str, int],
        jobs: dict[str, Any],
    ) -> tuple[list[int], list[str]] | None:
        """Wait for the imports, revise children already on eBay, and return what is left.

        Returns (submission ids still to publish, children to publish), or None when the
        whole batch was failed for review before anything reached eBay.

        A child is revised when the batch's own export carries its eBay item id, so a
        resubmit refreshes what is live instead of launching it again ("already active on
        eBay"), and a partly listed parent can be finished: its listed sizes revise and the
        missing ones launch in the same attempt.
        """
        await ListingSubmission.filter(
            id__in=sent_ids, status=SubmissionStatus.PROCESSING
        ).update(updated_at=datetime.now(timezone.utc))

        # 1. The imports first. A revise or launch that overtakes a queued import sends the
        #    old data: a launch that beat its catalog import is how "Description template is
        #    not defined" happens, and a revise would push stale values onto a live listing.
        # A catalog import is only in `jobs` when catalog rows were sent; if it is there with no
        # id, that import cannot be waited on any more than a missing specifics job can.
        missing = [name for name in ("catalog", "specifics")
                   if (name in jobs or name == "specifics") and not jobs.get(name)]
        if missing:
            await self._fail_rows(
                sent_ids, jobs, "imports",
                "SellerCloud gave no import job to wait on - nothing revised or published",
                f"{' and '.join(missing)} import returned no job id",
            )
            return None
        waited = await ebay_service.wait_for_jobs(
            [job for job in (jobs.get("catalog"), jobs.get("specifics")) if job]
        )
        await ListingSubmission.filter(
            id__in=sent_ids, status=SubmissionStatus.PROCESSING
        ).update(updated_at=datetime.now(timezone.utc))
        stuck = sorted(job for job, state in waited.items() if state == "timeout")
        if stuck:
            await self._fail_rows(
                sent_ids, jobs, "imports",
                "SellerCloud imports still queued - nothing revised or published",
                f"imports not finished after {IMPORT_WAIT_SECONDS}s: job(s) {', '.join(stuck)}",
            )
            return None

        # 2. Which children are already on eBay, per submission. Recorded before any call,
        #    so settle, the image file and the dashboard read the split this attempt made
        #    rather than re-deriving it from a child list that may have changed since.
        revise_map, launch_all = ebay_service.split_by_listing_id(current, catalog_skus)
        revise_by_sub: dict[int, dict[str, str]] = defaultdict(dict)
        launch_by_sub: dict[int, list[str]] = defaultdict(list)
        for sku, item_id in revise_map.items():
            if (owner := sku_owner.get(sku)) is not None:
                revise_by_sub[owner][sku] = item_id
        for sku in launch_all:
            if (owner := sku_owner.get(sku)) is not None:
                launch_by_sub[owner].append(sku)
        for sid in sent_ids:
            # One call per submission: record_step writes the same meta to every id it gets.
            await record_step(
                [sid], "listing_ids_read",
                meta={"ebay_split": {"revised": dict(revise_by_sub.get(sid, {})),
                                     "launch": sorted(launch_by_sub.get(sid, []))}},
                revise=len(revise_by_sub.get(sid, {})),
                launch=len(launch_by_sub.get(sid, [])),
                imports=waited,
            )

        # 3. Revise, a bounded list per call, packed by WHOLE submission so a refused or
        #    timed-out call fails only the submissions it carried. A failed call fails those
        #    submissions and none of their other children launch: one genuine attempt per
        #    submission, and nothing falls back to a launch on its own.
        #
        #    Each call first re-reads which of its rows are still PROCESSING: the stale sweep
        #    runs alongside this batch, and a row it failed must not be revised after the fact.
        failed: set[int] = set()
        tasks: list[str] = []
        packs = self._pack_by_submission(
            [(sid, sorted(revise_by_sub[sid])) for sid in sent_ids if revise_by_sub.get(sid)]
        )
        for pack in packs:
            owners = await self._still_processing([sid for sid, _ in pack])
            failed.update(sid for sid, _ in pack if sid not in owners)
            chunk = [sku for sid, skus in pack if sid in owners for sku in skus]
            if not chunk:
                continue
            try:
                result = await ebay_service.revise_on_ebay(chunk)
            except Exception as exc:  # noqa: BLE001 - recorded on the rows, not swallowed
                logger.exception("%s: eBay revise call failed", self.name)
                detail = getattr(getattr(exc, "response", None), "text", "") or str(exc)
                await self._fail_rows(
                    owners, jobs, "revise",
                    "eBay revise timed out - it may have been sent"
                    if isinstance(exc, httpx.TimeoutException)
                    else "eBay revise call failed - check SellerCloud before resubmitting",
                    f"{type(exc).__name__}: {detail}",
                )
                failed.update(owners)
                continue
            if not result.get("ok"):
                await self._fail_rows(
                    owners, jobs, "revise", "eBay revise refused",
                    str(result.get("message") or "Success=false"),
                )
                failed.update(owners)
                continue
            if result.get("task_id"):
                tasks.append(result["task_id"])
            await self._heartbeat(sent_ids)
        jobs["revise_tasks"] = tasks

        alive = set(await self._still_processing(sent_ids))
        failed.update(sid for sid in sent_ids if sid not in alive)
        revised_ok = [sid for sid in sent_ids if revise_by_sub.get(sid) and sid not in failed]
        if revised_ok:
            await self._stage(revised_ok, "revise_sent", jobs, tasks=tasks)

        # 4. Close what needs nothing more; hand back what still needs a launch.
        revise_only = [sid for sid in revised_ok if not launch_by_sub.get(sid)]
        if revise_only:
            await self._close_revised(revise_only, revise_by_sub, jobs)
        launch_ids = [sid for sid in sent_ids if sid not in failed and launch_by_sub.get(sid)]
        launch_skus = sorted(sku for sid in launch_ids for sku in launch_by_sub[sid])
        return launch_ids, launch_skus

    async def _close_revised(
        self, ids: list[int], revise_by_sub: dict[int, dict[str, str]], jobs: dict[str, Any]
    ) -> None:
        """SUCCESS for submissions whose every child was revised.

        Nothing downstream is owed for them: no publish job to wait on, no item ids to read
        (the export already had them), and no image upload, because a revised child keeps
        the pictures it has on eBay. Parked in `published` instead, collect_item_ids would
        find the ids on the catalog grid and move them to awaiting_action for an upload
        nobody owes.

        SellerCloud's Success is the only signal a revise gives, so this is "sent", not
        "confirmed on eBay"; see revise_on_ebay.
        """
        subs = await ListingSubmission.filter(
            id__in=ids, status=SubmissionStatus.PROCESSING
        ).prefetch_related("listing")
        closed: list[int] = []
        for sub in subs:
            revised = revise_by_sub.get(sub.id) or {}
            external_id = {
                "item_ids": dict(revised), "revised": sorted(revised), "launched": [],
            }
            # A filtered update rather than a save: the stale sweep may have failed this row
            # since it was read, and a save would overwrite that failure with SUCCESS. It also
            # leaves platform_meta alone, which record_step owns.
            changed = await ListingSubmission.filter(
                id=sub.id, status=SubmissionStatus.PROCESSING
            ).update(
                status=SubmissionStatus.SUCCESS,
                platform_status=None,
                external_id=external_id,
                updated_at=datetime.now(timezone.utc),
            )
            if not changed:
                continue
            sub.status = SubmissionStatus.SUCCESS
            sub.external_id = external_id
            parent = sub.listing.product_id if sub.listing else None
            if parent and revised:
                await ExternalListingService.record(
                    "ebay",
                    [
                        {"level": "child", "sku": sku, "parent_sku": parent,
                         "external_id": item_id}
                        for sku, item_id in revised.items()
                    ],
                )
            closed.append(sub.id)
        if closed:
            await self._stage(closed, REVISED_STAGE, jobs, sizes=sum(
                len(revise_by_sub.get(sid) or {}) for sid in closed
            ))

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
            for sid in submission_ids:
                await record_step(
                    sid, "requeued", stage="build",
                    reason="; ".join(blocked.get(sid) or ["no rows built"])[:400],
                )
            return {"submission_count": 0, "rows": 0, "sent": False, "blocked": blocked}

        # Submissions that resolved to no rows at all. The batch is going ahead without
        # them -- they are not in `rows`, so nothing about them reaches SellerCloud -- and
        # before this they were simply abandoned in PROCESSING: every later stage writes to
        # `sent_ids`, so they never gained a `published` step or a terminal status, and
        # recover_stale_processing deliberately skips a row whose last step is past
        # `submitting`, reading it as "a later stage owns this". No later stage did.
        #
        # DNT-MBTM-0079 sat like that in import 3: its type, Men's Track Pants, maps to eBay
        # category 185075, which is not in pm_ebay_categories, so resolve_listing_category
        # returned None and build_rows produced nothing. It stopped at specifics_imported and
        # stayed PROCESSING with no error, while its 24 siblings finished.
        #
        # Failed HERE, before the first SellerCloud call, with the reason build_rows gave --
        # which is the only place that reason exists, since `blocked` is returned to the
        # caller and never persisted.
        sent_ids = [sid for sid in submission_ids if sid in per_submission]
        orphan_ids = [sid for sid in submission_ids if sid not in per_submission]
        for sid in orphan_ids:
            reasons = blocked.get(sid) or ["nothing to submit for this listing"]
            await ListingSubmission.filter(id=sid).update(
                status=SubmissionStatus.FAILED,
                error_display="; ".join(reasons)[:200],
            )
            await record_step(sid, "failed", stage="build", reason="; ".join(reasons)[:400])
        if orphan_ids:
            logger.warning(
                "%s: %d submission(s) produced no rows and were failed: %s",
                self.name, len(orphan_ids), orphan_ids,
            )

        # Recorded IMMEDIATELY before the first write, and written by nothing else. That
        # makes the step a commitment marker: a row in PROCESSING without it provably never
        # reached SellerCloud, which is what lets recover_stale_processing requeue it safely
        # rather than failing everything for manual review.
        #
        # sent_ids from here on, not submission_ids: an orphan is already FAILED above, and
        # adding catalog/specifics steps to it would describe work its rows were never part
        # of. That mismatch is what made 13862 look like it had got as far as
        # specifics_imported.
        await record_step(sent_ids, "submitting", rows=len(rows))
        jobs: dict[str, Any] = {}
        try:
            # --- step 1: catalog info, export then diff then import ------------------
            catalog_skus = sorted({r[0] for r in rows})
            current, export_job = await ebay_service.export_catalog_fields(catalog_skus)
            jobs["export"] = export_job
            await self._stage(sent_ids, "catalog_exported", jobs,
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
                # sent_ids, not submission_ids: an orphan is already FAILED with the
                # reason build_rows gave, and a batch-wide message would overwrite the only
                # record of why that particular listing produced nothing.
                await ListingSubmission.filter(
                    id__in=sent_ids, status=SubmissionStatus.PROCESSING
                ).update(
                    status=SubmissionStatus.FAILED,
                    error_display=f"{len(faults)} product(s) have a price or weight fault",
                )
                await self._stage(
                    sent_ids, "failed", jobs, stage="catalog",
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
                await self._stage(sent_ids, "catalog_imported", jobs,
                                  rows=len(catalog_rows), job=cat.get("job_id"))
            else:
                # Everything already correct. A normal outcome, not a failure: sending a
                # file that changes nothing would still queue a job and still take a minute.
                await self._stage(sent_ids, "catalog_unchanged", jobs,
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
            # sent_ids for the same reason as the catalog fault above.
            await ListingSubmission.filter(
                id__in=sent_ids, status=SubmissionStatus.PROCESSING
            ).update(
                status=SubmissionStatus.FAILED,
                error_display="eBay specifics import failed to send",
            )
            await self._stage(
                sent_ids, "failed", jobs, stage="import",
                reason=f"{type(exc).__name__}: {detail}"[:600],
            )
            raise

        ok = 200 <= int(result.get("status_code", 0)) < 300
        job_id = result.get("job_id")
        jobs["specifics"] = job_id
        if ok:
            await self._stage(sent_ids, "specifics_imported", jobs,
                              job=job_id, rows=len(rows))

        # --- step 3: wait for the imports, then revise what is already on eBay ---------
        # From here the rows still to publish are `launch_ids`, not `sent_ids`: a submission
        # whose children were all revised is closed as SUCCESS inside _revise_listed, and
        # one whose revise failed is already FAILED.
        specifics_ok = ok
        launch_ids: list[int] = list(sent_ids)
        launch_skus: list[str] = list(catalog_skus)
        if ok:
            try:
                remaining = await self._revise_listed(
                    sent_ids, catalog_skus, current, sku_owner, jobs
                )
            except Exception as exc:  # noqa: BLE001 - recorded on the rows, not swallowed
                # Nothing has been launched yet, and any revise call already made is on its
                # step. Failed for review here, rather than left for the sweep's generic line
                # half an hour later.
                logger.exception("%s: eBay revise stage failed", self.name)
                await self._fail_rows(
                    sent_ids, jobs, "revise",
                    "eBay revise step failed - check SellerCloud before resubmitting",
                    f"{type(exc).__name__}: {exc}",
                )
                raise
            if remaining is None:
                return {"submission_count": 0, "rows": len(rows), "sent": True, "ok": False,
                        "jobs": jobs, "blocked": blocked}
            launch_ids, launch_skus = remaining
            ok = bool(launch_ids)

        # --- step 4: publish what is not ------------------------------------------------
        if ok:
            await self._heartbeat(launch_ids)
            try:
                published = await ebay_service.publish_to_channel(launch_skus)
            except Exception as exc:  # noqa: BLE001 - recorded on the rows, not swallowed
                # This call sat outside the guard above, so a transport error here
                # left the rows in PROCESSING at `specifics_imported` with nothing
                # recorded, where the stale sweep would not touch them. The
                # specifics file is already in SellerCloud, so failed for review
                # rather than requeued, the same as the import POST above.
                logger.exception("%s: eBay publish call failed", self.name)
                detail = getattr(getattr(exc, "response", None), "text", "") or str(exc)
                await ListingSubmission.filter(
                    id__in=launch_ids, status=SubmissionStatus.PROCESSING
                ).update(
                    status=SubmissionStatus.FAILED,
                    error_display="eBay publish call failed - check SellerCloud before resubmitting",
                )
                await self._stage(
                    launch_ids, "failed", jobs, stage="publish",
                    reason=f"{type(exc).__name__}: {detail}"[:600],
                )
                raise
            jobs["publish"] = published.get("job_id")
            if not published.get("ok"):
                # 200 with Success=false. Treated as the failure it is.
                ok = False
                await ListingSubmission.filter(
                    id__in=launch_ids, status=SubmissionStatus.PROCESSING
                ).update(
                    status=SubmissionStatus.FAILED,
                    error_display="eBay publish to channel refused",
                )
                await self._stage(launch_ids, "failed", jobs, stage="publish",
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
            # Only rows still PROCESSING get the marker; one the sweep failed keeps its failure.
            launch_ids = await self._still_processing(launch_ids)
            await record_step(
                launch_ids,
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
            await ListingSubmission.filter(
                id__in=launch_ids, status=SubmissionStatus.PROCESSING
            ).update(platform_status=PUBLISHED_STAGE)
        elif not specifics_ok:
            await ListingSubmission.filter(
                id__in=sent_ids, status=SubmissionStatus.PROCESSING
            ).update(
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
            # The batch is ok when its imports were. `ok` alone reads false for a batch whose
            # every child was revised, which leaves nothing to publish but failed at nothing;
            # a refused or failed revise is recorded on its own submissions, not the batch.
            "ok": specifics_ok,
            "job_id": jobs.get("publish") or job_id,
            "jobs": jobs,
            "revise_tasks": jobs.get("revise_tasks"),
            "launched_submissions": len(launch_ids) if specifics_ok else 0,
            "status_code": result.get("status_code"),
            "response": result.get("response"),
            "blocked": blocked,
        }


ebay_poller = EbayPoller()
