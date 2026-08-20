"""Drains listing_ai_search_jobs.

Verification cannot run inside batch creation: a grounded call takes 5-47s
(measured), and /api/create_batch is posted by PhotoManagementNew behind a 120s
httpx timeout that is already funding the SellerCloud prefill and the aspects
model inside one open transaction. So create_batch records a row per listing
after it commits and this poller runs them out of band.

Concurrency is capped at 3, matching the semaphore batch creation already uses
and the configuration actually measured: two separate 10-listing runs at
concurrency 3 finished in 93s and 102s with zero 429s. Google publishes no
per-tier limits for this key, so 3 stays until there is evidence for more.
"""

import asyncio
import logging
import time
from typing import Any, Dict

from config import config
from exceptions.ai_search_exceptions import (
    PermanentAISearchError,
    TransientAISearchError,
    classify,
)
from services import ai_search_queue as queue
from services.base_poller import BasePoller

logger = logging.getLogger(__name__)


def _log_task_exception(task: asyncio.Task) -> None:
    if task.cancelled():
        return
    exc = task.exception()
    if exc:
        logger.exception(f"AISearchPoller kick failed: {exc}", exc_info=exc)


class AISearchPoller(BasePoller):
    def __init__(self) -> None:
        super().__init__(
            config_section="ai_search_poller", name="AISearchPoller"
        )
        cfg = config.get("ai_search_poller", {})
        self.batch_size: int = cfg.get("batch_size", 6)
        self.concurrency: int = cfg.get("concurrency", 3)
        self.max_attempts: int = cfg.get("max_attempts", 3)
        self.retry_backoff_seconds: int = cfg.get("retry_backoff_seconds", 120)
        self.stale_timeout_minutes: int = cfg.get("stale_timeout_minutes", 15)
        self.max_runs_per_day: int = cfg.get("max_runs_per_day", 200)
        # One-tick circuit breaker. A single 429 means the window is already
        # contended, and marching straight back in at concurrency 3 is how a soft
        # limit becomes a hard one.
        self._cool_off_next_cycle = False

    def kick(self) -> None:
        """Run one cycle now, so a manual re-run does not wait out the interval.

        Safe to overlap with the scheduled cycle because the claim is atomic.
        This is the fire-and-forget pattern from listing_routes._run_submissions_
        background, used under its stated precondition: the durable row already
        exists, so losing the task to a restart costs nothing but latency.
        """
        if not self.enabled:
            return
        task = asyncio.create_task(self._poll_cycle())
        task.add_done_callback(_log_task_exception)

    async def _poll_cycle(self) -> None:
        requeued = await queue.requeue_stale(self.stale_timeout_minutes, self.max_attempts)
        if requeued:
            logger.warning(f"{self.name}: re-queued {requeued} stale job(s)")

        if self._cool_off_next_cycle:
            self._cool_off_next_cycle = False
            logger.warning(f"{self.name}: cooling off one cycle after a rate limit")
            return

        spent = await queue.runs_in_last_day()
        if spent >= self.max_runs_per_day:
            logger.error(
                f"{self.name}: daily cap reached ({spent}/{self.max_runs_per_day} runs in 24h); "
                "skipping this cycle. Jobs stay pending, nothing is lost."
            )
            return

        jobs = await queue.claim_batch(self.batch_size)
        if not jobs:
            # Silent. A 30s poller logging every empty cycle writes 2,880 useless
            # lines a day.
            return

        logger.info(f"{self.name}: claimed {len(jobs)} job(s)")
        semaphore = asyncio.Semaphore(self.concurrency)
        await asyncio.gather(*(self._run_one(job, semaphore) for job in jobs))

    async def _run_one(self, job: Dict[str, Any], semaphore: asyncio.Semaphore) -> None:
        from models.db_models import Listing
        from services.ai_search_service import run_for_listing
        from tortoise import connections

        job_id = job["id"]
        listing_id = str(job["listing_id"])
        product_id = job["product_id"]

        async with semaphore:
            if self._shutdown_event.is_set():
                # Hand it straight back rather than starting a 30s call we cannot
                # finish; the stale sweep would otherwise have to recover it.
                await queue.mark_retry(job_id, "Shutting down", 0)
                return

            listing = await Listing.filter(id=listing_id).first()
            if not listing:
                await queue.mark_skipped(job_id, "Listing no longer exists")
                return

            started = time.monotonic()
            try:
                result = await run_for_listing(
                    listing,
                    reason=job.get("reason") or "batch_create",
                    requested_by=job.get("requested_by"),
                )
            except Exception as exc:
                await self._handle_failure(job, classify(exc), listing_id, product_id)
                return

            duration_ms = int((time.monotonic() - started) * 1000)

        # Targeted single-column UPDATE, never listing.save(). The call took up to
        # 47s, during which an operator may have saved this listing and its
        # triggers may have rewritten the row; a bare save() from a 47-second-old
        # instance would write all of that back. updated_at is left alone on
        # purpose: an AI search is not an operator edit.
        import json

        await connections.get("default").execute_query(
            "UPDATE listings SET ai_search = $2::jsonb WHERE id = $1",
            [listing_id, json.dumps(result)],
        )
        await queue.mark_completed(
            job_id, result["model"], result["auth"], duration_ms, result["cost_usd"]
        )

        conflicts = [
            name
            for name, v in (result.get("fields") or {}).items()
            if v.get("status") == "conflict"
        ]
        label = result.get("label") or {}
        logger.info(
            f"{self.name}: job {job_id} {product_id} done in {duration_ms / 1000:.1f}s, "
            f"{len(result.get('sources') or [])} sources, "
            f"{len(conflicts)} conflict(s){' (' + ', '.join(conflicts) + ')' if conflicts else ''}, "
            f"label={label.get('mpn') or '-'}/{label.get('colour_name') or '-'}, "
            f"${result['cost_usd']:.4f} via {result['auth']}"
        )

    async def _handle_failure(self, job, err, listing_id, product_id) -> None:
        """Retry, or park the job and write a failed result the UI can render."""
        from tortoise import connections

        job_id = job["id"]
        attempts = job.get("attempts") or 1
        detail = err.detail or err.message

        if isinstance(err, TransientAISearchError) and attempts < self.max_attempts:
            if "429" in (detail or "") or "RESOURCE_EXHAUSTED" in (detail or ""):
                self._cool_off_next_cycle = True
            delay = err.retry_after or self.retry_backoff_seconds * (2 ** (attempts - 1))
            await queue.mark_retry(job_id, detail, delay)
            logger.warning(
                f"{self.name}: job {job_id} {product_id} transient "
                f"(attempt {attempts}/{self.max_attempts}), retrying in {delay:.0f}s: {detail[:160]}"
            )
            return

        await queue.mark_failed(job_id, detail)
        if isinstance(err, PermanentAISearchError):
            logger.warning(f"{self.name}: job {job_id} {product_id} permanent: {detail[:200]}")
        else:
            logger.error(
                f"{self.name}: job {job_id} {product_id} gave up after {attempts} attempts: "
                f"{detail[:200]}"
            )

        # A failed result blob is what stops the UI spinning forever: without it
        # the listing looks queued indefinitely.
        import json

        await connections.get("default").execute_query(
            "UPDATE listings SET ai_search = $2::jsonb WHERE id = $1",
            [
                listing_id,
                json.dumps(
                    {
                        "schema_version": 1,
                        "status": "failed",
                        "error": err.message,
                        "error_detail": (detail or "")[:500],
                        "fields": {},
                        "sources": [],
                        "label": {},
                    }
                ),
            ],
        )


ai_search_poller = AISearchPoller()
