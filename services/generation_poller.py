"""Drains batch_generation_jobs: generates each batch's listings in the background.

Batches run in parallel, at most PER_BATCH_CONCURRENCY listings each, under a global cap of
[generation_poller] max_running_total running jobs (the user chose 12 after the connection
pool turned out to be 5 by default). Within a batch, the most valuable products go first.

Unlike AISearchPoller, a cycle does not wait for the work it claims. Each claimed job runs
as its own task and, when it finishes, wakes the loop, so a batch's slot refills the moment
a listing lands instead of on the next interval. Kicks coalesce into one claim loop: a burst
of completions produces one claim, not one per completion.

Failure policy:
- Every attempt is bounded by ATTEMPT_TIMEOUT_SECONDS; a hung model call is cancelled and
  retried like any other transient failure.
- A permanent failure (product gone, unknown to SellerCloud, no photos) fails the job at once.
- A transient one is retried with backoff up to MAX_ATTEMPTS, then fails. Operators Retry.
- Interrupted attempts (shutdown, a crash swept up by the stale requeue) never consume an
  attempt and are never capped.
- A 429 starts a short cool-off, and BREAKER_THRESHOLD consecutive transient failures pause
  all claiming for BREAKER_PAUSE_SECONDS, so an outage does not burn every job's attempts in
  minutes. The pause is shown in the batch view's loading panel.
"""

import asyncio
import contextvars
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

from tortoise import connections

from config import config
from exceptions.batch_generation_exceptions import (
    PAUSED,
    RATE_LIMITED,
    GenerationError,
    LeaseLost,
    PermanentGenerationError,
    TransientGenerationError,
    classify,
)
from services import generation_queue as queue
from services.base_poller import BasePoller
from services.generation_queue import ClaimedJob

logger = logging.getLogger(__name__)

PER_BATCH_CONCURRENCY = 3
INTERVAL_SECONDS = 10
MAX_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 60           # 60s, 120s, then failed
ATTEMPT_TIMEOUT_SECONDS = 300
STALE_TIMEOUT_MINUTES = 10           # above the attempt timeout: only a crash leaves a stale row
RATE_LIMIT_COOL_OFF_SECONDS = 60
BREAKER_THRESHOLD = 10
BREAKER_PAUSE_SECONDS = 300
KICK_DEBOUNCE_SECONDS = 0.25
SHUTDOWN_WAIT_SECONDS = 10


class GenerationPoller(BasePoller):
    def __init__(self) -> None:
        super().__init__(config_section="generation_poller", name="GenerationPoller")
        cfg = config.get("generation_poller", {})
        # Defaults off: without the migration every claim would fail, and production's
        # config comes from a secret where a missing section must not start anything.
        self.enabled = bool(cfg.get("enabled", False))
        self.interval = INTERVAL_SECONDS
        self.max_running_total = int(cfg.get("max_running_total", 12) or 0)
        self._wake = asyncio.Event()
        self._tasks: Dict[asyncio.Task, ClaimedJob] = {}
        self._consecutive_failures = 0
        self._paused_until: Optional[datetime] = None
        self._pause_reason: Optional[str] = None

    # ------------------------------------------------------------------
    # Loop
    # ------------------------------------------------------------------

    def kick(self) -> None:
        """Claim now instead of at the next interval. Safe to call from anywhere."""
        if self.enabled and not self._shutdown_event.is_set():
            self._wake.set()

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        while not self._shutdown_event.is_set():
            self._wake.clear()
            # A fresh context per cycle, as BasePoller does, so a leaked tortoise
            # transaction wrapper from one cycle cannot poison the next.
            cycle = loop.create_task(self._poll_cycle(), context=contextvars.Context())
            try:
                await cycle
            except asyncio.CancelledError:
                cycle.cancel()
                raise
            except Exception:
                logger.exception(f"{self.name}: poll cycle error")

            if self._shutdown_event.is_set():
                break
            await self._sleep_until_woken()

    async def _sleep_until_woken(self) -> None:
        wake = asyncio.ensure_future(self._wake.wait())
        stop = asyncio.ensure_future(self._shutdown_event.wait())
        try:
            await asyncio.wait(
                {wake, stop}, timeout=self.interval, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            for waiter in (wake, stop):
                if not waiter.done():
                    waiter.cancel()
        if self._wake.is_set() and not self._shutdown_event.is_set():
            # Let a burst of completions land before claiming once for all of them.
            await asyncio.sleep(KICK_DEBOUNCE_SECONDS)

    async def _poll_cycle(self) -> None:
        requeued = await queue.requeue_stale(STALE_TIMEOUT_MINUTES)
        if requeued:
            logger.warning(f"{self.name}: requeued {requeued} stale job(s)")

        if self._is_paused():
            return
        if self._paused_until is not None:
            logger.info(f"{self.name}: resuming claims after pause ({self._pause_reason})")
            self._paused_until = None
            self._pause_reason = None

        jobs = await queue.claim(PER_BATCH_CONCURRENCY, self.max_running_total or None)
        if not jobs:
            return

        loop = asyncio.get_running_loop()
        for job in jobs:
            # Started only after claim() committed, each in its own context, so no attempt
            # can inherit the claim's transaction.
            task = loop.create_task(self._run_one(job), context=contextvars.Context())
            task.set_name(f"generation-job-{job.id}")
            self._tasks[task] = job
            task.add_done_callback(self._on_done)

        logger.info(
            f"{self.name}: claimed {len(jobs)} job(s) across "
            f"{len({job.batch_id for job in jobs})} batch(es); {len(self._tasks)} running"
            f"{self._pool_note()}"
        )

    async def _run_one(self, job: ClaimedJob) -> None:
        from services.batch_generation_service import run_job

        try:
            await asyncio.wait_for(run_job(job), ATTEMPT_TIMEOUT_SECONDS)
        except asyncio.CancelledError:
            # stop() hands the job back; nothing is written from a cancelled task.
            raise
        except LeaseLost:
            logger.info(
                f"{self.name}: job {job.id} {job.product_id} lost its lease "
                f"(requeued, removed or its batch deleted); result discarded"
            )
            return
        except Exception as exc:  # noqa: BLE001
            await self._record_failure(job, classify(exc))
            return

        self._consecutive_failures = 0
        logger.info(
            f"{self.name}: job {job.id} {job.product_id} done (batch {job.batch_id})"
        )

    def _on_done(self, task: asyncio.Task) -> None:
        self._tasks.pop(task, None)
        if not task.cancelled() and task.exception() is not None:
            logger.error(
                f"{self.name}: generation task {task.get_name()} raised",
                exc_info=task.exception(),
            )
        self.kick()

    # ------------------------------------------------------------------
    # Failures and pausing
    # ------------------------------------------------------------------

    async def _record_failure(self, job: ClaimedJob, err: GenerationError) -> None:
        try:
            status = await queue.retry_or_fail(job, err, MAX_ATTEMPTS, RETRY_BACKOFF_SECONDS)
        except Exception:  # noqa: BLE001
            logger.exception(f"{self.name}: could not record the failure of job {job.id}")
            status = None

        detail = (err.detail or err.message)[:300]
        prefix = f"{self.name}: job {job.id} {job.product_id} (batch {job.batch_id})"
        if status is None:
            logger.info(f"{prefix} failed after losing its lease; nothing recorded: {detail}")
        elif isinstance(err, PermanentGenerationError):
            logger.warning(f"{prefix} failed permanently, {err.message}: {detail}")
        elif status == "failed":
            logger.error(
                f"{prefix} gave up after {job.attempts + 1} attempt(s), {err.message}: {detail}"
            )
        else:
            logger.warning(
                f"{prefix} attempt {job.attempts + 1}/{MAX_ATTEMPTS} failed, "
                f"{err.message}; retrying: {detail}"
            )

        self._note_failure(err)

    def _note_failure(self, err: GenerationError) -> None:
        # A permanent failure is about one product, not the outside world: it neither trips
        # the breaker nor resets the count.
        if not isinstance(err, TransientGenerationError):
            return
        now = datetime.now(timezone.utc)
        if err.rate_limited:
            self._pause(now + timedelta(seconds=RATE_LIMIT_COOL_OFF_SECONDS), RATE_LIMITED)
        if err.trips_breaker:
            self._consecutive_failures += 1
            if self._consecutive_failures >= BREAKER_THRESHOLD:
                self._pause(now + timedelta(seconds=BREAKER_PAUSE_SECONDS), err.message)
                logger.warning(
                    f"{self.name}: {self._consecutive_failures} consecutive failures "
                    f"({err.message}); pausing claims for {BREAKER_PAUSE_SECONDS}s"
                )
                self._consecutive_failures = 0

    def _pause(self, until: datetime, reason: str) -> None:
        if self._paused_until is None or until > self._paused_until:
            self._paused_until = until
            self._pause_reason = reason

    def _is_paused(self) -> bool:
        return self._paused_until is not None and datetime.now(timezone.utc) < self._paused_until

    def status(self) -> dict:
        """For the batch products endpoint. pause_reason is always a fixed display string."""
        if not self.enabled:
            return {"enabled": False, "paused_until": None, "pause_reason": PAUSED}
        if self._is_paused():
            return {
                "enabled": True,
                "paused_until": self._paused_until,
                "pause_reason": self._pause_reason,
            }
        return {"enabled": True, "paused_until": None, "pause_reason": None}

    def _pool_note(self) -> str:
        try:
            pool = connections.get("default")._pool
            size = pool.get_size()
            return f"; pool {size - pool.get_idle_size()}/{size} busy"
        except Exception:  # noqa: BLE001
            return ""

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    async def stop(self) -> None:
        self._shutdown_event.set()
        tasks = list(self._tasks)
        jobs = list(self._tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.wait(tasks, timeout=SHUTDOWN_WAIT_SECONDS)
            try:
                handed_back = await queue.mark_interrupted(
                    [(job.id, job.lease_token) for job in jobs]
                )
                logger.info(f"{self.name}: handed {handed_back} running job(s) back on shutdown")
            except Exception:  # noqa: BLE001
                logger.exception(
                    f"{self.name}: could not hand running jobs back; the stale requeue will"
                )
        await super().stop()


generation_poller = GenerationPoller()
