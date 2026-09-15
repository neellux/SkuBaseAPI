"""Scheduled daily flush for the batch platforms: spo, grailed, goat and ebay.

Those platforms park submissions in PENDING until a batch is sent. The size trigger sends
once `min_batch_size` are pending (not eBay, which has none), and Submit Pending on the
submissions dashboard sends whatever is there. A slow week leaves rows below the threshold
waiting for someone to click. This adds a third trigger, driven by two per-platform settings:

    platform_settings.<platform>.scheduled_flush_days       whole number 1-60, blank = off
    platform_settings.<platform>.scheduled_flush_min_size   whole number 1-5000, blank = 1

Each batch poller calls run_scheduled_check once per cycle. On the first cycle at or after
`[scheduled_flush] daily_hour:daily_minute` Eastern, the platform is flushed exactly the way
Submit Pending flushes it when all of these hold: the platform is enabled, manual_fallback is
on, a schedule is set, at least `days` Eastern calendar days have passed since the last flush,
and at least the minimum are ready. Otherwise the result is recorded and the next chance is
the next evening. There is no second attempt later the same evening.

Two pieces of state live in platform_flush_state and never in platform_settings, because
PUT /settings/platform_settings replaces the whole JSON with whatever copy a page loaded and
would overwrite them:

  last_flushed_at          stamped by EVERY trigger when a claim takes at least one row, and
                           put back when every claimed row ends the flush in PENDING again. A
                           batch that sent nothing (an unreachable AppScript, a tab that could
                           not be created) does not count as a flush, so the platform stays
                           due for the next evening (plan decision P1)
  last_scheduled_check_on  the Eastern date whose check has run, taken with a conditional
                           UPDATE, so a restart after the check time or a second process
                           during a deploy cannot check twice

Plan: docs/plans/2026-09-14-feat-scheduled-batch-flush-plan.md
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from tortoise.exceptions import IntegrityError
from tortoise.expressions import Q
from tortoise.transactions import in_transaction

from config import config
from models.db_models import (
    AppSettings,
    ListingSubmission,
    PlatformFlushState,
    SubmissionStatus,
)

logger = logging.getLogger(__name__)

_cfg = config.get("scheduled_flush", {})
# The defaults are the rollout values. Prod config comes from a CI secret that may not carry
# this section at all, so the code must be right without it.
TZ = ZoneInfo(str(_cfg.get("timezone", "America/New_York")))
CHECK_TIME = time(int(_cfg.get("daily_hour", 19)), int(_cfg.get("daily_minute", 0)))
# Runs the whole check and records "dry run, would send N" without sending anything. For
# TEST, whose SellerCloud is the live account.
DRY_RUN = bool(_cfg.get("dry_run", False))

MAX_DAYS = 60
MAX_MIN_SIZE = 5000
RESULT_MAX_LEN = 200

# Per-process short-circuit, so the cycles after today's check cost no query. The database
# slot is the real guard; this only saves a no-op UPDATE every cycle until midnight.
_checked_on: dict[str, date] = {}


@dataclass(frozen=True)
class Schedule:
    days: int
    min_size: int


@dataclass(frozen=True)
class FlushStamp:
    """What stamp_flush changed, so settle_stamp can put it back."""

    platform: str
    stamped_at: datetime
    previous: datetime | None


def _whole_number(value: Any, maximum: int) -> int | None:
    """`value` as an int in 1..maximum, or None.

    Blank, 0, negative, fractional and non-numeric values are all None. The UI saves an int
    or null, but the column is free-form JSON and set_platform_setting() writes strings.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        try:
            value = float(value)
        except ValueError:
            return None
    if isinstance(value, float):
        if not value.is_integer():
            return None
        value = int(value)
    if not isinstance(value, int) or not 1 <= value <= maximum:
        return None
    return value


def parse_schedule(
    platform_settings: dict[str, Any] | None, *, warn: bool = False
) -> Schedule | None:
    """The platform's schedule, or None when it is off.

    `warn` logs a stored value that is present but unusable. Only the daily check passes it;
    the dashboard polls every 10 seconds and would repeat the warning all day.
    """
    settings = platform_settings or {}
    raw_days = settings.get("scheduled_flush_days")
    days = _whole_number(raw_days, MAX_DAYS)
    if days is None:
        if warn and raw_days not in (None, "", 0):
            logger.warning(
                "scheduled_flush_days %r is not a whole number 1-%d, treated as off",
                raw_days, MAX_DAYS,
            )
        return None

    raw_min = settings.get("scheduled_flush_min_size")
    min_size = _whole_number(raw_min, MAX_MIN_SIZE)
    if min_size is None:
        if warn and raw_min not in (None, "", 0):
            logger.warning(
                "scheduled_flush_min_size %r is not a whole number 1-%d, treated as 1",
                raw_min, MAX_MIN_SIZE,
            )
        min_size = 1
    return Schedule(days=days, min_size=min_size)


def eastern_date(moment: datetime) -> date:
    return moment.astimezone(TZ).date()


def is_due(last_flushed_at: datetime | None, today: date, days: int) -> bool:
    """Whether `days` Eastern calendar days separate the last flush from `today`.

    Calendar dates, not 24-hour periods. With days=1, a flush at 7:05pm Monday leaves the
    platform due at 7pm Tuesday instead of pushing it to Wednesday, and a flush at 10am
    Monday keeps that evening's check from sending a second batch.
    """
    if last_flushed_at is None:
        return True
    return (today - eastern_date(last_flushed_at)).days >= days


def next_check_at(state: PlatformFlushState | None, days: int, now: datetime) -> datetime:
    """When the next check that could flush runs. It still needs the minimum to be ready."""
    today = eastern_date(now)
    checked_on = state.last_scheduled_check_on if state else None
    candidate = today if checked_on != today else today + timedelta(days=1)
    last_flushed_at = state.last_flushed_at if state else None
    if last_flushed_at is not None:
        candidate = max(candidate, eastern_date(last_flushed_at) + timedelta(days=days))
    return datetime.combine(candidate, CHECK_TIME, tzinfo=TZ)


async def _take_slot(platform: str, today: date) -> bool:
    """Claim today's check for `platform`. True for exactly one caller per Eastern date.

    `today` is bound from Python, never CURRENT_DATE: the database session runs in UTC, where
    it is already tomorrow from 8pm EDT, so tonight's check would take tomorrow's slot.
    """
    now = datetime.now(timezone.utc)
    taken = await (
        PlatformFlushState.filter(platform=platform)
        .filter(
            Q(last_scheduled_check_on__isnull=True) | Q(last_scheduled_check_on__lt=today)
        )
        .update(last_scheduled_check_on=today, updated_at=now)
    )
    if taken:
        return True
    if await PlatformFlushState.filter(platform=platform).exists():
        return False
    # The migration seeds all four platforms. This only covers a row deleted by hand.
    try:
        await PlatformFlushState.create(platform=platform, last_scheduled_check_on=today)
    except IntegrityError:
        return False
    return True


async def _record_result(platform: str, checked_at: datetime, result: str) -> None:
    try:
        await PlatformFlushState.filter(platform=platform).update(
            last_scheduled_check_at=checked_at,
            last_scheduled_result=result[:RESULT_MAX_LEN],
            updated_at=datetime.now(timezone.utc),
        )
    except Exception:
        logger.exception("%s: could not record the scheduled flush result %r", platform, result)


async def _evaluate_and_flush(
    platform: str,
    today: date,
    count_ready: Callable[[], Awaitable[int]],
    flush: Callable[[], Awaitable[int]],
    background: bool,
) -> str:
    settings = await AppSettings.first()
    # Enablement is membership in app_settings.platforms. manual_fallback alone is not
    # enough: the settings GET hydrates it true for eBay even while eBay is switched off, and
    # a save writes that copy back.
    if not settings or platform not in (settings.platforms or []):
        return "platform disabled"
    platform_settings = (settings.platform_settings or {}).get(platform) or {}
    if not platform_settings.get("manual_fallback"):
        return "manual fallback off"
    schedule = parse_schedule(platform_settings, warn=True)
    if schedule is None:
        return "off"

    state = await PlatformFlushState.get_or_none(platform=platform)
    last_flushed_at = state.last_flushed_at if state else None
    if not is_due(last_flushed_at, today, schedule.days):
        return "not due"

    ready = await count_ready()
    if ready < schedule.min_size:
        return f"skipped, {ready} of {schedule.min_size} ready"
    if DRY_RUN:
        return f"dry run, would send {ready}"

    sent = await flush()

    # The flush path stamps and settles the clock itself. If it is where it was, either the
    # claim found nothing or every claimed row went back to pending.
    after = await PlatformFlushState.get_or_none(platform=platform)
    if (after.last_flushed_at if after else None) == last_flushed_at:
        return "nothing sent, rows still pending"
    if background:
        return f"started a batch of {sent}"
    return f"sent {sent}" if sent else "batch attempted, see imports"


async def run_scheduled_check(
    platform: str,
    *,
    count_ready: Callable[[], Awaitable[int]],
    flush: Callable[[], Awaitable[int]],
    background: bool = False,
    now: datetime | None = None,
) -> str | None:
    """Run today's scheduled check for `platform` if it has not run yet. Never raises.

    `count_ready` returns how many rows the flush would take. `flush` sends them exactly as
    Submit Pending does and returns how many went. With `background` the flush only starts
    the batch (eBay), so the result says it started rather than that it sent.

    Returns the recorded result, or None when no check ran this cycle.
    """
    now_et = (now or datetime.now(timezone.utc)).astimezone(TZ)
    today = now_et.date()
    if now_et.time() < CHECK_TIME or _checked_on.get(platform) == today:
        return None
    # Set before the database is touched, so a missing table costs one logged error a day
    # rather than one every cycle until midnight.
    _checked_on[platform] = today
    try:
        if not await _take_slot(platform, today):
            return None
    except Exception:
        logger.exception(
            "%s: could not take the scheduled flush slot for %s, skipping today",
            platform, today.isoformat(),
        )
        return None

    try:
        result = await _evaluate_and_flush(platform, today, count_ready, flush, background)
    except Exception:
        # The slot is already taken, so there is no second attempt today. A flush path that
        # raised has already put its rows back or failed them.
        logger.exception("%s: scheduled flush check failed", platform)
        result = "error, see logs"

    await _record_result(platform, now_et, result)
    logger.info("%s: scheduled flush check for %s: %s", platform, today.isoformat(), result)
    return result


async def stamp_flush(platform: str) -> FlushStamp | None:
    """Start the flush clock for a batch that just claimed at least one row.

    Call it after the claim transaction commits, outside any lock on submission rows.
    Best-effort: bookkeeping must never block or fail a real batch, so an error is logged
    and None returned.
    """
    now = datetime.now(timezone.utc)
    try:
        async with in_transaction("default") as conn:
            state = await (
                PlatformFlushState.filter(platform=platform)
                .select_for_update()
                .using_db(conn)
                .first()
            )
            if state is None:
                await PlatformFlushState.create(
                    platform=platform, last_flushed_at=now, using_db=conn
                )
                return FlushStamp(platform=platform, stamped_at=now, previous=None)
            await (
                PlatformFlushState.filter(platform=platform)
                .using_db(conn)
                .update(last_flushed_at=now, updated_at=now)
            )
            return FlushStamp(platform=platform, stamped_at=now, previous=state.last_flushed_at)
    except Exception:
        logger.exception("%s: could not stamp the flush clock", platform)
        return None


async def settle_stamp(stamp: FlushStamp | None, claimed_ids: Sequence[int]) -> None:
    """Undo `stamp` when every claimed row is back in PENDING. Never raises.

    One row that moved on (sent, failed, or still in flight) keeps the stamp: something was
    attempted. Compare-and-set on the stamped value, so a newer flush's stamp is never
    overwritten. A crash before this runs leaves the stamp in place; that is accepted.
    """
    if stamp is None or not claimed_ids:
        return
    try:
        moved_on = await (
            ListingSubmission.filter(id__in=list(claimed_ids))
            .exclude(status=SubmissionStatus.PENDING)
            .exists()
        )
        if moved_on:
            return
        restored = await PlatformFlushState.filter(
            platform=stamp.platform, last_flushed_at=stamp.stamped_at
        ).update(last_flushed_at=stamp.previous, updated_at=datetime.now(timezone.utc))
        if restored:
            logger.info(
                "%s: all %d claimed row(s) went back to pending, flush clock restored",
                stamp.platform, len(claimed_ids),
            )
    except Exception:
        logger.exception("%s: could not settle the flush clock", stamp.platform)


async def schedule_summary(
    platform: str,
    platform_settings: dict[str, Any] | None,
    *,
    enabled_platforms: Sequence[str],
    poller_enabled: bool,
    now: datetime | None = None,
) -> dict[str, Any]:
    """The submissions dashboard's view of a platform's schedule. Never raises.

    The page polls every 10 seconds, so a missing migration must read as "unavailable",
    never as a 500.
    """
    summary: dict[str, Any] = {
        "scheduled_flush_status": "off",
        "scheduled_flush_days": None,
        "scheduled_flush_min_size": None,
        "next_scheduled_flush_at": None,
        "last_scheduled_check_at": None,
        "last_scheduled_result": None,
    }
    schedule = parse_schedule(platform_settings)
    if schedule is None:
        return summary
    summary["scheduled_flush_days"] = schedule.days
    summary["scheduled_flush_min_size"] = schedule.min_size

    try:
        state = await PlatformFlushState.get_or_none(platform=platform)
    except Exception:
        logger.exception("%s: could not read platform_flush_state", platform)
        summary["scheduled_flush_status"] = "unavailable"
        return summary

    if state is not None:
        summary["last_scheduled_check_at"] = state.last_scheduled_check_at
        summary["last_scheduled_result"] = state.last_scheduled_result

    if platform not in enabled_platforms:
        summary["scheduled_flush_status"] = "platform_disabled"
    elif not poller_enabled:
        summary["scheduled_flush_status"] = "poller_disabled"
    else:
        summary["scheduled_flush_status"] = "dry_run" if DRY_RUN else "active"
        summary["next_scheduled_flush_at"] = next_check_at(
            state, schedule.days, now or datetime.now(timezone.utc)
        )
    return summary
