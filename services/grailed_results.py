"""Decide waiting Grailed submissions from Grailed's own processor runs. Pure: no I/O.

Grailed's processor reads the AppScript sheet about every two hours and writes, per run, a
success file and an error file to Drive (services/grailed_results_parser.py reads them).
A submission the AppScript accepted waits in `sent` until these runs decide it. The rules
are the plan's R5 to R11 (docs/plans/2026-10-15-0841-feat-grailed-drive-result-tracking-
plan.md) and docs/grailed_integration.md:

  - Success files are events. A "Listing created" row for one of the sizes the submission
    sent counts from any run after its first send attempt. A "Listing updated" row counts
    only on a resubmit: a later attempt, or a size the AppScript reported as already on
    the sheet when it was sent. Deletions never count.
  - Error files are snapshots: every currently failing sheet row, every run. Only the
    latest one counts, and only when it is non-empty and from a run at least the grace
    period after the first send attempt (a run that read the sheet before the write would
    otherwise report old errors). The already-sold delete error never counts. An error row
    under an OLDER sheet key than the one the AppScript returned for the size is a stale row
    of the same sku still failing in the cumulative file, and does not fail a fresh send;
    the key sent, or a later one (the row re-added to the sheet since, outside SkuBase),
    counts. A size refreshed in place has no key back and is matched by sku.
  - A submission is never decided on its first report. It is decided at the first later
    run whose error file counts and that brings nothing new for it: failed if any of its
    sizes is in the current errors, success if any size was created (or updated, on a
    resubmit). An error that clears with no create resets the first report, and the row
    keeps waiting. There is no timeout.

Evidence lives in platform_meta.grailed_results as plain JSON (see Evidence.to_json).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Iterable, Optional

from services.grailed_results_parser import CREATED, ERROR, UPDATED
from utils.grailed_keys import parse_sheet_key

SUCCESS = "success"
FAILED = "failed"


@dataclass(frozen=True)
class SubmissionView:
    """What the engine needs to know about one waiting submission."""

    id: int
    sent_skus: frozenset[str]
    attempt_number: int
    already_on_sheet: frozenset[str]
    first_sending_at: datetime
    # sku -> the sheet key the AppScript returned for it ("<sku>_<MMDDYYYY>").
    references: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Run:
    """One processor run. `errors` is None when the error file is missing or empty: an
    empty error file decides nothing and clears nothing (R11)."""

    run_key: str
    run_at: datetime
    successes: tuple = ()
    errors: Optional[tuple] = None


@dataclass
class Evidence:
    last_run_at: Optional[datetime] = None
    first_report_run: Optional[str] = None
    first_report_at: Optional[datetime] = None
    # sku -> {"id", "run_at", "run_key", "kind"}
    created: dict[str, dict[str, Any]] = field(default_factory=dict)
    # sku -> Grailed error message, from the latest counted error snapshot
    errors: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_json(cls, data: Optional[dict[str, Any]]) -> "Evidence":
        data = data or {}
        return cls(
            last_run_at=_dt(data.get("last_run_at")),
            first_report_run=data.get("first_report_run"),
            first_report_at=_dt(data.get("first_report_at")),
            created={k: dict(v) for k, v in (data.get("created") or {}).items()},
            errors=dict(data.get("errors") or {}),
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "last_run_at": _iso(self.last_run_at),
            "first_report_run": self.first_report_run,
            "first_report_at": _iso(self.first_report_at),
            "created": self.created,
            "errors": self.errors,
        }


@dataclass(frozen=True)
class Decision:
    outcome: str  # SUCCESS | FAILED
    run_key: str
    ids: dict[str, str]  # sku -> Grailed listing id, for every size Grailed reported live
    sku_errors: dict[str, str]
    listed_at: Optional[datetime]  # first created/updated run, success only


def _dt(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value)


def _iso(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value else None


def _counts_as_live(row: Any, view: SubmissionView) -> bool:
    if row.kind == CREATED:
        return True
    if row.kind == UPDATED:
        return view.attempt_number > 1 or row.sku in view.already_on_sheet
    return False


def _is_older_row(reference: str, sent_key: Optional[str]) -> bool:
    """Whether an error row's sheet key is dated before the key sent for the size: a stale
    row of the same sku, not the one this submission wrote."""
    if not sent_key:
        return False
    sent = parse_sheet_key(sent_key)
    row = parse_sheet_key(reference)
    if not sent or not row:
        return False
    return row[1] < sent[1]


def _error_snapshot(rows: Iterable[Any], view: SubmissionView) -> dict[str, str]:
    """The submission's sizes in this error file. A row under an older key than the one
    sent is skipped; the already-sold delete error is not a size error."""
    snapshot: dict[str, str] = {}
    for row in rows:
        if row.kind != ERROR or row.sku not in view.sent_skus:
            continue
        if _is_older_row(row.reference, view.references.get(row.sku)):
            continue
        snapshot[row.sku] = row.message
    return snapshot


def apply_run(
    evidence: Evidence, view: SubmissionView, run: Run, grace: timedelta
) -> tuple[Evidence, Optional[Decision]]:
    """Fold one run into a waiting submission's evidence and decide it when R6 allows.

    Returns the new evidence and a Decision, or None to keep waiting. A run at or before
    the first send attempt, or at or before the last run already applied, changes nothing,
    which is what makes re-reading a run after a crash harmless.
    """
    if run.run_at <= view.first_sending_at:
        return evidence, None
    if evidence.last_run_at and run.run_at <= evidence.last_run_at:
        return evidence, None

    ev = Evidence(
        last_run_at=run.run_at,
        first_report_run=evidence.first_report_run,
        first_report_at=evidence.first_report_at,
        created=dict(evidence.created),
        errors=dict(evidence.errors),
    )
    new = False

    for row in run.successes or ():
        if row.sku not in view.sent_skus or row.sku in ev.created:
            continue
        if not _counts_as_live(row, view):
            continue
        ev.created[row.sku] = {
            "id": row.grailed_id,
            "run_at": run.run_at.isoformat(),
            "run_key": run.run_key,
            "kind": row.kind,
        }
        new = True

    errors_count = run.errors is not None and run.run_at >= view.first_sending_at + grace
    if errors_count:
        snapshot = _error_snapshot(run.errors, view)
        if snapshot != ev.errors:
            new = True
        ev.errors = snapshot

    if ev.first_report_run is None:
        if ev.created or ev.errors:
            ev.first_report_run = run.run_key
            ev.first_report_at = run.run_at
        return ev, None

    if new or not errors_count:
        return ev, None

    ids = {sku: c["id"] for sku, c in ev.created.items() if c.get("id")}
    if ev.errors:
        return ev, Decision(FAILED, run.run_key, ids, dict(ev.errors), None)
    if ev.created:
        listed_at = min(_dt(c["run_at"]) for c in ev.created.values())
        return ev, Decision(SUCCESS, run.run_key, ids, {}, listed_at)

    # A report whose error cleared with nothing created: nothing proves a listing exists.
    ev.first_report_run = None
    ev.first_report_at = None
    return ev, None
