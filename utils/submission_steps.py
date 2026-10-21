"""Ordered step history on ListingSubmission.platform_meta.

A submission moves through several stages, some of which call an external system
that hands back an id (the SPO product import) and some of which do not (the
AppScript offer sheet). Only the product upload used to leave a trace, so a
submission sitting at `listed` gave no way to tell whether its offers had been
sent, when, or for which SKUs.

record_step appends to `platform_meta.steps`, alongside the flat keys the
submissions dashboard already reads:

    {
      "product_import_id": 756401,
      "uploaded_at": "2026-07-06T17:20:07+00:00",
      "file_name": "spo_products_20260706_172007.xlsx",
      "steps": [
        {"step": "queued",             "at": "2026-07-02T17:51:14+00:00"},
        {"step": "pending",            "at": "2026-07-02T18:04:02+00:00"},
        {"step": "products_uploading", "at": "2026-07-06T17:19:51+00:00"},
        {"step": "offers_submitted",   "at": "2026-07-06T17:19:58+00:00",
         "skus": ["DNT-MHDS-0019/L"], "added": 1, "skipped": 0},
        {"step": "products_uploaded",  "at": "2026-07-06T17:20:07+00:00",
         "product_import_id": 756401},
        {"step": "listed",             "at": "2026-07-07T10:02:14+00:00"}
      ]
    }

The append is a single jsonb UPDATE, so a whole batch is recorded in one
statement and a later stage cannot wipe an earlier stage's entry the way
`.update(platform_meta={...})` did.

The same UPDATE stamps submitted_at and listed_at when the step being recorded is
the platform's handover or its go-live step. The mapping, and why the go-live step
differs per platform, is utils/listing_timestamps.py. Doing it here rather than in
each poller is what makes the two columns consistent across both dispatch paths and
all six platforms; the one write that cannot come through here is GOAT's SKU
readback, which needs a per-row step entry and so stamps listed_at in its own
statement (goat_poller._record_goat_skus).

Callers that write the row through the ORM afterwards must pass `update_fields`
without `platform_meta`, otherwise the instance's stale in-memory copy of the
column overwrites the steps recorded here. Likewise, record_step must be called
outside any transaction holding a lock on the rows: it uses its own connection
and would block on them.
"""

import json
import logging
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

from tortoise import connections

from utils import listing_timestamps

logger = logging.getLogger(__name__)

# jsonb_set with create_missing=true adds `steps` on first use. The CASE guards
# a pre-existing non-array `steps` value, which `||` would merge instead of
# append.
# $4/$5 are the platforms for which THIS step is the handover / the go-live
# (utils/listing_timestamps.py); either may be empty, which stamps nothing. $6 is the
# step entry's own `at`, so the column and the timeline can never disagree.
#
# Both stamps are first-write-wins: SET expressions all see the pre-UPDATE row, so a
# step re-recorded after a requeue keeps the original instant, and the CASE on
# listed_at_source sees the same old listed_at as the CASE on listed_at.
_APPEND_STEP_SQL = (
    "UPDATE listing_submissions "
    "SET platform_meta = jsonb_set("
    "        COALESCE(platform_meta, '{}'::jsonb) || $2::jsonb, "
    "        '{steps}', "
    "        COALESCE("
    "            CASE WHEN jsonb_typeof(platform_meta -> 'steps') = 'array' "
    "                 THEN platform_meta -> 'steps' END, "
    "            '[]'::jsonb"
    "        ) || $3::jsonb, "
    "        true"
    "    ), "
    "    updated_at = CURRENT_TIMESTAMP, "
    "    submitted_at = CASE "
    "        WHEN submitted_at IS NULL AND platform_id = ANY($4::text[]) "
    "        THEN $6::timestamptz ELSE submitted_at END, "
    "    listed_at = CASE "
    "        WHEN listed_at IS NULL AND platform_id = ANY($5::text[]) "
    "        THEN $6::timestamptz ELSE listed_at END, "
    "    listed_at_source = CASE "
    "        WHEN listed_at IS NULL AND platform_id = ANY($5::text[]) "
    "        THEN $7::text ELSE listed_at_source END "
    "WHERE id = ANY($1::bigint[])"
)


def new_step(step: str, **details: Any) -> dict[str, Any]:
    """A single step entry, for seeding platform_meta at row creation.

    Used where record_step cannot be: inside the transaction that INSERTs the
    submission, a second connection would block on the uncommitted row.
    """
    entry: dict[str, Any] = {"step": step, "at": datetime.now(timezone.utc).isoformat()}
    entry.update({k: v for k, v in details.items() if v is not None})
    return entry


async def record_step(
    submission_ids: Sequence[int] | int,
    step: str,
    *,
    meta: dict[str, Any] | None = None,
    **details: Any,
) -> None:
    """Append one `step` entry to each submission's platform_meta.steps.

    `meta` merges into the top level of platform_meta, for keys read directly by
    the dashboard (product_import_id, uploaded_at, file_name, sku_errors).
    `details` are stored on the step entry itself; None values are dropped so an
    entry carries only what is actually known.

    Raises on a database failure, like any other write - `meta` carries data the
    dashboard depends on, so it must not be silently lost.
    """
    ids = (
        [submission_ids]
        if isinstance(submission_ids, int)
        else [int(i) for i in submission_ids]
    )
    if not ids:
        return

    # Built before the UPDATE so the stamped columns reuse the entry's own `at`
    # rather than a second clock reading. Parsed back to a datetime because asyncpg
    # infers the type of a $n::timestamptz parameter and rejects a string for it; going
    # through the entry's own text is also what guarantees the column and the timeline
    # can never name different instants.
    entry = new_step(step, **details)
    stamped_at = datetime.fromisoformat(entry["at"])

    conn = connections.get("default")
    await conn.execute_query(
        _APPEND_STEP_SQL,
        [
            ids,
            json.dumps(meta or {}),
            json.dumps([entry]),
            listing_timestamps.handoff_platforms(step),
            listing_timestamps.go_live_platforms(step, details),
            stamped_at,
            step,
        ],
    )
    logger.debug(f"recorded step '{step}' for {len(ids)} submission(s)")


# Top-level merge only, no step appended. For state that is rewritten every poll
# cycle (the last status-check outcome) and must not grow the timeline by one
# entry per cycle. updated_at is deliberately NOT touched: the stale sweeps key on
# it, and a row whose status check fails every cycle must still age out.
_MERGE_META_SQL = (
    "UPDATE listing_submissions "
    "SET platform_meta = COALESCE(platform_meta, '{}'::jsonb) || $2::jsonb "
    "WHERE id = ANY($1::bigint[])"
)


async def merge_meta(submission_ids: Sequence[int] | int, meta: dict[str, Any]) -> None:
    """Merge `meta` into the top level of platform_meta without recording a step
    and without bumping updated_at."""
    ids = (
        [submission_ids]
        if isinstance(submission_ids, int)
        else [int(i) for i in submission_ids]
    )
    if not ids or not meta:
        return
    conn = connections.get("default")
    await conn.execute_query(_MERGE_META_SQL, [ids, json.dumps(meta)])


def last_step(submission: Any) -> str | None:
    """Name of the most recent step on a submission, or None if it has none."""
    steps = (getattr(submission, "platform_meta", None) or {}).get("steps") or []
    if not isinstance(steps, list) or not steps:
        return None
    entry = steps[-1]
    return entry.get("step") if isinstance(entry, dict) else None
