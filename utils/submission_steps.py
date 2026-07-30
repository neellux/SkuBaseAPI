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

logger = logging.getLogger(__name__)

# jsonb_set with create_missing=true adds `steps` on first use. The CASE guards
# a pre-existing non-array `steps` value, which `||` would merge instead of
# append.
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
    "    updated_at = CURRENT_TIMESTAMP "
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

    conn = connections.get("default")
    await conn.execute_query(
        _APPEND_STEP_SQL,
        [ids, json.dumps(meta or {}), json.dumps([new_step(step, **details)])],
    )
    logger.debug(f"recorded step '{step}' for {len(ids)} submission(s)")
