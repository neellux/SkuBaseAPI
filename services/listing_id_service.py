"""Listing ids saved by hand on the Listing history tab, with the delists that go with them.

One call saves one platform's id changes and delists for one product, as one recorded
change. Nothing is ever sent to a platform. Plan:
docs/plans/2026-10-15-1256-feat-listing-history-tab-ids-plan.md (U3; R6 to R14, R17; KTD4
to KTD9). The reading side (what each size shows and whether it can be edited) is the
history payload's ID EDITOR in platform_delist_service, and the save judges every field
by that same editor, so the page and the save cannot disagree.

REQUEST (POST /products/listing_ids)

    platform_id, the parent sku, and
    changes          [{"level": "product" | "size", "sku", "from", "to"}]
                       from  the id the page loaded (the editor's id, sent back unchanged)
                       to    the new id; null or blank clears it, which is a delist (R13)
                       sku   the child sku; ignored on a product-level change
    delist           child skus to take down, with or without an id
    delist_product   take down the whole product (a product with no sizes)
    comment          the note, kept on the manual row and on the delist (R12)

THE SAVE (save_listing_ids). Refusals are all or nothing; nothing is written before the
last check passes. In order:

    1. Before any query: the note's length, SellerCloud (read-only, no ids and no delist),
       the platform, each change's level (R4) and the format of each new id (KTD9).
    2. Lock (SUBMIT_LOCK_SQL, waiting at most lock_timeout, else busy), refuse while
       migrations/listing_id_edits.sql is not applied (_MIGRATION_GUARD_SQL), then read
       the platform's attempts, presence rows (locked), delists, the children's rows
       stored under another parent, and every row of the platform already holding one of
       the new ids, live or in an open delist's pre-image. Exclusions are read after the
       lock.
    3. plan_save:
         a platform-wide lock: excluded, in flight, awaiting action (the lock reason)
         an unknown size
         a `from` that no longer matches what is stored, compared normalized (KTD6, R10)
         a changed field that is not editable: delisted ("<size>: restore first") or
           stored under another parent ("<size>: stored under <parent>") (KTD7)
         a delist that covers the whole product, or any delist beside a product id
           change, in a save that also changes ids ("Delist alone first", R14)
         an id another size or product holds on the platform once the save is done, or
           that Restore of an open delist would put back on another size ("<size>: id
           used by <sku>") (KTD9)
    4. Write, in this order: the delist (platform_delist_service.write_delist, the same
       core delist() runs), then the manual row, then the presence upsert.

    Unchanged is judged on normalized values, so 1nventory "123" against a stored
    gid://shopify/Product/123 is no change, and a save whose every change is unchanged
    writes nothing (R6). Clearing a size's id adds that size to the delist; clearing a
    product-level id (1nventory product id, GOAT SKU) delists the whole product, because
    that id covers every size. On a platform with allow_partial_submit off, any size
    picked widens to the whole product (R14), as in delist().

THE MANUAL ROW (KTD4, KTD8, R7, R17). Written only when an id changed; a save that only
delists writes the delist alone. A listing_submissions row:

    listing_id       the product's newest listing (created_at, then id), or NULL (R8)
    status           success; platform_status 'manual' (MANUAL ROWS in
                     platform_delist_service)
    attempt_number   latest + 1 for that listing and platform, 1 with no listing
    submitted_by     the user
    created_at       clock_timestamp(), so it sorts after a delist made in the same save
    external_id      the platform's whole id set after the save, in its own shape:
                       ebay             {"item_ids": {child sku: item id}}
                       1nventory        {"product_gid": gid, "variant_gids": {sku: gid}},
                                        product_gid only while a product row holds one
                       grailed          [key, ...] in size order
                       goat(_sneakers)  NULL; platform_meta.goat_sku holds the GOAT SKU
    platform_meta    {"parent_sku", "manual": {"changes": [{"sku", "level", "old",
                     "new"}], "note", "delist_id"}, "steps": [manual_id_edit]}

The step is seeded inline as listing_routes does. manual_id_edit is no platform's go-live
or handover step (utils/listing_timestamps.py), so it stamps neither listed_at nor
submitted_at, and listed_date skips manual rows anyway (R18).

THE PRESENCE WRITE (KTD7). One upsert of the changed rows on the save's own connection
(ExternalListingService.record opens its own and swallows errors). It sets external_id and
source 'manual' and merges external_meta.manual_edit = {"at", "by", "submission_id"},
which Restore reads (KTD11). `at` is clock_timestamp(), never now(): now() is the
transaction start, before a delist made in the same save, and Restore would then delete
the edited row. It never moves parent_sku, which would fire the presence trigger with
unflag allowed and could reopen the old parent's batches: a row stored under another
parent is refused in step 3, and one that moved there meanwhile is skipped by the
conflict update, which refuses the whole save. A size's id writes that child row only,
never a parent row, on every platform.

REFUSAL CODES, beside platform_delist_service's (the route maps each to its status):

    invalid_id 400, no_id_level 400, sent_twice 400, delist_and_edit 400,
    changed 409, duplicate_id 409, delist_alone 409,
    restore_first 409, other_parent 409 (the id editor's codes),
    busy 409 (lock timeout), not_ready 503 (the migration is not applied)
"""

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from tortoise.transactions import in_transaction

from services import platform_delist_service as pds
from services.external_listing_service import CHILD, NEVER_GATED, PARENT, SOURCE_MANUAL
from services.goat_service import validate_goat_sku
from services.platform_delist_service import PRODUCT_LEVEL, SIZE_LEVEL, DelistRefused
from utils.grailed_keys import parse_sheet_key, split_sheet_key
from utils.submission_steps import new_step

logger = logging.getLogger(__name__)

INVALID_ID = "invalid_id"
NO_ID_LEVEL = "no_id_level"
SENT_TWICE = "sent_twice"
DELIST_AND_EDIT = "delist_and_edit"
CHANGED = "changed"
# The UI matches on this text (listingIdEdits.js STALE_DETAIL) to reload the row.
CHANGED_DETAIL = "Changed since you opened it"
DUPLICATE_ID = "duplicate_id"
DELIST_ALONE = "delist_alone"
NOT_READY = "not_ready"
BUSY = "busy"

# The manual row's one step. Must never be a go-live or handover step (R18).
MANUAL_STEP = "manual_id_edit"

# An editor level is a presence row level.
_ROW_LEVEL = {PRODUCT_LEVEL: PARENT, SIZE_LEVEL: CHILD}

# Set before the submit lock, as catalog_service.create_batch does: a save waits at most
# this long behind a submit, delist or Restore of the product (or a locked presence row),
# then answers busy instead of hanging the request.
_LOCK_TIMEOUT_SQL = "SET LOCAL lock_timeout = '5s'"

# Deploy-order guard. True means migrations/listing_id_edits.sql is not applied: the old
# closing test counts a manual row as a relist, so a save under an open delist would close
# it and its delisted sizes would read listed again.
_MIGRATION_GUARD_SQL = (
    "SELECT platform_delist_closing_attempt('success', 'manual', NULL) AS manual_closes"
)

# Every row of the platform already holding one of the new ids, under any parent. There is
# no index on external_id, so this filters the platform's rows: tens of thousands at most.
# Then every row in the pre-image of an OPEN delist of the platform that holds one
# (pre_image): Restore re-inserts it as it was, so its size still holds that id. Open ones
# only, the only ones Restore takes, and there are few (none in prod on 2026-10-15).
_HOLDERS_SQL = """
SELECT e.level, e.sku, e.parent_sku, e.external_id, false AS pre_image
  FROM external_listing_ids e
 WHERE e.platform_id = $1
   AND e.external_id = ANY($2::text[])
UNION ALL
SELECT r ->> 'level', r ->> 'sku', r ->> 'parent_sku', r ->> 'external_id', true
  FROM open_platform_delists() d
 CROSS JOIN LATERAL jsonb_array_elements(d.removed_rows) r
 WHERE d.platform_id = $1
   AND r ->> 'external_id' = ANY($2::text[])
"""

# The product's newest listing and the attempt number the manual row takes on it. No row
# means no listing: the manual row then has none, and attempt 1 (KTD4).
_NEWEST_LISTING_SQL = """
SELECT l.id::text AS listing_id,
       COALESCE((SELECT max(s.attempt_number)
                   FROM listing_submissions s
                  WHERE s.listing_id = l.id
                    AND s.platform_id = $2), 0) + 1 AS attempt_number
  FROM listings l
 WHERE l.product_id = $1
 ORDER BY l.created_at DESC, l.id DESC
 LIMIT 1
"""

# Inserted as success, so trigger_mark_listing_submitted_if_complete completes the listing
# (R9) and update_batch_platform_statuses records it. clock_timestamp(), as the delist
# entry, so a delist made earlier in the same save is older than this row.
_INSERT_MANUAL_SQL = f"""
INSERT INTO listing_submissions
       (listing_id, platform_id, status, platform_status, submitted_by, attempt_number,
        external_id, platform_meta, created_at, updated_at)
VALUES ($1::uuid, $2, 'success', '{pds.MANUAL_PLATFORM_STATUS}', $3, $4, $5::jsonb,
        $6::jsonb, clock_timestamp(), clock_timestamp())
RETURNING id, created_at
"""

# KTD7. $3, $4, $5: the rows' levels, skus and new ids. The conflict update skips a row
# under another parent, so RETURNING lists only the rows written.
_UPSERT_IDS_SQL = f"""
INSERT INTO external_listing_ids AS e
       (platform_id, level, sku, parent_sku, external_id, external_meta, source)
SELECT $1, u.level, u.sku, $2, u.external_id,
       jsonb_build_object('{pds.MANUAL_EDIT_META}', jsonb_build_object(
           'at', clock_timestamp(), 'by', $6::text, 'submission_id', $7::bigint)),
       '{SOURCE_MANUAL}'
  FROM unnest($3::text[], $4::text[], $5::text[]) AS u(level, sku, external_id)
ON CONFLICT (platform_id, level, sku) DO UPDATE
   SET external_id   = EXCLUDED.external_id,
       source        = EXCLUDED.source,
       external_meta = e.external_meta || EXCLUDED.external_meta,
       updated_at    = now()
 WHERE e.parent_sku = EXCLUDED.parent_sku
RETURNING e.sku
"""


@dataclass(frozen=True)
class Change:
    """One requested change, checked for format. `sent_from` is the loaded value and `new`
    the requested one, both normalized; `new` None clears the id."""

    level: str
    sku: str
    size: str
    sent_from: Optional[str]
    new: Optional[str]


@dataclass(frozen=True)
class Edit:
    """An id that changes to a new value: written, and recorded on the manual row (R17).
    `old` is the stored value as stored."""

    level: str
    sku: str
    size: str
    old: Optional[str]
    new: str

    def record(self) -> Dict[str, Any]:
        return {"sku": self.sku, "level": self.level, "old": self.old, "new": self.new}


@dataclass(frozen=True)
class SavePlan:
    delist: Optional[pds.DelistPlan]  # None: nothing to delist
    edits: List[Edit]
    external_id: Any  # the manual row's external_id (KTD8)
    goat_sku: Optional[str]


# ---------------------------------------------------------------------------
# Normalization (KTD9)
# ---------------------------------------------------------------------------


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _about(level: str, size: str, text: str) -> str:
    """A refusal about one field leads with its size, so the UI can mark that field. The
    first word is lowered after the size, unless it is an acronym ("ID")."""
    if level == SIZE_LEVEL:
        acronym = text[1:2].isupper()
        return f"{size}: {text if acronym else text[:1].lower() + text[1:]}"
    return text[:1].upper() + text[1:]


def _invalid(level: str, size: str, text: str) -> DelistRefused:
    return DelistRefused(_about(level, size, text), INVALID_ID, 400)


def comparable_id(platform_id: str, level: str, value: Any) -> Optional[str]:
    """The id as it would be stored, for comparing: trimmed, blank as None, and a bare
    1nventory number with its level's gid prefix. Never raises, so a stored value that
    fails the format check (a variant gid on a product row) still compares as itself."""
    text = _text(value)
    if not text:
        return None
    if platform_id == "1nventory":
        number = pds.gid_number(level, text)
        if number is not None:
            return pds._SHOPIFY_GID_PREFIX[level] + number
    return text


def normalize_id(platform_id: str, level: str, value: Any, *, sku: str, size: str) -> Optional[str]:
    """KTD9: the id as stored, or None to clear it. Raises DelistRefused (invalid_id, 400).

    ebay               digits
    1nventory product  digits or a Product gid, stored as the gid
    1nventory variant  digits or a ProductVariant gid, stored as the gid
    goat(_sneakers)    what the GOAT sheet's own check passes (goat_service.validate_goat_sku:
                       letters, digits, space and ._/- up to 64), as it cleans it
    grailed            <this size's sku>_<MMDDYYYY> with a real date, or digits (a
                       real Grailed id), as entered
    """
    text = _text(value)
    if not text:
        return None
    if platform_id == "ebay":
        if pds.ASCII_DIGITS.fullmatch(text):
            return text
        raise _invalid(level, size, "ID must be digits")
    if platform_id == "1nventory":
        number = pds.gid_number(level, text)
        if number is None:
            # Not digits, or a gid of the other level's kind (a variant gid on the product).
            raise _invalid(level, size, "not a valid ID")
        return pds._SHOPIFY_GID_PREFIX[level] + number
    if platform_id == "grailed":
        if pds.ASCII_DIGITS.fullmatch(text):
            return text
        split = split_sheet_key(text)
        if split is not None and split[0] != sku:
            # A sheet key whose child sku is another size's (or another product's).
            raise _invalid(level, size, "ID is for another size")
        if parse_sheet_key(text) is not None:
            return text
        raise _invalid(level, size, "not a valid ID")
    if platform_id in ("goat", "goat_sneakers"):
        # It reaches a live Shopify metafield, so the same allowlist as a sheet read-back.
        cleaned = validate_goat_sku(text)
        if cleaned is None:
            raise _invalid(level, size, "not a valid ID")
        return cleaned
    return text


def parse_changes(
    platform_id: str,
    parent_sku: str,
    changes: Sequence[Dict[str, Any]],
    labels: Dict[str, str],
) -> List[Change]:
    """Each change's level and format, before any query. labels: child sku -> size."""
    levels = pds.ID_LEVELS.get(platform_id, ())
    out: List[Change] = []
    seen = set()
    for raw in changes:
        level = raw.get("level")
        if level not in levels:
            label = pds.platform_label(platform_id)
            if level == PRODUCT_LEVEL:
                # Never "product id", which reads as the platform's own term for it.
                message = f"No ID on {label} for the product"
            else:
                message = f"No {'size' if level == SIZE_LEVEL else 'such'} ID on {label}"
            raise DelistRefused(message, NO_ID_LEVEL, 400)
        sku = parent_sku if level == PRODUCT_LEVEL else _text(raw.get("sku"))
        size = labels.get(sku) or pds._size_label(sku)
        if (level, sku) in seen:
            raise DelistRefused(_about(level, size, "sent twice"), SENT_TWICE, 400)
        seen.add((level, sku))
        sent_from = comparable_id(platform_id, level, raw.get("from"))
        if comparable_id(platform_id, level, raw.get("to")) == sent_from:
            # Unchanged as sent: no format to check. Still compared with what is stored.
            new = sent_from
        else:
            new = normalize_id(platform_id, level, raw.get("to"), sku=sku, size=size)
        out.append(Change(level=level, sku=sku, size=size, sent_from=sent_from, new=new))
    return out


# ---------------------------------------------------------------------------
# The plan, pure
# ---------------------------------------------------------------------------


def _ids_after(
    rows: Sequence[Dict[str, Any]], delist: Optional[pds.DelistPlan], edits: Sequence[Edit]
) -> Dict[Tuple[str, str], Optional[str]]:
    """{(row level, sku): id} of the parent's rows once the save is done."""
    removed = set(delist.remove_ids) if delist else set()
    after = {(r["level"], r["sku"]): r.get("external_id") for r in rows if r["id"] not in removed}
    for sku in delist.insert_skus if delist else ():
        after.setdefault((CHILD, sku), None)
    for edit in edits:
        after[(_ROW_LEVEL[edit.level], edit.sku)] = edit.new
    return after


def _refuse_duplicates(
    parent_sku: str,
    edits: Sequence[Edit],
    after: Dict[Tuple[str, str], Optional[str]],
    holders: Sequence[Dict[str, Any]],
) -> None:
    """KTD9: no two rows of a platform and level may hold one id once the save is done.
    The parent's own rows count as they will be, so swapping two sizes' ids, or reusing
    the id of a size delisted in the same save, is not a duplicate. An id in an open
    delist's pre-image counts as held by its size on any parent, this one included: Restore
    puts it back. Keyed on sku, never parent_sku, which is stale for reassigned children."""
    held: Dict[Tuple[str, str], List[str]] = {}
    for (level, sku), value in after.items():
        if value is not None:
            held.setdefault((level, value), []).append(sku)
    for h in holders:
        if h.get("pre_image") or h.get("parent_sku") != parent_sku:
            held.setdefault((h["level"], h["external_id"]), []).append(h["sku"])
    for edit in edits:
        others = [s for s in held.get((_ROW_LEVEL[edit.level], edit.new), []) if s != edit.sku]
        if others:
            raise DelistRefused(
                _about(edit.level, edit.size, f"ID used by {others[0]}"), DUPLICATE_ID
            )


def _refuse_unless_alone(
    edits: Sequence[Edit],
    whole: bool,
    picked: Sequence[str],
    size_list: Sequence[Dict[str, Any]],
    allow_partial: bool,
) -> None:
    """R14, for a save that delists and also changes ids: a delist of the whole product
    goes alone (a pick that widens to every active size is the whole product). So does any
    delist beside a product id change: a per-size delist takes the product row, and the id
    is read-only until Restore (KTD10)."""
    if not edits:
        return
    active = [s["sku"] for s in size_list if s.get("active")]
    scope = (
        None
        if whole
        else pds.delist_scope(pds.expand_selection(picked, active, allow_partial), active)
    )
    if scope is None or any(e.level == PRODUCT_LEVEL for e in edits):
        raise DelistRefused("Delist alone first", DELIST_ALONE)


def manual_external_id(
    platform_id: str,
    after: Dict[Tuple[str, str], Optional[str]],
    order: Sequence[str],
) -> Tuple[Any, Optional[str]]:
    """KTD8: (external_id, goat_sku) of the manual row, from the ids after the save.
    order: the size order, so lists and maps read like the page."""
    rank = {sku: i for i, sku in enumerate(order)}
    children = sorted(
        ((sku, v) for (level, sku), v in after.items() if level == CHILD and v),
        key=lambda item: (rank.get(item[0], len(rank)), item[0]),
    )
    product_id = next((v for (level, _), v in after.items() if level == PARENT and v), None)
    if platform_id == "ebay":
        return {"item_ids": dict(children)}, None
    if platform_id == "1nventory":
        # No product_gid key at all when no product row holds one: goat_poller reads the
        # newest success that has the key, so it falls back to the attempt that sent it.
        ids: Dict[str, Any] = {"product_gid": product_id} if product_id else {}
        ids["variant_gids"] = dict(children)
        return ids, None
    if platform_id == "grailed":
        return [v for _, v in children], None
    if platform_id in ("goat", "goat_sneakers"):
        return None, product_id
    return None, None


def plan_save(
    platform_id: str,
    *,
    parent_sku: str,
    changes: Sequence[Change],
    delist: Sequence[str],
    delist_product: bool,
    allow_partial: bool,
    excluded: bool,
    size_list: Sequence[Dict[str, Any]],
    attempts: Sequence[Dict[str, Any]],
    rows: Sequence[Dict[str, Any]],
    entries: Sequence[Dict[str, Any]],
    other_parent_rows: Sequence[Dict[str, Any]] = (),
    holders: Sequence[Dict[str, Any]] = (),
) -> SavePlan:
    """Decide the save from what was read under the lock, or raise DelistRefused.
    attempts, rows and entries are this platform's (_read_state with the platform)."""
    facts = pds.platform_facts(platform_id, size_list, attempts, rows, entries)
    refusal = pds.delist_refusal(platform_id, enabled=True, excluded=excluded, facts=facts)
    if refusal is not None and refusal.code in pds._EDIT_LOCKS:
        raise refusal
    skus = [s["sku"] for s in size_list]
    editor = pds.id_editor(
        platform_id,
        facts=facts,
        refusal=refusal,
        skus=skus,
        rows=rows,
        other_parent_rows=other_parent_rows,
    )
    fields = {}
    for c in changes:
        field = editor["product"] if c.level == PRODUCT_LEVEL else editor["sizes"].get(c.sku)
        if field is None:
            raise pds._unknown_size()
        fields[(c.level, c.sku)] = field
    if any(s not in facts.states for s in delist):
        raise pds._unknown_size()

    # KTD6: every change, unchanged ones included, must start from what is stored.
    stored = {k: comparable_id(platform_id, k[0], f["id"]) for k, f in fields.items()}
    if any(c.sent_from != stored[(c.level, c.sku)] for c in changes):
        raise DelistRefused(CHANGED_DETAIL, CHANGED)

    raw = {(r["level"], r["sku"]): r.get("external_id") for r in rows}
    clears: List[Change] = []
    edits: List[Edit] = []
    for c in changes:
        if c.new == stored[(c.level, c.sku)]:
            continue
        field = fields[(c.level, c.sku)]
        if not field["editable"]:
            # The editor's reason: "Restore first", or "Stored under <parent>" (KTD7).
            raise DelistRefused(_about(c.level, c.size, field["reason"]), field["code"])
        if c.new is None:
            clears.append(c)
        else:
            old = raw.get((_ROW_LEVEL[c.level], c.sku))
            edits.append(Edit(level=c.level, sku=c.sku, size=c.size, old=old, new=c.new))

    edited_sizes = {e.sku: e for e in edits if e.level == SIZE_LEVEL}
    both = next((s for s in delist if s in edited_sizes), None)
    if both is not None:
        size = edited_sizes[both].size
        raise DelistRefused(f"{size}: delist or edit, not both", DELIST_AND_EDIT, 400)

    whole = delist_product or any(c.level == PRODUCT_LEVEL for c in clears)
    picked = list(dict.fromkeys([*delist, *(c.sku for c in clears if c.level == SIZE_LEVEL)]))
    delist_plan = None
    if whole or picked:
        _refuse_unless_alone(edits, whole, picked, size_list, allow_partial)
        delist_plan = pds.plan_delist(
            platform_id,
            None if whole else picked,
            enabled=True,
            allow_partial=allow_partial,
            excluded=excluded,
            size_list=size_list,
            facts=facts,
            rows=rows,
        )

    after = _ids_after(rows, delist_plan, edits)
    _refuse_duplicates(parent_sku, edits, after, holders)
    external_id, goat_sku = manual_external_id(platform_id, after, skus)
    return SavePlan(delist=delist_plan, edits=edits, external_id=external_id, goat_sku=goat_sku)


# ---------------------------------------------------------------------------
# The save
# ---------------------------------------------------------------------------


async def save_listing_ids(
    parent_sku: str,
    platform_id: str,
    changes: Sequence[Dict[str, Any]],
    delist: Sequence[str],
    comment: Optional[str],
    user_id: str,
    *,
    delist_product: bool = False,
) -> Dict[str, Any]:
    """Save one platform's id changes and delists for a PARENT sku (module docstring).

    One transaction under the submit lock. Raises DelistRefused. Returns {"platform_id",
    "parent_sku", "delist_id", "submission_id", "changes"}, the ids null when nothing of
    that kind was written; the route answers with the fresh history instead."""
    saved, _product, _config = await save_listing_ids_with_inputs(
        parent_sku,
        platform_id,
        changes,
        delist,
        comment,
        user_id,
        delist_product=delist_product,
    )
    return saved


async def save_listing_ids_with_inputs(
    parent_sku: str,
    platform_id: str,
    changes: Sequence[Dict[str, Any]],
    delist: Sequence[str],
    comment: Optional[str],
    user_id: str,
    *,
    delist_product: bool = False,
) -> Tuple[Dict[str, Any], pds.ProductFacts, Tuple[List[str], Dict[str, Any]]]:
    """save_listing_ids, also returning the product and the platform config
    (_load_platform_config's pair) it loaded before the lock, so the route's history after
    the save need not load them again (pds.get_history's product and config)."""
    comment = pds.normalize_comment(comment)
    if platform_id in NEVER_GATED:
        raise DelistRefused(pds._read_only_reason(platform_id), pds.READ_ONLY, 400)
    # Read-only and outside the transaction, so they run together as in pds.get_history:
    # an unknown product answers 404 before the config read's error.
    product, config = await asyncio.gather(
        pds._load_product(parent_sku), pds._load_platform_config(), return_exceptions=True
    )
    if isinstance(product, BaseException):
        raise product
    if product is None:
        raise pds._not_found()
    if isinstance(config, BaseException):
        raise config
    platforms, settings = config
    if platform_id not in platforms:
        raise DelistRefused("Unknown platform", pds.UNKNOWN_PLATFORM, 400)
    allow_partial = pds.allow_partial_submit(settings, platform_id)
    labels = {c["sku"]: c["size"] for c in product.active}
    labels.update(product.inactive)
    parsed = parse_changes(platform_id, parent_sku, changes, labels)
    picked = [s for s in dict.fromkeys(_text(s) for s in delist) if s]
    # Only a value that differs from the one loaded can be an edit: the rest are either
    # unchanged or refused as changed since opened.
    new_ids = sorted({c.new for c in parsed if c.new is not None and c.new != c.sent_from})

    try:
        async with in_transaction("default") as conn:
            await conn.execute_query(_LOCK_TIMEOUT_SQL)
            await conn.execute_query(pds.SUBMIT_LOCK_SQL, [parent_sku])
            guard = await conn.execute_query_dict(_MIGRATION_GUARD_SQL)
            if guard and guard[0]["manual_closes"]:
                logger.error(
                    "listing ids: migrations/listing_id_edits.sql is not applied "
                    "(platform_delist_closing_attempt counts a manual row), refusing %s %s",
                    platform_id,
                    parent_sku,
                )
                raise DelistRefused("Saving IDs is not available yet", NOT_READY, 503)
            attempts, rows, entries = await pds._read_state(
                conn, parent_sku, platform_id, lock_rows=True
            )
            excluded = bool((await pds._exclusions(product, [platform_id])).get(platform_id))
            sizes = pds.size_list(product, rows, entries)
            # An inactive child appears in the history when ANY platform has a row for it,
            # so its field can be edited here even with no row on this platform.
            shown = {s["sku"] for s in sizes}
            for c in parsed:
                if c.level == SIZE_LEVEL and c.sku in product.inactive and c.sku not in shown:
                    sizes.append({"sku": c.sku, "size": product.inactive[c.sku], "active": False})
                    shown.add(c.sku)
            other_rows = await pds._read_other_parent_rows(
                conn, parent_sku, product.child_skus, [platform_id]
            )
            holders = (
                await conn.execute_query_dict(_HOLDERS_SQL, [platform_id, new_ids])
                if new_ids
                else []
            )
            plan = plan_save(
                platform_id,
                parent_sku=parent_sku,
                changes=parsed,
                delist=picked,
                delist_product=delist_product,
                allow_partial=allow_partial,
                excluded=excluded,
                size_list=sizes,
                attempts=attempts,
                rows=rows,
                entries=entries,
                other_parent_rows=other_rows,
                holders=list(holders),
            )
            delist_id, submission_id = await _write(
                conn, parent_sku, platform_id, plan, rows, comment, user_id
            )
    except DelistRefused as refused:
        logger.info(
            "listing ids refused: %s %s changes=%s delist=%s by %s: %s",
            platform_id,
            parent_sku,
            [(c.level, c.sku) for c in parsed],
            picked or ("product" if delist_product else []),
            user_id,
            refused.code,
        )
        raise
    except Exception as exc:
        # Postgres's text for a lock_timeout cancel, matched as catalog_service does: it
        # survives tortoise wrapping the driver's error.
        if "lock timeout" not in str(exc):
            raise
        logger.warning(
            "listing ids: lock timeout for %s %s by %s: %s", platform_id, parent_sku, user_id, exc
        )
        raise DelistRefused("Product is busy, try again", BUSY) from exc

    if delist_id or submission_id:
        pds._clear_summary_cache()
    logger.info(
        "listing ids %s %s by %s: %d changed, delist %s, manual row %s",
        platform_id,
        parent_sku,
        user_id,
        len(plan.edits),
        delist_id,
        submission_id,
    )
    saved = {
        "platform_id": platform_id,
        "parent_sku": parent_sku,
        "delist_id": delist_id,
        "submission_id": submission_id,
        "changes": [e.record() for e in plan.edits],
    }
    return saved, product, config


async def _write(
    conn: Any,
    parent_sku: str,
    platform_id: str,
    plan: SavePlan,
    rows: Sequence[Dict[str, Any]],
    comment: Optional[str],
    user_id: str,
) -> Tuple[Optional[str], Optional[int]]:
    """The delist, then the manual row, then the presence upsert (module docstring).
    Returns (delist id, manual row id)."""
    delist_id = None
    if plan.delist is not None:
        entry, _inserted = await pds.write_delist(
            conn,
            platform_id=platform_id,
            parent_sku=parent_sku,
            plan=plan.delist,
            rows=rows,
            comment=comment,
            user_id=user_id,
        )
        delist_id = entry["id"]
    if not plan.edits:
        return delist_id, None

    newest = await conn.execute_query_dict(_NEWEST_LISTING_SQL, [parent_sku, platform_id])
    listing_id = newest[0]["listing_id"] if newest else None
    attempt_number = int(newest[0]["attempt_number"]) if newest else 1
    meta: Dict[str, Any] = {
        "parent_sku": parent_sku,
        "manual": {
            "changes": [e.record() for e in plan.edits],
            "note": comment,
            "delist_id": delist_id,
        },
        # Seeded inline rather than via record_step: that uses its own connection and
        # would block on this uncommitted row.
        "steps": [new_step(MANUAL_STEP, attempt=attempt_number)],
    }
    if plan.goat_sku is not None:
        meta["goat_sku"] = plan.goat_sku
    inserted = await conn.execute_query_dict(
        _INSERT_MANUAL_SQL,
        [
            listing_id,
            platform_id,
            user_id,
            attempt_number,
            json.dumps(plan.external_id) if plan.external_id is not None else None,
            json.dumps(meta),
        ],
    )
    submission_id = int(inserted[0]["id"])

    written = await conn.execute_query_dict(
        _UPSERT_IDS_SQL,
        [
            platform_id,
            parent_sku,
            [_ROW_LEVEL[e.level] for e in plan.edits],
            [e.sku for e in plan.edits],
            [e.new for e in plan.edits],
            user_id,
            submission_id,
        ],
    )
    done = {r["sku"] for r in written}
    moved = next((e for e in plan.edits if e.sku not in done), None)
    if moved is not None:
        # Moved under another parent since the read: never moved back (KTD7).
        logger.warning(
            "listing ids %s %s: %s is stored under another parent now",
            platform_id,
            parent_sku,
            moved.sku,
        )
        raise DelistRefused(
            _about(moved.level, moved.size, "stored under another product"), pds.OTHER_PARENT
        )
    return delist_id, submission_id
