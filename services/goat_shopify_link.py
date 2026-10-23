"""GOAT's platform actions (platform_actions): the GOAT link on the 1nventory Shopify product.

GOAT's stage 3 (goat_poller._sync_to_1nventory) writes the GOAT ID onto the 1nventory
Shopify product as the goat.goat_sku metafield, and adds the gsync tag when GOAT ticked
STV. gsync is the only trace of STV on Shopify: no goat.* metafield holds it. Something
outside SkuBase reads both to keep the product on GOAT. So, for goat and goat_sneakers:

    after_delist     a whole-product delist removes the metafield and the gsync tag, and
                     records what came off on the entry (platform_delists.platform_action)
    after_restore    Restore puts exactly that back
    after_id_change  a GOAT ID saved by hand is written to the metafield; gsync is left
                     as it is, because a hand-saved ID says nothing about STV

Each runs after the SkuBase change commits, and the dispatcher logs anything it raises.

THE STV SWITCH (get_stv, set_stv; GOAT only, STV_PLATFORMS). The Listings tab reads gsync
live and adds or removes it: the tag IS the product's STV. SkuBase's own note of it, the
GOAT presence row's external_meta.stv, is nearly empty on prod (backfilled rows never had
one), so nothing reads it; set_stv merges the confirmed value into it with stv_edit
{"at", "by", "previous"}. The switch is locked unless the parent is listed on GOAT and
settled (platform_delist_service.listing_lock), then needs the 1nventory product (WHICH
PRODUCT) and [goat] execute. A change is read back from Shopify before it is answered.

WHICH PRODUCT. 1nventory's product presence row, then 1nventory's latest successful
submission (what goat_poller._product_gids reads), then a variant SKU search. A product
is used only when one of its variant SKUs is a child of the parent: a mistyped 1nventory
id must not touch another product's GOAT link.

NOTHING WRITTEN, and why, logged (and on the entry for a delist):
    a delist of some sizes (the product is still on GOAT)
    the parent still listed on the other GOAT platform (both share the metafield and tag)
    [<platform>] execute false, the same switch stage 3 obeys
    no product on Shopify, or nothing on it to remove or change

THE DELIST RECORD, platform_delists.platform_action:

    {"action": "goat_shopify_link", "store", "product_gid",
     "found_by": "presence" | "submission" | "sku",
     "metafield": {"namespace", "key", "type", "value"} | null,   # what came off
     "tags": ["gsync"] | [],                                       # what came off
     "status": "pending" | "removed" | "failed" | "nothing" | "no_product"
               | "unverified" | "skipped",
     "done": {"metafield": bool, "tags": [..]},                    # on "failed"
     "reason", "error", "foreign_gids", "at",
     "restore": {"status": "restored" | "failed" | "no_product" | "skipped",
                 "metafield": bool, "tags": [..], "reason", "error", "at"}}

    Written as "pending" BEFORE the first Shopify write, so nothing comes off unrecorded.
    A failure keeps the whole pre-image, with "done" saying how far it got: a timed-out
    call may still have landed, and putting back what never came off is a no-op, because
    Restore sets the metafield only while the product has none (a newer value wins) and
    adds only the tags it lacks.

THE ID CHANGE RECORD, steps on the manual row: SYNCED_STEP {"goat_sku", "previous"} with
platform_meta.product_gid, as stage 3 records it, or SYNC_FAILED_STEP {"error"}.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from tortoise import connections

from config import config
from services.goat_poller import GOAT, GOAT_SNEAKERS, SYNC_STORE
from services.shopify_admin import ShopifyAdmin
from services.shopify_client import enable_writes, get_shopify_client, writes_enabled
from utils.submission_steps import record_step

logger = logging.getLogger(__name__)

# The two GOAT platforms, by platform id: their config section and label.
VARIANTS = {v.platform_id: v for v in (GOAT, GOAT_SNEAKERS)}

ACTION = "goat_shopify_link"

# Stage 3's step name, so a hand-saved ID reads like a synced one on the timeline.
SYNCED_STEP = "1nventory_synced"
SYNC_FAILED_STEP = "1nventory_sync_failed"

# platform_delist_service.PRODUCT_LEVEL: the GOAT ID is the product-level id.
PRODUCT_LEVEL = "product"

# Stored errors are for a person reading the row, not a full trace.
ERROR_MAX = 300

# Statuses whose metafield and tags Restore puts back. "pending" is a removal that died
# part way: its record holds everything it was about to remove.
RESTORABLE = ("removed", "failed", "pending")

_PRODUCT_GID = "gid://shopify/Product/"

_SIBLING_SQL = """
SELECT 1 FROM external_listing_ids
 WHERE platform_id = ANY($1::text[]) AND parent_sku = $2
 LIMIT 1
"""

# 1nventory's own ids for the parent, best first: the presence row (an id saved by hand
# lands here), then the latest successful submission on any of the parent's listings.
_STORED_GIDS_SQL = """
SELECT external_id AS gid, 'presence' AS found_by, 0 AS rank, NULL::timestamptz AS at
  FROM external_listing_ids
 WHERE platform_id = '1nventory' AND level = 'parent' AND parent_sku = $1
UNION ALL
SELECT s.external_id ->> 'product_gid', 'submission', 1, s.created_at
  FROM listing_submissions s
  JOIN listings l ON l.id = s.listing_id
 WHERE l.product_id = $1 AND s.platform_id = '1nventory' AND s.status = 'success'
   AND s.external_id ? 'product_gid'
 ORDER BY rank, at DESC NULLS LAST
"""

_RECORD_SQL = "UPDATE platform_delists SET platform_action = $2::jsonb WHERE id = $1::uuid"

_RECORD_RESTORE_SQL = """
UPDATE platform_delists
   SET platform_action = platform_action || jsonb_build_object('restore', $2::jsonb)
 WHERE id = $1::uuid
"""

_READ_SQL = "SELECT platform_action FROM platform_delists WHERE id = $1::uuid"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _error(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"[:ERROR_MAX]


def _settings(platform_id: str) -> Dict[str, Any]:
    section = config.get(VARIANTS[platform_id].config_section, {})
    return {
        "namespace": section.get("metafield_namespace", "goat"),
        "key": section.get("metafield_key", "goat_sku"),
        "tag": section.get("gsync_tag", "gsync"),
        "execute": bool(section.get("execute", False)),
    }


def owns(product: Dict[str, Any], child_skus: Sequence[str]) -> bool:
    """Is this Shopify product the parent's: does it carry one of its child SKUs."""
    return bool(set(product.get("skus") or []) & set(child_skus))


def removal(product: Dict[str, Any], namespace: str, key: str, tag: str) -> Dict[str, Any]:
    """What a delist removes from this product: the metafield if set, the tag if there."""
    field = product.get("metafield")
    return {
        "metafield": (
            {"namespace": namespace, "key": key, "type": field.get("type"), "value": field.get("value")}
            if field
            else None
        ),
        "tags": [tag] if tag in (product.get("tags") or []) else [],
    }


def put_back(record: Dict[str, Any], product: Dict[str, Any]) -> Dict[str, Any]:
    """What Restore writes: the removed metafield unless the product has one again, and
    the removed tags it lacks."""
    field = record.get("metafield")
    tags = set(product.get("tags") or [])
    return {
        "metafield": field if field and not product.get("metafield") else None,
        "tags": [t for t in record.get("tags") or [] if t not in tags],
    }


def new_goat_id(changes: Sequence[Dict[str, Any]]) -> Optional[str]:
    """The GOAT ID an ID save set, or None when it set none."""
    return next(
        (c["new"] for c in changes if c.get("level") == PRODUCT_LEVEL and c.get("new")), None
    )


def _as_json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _metafield_input(gid: str, namespace: str, key: str, type_: Optional[str], value: str):
    return {
        "ownerId": gid,
        "namespace": namespace,
        "key": key,
        "type": type_ or "single_line_text_field",
        "value": value,
    }


async def _set_metafield(admin: ShopifyAdmin, entry: Dict[str, Any]) -> None:
    failures = await admin.set_metafields([entry])
    if failures:
        raise RuntimeError(next(iter(failures.values())))


async def _record(conn: Any, delist_id: str, record: Dict[str, Any]) -> None:
    await conn.execute_query(_RECORD_SQL, [delist_id, json.dumps({"action": ACTION, **record})])


async def _admin() -> ShopifyAdmin:
    return ShopifyAdmin(await get_shopify_client(SYNC_STORE))


def _arm(platform_id: str) -> None:
    if not writes_enabled():
        enable_writes(f"{VARIANTS[platform_id].label} platform action")


async def _find_product(
    conn: Any,
    admin: ShopifyAdmin,
    parent_sku: str,
    child_skus: Sequence[str],
    namespace: str,
    key: str,
) -> tuple[Optional[Dict[str, Any]], Optional[str], List[str]]:
    """(product, found_by, stored gids that were not the parent's). See WHICH PRODUCT."""
    stored = await conn.execute_query_dict(_STORED_GIDS_SQL, [parent_sku])
    seen: List[str] = []
    foreign: List[str] = []
    for row in stored:
        gid = row.get("gid")
        if not gid or not gid.startswith(_PRODUCT_GID) or gid in seen:
            continue
        seen.append(gid)
        product = await admin.product_metafield(gid, namespace, key)
        if product is None:
            continue
        if owns(product, child_skus):
            return product, row["found_by"], foreign
        foreign.append(gid)
    if child_skus:
        node = await admin.find_product_by_variant_sku(child_skus)
        if node and node["id"] not in seen:
            product = await admin.product_metafield(node["id"], namespace, key)
            if product is not None and owns(product, child_skus):
                return product, "sku", foreign
    return None, None, foreign


# ---------------------------------------------------------------------------
# after_delist
# ---------------------------------------------------------------------------


async def after_delist(
    *,
    platform_id: str,
    delist_id: str,
    parent_sku: str,
    scope: Optional[Sequence[str]],
    child_skus: Sequence[str],
) -> None:
    """Take the GOAT link off the product's Shopify listing (module docstring)."""
    if scope is not None:
        logger.info(
            "goat shopify: delist %s of %s on %s covers some sizes, Shopify left linked",
            delist_id,
            parent_sku,
            platform_id,
        )
        return
    conn = connections.get("default")
    settings = _settings(platform_id)
    siblings = [p for p in VARIANTS if p != platform_id]
    if await conn.execute_query_dict(_SIBLING_SQL, [siblings, parent_sku]):
        await _record(
            conn,
            delist_id,
            {"status": "skipped", "reason": "listed on the other GOAT platform", "at": _now()},
        )
        logger.info(
            "goat shopify: %s still listed on %s, Shopify left linked", parent_sku, siblings
        )
        return
    if not settings["execute"]:
        await _record(
            conn,
            delist_id,
            {"status": "skipped", "reason": f"[{platform_id}] execute is false", "at": _now()},
        )
        logger.warning(
            "goat shopify: [%s] execute is false, %s left linked on Shopify",
            platform_id,
            parent_sku,
        )
        return

    admin = await _admin()
    product, found_by, foreign = await _find_product(
        conn, admin, parent_sku, child_skus, settings["namespace"], settings["key"]
    )
    if product is None:
        status = "unverified" if foreign else "no_product"
        await _record(
            conn,
            delist_id,
            {"status": status, "store": SYNC_STORE, "foreign_gids": foreign, "at": _now()},
        )
        if foreign:
            logger.warning(
                "goat shopify: %s 1nventory ids %s carry none of its SKUs, nothing removed",
                parent_sku,
                foreign,
            )
        else:
            logger.info("goat shopify: %s is not on Shopify, nothing to remove", parent_sku)
        return

    gid = product["id"]
    planned = removal(product, settings["namespace"], settings["key"], settings["tag"])
    record: Dict[str, Any] = {
        "store": SYNC_STORE,
        "product_gid": gid,
        "found_by": found_by,
        **planned,
        "status": "pending",
        "at": _now(),
    }
    if not planned["metafield"] and not planned["tags"]:
        record["status"] = "nothing"
        await _record(conn, delist_id, record)
        return
    # Recorded before any write, so Restore always knows what came off.
    await _record(conn, delist_id, record)

    _arm(platform_id)
    done: Dict[str, Any] = {"metafield": False, "tags": []}
    try:
        if planned["metafield"]:
            await admin.delete_metafields(
                [{"ownerId": gid, "namespace": settings["namespace"], "key": settings["key"]}]
            )
            done["metafield"] = True
        if planned["tags"]:
            await admin.remove_tags(gid, planned["tags"])
            done["tags"] = planned["tags"]
    except Exception as exc:
        record.update(status="failed", done=done, error=_error(exc))
        await _record(conn, delist_id, record)
        logger.error(
            "goat shopify: delist %s of %s, removed %s from %s before %s",
            delist_id,
            parent_sku,
            done,
            gid,
            record["error"],
        )
        return
    record["status"] = "removed"
    await _record(conn, delist_id, record)
    logger.info(
        "goat shopify: delist %s of %s removed %s from %s", delist_id, parent_sku, done, gid
    )


# ---------------------------------------------------------------------------
# after_restore
# ---------------------------------------------------------------------------


async def after_restore(*, platform_id: str, delist_id: str, parent_sku: str) -> None:
    """Put back what this entry's after_delist took off Shopify (module docstring)."""
    conn = connections.get("default")
    rows = await conn.execute_query_dict(_READ_SQL, [delist_id])
    record = (_as_json(rows[0].get("platform_action")) if rows else None) or {}
    if record.get("action") != ACTION or record.get("status") not in RESTORABLE:
        return
    if not (record.get("metafield") or record.get("tags")):
        return

    async def result(outcome: Dict[str, Any]) -> None:
        await conn.execute_query(
            _RECORD_RESTORE_SQL, [delist_id, json.dumps({**outcome, "at": _now()})]
        )

    settings = _settings(platform_id)
    if not settings["execute"]:
        await result({"status": "skipped", "reason": f"[{platform_id}] execute is false"})
        logger.warning(
            "goat shopify: [%s] execute is false, restored delist %s of %s not relinked",
            platform_id,
            delist_id,
            parent_sku,
        )
        return

    gid = record["product_gid"]
    field = record.get("metafield") or {}
    admin = await _admin()
    product = await admin.product_metafield(
        gid, field.get("namespace") or settings["namespace"], field.get("key") or settings["key"]
    )
    if product is None:
        await result({"status": "no_product"})
        logger.warning(
            "goat shopify: %s is gone, restored delist %s of %s not relinked",
            gid,
            delist_id,
            parent_sku,
        )
        return

    todo = put_back(record, product)
    done: Dict[str, Any] = {"metafield": False, "tags": []}
    _arm(platform_id)
    try:
        if todo["metafield"]:
            m = todo["metafield"]
            await _set_metafield(
                admin, _metafield_input(gid, m["namespace"], m["key"], m.get("type"), m["value"])
            )
            done["metafield"] = True
        if todo["tags"]:
            await admin.add_tags(gid, todo["tags"])
            done["tags"] = todo["tags"]
    except Exception as exc:
        await result({"status": "failed", **done, "error": _error(exc)})
        logger.error(
            "goat shopify: restored delist %s of %s, put back %s on %s before %s",
            delist_id,
            parent_sku,
            done,
            gid,
            _error(exc),
        )
        return
    await result({"status": "restored", **done})
    logger.info(
        "goat shopify: restored delist %s of %s, put back %s on %s",
        delist_id,
        parent_sku,
        done,
        gid,
    )


# ---------------------------------------------------------------------------
# after_id_change
# ---------------------------------------------------------------------------


async def after_id_change(
    *,
    platform_id: str,
    parent_sku: str,
    child_skus: Sequence[str],
    changes: Sequence[Dict[str, Any]],
    submission_id: int,
) -> None:
    """Write a GOAT ID saved by hand to the metafield (module docstring)."""
    goat_id = new_goat_id(changes)
    if not goat_id:
        return
    settings = _settings(platform_id)
    if not settings["execute"]:
        logger.warning(
            "goat shopify: [%s] execute is false, %s's new GOAT ID not written to Shopify",
            platform_id,
            parent_sku,
        )
        return

    conn = connections.get("default")
    admin = await _admin()
    product, _found_by, foreign = await _find_product(
        conn, admin, parent_sku, child_skus, settings["namespace"], settings["key"]
    )
    if product is None:
        log = logger.warning if foreign else logger.info
        log(
            "goat shopify: no Shopify product for %s (foreign ids %s), GOAT ID not written",
            parent_sku,
            foreign,
        )
        return

    gid = product["id"]
    previous = (product.get("metafield") or {}).get("value")
    if previous == goat_id:
        logger.info("goat shopify: %s already carries GOAT ID %s", gid, goat_id)
        return
    _arm(platform_id)
    try:
        await _set_metafield(
            admin,
            _metafield_input(
                gid,
                settings["namespace"],
                settings["key"],
                (product.get("metafield") or {}).get("type"),
                goat_id,
            ),
        )
    except Exception as exc:
        await record_step(submission_id, SYNC_FAILED_STEP, product_gid=gid, error=_error(exc))
        logger.error(
            "goat shopify: GOAT ID %s for %s not written to %s: %s",
            goat_id,
            parent_sku,
            gid,
            _error(exc),
        )
        return
    await record_step(
        submission_id,
        SYNCED_STEP,
        meta={"product_gid": gid},
        goat_sku=goat_id,
        previous=previous,
    )
    logger.info(
        "goat shopify: %s GOAT ID %s -> %s on %s", parent_sku, previous, goat_id, gid
    )


# ---------------------------------------------------------------------------
# The STV switch
# ---------------------------------------------------------------------------

# GOAT only: GOAT Sneakers' sheet has had no STV since 2026-10-23.
STV_PLATFORMS = (GOAT.platform_id,)

NOT_LISTED_REASON = "Not listed on GOAT"
NOT_ENABLED_REASON = "GOAT is not enabled"
NO_PRODUCT_REASON = "Not on 1nventory"
SYNC_OFF_REASON = "Shopify sync is off"
CHECK_FAILED = "Could not check Shopify"
UPDATE_FAILED = "Could not update Shopify"

# Refusal codes, beside platform_delist_service's.
NO_PRODUCT = "no_product"
SYNC_OFF = "sync_off"
SHOPIFY_FAILED = "shopify_failed"

# The key set_stv merges into the GOAT presence row's external_meta beside stv.
STV_EDIT_META = "stv_edit"

# Leaves the presence rows' recompute triggers alone: they fire on insert, delete and a
# parent_sku change only.
_STV_RECORD_SQL = """
UPDATE external_listing_ids
   SET external_meta = external_meta || $3::jsonb, updated_at = now()
 WHERE platform_id = $1 AND level = 'parent' AND parent_sku = $2
"""


def _delists():
    """platform_delist_service, imported at call time: it imports platform_actions, which
    imports this module, so an import at the top breaks whichever of the two loads first."""
    from services import platform_delist_service

    return platform_delist_service


def stv_answer(stv: Optional[bool], reason: Optional[str] = None) -> Dict[str, Any]:
    """The switch's payload: stv None when unknown; can_change only with no reason."""
    return {"stv": stv, "can_change": reason is None and stv is not None, "reason": reason}


def stv_lock_reason(refusal: Any) -> str:
    """platform_delist_service.listing_lock's refusal in the switch's words."""
    pds = _delists()
    if refusal.code in (pds.NOT_LISTED, pds.ALREADY_DELISTED):
        return NOT_LISTED_REASON
    if refusal.code == pds.UNKNOWN_PLATFORM:
        return NOT_ENABLED_REASON
    return refusal.message


async def _stv_product(parent_sku: str, child_skus: Sequence[str], settings: Dict[str, Any]):
    """(admin, the parent's 1nventory product or None). Raises what Shopify raises."""
    admin = await _admin()
    product, _found_by, foreign = await _find_product(
        connections.get("default"),
        admin,
        parent_sku,
        child_skus,
        settings["namespace"],
        settings["key"],
    )
    if product is None and foreign:
        logger.warning("goat stv: %s 1nventory ids %s carry none of its SKUs", parent_sku, foreign)
    return admin, product


async def get_stv(parent_sku: str, platform_id: str = GOAT.platform_id) -> Dict[str, Any]:
    """The parent's STV as Shopify has it now: gsync on its 1nventory product.

    Asks Shopify nothing unless the parent is listed on GOAT and settled (not delisted, no
    submission in flight or awaiting action). A Shopify read that fails answers stv None
    with a reason, so the Listings tab still draws.
    """
    product, refusal = await _delists().listing_lock(parent_sku, platform_id)
    if refusal is not None:
        return stv_answer(None, stv_lock_reason(refusal))
    settings = _settings(platform_id)
    try:
        _, found = await _stv_product(parent_sku, product.child_skus, settings)
    except Exception:
        logger.exception("goat stv: reading %s from Shopify failed", parent_sku)
        return stv_answer(None, CHECK_FAILED)
    if found is None:
        return stv_answer(None, NO_PRODUCT_REASON)
    stv = settings["tag"] in found["tags"]
    return stv_answer(stv, None if settings["execute"] else SYNC_OFF_REASON)


async def _record_stv(platform_id: str, parent_sku: str, stv: bool, previous: bool, user_id: Any):
    """Note the confirmed value on the GOAT presence row, with who and when. Never raises:
    Shopify already holds the change, and that is what the tab reads. Nothing reads this
    note back (a later read-back writes GOAT's sheet value over stv)."""
    meta = {
        "stv": stv,
        STV_EDIT_META: {
            "at": _now(),
            "by": str(user_id) if user_id is not None else None,
            "previous": previous,
        },
    }
    try:
        affected, _rows = await connections.get("default").execute_query(
            _STV_RECORD_SQL, [platform_id, parent_sku, json.dumps(meta)]
        )
    except Exception:
        logger.exception("goat stv: %s set on Shopify, presence row not updated", parent_sku)
        return
    if not affected:
        logger.info("goat stv: %s has no %s presence row, nothing noted", parent_sku, platform_id)


async def set_stv(
    parent_sku: str, stv: bool, user_id: Any, platform_id: str = GOAT.platform_id
) -> Dict[str, Any]:
    """Turn STV on or off: add or remove gsync on the parent's 1nventory product, read the
    product back, and answer with what Shopify now holds, never with what was asked for.

    Raises platform_delist_service.DelistRefused: 409 while the switch is locked (not listed
    and settled on GOAT, not on 1nventory, [<platform>] execute false), 502 when Shopify
    failed or did not take the change, with nothing noted. The check and the write are not
    under the submit lock: a submission starting in between can only lead to stage 3 adding
    gsync later, which it does whenever GOAT ticks STV anyway.
    """
    pds = _delists()
    product, refusal = await pds.listing_lock(parent_sku, platform_id)
    if refusal is not None:
        raise pds.DelistRefused(stv_lock_reason(refusal), refusal.code)
    settings = _settings(platform_id)
    if not settings["execute"]:
        raise pds.DelistRefused(SYNC_OFF_REASON, SYNC_OFF)
    try:
        admin, found = await _stv_product(parent_sku, product.child_skus, settings)
    except Exception:
        logger.exception("goat stv: reading %s from Shopify failed", parent_sku)
        raise pds.DelistRefused(CHECK_FAILED, SHOPIFY_FAILED, 502)
    if found is None:
        raise pds.DelistRefused(NO_PRODUCT_REASON, NO_PRODUCT)

    gid, tag = found["id"], settings["tag"]
    previous = tag in found["tags"]
    if previous == stv:
        return stv_answer(stv)

    _arm(platform_id)
    try:
        if stv:
            await admin.add_tags(gid, [tag])
        else:
            await admin.remove_tags(gid, [tag])
        after = await admin.product_metafield(gid, settings["namespace"], settings["key"])
    except Exception:
        logger.exception("goat stv: setting %s to %s on %s failed", parent_sku, stv, gid)
        raise pds.DelistRefused(UPDATE_FAILED, SHOPIFY_FAILED, 502)
    now = after is not None and tag in after["tags"]
    if now != stv:
        logger.error(
            "goat stv: %s read back %s on %s after setting %s",
            parent_sku,
            "no product" if after is None else now,
            gid,
            stv,
        )
        raise pds.DelistRefused(UPDATE_FAILED, SHOPIFY_FAILED, 502)

    await _record_stv(platform_id, parent_sku, stv, previous, user_id)
    logger.info("goat stv: %s %s -> %s on %s by %s", parent_sku, previous, stv, gid, user_id)
    return stv_answer(stv)
