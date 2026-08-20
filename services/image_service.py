import asyncio
import hashlib
import io
import json
import logging
import re
import struct
import traceback
import uuid
from typing import Any, Dict, List, Optional, TypedDict

from gcloud.aio.storage import Storage
from tortoise import Tortoise, connections

from config import config
from services import gallery_image_sync_queue
from utils.image_processor import (
    load_resolutions_config,
    load_washtag_resolutions_config,
    process_image_resolutions,
)

logger = logging.getLogger(__name__)

GCS_BUCKET = config.get("gcs_bucket_products", "lux_products")
GCS_BASE_URL = f"https://storage.googleapis.com/{GCS_BUCKET}"
SERVICE_ACCOUNT_FILE = config.get("gcs_service_account", "service-account-2.json")

# These URLs are stable but their bytes are not: a re-shoot or a gallery edit replaces
# the object in place. "no-cache" does not mean "do not cache", it means "cache it, then
# revalidate before each use", so a browser keeps the bytes and spends one conditional
# request per view, getting a 304 with no body while the image is unchanged. The previous
# "max-age=31536000, immutable" promised the opposite and was why an edited image kept
# showing the old photo for a year, in the UI and on every platform that fetched the URL.
GCS_CACHE_CONTROL = "public, no-cache"
MAX_PRODUCT_IMAGES = 8
MAX_WASHTAG_IMAGES = 3
MAX_CONCURRENT_RESIZE = 3
# gcloud-aio defaults to 10s per call and these run while a pooled connection
# from a 5-connection pool is held, so an unbounded stall starves every other
# photography_db reader.
GCS_TIMEOUT_SECONDS = 15

# "/" is allowed because a parent SKU can legitimately contain one: ESSX parents are
# ESSX/BRAND/SEASON/STYLE/COLOUR, the photography app writes them into
# productimages.product_id as-is, and GCS already holds blobs under those prefixes.
# The traversal defence is not this whitelist, it is the explicit checks in
# validate_product_id below, which stay.
# \A and \Z rather than ^ and $: Python's $ also matches just before a trailing
# newline, so "SKU-0001\n" passed this check and reached the log lines and the GCS
# object name below.
PRODUCT_ID_PATTERN = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9\-_/]{0,199}\Z")

# The photography app keys its rows on (product_id, batch_id, image_source), so a
# product accumulates rows rather than having one, and the two sections have
# different owners: washtags are uploaded once, during batch creation, onto that
# batch's "batch_creation" row, while the retouched product photos land later on a
# separate "upload" row that is always created with washtag_count = 0 and no
# washtag_data. A product's images and its washtags therefore live on two
# different rows and each section is resolved on its own.
#
# Each section only considers the sources that publish it. That is what makes
# ordering by updated_at safe: updated_at is per row, so writing washtags moves it
# for the whole row, but a batch_creation row is never a candidate for images, so
# that write cannot pull the image section onto a stale row. updated_at is
# preferred over created_at because the photography app updates rows in place when
# a batch is re-processed, and only updated_at reflects that rewrite.
#
# Resolution is deliberately not scoped to the latest batch. GCS blobs are keyed
# by product_id alone, so each section shows whatever wrote that blob namespace
# last, and for a product shot twice the two shoots can win different sections.
#
# Writes follow the same split. Image saves only ever land on a row from
# IMAGE_SOURCES: an existing one when there is one, otherwise a fresh "upload" row
# with source_id "manual" and no batch_id. They never write to a batch_creation
# row, because the photography app owns it and rewrites it whenever the batch is
# re-processed. Washtag saves stay on the batch_creation row for the same reason in
# reverse: it is the row the photography app looks for.
IMAGE_SOURCES = ("upload", "manual")
WASHTAG_SOURCE = "batch_creation"

_resize_semaphore = asyncio.Semaphore(MAX_CONCURRENT_RESIZE)

# Distinguishes "the caller is not asserting anything about the current state" from
# "the caller read no washtag row and is asserting that is still true". Both were None
# before, which made every destination that already had washtags look like a conflict.
_UNSET = object()


def _as_list(value) -> List[Dict]:
    if not value:
        return []
    if isinstance(value, str):
        value = json.loads(value)
    return value or []


def _resolve_rows(rows: List[Any]) -> tuple:
    """Return (image_row, washtag_row) from a product's rows, most recent first.

    Each section prefers the newest row whose source publishes that section and
    that actually holds data, then degrades: any row holding data at all, so a
    product whose only row is a batch_creation row still shows its images; and for
    washtags, an empty batch_creation row before the image row, so a first-time
    washtag upload lands on the row the photography app owns and a later washtag
    run updates it instead of creating a second copy that would shadow ours.
    """
    if not rows:
        return None, None

    image_row = (
        next((r for r in rows
              if r["image_source"] in IMAGE_SOURCES and _as_list(r["image_data"])), None)
        or next((r for r in rows if _as_list(r["image_data"])), None)
        or rows[0]
    )
    washtag_row = (
        next((r for r in rows
              if r["image_source"] == WASHTAG_SOURCE and _as_list(r["washtag_data"])), None)
        or next((r for r in rows if _as_list(r["washtag_data"])), None)
        or next((r for r in rows if r["image_source"] == WASHTAG_SOURCE), None)
        or image_row
    )
    return image_row, washtag_row


class WashtagSelection(TypedDict):
    """One pick in a washtag replacement: which parent, which slot, what we expect there."""

    product_id: str
    index: int  # 1-based, matching get_product_images and the washtag_{i}.jpg blob name
    md5_hash: Optional[str]


def _plan_washtag_copy(
    selections: List[Dict],
    washtags_by_product: Dict[str, List[Dict]],
) -> tuple:
    """Turn an ordered selection into (new washtag_data, source blob paths).

    Pure: no DB, no GCS, so the ordering and provenance rules are checkable without
    either. Position in `selections` is the destination slot, so the returned lists are
    parallel and 0-indexed here but 1-indexed as blob names.

    Raises ValueError for an unknown source or an out-of-range index; callers map that
    to a 400.

    The source entry's `id` is deliberately NOT carried over. It is a foreign key into
    the photography app's WashTag table, and washtag_ai_processor resolves it with
    `WashTag.filter(id__in=...).order_by("index")` before rebuilding URLs from the
    *destination* product_id. Copying it verbatim would point the destination's AI
    analysis at the source parent's rows and re-order the result by the source's index.
    "manual" is the shape this file already writes for operator-supplied entries, so
    every existing consumer already tolerates it; provenance moves to `copied_from`,
    which nothing dereferences.
    """
    new_data: List[Dict] = []
    blob_paths: List[str] = []

    for slot, sel in enumerate(selections, start=1):
        pid = sel["product_id"]
        idx = sel["index"]

        if isinstance(idx, bool) or not isinstance(idx, int):
            raise ValueError(f"Washtag slot must be a whole number, got {idx!r}")

        source = washtags_by_product.get(pid)
        if source is None:
            raise ValueError(f"No washtags found for {pid}")
        if not 1 <= idx <= len(source):
            raise ValueError(f"{pid} has no washtag {idx}")

        entry = source[idx - 1]
        expected = sel.get("md5_hash")
        actual = entry.get("md5_hash")
        # Only assert when both sides have a hash. The photography writer leaves it
        # None when encoding fails, and the April backfill left it null wholesale, so
        # requiring it would reject legitimate rows.
        if expected and actual and expected != actual:
            raise ValueError(f"{pid} washtag {idx} changed since it was loaded")

        new_data.append(
            {
                "id": "manual",
                "shot_type": entry.get("shot_type"),
                "md5_hash": actual,
                "copied_from": {
                    "product_id": pid,
                    "index": idx,
                    "washtag_id": entry.get("id"),
                },
            }
        )
        blob_paths.append(f"{pid}/washtag_{idx}.jpg")

    return new_data, blob_paths


async def _ensure_pool(conn) -> None:
    """Force Tortoise to build the asyncpg pool before anyone reaches into it.

    Tortoise creates the pool lazily on the first query, so on a freshly started
    process conn._pool is still None and conn._pool.acquire() raises AttributeError.
    In the API a read has usually run first, which masks it, and that is what makes it
    unpleasant: it only bites the first write after a restart, and both write paths
    below swallow the failure into a logged warning.
    """
    if getattr(conn, "_pool", None) is None:
        await conn.execute_query("SELECT 1")


def validate_product_id(product_id: str) -> str:
    """Validate a product id used as a GCS blob prefix and as an advisory lock key.

    Callers must use the RETURNED value, not the one they passed in: two spellings of
    the same product would otherwise take different locks while writing the same blobs.
    """
    if (
        not product_id
        or not PRODUCT_ID_PATTERN.match(product_id)
        # Anywhere, not just as a whole segment: this is what keeps a "/" from being
        # usable to climb out of the product's own blob namespace.
        or ".." in product_id
        or "//" in product_id
        or product_id.endswith("/")
    ):
        raise ValueError(f"Invalid product_id format: {product_id}")
    return product_id


def _product_lock_key(product_id: str) -> int:
    digest = hashlib.sha256(product_id.encode()).digest()
    return struct.unpack(">q", digest[:8])[0]


class ImageService:
    _storage: Optional[Storage] = None

    async def initialize(self) -> None:
        self._storage = Storage(service_file=SERVICE_ACCOUNT_FILE)
        logger.info("ImageService initialized")

    async def close(self) -> None:
        if self._storage:
            await self._storage.close()
            self._storage = None
        logger.info("ImageService closed")

    def _get_conn(self):
        return Tortoise.get_connection("photography_db")

    # ── GET ──────────────────────────────────────────────────────────

    async def get_product_images(self, product_id: str) -> Dict[str, Any]:
        product_id = validate_product_id(product_id)
        conn = self._get_conn()

        rows = await conn.execute_query_dict(
            """
            SELECT id, product_id, image_source, product_images_count, image_data,
                   washtag_count, washtag_data, product_type, updated_at
            FROM productimages
            WHERE product_id = $1
            ORDER BY updated_at DESC
            """,
            [product_id],
        )

        if not rows:
            return {
                "product_id": product_id,
                "product_type": None,
                "updated_at": None,
                "washtag_updated_at": None,
                "images": [],
                "washtags": [],
                "image_count": 0,
                "washtag_count": 0,
            }

        image_row, washtag_row = _resolve_rows(rows)
        image_data = _as_list(image_row["image_data"])
        washtag_data = _as_list(washtag_row["washtag_data"])

        images = []
        for i, entry in enumerate(image_data, start=1):
            images.append({
                "index": i,
                "id": entry.get("id"),
                "shot_type": entry.get("shot_type"),
                "md5_hash": entry.get("md5_hash"),
                "urls": {
                    "300": f"{GCS_BASE_URL}/{product_id}/{i}_300.jpg",
                    "600": f"{GCS_BASE_URL}/{product_id}/{i}_600.jpg",
                    "1500": f"{GCS_BASE_URL}/{product_id}/{i}_1500.jpg",
                },
            })

        washtags = []
        for i, entry in enumerate(washtag_data, start=1):
            washtags.append({
                "index": i,
                "id": entry.get("id"),
                "shot_type": entry.get("shot_type"),
                "md5_hash": entry.get("md5_hash"),
                "url": f"{GCS_BASE_URL}/{product_id}/washtag_{i}.jpg",
            })

        return {
            "product_id": product_id,
            "product_type": image_row.get("product_type") or washtag_row.get("product_type"),
            "updated_at": image_row["updated_at"].isoformat() if image_row["updated_at"] else None,
            "washtag_updated_at": washtag_row["updated_at"].isoformat() if washtag_row["updated_at"] else None,
            "images": images,
            "washtags": washtags,
            "image_count": len(images),
            "washtag_count": len(washtags),
        }

    # ── SAVE (batch: reorder + upload + delete) ──────────────────────

    async def save_product_images(
        self,
        product_id: str,
        updated_at: str,
        new_order: List[str],
        deleted_indices: List[int],
        new_files: List[Dict],
        image_type: str = "image",
        shot_types: Dict[str, str] = None,
        product_type: str = None,
    ) -> Dict[str, Any]:
        product_id = validate_product_id(product_id)
        conn = self._get_conn()
        lock_key = _product_lock_key(product_id)

        try:
            await _ensure_pool(conn)
            # Hold a session-scoped advisory lock on a single pooled connection for the
            # entire fetch → validate → GCS → UPDATE flow, so concurrent saves for the
            # same product can't race on GCS or clobber each other's DB writes.
            async with conn._pool.acquire() as raw_conn:
                acquired = await raw_conn.fetchval(
                    "SELECT pg_try_advisory_lock($1)", lock_key
                )
                if not acquired:
                    return {
                        "success": False,
                        "error": "Product images are being modified by another process",
                        "status_code": 409,
                    }

                try:
                    rows = await raw_conn.fetch(
                        """
                        SELECT id, image_source, image_data, washtag_data,
                               product_images_count, washtag_count, product_type,
                               updated_at
                        FROM productimages
                        WHERE product_id = $1
                        ORDER BY updated_at DESC
                        """,
                        product_id,
                    )

                    # Edit the row that actually owns this section, which for
                    # washtags is usually not the newest row. See _resolve_rows.
                    image_row, washtag_row = _resolve_rows(rows)
                    # The row the client read. Its updated_at is what came back in the
                    # form and its data is what new_order indexes into, even when the
                    # write itself lands on a different row.
                    source_row = image_row if image_type == "image" else washtag_row

                    db_updated_at = (
                        source_row["updated_at"].isoformat()
                        if source_row and source_row["updated_at"]
                        else None
                    )
                    if updated_at and db_updated_at and updated_at != db_updated_at:
                        return {
                            "success": False,
                            "error": "Product images were updated by another user. Refreshing...",
                            "status_code": 409,
                        }

                    data_field = "image_data" if image_type == "image" else "washtag_data"
                    current_data = (source_row[data_field] if source_row else None) or []
                    if isinstance(current_data, str):
                        current_data = json.loads(current_data)

                    resolutions = load_resolutions_config() if image_type == "image" else load_washtag_resolutions_config()

                    row = source_row
                    # An existing upload or manual row is ours to update, partial
                    # replacements included, so slots the operator kept stay where the
                    # rest of the set already lives. Only take a new row when there is
                    # none, or when the image section resolved onto a batch_creation
                    # row: that one belongs to the photography app, which rewrites it
                    # in place every time the batch is re-processed, so writing there
                    # would let a re-process silently replace the operator's edits.
                    if row is None or (
                        image_type == "image" and row["image_source"] not in IMAGE_SOURCES
                    ):
                        # 'upload' so everything that looks for a product's published
                        # photos sees this row, source_id 'manual' to mark who wrote
                        # it, and no batch_id, which is what keeps the photography
                        # app's (product_id, batch_id, 'upload') upsert from ever
                        # finding it. A washtag-only row stays 'manual': it publishes
                        # nothing an 'upload' consumer should be counting.
                        # product_resolutions cannot be left NULL on an upload row,
                        # PhotoManagementNew's /getProductImagesCount evaluates
                        # `"fullsize" in record["product_resolutions"]` over all of
                        # them. batch_id is nullable; source_id is NOT NULL without a
                        # default.
                        is_image = image_type == "image"
                        row = await raw_conn.fetchrow(
                            """
                            INSERT INTO productimages
                                (id, product_id, image_source, source_id, product_type,
                                 product_resolutions)
                            VALUES ($1, $2, $3, $4, $5, $6)
                            RETURNING id
                            """,
                            uuid.uuid4(),
                            product_id,
                            "upload" if is_image else "manual",
                            "manual" if is_image else "",
                            source_row["product_type"] if source_row else None,
                            [r["name"] for r in resolutions] if is_image else None,
                        )
                        logger.info(
                            f"Created gallery productimages row for {product_id} "
                            f"(was: {source_row['image_source'] if source_row else 'no row'})"
                        )

                    record_id = row["id"]

                    indices_to_delete = set(deleted_indices)

                    # Build reordered_data from unified slot list.
                    # Each slot is either a 1-based existing index ("3") or "new:<file_idx>".
                    reordered_data = []
                    new_file_placements: List[tuple] = []  # [(final_position_1based, file_info)]
                    for slot in new_order:
                        slot_str = str(slot)
                        if slot_str.startswith("new:"):
                            file_idx = int(slot_str[4:])
                            if not (0 <= file_idx < len(new_files)):
                                raise ValueError(f"Invalid new file reference: {slot_str}")
                            file_info = new_files[file_idx]
                            reordered_data.append({
                                "id": "manual",
                                "shot_type": None,
                                "md5_hash": file_info.get("md5_hash"),
                            })
                            new_file_placements.append((len(reordered_data), file_info))
                        else:
                            idx = int(slot_str)
                            if idx in indices_to_delete:
                                continue
                            if 1 <= idx <= len(current_data):
                                reordered_data.append(current_data[idx - 1])

                    # Apply shot_types by final position — aligned with UI's images array.
                    if shot_types:
                        for i, entry in enumerate(reordered_data):
                            idx_key = str(i + 1)
                            if idx_key in shot_types:
                                entry["shot_type"] = shot_types[idx_key]

                    # GCS operations — still holding the session advisory lock on raw_conn.
                    top_shot_resource = await self._sync_gcs(
                        product_id=product_id,
                        current_data=current_data,
                        new_data=reordered_data,
                        new_file_placements=new_file_placements,
                        deleted_indices=deleted_indices,
                        new_order=new_order,
                        image_type=image_type,
                        resolutions=resolutions,
                    )

                    # DB update — same connection, still holding the lock.
                    count_field = "product_images_count" if image_type == "image" else "washtag_count"
                    product_type_clause = ", product_type = $4" if product_type else ""
                    params = [json.dumps(reordered_data), len(reordered_data), record_id]
                    if product_type:
                        params.append(product_type)

                    await raw_conn.execute(
                        f"""
                        UPDATE productimages
                        SET {data_field} = $1::jsonb,
                            {count_field} = $2,
                            updated_at = NOW()
                            {product_type_clause}
                        WHERE id = $3
                        """,
                        *params,
                    )

                    if image_type == "image":
                        await self._queue_sellercloud_sync(
                            product_id=product_id,
                            record_id=record_id,
                            before=current_data[0] if current_data else None,
                            after=reordered_data[0] if reordered_data else None,
                            top_shot_resource=top_shot_resource,
                        )

                    return {"success": True, "image_count": len(reordered_data)}
                finally:
                    # Always release the session advisory lock before returning the
                    # connection to the pool, otherwise the lock would leak onto the
                    # next caller that happens to reuse this connection.
                    try:
                        await raw_conn.fetchval("SELECT pg_advisory_unlock($1)", lock_key)
                    except Exception as unlock_err:
                        logger.warning(f"Failed to release advisory lock for {product_id}: {unlock_err}")

        except Exception as e:
            error_msg = f"Error saving product images: {str(e)}\n{traceback.format_exc()}"
            logger.error(error_msg)
            return {"success": False, "error": str(e), "status_code": 500}

    # ── Washtag replacement (reassignment) ────────────────────────────

    async def replace_washtags(
        self,
        target_product_id: str,
        selections: List[Dict],
        source_parent: Optional[str] = None,
        expected_washtag_updated_at=_UNSET,
        allow_clear: bool = False,
    ) -> Dict[str, Any]:
        """Replace a parent's washtag set with an ordered pick from any parents.

        Washtags are keyed on the parent SKU, so a reassigned child silently inherits
        its new parent's set. This is what lets an operator carry the old parent's
        washtags across at reassign time.

        Sources are read-only; only the target is written, which is why the advisory
        lock is taken on the target alone. The target's list is REPLACED, so an empty
        `selections` clears it, which is what `allow_clear` guards.
        """
        # This is reached from the reassignment flow rather than from a route, and its
        # caller treats any raise as an unexpected failure worth a stack trace. A bad
        # SKU is a plain bad request, so it returns like every other one rather than
        # escaping as an exception.
        try:
            target_product_id = validate_product_id(target_product_id)
            source_ids = {validate_product_id(s["product_id"]) for s in selections}
        except ValueError as e:
            return {"success": False, "error": str(e), "status_code": 400}

        if not selections and not allow_clear:
            return {
                "success": False,
                "error": "Select at least one washtag",
                "status_code": 400,
            }

        seen = set()
        for s in selections:
            key = (s["product_id"], s["index"])
            if key in seen:
                return {
                    "success": False,
                    "error": "The same washtag was selected twice",
                    "status_code": 400,
                }
            seen.add(key)

        conn = self._get_conn()
        lock_key = _product_lock_key(target_product_id)

        try:
            await _ensure_pool(conn)
            async with conn._pool.acquire() as raw_conn:
                acquired = await raw_conn.fetchval(
                    "SELECT pg_try_advisory_lock($1)", lock_key
                )
                if not acquired:
                    return {
                        "success": False,
                        "error": "Washtags are being modified by another process",
                        "status_code": 409,
                    }
                try:
                    return await self._replace_washtags_locked(
                        raw_conn,
                        target_product_id,
                        sorted(source_ids),
                        selections,
                        source_parent,
                        expected_washtag_updated_at,
                    )
                finally:
                    try:
                        await raw_conn.fetchval(
                            "SELECT pg_advisory_unlock($1)", lock_key
                        )
                    except Exception as unlock_err:
                        logger.warning(
                            f"Failed to release advisory lock for {target_product_id}: {unlock_err}"
                        )
        except ValueError as e:
            return {"success": False, "error": str(e), "status_code": 400}
        except Exception as e:
            logger.error(
                f"Error replacing washtags for {target_product_id}: {e}\n{traceback.format_exc()}"
            )
            return {"success": False, "error": "Could not update washtags", "status_code": 500}

    async def _replace_washtags_locked(
        self,
        raw_conn,
        target_product_id: str,
        source_ids: List[str],
        selections: List[Dict],
        source_parent: Optional[str],
        expected_washtag_updated_at: Optional[str],
    ) -> Dict[str, Any]:
        # One query for the target and every source. asyncpg connections are not
        # concurrency-safe, so gathering a query per source on this locked connection
        # would raise "another operation is in progress". _resolve_rows reads
        # image_source, image_data and washtag_data, so a trimmed SELECT would KeyError.
        wanted = sorted({target_product_id, *source_ids})
        rows = await raw_conn.fetch(
            """
            SELECT id, product_id, image_source, image_data, washtag_data,
                   washtag_count, product_type, updated_at
            FROM productimages
            WHERE product_id = ANY($1::text[])
            ORDER BY updated_at DESC
            """,
            wanted,
        )

        by_product: Dict[str, List] = {pid: [] for pid in wanted}
        for r in rows:
            by_product[r["product_id"]].append(r)

        target_rows = by_product[target_product_id]
        _, washtag_row = _resolve_rows(target_rows)

        db_updated_at = (
            washtag_row["updated_at"].isoformat()
            if washtag_row and washtag_row["updated_at"]
            else None
        )
        # Only when the caller actually read the state and is asserting it. Plain !=
        # rather than save_product_images' `a and b and a != b`: that form skips the
        # check whenever either side is None, and "the destination has no washtag row
        # yet" is precisely the case this feature exists for, so a row appearing under
        # us has to be a conflict. The server-side apply has no such read to assert, so
        # it leaves this unset and relies on the advisory lock plus the guarded UPDATE.
        if expected_washtag_updated_at is not _UNSET:
            if (expected_washtag_updated_at or None) != db_updated_at:
                return {
                    "success": False,
                    "error": "Washtags changed elsewhere. Reload and try again.",
                    "status_code": 409,
                }

        washtags_by_product = {
            pid: _as_list(_resolve_rows(prows)[1]["washtag_data"]) if prows else []
            for pid, prows in by_product.items()
        }
        new_data, blob_paths = _plan_washtag_copy(selections, washtags_by_product)

        current = _as_list(washtag_row["washtag_data"]) if washtag_row else []
        old_count = len(current)

        def _identity(entries):
            return [
                (e.get("md5_hash"), (e.get("copied_from") or {}).get("product_id"),
                 (e.get("copied_from") or {}).get("index"))
                for e in entries
            ]

        unchanged = _identity(current) == _identity(new_data)

        if not unchanged:
            ok = await self._copy_washtag_blobs(target_product_id, blob_paths)
            if not ok:
                # GCS first, DB last, and abort here rather than committing a row that
                # claims washtags the bucket does not have. The row still describes the
                # old state, so a re-run recomputes and converges.
                return {
                    "success": False,
                    "error": "Could not copy washtag images, please retry",
                    "status_code": 502,
                }

            record_id = washtag_row["id"] if washtag_row else None
            if record_id is None:
                # Only when the product has no productimages row at all. _resolve_rows
                # falls back washtag_row -> image_row -> rows[0], so any existing row
                # means we write onto it; creating one here would shadow the row the
                # photography app owns.
                record_id = await raw_conn.fetchval(
                    """
                    INSERT INTO productimages
                        (id, product_id, image_source, source_id, product_type,
                         washtag_data, washtag_count, reassigned_from)
                    VALUES ($1, $2, 'manual', '', $3, $4::jsonb, $5, $6)
                    RETURNING id
                    """,
                    uuid.uuid4(),
                    target_product_id,
                    None,
                    json.dumps(new_data),
                    len(new_data),
                    source_parent,
                )
                logger.info(
                    f"replace_washtags {target_product_id} row={record_id} "
                    f"before=[] after={json.dumps(new_data)} (new row)"
                )
            else:
                # Pre-image at INFO: once this UPDATE lands the destination's previous
                # array exists nowhere else, and the log is what made the SPO mapping
                # wipe recoverable.
                logger.info(
                    f"replace_washtags {target_product_id} row={record_id} "
                    f"before={json.dumps(current)} after={json.dumps(new_data)}"
                )
                updated = await raw_conn.execute(
                    """
                    UPDATE productimages
                    SET washtag_data = $1::jsonb,
                        washtag_count = $2,
                        reassigned_from = COALESCE($3, reassigned_from),
                        updated_at = NOW()
                    WHERE id = $4 AND updated_at = $5
                    """,
                    json.dumps(new_data),
                    len(new_data),
                    source_parent,
                    record_id,
                    washtag_row["updated_at"],
                )
                # The photography app writes this row and takes no lock, so the
                # advisory lock only serialises SkuBase against SkuBase.
                if updated.endswith(" 0"):
                    return {
                        "success": False,
                        "error": "Washtags changed elsewhere. Reload and try again.",
                        "status_code": 409,
                    }

        # Always, including the unchanged path: the DB is not the authority on what the
        # bucket holds. sellercloud_service blind-probes washtag_1..3 at submission
        # time, so a ghost blob past the end reaches live listings, and short-circuiting
        # on a DB-only comparison would freeze that divergence forever.
        await self._cleanup_washtag_blobs(
            target_product_id, len(new_data), max(old_count, MAX_WASHTAG_IMAGES)
        )

        return {"success": True, "washtag_count": len(new_data), "unchanged": unchanged}

    async def _copy_washtag_blobs(self, target_product_id: str, blob_paths: List[str]) -> bool:
        """Copy sources into the target's washtag slots. True if every copy landed.

        Staging through temp names is only needed when a copy READS a target slot the
        same operation also WRITES, which can only happen when a source is the target
        itself and the slot moves. Cross-parent copies cannot collide, and a slot
        copying to itself is a no-op worth skipping outright. The picker cannot produce
        the colliding shape, so the staged path is a guard for direct callers.
        """
        needs_staging = any(
            src.startswith(f"{target_product_id}/") and src != f"{target_product_id}/washtag_{slot}.jpg"
            for slot, src in enumerate(blob_paths, start=1)
        )

        direct = [
            (src, f"{target_product_id}/washtag_{slot}.jpg")
            for slot, src in enumerate(blob_paths, start=1)
            if src != f"{target_product_id}/washtag_{slot}.jpg"
        ]
        if not direct:
            return True

        if not needs_staging:
            results = await asyncio.gather(*(self._copy_blob(s, d) for s, d in direct))
            return all(r is not None for r in results)

        temp_prefix = f"_tmp_{uuid.uuid4().hex[:8]}"
        staged = [
            (src, f"{target_product_id}/{temp_prefix}_washtag_{i}.jpg", dest)
            for i, (src, dest) in enumerate(direct, start=1)
        ]
        try:
            first = await asyncio.gather(*(self._copy_blob(s, t) for s, t, _ in staged))
            if any(r is None for r in first):
                # Nothing live has been touched yet, so aborting here is free.
                return False
            second = await asyncio.gather(*(self._copy_blob(t, d) for _, t, d in staged))
            return all(r is not None for r in second)
        finally:
            # In a finally, not on the success path: an abort above would otherwise
            # leave _tmp_ blobs under the product's own prefix, invisible to the
            # gallery because it reads the DB, and reaped by nothing.
            await asyncio.gather(
                *(self._delete_blob(t) for _, t, _ in staged), return_exceptions=True
            )

    async def _cleanup_washtag_blobs(self, product_id: str, keep: int, upper: int) -> None:
        """Delete washtag slots above `keep`, up to `upper` inclusive."""
        if upper <= keep:
            return
        await asyncio.gather(
            *(
                self._delete_blob(f"{product_id}/washtag_{i}.jpg")
                for i in range(keep + 1, upper + 1)
            ),
            return_exceptions=True,
        )

    # ── SellerCloud hand-off ───────────────────────────────────────────

    async def _queue_sellercloud_sync(
        self,
        product_id: str,
        record_id,
        before: Optional[Dict],
        after: Optional[Dict],
        top_shot_resource: Optional[Dict],
    ) -> None:
        """Queue a SellerCloud push when this save changed the product's slot 1.

        SellerCloud does not read our GCS URL, it copies the bytes once, so an edited
        top shot only reaches the listing if something pushes it. The push is an export
        job plus an import job, minutes of waiting, so all that happens here is one row;
        GalleryImageSyncPoller does the rest.

        Never raises. The images are already written and the operator's save has already
        succeeded, so a queue problem is a logged warning, not a failed save.
        """
        try:
            identity = lambda entry: (
                (entry.get("id"), entry.get("md5_hash")) if entry else None
            )
            if identity(before) == identity(after):
                return

            if after is None:
                action = "delete_all"
                top_shot_md5 = None
                generation = None
            else:
                action = "replace"
                top_shot_md5 = after.get("md5_hash")
                generation = (top_shot_resource or {}).get("generation")

            child_rows = await connections.get("product_db").execute_query_dict(
                "SELECT sku FROM child_products "
                "WHERE parent_sku = $1 AND is_active = TRUE ORDER BY sku",
                [product_id],
            )
            child_skus = [r["sku"] for r in child_rows]
            if not child_skus:
                logger.info(
                    f"Slot 1 changed for {product_id} but it has no active children, "
                    f"nothing to push to SellerCloud"
                )
                return

            job_id = await gallery_image_sync_queue.enqueue(
                product_id=product_id,
                child_skus=child_skus,
                action=action,
                productimages_id=str(record_id),
                top_shot_md5=top_shot_md5,
                gcs_generation=str(generation) if generation else None,
            )
            logger.info(
                f"Queued SellerCloud image sync {job_id} for {product_id} "
                f"({action}, {len(child_skus)} children, generation={generation})"
            )
        except Exception:
            logger.warning(
                f"Failed to queue SellerCloud image sync for {product_id}; "
                f"images were saved:\n{traceback.format_exc()}"
            )

    # ── Shot Type Queries ──────────────────────────────────────────────

    async def get_product_types(self) -> list:
        conn = self._get_conn()
        rows = await conn.execute_query_dict(
            """SELECT name FROM product_type
            WHERE active = true ORDER BY sort_order, name"""
        )
        return [r["name"] for r in rows]

    async def get_shot_types(self, product_type: str) -> list:
        conn = self._get_conn()
        rows = await conn.execute_query_dict(
            """SELECT suffix FROM product_shot_type
            WHERE applies_to = $1 AND active = true AND disabled = 'FALSE'
            ORDER BY sort_order""",
            [product_type],
        )
        return [r["suffix"] for r in rows]

    async def get_required_shots(self, product_type: str) -> list:
        conn = self._get_conn()
        rows = await conn.execute_query_dict(
            """SELECT suffix FROM product_shot_type
            WHERE applies_to = $1 AND active = true AND disabled = 'FALSE' AND required = 'TRUE'
            ORDER BY sort_order""",
            [product_type],
        )
        return [r["suffix"] for r in rows]

    # ── GCS Operations ───────────────────────────────────────────────

    async def _sync_gcs(
        self,
        product_id: str,
        current_data: list,
        new_data: list,
        new_file_placements: List[tuple],
        deleted_indices: List[int],
        new_order: List,
        image_type: str,
        resolutions: List[Dict],
    ) -> Optional[Dict]:
        """Apply the save to GCS and return slot 1's object resource, if this save wrote it.

        The resource carries `generation`, which the SellerCloud push uses as its
        cache-buster: GCS serves these blobs immutable for a year, so an unversioned URL
        hands SellerCloud whatever a cache still holds. Reading it from the write that
        produced it, rather than a HEAD afterwards, means a later edit cannot slip in
        between and make us publish a version we never wrote. None when slot 1 was left
        untouched, deleted, or when the write failed.
        """
        if isinstance(current_data, str):
            current_data = json.loads(current_data)

        current_count = len(current_data)
        resolution_names = [r["name"] for r in resolutions]
        deleted_set = set(deleted_indices)
        top_shot_blob = f"{product_id}/1_1500.jpg" if image_type == "image" else None
        top_shot_resource: Optional[Dict] = None

        # 1. Delete removed images from GCS
        if deleted_indices:
            delete_tasks = []
            for idx in deleted_indices:
                if image_type == "image":
                    for res_name in resolution_names:
                        blob_path = f"{product_id}/{idx}_{res_name}.jpg"
                        delete_tasks.append(self._delete_blob(blob_path))
                else:
                    blob_path = f"{product_id}/washtag_{idx}.jpg"
                    delete_tasks.append(self._delete_blob(blob_path))
            await asyncio.gather(*delete_tasks, return_exceptions=True)

        # 2. Build old→new map for surviving existing images, honoring new-file slots.
        # Each slot in new_order consumes one position, whether it's an existing idx or a new file.
        old_to_new = {}
        new_idx = 1
        for slot in new_order:
            slot_str = str(slot)
            if slot_str.startswith("new:"):
                new_idx += 1
                continue
            idx = int(slot_str)
            if idx in deleted_set:
                continue
            if idx != new_idx:
                old_to_new[idx] = new_idx
            new_idx += 1

        if old_to_new:
            temp_prefix = f"_tmp_{uuid.uuid4().hex[:8]}"

            # Copy to temp paths
            copy_to_temp_tasks = []
            for old_idx in old_to_new:
                if image_type == "image":
                    for res_name in resolution_names:
                        src = f"{product_id}/{old_idx}_{res_name}.jpg"
                        tmp = f"{product_id}/{temp_prefix}_{old_idx}_{res_name}.jpg"
                        copy_to_temp_tasks.append(self._copy_blob(src, tmp))
                else:
                    src = f"{product_id}/washtag_{old_idx}.jpg"
                    tmp = f"{product_id}/{temp_prefix}_washtag_{old_idx}.jpg"
                    copy_to_temp_tasks.append(self._copy_blob(src, tmp))
            await asyncio.gather(*copy_to_temp_tasks, return_exceptions=True)

            # Copy from temp to final positions
            copy_to_final_tasks = []
            copy_to_final_targets = []
            for old_idx, new_idx_val in old_to_new.items():
                if image_type == "image":
                    for res_name in resolution_names:
                        tmp = f"{product_id}/{temp_prefix}_{old_idx}_{res_name}.jpg"
                        final = f"{product_id}/{new_idx_val}_{res_name}.jpg"
                        copy_to_final_tasks.append(self._copy_blob(tmp, final))
                        copy_to_final_targets.append(final)
                else:
                    tmp = f"{product_id}/{temp_prefix}_washtag_{old_idx}.jpg"
                    final = f"{product_id}/washtag_{new_idx_val}.jpg"
                    copy_to_final_tasks.append(self._copy_blob(tmp, final))
                    copy_to_final_targets.append(final)
            copy_results = await asyncio.gather(
                *copy_to_final_tasks, return_exceptions=True
            )
            # A reorder that promotes an existing photo into slot 1 writes the blob by
            # copy, so the new generation comes from here rather than from an upload.
            for target, result in zip(copy_to_final_targets, copy_results):
                if target == top_shot_blob and isinstance(result, dict):
                    top_shot_resource = result

            # Delete temp files
            delete_temp_tasks = []
            for old_idx in old_to_new:
                if image_type == "image":
                    for res_name in resolution_names:
                        tmp = f"{product_id}/{temp_prefix}_{old_idx}_{res_name}.jpg"
                        delete_temp_tasks.append(self._delete_blob(tmp))
                else:
                    tmp = f"{product_id}/{temp_prefix}_washtag_{old_idx}.jpg"
                    delete_temp_tasks.append(self._delete_blob(tmp))
            await asyncio.gather(*delete_temp_tasks, return_exceptions=True)

        # 3. Upload new files at their final positions (interleaved among existing).
        for file_index, file_info in new_file_placements:
            file_bytes = file_info["bytes"]

            async with _resize_semaphore:
                processed = await asyncio.to_thread(
                    process_image_resolutions, file_bytes, resolutions
                )

            upload_tasks = []
            upload_targets = []
            for res_name, img_data, extension, storage_class in processed:
                if image_type == "image":
                    blob_path = f"{product_id}/{file_index}_{res_name}.{extension}"
                else:
                    blob_path = f"{product_id}/washtag_{file_index}.{extension}"

                content_type_map = {
                    "jpg": "image/jpeg",
                    "png": "image/png",
                    "webp": "image/webp",
                }
                content_type = content_type_map.get(extension, "image/jpeg")

                upload_tasks.append(
                    self._upload_blob(blob_path, img_data, content_type, storage_class)
                )
                upload_targets.append(blob_path)
            upload_results = await asyncio.gather(*upload_tasks)
            for target, result in zip(upload_targets, upload_results):
                if target == top_shot_blob and isinstance(result, dict):
                    top_shot_resource = result

        # 4. Clean up: delete GCS files beyond the new count
        new_count = len(new_data)
        if current_count > new_count:
            cleanup_tasks = []
            for idx in range(new_count + 1, current_count + 1):
                if image_type == "image":
                    for res_name in resolution_names:
                        blob_path = f"{product_id}/{idx}_{res_name}.jpg"
                        cleanup_tasks.append(self._delete_blob(blob_path))
                else:
                    blob_path = f"{product_id}/washtag_{idx}.jpg"
                    cleanup_tasks.append(self._delete_blob(blob_path))
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)

        return top_shot_resource

    async def _copy_blob(self, src_path: str, dest_path: str) -> Optional[Dict]:
        """Returns the destination's object resource, or None if the copy failed.

        gcloud-aio copies through rewriteTo, whose response wraps the new object under
        "resource" rather than being the object itself.
        """
        try:
            result = await self._storage.copy(
                GCS_BUCKET, src_path,
                GCS_BUCKET, new_name=dest_path,
                # rewriteTo copies the SOURCE object's metadata when none is given, and
                # blobs written before the no-cache fix still carry
                # "max-age=31536000, immutable". Copying one forward would stamp that
                # onto the destination path, which is the bug GCS_CACHE_CONTROL exists
                # to prevent, on a URL SellerCloud and every platform also fetch.
                metadata={
                    "cache-control": GCS_CACHE_CONTROL,
                    "content-disposition": "inline",
                },
                # A fresh dict per call: gcloud-aio assigns params["rewriteToken"] in
                # place while draining a multi-part rewrite, so a shared dict leaks a
                # stale token into the next copy.
                params={},
                timeout=GCS_TIMEOUT_SECONDS,
            )
        except Exception as e:
            logger.warning(f"Failed to copy {src_path} -> {dest_path}: {e}")
            return None
        if isinstance(result, dict):
            return result.get("resource") or result
        return None

    async def _delete_blob(self, blob_path: str):
        try:
            await self._storage.delete(
                GCS_BUCKET, blob_path, timeout=GCS_TIMEOUT_SECONDS
            )
        except Exception as e:
            logger.warning(f"Failed to delete {blob_path}: {e}")

    async def _upload_blob(
        self, blob_path: str, img_data: io.BytesIO, content_type: str,
        storage_class: str = "STANDARD",
    ) -> Optional[Dict]:
        """Returns the uploaded object's resource, which carries generation and md5Hash."""
        try:
            image_bytes = img_data.getvalue()
            result = await self._storage.upload(
                GCS_BUCKET,
                blob_path,
                image_bytes,
                content_type=content_type,
                metadata={
                    "cache-control": GCS_CACHE_CONTROL,
                    "content-disposition": "inline",
                    "storage-class": storage_class,
                },
            )
            logger.info(f"Uploaded: {blob_path}")
            return result if isinstance(result, dict) else None
        finally:
            img_data.close()


# Module-level singleton
image_service = ImageService()
