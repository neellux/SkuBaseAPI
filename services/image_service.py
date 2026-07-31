import asyncio
import hashlib
import io
import json
import logging
import re
import struct
import traceback
import uuid
from typing import Any, Dict, List, Optional

from gcloud.aio.storage import Storage
from tortoise import Tortoise

from config import config
from utils.image_processor import (
    load_resolutions_config,
    load_washtag_resolutions_config,
    process_image_resolutions,
)

logger = logging.getLogger(__name__)

GCS_BUCKET = config.get("gcs_bucket_products", "lux_products")
GCS_BASE_URL = f"https://storage.googleapis.com/{GCS_BUCKET}"
SERVICE_ACCOUNT_FILE = config.get("gcs_service_account", "service-account-2.json")
MAX_PRODUCT_IMAGES = 8
MAX_WASHTAG_IMAGES = 3
MAX_CONCURRENT_RESIZE = 3

# "/" is allowed because a parent SKU can legitimately contain one: ESSX parents are
# ESSX/BRAND/SEASON/STYLE/COLOUR, the photography app writes them into
# productimages.product_id as-is, and GCS already holds blobs under those prefixes.
# The traversal defence is not this whitelist, it is the explicit checks in
# validate_product_id below, which stay.
PRODUCT_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9\-_/]{0,199}$")

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
                    await self._sync_gcs(
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
    ):
        if isinstance(current_data, str):
            current_data = json.loads(current_data)

        current_count = len(current_data)
        resolution_names = [r["name"] for r in resolutions]
        deleted_set = set(deleted_indices)

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
            for old_idx, new_idx_val in old_to_new.items():
                if image_type == "image":
                    for res_name in resolution_names:
                        tmp = f"{product_id}/{temp_prefix}_{old_idx}_{res_name}.jpg"
                        final = f"{product_id}/{new_idx_val}_{res_name}.jpg"
                        copy_to_final_tasks.append(self._copy_blob(tmp, final))
                else:
                    tmp = f"{product_id}/{temp_prefix}_washtag_{old_idx}.jpg"
                    final = f"{product_id}/washtag_{new_idx_val}.jpg"
                    copy_to_final_tasks.append(self._copy_blob(tmp, final))
            await asyncio.gather(*copy_to_final_tasks, return_exceptions=True)

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
            await asyncio.gather(*upload_tasks)

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

    async def _copy_blob(self, src_path: str, dest_path: str):
        try:
            await self._storage.copy(
                GCS_BUCKET, src_path,
                GCS_BUCKET, new_name=dest_path,
            )
        except Exception as e:
            logger.warning(f"Failed to copy {src_path} -> {dest_path}: {e}")

    async def _delete_blob(self, blob_path: str):
        try:
            await self._storage.delete(GCS_BUCKET, blob_path)
        except Exception as e:
            logger.warning(f"Failed to delete {blob_path}: {e}")

    async def _upload_blob(
        self, blob_path: str, img_data: io.BytesIO, content_type: str,
        storage_class: str = "STANDARD",
    ):
        try:
            image_bytes = img_data.getvalue()
            await self._storage.upload(
                GCS_BUCKET,
                blob_path,
                image_bytes,
                content_type=content_type,
                metadata={
                    "cache-control": "public, max-age=31536000, immutable",
                    "content-disposition": "inline",
                    "storage-class": storage_class,
                },
            )
            logger.info(f"Uploaded: {blob_path}")
        finally:
            img_data.close()


# Module-level singleton
image_service = ImageService()
