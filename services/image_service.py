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

PRODUCT_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9\-_]{0,199}$")

_resize_semaphore = asyncio.Semaphore(MAX_CONCURRENT_RESIZE)


def validate_product_id(product_id: str) -> str:
    if not product_id or not PRODUCT_ID_PATTERN.match(product_id) or ".." in product_id:
        raise ValueError(f"Invalid product_id format: {product_id}")
    return product_id.strip("/")


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
        validate_product_id(product_id)
        conn = self._get_conn()

        record = await conn.execute_query_dict(
            """
            SELECT id, product_id, product_images_count, image_data,
                   washtag_count, washtag_data, product_type, updated_at
            FROM productimages
            WHERE product_id = $1
            ORDER BY created_at DESC
            LIMIT 1
            """,
            [product_id],
        )

        if not record:
            return {
                "product_id": product_id,
                "product_type": None,
                "updated_at": None,
                "images": [],
                "washtags": [],
                "image_count": 0,
                "washtag_count": 0,
            }

        row = record[0]
        image_data = row["image_data"] or []
        washtag_data = row["washtag_data"] or []

        if isinstance(image_data, str):
            image_data = json.loads(image_data)
        if isinstance(washtag_data, str):
            washtag_data = json.loads(washtag_data)

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
            "product_type": row.get("product_type"),
            "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
            "images": images,
            "washtags": washtags,
            "image_count": row["product_images_count"] or 0,
            "washtag_count": row["washtag_count"] or 0,
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
        validate_product_id(product_id)
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
                    row = await raw_conn.fetchrow(
                        """
                        SELECT id, image_data, washtag_data, product_images_count,
                               washtag_count, updated_at
                        FROM productimages
                        WHERE product_id = $1
                        ORDER BY created_at DESC
                        LIMIT 1
                        """,
                        product_id,
                    )

                    if not row:
                        # First save for this product — create a manual-source row.
                        # batch_id is nullable; source_id is NOT NULL without a default.
                        row = await raw_conn.fetchrow(
                            """
                            INSERT INTO productimages
                                (id, product_id, image_source, source_id)
                            VALUES ($1, $2, 'manual', '')
                            RETURNING id, image_data, washtag_data, product_images_count,
                                      washtag_count, updated_at
                            """,
                            uuid.uuid4(),
                            product_id,
                        )
                        logger.info(f"Created manual productimages row for {product_id}")

                    db_updated_at = row["updated_at"].isoformat() if row["updated_at"] else None
                    if updated_at and db_updated_at and updated_at != db_updated_at:
                        return {
                            "success": False,
                            "error": "Product images were updated by another user. Refreshing...",
                            "status_code": 409,
                        }

                    record_id = row["id"]
                    data_field = "image_data" if image_type == "image" else "washtag_data"
                    current_data = row[data_field] or []
                    if isinstance(current_data, str):
                        current_data = json.loads(current_data)

                    resolutions = load_resolutions_config() if image_type == "image" else load_washtag_resolutions_config()
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
