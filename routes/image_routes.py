import hashlib
import orjson
import logging
from typing import List

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile

from services.image_service import image_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/products/images", tags=["product-images"])

MAX_FILE_SIZE = 30 * 1024 * 1024


def _parse_json(data: str, field: str):
    try:
        return orjson.loads(data)
    except orjson.JSONDecodeError:
        raise HTTPException(status_code=400, detail=f"Invalid {field} format")
ALLOWED_CONTENT_TYPES = {"image/jpeg", "image/png", "image/webp"}


@router.get("")
async def get_product_images(product_id: str = Query(..., min_length=1)):
    try:
        result = await image_service.get_product_images(product_id)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting product images: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/product_types")
async def get_product_types():
    try:
        return await image_service.get_product_types()
    except Exception as e:
        logger.error(f"Error getting product types: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/shot_types")
async def get_shot_types(product_type: str = Query(..., min_length=1)):
    try:
        return await image_service.get_shot_types(product_type)
    except Exception as e:
        logger.error(f"Error getting shot types: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/required_shots")
async def get_required_shots(product_type: str = Query(..., min_length=1)):
    try:
        return await image_service.get_required_shots(product_type)
    except Exception as e:
        logger.error(f"Error getting required shots: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/save")
async def save_product_images(
    product_id: str = Form(...),
    updated_at: str = Form(""),
    new_order: str = Form("[]"),
    deleted_indices: str = Form("[]"),
    image_type: str = Form("image"),
    shot_types: str = Form("{}"),
    product_type: str = Form(""),
    files: List[UploadFile] = File(default=[]),
):
    try:
        order_list = _parse_json(new_order, "new_order")
        delete_list = _parse_json(deleted_indices, "deleted_indices")
        shot_types_map = _parse_json(shot_types, "shot_types")

        if image_type not in ("image", "washtag"):
            raise HTTPException(status_code=400, detail="image_type must be 'image' or 'washtag'")

        new_files = []
        for file in files:
            if file.content_type not in ALLOWED_CONTENT_TYPES:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid file type: {file.content_type}. Allowed: JPEG, PNG, WebP",
                )

            file_bytes = await file.read()

            if len(file_bytes) > MAX_FILE_SIZE:
                raise HTTPException(
                    status_code=413,
                    detail=f"File too large. Max size: {MAX_FILE_SIZE // (1024*1024)}MB",
                )

            md5_hash = hashlib.md5(file_bytes).hexdigest()

            new_files.append({
                "bytes": file_bytes,
                "md5_hash": md5_hash,
                "filename": file.filename,
            })

        result = await image_service.save_product_images(
            product_id=product_id,
            updated_at=updated_at,
            new_order=order_list,
            deleted_indices=delete_list,
            new_files=new_files,
            image_type=image_type,
            shot_types=shot_types_map if shot_types_map else None,
            product_type=product_type if product_type else None,
        )

        if not result.get("success"):
            status_code = result.get("status_code", 400)
            raise HTTPException(status_code=status_code, detail=result.get("error"))

        return await image_service.get_product_images(product_id)

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error saving product images: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")
