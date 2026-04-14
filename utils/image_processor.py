import io
from typing import Dict, List, Tuple

from config import config
from PIL import Image as PILImage, ImageOps

# Guard against decompression bombs
PILImage.MAX_IMAGE_PIXELS = 25_000_000  # ~5000x5000 max


def load_resolutions_config() -> List[Dict]:
    return config.get("resolutions", [])


def load_washtag_resolutions_config() -> List[Dict]:
    return config.get("washtag_resolutions", [])


def resize_image(
    img: PILImage.Image,
    max_side: int = None,
    min_side: int = None,
) -> PILImage.Image:
    if not max_side and not min_side:
        return img

    width, height = img.size
    new_width, new_height = width, height

    if max_side:
        if width >= height:
            new_width = max_side
            new_height = int(max_side * height / width)
        else:
            new_height = max_side
            new_width = int(max_side * width / height)

    if min_side:
        if new_width <= new_height:
            scale_factor = min_side / new_width
            new_width = min_side
            new_height = int(new_height * scale_factor)
        else:
            scale_factor = min_side / new_height
            new_height = min_side
            new_width = int(new_width * scale_factor)

    if (new_width, new_height) != (width, height):
        img = img.resize((new_width, new_height), PILImage.Resampling.LANCZOS)

    return img


def process_image_format(
    img: PILImage.Image, format_config: Dict
) -> Tuple[io.BytesIO, str]:
    output = io.BytesIO()

    img_format = format_config.get("format", "jpeg").upper()
    quality = format_config.get("quality", 85)

    if img_format == "JPEG":
        img.save(output, format="JPEG", quality=quality, optimize=True)
        extension = "jpg"
    elif img_format == "PNG":
        compress_level = max(0, min(9, int((100 - quality) / 10)))
        img.save(output, format="PNG", compress_level=compress_level, optimize=True)
        extension = "png"
    elif img_format == "WEBP":
        img.save(output, format="WEBP", quality=quality, method=6)
        extension = "webp"
    else:
        img.save(output, format="JPEG", quality=quality, optimize=True)
        extension = "jpg"

    output.seek(0)
    return output, extension


def process_image_resolutions(
    image_bytes: bytes, resolutions_config: List[Dict]
) -> List[Tuple[str, io.BytesIO, str]]:
    results = []
    img_buffer = io.BytesIO(image_bytes)
    try:
        img = PILImage.open(img_buffer)

        # Fix EXIF orientation immediately
        img = ImageOps.exif_transpose(img)

        # Re-encode: convert to RGB (strips EXIF, alpha, embedded payloads)
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")

        for res_config in resolutions_config:
            no_compression = res_config.get("no_compression", False)

            if no_compression:
                # Store original bytes without re-processing (matches Photography API pattern)
                img_data = io.BytesIO(image_bytes)
                # Determine extension from content
                extension_map = {
                    "JPEG": "jpg",
                    "PNG": "png",
                    "WEBP": "webp",
                }
                extension = extension_map.get(img.format, "jpg")
                resolution_name = res_config.get("name", "default")
                storage_class = res_config.get("storage_class", "STANDARD")
                results.append((resolution_name, img_data, extension, storage_class))
            else:
                img_copy = img.copy()
                processed_img = resize_image(
                    img_copy,
                    max_side=res_config.get("max_side"),
                    min_side=res_config.get("min_side"),
                )

                img_data, extension = process_image_format(processed_img, res_config)

                resolution_name = res_config.get("name", "default")
                results.append((resolution_name, img_data, extension, "STANDARD"))

                processed_img.close()
                img_copy.close()
    finally:
        img_buffer.close()
        if "img" in locals():
            img.close()

    return results
