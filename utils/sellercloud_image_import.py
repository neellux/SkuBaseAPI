"""The SellerCloud image-import file.

`/Catalog/Imports/Images` takes a tab-separated file whose column order is fixed by
SellerCloud's schema. Three callers build one now (the daily no-image backfill, the
gallery sync poller, and the one-off remediation script), so the shape lives here.

A row either ADDs an image by URL or DELETEs one by ImageID. To replace a product's
image, send both: the DELETE first, then the ADD.
"""
import io
from typing import Any, Dict, List, Optional

import pandas as pd

# Column order is SellerCloud's, not ours. See PhotoManagementNew's update_images_new,
# which writes the same file from the photography side.
IMAGE_IMPORT_COLUMNS = [
    "ProductID", "ImageID", "ImageURL", "IsDefault", "IsMainDescriptionImage",
    "IsSupplementImage", "SupplementImageOrder", "IsOtherImage", "IsSwatchImage",
    "Caption", "ImageSource", "IsWarehouseImage", "_ACTION_",
]


def add_default_image_row(product_id: str, image_url: str) -> Dict[str, Any]:
    """Add `image_url` as the product's default image."""
    return {
        "ProductID": product_id,
        "ImageID": None,
        "ImageURL": image_url,
        "IsDefault": True,
        "IsMainDescriptionImage": True,
        "IsSupplementImage": False,
        "_ACTION_": None,
    }


def delete_image_row(product_id: str, image_id: Any) -> Dict[str, Any]:
    """Remove one existing image, identified by the ImageID a kind-11 export reports."""
    return {
        "ProductID": product_id,
        "ImageID": image_id,
        "ImageURL": "",
        "IsDefault": True,
        "IsMainDescriptionImage": True,
        "IsSupplementImage": False,
        "_ACTION_": "DELETE",
    }


def build_image_import_tsv(rows: List[Dict[str, Any]]) -> bytes:
    """Rows in, import-file bytes out. Missing columns are filled, order is enforced."""
    df = pd.DataFrame(rows)
    for column in IMAGE_IMPORT_COLUMNS:
        if column not in df.columns:
            df[column] = None
    buf = io.StringIO()
    df[IMAGE_IMPORT_COLUMNS].to_csv(buf, index=False, sep="\t")
    return buf.getvalue().encode("utf-8")


def image_rows_from_export(raw: bytes) -> List[Dict[str, Optional[str]]]:
    """Parse a kind-11 export output file into [{product_id, image_id, image_url}].

    Products with no image simply have no row, so a caller must not assume every
    requested product appears.
    """
    df = pd.read_excel(io.BytesIO(raw))
    out = []
    for _, row in df.iterrows():
        product_id = row.get("ProductID")
        image_id = row.get("ImageID")
        if pd.isna(product_id) or pd.isna(image_id):
            continue
        image_url = row.get("ImageURL")
        out.append({
            "product_id": str(product_id),
            # Excel reads the id as a float, and "2776025.0" is not an ImageID.
            "image_id": str(int(image_id)) if isinstance(image_id, float) else str(image_id),
            "image_url": None if pd.isna(image_url) else str(image_url),
        })
    return out
