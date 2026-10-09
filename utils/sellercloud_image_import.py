"""The SellerCloud image import files.

`/Catalog/Imports/Images` takes a tab-separated file whose column order is fixed by
SellerCloud's schema. Three callers build one now (the daily no-image backfill, the
gallery sync poller, and the one-off remediation script), so the shape lives here.

`/Catalog/Imports/Custom` takes a bulk-product-update file instead, whose columns are
whatever you name in the header. `DescriptionImageURL` is the one we use; see
build_description_image_tsv.

A row either ADDs an image by URL or DELETEs one by ImageID. To replace a product's
image, send both: the DELETE first, then the ADD.

IsDefault and IsMainDescriptionImage both mean "this is the product's slot-1 image".
SellerCloud holds exactly one of each per product and moves them together: importing a
new image with them set demotes the previous image to False/False. Measured across 193
rows (MSNK export 4202240 plus the July 2026 backup) they never diverge.

Both pushes here send exactly one row per child -- the parent's `1_1500.jpg`, which is
the priority-1 shot -- so both flags are always True on it. Do not try to express
"studio vs edited" through IsMainDescriptionImage: withholding it would also withhold
IsDefault, leaving the product with no visible image at all.

DescriptionImageURL is the read-side name for the IsMainDescriptionImage row: a custom
export reports it populated only while some image carries that flag. The flags travel
together only because every writer here sets them together, so a writer that sets
IsDefault alone leaves a product with a GalleryImageURL and an empty DescriptionImageURL.
72 active children were in that state on 2026-09-21, all of them sizes added to a parent
after its shoot, whose image is copied from a template sibling through POST /ProductImage
(sellercloud_service.upload_product_image) rather than through this file.
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
    """Add `image_url` as the product's slot-1 image: default AND main description image.

    Every caller sends the priority-1 shot, so both flags belong on it. See the module
    docstring for why they are not separable.
    """
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
    """Remove one existing image, identified by the ImageID a kind-11 export reports.

    The flags are left blank: a DELETE is matched on ProductID + ImageID, so sending
    IsMainDescriptionImage=True here only claimed something about a row on its way out.
    """
    return {
        "ProductID": product_id,
        "ImageID": image_id,
        "ImageURL": "",
        "IsDefault": None,
        "IsMainDescriptionImage": None,
        "IsSupplementImage": None,
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


def build_description_image_tsv(rows: List[tuple]) -> bytes:
    """Bulk-product-update file for `/Catalog/Imports/Custom`: (product_id, image_url) in.

    Measured against SellerCloud on 2026-09-21 (TEST-MSNK-0004, import job 4253201), and
    it does NOT behave like the images file above:

      * it ADDS a new image record and flags THAT one IsMainDescriptionImage, even when
        the URL given is one the product already holds. The old image keeps IsDefault and
        loses IsMainDescriptionImage, so the product ends up with a gallery image and a
        separate description image rather than one image carrying both flags.
      * it is therefore NOT idempotent. Sending the same row twice adds a second copy.
        Every caller must first establish that the product has no description image, via
        `/ProductImage/GetProductsImages` or a DescriptionImageURL export column.

    Which is why this is only ever used to repair a product that HAS an image and lacks
    the description flag. A product with no image at all gets add_default_image_row
    instead, one image carrying both flags.
    """
    header = "ProductID\tDescriptionImageURL\n"
    body = "".join(f"{product_id}\t{image_url}\n" for product_id, image_url in rows)
    return (header + body).encode("utf-8")


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
