"""HEAD probe for a product's primary photograph in the public lux_products bucket.

Used by background generation, right before a job builds its listing. The same probe as
daily_image_import_poller: `{parent}/1_1500.jpg`, the first image the batch confirm step
requires. The catalog decides whether a product has images from photography's
productimages instead (see catalog_sync_poller).

Three outcomes, never two. A 404 is the only proof a photograph is missing. Anything else
(a 5xx, a timeout, a network error) means the store could not answer, and callers must not
turn that into "no photos": sellercloud_service.get_product_images does exactly that, which
would permanently fail a generation job over a GCS blip.
"""

import enum
import logging
from typing import Optional
from urllib.parse import quote

import httpx

logger = logging.getLogger(__name__)

GCS_BASE = "https://storage.googleapis.com/lux_products"
PRIMARY_IMAGE = "1_1500.jpg"
PROBE_TIMEOUT_SECONDS = 10.0


class Probe(enum.Enum):
    PRESENT = "present"
    ABSENT = "absent"
    UNKNOWN = "unknown"


def primary_image_url(parent_sku: str) -> Optional[str]:
    """The probe URL, or None for a SKU that cannot safely become a path.

    Slashes stay literal (ESSX parents contain them, and their images live under the nested
    prefix), everything else is escaped. A segment that is empty, "." or ".." would let
    httpx normalise the path out of the product's own prefix, so those are refused.
    """
    if not parent_sku:
        return None
    segments = parent_sku.split("/")
    if any(segment in ("", ".", "..") for segment in segments):
        return None
    return f"{GCS_BASE}/{quote(parent_sku, safe='/')}/{PRIMARY_IMAGE}"


async def probe_primary_image(client: httpx.AsyncClient, parent_sku: str) -> Probe:
    url = primary_image_url(parent_sku)
    if url is None:
        logger.warning("Refusing to probe images for unsafe parent SKU %r", parent_sku)
        return Probe.UNKNOWN
    try:
        response = await client.head(url, follow_redirects=False)
    except Exception as exc:  # noqa: BLE001
        logger.debug("Image probe for %s failed: %s: %s", parent_sku, type(exc).__name__, exc)
        return Probe.UNKNOWN
    if response.status_code == 200:
        return Probe.PRESENT
    if response.status_code == 404:
        return Probe.ABSENT
    logger.debug("Image probe for %s returned %s", parent_sku, response.status_code)
    return Probe.UNKNOWN


async def probe_one(parent_sku: str) -> Probe:
    async with httpx.AsyncClient(timeout=PROBE_TIMEOUT_SECONDS) as client:
        return await probe_primary_image(client, parent_sku)
