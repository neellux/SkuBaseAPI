"""Which step means "we handed it over" and which means "the platform put it live".

Two timestamps on listing_submissions, stamped by record_step as the steps arrive:

    submitted_at  we handed this submission to the platform
    listed_at     the platform confirmed it is live
    listed_at_source  which signal produced listed_at, so the value stays readable

Before these columns the listing date was guessed, preferring a step literally named
`listed` and falling back to completed_at and then updated_at. Measured on prod
2026-10-14 that guess was wrong for effectively every row:

  - `listed` is recorded by OUR code the instant the outbound call returns, so on
    sellercloud it sat a median 3s after the submission row was created and on
    1nventory 15s. 52% of all published dates were within a minute of the submit.
  - completed_at is the operator confirming a manual step (see its column comment),
    not a platform event. It was populated on 221 eBay rows and nowhere else, a
    median 25h from the publish.
  - updated_at is the last touch of the row and supplied 5,349 of 10,037 dates.
  - eBay and GOAT never write a `listed` step at all, so both fell through to
    updated_at, which on GOAT landed 14 days after the submission.

So the step that means "live" is per platform, and for two platforms it is not the
step called `listed`:

  sellercloud  the submit call is synchronous and SellerCloud accepts the product
               outright, so the handover IS the go-live.
  1nventory    same shape: the Shopify write returns the product_gid.
  grailed      async. The AppScript writes the row to the sheet (`sent`); Grailed's
               own processor then reads the sheet about every two hours and writes a
               result file to Drive, and `listed` is recorded, dated at that run, once
               a result shows the listing was created (services/grailed_poller.py,
               results check). With results tracking off, `listed` follows `sent` at
               accept, which is the old sheet-write proxy (operator decision,
               2026-10-14).
  spo          async. The import poller confirms the upload COMPLETE, and only
               then is the product live.
  ebay         async, and `published` only means SellerCloud QUEUED the publish
               job (see ebay_poller). The listing is live once its real eBay item
               ids are read back, which is `item_ids_read` carrying listed > 0.
  goat         async. The row goes on the sheet, then GOAT assigns it a SKU, which
               is what `goat_sku_received` records.

Both maps are keyed on every platform in app_settings.platforms; a new platform
without an entry stamps nothing, which test_listing_timestamps.py fails on.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

# The outbound call returned / the file was handed over. Two platforms submit
# synchronously, so their handover and their go-live are the same step.
HANDOFF_STEP: dict[str, str] = {
    "sellercloud": "listed",
    "1nventory": "listed",
    "grailed": "sent",
    "spo": "products_uploading",
    "ebay": "submitting",
    "goat": "sheet_written",
    "goat_sneakers": "sheet_written",
}

# The platform confirmed the listing is live. One step per platform: the signal a
# poller can actually observe, which is NOT always the one named `listed`.
GO_LIVE_STEP: dict[str, str] = {
    "sellercloud": "listed",
    "1nventory": "listed",
    "grailed": "listed",
    "spo": "listed",
    "ebay": "item_ids_read",
    "goat": "goat_sku_received",
    "goat_sneakers": "goat_sku_received",
}

# Tried only by the one-time backfill, after GO_LIVE_STEP, and never stamped live.
# 14 eBay rows on prod are accepted but end on `images_uploaded` with no item ids
# read back; `published` is the last thing known about them. Kept out of the live
# path on purpose: it is the weaker signal and would otherwise win on arrival
# order, since it is always recorded before `item_ids_read`.
GO_LIVE_BACKFILL_ONLY: dict[str, tuple[str, ...]] = {"ebay": ("published",)}


def _gate_allows(platform: str, step: str, details: Mapping[str, Any]) -> bool:
    """Whether this step entry really means "live" for this platform.

    Only eBay needs one. An `item_ids_read` step is recorded for every submission the
    readback touched, including ones where every launch was refused and only revised
    children remain; `listed` counts the children that actually got an eBay item id,
    so 0 means nothing new went live.
    """
    if platform == "ebay" and step == "item_ids_read":
        try:
            return int(details.get("listed") or 0) > 0
        except (TypeError, ValueError):
            return False
    return True


def handoff_platforms(step: str) -> list[str]:
    """Platforms for which `step` is the handover, so submitted_at is stamped."""
    return sorted(p for p, s in HANDOFF_STEP.items() if s == step)


def go_live_platforms(step: str, details: Mapping[str, Any] | None = None) -> list[str]:
    """Platforms for which `step` is the go-live, so listed_at is stamped.

    `details` are the keys recorded on the step entry itself, which is what the eBay
    gate reads. One record_step call carries one set of them, which is correct because
    every caller submits a single platform's rows.
    """
    details = details or {}
    return sorted(
        p
        for p, s in GO_LIVE_STEP.items()
        if s == step and _gate_allows(p, step, details)
    )
