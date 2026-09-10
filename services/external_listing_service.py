"""external_listing_ids: "this parent is already on that platform".

The second input to submit gating, beside platform_settings.<p>.allow_resubmit.
A row asserts PRESENCE and only optionally carries a platform id, because half
the platforms give us nothing usable:

    grailed      listing_submissions.external_id is ["<child sku>_<MMDDYYYY>"],
                 our own sku plus a batch date. A Google Sheet row key, not a
                 Grailed id. The AppScript's only action is addListings and it
                 reads nothing back, so no Grailed id exists anywhere.
    ebay         real item ids, child level only, external_id.item_ids{sku}
    1nventory    product_gid (parent) + variant_gids{sku} (child)
    goat         SKU (GOAT), but it lives in platform_meta.goat_sku
    spo          nothing, ever. 0 of 2,291 rows
    sellercloud  the "ProductID" is the sku we sent it; listings.info_product_id
                 already holds it

So external_id is nullable and every consumer keys on the row EXISTING.

THE PRECEDENCE RULE (plan section 2.2). This module owns the submit-gate half:

    submit gate   an external id blocks REGARDLESS of submission row state,
                  unless allow_resubmit. It asks "would this post a duplicate",
                  and presence alone answers that.
    completion    recompute_listing_submitted uses an external id ONLY when the
                  listing has no submission row for that platform at all. It
                  asks "is the work done", and a row that exists is always more
                  specific. Without that asymmetry, eBay's item ids (written in
                  the same save() that sets awaiting_action) would complete a
                  listing whose images are still owed.

Nothing here may raise into a caller's success path. A submission that reached
the platform and then failed to record a row must still be success; the row is
recoverable by backfill, the submission is not.
"""

import json
import logging
from typing import Any, Iterable

from tortoise import connections

logger = logging.getLogger(__name__)

PARENT = "parent"
CHILD = "child"

SOURCE_SUBMISSION = "submission"
SOURCE_BACKFILL = "backfill"
SOURCE_MANUAL = "manual"

# Skip reasons returned by gate_decision. The first three are today's behaviour,
# reproduced here so one function answers the whole question; the fourth is new.
# They double as UI lock reasons, so the strings are part of the contract with
# ListingView.resubmitLockedPlatforms.
SKIP_IN_FLIGHT = "in_flight"
SKIP_AWAITING_ACTION = "awaiting_action"
SKIP_NO_RESUBMIT = "no_resubmit"
SKIP_ALREADY_LISTED = "already_listed"

_IN_FLIGHT_STATUSES = ("queued", "pending", "processing")

# sellercloud is exempt by CODE, not by its allow_resubmit setting.
# listing_required_platforms never lets it be excluded, the pill never lets it be
# deselected, and capture writes a sellercloud row for every listing. Gating it
# would 409 every submit in the system the moment someone flipped that flag.
NEVER_GATED = frozenset({"sellercloud"})

_UPSERT_SQL = """
INSERT INTO external_listing_ids
       (platform_id, level, sku, parent_sku, external_id, external_meta, source)
VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
ON CONFLICT (platform_id, level, sku) DO UPDATE
   SET external_id   = COALESCE(EXCLUDED.external_id, external_listing_ids.external_id),
       external_meta = external_listing_ids.external_meta || EXCLUDED.external_meta,
       updated_at    = NOW()
"""


class ExternalListingService:
    """Reads and writes external_listing_ids. No route owns this; five pollers do."""

    # ------------------------------------------------------------------ gate

    @staticmethod
    def gate_decision(
        platform_id: str,
        latest_status: str | None,
        allow_resubmit: bool,
        has_external_id: bool,
        manual_fallback: bool = False,
    ) -> str | None:
        """None to submit, otherwise the reason this platform is skipped.

        Pure, so it can be table-tested. API/tests/ cannot import route modules
        (pyproject scopes pytest to tests/ because importing one used to fire a
        live authenticated PUT), so the decision lives here and the route calls
        it.

        Order matters. The in-flight reasons come FIRST and keep precedence over
        already_listed, so the pill tooltip names what is actually happening: a
        platform mid-submit is not "already listed", it is busy, and telling an
        operator otherwise sends them to the wrong screen.
        """
        if latest_status in _IN_FLIGHT_STATUSES:
            return SKIP_IN_FLIGHT

        # The platform ACCEPTED it and a person owes it a manual step. A new
        # attempt cannot advance that, and for eBay the item is already live.
        if latest_status == SKIP_AWAITING_ACTION:
            return SKIP_AWAITING_ACTION

        if platform_id in NEVER_GATED:
            return None

        if allow_resubmit:
            return None

        if latest_status == "success":
            return SKIP_NO_RESUBMIT

        # New: no successful row on THIS listing, but the parent is demonstrably
        # on the platform. Blocks regardless of what the row says, including a
        # failed one, because posting again would duplicate. The escape hatches
        # are deleting the row or flipping allow_resubmit.
        if has_external_id:
            return SKIP_ALREADY_LISTED

        return None

    @staticmethod
    def gated_platforms(
        already_listed: Iterable[str],
        platform_settings: dict[str, Any],
    ) -> set[str]:
        """The platforms an external id actually blocks, for this submit.

        Split out from gate_decision because the mapping 422 chain needs it
        before any submission row is looked at: a platform we are not going to
        submit must not 422 the operator into fixing its brand mapping.
        """
        return {
            p
            for p in already_listed
            if p not in NEVER_GATED
            and not platform_settings.get(p, {}).get("allow_resubmit", True)
        }

    @staticmethod
    async def platforms_for_parent(parent_sku: str | None) -> set[str]:
        """Which platforms claim this parent. One index hit on
        idx_external_listing_ids_gate."""
        if not parent_sku:
            return set()
        try:
            rows = await connections.get("default").execute_query_dict(
                "SELECT DISTINCT platform_id FROM external_listing_ids "
                "WHERE parent_sku = $1",
                [parent_sku],
            )
        except Exception:
            # Read failure must not 500 a submit. Falling back to "nothing is
            # claimed" restores exactly today's behaviour, which is the safe
            # direction: a duplicate post is recoverable, a blocked submit with
            # no explanation is not.
            logger.exception(
                "external_listing_ids lookup failed for %s; gate skipped", parent_sku
            )
            return set()
        return {r["platform_id"] for r in rows}

    @staticmethod
    async def coverage_for_parent(parent_sku: str | None) -> dict[str, dict[str, Any]]:
        """Per-platform coverage detail for the Listing view.

        Deliberately does NOT compute the gap against child_products: that table
        is in lux_products_2 and the UI already holds the children from
        get_product_children, so the subtraction is free in the browser and
        costs a cross-database read here.
        """
        if not parent_sku:
            return {}
        try:
            rows = await connections.get("default").execute_query_dict(
                "SELECT platform_id, level, sku, external_id "
                "FROM external_listing_ids WHERE parent_sku = $1 "
                "ORDER BY platform_id, level, sku",
                [parent_sku],
            )
        except Exception:
            logger.exception("external_listing_ids coverage failed for %s", parent_sku)
            return {}

        out: dict[str, dict[str, Any]] = {}
        for r in rows:
            entry = out.setdefault(
                r["platform_id"],
                {"parent_sku": parent_sku, "has_parent_row": False, "child_skus": []},
            )
            if r["level"] == PARENT:
                # A parent row means the WHOLE parent is covered. Partial exists
                # only when there are child rows and no parent row. Not a
                # convenience: a successful Grailed submission proves the parent
                # is on the sheet, while added_references lists only the children
                # that were NEWLY added, so counting child rows understates it by
                # 90 parents on today's data.
                entry["has_parent_row"] = True
            else:
                entry["child_skus"].append(r["sku"])
        return out

    # --------------------------------------------------------------- capture

    @staticmethod
    async def record(
        platform_id: str,
        rows: list[dict[str, Any]],
        source: str = SOURCE_SUBMISSION,
    ) -> int:
        """Upsert presence rows. Never raises.

        `rows` are dicts of level/sku/parent_sku and optionally external_id and
        external_meta. Written one statement per row rather than one batch, so a
        single malformed row cannot lose the others; the counts here are single
        digits per submission.

        On conflict, external_id is COALESCEd rather than overwritten: a resubmit
        that returns nothing (grailed) must not blank an id an earlier capture
        recorded, and first_seen_at is left alone so it keeps meaning "first
        seen".
        """
        if not rows:
            return 0
        conn = connections.get("default")
        written = 0
        for row in rows:
            sku = (row.get("sku") or "").strip()
            parent_sku = (row.get("parent_sku") or "").strip()
            level = row.get("level") or CHILD
            if not sku or not parent_sku:
                continue
            if level == PARENT and sku != parent_sku:
                # The CHECK would reject it anyway; catching it here keeps the
                # caller's log readable.
                logger.warning(
                    "external_listing_ids: parent row %s/%s disagrees with itself",
                    sku,
                    parent_sku,
                )
                continue
            try:
                await conn.execute_query(
                    _UPSERT_SQL,
                    [
                        platform_id,
                        level,
                        sku,
                        parent_sku,
                        row.get("external_id"),
                        json.dumps(row.get("external_meta") or {}),
                        source,
                    ],
                )
                written += 1
            except Exception:
                logger.exception(
                    "external_listing_ids upsert failed: %s %s %s",
                    platform_id,
                    level,
                    sku,
                )
        return written

    @staticmethod
    async def record_sellercloud(listing_id: Any, parent_sku: str | None) -> None:
        """The SellerCloud presence row, for both dispatch paths.

        A helper rather than two inline blocks because listing_routes and
        submission_poller keep their SellerCloud branches deliberately identical,
        so a row is indistinguishable whichever produced it. One call site each
        keeps it that way.

        SellerCloud issues no new identifier: submit_listing_to_sellercloud
        returns a bare bool, _update_single_product_with_retry discards the
        response body, and the "ProductID" we send it IS the sku. The id worth
        recording is listings.info_product_id, which is written at creation and
        is the full SellerCloud id including the variation suffix. So this is a
        copy, not an integration.

        These rows never gate: sellercloud is in NEVER_GATED. They exist so the
        table is a complete picture of where a parent lives.
        """
        if not parent_sku:
            return
        external_id = None
        try:
            rows = await connections.get("default").execute_query_dict(
                "SELECT info_product_id FROM listings WHERE id = $1::uuid",
                [str(listing_id)],
            )
            if rows:
                external_id = rows[0].get("info_product_id")
        except Exception:
            # The presence assertion is the point; the id is decoration. Record
            # the row without it rather than losing it.
            logger.exception("info_product_id lookup failed for listing %s", listing_id)
        await ExternalListingService.record(
            "sellercloud",
            [{"level": PARENT, "sku": parent_sku, "parent_sku": parent_sku,
              "external_id": external_id}],
        )
