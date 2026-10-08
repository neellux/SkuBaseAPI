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
        open_delist: bool = False,
    ) -> str | None:
        """None to submit, otherwise the reason this platform is skipped.

        Pure, so it can be table-tested. API/tests/ cannot import route modules
        (pyproject scopes pytest to tests/ because importing one used to fire a
        live authenticated PUT), so the decision lives here and the route calls
        it, once per platform, with inputs read under the per-product lock.

        Order matters. The in-flight reasons come FIRST and keep precedence over
        already_listed, so the pill tooltip names what is actually happening: a
        platform mid-submit is not "already listed", it is busy, and telling an
        operator otherwise sends them to the wrong screen.

        open_delist: an operator took the parent down on this platform by hand
        (platform_delists, open per open_platform_delists()). It overrides the
        two "it is on the platform" answers and nothing else (KTD4): an attempt
        in flight or awaiting action is a fact about now, not about the past.
        """
        if latest_status in _IN_FLIGHT_STATUSES:
            return SKIP_IN_FLIGHT

        # The platform ACCEPTED it and a person owes it a manual step. A new
        # attempt cannot advance that, and for eBay the item is already live.
        if latest_status == SKIP_AWAITING_ACTION:
            return SKIP_AWAITING_ACTION

        if allow_resubmit:
            return None

        # Taken down since, so posting it again is a relist, not a duplicate.
        if open_delist:
            return None

        # Before NEVER_GATED on purpose: the route has always skipped a successful
        # row on any platform whose allow_resubmit is false, sellercloud included,
        # and the pill locks it the same way. The exemption is from presence only.
        if latest_status == "success":
            return SKIP_NO_RESUBMIT

        if platform_id in NEVER_GATED:
            return None

        # No successful row on THIS listing, but the parent is demonstrably on the
        # platform. Blocks regardless of what the row says, including a failed
        # one, because posting again would duplicate. The escape hatches are
        # delisting it from the product page or flipping allow_resubmit.
        if has_external_id:
            return SKIP_ALREADY_LISTED

        return None

    @staticmethod
    def gated_platforms(
        already_listed: Iterable[str],
        platform_settings: dict[str, Any],
        open_delists: Iterable[str] = (),
    ) -> set[str]:
        """The platforms an external id actually blocks, for this submit.

        Split out from gate_decision because the mapping 422 chain needs it
        before any submission row is looked at: a platform we are not going to
        submit must not 422 the operator into fixing its brand mapping. A
        platform with an open delist is going to be submitted, so it is not
        gated, the same answer gate_decision gives.
        """
        delisted = set(open_delists)
        return {
            p
            for p in already_listed
            if p not in NEVER_GATED
            and p not in delisted
            and not platform_settings.get(p, {}).get("allow_resubmit", True)
        }

    @staticmethod
    async def _platform_ids(sql: str, parent_sku: str | None, conn: Any, what: str) -> set[str]:
        """DISTINCT platform_id rows for one parent, for the gate.

        Without `conn`, a read failure must not 500 a submit, so it falls back to
        "nothing": for presence that restores the gate as it was before
        external_listing_ids, and for delists it gates MORE, since nothing is
        lifted. Either way a duplicate post is recoverable and a blocked submit
        with no explanation is not. This is the early read that only narrows the
        mapping gates.

        With `conn` it is the submit's authoritative read, on its transaction
        under the per-product lock, and it raises: Postgres has already aborted
        the transaction on the failed statement, so there is nothing left to
        fall back to.
        """
        if not parent_sku:
            return set()
        if conn is not None:
            rows = await conn.execute_query_dict(sql, [parent_sku])
            return {r["platform_id"] for r in rows}
        try:
            rows = await connections.get("default").execute_query_dict(sql, [parent_sku])
        except Exception:
            logger.exception("%s lookup failed for %s; gate skipped", what, parent_sku)
            return set()
        return {r["platform_id"] for r in rows}

    @staticmethod
    async def platforms_for_parent(parent_sku: str | None, conn: Any = None) -> set[str]:
        """Which platforms claim this parent. One index hit on
        idx_external_listing_ids_gate. See _platform_ids for `conn`."""
        return await ExternalListingService._platform_ids(
            "SELECT DISTINCT platform_id FROM external_listing_ids WHERE parent_sku = $1",
            parent_sku,
            conn,
            "external_listing_ids",
        )

    @staticmethod
    async def open_delist_platforms(parent_sku: str | None, conn: Any = None) -> set[str]:
        """Platforms with an open delist for this parent: gate_decision's open_delist.
        open_platform_delists() is the only definition of open; never re-derive it.
        See _platform_ids for `conn`."""
        return await ExternalListingService._platform_ids(
            "SELECT DISTINCT platform_id FROM open_platform_delists() WHERE parent_sku = $1",
            parent_sku,
            conn,
            "platform_delists",
        )

    @staticmethod
    async def open_delists_for_parent(parent_sku: str | None) -> dict[str, list[dict[str, Any]]]:
        """{platform_id: [open entry, oldest first]} for the Listing view, where an
        open delist unlocks the platform pill. Several entries per platform are
        normal: delisting more sizes later adds an entry rather than widening one.

        child_skus None means the whole product. Best effort, like
        coverage_for_parent: the caller is polled, so a failure drops the entries,
        not the poll.
        """
        if not parent_sku:
            return {}
        try:
            rows = await connections.get("default").execute_query_dict(
                "SELECT id::text AS id, platform_id, child_skus, created_at "
                "FROM open_platform_delists() WHERE parent_sku = $1 "
                "ORDER BY platform_id, created_at, id",
                [parent_sku],
            )
        except Exception:
            logger.exception("open delists lookup failed for %s", parent_sku)
            return {}
        out: dict[str, list[dict[str, Any]]] = {}
        for r in rows:
            out.setdefault(r["platform_id"], []).append(
                {
                    "id": r["id"],
                    "platform_id": r["platform_id"],
                    "child_skus": list(r["child_skus"]) if r["child_skus"] else None,
                    "created_at": r["created_at"],
                }
            )
        return out

    # A latest attempt in one of these is still on its way to the platform.
    SIBLING_IN_FLIGHT_STATUSES = ("queued", "pending", "processing", "awaiting_action")

    @staticmethod
    async def in_flight_elsewhere(
        conn: Any, platform_id: str, product_id: str | None, listing_id: Any
    ) -> bool:
        """Whether ANOTHER listing for this parent has an attempt on this platform in flight.

        The latest attempt per listing, the same rule the submit route applies to a
        listing's own rows, so an old attempt superseded by a later one blocks nothing.
        Runs on the submit transaction's connection, under its per-product lock.

        In-flight only, never success. Where a platform keeps presence rows, the
        external-id gate already covers a success; blocking on a success without presence
        would leave the blocked listing with no row and no presence, unable to complete.
        """
        if not product_id:
            return False
        rows = await conn.execute_query_dict(
            """
            SELECT 1
            FROM listings l
            JOIN LATERAL (
                SELECT s.status
                FROM listing_submissions s
                WHERE s.listing_id = l.id AND s.platform_id = $3
                ORDER BY s.attempt_number DESC
                LIMIT 1
            ) latest ON true
            WHERE l.product_id = $1
              AND l.id <> $2::uuid
              AND latest.status = ANY($4::text[])
            LIMIT 1
            """,
            [
                product_id,
                str(listing_id),
                platform_id,
                list(ExternalListingService.SIBLING_IN_FLIGHT_STATUSES),
            ],
        )
        return bool(rows)

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
