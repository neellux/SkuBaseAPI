"""GOAT batch poller. Three stages, mirroring how SPO carries a submission
through `products_uploading` -> `products_processing` -> `listed`.

    STAGE 1  claim PENDING, build rows, create the batch's TAB, append
             -> platform_status = awaiting_sku
    STAGE 2  read STV / Denied / SKU (GOAT) back out of the tab
             -> awaiting_1nventory (approved), or terminal (denied)
    STAGE 3  write goat.goat_sku onto the 1nventory Shopify product
             -> status = success, platform_status = listed

Every batch is a TAB in one spreadsheet, so the read-back reads every open tab in
a single values.batchGet. The tab id replaces the old per-batch Drive file id as
the key that scopes matching to one batch, which is what keeps a non-unique
Custom SKU (183 duplicates in prod) from attributing a GOAT SKU to the wrong
listing.

Stage 2 is a wait measured in days: the tab sits with the GOAT team until someone
fills a cell. That is why the stale sweep here covers ONLY the `sheet_writing`
window and never touches `awaiting_sku` - failing a row for taking a week would
be wrong, and is the eBay incident recorded at ebay_poller.py:71-78.
"""

import asyncio
import contextlib
import json
import logging
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any

from tortoise import connections
from tortoise.transactions import in_transaction

from config import config
from exceptions.goat_exceptions import GoatBuildError, GoatPermanentError, GoatTransientError
from models.db_models import AppSettings, ListingSubmission, SubmissionStatus
from services import goat_service
from services import email_service
from services.base_poller import BasePoller
from services.goat_service import GoatStage
from services.goat_sheets import goat_sheets, tab_title_for
from services.shopify_admin import ShopifyAdmin
from services.shopify_client import (
    ShopifyPermanentError,
    ShopifyTransientError,
    enable_writes,
    get_shopify_client,
    writes_enabled,
)
from utils.submission_steps import record_step

logger = logging.getLogger(__name__)

# The 1nventory source store. A module constant, not config, matching
# oneinventory_service.STORE - GOAT writes to the same store and a second
# spelling of it in config.toml would be one more thing to keep in agreement.
SYNC_STORE = "high-end-merchandise"

# Where a submission rests while the GOAT team fills its cell.
#
# PROCESSING keeps batch completion behaving exactly as it does today. The
# alternative, AWAITING_ACTION, is more honest - the live
# recompute_listing_submitted blocks completion on it unconditionally, so a batch
# would stay open until GOAT actually answers - but it means batches no longer
# close on our own schedule. Changing this is a one-line edit here.
PARKED_STATUS = SubmissionStatus.PROCESSING

# A weekend with the poller off, or the first cycle after enablement, must not
# produce one enormous all-or-nothing append.
MAX_CLAIM = 500


class GoatPoller(BasePoller):
    PLATFORM_ID = goat_service.PLATFORM_ID

    def __init__(self) -> None:
        super().__init__(config_section="goat_poller", name="GoatPoller")
        cfg = config.get("goat_poller", {})
        self.stale_minutes: int = int(cfg.get("stale_processing_timeout_minutes", 30))
        self.readback_interval = timedelta(
            minutes=int(cfg.get("readback_interval_minutes", 60))
        )
        # Do not read a tab while somebody may be mid-keystroke in it.
        self.readback_quiet = timedelta(
            minutes=int(cfg.get("readback_quiet_minutes", 15))
        )
        self._last_readback: datetime | None = None
        # The spreadsheet's modifiedTime immediately after OUR last write. Without
        # this the quiet gate never opens: our own dashboard write bumps
        # modifiedTime every cycle and would look like somebody typing.
        self._our_write_mtime: datetime | None = None
        # Signature of the dashboard as last written, so an unchanged dashboard
        # costs zero calls.
        self._master_signature: str | None = None

    # -- settings ----------------------------------------------------------

    async def _get_goat_settings(self) -> dict[str, Any]:
        settings = await AppSettings.first()
        if not settings:
            return {}
        return (settings.platform_settings or {}).get(self.PLATFORM_ID) or {}

    @staticmethod
    def _parse_min_batch_size(settings: dict[str, Any]) -> int:
        try:
            return max(1, int(settings.get("min_batch_size", 100)))
        except (TypeError, ValueError):
            return 100

    async def _poll_cycle(self) -> None:
        await self._recover_stale_processing()
        await self._batch_upload_pending(force=False)
        await self._check_readback()
        await self._sync_to_1nventory()
        await self._update_master()

    # -- stale recovery ----------------------------------------------------

    async def _recover_stale_processing(self) -> None:
        """Only the `sheet_writing` window, and only requeue when nothing was sent.

        Keys on the LAST step, never on "contains": every successful row passes
        THROUGH sheet_writing on its way to awaiting_sku, so a contains-check
        would fail every one of them on the first cycle.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=self.stale_minutes)
        stale = await ListingSubmission.filter(
            platform_id=self.PLATFORM_ID,
            status=SubmissionStatus.PROCESSING,
            platform_status=GoatStage.SHEET_WRITING,
            updated_at__lt=cutoff,
        )
        for sub in stale:
            steps = (sub.platform_meta or {}).get("steps") or []
            last = steps[-1].get("step") if steps else None
            if last == "tab_created":
                # The tab exists and the append may have landed. A blind requeue
                # would duplicate rows for the GOAT team.
                sub.status = SubmissionStatus.FAILED
                sub.error_display = (
                    "Interrupted after the tab was created; verify the sheet "
                    "before resubmitting"
                )
                await sub.save(update_fields=["status", "error_display", "updated_at"])
                await record_step(sub.id, "failed", stage="sheet_writing", reason="ambiguous")
            else:
                sub.status = SubmissionStatus.PENDING
                sub.platform_status = None
                await sub.save(update_fields=["status", "platform_status", "updated_at"])
                await record_step(sub.id, "requeued", stage="sheet_writing")
        if stale:
            logger.info("GOAT: recovered %d stale sheet_writing rows", len(stale))

    # -- stage 1 -----------------------------------------------------------

    async def _next_batch_number(self) -> int:
        conn = connections.get("default")
        rows = await conn.execute_query_dict("SELECT nextval('goat_batch_seq') AS n")
        return int(rows[0]["n"])

    async def begin_batch(self, force: bool = False) -> tuple[int | None, list[int]]:
        """Claim pending rows and mint the import id, synchronously.

        eBay's shape rather than Grailed's: the caller gets an accurate count
        because the rows are claimed under the lock here, not counted now and
        claimed later by a background task that races the scheduled cycle.

        The transaction ends at the status flip. No Google or Shopify call ever
        happens inside it.
        """
        settings = await self._get_goat_settings()
        manual_fallback = bool(settings.get("manual_fallback"))
        min_batch_size = self._parse_min_batch_size(settings)

        async with in_transaction("default") as conn:
            pending = await (
                ListingSubmission.filter(
                    platform_id=self.PLATFORM_ID, status=SubmissionStatus.PENDING
                )
                .order_by("id")
                .limit(MAX_CLAIM)
                .select_for_update(skip_locked=True)
                .using_db(conn)
            )
            if not pending:
                return None, []
            if not force and manual_fallback and len(pending) < min_batch_size:
                return None, []

            ids = [s.id for s in pending]
            import_id = await self._next_batch_number()
            await ListingSubmission.filter(id__in=ids).using_db(conn).update(
                status=SubmissionStatus.PROCESSING,
                platform_status=GoatStage.SHEET_WRITING,
            )

        # Outside the transaction: record_step uses its own connection and would
        # block on the rows the claim just locked.
        #
        # product_import_id goes in meta=, NOT as a **details kwarg. Only meta
        # merges into the TOP LEVEL of platform_meta, and the top level is where
        # the dashboard reads it (submissions_routes.py:141). Recorded at claim
        # time so a batch that fails during build is still a visible import.
        await record_step(
            ids, "sheet_writing", meta={"product_import_id": import_id}, batch_size=len(ids)
        )
        return import_id, ids

    async def run_batch(self, import_id: int, submission_ids: list[int]) -> dict[str, Any]:
        """Build the rows, create the batch's tab, append. Never raises."""
        live = set(submission_ids)
        subs = await ListingSubmission.filter(id__in=submission_ids).prefetch_related("listing")

        app_settings = await AppSettings.first()
        field_templates = (app_settings.field_templates or {}) if app_settings else {}
        platform_settings = (
            (app_settings.platform_settings or {}).get(self.PLATFORM_ID) or {}
        ) if app_settings else {}
        template = await self._load_template()
        field_definitions = template.field_definitions if template else []

        usable = [s for s in subs if s.listing is not None]
        for sub in subs:
            if sub.listing is None:
                await self._fail(sub, "Listing not found", stage="build_row")
                live.discard(sub.id)

        lookups = await goat_service.load_lookups(
            [s.listing.data or {} for s in usable],
            field_definitions, platform_settings, field_templates,
        )

        rows: list[list[Any]] = []
        row_ids: list[int] = []
        for sub in usable:
            try:
                row = goat_service.build_row(
                    sub.listing.product_id, sub.listing.data or {}, lookups
                )
            except GoatBuildError as exc:
                await self._fail(sub, str(exc), stage="build_row")
                live.discard(sub.id)
                continue
            except Exception:
                await self._fail(sub, "Failed to build the GOAT row", stage="build_row",
                                 error=traceback.format_exc())
                live.discard(sub.id)
                continue
            rows.append(row.as_cells())
            row_ids.append(sub.id)

        if not rows:
            # Nothing was sent, so the claim is undone rather than failed.
            await self._requeue(sorted(live), stage="sheet_writing")
            return {"submission_count": 0, "product_import_id": import_id}

        try:
            tab = await goat_sheets.create_tab(tab_title_for())
        except (GoatTransientError, GoatPermanentError) as exc:
            logger.warning("GOAT: tab creation failed: %s", exc.detail)
            await self._requeue(row_ids, stage="sheet_writing", reason=str(exc))
            return {"submission_count": 0, "product_import_id": import_id}

        # Persist the handle BEFORE the append. Without this, a crash between the
        # append and this step leaves rows in a tab with no record of WHICH tab,
        # and nothing can recover them.
        await record_step(
            row_ids, "tab_created",
            meta={"product_import_id": import_id, "tab_id": tab["tab_id"],
                  "tab_title": tab["tab_title"], "sheet_url": tab["sheet_url"]},
        )

        try:
            written = await goat_sheets.write_rows(tab["tab_title"], rows)
        except Exception as exc:
            # Ambiguous by construction: values.append has no idempotency key, so
            # a timeout may have committed. Never retried; a human reconciles
            # against the tab, whose id is already recorded above.
            for sub in usable:
                if sub.id in row_ids:
                    await self._fail(
                        sub, "Sheet write did not confirm; verify the sheet before resubmitting",
                        stage="write", error=str(exc),
                    )
            return {"submission_count": 0, "product_import_id": import_id}

        await self._note_our_write()
        # row_count in meta, not just as a step detail: the read-back reads it to
        # bound its range, and only meta reaches the top level of platform_meta.
        await record_step(row_ids, "sheet_written", meta={"row_count": written}, rows=written)
        await ListingSubmission.filter(id__in=row_ids).update(
            status=PARKED_STATUS, platform_status=GoatStage.AWAITING_SKU
        )
        logger.info("GOAT: batch %s wrote %d rows to tab %r",
                    import_id, written, tab["tab_title"])
        await self._notify_batch_ready(tab, written, import_id)
        return {"submission_count": written, "product_import_id": import_id,
                "sheet_url": tab["sheet_url"]}

    async def _batch_upload_pending(self, force: bool = False) -> dict[str, Any]:
        import_id, ids = await self.begin_batch(force=force)
        if not ids or import_id is None:
            return {"submission_count": 0, "product_import_id": None}
        return await self.run_batch(import_id, ids)

    async def manual_flush(self) -> dict[str, Any]:
        return await self._batch_upload_pending(force=True)

    async def _notify_batch_ready(
        self, tab: dict[str, Any], written: int, import_id: int
    ) -> None:
        """Tell the GOAT list a tab is ready. Never fails the batch.

        This replaces the per-batch Drive sharing email the file-per-batch model
        sent: with one shared spreadsheet there is nothing to grant, so the
        notification is the only thing that tells anyone a new tab exists.
        """
        list_name = config.get("goat", {}).get("email_list_name", "")
        if not list_name:
            return
        # M/D/YYYY with slashes in the subject, matching how the sheet is
        # referred to in conversation. The tab itself is named with dots.
        now = datetime.now(timezone.utc)
        subject = f"New PT Sheet: {now.month}/{now.day}/{now.year}"
        url = tab["sheet_url"]
        # Plain-text fallback spells the URL out, because the AppScript may render
        # only this one. The HTML version carries the "here" hyperlink.
        body = f"Please see the new PT sheet here: {url}"
        html_body = f'<p>Please see the new PT sheet <a href="{url}">here</a></p>'
        await email_service.send_to_list(list_name, subject, body, html_body)

    # -- stage 2 -----------------------------------------------------------

    async def _note_our_write(self) -> None:
        """Remember the file's modifiedTime after we write, so the quiet gate can
        tell our own edit apart from the GOAT team's."""
        with contextlib.suppress(Exception):
            self._our_write_mtime = await goat_sheets.file_modified_time()

    async def _check_readback(self) -> None:
        now = datetime.now(timezone.utc)
        if self._last_readback and now - self._last_readback < self.readback_interval:
            return

        open_rows = await ListingSubmission.filter(
            platform_id=self.PLATFORM_ID,
            status=PARKED_STATUS,
            platform_status=GoatStage.AWAITING_SKU,
        ).prefetch_related("listing")
        if not open_rows:
            self._last_readback = now
            return

        by_tab: dict[str, list[ListingSubmission]] = {}
        rows_per_tab: dict[str, int] = {}
        for sub in open_rows:
            meta = sub.platform_meta or {}
            title = meta.get("tab_title")
            if title:
                by_tab.setdefault(title, []).append(sub)
                rows_per_tab[title] = max(
                    rows_per_tab.get(title, 0), int(meta.get("row_count") or 0)
                )
        if not by_tab:
            self._last_readback = now
            return

        try:
            mtime = await goat_sheets.file_modified_time()
        except (GoatTransientError, GoatPermanentError) as exc:
            logger.warning("GOAT: modifiedTime check failed: %s", exc.detail)
            return

        # Nothing has happened since our own last write.
        if self._our_write_mtime is not None and mtime <= self._our_write_mtime:
            self._last_readback = now
            return
        # Somebody edited recently; they may still be typing.
        if now - mtime < self.readback_quiet:
            logger.debug("GOAT: sheet edited %s ago, waiting for quiet", now - mtime)
            return

        self._last_readback = now
        try:
            grids = await goat_sheets.batch_read_tabs(rows_per_tab)
        except (GoatTransientError, GoatPermanentError) as exc:
            logger.warning("GOAT: read-back failed: %s", exc.detail)
            return

        for title, grid in grids.items():
            try:
                await self._apply_readback(title, grid, by_tab[title])
            except (GoatBuildError, GoatTransientError, GoatPermanentError) as exc:
                logger.warning("GOAT: read-back of tab %r failed: %s", title, exc)

    async def _apply_readback(
        self, tab_title: str, grid: list[list[Any]], candidates: list[ListingSubmission]
    ) -> None:
        """Match by Custom SKU, but ONLY within this tab's own batch.

        `listings.product_id` is not unique (183 values appear on more than one
        listing in prod) and the sheet is writable by the GOAT team, so a global
        match could attribute a SKU to the wrong product. Scoping to the rows we
        wrote into THIS tab removes that entirely.
        """
        if not grid:
            return
        index = goat_service.header_index(grid[0])

        by_sku: dict[str, list[ListingSubmission]] = {}
        for sub in candidates:
            if sub.listing is not None:
                by_sku.setdefault(sub.listing.product_id, []).append(sub)

        approved: dict[int, dict[str, Any]] = {}
        denied: list[int] = []
        seen_skus: dict[str, int] = {}

        for raw in grid[1:]:
            parsed = goat_service.parse_readback_row(raw, index)
            if parsed is None or parsed.outcome == goat_service.OUTCOME_PENDING:
                continue
            matches = by_sku.get(parsed.custom_sku)
            if not matches:
                logger.info("GOAT: tab %r row for %r matches no open row", tab_title, parsed.custom_sku)
                continue
            if len(matches) > 1:
                logger.warning("GOAT: %r is ambiguous in tab %r, skipping", parsed.custom_sku, tab_title)
                continue
            sub = matches[0]
            expected = goat_service.clean_cell((sub.listing.data or {}).get("manufacturer_sku"))
            if expected and parsed.style_code and expected != parsed.style_code:
                logger.warning("GOAT: style code disagrees for %r in tab %r, skipping",
                               parsed.custom_sku, tab_title)
                continue

            if parsed.outcome == goat_service.OUTCOME_DENIED:
                denied.append(sub.id)
            else:
                seen_skus[parsed.goat_sku] = seen_skus.get(parsed.goat_sku, 0) + 1
                approved[sub.id] = {"goat_sku": parsed.goat_sku, "stv": parsed.stv}

        # A drag-fill down the SKU column would stamp one identifier onto many
        # products. Refuse them all rather than writing it to Shopify.
        duplicated = {sku for sku, n in seen_skus.items() if n > 1}
        if duplicated:
            logger.warning("GOAT: tab %r repeats %d GOAT SKU(s); those rows are skipped",
                           tab_title, len(duplicated))
            approved = {k: v for k, v in approved.items() if v["goat_sku"] not in duplicated}

        if denied:
            await ListingSubmission.filter(id__in=denied).update(
                status=SubmissionStatus.SUCCESS, platform_status=GoatStage.DENIED
            )
            await record_step(denied, "denied", tab=tab_title)
            logger.info("GOAT: %d row(s) denied in tab %r", len(denied), tab_title)

        if approved:
            await self._record_goat_skus(approved)
            await ListingSubmission.filter(id__in=list(approved)).update(
                platform_status=GoatStage.AWAITING_1NVENTORY
            )
            logger.info("GOAT: read back %d SKU(s) from tab %r", len(approved), tab_title)

    @staticmethod
    async def _record_goat_skus(found: dict[int, dict[str, Any]]) -> None:
        """One statement for the whole tab.

        record_step takes a single shared `meta` for all ids, but each row has a
        different goat_sku, so the shared form is unusable and a per-row loop
        would be one round trip each.
        """
        conn = connections.get("default")
        ids, metas, steps = [], [], []
        now = datetime.now(timezone.utc).isoformat()
        for sub_id, info in found.items():
            ids.append(sub_id)
            metas.append(json.dumps({"goat_sku": info["goat_sku"], "stv": info["stv"]}))
            steps.append(json.dumps([{
                "step": "goat_sku_received", "at": now,
                "goat_sku": info["goat_sku"], "stv": info["stv"],
            }]))
        await conn.execute_query(
            """
            UPDATE listing_submissions s
               SET platform_meta = jsonb_set(
                       COALESCE(s.platform_meta, '{}'::jsonb) || v.meta,
                       '{steps}',
                       COALESCE(CASE WHEN jsonb_typeof(s.platform_meta -> 'steps') = 'array'
                                     THEN s.platform_meta -> 'steps' END, '[]'::jsonb) || v.step,
                       true),
                   updated_at = CURRENT_TIMESTAMP
              FROM (SELECT * FROM unnest($1::bigint[], $2::jsonb[], $3::jsonb[])
                      AS t(id, meta, step)) v
             WHERE s.id = v.id
            """,
            [ids, metas, steps],
        )

    # -- stage 3 -----------------------------------------------------------

    async def _sync_to_1nventory(self) -> None:
        """Write goat.goat_sku onto the Shopify product, once it exists."""
        waiting = await ListingSubmission.filter(
            platform_id=self.PLATFORM_ID,
            status=PARKED_STATUS,
            platform_status=GoatStage.AWAITING_1NVENTORY,
        ).order_by("updated_at").limit(200)
        if not waiting:
            return
        if not goat_sheets.execute:
            logger.debug("GOAT: [goat] execute is false, skipping the 1nventory sync")
            return

        gids = await self._product_gids([s.listing_id for s in waiting])
        ready = [(s, gids[s.listing_id]) for s in waiting if gids.get(s.listing_id)]
        if not ready:
            return

        settings = config.get("goat", {})
        entries = [
            {"ownerId": gid,
             "namespace": settings.get("metafield_namespace", "goat"),
             "key": settings.get("metafield_key", "goat_sku"),
             "type": "single_line_text_field",
             "value": (sub.platform_meta or {}).get("goat_sku", "")}
            for sub, gid in ready
            if (sub.platform_meta or {}).get("goat_sku")
        ]
        if not entries:
            return

        if not writes_enabled():
            enable_writes("GoatPoller execute=true")
        client = await get_shopify_client(SYNC_STORE)
        admin = ShopifyAdmin(client)
        try:
            failures = await admin.set_metafields(entries)
        except ShopifyTransientError as exc:
            logger.warning("GOAT: metafield write deferred: %s", exc)
            return
        except ShopifyPermanentError as exc:
            logger.error("GOAT: metafield write rejected: %s", exc)
            return

        done_ids: list[int] = []
        for sub, gid in ready:
            if gid in failures:
                logger.warning("GOAT: %s metafield failed: %s", gid, failures[gid])
                continue
            # STV is GOAT's checkbox, read back in stage 2. It is the whole
            # derivation of the gsync tag - there is no local predicate.
            tagged = False
            if (sub.platform_meta or {}).get("stv"):
                await admin.add_tags(gid, [settings.get("gsync_tag", "gsync")])
                tagged = True
            await record_step(sub.id, "1nventory_synced", meta={"product_gid": gid}, tagged=tagged)
            done_ids.append(sub.id)

        if done_ids:
            await ListingSubmission.filter(id__in=done_ids).update(
                status=SubmissionStatus.SUCCESS, platform_status=GoatStage.LISTED
            )
            logger.info("GOAT: synced %d row(s) to 1nventory", len(done_ids))

    @staticmethod
    async def _product_gids(listing_ids: list[Any]) -> dict[Any, str]:
        """The 1nventory Shopify product id, from 1nventory's OWN submission row.

        NOT internal_platform_state.source_product_gid: that is written by the
        consignment source scan, never by the 1nventory submission path, is not
        written at all on TEST, and carries no internal_platform_id filter.
        """
        if not listing_ids:
            return {}
        conn = connections.get("default")
        rows = await conn.execute_query_dict(
            """
            SELECT DISTINCT ON (listing_id)
                   listing_id, external_id ->> 'product_gid' AS gid
              FROM listing_submissions
             WHERE platform_id = '1nventory' AND status = 'success'
               AND listing_id = ANY($1::uuid[])
               AND external_id ? 'product_gid'
             ORDER BY listing_id, attempt_number DESC
            """,
            [[str(i) for i in listing_ids]],
        )
        return {r["listing_id"]: r["gid"] for r in rows if r["gid"]}

    # -- the Master Sheet dashboard ----------------------------------------

    async def _update_master(self) -> None:
        """Rewrite the whole dashboard, but only when a count actually changed.

        One grouped query builds every row, so a tab that is Done costs nothing to
        keep rendering. The signature check is what stops us writing - and so
        bumping the file's modifiedTime, which the quiet gate reads - every cycle.
        """
        if not goat_sheets.execute or not goat_sheets.configured:
            return
        conn = connections.get("default")
        rows = await conn.execute_query_dict(
            """
            SELECT platform_meta ->> 'tab_title'            AS tab_title,
                   (platform_meta ->> 'tab_id')::bigint     AS tab_id,
                   count(*)                                 AS submitted,
                   count(*) FILTER (WHERE platform_meta ? 'goat_sku') AS approved,
                   count(*) FILTER (WHERE platform_status = $1)       AS denied
              FROM listing_submissions
             WHERE platform_id = $2 AND platform_meta ? 'tab_title'
             GROUP BY 1, 2
            """,
            [GoatStage.DENIED.value, self.PLATFORM_ID],
        )
        if not rows:
            return
        master_rows = [
            goat_service.MasterRow(
                tab_title=r["tab_title"], tab_id=int(r["tab_id"]),
                submitted=int(r["submitted"]), approved=int(r["approved"]),
                denied=int(r["denied"]),
            )
            for r in rows if r["tab_title"] and r["tab_id"] is not None
        ]
        grid = goat_service.master_grid(master_rows, goat_sheets.spreadsheet_id)
        signature = json.dumps(grid, sort_keys=True, default=str)
        if signature == self._master_signature:
            return
        try:
            await goat_sheets.write_master(grid)
        except (GoatTransientError, GoatPermanentError) as exc:
            logger.warning("GOAT: master sheet update failed: %s", exc.detail)
            return
        self._master_signature = signature
        await self._note_our_write()
        logger.info("GOAT: master sheet updated, %d batch row(s)", len(master_rows))

    # -- helpers -----------------------------------------------------------

    @staticmethod
    async def _load_template() -> Any:
        from models.db_models import Template

        return await Template.filter(name="default").first() or await Template.first()

    @staticmethod
    async def _fail(sub: ListingSubmission, display: str, *, stage: str,
                    error: str | None = None) -> None:
        sub.status = SubmissionStatus.FAILED
        sub.error_display = display[:500]
        if error:
            sub.error = error
        fields = ["status", "error_display", "updated_at"]
        if error:
            fields.append("error")
        await sub.save(update_fields=fields)
        await record_step(sub.id, "failed", stage=stage, reason=display[:300])

    @staticmethod
    async def _requeue(ids: list[int], *, stage: str, reason: str | None = None) -> None:
        if not ids:
            return
        await ListingSubmission.filter(id__in=ids).update(
            status=SubmissionStatus.PENDING, platform_status=None
        )
        await record_step(ids, "requeued", stage=stage, reason=reason)


goat_poller = GoatPoller()
