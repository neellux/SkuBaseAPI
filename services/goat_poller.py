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

One class, two platforms. goat and goat_sneakers share this whole flow and differ
only in what a GoatVariant holds: the platform id, the config sections and the sheet
layout. Each gets its own poller instance (GOAT_POLLERS), so one platform's failed
cycle never stops the other's, and normally both write tabs into the same spreadsheet.

Stage 2 is a wait measured in days: the tab sits with the GOAT team until someone
fills a cell. That is why the stale sweep here covers ONLY the `sheet_writing`
window and never touches `awaiting_sku` - failing a row for taking a week would
be wrong, and is the eBay incident recorded at ebay_poller.py:71-78.
"""

import asyncio
import contextlib
import dataclasses
import json
import logging
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Mapping

from tortoise import connections
from tortoise.transactions import in_transaction

from config import config
from exceptions.goat_exceptions import GoatBuildError, GoatPermanentError, GoatTransientError
from models.db_models import AppSettings, ListingSubmission, SubmissionStatus
from services import goat_service
from services import goat_sneakers_service
from services import email_service
from services.base_poller import BasePoller
from services.goat_service import GoatStage
from services.external_listing_service import ExternalListingService
from services.goat_sheets import (
    EASTERN,
    GoatSheets,
    goat_sheets,
    goat_sneakers_sheets,
    tab_title_for,
)
from services.scheduled_flush import FlushStamp, run_scheduled_check, settle_stamp, stamp_flush
from services.shopify_admin import ShopifyAdmin
from services.shopify_client import (
    ShopifyPermanentError,
    ShopifyTransientError,
    enable_writes,
    get_shopify_client,
    writes_enabled,
)
from utils.listing_timestamps import GO_LIVE_STEP
from utils.submission_steps import record_step

logger = logging.getLogger(__name__)

# The 1nventory source store. A module constant, not config, matching
# oneinventory_service.STORE - GOAT writes to the same store and a second
# spelling of it in config.toml would be one more thing to keep in agreement.
SYNC_STORE = "high-end-merchandise"

# The name shown on the From line of the batch email. The mailbox is still the
# email AppScript's owner.
EMAIL_SENDER_NAME = "Luxury Emporium PT Submissions"
# Who the batch email tells its readers to contact. Hardcoded for now.
EMAIL_CONTACT = "yehuda@luxemporiumusa.com"

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


@dataclass(frozen=True, slots=True)
class GoatVariant:
    """One GOAT-family platform: everything that differs between goat and goat_sneakers.

    The flow is the same for both, so there is one poller class and two instances of
    it. What a variant owns is its identity, its config, and its sheet layout.
    """

    platform_id: str
    # Prefix of every log line and build error.
    label: str
    name: str
    # config.toml tables: the spreadsheet, template, email list and metafield keys,
    # and the poller's own cadence.
    config_section: str
    poller_section: str
    # Appended to the dated tab title, so two platforms can share one spreadsheet.
    tab_suffix: str
    default_sources: Mapping[str, str]
    build_row: Callable[[str, Mapping[str, Any], goat_service.GoatLookups], Any]
    # BasePoller turns a poller ON when its config section is missing. goat keeps that
    # (its config comment documents it). A variant added later must not inherit it: a
    # deploy that reaches a server before its config does would start a poller with no
    # spreadsheet, which claims and requeues its pending rows on every cycle.
    enabled_without_config: bool
    # A layout with a Qty column exports each child's available quantity from SellerCloud
    # while the batch is built: (listing data list, heartbeat=) -> (quantities, job ids).
    load_quantities: Callable[..., Awaitable[tuple[dict[str, int], list[str]]]] | None = None

    @property
    def batch_sequence(self) -> str:
        return f"{self.platform_id}_batch_seq"


GOAT = GoatVariant(
    platform_id=goat_service.PLATFORM_ID,
    label=goat_service.LABEL,
    name="GoatPoller",
    config_section="goat",
    poller_section="goat_poller",
    tab_suffix="",
    default_sources=goat_service.DEFAULT_SOURCES,
    build_row=goat_service.build_row,
    enabled_without_config=True,
)

GOAT_SNEAKERS = GoatVariant(
    platform_id=goat_sneakers_service.PLATFORM_ID,
    label=goat_sneakers_service.LABEL,
    name="GoatSneakersPoller",
    config_section="goat_sneakers",
    poller_section="goat_sneakers_poller",
    tab_suffix=" - Shoes",
    default_sources=goat_sneakers_service.DEFAULT_SOURCES,
    build_row=goat_sneakers_service.build_row,
    enabled_without_config=False,
    load_quantities=goat_sneakers_service.load_quantities,
)

# Signature of each dashboard as last written, so an unchanged dashboard costs zero
# calls. Keyed by (spreadsheet id, dashboard tab) and shared between the pollers that
# write into it: both compute the same combined grid, and whichever gets there first
# writes it. Read and written under GoatSheets.lock.
_master_signatures: dict[tuple[str, str], str] = {}


def build_batch_email(url: str) -> tuple[str, str]:
    """(plain text, HTML) bodies of the batch-ready email.

    The plain-text one spells the URL out, because the AppScript may render only
    that one. The HTML version carries the "here" hyperlink.
    """
    body = (
        f"Please see the new PT sheet here: {url}\n\n"
        "This is an automated email.\n"
        f"Please reach out to {EMAIL_CONTACT} for any questions."
    )
    html_body = (
        f'<p>Please see the new PT sheet <a href="{url}">here</a></p>'
        "<p>This is an automated email.<br>"
        f'Please reach out to <a href="mailto:{EMAIL_CONTACT}">{EMAIL_CONTACT}</a>'
        " for any questions.</p>"
    )
    return body, html_body


class GoatPoller(BasePoller):
    def __init__(self, variant: GoatVariant = GOAT, sheets: GoatSheets = goat_sheets) -> None:
        super().__init__(config_section=variant.poller_section, name=variant.name)
        self.variant = variant
        self.platform_id = variant.platform_id
        self.tag = variant.label
        self.sheets = sheets
        if variant.poller_section not in config and not variant.enabled_without_config:
            self.enabled = False
        cfg = config.get(variant.poller_section, {})
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
        #
        # Per poller even when two share a spreadsheet. Shared, the other poller's
        # write would move this past an edit the GOAT team made in between, and the
        # gate would then read that edit as "nothing happened since our write".
        self._our_write_mtime: datetime | None = None
        # Each batch's flush clock stamp, held between begin_batch and run_batch because the
        # submit route calls them separately (services/scheduled_flush.py).
        self._flush_stamps: dict[int, FlushStamp | None] = {}

    # -- settings ----------------------------------------------------------

    async def _get_goat_settings(self) -> dict[str, Any]:
        settings = await AppSettings.first()
        if not settings:
            return {}
        return (settings.platform_settings or {}).get(self.platform_id) or {}

    @staticmethod
    def _parse_min_batch_size(settings: dict[str, Any]) -> int:
        try:
            return max(1, int(settings.get("min_batch_size", 100)))
        except (TypeError, ValueError):
            return 100

    async def _poll_cycle(self) -> None:
        await self._recover_stale_processing()
        await run_scheduled_check(
            self.platform_id, count_ready=self._count_pending, flush=self._scheduled_flush
        )
        await self._batch_upload_pending(force=False)
        await self._check_readback()
        await self._sync_to_1nventory()
        await self._update_master()

    async def _count_pending(self) -> int:
        # The raw count, although one flush claims at most MAX_CLAIM: a minimum that high
        # would be unusual, and the size trigger drains what is left.
        return await ListingSubmission.filter(
            platform_id=self.platform_id, status=SubmissionStatus.PENDING
        ).count()

    async def _scheduled_flush(self) -> int:
        # force=True, exactly as Submit Pending.
        result = await self._batch_upload_pending(force=True)
        return int(result.get("submission_count") or 0)

    # -- stale recovery ----------------------------------------------------

    async def _recover_stale_processing(self) -> None:
        """Only the `sheet_writing` window, and only requeue when nothing was sent.

        Keys on the LAST step, never on "contains": every successful row passes
        THROUGH sheet_writing on its way to awaiting_sku, so a contains-check
        would fail every one of them on the first cycle.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=self.stale_minutes)
        stale = await ListingSubmission.filter(
            platform_id=self.platform_id,
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
            logger.info("%s: recovered %d stale sheet_writing rows", self.tag, len(stale))

    # -- stage 1 -----------------------------------------------------------

    async def _next_batch_number(self) -> int:
        conn = connections.get("default")
        # ::text::regclass, not ::regclass: asyncpg binds a regclass parameter as an oid.
        rows = await conn.execute_query_dict(
            "SELECT nextval($1::text::regclass) AS n", [self.variant.batch_sequence]
        )
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
                    platform_id=self.platform_id, status=SubmissionStatus.PENDING
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
        # The flush clock, settled when run_batch finishes (services/scheduled_flush.py).
        self._flush_stamps[import_id] = await stamp_flush(self.platform_id)
        return import_id, ids

    async def run_batch(self, import_id: int, submission_ids: list[int]) -> dict[str, Any]:
        """Run a claimed batch, then settle the flush clock.

        The clock is put back when every row ends in pending again, which is what happens
        when no row built or the tab could not be created: nothing reached the sheet.
        """
        stamp = self._flush_stamps.pop(import_id, None)
        try:
            return await self._build_and_write_batch(import_id, submission_ids)
        finally:
            await settle_stamp(stamp, submission_ids)

    async def _build_and_write_batch(
        self, import_id: int, submission_ids: list[int]
    ) -> dict[str, Any]:
        """Build the rows, create the batch's tab, append. Never raises."""
        live = set(submission_ids)
        subs = await ListingSubmission.filter(id__in=submission_ids).prefetch_related("listing")

        app_settings = await AppSettings.first()
        field_templates = (app_settings.field_templates or {}) if app_settings else {}
        platform_settings = (
            (app_settings.platform_settings or {}).get(self.platform_id) or {}
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
            platform_id=self.platform_id,
            default_sources=self.variant.default_sources,
            label=self.tag,
        )

        if self.variant.load_quantities is not None and usable:
            # One SellerCloud export for the whole batch. Before the tab exists, so a
            # failure here has sent nothing and the batch goes back to pending.
            try:
                quantities, job_ids = await self.variant.load_quantities(
                    [s.listing.data or {} for s in usable],
                    heartbeat=lambda: self._heartbeat(sorted(live)),
                )
            except Exception as exc:  # noqa: BLE001 - every failure puts the batch back
                logger.warning("%s: quantity export failed for batch %s: %s",
                               self.tag, import_id, exc)
                await self._requeue(sorted(live), stage="qty_export",
                                    reason=f"Quantity export failed: {exc}"[:300])
                return {"submission_count": 0, "product_import_id": import_id}
            await record_step(sorted(live), "qty_exported", job_ids=job_ids,
                              skus=len(quantities))
            lookups = dataclasses.replace(lookups, quantities=quantities)

        rows: list[list[Any]] = []
        row_ids: list[int] = []
        for sub in usable:
            try:
                row = self.variant.build_row(
                    sub.listing.product_id, sub.listing.data or {}, lookups
                )
            except GoatBuildError as exc:
                await self._fail(sub, str(exc), stage="build_row")
                live.discard(sub.id)
                continue
            except Exception:
                await self._fail(sub, f"Failed to build the {self.tag} row", stage="build_row",
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
            tab = await self.sheets.create_tab(tab_title_for(suffix=self.variant.tab_suffix))
        except (GoatTransientError, GoatPermanentError) as exc:
            logger.warning("%s: tab creation failed: %s", self.tag, exc.detail)
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
            written = await self.sheets.write_rows(tab["tab_title"], rows)
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
        logger.info("%s: batch %s wrote %d rows to tab %r",
                    self.tag, import_id, written, tab["tab_title"])
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
        list_name = config.get(self.variant.config_section, {}).get("email_list_name", "")
        if not list_name:
            return
        # M/D/YYYY with slashes in the subject, matching how the sheet is
        # referred to in conversation. The tab itself is named with dots. Eastern date,
        # the same day as the tab name (goat_sheets.tab_title_for).
        now = datetime.now(EASTERN)
        subject = f"New PT Sheet: {now.month}/{now.day}/{now.year}{self.variant.tab_suffix}"
        body, html_body = build_batch_email(tab["sheet_url"])
        await email_service.send_to_list(
            list_name, subject, body, html_body, sender_name=EMAIL_SENDER_NAME
        )

    # -- stage 2 -----------------------------------------------------------

    async def _note_our_write(self) -> None:
        """Remember the file's modifiedTime after we write, so the quiet gate can
        tell our own edit apart from the GOAT team's."""
        with contextlib.suppress(Exception):
            self._our_write_mtime = await self.sheets.file_modified_time()

    async def _check_readback(self) -> None:
        now = datetime.now(timezone.utc)
        if self._last_readback and now - self._last_readback < self.readback_interval:
            return

        open_rows = await ListingSubmission.filter(
            platform_id=self.platform_id,
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
            mtime = await self.sheets.file_modified_time()
        except (GoatTransientError, GoatPermanentError) as exc:
            logger.warning("%s: modifiedTime check failed: %s", self.tag, exc.detail)
            return

        # Nothing has happened since our own last write.
        if self._our_write_mtime is not None and mtime <= self._our_write_mtime:
            self._last_readback = now
            return
        # Somebody edited recently; they may still be typing.
        if now - mtime < self.readback_quiet:
            logger.debug("%s: sheet edited %s ago, waiting for quiet", self.tag, now - mtime)
            return

        self._last_readback = now
        try:
            grids = await self.sheets.batch_read_tabs(rows_per_tab)
        except (GoatTransientError, GoatPermanentError) as exc:
            logger.warning("%s: read-back failed: %s", self.tag, exc.detail)
            return

        for title, grid in grids.items():
            try:
                await self._apply_readback(title, grid, by_tab[title])
            except (GoatBuildError, GoatTransientError, GoatPermanentError) as exc:
                logger.warning("%s: read-back of tab %r failed: %s", self.tag, title, exc)

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
                logger.info("%s: tab %r row for %r matches no open row",
                            self.tag, tab_title, parsed.custom_sku)
                continue
            if len(matches) > 1:
                logger.warning("%s: %r is ambiguous in tab %r, skipping",
                               self.tag, parsed.custom_sku, tab_title)
                continue
            sub = matches[0]
            expected = goat_service.clean_cell((sub.listing.data or {}).get("manufacturer_sku"))
            if expected and parsed.style_code and expected != parsed.style_code:
                logger.warning("%s: style code disagrees for %r in tab %r, skipping",
                               self.tag, parsed.custom_sku, tab_title)
                continue

            if parsed.outcome == goat_service.OUTCOME_DENIED:
                denied.append(sub.id)
            else:
                seen_skus[parsed.goat_sku] = seen_skus.get(parsed.goat_sku, 0) + 1
                # parent_sku is carried from here because this is the only scope
                # that holds the prefetched listing; _record_goat_skus is keyed by
                # submission id alone.
                approved[sub.id] = {
                    "goat_sku": parsed.goat_sku,
                    "stv": parsed.stv,
                    "parent_sku": sub.listing.product_id if sub.listing else None,
                }

        # A drag-fill down the SKU column would stamp one identifier onto many
        # products. Refuse them all rather than writing it to Shopify.
        duplicated = {sku for sku, n in seen_skus.items() if n > 1}
        if duplicated:
            logger.warning("%s: tab %r repeats %d GOAT SKU(s); those rows are skipped",
                           self.tag, tab_title, len(duplicated))
            approved = {k: v for k, v in approved.items() if v["goat_sku"] not in duplicated}

        if denied:
            await ListingSubmission.filter(id__in=denied).update(
                status=SubmissionStatus.SUCCESS, platform_status=GoatStage.DENIED
            )
            await record_step(denied, "denied", tab=tab_title)
            logger.info("%s: %d row(s) denied in tab %r", self.tag, len(denied), tab_title)

        if approved:
            await self._record_goat_skus(approved)
            await ListingSubmission.filter(id__in=list(approved)).update(
                platform_status=GoatStage.AWAITING_1NVENTORY
            )
            # GOAT's identifier is the SKU (GOAT) the team writes back into the
            # sheet, and it lives in platform_meta, not external_id. Recorded here
            # rather than in _record_goat_skus so that stays one statement.
            #
            # Parent level: GOAT takes one row per parent listing, so there are no
            # per-child ids to have.
            #
            # This runs at the read-back, not at submit, which is the right moment:
            # a row parked in the sheet with no SKU (GOAT) yet is not on GOAT.
            await ExternalListingService.record(
                self.platform_id,
                [
                    {"level": "parent", "sku": info["parent_sku"],
                     "parent_sku": info["parent_sku"],
                     "external_id": info["goat_sku"],
                     "external_meta": {"stv": info["stv"]}}
                    for info in approved.values()
                    if info.get("parent_sku")
                ],
            )
            logger.info("%s: read back %d SKU(s) from tab %r", self.tag, len(approved), tab_title)

    @staticmethod
    async def _record_goat_skus(found: dict[int, dict[str, Any]]) -> None:
        """One statement for the whole tab.

        record_step takes a single shared `meta` for all ids, but each row has a
        different goat_sku, so the shared form is unusable and a per-row loop
        would be one round trip each.
        """
        conn = connections.get("default")
        ids, metas, steps = [], [], []
        # One reading, in both shapes: the text the step entries carry, and the datetime
        # the listed_at bind needs (asyncpg rejects a string for a timestamptz param).
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat()
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
                   updated_at = CURRENT_TIMESTAMP,
                   -- The go-live stamp record_step would have applied. GOAT is the one
                   -- platform whose go-live step cannot go through it (a different
                   -- goat_sku per row), so the mapping is honoured here instead; first
                   -- write wins, exactly as it does there.
                   listed_at = COALESCE(s.listed_at, $4::timestamptz),
                   listed_at_source = CASE
                       WHEN s.listed_at IS NULL THEN $5::text
                       ELSE s.listed_at_source END
              FROM (SELECT * FROM unnest($1::bigint[], $2::jsonb[], $3::jsonb[])
                      AS t(id, meta, step)) v
             WHERE s.id = v.id
            """,
            [ids, metas, steps, now_dt, GO_LIVE_STEP["goat"]],
        )

    # -- stage 3 -----------------------------------------------------------

    async def _sync_to_1nventory(self) -> None:
        """Write goat.goat_sku onto the Shopify product, once it exists."""
        waiting = await ListingSubmission.filter(
            platform_id=self.platform_id,
            status=PARKED_STATUS,
            platform_status=GoatStage.AWAITING_1NVENTORY,
        ).order_by("updated_at").limit(200)
        if not waiting:
            return
        if not self.sheets.execute:
            logger.debug("%s: [%s] execute is false, skipping the 1nventory sync",
                         self.tag, self.variant.config_section)
            return

        gids = await self._product_gids([s.listing_id for s in waiting])
        ready = [(s, gids[s.listing_id]) for s in waiting if gids.get(s.listing_id)]
        if not ready:
            return

        settings = config.get(self.variant.config_section, {})
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
            logger.warning("%s: metafield write deferred: %s", self.tag, exc)
            return
        except ShopifyPermanentError as exc:
            logger.error("%s: metafield write rejected: %s", self.tag, exc)
            return

        done_ids: list[int] = []
        for sub, gid in ready:
            if gid in failures:
                logger.warning("%s: %s metafield failed: %s", self.tag, gid, failures[gid])
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
            logger.info("%s: synced %d row(s) to 1nventory", self.tag, len(done_ids))

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

    def _dashboard_platforms(self) -> list[str]:
        """Every GOAT-family platform whose tabs belong on this poller's dashboard."""
        return [
            poller.platform_id
            for poller in GOAT_POLLERS.values()
            if poller.sheets.spreadsheet_id == self.sheets.spreadsheet_id
            and poller.sheets.master_tab == self.sheets.master_tab
        ]

    async def _update_master(self) -> None:
        """Rewrite the whole dashboard, but only when a count actually changed.

        One grouped query builds every row, so a tab that is Done costs nothing to
        keep rendering. The signature check is what stops us writing - and so
        bumping the file's modifiedTime, which the quiet gate reads - every cycle.

        The dashboard belongs to the spreadsheet, not to the platform: goat and
        goat_sneakers normally share one file, so each poller renders the tabs of
        both and the signature is shared (_master_signatures). Whichever poller
        notices a change writes it; the other then finds the signature current.
        """
        if not self.sheets.execute or not self.sheets.configured:
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
             WHERE platform_id = ANY($2::text[]) AND platform_meta ? 'tab_title'
             GROUP BY 1, 2
            """,
            [GoatStage.DENIED.value, self._dashboard_platforms()],
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
        grid = goat_service.master_grid(master_rows, self.sheets.spreadsheet_id)
        signature = json.dumps(grid, sort_keys=True, default=str)
        key = (self.sheets.spreadsheet_id, self.sheets.master_tab)
        # The lock covers the compare as well as the two-call write, so the other
        # poller cannot clear the dashboard between this one's clear and update.
        async with self.sheets.lock:
            if signature == _master_signatures.get(key):
                return
            try:
                await self.sheets.write_master(grid)
            except (GoatTransientError, GoatPermanentError) as exc:
                logger.warning("%s: master sheet update failed: %s", self.tag, exc.detail)
                return
            _master_signatures[key] = signature
        await self._note_our_write()
        logger.info("%s: master sheet updated, %d batch row(s)", self.tag, len(master_rows))

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
    async def _heartbeat(ids: list[int]) -> None:
        """Refresh updated_at on rows still claimed, so a long SellerCloud wait is not
        mistaken for a dead batch by _recover_stale_processing."""
        if ids:
            await ListingSubmission.filter(
                id__in=ids, status=SubmissionStatus.PROCESSING
            ).update(updated_at=datetime.now(timezone.utc))

    @staticmethod
    async def _requeue(ids: list[int], *, stage: str, reason: str | None = None) -> None:
        if not ids:
            return
        await ListingSubmission.filter(id__in=ids).update(
            status=SubmissionStatus.PENDING, platform_status=None
        )
        await record_step(ids, "requeued", stage=stage, reason=reason)


goat_poller = GoatPoller(GOAT, goat_sheets)
goat_sneakers_poller = GoatPoller(GOAT_SNEAKERS, goat_sneakers_sheets)

# By platform id, for the routes that act on whichever one the request names.
GOAT_POLLERS: dict[str, GoatPoller] = {
    poller.platform_id: poller for poller in (goat_poller, goat_sneakers_poller)
}
