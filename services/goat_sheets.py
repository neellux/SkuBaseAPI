"""Google Sheets transport for the GOAT platform. No business logic.

EVERYTHING LIVES IN ONE SPREADSHEET. Each batch is a new TAB in the file named by
`[goat] spreadsheet_id`, and a `Master Sheet` tab in the same file carries a
dashboard of every batch. An earlier design created one Drive FILE per batch; the
tab model replaced it because:

  - polling collapses to a single call. values.batchGet takes one range per tab,
    so every open batch is read in one request no matter how many there are
  - the Drive storage-quota problem disappears. Adding a tab is a Sheets
    batchUpdate against an existing file, not a Drive file creation, so the
    service account never owns anything and needs no Shared Drive
  - sharing is a one-time act on one file rather than a call per batch

Only two Drive calls remain, both read-only metadata: the modifiedTime used by
the quiet-period gate. Everything else is the Sheets API.

CALL BUDGET, which is part of the design rather than an implementation detail:

    flush        4 calls, ANY batch size  (list tabs, copyTo, rename, append)
    readback     1 call when nothing changed (files.get modifiedTime)
                 2 calls when it did       (+ one batchGet for ALL open tabs)
    dashboard    1 call, and only when a count actually changed
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Final, Mapping, Sequence

import httpx

from config import config
from exceptions.goat_exceptions import GoatPermanentError, GoatTransientError, redact

logger = logging.getLogger(__name__)

DRIVE_FILES = "https://www.googleapis.com/drive/v3/files"
SHEETS = "https://sheets.googleapis.com/v4/spreadsheets"

# drive.metadata.readonly, not full drive: the only Drive call left is reading
# modifiedTime. Nothing here creates, shares or deletes a Drive file any more.
SCOPES: Final = [
    "https://www.googleapis.com/auth/drive.metadata.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]

# The read-back needs STV, Denied, SKU (GOAT), Style Code and Custom SKU, which
# are columns A..E. Row 1 is included so each tab's own header row resolves the
# column positions - never fixed offsets, since GOAT can insert columns.
#
# The row bound matters. The template carries checkbox validation to row 1000, and
# a checkbox cell holds a real FALSE even when nobody has touched it, so Sheets
# does NOT truncate the range: an unbounded A1:E returns ~1000 rows for a 2-row
# batch. Reading only as far as we actually wrote keeps the payload proportional.
READBACK_COLUMNS_A1: Final = "A1:E"

_TRANSIENT_REASONS = frozenset(
    {"rateLimitExceeded", "userRateLimitExceeded", "backendError",
     "internalError", "transientError"}
)


def tab_title_for(when: datetime | None = None) -> str:
    """`9.4.2026` - M.D.YYYY, unpadded, matching the Master Sheet dashboard."""
    d = when or datetime.now(timezone.utc)
    return f"{d.month}.{d.day}.{d.year}"


def a1(tab_title: str, cells: str) -> str:
    """Quote a tab title into an A1 range.

    Unqualified ranges silently resolve to the FIRST tab, which was harmless when
    each batch had its own file and is a data-corruption bug now.
    """
    return f"'{tab_title.replace(chr(39), chr(39) * 2)}'!{cells}"


class GoatSheets:
    """One spreadsheet. Owns the token and the HTTP client, nothing else."""

    def __init__(self) -> None:
        cfg = config.get("goat", {})
        self.service_account: str = cfg.get("service_account", "service-account-3.json")
        self.spreadsheet_id: str = cfg.get("spreadsheet_id", "")
        self.master_tab: str = cfg.get("master_tab", "Master Sheet")
        self.template_file_id: str = cfg.get("template_file_id", "")
        self.template_tab_id: int = int(cfg.get("template_tab_id", 0))
        # Affirmative opt-in. A missing key must mean "do not write". Covers the
        # Sheets writes here; the poller applies the same flag to Shopify.
        self.execute: bool = bool(cfg.get("execute", False))

        self._token: Any | None = None
        self._token_lock = asyncio.Lock()
        self._client: httpx.AsyncClient | None = None
        self._client_lock = asyncio.Lock()

    @property
    def configured(self) -> bool:
        return bool(self.spreadsheet_id and self.template_file_id)

    def tab_url(self, tab_id: int) -> str:
        return f"https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}/edit#gid={tab_id}"

    # -- plumbing ----------------------------------------------------------

    async def _get_token(self) -> str:
        """Lazily built, so a missing key file cannot break app import."""
        if self._token is None:
            async with self._token_lock:
                if self._token is None:
                    from gcloud.aio.auth import Token

                    self._token = Token(service_file=self.service_account, scopes=SCOPES)
        return await self._token.get()

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            async with self._client_lock:
                if self._client is None:
                    self._client = httpx.AsyncClient(timeout=httpx.Timeout(120.0))
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        if self._token is not None:
            await self._token.close()
            self._token = None

    def _require_execute(self, what: str) -> None:
        if not self.execute:
            raise GoatPermanentError(
                f"GOAT writes are disabled ([goat] execute = false), refusing {what}"
            )

    def _require_configured(self) -> None:
        if not self.configured:
            raise GoatPermanentError(
                "GOAT: [goat] spreadsheet_id or template_file_id is not set"
            )

    @staticmethod
    def _raise_for_status(response: httpx.Response, operation: str) -> None:
        """Split a Google error into requeue-vs-fail on `reason`, not on status.

        403 is genuinely both: a rate limit clears on its own, a permission
        problem does not.
        """
        if response.status_code < 400:
            return
        reason, message = "", response.text[:400]
        try:
            err = response.json().get("error", {}) or {}
            message = err.get("message") or message
            errors = err.get("errors") or []
            if errors:
                reason = errors[0].get("reason", "") or ""
        except ValueError:
            pass
        detail = f"{operation}: HTTP {response.status_code} {reason} {message}".strip()
        if response.status_code in (429, 500, 502, 503, 504) or reason in _TRANSIENT_REASONS:
            raise GoatTransientError(
                f"GOAT: Google API temporarily unavailable ({operation})", detail=detail
            )
        raise GoatPermanentError(
            f"GOAT: Google API rejected {operation} ({response.status_code})", detail=detail
        )

    async def _request(self, method: str, url: str, operation: str, **kwargs: Any) -> dict:
        client = await self._get_client()
        headers = {"Authorization": f"Bearer {await self._get_token()}"}
        try:
            response = await client.request(method, url, headers=headers, **kwargs)
        except httpx.HTTPError as exc:
            raise GoatTransientError(
                f"GOAT: {operation} did not complete", detail=redact(exc)
            ) from exc
        self._raise_for_status(response, operation)
        return response.json() if response.content else {}

    # -- reads -------------------------------------------------------------

    async def file_modified_time(self) -> datetime:
        """The spreadsheet's last modification, for the quiet-period gate.

        File level, not tab level: Drive exposes no per-tab modifiedTime, so any
        edit to any tab moves this. That is accepted - the gate exists to avoid
        reading while somebody types, and it is safe for it to be conservative.
        """
        data = await self._request(
            "GET",
            f"{DRIVE_FILES}/{self.spreadsheet_id}",
            "files.get",
            params={"fields": "modifiedTime", "supportsAllDrives": "true"},
        )
        return datetime.fromisoformat(data["modifiedTime"].replace("Z", "+00:00"))

    async def list_tabs(self) -> dict[str, int]:
        """{title: sheetId} for every tab, so titles stay unique."""
        data = await self._request(
            "GET",
            f"{SHEETS}/{self.spreadsheet_id}",
            "spreadsheets.get",
            params={"fields": "sheets(properties(sheetId,title))"},
        )
        return {
            s["properties"]["title"]: s["properties"]["sheetId"]
            for s in data.get("sheets", [])
        }

    async def batch_read_tabs(self, tabs: Mapping[str, int]) -> dict[str, list[list[Any]]]:
        """Every open tab in ONE call. This is the point of the one-file model.

        `tabs` is {tab_title: number of data rows we wrote}, which bounds each
        range - see READBACK_COLUMNS_A1 for why an unbounded range is expensive.
        A row count of 0 or less falls back to the template's full height.

        Rows still come back ragged where a column is genuinely empty, and the
        checkbox columns come back as real booleans under UNFORMATTED_VALUE, so
        callers must not blanket-stringify them.
        """
        if not tabs:
            return {}
        tab_titles = list(tabs)
        params: list[tuple[str, str]] = [
            ("ranges", a1(title, f"A1:E{tabs[title] + 1}" if tabs[title] > 0
                                 else READBACK_COLUMNS_A1))
            for title in tab_titles
        ]
        params += [
            ("valueRenderOption", "UNFORMATTED_VALUE"),
            ("majorDimension", "ROWS"),
            ("fields", "valueRanges(range,values)"),
        ]
        data = await self._request(
            "GET", f"{SHEETS}/{self.spreadsheet_id}/values:batchGet",
            "values.batchGet", params=params,
        )
        # batchGet preserves request order, which is the only reliable way back to
        # a title: the echoed `range` re-quotes and may not match byte for byte.
        out: dict[str, list[list[Any]]] = {}
        for title, value_range in zip(tab_titles, data.get("valueRanges", [])):
            out[title] = value_range.get("values") or []
        return out

    # -- writes ------------------------------------------------------------

    async def create_tab(self, desired_title: str) -> dict[str, Any]:
        """Copy the template tab into the spreadsheet and rename it.

        copyTo rather than duplicating a local template tab, so a column change on
        the template (STV and Denied were added this way) reaches the next batch
        with no deploy. The copy arrives as "Copy of <template>", hence the rename.

        Titles must be unique within a spreadsheet, so a second batch on the same
        day becomes `9.4.2026 (2)`.
        """
        self._require_execute("a tab creation")
        self._require_configured()

        existing = await self.list_tabs()
        title, n = desired_title, 1
        while title in existing:
            n += 1
            title = f"{desired_title} ({n})"

        props = await self._request(
            "POST",
            f"{SHEETS}/{self.template_file_id}/sheets/{self.template_tab_id}:copyTo",
            "sheets.copyTo",
            json={"destinationSpreadsheetId": self.spreadsheet_id},
        )
        tab_id = props["sheetId"]

        await self._request(
            "POST", f"{SHEETS}/{self.spreadsheet_id}:batchUpdate",
            "batchUpdate.updateSheetProperties",
            json={"requests": [{"updateSheetProperties": {
                "properties": {"sheetId": tab_id, "title": title, "index": 1},
                "fields": "title,index",
            }}]},
        )
        logger.info("GOAT: created tab %r (gid=%s)", title, tab_id)
        return {"tab_id": tab_id, "tab_title": title, "sheet_url": self.tab_url(tab_id)}

    async def write_rows(self, tab_title: str, rows: Sequence[Sequence[Any]]) -> int:
        """Write the WHOLE batch into a fresh tab in one call, starting at A2.

        values.update at an explicit range, NOT values.append. append locates the
        end of the "table" and writes after it - and because the template carries
        checkbox validation down to row 1000, every one of those rows holds a real
        FALSE, which append counts as data. It therefore appended at row 1001,
        leaving 999 empty rows above. Verified live before this was changed.

        Writing at A2 also keeps the template's own formatting and validation,
        since it writes INTO the prepared rows rather than inserting new ones.
        That is what stops the rows coming out bold: an inserted row inherits from
        the bold header above it.

        valueInputOption=RAW so a style code like FW22CS002_PRINCESS_BLUE is not
        coerced, and so a Python bool lands as a real boolean in the checkbox
        cells rather than the string "false".

        NEVER RETRIED, by this method or its caller: a retry after a
        timed-out-but-committed request would duplicate rows for the GOAT team.
        """
        self._require_execute("a sheet write")
        if not rows:
            return 0
        data = await self._request(
            "PUT",
            f"{SHEETS}/{self.spreadsheet_id}/values/{a1(tab_title, 'A2')}",
            "values.update",
            params={
                "valueInputOption": "RAW",
                "fields": "updatedRange,updatedRows",
            },
            json={"values": [list(r) for r in rows]},
        )
        written = int(data.get("updatedRows") or 0)
        if written != len(rows):
            raise GoatPermanentError(
                f"GOAT: sheet write covered {written} of {len(rows)} rows, "
                "verify the sheet before resubmitting",
                detail=f"updatedRange={data.get('updatedRange')}",
            )
        return written

    async def write_master(self, grid: Sequence[Sequence[Any]]) -> None:
        """Rewrite the whole dashboard in ONE call.

        USER_ENTERED, unlike every other write here, because the Sheet Name column
        is a live HYPERLINK formula. Clearing first stops a shrinking dashboard
        leaving orphaned rows behind.
        """
        self._require_execute("a master sheet write")
        self._require_configured()
        await self._request(
            "POST",
            f"{SHEETS}/{self.spreadsheet_id}/values/{a1(self.master_tab, 'A1:F1000')}:clear",
            "values.clear",
        )
        await self._request(
            "PUT",
            f"{SHEETS}/{self.spreadsheet_id}/values/{a1(self.master_tab, 'A1')}",
            "values.update",
            params={"valueInputOption": "USER_ENTERED",
                    "fields": "updatedRange,updatedRows"},
            json={"values": [list(r) for r in grid]},
        )


goat_sheets = GoatSheets()
