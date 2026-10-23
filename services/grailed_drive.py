"""Google Drive transport for Grailed processor result files. No business logic.

Grailed's processor drops one result file per run into a Drive folder tree (a folder
per date). This module lists folders and reads those files with service account 3;
parsing and deciding what a row means live elsewhere.

The real files are native Google Sheets (`375-successes-2026-10-21--08h31.csv` is a
Sheet despite the name), so they are EXPORTED as CSV. An uploaded .xlsx is downloaded
as-is with alt=media. Anything else is refused.

Mirrors services/goat_sheets.py: a lazy token under a lock, one shared httpx client,
and a transient/permanent split decided by Google's error `reason`, not the status.
"""

import asyncio
import logging
from typing import Any, Final
from urllib.parse import quote

import httpx

from config import config
from exceptions.grailed_exceptions import (
    GrailedDrivePermanentError,
    GrailedDriveTransientError,
)

logger = logging.getLogger(__name__)

DRIVE_FILES = "https://www.googleapis.com/drive/v3/files"

# drive.readonly, not drive.metadata.readonly: metadata scope can list the folder but
# cannot download or export a file.
SCOPES: Final = ["https://www.googleapis.com/auth/drive.readonly"]

FOLDER_MIME: Final = "application/vnd.google-apps.folder"
SHEET_MIME: Final = "application/vnd.google-apps.spreadsheet"
XLSX_MIME: Final = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

LIST_FIELDS: Final = "nextPageToken,files(id,name,mimeType,createdTime,modifiedTime)"

_TRANSIENT_REASONS = frozenset(
    {"rateLimitExceeded", "userRateLimitExceeded", "backendError",
     "internalError", "transientError"}
)


def _file_url(file_id: str) -> str:
    return f"{DRIVE_FILES}/{quote(file_id, safe='')}"


class GrailedDrive:
    """Read-only Drive access. Owns the token and the HTTP client, nothing else."""

    def __init__(self, service_account: str | None = None) -> None:
        self.service_account: str = service_account or config.get("grailed_poller", {}).get(
            "results_service_account", "service-account-3.json"
        )
        self._token: Any | None = None
        self._token_lock = asyncio.Lock()
        self._client: httpx.AsyncClient | None = None
        self._client_lock = asyncio.Lock()

    # -- plumbing ----------------------------------------------------------

    async def _get_token(self) -> str:
        """Lazily built, so a missing key file cannot break app import."""
        if self._token is None:
            async with self._token_lock:
                if self._token is None:
                    from gcloud.aio.auth import Token

                    try:
                        self._token = Token(service_file=self.service_account, scopes=SCOPES)
                    except (OSError, ValueError) as exc:
                        raise GrailedDrivePermanentError(
                            "service account key could not be loaded", detail=exc
                        ) from exc
        try:
            return await self._token.get()
        except Exception as exc:  # aiohttp errors, an error body without access_token
            raise GrailedDriveTransientError(
                "could not get an access token", detail=exc
            ) from exc

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

    @staticmethod
    def _raise_for_status(response: httpx.Response, operation: str) -> None:
        """Split a Google error into try-again vs give-up on `reason`, not on status.

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
        except (ValueError, AttributeError):
            pass
        detail = f"{operation}: HTTP {response.status_code} {reason} {message}".strip()
        if response.status_code in (429, 500, 502, 503, 504) or reason in _TRANSIENT_REASONS:
            raise GrailedDriveTransientError(
                f"Google API temporarily unavailable ({operation})", detail=detail
            )
        raise GrailedDrivePermanentError(
            f"Google API rejected {operation} ({response.status_code})", detail=detail
        )

    async def _send(self, method: str, url: str, operation: str, **kwargs: Any) -> httpx.Response:
        client = await self._get_client()
        headers = {"Authorization": f"Bearer {await self._get_token()}"}
        try:
            response = await client.request(method, url, headers=headers, **kwargs)
        except httpx.HTTPError as exc:
            raise GrailedDriveTransientError(f"{operation} did not complete", detail=exc) from exc
        self._raise_for_status(response, operation)
        return response

    async def _request(self, method: str, url: str, operation: str, **kwargs: Any) -> dict:
        response = await self._send(method, url, operation, **kwargs)
        if not response.content:
            return {}
        try:
            return response.json()
        except ValueError as exc:
            # A 200 that is not JSON is a proxy or captive page, not Drive.
            raise GrailedDriveTransientError(
                f"{operation} returned a non-JSON body", detail=response.text[:400]
            ) from exc

    # -- reads -------------------------------------------------------------

    async def list_children(self, folder_id: str) -> list[dict]:
        """Every non-trashed child of `folder_id`, in API order, across all pages.

        Each dict carries id, name, mimeType, createdTime and modifiedTime.
        """
        if not folder_id:
            raise GrailedDrivePermanentError("no folder id to list")
        escaped = folder_id.replace("\\", "\\\\").replace("'", "\\'")
        params: dict[str, Any] = {
            "q": f"'{escaped}' in parents and trashed=false",
            "fields": LIST_FIELDS,
            "pageSize": 1000,
            "supportsAllDrives": "true",
            "includeItemsFromAllDrives": "true",
        }
        files: list[dict] = []
        seen_tokens: set[str] = set()
        while True:
            data = await self._request("GET", DRIVE_FILES, "files.list", params=params)
            files.extend(data.get("files") or [])
            page_token = data.get("nextPageToken")
            if not page_token:
                return files
            # Drive handing back a token it already gave would otherwise loop forever.
            if page_token in seen_tokens:
                raise GrailedDriveTransientError(
                    "files.list repeated a page token",
                    detail=f"files.list: repeated page token after {len(files)} files",
                )
            seen_tokens.add(page_token)
            params = {**params, "pageToken": page_token}

    async def read_file(self, file_id: str, mime_type: str) -> tuple[bytes, str]:
        """The file's bytes and their format: ("csv" | "xlsx").

        A native Google Sheet is exported as CSV, which exports the FIRST tab only
        (UTF-8). An uploaded xlsx comes back byte for byte.
        """
        if mime_type not in (SHEET_MIME, XLSX_MIME):
            raise GrailedDrivePermanentError(f"unsupported result file type {mime_type}")
        if not file_id:
            raise GrailedDrivePermanentError("no file id to read")
        if mime_type == SHEET_MIME:
            response = await self._send(
                "GET", f"{_file_url(file_id)}/export", "files.export",
                params={"mimeType": "text/csv"},
            )
            return response.content, "csv"
        response = await self._send(
            "GET", _file_url(file_id), "files.get media",
            params={"alt": "media", "supportsAllDrives": "true"},
        )
        return response.content, "xlsx"


grailed_drive = GrailedDrive()
