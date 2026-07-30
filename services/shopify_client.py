"""Shopify Admin GraphQL transport.

Transport only: auth, throttling, error classification, pagination. It knows nothing
about products. Domain operations live in services/shopify_admin.py, which changes
every phase while this file should stay stable.

Three things here are load-bearing and were learned the hard way:

1. GraphQL returns HTTP 200 for failures. Throttling, scope denial and shop-inactive
   all arrive as 200 with a top-level `errors` array. Verified live: a locations query
   without read_locations returns 200, `data: null`, `errors[0].extensions.code =
   ACCESS_DENIED`. Status-code-based handling silently reads those as empty successes.

2. There are THREE failure channels, not two: transport status, top-level `errors[]`,
   and `data.<mutation>.userErrors[]`. execute() checks all three so callers cannot
   record a false success by forgetting one.

3. One client instance per store, never a store_id parameter. Store identity bound to
   an object cannot be passed wrong at a call site; the destination store is the one
   with write access, and the source store is the live operational catalog.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Final, Mapping, Sequence

import httpx

from config import config

logger = logging.getLogger(__name__)

# Namespace for deterministic idempotency keys. Fixed forever: changing it would make
# every previously-issued key un-matchable.
IDEMPOTENCY_NAMESPACE: Final[uuid.UUID] = uuid.UUID("6f9619ff-8b86-d011-b42d-00c04fc964ff")

_STORE_ID_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z0-9][a-z0-9-]{0,58}[a-z0-9]$")
_API_VERSION_RE: Final[re.Pattern[str]] = re.compile(r"^\d{4}-\d{2}$")

_SECRET_RE: Final[re.Pattern[str]] = re.compile(
    r"(shpat_[A-Za-z0-9]+|shpss_[A-Za-z0-9]+|"
    r'"?(?:client_secret|access_token|password)"?\s*[:=]\s*"?[^\s",}]+)'
)


def redact(text: str) -> str:
    """Strip anything token-shaped before it reaches a log or the ledger."""
    return _SECRET_RE.sub("[REDACTED]", text)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class ShopifyError(Exception):
    """Base. str(e) is safe to show a user; .detail is the technical message.

    Mirrors the SellercloudPermanentError convention already used in this codebase.
    """

    def __init__(self, message: str, *, store_id: str, operation: str,
                 detail: str | None = None) -> None:
        super().__init__(message)
        self.store_id = store_id
        self.operation = operation
        self.detail = redact(detail or message)


class ShopifyTransientError(ShopifyError):
    """429, 5xx, timeouts, throttling. The row REQUEUES; it does not fail.

    A throttled mutation never executed, so retrying after backoff is still one
    genuine attempt. This does not conflict with the no-auto-resubmit convention.
    """

    def __init__(self, *args: Any, retry_after_seconds: float | None = None,
                 **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.retry_after_seconds = retry_after_seconds


class ShopifyPermanentError(ShopifyError):
    """A retry cannot fix this. Row -> failed."""


class ShopifyScopeError(ShopifyPermanentError):
    """Missing access scope.

    A distinct class because it is operationally load-bearing: this is the expected
    state for every source-store write today. Without it the first enabled source pass
    would produce ~900 identical failures. Callers latch on the first occurrence per
    cycle, log once, and skip the pass.
    """


class ShopifySemanticError(ShopifyPermanentError):
    """HTTP 200 with a populated userErrors array. A business-rule rejection."""

    def __init__(self, *args: Any, user_errors: Sequence[Mapping[str, Any]] = (),
                 **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.user_errors = tuple(user_errors)


class ShopifyMaxCostError(ShopifyPermanentError):
    """Single query exceeded the per-query cost cap. Halve the page size."""


class ShopifyWritesDisabled(ShopifyPermanentError):
    """A write was attempted while the global write guard is engaged."""


# ---------------------------------------------------------------------------
# Global write guard
# ---------------------------------------------------------------------------
# Defaults to OFF. Every mutation in this module goes through execute(is_write=True),
# and that path raises unless writes have been explicitly enabled. This is deliberately
# a structural guarantee rather than a convention: config.toml is the TEST config but
# its Shopify credentials point at the SAME live stores as prod - there is no Shopify
# sandbox - so an accidental write from a dev process hits the real storefront.
#
# enable_writes() is opt-in, per process, and the reason is logged loudly.
_WRITES_ENABLED = False
_WRITES_REASON = ""


def writes_allowed_by_config() -> bool:
    """Whether this ENVIRONMENT permits Shopify writes at all.

    `enabled`/`execute` on a poller are settings a caller obeys. This is a setting a caller
    cannot route around, and the difference matters: three maintenance scripts call
    enable_writes() directly during setup, so they honour no poller config and would write
    to the live stores from a machine nobody considers a writer.

    Absent means True. Adding the key to one config must not change behaviour in every
    other environment.
    """
    return bool(config.get("shopify", {}).get("allow_writes", True))


def enable_writes(reason: str) -> None:
    """Arm the write guard, unless the environment forbids writes outright.

    Deliberately does NOT raise when forbidden. A script calls this during setup, long
    before it knows what it will write; failing here would abort with a message about
    configuration, while failing at the mutation gives ShopifyWritesDisabled naming the
    exact operation that was refused. Same outcome - nothing is written - but the second
    one tells you what would have been.
    """
    global _WRITES_ENABLED, _WRITES_REASON
    if not writes_allowed_by_config():
        logger.warning(
            "SHOPIFY WRITES REFUSED: %s requested writes but [shopify] allow_writes is "
            "false in this environment; the guard stays engaged and every mutation will "
            "raise ShopifyWritesDisabled", reason,
        )
        return
    _WRITES_ENABLED = True
    _WRITES_REASON = reason
    logger.warning("SHOPIFY WRITES ENABLED: %s", reason)


def disable_writes() -> None:
    global _WRITES_ENABLED, _WRITES_REASON
    _WRITES_ENABLED = False
    _WRITES_REASON = ""
    logger.info("Shopify writes disabled")


def writes_enabled() -> bool:
    return _WRITES_ENABLED


# ---------------------------------------------------------------------------
# Cost governor
# ---------------------------------------------------------------------------

@dataclass
class ThrottleStatus:
    maximum_available: float = 4000.0
    currently_available: float = 4000.0
    restore_rate: float = 200.0
    observed_at: float = field(default_factory=time.monotonic)


class CostGovernor:
    """Local mirror of Shopify's leaky bucket, one per store.

    MUST be shared across every caller touching the same store. If the destination
    poller and the repricer each keep their own governor, both believe they own the
    full bucket, together they overdraw, and throttling hits exactly when both are busy.

    Note the documented cost formula and live measurement disagree by roughly 28x
    (formula predicts ~2000 for a 250-product page with variants; measured 79). So we
    do not estimate statically: we read throttleStatus off every response and adapt.
    """

    def __init__(self, store_id: str, reserve_fraction: float = 0.20) -> None:
        self.store_id = store_id
        self.reserve_fraction = reserve_fraction
        self.status = ThrottleStatus()
        self._lock = asyncio.Lock()
        # Starts pessimistic; calibrated from the first response.
        self._estimated_cost = 100.0

    def _available_now(self) -> float:
        s = self.status
        elapsed = time.monotonic() - s.observed_at
        return min(s.maximum_available, s.currently_available + elapsed * s.restore_rate)

    async def acquire(self) -> None:
        async with self._lock:
            reserve = self.status.maximum_available * self.reserve_fraction
            need = self._estimated_cost + reserve
            available = self._available_now()
            if available < need:
                wait = (need - available) / max(self.status.restore_rate, 1.0)
                logger.debug(
                    "%s: pacing %.2fs (avail %.0f, need %.0f)",
                    self.store_id, wait, available, need,
                )
                await asyncio.sleep(wait)

    def observe(self, extensions: Mapping[str, Any] | None) -> None:
        if not extensions:
            return
        cost = extensions.get("cost") or {}
        ts = cost.get("throttleStatus") or {}
        if ts:
            self.status = ThrottleStatus(
                maximum_available=float(ts.get("maximumAvailable", 4000)),
                currently_available=float(ts.get("currentlyAvailable", 4000)),
                restore_rate=float(ts.get("restoreRate", 200)),
                observed_at=time.monotonic(),
            )
        requested = cost.get("requestedQueryCost")
        if requested:
            # Track the high-water mark so pacing stays conservative for the most
            # expensive query shape we actually issue.
            self._estimated_cost = max(float(requested), self._estimated_cost * 0.9)

    def throttled_wait(self, extensions: Mapping[str, Any] | None) -> float:
        """Exact wait implied by a THROTTLED response, not a guess."""
        cost = (extensions or {}).get("cost") or {}
        ts = cost.get("throttleStatus") or {}
        requested = float(cost.get("requestedQueryCost", self._estimated_cost))
        available = float(ts.get("currentlyAvailable", 0))
        rate = float(ts.get("restoreRate", 200)) or 200.0
        return max(1.0, (requested - available) / rate)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ShopifyStoreConfig:
    store_id: str
    client_id: str
    client_secret: str
    api_version: str
    token_refresh_margin_seconds: int = 300

    @property
    def graphql_url(self) -> str:
        return (
            f"https://{self.store_id}.myshopify.com"
            f"/admin/api/{self.api_version}/graphql.json"
        )

    @property
    def token_url(self) -> str:
        return f"https://{self.store_id}.myshopify.com/admin/oauth/access_token"


def load_store_config(store_key: str) -> ShopifyStoreConfig:
    """Resolve a store from config.toml. The key is the ONLY source of the hostname.

    internal_platforms.source_store / dest_store are lookup keys into this table, never
    interpolated into a URL. The token exchange POSTs the client secret to
    https://{store}.myshopify.com, so letting a DB-writable value reach the host would
    be an SSRF that exfiltrates the secret.
    """
    shopify_cfg = config.get("shopify") or {}
    stores = shopify_cfg.get("stores") or {}
    if store_key not in stores:
        raise ShopifyPermanentError(
            f"Shopify store {store_key!r} is not configured",
            store_id=store_key, operation="config",
            detail=f"add [shopify.stores.{store_key}] to config.toml; "
                   f"known: {sorted(stores)}",
        )
    entry = stores[store_key]

    store_id = str(entry.get("store_id") or store_key)
    if not _STORE_ID_RE.match(store_id):
        raise ShopifyPermanentError(
            f"Invalid Shopify store id {store_id!r}",
            store_id=store_id, operation="config",
            detail="store_id must be a bare myshopify subdomain",
        )

    api_version = str(shopify_cfg.get("api_version") or "")
    if not _API_VERSION_RE.match(api_version):
        raise ShopifyPermanentError(
            f"Invalid Shopify api_version {api_version!r}",
            store_id=store_id, operation="config",
            detail="expected YYYY-MM; it is interpolated into the request path",
        )

    for key in ("client_id", "client_secret"):
        if not entry.get(key):
            # Fail loudly at startup rather than as a confusing 401 on first use.
            raise ShopifyPermanentError(
                f"Shopify store {store_key!r} is missing {key}",
                store_id=store_id, operation="config",
            )

    return ShopifyStoreConfig(
        store_id=store_id,
        client_id=str(entry["client_id"]),
        client_secret=str(entry["client_secret"]),
        api_version=api_version,
        token_refresh_margin_seconds=int(
            shopify_cfg.get("token_refresh_margin_seconds", 300)
        ),
    )


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class ShopifyClient:
    """One store. Owns its token, its bucket, and its HTTP client."""

    def __init__(self, store: ShopifyStoreConfig, *,
                 read_concurrency: int = 1, write_concurrency: int = 4) -> None:
        self.store = store
        self.governor = CostGovernor(store.store_id)

        self._token: str | None = None
        self._token_expires_at: float = 0.0
        self._scopes: tuple[str, ...] = ()
        self._token_lock = asyncio.Lock()
        self._client: httpx.AsyncClient | None = None
        self._client_lock = asyncio.Lock()

        # Reads are cost-bound: extra concurrency only drains the bucket faster.
        # Writes are latency-bound and genuinely benefit.
        self._read_sem = asyncio.Semaphore(read_concurrency)
        self._write_sem = asyncio.Semaphore(write_concurrency)

    # -- lifecycle ---------------------------------------------------------

    async def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            async with self._client_lock:
                if self._client is None:
                    self._client = httpx.AsyncClient(
                        timeout=httpx.Timeout(60.0),
                        # Explicit: httpx strips Authorization on cross-origin
                        # redirects but NOT custom headers, so a redirect would carry
                        # X-Shopify-Access-Token to the redirect target. Two other
                        # services in this repo set follow_redirects=True, so a
                        # copy-paste would be silent.
                        follow_redirects=False,
                    )
        return self._client

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # -- auth --------------------------------------------------------------

    def _token_valid(self) -> bool:
        return bool(self._token) and time.time() < self._token_expires_at

    async def _ensure_token(self, *, force: bool = False) -> str:
        # Pre-check outside the lock. Without this the lock is acquired on every
        # request and the whole client serializes to concurrency 1.
        if not force and self._token_valid():
            return self._token  # type: ignore[return-value]

        async with self._token_lock:
            # Re-check inside: another caller may have refreshed while we waited.
            if not force and self._token_valid():
                return self._token  # type: ignore[return-value]
            await self._fetch_token()
            return self._token  # type: ignore[return-value]

    async def _fetch_token(self) -> None:
        client = await self._http()
        try:
            resp = await client.post(
                self.store.token_url,
                json={
                    "client_id": self.store.client_id,
                    "client_secret": self.store.client_secret,
                    "grant_type": "client_credentials",
                },
                headers={"Content-Type": "application/json"},
            )
        except httpx.HTTPError as exc:
            raise ShopifyTransientError(
                "Could not reach Shopify to authenticate",
                store_id=self.store.store_id, operation="token", detail=str(exc),
            ) from exc

        if resp.status_code != 200:
            # Never log the body: on some failures it echoes the request.
            raise ShopifyPermanentError(
                f"Shopify rejected the credentials for {self.store.store_id}",
                store_id=self.store.store_id, operation="token",
                detail=f"HTTP {resp.status_code}",
            )

        data = resp.json()
        self._token = data["access_token"]
        # The grant response carries the scope list; keep it so granted_scopes() is free.
        self._scopes = tuple(s for s in (data.get("scope") or "").split(",") if s)
        expires_in = int(data.get("expires_in", 86399))
        self._token_expires_at = (
            time.time() + expires_in - self.store.token_refresh_margin_seconds
        )
        logger.info(
            "%s: token acquired, scope=%s, expires_in=%ss",
            self.store.store_id, data.get("scope"), expires_in,
        )

    async def granted_scopes(self) -> tuple[str, ...]:
        """Scopes actually granted. Confirm capability from this, not by attempting a write.

        Served from the cached token rather than a fresh OAuth round trip. The token
        exchange already returns `scope`, and scopes only change on app reinstall - which
        revokes the token and is therefore picked up by the existing 401-triggered
        refresh. This used to POST to the token endpoint on EVERY call, and the Internal
        Platforms overview calls it twice per request while polling every 30s: two live
        HTTPS round trips a minute, forever, to re-learn a constant.
        """
        await self._ensure_token()
        return self._scopes

    # -- execute -----------------------------------------------------------

    async def execute(
        self,
        query: str,
        variables: Mapping[str, Any] | None = None,
        *,
        operation: str,
        mutation_name: str | None = None,
        is_write: bool = False,
        max_attempts: int = 4,
    ) -> dict[str, Any]:
        """Run a document and return `data`, or raise.

        All three failure channels are checked here so a caller physically cannot
        record a false success by forgetting one.

        `operation` is required and keyword-only: it lands in logs and error_display,
        and without it every failure reads "GraphQL error" with no clue which query.
        """
        if is_write and not _WRITES_ENABLED:
            raise ShopifyWritesDisabled(
                f"Shopify writes are disabled; refused {operation}",
                store_id=self.store.store_id, operation=operation,
                detail=(
                    "[shopify] allow_writes is false in this environment; no caller can "
                    "enable writes here"
                    if not writes_allowed_by_config() else
                    "call services.shopify_client.enable_writes(reason) to permit "
                    "writes. Off by default because the TEST config points at the "
                    "live stores."
                ),
            )

        sem = self._write_sem if is_write else self._read_sem
        attempt = 0

        while True:
            attempt += 1
            async with sem:
                await self.governor.acquire()
                token = await self._ensure_token()
                client = await self._http()

                try:
                    resp = await client.post(
                        self.store.graphql_url,
                        json={"query": query, "variables": dict(variables or {})},
                        headers={
                            "X-Shopify-Access-Token": token,
                            "Content-Type": "application/json",
                        },
                    )
                except httpx.HTTPError as exc:
                    if attempt >= max_attempts:
                        raise ShopifyTransientError(
                            "Shopify request failed",
                            store_id=self.store.store_id, operation=operation,
                            detail=str(exc),
                        ) from exc
                    await asyncio.sleep(min(2 ** attempt, 30))
                    continue

            # -- channel 1: transport status
            if resp.status_code == 401:
                # Token revoked, typically by an app reinstall (which is exactly what
                # granting a new scope does). A pure TTL cache would serve a dead
                # token for up to 24h after that.
                if attempt < max_attempts:
                    logger.warning("%s: 401 on %s, forcing token refresh",
                                   self.store.store_id, operation)
                    await self._ensure_token(force=True)
                    continue
                raise ShopifyPermanentError(
                    "Shopify authentication failed", store_id=self.store.store_id,
                    operation=operation, detail="401 after refresh",
                )
            if resp.status_code == 403:
                raise ShopifyScopeError(
                    f"Missing Shopify access scope for {operation}",
                    store_id=self.store.store_id, operation=operation,
                    detail=redact(resp.text[:500]),
                )
            if resp.status_code >= 500 or resp.status_code == 429:
                if attempt < max_attempts:
                    await asyncio.sleep(min(2 ** attempt, 30))
                    continue
                raise ShopifyTransientError(
                    "Shopify is unavailable", store_id=self.store.store_id,
                    operation=operation, detail=f"HTTP {resp.status_code}",
                )
            if resp.status_code != 200:
                raise ShopifyPermanentError(
                    f"Shopify returned HTTP {resp.status_code}",
                    store_id=self.store.store_id, operation=operation,
                    detail=redact(resp.text[:500]),
                )

            body = resp.json()
            self.governor.observe(body.get("extensions"))

            # -- channel 2: top-level errors, at HTTP 200
            top = body.get("errors") or []
            if top:
                code = (top[0].get("extensions") or {}).get("code")
                message = top[0].get("message", "GraphQL error")

                if code == "THROTTLED":
                    if attempt < max_attempts:
                        wait = self.governor.throttled_wait(body.get("extensions"))
                        logger.info("%s: throttled on %s, waiting %.1fs",
                                    self.store.store_id, operation, wait)
                        await asyncio.sleep(wait)
                        continue
                    raise ShopifyTransientError(
                        "Shopify rate limit exhausted", store_id=self.store.store_id,
                        operation=operation, detail=message,
                    )
                if code == "MAX_COST_EXCEEDED":
                    raise ShopifyMaxCostError(
                        "Query too expensive; reduce the page size",
                        store_id=self.store.store_id, operation=operation,
                        detail=message,
                    )
                if code == "ACCESS_DENIED":
                    raise ShopifyScopeError(
                        f"Missing Shopify access scope for {operation}: {message}",
                        store_id=self.store.store_id, operation=operation,
                        detail=message,
                    )
                if code in ("SHOP_INACTIVE", "INTERNAL_SERVER_ERROR"):
                    raise ShopifyTransientError(
                        f"Shopify unavailable: {message}",
                        store_id=self.store.store_id, operation=operation,
                        detail=message,
                    )
                raise ShopifyPermanentError(
                    f"Shopify rejected the query: {message}",
                    store_id=self.store.store_id, operation=operation, detail=str(top),
                )

            data = body.get("data") or {}

            # -- channel 3: userErrors, at HTTP 200 with no top-level errors
            if mutation_name:
                payload = data.get(mutation_name) or {}
                user_errors = payload.get("userErrors") or []
                if user_errors:
                    msg = "; ".join(
                        f"{e.get('field') or '?'}: {e.get('message')}" for e in user_errors
                    )
                    raise ShopifySemanticError(
                        msg, store_id=self.store.store_id, operation=operation,
                        detail=str(user_errors), user_errors=user_errors,
                    )
            return data

    # -- pagination --------------------------------------------------------

    async def paginate(
        self,
        query: str,
        variables: Mapping[str, Any],
        *,
        connection_path: Sequence[str],
        operation: str,
        page_size: int = 250,
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield NODES across pages. Never materializes the whole catalog.

        Page size 250 is measured-safe on 2026-01 (79 points for products + variants +
        metafield). If MAX_COST_EXCEEDED ever appears the caller should halve it.
        """
        cursor: str | None = None
        while True:
            data = await self.execute(
                query,
                {**variables, "first": page_size, "after": cursor},
                operation=operation,
            )
            node = data
            for key in connection_path:
                node = (node or {}).get(key) or {}
            for item in node.get("nodes") or []:
                yield item
            page_info = node.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                return
            cursor = page_info.get("endCursor")
            if not cursor:
                return

    # -- idempotency -------------------------------------------------------

    @staticmethod
    def idempotency_key(*parts: Any) -> str:
        """Deterministic UUIDv5 so a requeued attempt reuses the same key.

        Required on inventorySetQuantities from API version 2026-04; available and
        optional in 2026-01, so we write it now. Shopify retains keys for 24 hours.
        """
        return str(uuid.uuid5(IDEMPOTENCY_NAMESPACE, "|".join(str(p) for p in parts)))


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

_clients: dict[str, ShopifyClient] = {}
_registry_lock = asyncio.Lock()


async def get_shopify_client(store_key: str) -> ShopifyClient:
    """Process-wide singleton per store.

    Shared deliberately: the cost governor is only correct if every caller touching a
    store paces against the same bucket.
    """
    if store_key in _clients:
        return _clients[store_key]
    async with _registry_lock:
        if store_key not in _clients:
            _clients[store_key] = ShopifyClient(load_store_config(store_key))
        return _clients[store_key]


async def close_shopify_clients() -> None:
    """Call from app shutdown, alongside the other long-lived httpx clients."""
    for client in list(_clients.values()):
        await client.aclose()
    _clients.clear()
