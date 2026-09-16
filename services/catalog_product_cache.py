"""The active parent products, held in memory instead of mirrored into skubase.

The catalog filters and sorts across two databases: a product's own fields (title, mpn,
brand, product type, company) live in the products DB, while its value, platform coverage,
listing state and images live in skubase. Postgres cannot join across databases, and this
managed role cannot create a foreign-data wrapper (not superuser, and postgres_fdw is
available but not installed), so one side has to travel. It used to travel as a table,
catalog_products, refreshed every 15 minutes by CatalogSyncPoller. Now it travels as a bound
text[] of the SKUs a request matched, and this module is where those SKUs come from.

Measured on TEST for one page of 50 rows: joining the mirror took 67 ms server-side, binding
the whole 41,570-SKU universe takes 36.6 ms, and 5.2 ms when the array arrives already
sorted, because then the query only has to page it. Search, the exclusion rules and the
non-value sorts run in Python against this cache, so the bound array is usually far smaller
than the universe.

Freshness. A full load runs at start and every 5 minutes after (the mirror it replaces ran
every 15). Between loads, the products DB's own cache_invalidate NOTIFY patches single
products: that trigger already fires on parent_products for WarehouseManagement, so nothing
had to change in that database, and the payload carries the whole row, so a patch costs no
query. The trigger swallows its own errors and pg_notify silently drops a payload over 8 KB,
so a missed event is always possible. The timed reload is the backstop for that, not an
optimization.

Readers never lock and never await. A load or a patch builds a whole new Snapshot and
rebinds one module global, which is atomic under the GIL, so a reader sees either the old
snapshot or the new one, never a half-built index. Rebuilding the sort orders after a patch
is coalesced, because a bulk import in the products DB can emit thousands of notifications
in a second and each one would otherwise re-sort 42k SKUs.
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import asyncpg
from tortoise import connections

from config import PRODUCT_DB_URL

logger = logging.getLogger(__name__)

REFRESH_INTERVAL_SECONDS = 300
# A burst of notifications rebuilds the sort orders once, shortly after it stops.
PATCH_COALESCE_SECONDS = 0.5
NOTIFY_CHANNEL = "cache_invalidate"
LISTEN_READY_TIMEOUT = 10

LOAD_SQL = """
SELECT sku, title, mpn, brand, product_type, company_code, created_at
  FROM parent_products
 WHERE is_active
"""


@dataclass(frozen=True, slots=True)
class Product:
    """One active parent, with the fields the catalog shows or filters on.

    `haystack` is sku, mpn and title upper-cased into one string, so a contains-search is a
    single `in` per product rather than three lower/upper calls over 42k rows per request.
    """

    sku: str
    title: str
    mpn: Optional[str]
    brand: Optional[str]
    product_type: Optional[str]
    company_code: Optional[int]
    created_at: Optional[datetime]
    haystack: str


@dataclass(frozen=True, slots=True)
class Snapshot:
    """An immutable view of the cache, swapped in whole.

    `version` increments on every rebuild, so callers that derive expensive things from a
    snapshot (catalog_service memoizes the exclusion pairs) can key on it instead of on
    object identity, which a garbage collector is free to reuse.
    """

    by_sku: Dict[str, Product]
    order_sku: Tuple[str, ...]
    order_newest: Tuple[str, ...]
    loaded_at: datetime
    version: int


_working: Dict[str, Product] = {}
_snapshot: Optional[Snapshot] = None
_version = 0
_loaded_at: Optional[datetime] = None

_dirty: Optional[asyncio.Event] = None
_shutdown: Optional[asyncio.Event] = None
_listen_ready: Optional[asyncio.Event] = None
_refresh_task: Optional[asyncio.Task] = None
_listen_task: Optional[asyncio.Task] = None
_rebuild_task: Optional[asyncio.Task] = None
_listen_conn: Optional[asyncpg.Connection] = None


def _product(sku: str, title: Any, mpn: Any, brand: Any, product_type: Any,
             company_code: Any, created_at: Any) -> Product:
    title = title or ""
    mpn = mpn or None
    parts = [sku, mpn or "", title]
    return Product(
        sku=sku,
        title=title,
        mpn=mpn,
        brand=brand or None,
        product_type=product_type or None,
        company_code=company_code,
        created_at=created_at,
        haystack="\x00".join(parts).upper(),
    )


def _rebuild_snapshot() -> None:
    """Build the ordered indexes from `_working` and publish them."""
    global _snapshot, _version
    _version += 1
    products = list(_working.values())
    order_sku = tuple(sorted(_working))
    # Newest first, undated last, SKU breaking ties so paging is stable. Two stable passes,
    # not one reversed tuple: the mirror ordered `product_created_at DESC NULLS LAST, sku`,
    # so products sharing a created date came back in ASCENDING SKU order, and reversing a
    # (created_at, sku) tuple reverses the tie-break too. A parity check against the mirror
    # caught it on a run of products created in the same import.
    dated = sorted((p for p in products if p.created_at is not None), key=lambda p: p.sku)
    dated.sort(key=lambda p: p.created_at, reverse=True)
    undated = sorted(p.sku for p in products if p.created_at is None)
    order_newest = tuple([p.sku for p in dated] + undated)
    _snapshot = Snapshot(
        by_sku=dict(_working),
        order_sku=order_sku,
        order_newest=order_newest,
        loaded_at=_loaded_at or datetime.now(timezone.utc),
        version=_version,
    )


async def _full_load() -> int:
    """Replace the cache from the products DB. Raises rather than publishing a partial load."""
    global _working, _loaded_at
    started = time.monotonic()
    rows = await connections.get("product_db").execute_query_dict(LOAD_SQL)
    # An empty products DB is a failed read, not an empty catalog: keep what we have, the
    # same judgment the mirror's 90% read guard made.
    if not rows and _working:
        raise RuntimeError(
            f"products DB returned no active parents while the cache holds {len(_working)}"
        )
    _working = {
        row["sku"]: _product(
            row["sku"], row["title"], row["mpn"], row["brand"],
            row["product_type"], row["company_code"], row["created_at"],
        )
        for row in rows
        if row.get("sku")
    }
    _loaded_at = datetime.now(timezone.utc)
    _rebuild_snapshot()
    logger.info(
        "catalog_product_cache: loaded %d active parents in %.2fs",
        len(_working), time.monotonic() - started,
    )
    return len(_working)


def _parse_created_at(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _patch(row: Dict[str, Any], op: str) -> None:
    """Apply one notification. The payload carries the whole row, so this costs no query."""
    sku = row.get("sku")
    if not sku:
        return
    if op == "delete" or not row.get("is_active", True):
        if _working.pop(sku, None) is None:
            return
    else:
        _working[sku] = _product(
            sku, row.get("title"), row.get("mpn"), row.get("brand"),
            row.get("product_type"), row.get("company_code"),
            _parse_created_at(row.get("created_at")),
        )
    if _dirty is not None:
        _dirty.set()


def _on_notify(conn, pid, channel, payload) -> None:  # asyncpg listener callback
    try:
        data = json.loads(payload) if payload else {}
    except Exception:  # noqa: BLE001
        return
    if data.get("table") != "parent_products":
        return
    try:
        _patch(data.get("row") or {}, str(data.get("op") or ""))
    except Exception:  # noqa: BLE001
        logger.exception("catalog_product_cache: patch failed")


async def _rebuild_loop() -> None:
    """Coalesce patches: rebuild the sort orders once a burst has settled."""
    assert _dirty is not None and _shutdown is not None
    while not _shutdown.is_set():
        await _dirty.wait()
        if _shutdown.is_set():
            return
        await asyncio.sleep(PATCH_COALESCE_SECONDS)
        _dirty.clear()
        try:
            _rebuild_snapshot()
        except Exception:  # noqa: BLE001
            logger.exception("catalog_product_cache: rebuild after patches failed")


async def _refresh_loop() -> None:
    assert _shutdown is not None
    while not _shutdown.is_set():
        try:
            await asyncio.wait_for(_shutdown.wait(), timeout=REFRESH_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            pass
        if _shutdown.is_set():
            return
        try:
            await _full_load()
        except Exception:  # noqa: BLE001
            logger.exception("catalog_product_cache: periodic load failed; keeping the last one")


async def _listen_loop() -> None:
    """Hold a connection LISTENing on the products DB, and reload after any gap in it."""
    global _listen_conn
    assert _shutdown is not None
    backoff = 1.0
    first = True
    while not _shutdown.is_set():
        try:
            _listen_conn = await asyncpg.connect(
                PRODUCT_DB_URL.replace("postgres://", "postgresql://")
            )
            await _listen_conn.add_listener(NOTIFY_CHANNEL, _on_notify)
            logger.info("catalog_product_cache: LISTEN %s active", NOTIFY_CHANNEL)
            backoff = 1.0
            if _listen_ready is not None:
                _listen_ready.set()
            # A reconnect means notifications were missed while it was down.
            if not first:
                try:
                    await _full_load()
                except Exception:  # noqa: BLE001
                    logger.exception("catalog_product_cache: load after reconnect failed")
            first = False
            while not _shutdown.is_set():
                try:
                    await asyncio.wait_for(_shutdown.wait(), timeout=30)
                except asyncio.TimeoutError:
                    try:
                        await _listen_conn.execute("SELECT 1")  # keepalive
                    except Exception:  # noqa: BLE001
                        break
        except Exception:  # noqa: BLE001
            logger.exception("catalog_product_cache: LISTEN dropped, retrying in %.0fs", backoff)
        finally:
            if _listen_conn is not None:
                try:
                    await _listen_conn.close()
                except Exception:  # noqa: BLE001
                    pass
                _listen_conn = None
        if _shutdown.is_set():
            return
        try:
            await asyncio.wait_for(_shutdown.wait(), timeout=backoff)
        except asyncio.TimeoutError:
            pass
        backoff = min(backoff * 2, 30.0)


async def start() -> None:
    """Load the cache, then keep it fresh. Raises if the first load fails, so a caller can
    decide whether the API should still come up."""
    global _dirty, _shutdown, _listen_ready, _refresh_task, _listen_task, _rebuild_task
    _dirty = asyncio.Event()
    _shutdown = asyncio.Event()
    _listen_ready = asyncio.Event()
    await _full_load()
    _refresh_task = asyncio.create_task(_refresh_loop(), name="catalog_product_cache_refresh")
    _rebuild_task = asyncio.create_task(_rebuild_loop(), name="catalog_product_cache_rebuild")
    _listen_task = asyncio.create_task(_listen_loop(), name="catalog_product_cache_listen")
    # Not fatal: without LISTEN the cache is still correct, just up to one refresh stale.
    try:
        await asyncio.wait_for(_listen_ready.wait(), timeout=LISTEN_READY_TIMEOUT)
    except asyncio.TimeoutError:
        logger.warning(
            "catalog_product_cache: LISTEN not ready in %ds; relying on the %ds reload",
            LISTEN_READY_TIMEOUT, REFRESH_INTERVAL_SECONDS,
        )


async def stop() -> None:
    if _shutdown is not None:
        _shutdown.set()
    if _dirty is not None:
        _dirty.set()  # release the rebuild loop from its wait
    for task in (_refresh_task, _rebuild_task, _listen_task):
        if task is None:
            continue
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=5)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            task.cancel()
        except Exception:  # noqa: BLE001
            pass
    if _listen_conn is not None:
        try:
            await _listen_conn.close()
        except Exception:  # noqa: BLE001
            pass


def snapshot() -> Optional[Snapshot]:
    """The current snapshot, or None before the first successful load."""
    return _snapshot


def status() -> Dict[str, Any]:
    snap = _snapshot
    return {
        "loaded_at": snap.loaded_at if snap else None,
        "products": len(snap.by_sku) if snap else 0,
        "version": snap.version if snap else 0,
        "listening": _listen_conn is not None,
    }
