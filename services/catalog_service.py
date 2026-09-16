"""The catalog browser: every active parent product with its value, platform coverage and
listing state, and batch creation from a selection of them.

The data is split across two databases that cannot be joined:

- Product fields (title, mpn, brand, product type, company, created date) come from
  catalog_product_cache, which holds the active parents in memory, loaded from the products
  DB. They used to be mirrored into skubase as catalog_products; that table is gone.
- Value, platform coverage, listing state, open work and images come from skubase, so one
  query still filters, sorts and pages across all of them.

So a request is resolved in two halves. Python matches the search term, works out which
platforms each product is excluded from, and applies the sku and newest sorts, all against
the cache; the resulting SKUs are bound into the SQL as one text[] and become the catalog's
driving table (`cat`, with the Python order preserved as `ord`). SQL does the rest. Binding
the whole 41,570-SKU universe measured faster than joining the old mirror (36.6 ms against
67 ms, and 5.2 ms when the array is already ordered), and a filtered request binds far less.

Raw SQL over the default connection, like product_queue_service. Every WHERE fragment is
built by build_catalog_where and coverage_join, which bind every request value as a
parameter; sort orders and states are looked up in fixed dictionaries, never interpolated.
"""

import json
import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from fastapi import HTTPException
from tortoise import connections
from tortoise.transactions import in_transaction

from models.api_models import CatalogFilters
from models.db_models import Batch
from services import catalog_product_cache
from services.catalog_product_cache import Product, Snapshot
from services.product_queue_service import resolve_search

logger = logging.getLogger(__name__)

# value_desc sorts in SQL, because the values live there. The other two are applied to the
# SKU array in Python, so the query only has to page an order it was handed: `ord`.
CATALOG_SORTS = {
    "value_desc": "v.value DESC NULLS LAST, c.sku",
    "newest": "c.ord",
    "sku": "c.ord",
}

# The same orders again, over the materialized page rather than the join.
OUTER_SORTS = {
    "value_desc": "page.value DESC NULLS LAST, page.sku",
    "newest": "page.ord",
    "sku": "page.ord",
}

SUMMARY_CACHE_SECONDS = 60
PLATFORMS_CACHE_SECONDS = 60

# What "is this parent on this platform" means, in two shapes that must agree:
#
# - Excluded: its brand, product type or company excludes the platform (exclusions_for).
#   Wins over everything below, as it does on the listing view, and never counts as None.
# - Listed: a row in external_listing_ids (which never expire, so "has been listed"), or a
#   latest submission that succeeded before ids were captured.
# - In progress / failed: the latest submission for the platform across the parent's
#   listings.
# - None: neither.
#
# coverage_state_sql is the per-row form, used for the pills on one page of rows; the
# excluded case is applied in Python afterwards, from the cache.
# COVERAGE_CTE is the set form, used to filter the whole catalog: evaluating the per-row
# form for every one of ~42k parents took seconds, while the set form is two small scans.
_COVERAGE_TEMPLATE = """CASE
  WHEN EXISTS (
      SELECT 1 FROM external_listing_ids e
       WHERE e.platform_id = {platform} AND e.parent_sku = {sku}
  ) THEN 'listed'
  ELSE COALESCE((
      SELECT CASE WHEN s.status IN ('queued', 'pending', 'processing', 'awaiting_action') THEN 'in_progress'
                  WHEN s.status = 'success' THEN 'listed'
                  WHEN s.status = 'failed' THEN 'failed' END
        FROM listings l
        JOIN listing_submissions s ON s.listing_id = l.id
       WHERE l.product_id = {sku} AND s.platform_id = {platform}
       ORDER BY l.created_at DESC, s.attempt_number DESC
       LIMIT 1
  ), 'none')
END"""

# Why a platform is excluded for a product: the rules listing_required_platforms applies to
# a listing. A brand or product type (its value or an alias, case-insensitive) whose record
# lists the platform in excluded_platforms, or a platform whose platform_settings
# .company_codes does not include the product's SellerCloud company. SellerCloud is never
# excluded. The rules are read by exclusion_rules, indexed by rule_index and applied to the
# cached products by exclusions_for; the set form arrives in SQL as bound pairs.

COVERAGE_CTE = """excl AS (
    -- The picked platforms each product is excluded from, worked out in Python from the
    -- product cache and bound as (sku, platform) pairs by exclusion_pairs_cte.
    SELECT sku, platform_id FROM excl_pairs WHERE platform_id = ANY({platforms})
),
cov_listing AS (
    SELECT COALESCE(e.parent_sku, ls.product_id) AS sku,
           COALESCE(e.platform_id, ls.platform_id) AS platform_id,
           CASE WHEN e.parent_sku IS NOT NULL THEN 'listed'
                WHEN ls.status IN ('queued', 'pending', 'processing', 'awaiting_action') THEN 'in_progress'
                WHEN ls.status = 'success' THEN 'listed'
                WHEN ls.status = 'failed' THEN 'failed'
                ELSE 'none' END AS state
      FROM (SELECT DISTINCT parent_sku, platform_id FROM external_listing_ids
             WHERE platform_id = ANY({platforms})) e
      FULL JOIN (
          SELECT DISTINCT ON (l.product_id, s.platform_id) l.product_id, s.platform_id, s.status
            FROM listing_submissions s
            JOIN listings l ON l.id = s.listing_id
           WHERE s.platform_id = ANY({platforms})
           ORDER BY l.product_id, s.platform_id, l.created_at DESC, s.attempt_number DESC
      ) ls ON ls.product_id = e.parent_sku AND ls.platform_id = e.platform_id
),
cov AS (
    -- An exclusion wins over any listing state, as on the listing view.
    SELECT COALESCE(x.sku, l.sku) AS sku,
           COALESCE(x.platform_id, l.platform_id) AS platform_id,
           CASE WHEN x.sku IS NOT NULL THEN 'excluded' ELSE l.state END AS state
      FROM excl x
      FULL JOIN cov_listing l ON l.sku = x.sku AND l.platform_id = x.platform_id
),
cov_match AS (
    -- One row per product with coverage on any picked platform: is every picked platform in
    -- a chosen state? A picked platform with no row in cov is "none" for that product.
    SELECT sku,
           count(*) FILTER (WHERE state = ANY({states}))
             + CASE WHEN 'none' = ANY({states}) THEN cardinality({platforms}) - count(*) ELSE 0 END
             = cardinality({platforms}) AS ok
      FROM cov
     GROUP BY sku
)"""


def coverage_state_sql(platform: str, sku: str = "c.sku") -> str:
    """The per-row coverage CASE for one platform. Both arguments are SQL expressions (a
    bound placeholder or a column name), never request text."""
    return _COVERAGE_TEMPLATE.format(platform=platform, sku=sku)


CATALOG_FROM = """
FROM cat c
LEFT JOIN parent_product_values v ON v.parent_sku = c.sku
LEFT JOIN catalog_product_images im ON im.parent_sku = c.sku
LEFT JOIN (
    SELECT DISTINCT ON (w.product_id) w.product_id, w.batch_id, w.kind
      FROM (
          SELECT l.product_id, l.batch_id, 'listing' AS kind
            FROM batches b
            JOIN listings l ON l.batch_id = b.id
           WHERE b.status IN ('new', 'in_progress')
             AND NOT l.submitted
          UNION ALL
          SELECT j.product_id, j.batch_id, 'generating' AS kind
            FROM batch_generation_jobs j
           WHERE j.status <> 'done'
      ) w
     ORDER BY w.product_id, w.kind DESC, w.batch_id
) ow ON ow.product_id = c.sku
"""

# The blockers, in precedence order: already being worked somewhere wins over images.
BLOCKED_REASON_SQL = """CASE
    WHEN ow.batch_id IS NOT NULL THEN 'in_open_batch'
    WHEN im.parent_sku IS NULL THEN 'no_images'
END"""

ELIGIBLE_SQL = "(ow.batch_id IS NULL AND im.parent_sku IS NOT NULL)"

NEVER_LISTED_SQL = """(
    NOT EXISTS (SELECT 1 FROM listings lx WHERE lx.product_id = c.sku)
    AND NOT EXISTS (SELECT 1 FROM batch_generation_jobs jx WHERE jx.product_id = c.sku AND jx.status <> 'done')
)"""


# The quick filters. "Listed" and "Platforms pending" are about EVERY enabled platform the
# product is not excluded from ("eligible"), so they need the exclusion pairs for all of
# them and a listed set over all of them, not just the ones a coverage filter picked.
# "Images pending" is the state where a listing counts as submitted everywhere but
# photography has not delivered: submissions for platforms with requires_images sit queued
# until upload_status flips.
LISTING_STATE_CTE = """excl_all AS (
    -- Every enabled platform each product is excluded from, from the same bound pairs.
    SELECT sku, platform_id FROM excl_pairs WHERE platform_id = ANY({platforms})
),
listed_all AS (
    -- "Listed" exactly as the tiles mean it: an external id, or a latest submission that
    -- succeeded. Keep in step with cov_listing in COVERAGE_CTE.
    SELECT DISTINCT parent_sku AS sku, platform_id
      FROM external_listing_ids
     WHERE platform_id = ANY({platforms})
    UNION
    SELECT ls.product_id, ls.platform_id
      FROM (
          SELECT DISTINCT ON (l.product_id, s.platform_id) l.product_id, s.platform_id, s.status
            FROM listing_submissions s
            JOIN listings l ON l.id = s.listing_id
           WHERE s.platform_id = ANY({platforms})
           ORDER BY l.product_id, s.platform_id, l.created_at DESC, s.attempt_number DESC
      ) ls
     WHERE ls.status = 'success'
),
listing_state AS (
    -- Only for products that have an exclusion or a listing: those are the only ones whose
    -- counts differ from "every enabled platform eligible, nothing listed", which the WHERE
    -- supplies by COALESCE for everyone else.
    --
    -- Derived from the small side deliberately. Scanning the bound SKU array here instead
    -- cost 250 ms against the old mirror join's 140 ms, because a materialized array has no
    -- statistics and no index for the planner to work with, while this side is the ~4k
    -- products with listings rather than all ~42k.
    SELECT s.sku,
           cardinality({platforms}) - COALESCE(x.n, 0) AS eligible,
           COALESCE(l.n, 0) AS listed
      FROM (SELECT sku FROM excl_all UNION SELECT sku FROM listed_all) s
      LEFT JOIN (SELECT sku, count(*) AS n FROM excl_all GROUP BY sku) x ON x.sku = s.sku
      LEFT JOIN (
          SELECT la.sku, count(*) AS n
            FROM listed_all la
           WHERE NOT EXISTS (
               SELECT 1 FROM excl_all e WHERE e.sku = la.sku AND e.platform_id = la.platform_id
           )
           GROUP BY la.sku
      ) l ON l.sku = s.sku
)"""

# Submitted everywhere it had to be, and still waiting on photography.
IMAGES_PENDING_SQL = """EXISTS (
    SELECT 1 FROM listings l
     WHERE l.product_id = c.sku AND l.submitted AND l.upload_status = 'pending'
)"""


# ---------------------------------------------------------------------------
# The cache side: the driving SKU list, and the exclusion rules applied to it
# ---------------------------------------------------------------------------


def require_snapshot() -> Snapshot:
    """The product cache, or a 503. The catalog is the only page that needs it, so the API
    starts without it and says so here rather than failing to boot."""
    snap = catalog_product_cache.snapshot()
    if snap is None:
        raise HTTPException(status_code=503, detail="Catalog is still loading")
    return snap


def match_skus(
    snap: Snapshot,
    exact: Optional[str],
    like: Optional[str],
    sort: str,
    companies: Optional[Set[int]] = None,
) -> List[str]:
    """The SKUs a search matches, in the order the sort wants them bound.

    An exact term is a resolved parent SKU (resolve_search turns a child SKU into its
    parent), so it matches one product or none. Anything else is a case-insensitive
    substring of the SKU, MPN or title, which each cached product carries pre-upper-cased as
    one haystack. There is no minimum term length any more: the old three-character floor
    existed because a shorter term had no trigrams to look up in the mirror's GIN indexes,
    and scanning 42k strings in Python has no such cliff.
    """
    order = snap.order_newest if sort == "newest" else snap.order_sku
    by_sku = snap.by_sku

    if exact:
        skus = [exact] if exact in by_sku else []
    elif like:
        term = like.upper()
        skus = [sku for sku in order if term in by_sku[sku].haystack]
    else:
        skus = list(order)

    # A product belongs to exactly one company, so several picked companies mean "any of
    # these". Contrast coverage_platform, where several mean "all of these".
    if companies:
        skus = [sku for sku in skus if by_sku[sku].company_code in companies]
    return skus


def catalog_companies(snap: Snapshot) -> List[Dict[str, Any]]:
    """The companies the catalog holds products for, lowest code first.

    Derived from the cache so the filter's options need no UI change when a company is
    added, and labelled through ebay_service's existing map rather than a second copy of
    it. Deferred import: ebay_service is heavy and nothing else here needs it.
    """
    from services.ebay_service import company_label

    codes = sorted({p.company_code for p in snap.by_sku.values() if p.company_code is not None})
    return [{"code": code, "label": company_label(code)} for code in codes]


@dataclass(frozen=True)
class RuleIndex:
    """The exclusion rules turned inside out, for lookups by product."""

    by_brand: Dict[str, Tuple[str, ...]] = field(default_factory=dict)
    by_type: Dict[str, Tuple[str, ...]] = field(default_factory=dict)
    company_allowed: Dict[str, Set[str]] = field(default_factory=dict)
    version: int = 0


def rule_index(rules: "ExclusionRules") -> RuleIndex:
    by_brand: Dict[str, List[str]] = {}
    by_type: Dict[str, List[str]] = {}
    for kind, key, platform in zip(rules.kinds, rules.keys, rules.platforms):
        target = by_brand if kind == "brand" else by_type
        target.setdefault(key.lower(), []).append(platform)
    allowed: Dict[str, Set[str]] = {platform: set() for platform in rules.company_platforms}
    for platform, code in zip(rules.allowed_platforms, rules.allowed_codes):
        allowed.setdefault(platform, set()).add(code)
    return RuleIndex(
        by_brand={k: tuple(v) for k, v in by_brand.items()},
        by_type={k: tuple(v) for k, v in by_type.items()},
        company_allowed=allowed,
        version=rules.version,
    )


def exclusions_for(
    product: Product, index: RuleIndex, platforms: Set[str]
) -> Dict[str, List[str]]:
    """{platform: reasons} for one product, over the platforms asked about.

    The reasons are the same strings the SQL form used ("brand", "product type",
    "company"), sorted, because the UI shows them and the two forms have to agree.
    """
    out: Dict[str, List[str]] = {}

    def add(platform: str, reason: str) -> None:
        if platform not in platforms:
            return
        reasons = out.setdefault(platform, [])
        if reason not in reasons:
            reasons.append(reason)

    if product.brand:
        for platform in index.by_brand.get(product.brand.lower(), ()):
            add(platform, "brand")
    if product.product_type:
        for platform in index.by_type.get(product.product_type.lower(), ()):
            add(platform, "product type")
    if product.company_code is not None:
        code = str(product.company_code)
        for platform, allowed in index.company_allowed.items():
            if code not in allowed:
                add(platform, "company")
    for reasons in out.values():
        reasons.sort()
    return out


_pairs_memo: Dict[Tuple[int, int, Tuple[str, ...]], Tuple[List[str], List[str]]] = {}


def exclusion_pairs(
    snap: Snapshot, index: RuleIndex, platforms: Iterable[str]
) -> Tuple[List[str], List[str]]:
    """Every (sku, platform) exclusion over the whole snapshot, as two parallel lists.

    Memoized on the snapshot and rule versions: this walks 42k products against every
    platform asked about, while the snapshot changes every few minutes and the rules every
    minute at most. A company allow-list makes this large (on prod eBay allows one company,
    so most of the catalog is excluded from it), which is the price of computing exclusions
    off a cache instead of in SQL.
    """
    key = (snap.version, index.version, tuple(sorted(set(platforms))))
    hit = _pairs_memo.get(key)
    if hit is not None:
        return hit
    wanted = set(key[2])
    skus: List[str] = []
    names: List[str] = []
    for sku, product in snap.by_sku.items():
        for platform in exclusions_for(product, index, wanted):
            skus.append(sku)
            names.append(platform)
    if len(_pairs_memo) > 8:
        _pairs_memo.clear()
    _pairs_memo[key] = (skus, names)
    return skus, names


# ---------------------------------------------------------------------------
# The SQL side
# ---------------------------------------------------------------------------


def catalog_cte(params: List[Any], skus: Sequence[str]) -> str:
    """`cat(sku, ord)`: the request's SKUs as the catalog's driving table.

    WITH ORDINALITY keeps the order Python bound them in, so a sku or newest sort costs the
    query nothing. Bound first, so it is $1 and the rest number on from there.
    """
    params.append(list(skus))
    return f"""cat AS (
    SELECT t.sku, t.ord FROM unnest(${len(params)}::text[]) WITH ORDINALITY AS t(sku, ord)
)"""


def exclusion_pairs_cte(params: List[Any], pairs: Tuple[List[str], List[str]]) -> str:
    """`excl_pairs(sku, platform_id)` from two bound arrays, which excl and excl_all read.

    MATERIALIZED because both of those may read it and the planner cannot see into an
    unnest twice for free.
    """
    skus, platforms = pairs
    params.append(list(skus))
    skus_param = f"${len(params)}::text[]"
    params.append(list(platforms))
    platforms_param = f"${len(params)}::text[]"
    return f"""excl_pairs AS MATERIALIZED (
    SELECT e.sku, e.platform_id
      FROM unnest({skus_param}, {platforms_param}) AS e(sku, platform_id)
)"""


def coverage_filter_on(filters: CatalogFilters) -> bool:
    """A coverage filter needs both a platform and a state; either alone filters nothing."""
    return bool(filters.coverage_platform and filters.coverage_state)


def listing_state_on(filters: CatalogFilters) -> bool:
    """Whether the quick filter needs the eligible-and-listed counts (CTEs and a join)."""
    return filters.listing_status in ("platforms_pending", "listed")


def listing_state_join(params: List[Any], enabled: List[str]) -> Tuple[str, str, str]:
    """(CTEs, JOIN, platforms placeholder) for the quick filters, binding the enabled
    platforms. Refers to excl_pairs, which must come first in the WITH.

    The placeholder comes back because build_catalog_where needs it: a product with no
    listing_state row is eligible on every enabled platform, and that default is
    cardinality() of this same bound array.
    """
    params.append(list(enabled))
    param = f"${len(params)}::text[]"
    return (
        LISTING_STATE_CTE.format(platforms=param),
        "LEFT JOIN listing_state ls ON ls.sku = c.sku",
        param,
    )


def coverage_join(params: List[Any], filters: CatalogFilters) -> Tuple[str, str]:
    """(CTEs, JOIN) for a coverage filter, or ("", "") without one. Binds the platforms, then
    the states.

    Several platforms combine with AND: a product matches only when every picked platform is
    in one of the chosen states. The CTEs refer to excl_pairs, which must come first in the
    WITH. Call before build_catalog_where: the CTEs' placeholders must be numbered first.
    """
    if not coverage_filter_on(filters):
        return "", ""
    # Deduplicated, since cardinality() of the bound array is the number that must match.
    params.append(list(dict.fromkeys(filters.coverage_platform)))
    platforms = f"${len(params)}::text[]"
    params.append(list(filters.coverage_state))
    states = f"${len(params)}::text[]"
    return (
        COVERAGE_CTE.format(platforms=platforms, states=states),
        "LEFT JOIN cov_match cm ON cm.sku = c.sku",
    )


def build_catalog_where(
    params: List[Any], filters: CatalogFilters, platforms_param: Optional[str] = None
) -> str:
    """WHERE body for the catalog, appending its parameters to `params` in order.

    `platforms_param` is listing_state_join's bound platform array, required by the two
    quick filters that compare eligible with listed.

    Pure, so the placeholder numbering is testable without a database: a mismatched $n does
    not raise, it binds the wrong value, and a catalog silently filtered by the wrong thing
    looks exactly like a catalog with nothing in it. The search term is not here: it is
    resolved against the product cache by match_skus, which decides the bound SKU list.
    """
    clauses: List[str] = ["TRUE"]

    def placeholder(value: Any) -> str:
        params.append(value)
        return f"${len(params)}"

    if filters.coverage_platform and filters.coverage_state:
        # cov_match answers for products with coverage on some picked platform. A product
        # with none is "none" on every one of them, so it matches exactly when "none" is a
        # chosen state. A fixed literal, never request text.
        none_chosen = "none" in filters.coverage_state
        clauses.append("COALESCE(cm.ok, TRUE)" if none_chosen else "COALESCE(cm.ok, FALSE)")

    if filters.value_min is not None:
        clauses.append(f"v.value >= {placeholder(Decimal(str(filters.value_min)))}")
    if filters.value_max is not None:
        clauses.append(f"v.value <= {placeholder(Decimal(str(filters.value_max)))}")
    if filters.in_stock:
        clauses.append("v.qty > 0")
    if filters.unvalued:
        clauses.append("v.parent_sku IS NULL")

    if filters.listing_state == "never_listed":
        clauses.append(NEVER_LISTED_SQL)
    elif filters.listing_state == "in_open_batch":
        clauses.append("ow.batch_id IS NOT NULL")
    elif filters.listing_state == "not_in_open_batch":
        clauses.append("ow.batch_id IS NULL")

    if filters.listing_status == "images_pending":
        clauses.append(IMAGES_PENDING_SQL)
    elif filters.listing_status in ("platforms_pending", "listed"):
        if not platforms_param:
            raise ValueError("listing_status needs listing_state_join's platforms parameter")
        # No listing_state row means no exclusions and no listings: every enabled platform
        # is eligible and none is listed. listing_state only covers the products where that
        # is not true.
        eligible = f"COALESCE(ls.eligible, cardinality({platforms_param}))"
        listed = "COALESCE(ls.listed, 0)"
        if filters.listing_status == "platforms_pending":
            clauses.append(f"{listed} < {eligible}")
        else:
            # A product with nothing it may list on is neither listed nor pending.
            clauses.append(f"({eligible} > 0 AND {listed} = {eligible})")

    if filters.has_images is True:
        clauses.append("im.parent_sku IS NOT NULL")
    elif filters.has_images is False:
        clauses.append("im.parent_sku IS NULL")

    return "\n  AND ".join(clauses)


_platforms_cache: Tuple[float, List[str]] = (0.0, [])


async def enabled_platforms() -> List[str]:
    """app_settings.platforms, cached briefly. Every filtered request validates against it,
    and the list changes when an admin enables a platform, not per request."""
    global _platforms_cache
    now = time.monotonic()
    if _platforms_cache[1] and now - _platforms_cache[0] < PLATFORMS_CACHE_SECONDS:
        return _platforms_cache[1]
    rows = await connections.get("default").execute_query_dict(
        """
        SELECT p.platform_id
          FROM (SELECT platforms FROM app_settings ORDER BY id LIMIT 1) s
         CROSS JOIN LATERAL jsonb_array_elements_text(s.platforms) AS p(platform_id)
        """
    )
    platforms = [row["platform_id"] for row in rows]
    _platforms_cache = (now, platforms)
    return platforms


async def require_known_platforms(platforms: List[str]) -> None:
    if not platforms:
        return
    enabled = await enabled_platforms()
    if any(platform not in enabled for platform in platforms):
        raise HTTPException(status_code=400, detail="Unknown platform")


EXCLUSION_RULES_CACHE_SECONDS = 60


@dataclass
class ExclusionRules:
    """The exclusion rules as parallel lists.

    Read once and cached. They used to be bound into the query as arrays and joined against
    the mirror; now rule_index turns them into lookups that exclusions_for applies to the
    cached products, and only the resulting (sku, platform) pairs reach SQL. Derived in SQL
    from the option tables instead, every JSON array expansion was assumed to yield 100
    rows; the resulting cost estimate (3.7M) pushed the page query past the JIT threshold,
    and it spent 1.2 s compiling a query that runs in 75 ms.
    """

    kinds: List[str] = field(default_factory=list)  # "brand" or "product type"
    keys: List[str] = field(default_factory=list)  # a value or alias, as stored
    platforms: List[str] = field(default_factory=list)  # the platform that key excludes
    company_platforms: List[str] = field(default_factory=list)  # platforms with an allow-list
    allowed_platforms: List[str] = field(default_factory=list)  # (platform, company) pairs
    allowed_codes: List[str] = field(default_factory=list)  # each allow-list accepts
    version: int = 0  # bumped on every re-read, so derived indexes can be memoized


def build_exclusion_rules(
    brands: List[Dict[str, Any]],
    types: List[Dict[str, Any]],
    platform_settings: Dict[str, Any],
) -> ExclusionRules:
    """Pure: the rules from option records ({name, aliases, platforms}) and platform_settings.
    A record excludes by its value and every alias; a platform restricts companies only when
    it declares a company_codes list; SellerCloud is never excluded."""
    rules = ExclusionRules()
    seen = set()
    for kind, records in (("brand", brands), ("product type", types)):
        for record in records or []:
            excluded = record.get("platforms")
            if not isinstance(excluded, list):
                continue
            aliases = record.get("aliases")
            names = [record.get("name"), *(aliases if isinstance(aliases, list) else [])]
            for name in names:
                if not isinstance(name, str) or not name:
                    continue
                for platform in map(str, excluded):
                    marker = (kind, name.lower(), platform)
                    if platform == "sellercloud" or marker in seen:
                        continue
                    seen.add(marker)
                    rules.kinds.append(kind)
                    rules.keys.append(name)
                    rules.platforms.append(platform)
    for platform, settings in (platform_settings or {}).items():
        codes = settings.get("company_codes") if isinstance(settings, dict) else None
        if platform == "sellercloud" or not isinstance(codes, list):
            continue
        rules.company_platforms.append(platform)
        for code in codes:
            rules.allowed_platforms.append(platform)
            rules.allowed_codes.append(str(code))
    return rules


_exclusion_cache: Tuple[float, Optional[ExclusionRules]] = (0.0, None)
_exclusion_version = 0


def _as_json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


async def exclusion_rules() -> ExclusionRules:
    """The exclusion rules, cached briefly like enabled_platforms: they change when an admin
    edits a brand, a type or platform settings, not per request. One plain read with no JSON
    expansion, so it is cheap to plan."""
    global _exclusion_cache, _exclusion_version
    now = time.monotonic()
    cached_at, cached = _exclusion_cache
    if cached is not None and now - cached_at < EXCLUSION_RULES_CACHE_SECONDS:
        return cached
    row = (
        await connections.get("default").execute_query_dict(
            """
            SELECT
              (SELECT COALESCE(jsonb_agg(jsonb_build_object(
                          'name', b.brand, 'aliases', b.aliases, 'platforms', b.excluded_platforms)),
                      '[]'::jsonb)
                 FROM listingoptions_brands b
                WHERE jsonb_typeof(b.excluded_platforms) = 'array'
                  AND jsonb_array_length(b.excluded_platforms) > 0) AS brands,
              (SELECT COALESCE(jsonb_agg(jsonb_build_object(
                          'name', t.type, 'aliases', t.aliases, 'platforms', t.excluded_platforms)),
                      '[]'::jsonb)
                 FROM listingoptions_types t
                WHERE jsonb_typeof(t.excluded_platforms) = 'array'
                  AND jsonb_array_length(t.excluded_platforms) > 0) AS types,
              (SELECT platform_settings FROM app_settings ORDER BY id LIMIT 1) AS settings
            """
        )
    )[0]
    rules = build_exclusion_rules(
        _as_json(row["brands"]) or [],
        _as_json(row["types"]) or [],
        _as_json(row["settings"]) or {},
    )
    _exclusion_version += 1
    rules.version = _exclusion_version
    _exclusion_cache = (now, rules)
    return rules


def with_clause(ctes: List[str]) -> str:
    return ("WITH " + ",\n".join(ctes) + "\n") if ctes else ""


async def _filter_parts(
    params: List[Any],
    filters: CatalogFilters,
    *,
    skus: Sequence[str],
    snap: Snapshot,
    index: RuleIndex,
    enabled: Optional[List[str]] = None,
) -> Tuple[List[str], str, Optional[str]]:
    """(CTEs, JOIN fragments) for one request, binding in the order the SQL expects: the
    driving SKU array, then the exclusion pairs, then the coverage filter, then the quick
    filter.

    The exclusion pairs are bound only when a filter reads them. A page's own tiles no
    longer need them in SQL: 50 rows are resolved from the cache in Python.
    """
    ctes: List[str] = [catalog_cte(params, skus)]
    joins: List[str] = []
    needs_state = listing_state_on(filters)
    needs_coverage = coverage_filter_on(filters)
    if needs_coverage or needs_state:
        platforms: Set[str] = set()
        if needs_coverage:
            platforms.update(filters.coverage_platform)
        if needs_state:
            platforms.update(enabled if enabled is not None else await enabled_platforms())
        ctes.append(exclusion_pairs_cte(params, exclusion_pairs(snap, index, platforms)))
    cov_ctes, cov_join = coverage_join(params, filters)
    if cov_ctes:
        ctes.append(cov_ctes)
        joins.append(cov_join)
    state_param: Optional[str] = None
    if needs_state:
        state_ctes, state_join, state_param = listing_state_join(
            params, enabled if enabled is not None else await enabled_platforms()
        )
        ctes.append(state_ctes)
        joins.append(state_join)
    return ctes, "\n".join(joins), state_param


def _shape_row(
    row: Dict[str, Any],
    product: Optional[Product],
    coverage: Dict[str, str],
    exclusions: Dict[str, List[str]],
) -> Dict[str, Any]:
    value = None
    if row.get("value") is not None:
        value = {
            "value": row["value"],
            "qty": row["qty"],
            "children": row["children"],
            "priced": row["priced"],
            "exported": row["exported"],
            "as_of": row["as_of"],
        }
    return {
        "sku": row["sku"],
        "title": product.title if product else "",
        "mpn": product.mpn if product else None,
        "brand": product.brand if product else None,
        "product_type": product.product_type if product else None,
        "company_code": product.company_code if product else None,
        "value": value,
        "coverage": coverage,
        "exclusions": exclusions,
        "open_batch_id": row["open_batch_id"],
        "open_kind": row["open_kind"],
        "has_listing": bool(row["has_listing"]),
        "has_images": bool(row["has_images"]),
        "images_taken_at": row["images_taken_at"],
        "blocked_reason": row["blocked_reason"],
    }


async def get_page(filters: CatalogFilters, page: int, page_size: int) -> List[Dict[str, Any]]:
    """One page of the catalog, as a bare list; the page's total comes from get_summary.

    One round trip. The page is selected first and each of its rows then gets its coverage
    across every enabled platform aggregated onto it, so per-row coverage is computed for at
    most page_size products, never for the whole filtered set. The product's own fields and
    its exclusions are merged in from the cache afterwards, which never touches the database.
    """
    snap = require_snapshot()
    exact, like = await resolve_search(filters.search)
    enabled = await enabled_platforms()
    index = rule_index(await exclusion_rules())
    skus = match_skus(snap, exact, like, filters.sort, set(filters.company))
    if not skus:
        return []

    params: List[Any] = []
    ctes, joins, state_param = await _filter_parts(
        params, filters, skus=skus, snap=snap, index=index, enabled=enabled
    )
    where = build_catalog_where(params, filters, state_param)
    order = CATALOG_SORTS[filters.sort]
    outer_order = OUTER_SORTS[filters.sort]
    params.append(page_size)
    limit = f"${len(params)}"
    params.append((page - 1) * page_size)
    offset = f"${len(params)}"
    # Bound rather than read from app_settings, so the planner knows how many there are.
    params.append(list(enabled))
    platforms_param = f"${len(params)}::text[]"
    with_prefix = "WITH " + ",\n".join(ctes) + ",\n"

    sql = f"""
{with_prefix}page AS (
    SELECT c.sku, c.ord,
           v.value, v.qty, v.children, v.priced, v.exported, v.as_of,
           (im.parent_sku IS NOT NULL) AS has_images,
           im.taken_at AS images_taken_at,
           ow.batch_id AS open_batch_id,
           ow.kind AS open_kind,
           EXISTS (SELECT 1 FROM listings lh WHERE lh.product_id = c.sku) AS has_listing,
           {BLOCKED_REASON_SQL} AS blocked_reason
    {CATALOG_FROM}
    {joins}
    WHERE {where}
    ORDER BY {order}
    LIMIT {limit} OFFSET {offset}
)
SELECT page.*, cx.coverage
  FROM page
  LEFT JOIN LATERAL (
      SELECT COALESCE(jsonb_object_agg(
                 p.platform_id, {coverage_state_sql('p.platform_id', 'page.sku')}
             ), '{{}}'::jsonb) AS coverage
        FROM unnest({platforms_param}) AS p(platform_id)
  ) cx ON true
 ORDER BY {outer_order}
"""
    rows = await connections.get("default").execute_query_dict(sql, params)
    wanted = set(enabled)
    shaped = []
    for row in rows:
        coverage = row["coverage"]
        if isinstance(coverage, str):
            coverage = json.loads(coverage)
        coverage = coverage or {}
        product = snap.by_sku.get(row["sku"])
        exclusions = exclusions_for(product, index, wanted) if product else {}
        # An exclusion wins over any listing state, as in COVERAGE_CTE and on the listing
        # view. Applied here because the reasons come from the cache, not from SQL.
        for platform in exclusions:
            coverage[platform] = "excluded"
        shaped.append(_shape_row(row, product, coverage, exclusions))
    return shaped


_summary_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}


async def get_summary(filters: CatalogFilters) -> Dict[str, Any]:
    """Counts behind the header strip, the pagination and select-all, cached briefly per
    filter set: the catalog changes on a product cache reload and a nightly value run, not
    per second. The cache's version is part of the key, so a reload cannot serve counts for
    a catalog that has since changed."""
    snap = require_snapshot()
    exact, like = await resolve_search(filters.search)
    index = rule_index(await exclusion_rules())
    skus = match_skus(snap, exact, like, filters.sort, set(filters.company))

    key = f"{snap.version}:{index.version}:{filters.model_dump_json()}"
    now = time.monotonic()
    cached = _summary_cache.get(key)
    if cached and now - cached[0] < SUMMARY_CACHE_SECONDS:
        return cached[1]

    from services.batch_service import BACKGROUND

    if not skus:
        summary = {
            "count": 0,
            "eligible_count": 0,
            "skipped": {"in_open_batch": 0, "no_images": 0, "not_in_catalog": 0},
            "total_value": Decimal(0),
            "negative_value": 0,
            "unvalued": 0,
            "values_as_of": None,
            "catalog_synced_at": snap.loaded_at,
            "platforms": await enabled_platforms(),
            "companies": catalog_companies(snap),
            "can_create": BACKGROUND,
        }
        _summary_cache[key] = (now, summary)
        return summary

    params: List[Any] = []
    ctes, joins, state_param = await _filter_parts(
        params, filters, skus=skus, snap=snap, index=index
    )
    where = build_catalog_where(params, filters, state_param)
    with_prefix = with_clause(ctes)
    sql = f"""
{with_prefix}SELECT count(*)::int AS count,
       count(*) FILTER (WHERE {ELIGIBLE_SQL})::int AS eligible_count,
       count(*) FILTER (WHERE ow.batch_id IS NOT NULL)::int AS in_open_batch,
       count(*) FILTER (WHERE ow.batch_id IS NULL AND im.parent_sku IS NULL)::int AS no_images,
       -- A negative value is negative stock in SellerCloud, not a debt. Netted in, a few
       -- thousand oversold parents cancel out the whole catalog, so they count as 0 here and
       -- are reported on their own.
       COALESCE(sum(GREATEST(v.value, 0)), 0) AS total_value,
       count(*) FILTER (WHERE v.value < 0)::int AS negative_value,
       count(*) FILTER (WHERE v.parent_sku IS NULL)::int AS unvalued,
       (SELECT max(as_of) FROM parent_product_values) AS values_as_of
{CATALOG_FROM}
{joins}
WHERE {where}
"""
    # One round trip: the freshness stamp rides along with the counts, and the platform list
    # comes from enabled_platforms' short cache.
    row = (await connections.get("default").execute_query_dict(sql, params))[0]

    summary = {
        "count": row["count"],
        "eligible_count": row["eligible_count"],
        "skipped": {
            "in_open_batch": row["in_open_batch"],
            "no_images": row["no_images"],
            "not_in_catalog": 0,
        },
        "total_value": row["total_value"],
        "negative_value": row["negative_value"],
        "unvalued": row["unvalued"],
        "values_as_of": row["values_as_of"],
        # What the product side is as of: the cache's last load, not a mirror's last sync.
        "catalog_synced_at": snap.loaded_at,
        "platforms": await enabled_platforms(),
        "companies": catalog_companies(snap),
        "can_create": BACKGROUND,
    }
    _summary_cache[key] = (now, summary)
    if len(_summary_cache) > 500:
        _summary_cache.clear()
    return summary


class _NothingEligible(Exception):
    pass


async def _resolve_selection(
    conn,
    selection,
    search_exact: Optional[str],
    search_like: Optional[str],
    rules: ExclusionRules,
    enabled: List[str],
    snap: Snapshot,
) -> Tuple[List[str], Dict[str, int]]:
    """(eligible parent SKUs, most valuable first; skipped counts by reason)."""
    params: List[Any] = []
    index = rule_index(rules)
    skipped = {"in_open_batch": 0, "no_images": 0, "not_in_catalog": 0}
    if selection.mode == "ids":
        ids = list(dict.fromkeys(selection.product_ids))
        skus = [sku for sku in ids if sku in snap.by_sku]
        # An id nobody knows is not in the catalog: the cache says so without a query.
        skipped["not_in_catalog"] = len(ids) - len(skus)
        filters = CatalogFilters()
    else:
        filters = selection.filters
        skus = match_skus(snap, search_exact, search_like, "value_desc", set(filters.company))
    if not skus:
        return [], skipped

    ctes, joins, state_param = await _filter_parts(
        params, filters, skus=skus, snap=snap, index=index, enabled=enabled
    )
    where = build_catalog_where(params, filters, state_param)
    with_prefix = with_clause(ctes)

    rows = await conn.execute_query_dict(
        f"""
{with_prefix}SELECT c.sku, {BLOCKED_REASON_SQL} AS blocked_reason
{CATALOG_FROM}
{joins}
WHERE {where}
ORDER BY v.value DESC NULLS LAST, c.sku
""",
        params,
    )
    eligible: List[str] = []
    for row in rows:
        reason = row["blocked_reason"]
        if reason:
            skipped[reason] += 1
        else:
            eligible.append(row["sku"])
    return eligible, skipped


async def create_batch(
    selection,
    expected_count: Optional[int],
    comment: Optional[str],
    assigned_to: Optional[str],
    priority: str,
    created_by: str,
) -> Dict[str, Any]:
    """Create a batch from a catalog selection.

    Eligibility is decided again here, inside the transaction that inserts the jobs and
    under a lock every catalog create takes, so two operators (or a double click) can never
    put the same parent into two new batches: the second request sees those parents as
    already in an open batch. The selection is resolved now, not from what the operator
    previewed; `newly_skipped` reports the difference.
    """
    from services.batch_service import BACKGROUND, BatchService, NewBatchSpec

    if not BACKGROUND:
        raise HTTPException(status_code=503, detail="Background generation is off")

    snap = require_snapshot()
    search_exact = search_like = None
    if selection.mode == "filter":
        # Resolved before the transaction: it reads the products DB and must not hold the lock.
        search_exact, search_like = await resolve_search(selection.filters.search)
    # Read before the transaction too: they may query, and must not while holding the lock.
    rules = await exclusion_rules()
    enabled = await enabled_platforms()

    try:
        async with in_transaction("default") as conn:
            await conn.execute_query("SET LOCAL lock_timeout = '5s'")
            await conn.execute_query("SET LOCAL statement_timeout = '30s'")
            await conn.execute_query("SELECT pg_advisory_xact_lock(hashtext('catalog_batch_create'))")
            eligible, skipped = await _resolve_selection(
                conn, selection, search_exact, search_like, rules, enabled, snap
            )
            if not eligible:
                raise _NothingEligible()
            batch = await BatchService.insert_batch_with_jobs(
                conn,
                NewBatchSpec(
                    comment=comment or "",
                    assigned_to=assigned_to,
                    priority=priority,
                    created_by=created_by,
                ),
                {sku: sku for sku in eligible},
            )
    except _NothingEligible:
        raise HTTPException(
            status_code=409,
            detail="All selected products are already in an open batch or have no images",
        )
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        text = str(exc)
        if "lock timeout" in text or "statement timeout" in text or "canceling statement" in text:
            raise HTTPException(status_code=409, detail="Another batch is being created, try again")
        logger.exception("Catalog batch creation failed")
        raise HTTPException(status_code=500, detail="Could not create the batch")

    await BatchService.after_create(batch, eligible)
    logger.info(
        f"Catalog batch {batch.id} created by {created_by}: {len(eligible)} product(s), "
        f"skipped {skipped}, selection "
        + (
            f"{len(selection.product_ids)} ids"
            if selection.mode == "ids"
            else json.dumps(selection.filters.model_dump(), default=str)
        )
    )

    batch = await Batch.get(id=batch.id)
    newly_skipped = max(expected_count - len(eligible), 0) if expected_count is not None else 0
    return {
        "batch": await BatchService._to_response(batch, include_listings=False),
        "eligible": len(eligible),
        "skipped": skipped,
        "newly_skipped": newly_skipped,
    }
