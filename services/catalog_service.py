"""The catalog browser: every active parent product with its value, platform coverage and
listing state, and batch creation from a selection of them.

Reads catalog_products (the skubase mirror of parent_products) and catalog_product_images
(the skubase mirror of which parents photography has images for), both kept by
CatalogSyncPoller, plus parent_product_values, listings, batches, batch_generation_jobs,
external_listing_ids and listing_submissions: all skubase, so one query can filter, sort and
page across them.

Raw SQL over the default connection, like product_queue_service. Every WHERE fragment is
built by build_catalog_where and coverage_join, which bind every request value as a
parameter; sort orders and states are looked up in fixed dictionaries, never interpolated.
"""

import json
import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException
from tortoise import connections
from tortoise.transactions import in_transaction

from models.api_models import CatalogFilters
from models.db_models import Batch
from services.product_queue_service import resolve_search

logger = logging.getLogger(__name__)

CATALOG_SORTS = {
    "value_desc": "v.value DESC NULLS LAST",
    "newest": "c.product_created_at DESC NULLS LAST",
    "sku": "c.sku",
}

# Below this, a contains-search has no trigrams to use and scans the whole index, so short
# terms match SKU prefixes only.
SEARCH_MIN_CONTAINS = 3
SUMMARY_CACHE_SECONDS = 60
PLATFORMS_CACHE_SECONDS = 60

# What "is this parent on this platform" means, in two shapes that must agree:
#
# - Excluded: its brand, product type or company excludes the platform (exclusion_rules).
#   Wins over everything below, as it does on the listing view, and never counts as None.
# - Listed: a row in external_listing_ids (which never expire, so "has been listed"), or a
#   latest submission that succeeded before ids were captured.
# - In progress / failed: the latest submission for the platform across the parent's
#   listings.
# - None: neither.
#
# coverage_state_sql is the per-row form, used for the pills on one page of rows.
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
# excluded. The rules are read by exclusion_rules and bound into queries by exclusion_ctes,
# which define excl_keys, company_rules and company_allowed for the SQL below.

COVERAGE_CTE = """excl AS (
    -- The picked platforms each product is excluded from.
    SELECT c.sku, k.platform_id
      FROM catalog_products c
      JOIN excl_keys k ON k.kind = 'brand' AND k.key = lower(c.brand)
     WHERE k.platform_id = ANY({platforms})
    UNION
    SELECT c.sku, k.platform_id
      FROM catalog_products c
      JOIN excl_keys k ON k.kind = 'product type' AND k.key = lower(c.product_type)
     WHERE k.platform_id = ANY({platforms})
    UNION
    SELECT c.sku, r.platform_id
      FROM catalog_products c
      JOIN company_rules r ON r.platform_id = ANY({platforms})
     WHERE c.company_code IS NOT NULL
       AND NOT EXISTS (
           SELECT 1 FROM company_allowed a
            WHERE a.platform_id = r.platform_id AND a.code = c.company_code::text
       )
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
FROM catalog_products c
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


def escape_like(term: str) -> str:
    return term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


# The quick filters. "Listed" and "Platforms pending" are about EVERY enabled platform the
# product is not excluded from ("eligible"), so they need the exclusion rules and a listed
# set over all of them, not just the ones a coverage filter picked. "Images pending" is the
# state where a listing counts as submitted everywhere but photography has not delivered:
# submissions for platforms with requires_images sit queued until upload_status flips.
LISTING_STATE_CTE = """excl_all AS (
    -- Every enabled platform each product is excluded from.
    SELECT c.sku, k.platform_id
      FROM catalog_products c
      JOIN excl_keys k ON k.kind = 'brand' AND k.key = lower(c.brand)
     WHERE k.platform_id = ANY({platforms})
    UNION
    SELECT c.sku, k.platform_id
      FROM catalog_products c
      JOIN excl_keys k ON k.kind = 'product type' AND k.key = lower(c.product_type)
     WHERE k.platform_id = ANY({platforms})
    UNION
    SELECT c.sku, r.platform_id
      FROM catalog_products c
      JOIN company_rules r ON r.platform_id = ANY({platforms})
     WHERE c.company_code IS NOT NULL
       AND NOT EXISTS (
           SELECT 1 FROM company_allowed a
            WHERE a.platform_id = r.platform_id AND a.code = c.company_code::text
       )
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
    -- Per product: how many enabled platforms it may list on, and how many of those it is
    -- listed on. A platform it is excluded from counts as neither.
    SELECT c.sku,
           cardinality({platforms}) - COALESCE(x.n, 0) AS eligible,
           COALESCE(l.n, 0) AS listed
      FROM catalog_products c
      LEFT JOIN (SELECT sku, count(*) AS n FROM excl_all GROUP BY sku) x ON x.sku = c.sku
      LEFT JOIN (
          SELECT la.sku, count(*) AS n
            FROM listed_all la
           WHERE NOT EXISTS (
               SELECT 1 FROM excl_all e WHERE e.sku = la.sku AND e.platform_id = la.platform_id
           )
           GROUP BY la.sku
      ) l ON l.sku = c.sku
)"""

# Submitted everywhere it had to be, and still waiting on photography.
IMAGES_PENDING_SQL = """EXISTS (
    SELECT 1 FROM listings l
     WHERE l.product_id = c.sku AND l.submitted AND l.upload_status = 'pending'
)"""


def coverage_filter_on(filters: CatalogFilters) -> bool:
    """A coverage filter needs both a platform and a state; either alone filters nothing."""
    return bool(filters.coverage_platform and filters.coverage_state)


def listing_state_on(filters: CatalogFilters) -> bool:
    """Whether the quick filter needs the eligible-and-listed counts (CTEs and a join)."""
    return filters.listing_status in ("platforms_pending", "listed")


def listing_state_join(params: List[Any], enabled: List[str]) -> Tuple[str, str]:
    """(CTEs, JOIN) for the quick filters, binding the enabled platforms. Refers to
    exclusion_ctes', which must come first in the WITH."""
    params.append(list(enabled))
    return (
        LISTING_STATE_CTE.format(platforms=f"${len(params)}::text[]"),
        "LEFT JOIN listing_state ls ON ls.sku = c.sku",
    )


def coverage_join(params: List[Any], filters: CatalogFilters) -> Tuple[str, str]:
    """(CTEs, JOIN) for a coverage filter, or ("", "") without one. Binds the platforms, then
    the states.

    Several platforms combine with AND: a product matches only when every picked platform is
    in one of the chosen states. The CTEs refer to exclusion_ctes', which must come first in
    the WITH. Call before build_catalog_where: the CTEs' placeholders must be numbered first.
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
    params: List[Any],
    filters: CatalogFilters,
    search_exact: Optional[str] = None,
    search_like: Optional[str] = None,
) -> str:
    """WHERE body for the catalog, appending its parameters to `params` in order.

    Pure, so the placeholder numbering is testable without a database: a mismatched $n does
    not raise, it binds the wrong value. The caller resolves the search term first (see
    product_queue_service.resolve_search) and passes exactly one of search_exact /
    search_like, and joins coverage_join's CTE when filtering on coverage.
    """
    clauses: List[str] = ["TRUE"]

    def placeholder(value: Any) -> str:
        params.append(value)
        return f"${len(params)}"

    if search_exact:
        clauses.append(f"c.sku = {placeholder(search_exact)}")
    elif search_like:
        if len(search_like) >= SEARCH_MIN_CONTAINS:
            term = placeholder(f"%{escape_like(search_like)}%")
            clauses.append(
                f"(c.sku ILIKE {term} ESCAPE '\\' OR c.mpn ILIKE {term} ESCAPE '\\' "
                f"OR c.title ILIKE {term} ESCAPE '\\')"
            )
        else:
            clauses.append(f"c.sku ILIKE {placeholder(escape_like(search_like) + '%')} ESCAPE '\\'")

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
    elif filters.listing_status == "platforms_pending":
        clauses.append("ls.listed < ls.eligible")
    elif filters.listing_status == "listed":
        # A product with nothing it may list on is neither listed nor pending.
        clauses.append("(ls.eligible > 0 AND ls.listed = ls.eligible)")

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
    """The exclusion rules as parallel lists, ready to bind as arrays.

    Read once, cached, and bound into each query through unnest(), whose row count the
    planner can see. Derived in SQL from the option tables instead, every JSON array
    expansion was assumed to yield 100 rows; the resulting cost estimate (3.7M) pushed the
    page query past the JIT threshold, and it spent 1.2 s compiling a query that runs in
    75 ms.
    """

    kinds: List[str] = field(default_factory=list)  # "brand" or "product type"
    keys: List[str] = field(default_factory=list)  # a value or alias, as stored
    platforms: List[str] = field(default_factory=list)  # the platform that key excludes
    company_platforms: List[str] = field(default_factory=list)  # platforms with an allow-list
    allowed_platforms: List[str] = field(default_factory=list)  # (platform, company) pairs
    allowed_codes: List[str] = field(default_factory=list)  # each allow-list accepts


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


def _as_json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


async def exclusion_rules() -> ExclusionRules:
    """The exclusion rules, cached briefly like enabled_platforms: they change when an admin
    edits a brand, a type or platform settings, not per request. One plain read with no JSON
    expansion, so it is cheap to plan."""
    global _exclusion_cache
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
    _exclusion_cache = (now, rules)
    return rules


def exclusion_ctes(params: List[Any], rules: ExclusionRules) -> str:
    """The rules as MATERIALIZED CTEs over bound arrays: excl_keys (kind, key, platform_id),
    company_rules (platform_id) and company_allowed (platform_id, code). Binds six
    parameters, so call it before anything that refers to the CTEs."""

    def bind(values: List[str]) -> str:
        params.append(list(values))
        return f"${len(params)}::text[]"

    kinds, keys, platforms = bind(rules.kinds), bind(rules.keys), bind(rules.platforms)
    company = bind(rules.company_platforms)
    allowed_platforms, allowed_codes = bind(rules.allowed_platforms), bind(rules.allowed_codes)
    return f"""excl_keys AS MATERIALIZED (
    SELECT r.kind, lower(r.key) AS key, r.platform_id
      FROM unnest({kinds}, {keys}, {platforms}) AS r(kind, key, platform_id)
),
company_rules AS MATERIALIZED (
    SELECT r.platform_id FROM unnest({company}) AS r(platform_id)
),
company_allowed AS MATERIALIZED (
    SELECT a.platform_id, a.code
      FROM unnest({allowed_platforms}, {allowed_codes}) AS a(platform_id, code)
)"""


def with_clause(ctes: List[str]) -> str:
    return ("WITH " + ",\n".join(ctes) + "\n") if ctes else ""


async def _filter_parts(
    params: List[Any],
    filters: CatalogFilters,
    *,
    always_rules: bool,
    rules: Optional[ExclusionRules] = None,
    enabled: Optional[List[str]] = None,
) -> Tuple[List[str], str]:
    """(CTEs, JOIN fragments) for one request, binding in the order the SQL expects: the
    exclusion rules, then the coverage filter, then the quick filter."""
    ctes: List[str] = []
    joins: List[str] = []
    needs_state = listing_state_on(filters)
    if always_rules or coverage_filter_on(filters) or needs_state:
        ctes.append(exclusion_ctes(params, rules if rules is not None else await exclusion_rules()))
    cov_ctes, cov_join = coverage_join(params, filters)
    if cov_ctes:
        ctes.append(cov_ctes)
        joins.append(cov_join)
    if needs_state:
        state_ctes, state_join = listing_state_join(
            params, enabled if enabled is not None else await enabled_platforms()
        )
        ctes.append(state_ctes)
        joins.append(state_join)
    return ctes, "\n".join(joins)


def _shape_row(
    row: Dict[str, Any], coverage: Dict[str, str], exclusions: Dict[str, List[str]]
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
        "title": row["title"],
        "mpn": row["mpn"],
        "brand": row["brand"],
        "product_type": row["product_type"],
        "company_code": row["company_code"],
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
    most page_size products, never for the whole filtered set.
    """
    params: List[Any] = []
    exact, like = await resolve_search(filters.search)
    enabled = await enabled_platforms()
    # The exclusion rules feed the page's own tiles whether or not a filter needs them.
    ctes, joins = await _filter_parts(params, filters, always_rules=True, enabled=enabled)
    where = build_catalog_where(params, filters, exact, like)
    order = CATALOG_SORTS[filters.sort]
    outer_order = {
        "value_desc": "page.value DESC NULLS LAST",
        "newest": "page.product_created_at DESC NULLS LAST",
        "sku": "page.sku",
    }[filters.sort]
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
    SELECT c.sku, c.title, c.mpn, c.brand, c.product_type, c.company_code, c.product_created_at,
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
    ORDER BY {order}, c.sku
    LIMIT {limit} OFFSET {offset}
)
SELECT page.*, cx.coverage, cx.exclusions
  FROM page
  LEFT JOIN LATERAL (
      SELECT COALESCE(jsonb_object_agg(
                 p.platform_id,
                 CASE WHEN x.reasons IS NOT NULL THEN 'excluded'
                      ELSE {coverage_state_sql('p.platform_id', 'page.sku')} END
             ), '{{}}'::jsonb) AS coverage,
             COALESCE(jsonb_object_agg(p.platform_id, to_jsonb(x.reasons))
                          FILTER (WHERE x.reasons IS NOT NULL), '{{}}'::jsonb) AS exclusions
        FROM unnest({platforms_param}) AS p(platform_id)
        LEFT JOIN LATERAL (
            SELECT array_agg(DISTINCT r.reason ORDER BY r.reason) AS reasons
              FROM (
                  SELECT k.kind AS reason
                    FROM excl_keys k
                   WHERE k.platform_id = p.platform_id
                     AND ((k.kind = 'brand' AND k.key = lower(page.brand))
                       OR (k.kind = 'product type' AND k.key = lower(page.product_type)))
                  UNION ALL
                  SELECT 'company'
                    FROM company_rules cr
                   WHERE cr.platform_id = p.platform_id
                     AND page.company_code IS NOT NULL
                     AND NOT EXISTS (
                         SELECT 1 FROM company_allowed a
                          WHERE a.platform_id = cr.platform_id
                            AND a.code = page.company_code::text)
              ) r
        ) x ON true
  ) cx ON true
 ORDER BY {outer_order}, page.sku
"""
    rows = await connections.get("default").execute_query_dict(sql, params)
    shaped = []
    for row in rows:
        coverage, exclusions = row["coverage"], row["exclusions"]
        if isinstance(coverage, str):
            coverage = json.loads(coverage)
        if isinstance(exclusions, str):
            exclusions = json.loads(exclusions)
        shaped.append(_shape_row(row, coverage or {}, exclusions or {}))
    return shaped


_summary_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}


async def get_summary(filters: CatalogFilters) -> Dict[str, Any]:
    """Counts behind the header strip, the pagination and select-all, cached briefly per
    filter set: the catalog changes on a 15-minute mirror and a nightly value run, not per
    second."""
    key = filters.model_dump_json()
    now = time.monotonic()
    cached = _summary_cache.get(key)
    if cached and now - cached[0] < SUMMARY_CACHE_SECONDS:
        return cached[1]

    params: List[Any] = []
    exact, like = await resolve_search(filters.search)
    ctes, joins = await _filter_parts(params, filters, always_rules=False)
    where = build_catalog_where(params, filters, exact, like)
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

    from services.batch_service import BACKGROUND
    from services.catalog_sync_poller import catalog_sync_poller

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
        "catalog_synced_at": catalog_sync_poller.status()["catalog_synced_at"],
        "platforms": await enabled_platforms(),
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
) -> Tuple[List[str], Dict[str, int]]:
    """(eligible parent SKUs, most valuable first; skipped counts by reason)."""
    params: List[Any] = []
    requested: Optional[int] = None
    ctes: List[str] = []
    joins = ""
    if selection.mode == "ids":
        ids = list(dict.fromkeys(selection.product_ids))
        requested = len(ids)
        params.append(ids)
        where = "c.sku = ANY($1::text[])"
    else:
        ctes, joins = await _filter_parts(
            params, selection.filters, always_rules=False, rules=rules, enabled=enabled
        )
        where = build_catalog_where(params, selection.filters, search_exact, search_like)
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
    skipped = {"in_open_batch": 0, "no_images": 0, "not_in_catalog": 0}
    eligible: List[str] = []
    for row in rows:
        reason = row["blocked_reason"]
        if reason:
            skipped[reason] += 1
        else:
            eligible.append(row["sku"])
    if requested is not None:
        skipped["not_in_catalog"] = max(requested - len(rows), 0)
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
                conn, selection, search_exact, search_like, rules, enabled
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
