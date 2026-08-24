"""Business rules for the consignment pipeline.

NO I/O. The caller hands us plain values, we return deterministic verdicts. Importable
by both the dry-run script (scripts/internal_platform_dryrun.py) and the pollers, and
unit-testable without touching a live Shopify store.

This contract is copied from services/daily_sellercloud_sync_service.py, which is why
that pipeline has a real dry-run. Do not add httpx, tortoise, or `config` imports here.

Money is Decimal throughout. Shopify returns Money as strings ("42.00", "42.0", "42"
are all the same price), so float comparison produces false drift on effectively every
product and would silently turn skip-unchanged into a 100% write rate.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import ROUND_CEILING, Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Final, Mapping, Sequence

from models.db_models import InternalPlatformSkipReason
from services.internal_platform_taxonomy import (
    OWNED_TAG_PREFIXES,
    is_sts_native_sku,
    normalize_vendor,
)

if TYPE_CHECKING:  # import only for typing: this module must stay I/O-free
    from services.internal_platform_type_map import TypeTaxonomy

ONE = Decimal("1")
HUNDRED = Decimal("100")


@dataclass(frozen=True, slots=True)
class PricingRule:
    """Numeric business rules.

    Lives here rather than on the internal_platforms row so there is exactly one
    source of truth. When internal_platforms.config is populated these values move
    together, not piecemeal - a half-populated config where editing price_floor in
    the DB has no effect is worse than no config at all.
    """

    markup_pct: Decimal = Decimal("10")
    price_floor: Decimal = Decimal("21")
    max_discount_pct: Decimal = Decimal("80")
    min_discount_pct: Decimal = Decimal("15")


DEFAULT_PRICING: Final[PricingRule] = PricingRule()


class PriceOutcome(StrEnum):
    PRICED = "priced"
    BELOW_FLOOR = "below_floor"
    NO_COMPARE_AT = "no_compare_at"
    DISCOUNT_TOO_HIGH = "discount_too_high"
    NO_PRICED_VARIANTS = "no_priced_variants"


@dataclass(frozen=True, slots=True)
class PricingResult:
    outcome: PriceOutcome
    price: Decimal | None
    compare_at: Decimal | None
    reason: str

    @property
    def ok(self) -> bool:
        return self.outcome is PriceOutcome.PRICED

    @property
    def skip_reason(self) -> InternalPlatformSkipReason | None:
        return {
            PriceOutcome.BELOW_FLOOR: InternalPlatformSkipReason.BELOW_PRICE_FLOOR,
            PriceOutcome.NO_COMPARE_AT: InternalPlatformSkipReason.NO_COMPARE_AT,
            PriceOutcome.DISCOUNT_TOO_HIGH: InternalPlatformSkipReason.DISCOUNT_TOO_HIGH,
            PriceOutcome.NO_PRICED_VARIANTS: InternalPlatformSkipReason.NO_PRICED_VARIANTS,
        }.get(self.outcome)


def _ceil_dollars(value: Decimal) -> Decimal:
    return value.quantize(Decimal("1"), rounding=ROUND_CEILING)


def money(value: str | int | float | Decimal | None) -> Decimal | None:
    """Parse a Shopify Money string into a 2dp Decimal. None stays None."""
    if value is None or value == "":
        return None
    return Decimal(str(value)).quantize(Decimal("0.01"))


def pricing_basis(
    variant_prices: Sequence[Decimal | None],
    variant_compare_at: Sequence[Decimal | None],
    variant_inventory: Sequence[int] | None = None,
) -> tuple[Decimal | None, Decimal | None]:
    """(cheapest in-stock price, highest in-stock compare-at).

    The single definition of "which variants count". compute_price prices from this, and
    the scan stores the same two numbers on the row, so the Products tab cannot show a
    price the engine did not use - a row reading "$164 -> $220" would be arithmetic
    nobody can follow, since $220 is 10% above $200, not above $164.

    Stock is required to be strictly positive. Negative quantities exist on 1nventory
    (oversold), and a variant at -6 is no more buyable than one at 0.
    """
    if variant_inventory:
        pairs = [
            (p, c)
            for p, c, q in zip(variant_prices, variant_compare_at, variant_inventory)
            if (q or 0) > 0
        ]
    else:
        pairs = list(zip(variant_prices, variant_compare_at))

    prices = [p for p, _ in pairs if p is not None and p > 0]
    compares = [c for _, c in pairs if c is not None and c > 0]
    return (min(prices) if prices else None, max(compares) if compares else None)


def compute_price(
    variant_prices: Sequence[Decimal | None],
    variant_compare_at: Sequence[Decimal | None],
    rule: PricingRule = DEFAULT_PRICING,
    variant_inventory: Sequence[int] | None = None,
) -> PricingResult:
    """Uniform destination price for one product.

        price      = ceil(min(source prices) * 1.10)
        compare_at = max(source compare-at)

    Then the STS floor and the 80% discount cap. A product whose discount exceeds the cap
    at that price is REJECTED, never repriced to fit: the markup rule above is the policy,
    and moving the price to satisfy a filter would make the filter the policy instead.

    Note the multi-variant spread hazard: variants at $20 and $200 give price $22
    against compare_at $200, an 89% discount that permanently trips the cap. The
    numbers are surfaced in `reason` so it shows up in the first dry-run rather than
    as a mystery rejection weeks later.
    """
    # Price from what a customer can actually buy. The cheapest variant is often sold
    # out, and pricing off it produces a price and a discount that describe a size nobody
    # can order - measured 2026-07-29, that was 133 of 1,947 listed/ready products.
    base, in_stock_compare = pricing_basis(
        variant_prices, variant_compare_at, variant_inventory
    )
    if base is None:
        return PricingResult(
            PriceOutcome.NO_PRICED_VARIANTS, None, None,
            "no in-stock variant carries a price" if variant_inventory
            else "no variant carries a price",
        )
    price = _ceil_dollars(base * (ONE + rule.markup_pct / HUNDRED))

    if price < rule.price_floor:
        return PricingResult(
            PriceOutcome.BELOW_FLOOR,
            None,
            None,
            f"computed {price} below floor {rule.price_floor} (source min {base})",
        )

    if in_stock_compare is None:
        # Divide-by-zero guard. Measured rare (0 of 250 sampled) but Syncio-copied
        # products can carry a null compare-at, and the discount rule divides by it.
        return PricingResult(
            PriceOutcome.NO_COMPARE_AT,
            None,
            None,
            f"no compare-at present; cannot evaluate the {rule.max_discount_pct}% rule",
        )

    compare_at = in_stock_compare
    if compare_at <= price:
        # Nothing to discount. Legal, and the cap cannot be violated.
        return PricingResult(
            PriceOutcome.PRICED, price, compare_at,
            f"price {price} from min {base} +{rule.markup_pct}%; compare-at {compare_at}",
        )

    discount_pct = (compare_at - price) / compare_at * HUNDRED
    if discount_pct <= rule.max_discount_pct:
        return PricingResult(
            PriceOutcome.PRICED, price, compare_at,
            f"price {price} from min {base} +{rule.markup_pct}%; "
            f"compare-at {compare_at}; discount {discount_pct:.1f}%",
        )

    # Over the cap: reject. There is deliberately NO price bump here.
    #
    # An earlier revision raised the price until the discount fitted inside 80%. That
    # silently broke the one rule this function exists to implement - 69LE-1711 went from
    # $46 to $92, a 100% markup rather than the specified 10%, because $460 MSRP needed a
    # $92 price to show as 80% off. The markup rule is the product of the pricing policy;
    # the discount cap is a filter on it, not a licence to overwrite it.
    return PricingResult(
        PriceOutcome.DISCOUNT_TOO_HIGH, None, compare_at,
        f"discount {discount_pct:.1f}% at the {rule.markup_pct}% markup price {price} "
        f"exceeds the {rule.max_discount_pct}% cap against compare-at {compare_at}",
    )


# ---------------------------------------------------------------------------
# Ownership
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class OwnershipVerdict:
    ours: bool
    parent_sku: str | None
    reason: str


def assess_ownership(
    variant_skus: Sequence[str | None],
    tags: Sequence[str],
    trigger_tag: str,
    syncio_source_gid: str | None,
    registered: Mapping[str, str],
    reassigned: Mapping[str, str],
) -> OwnershipVerdict:
    """Is this destination product one of ours?

    Shopify-derived, deliberately not ledger-derived. A ledger-based definition has a
    permanent-orphan failure mode: crash between the Shopify write and the DB write
    and that product is never touched again, including never delisted. `registered` is
    the product CATALOG (child_products), not the ledger, so it does not reintroduce
    that failure mode - and it is the same resolution the source poller has always
    used. Callers load it once per cycle and pass it in; this module stays DB-free.

    Ownership is catalog membership. A SKU that resolves to a registered parent is ours;
    one that does not is not. That replaces the old PARENT/SIZE string test, which was
    never a definition of ownership - only a proxy for one. The proxy rejected 1,893 of
    167,801 registered child SKUs that legitimately carry no size suffix, and on the live
    STS set it stranded 11 delivered products that could never be linked, normalized or
    safely delisted. See the twin note in the source poller: "String-splitting is
    deliberately not a resolution path - it would invent unregistered parents."

    Reassigned SKUs are rejected outright. A merge repoints child_products.parent_sku at
    the new parent while the SKU STRING keeps its original shape, so the catalog answers
    "which parent" with a value the 1nventory product is not - and internal_platform_state
    is keyed on the old one. Resolving them would silently relink live destination
    products to a different garment. Skipping matches what the source poller already does.

    Any STS-native SKU anywhere in the product is an immediate rejection, not a majority
    vote. That check is now belt-and-braces rather than the primary guard: STS-native SKUs
    are not in child_products at all (verified in prod: zero SKUs match the `i175851`
    shape), so they would fail resolution regardless. It is kept because it names the
    reason precisely in the skip report.
    """
    if not any(t.upper() == trigger_tag.upper() for t in tags):
        return OwnershipVerdict(False, None, f"missing trigger tag {trigger_tag}")

    if any(is_sts_native_sku(s) for s in variant_skus):
        return OwnershipVerdict(False, None, "carries an STS-native SKU")

    merged = sorted({s for s in variant_skus if s and s in reassigned})
    if merged:
        return OwnershipVerdict(
            False, None, f"carries reassigned SKU(s): {merged}"
        )

    parents = {registered.get(s) for s in variant_skus if s}
    parents.discard(None)
    if not parents:
        return OwnershipVerdict(
            False, None, "no variant SKU resolves to a registered parent"
        )
    if len(parents) > 1:
        return OwnershipVerdict(
            False, None, f"variants resolve to multiple parents: {sorted(parents)}"
        )

    if not syncio_source_gid:
        return OwnershipVerdict(
            False, None, "no syncio.source_product_id metafield; not Syncio-paired"
        )

    return OwnershipVerdict(True, parents.pop(), "ok")


# ---------------------------------------------------------------------------
# Candidate filtering
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class SourceProduct:
    """A source-store product, already normalized out of the GraphQL shape."""

    gid: str
    parent_sku: str | None
    vendor: str | None
    product_type: str | None
    tags: tuple[str, ...]
    total_inventory: int
    variant_prices: tuple[Decimal | None, ...]
    variant_compare_at: tuple[Decimal | None, ...]
    # Parallel to variant_prices. Pricing uses the cheapest IN-STOCK variant, so it needs
    # to know which are buyable. Defaults to empty, which means "no stock data supplied"
    # and prices off every variant - the pre-2026-07-29 behaviour.
    variant_inventory: tuple[int, ...] = ()
    updated_at: str | None = None
    # Shopify product status on the SOURCE store: ACTIVE / DRAFT / ARCHIVED.
    # None means "not supplied", which is treated as ACTIVE so a caller that does not
    # populate it keeps the pre-2026-08-21 behaviour instead of silently rejecting
    # everything.
    status: str | None = None


@dataclass(frozen=True, slots=True)
class FilterVerdict:
    qualified: bool
    rejected_by: str | None
    reason: str | None
    pricing: PricingResult | None = None


@dataclass(frozen=True, slots=True)
class Allowlists:
    """Proven vendors / types derived from STS sales analysis. Human-supplied.

    Fails CLOSED: empty means nothing qualifies. The opposite default would make a
    missing file turn every listed product into a delist candidate.
    """

    vendors: frozenset[str] = frozenset()
    product_types: frozenset[str] = frozenset()
    strict: bool = False  # True = must match BOTH vendor and type

    @property
    def usable(self) -> bool:
        return bool(self.vendors or self.product_types)


def qualifies(
    p: SourceProduct,
    allow: Allowlists,
    rule: PricingRule = DEFAULT_PRICING,
    *,
    taxonomy: "TypeTaxonomy",
) -> FilterVerdict:
    """Does this product belong on STS *right now*?

    CRITICAL: this is a pure qualification test. It must NOT consider whether the
    product is already tagged. An earlier revision folded "not currently tagged" in
    here, which made every successfully listed product permanently unqualified and
    therefore a delete candidate - i.e. it would have deleted the entire catalog on
    the first run. Tagging state belongs in needs_tagging/needs_delisting only.
    """
    # A product that is not ACTIVE on 1nventory is not for sale there, so it has no
    # business being copied onto Shop The Sample. Checked FIRST because it is the most
    # decisive fact about the product; everything below describes a product that is at
    # least live at source.
    #
    # Treated as transient, like no_inventory: DRAFT is how the ops team parks a product
    # (bad photos, pricing review) and it gets flipped back. See is_delist_candidate.
    if p.status is not None and p.status != "ACTIVE":
        return FilterVerdict(False, "not_active", f"source status {p.status}")

    if p.total_inventory < 1:
        return FilterVerdict(False, "no_inventory", f"stock {p.total_inventory}")

    # --- Allowlist filters DISABLED for now (2026-07-24) ---------------------
    # Selection is currently min-price + discount-band only, per request. The
    # proven-vendor/type allowlist from STS sales analysis is not being applied.
    # Re-enable both blocks once allow_vendors / allow_product_types are populated
    # in config. NOTE with these off there is no fail-closed on an empty allowlist,
    # so far more source products qualify and the per-cycle tag cap becomes the main
    # limiter on blast radius.
    #
    # if not allow.usable:
    #     return FilterVerdict(False, "no_allowlist", "allowlist empty; failing closed")
    #
    # vendor = normalize_vendor(p.vendor)
    # vendor_ok = bool(vendor and vendor in allow.vendors)
    # type_ok = bool(p.product_type and p.product_type in allow.product_types)
    # matched = (vendor_ok and type_ok) if allow.strict else (vendor_ok or type_ok)
    # if not matched:
    #     mode = "both" if allow.strict else "either"
    #     return FilterVerdict(
    #         False, "not_allowlisted",
    #         f"vendor {vendor!r} / type {p.product_type!r} fail {mode}-match",
    #     )
    # ------------------------------------------------------------------------

    if taxonomy.category_for(p.product_type) is None:
        return FilterVerdict(
            False, "unmapped_product_type",
            f"{p.product_type!r} has no Shop The Sample type mapping in listing options",
        )
    if taxonomy.gender_for(p.product_type) is None:
        return FilterVerdict(
            False, "underivable_gender",
            f"{p.product_type!r} has no usable gender in listing options "
            "(missing type, no parent, or a gender outside LO_GENDER_TO_STS)",
        )

    pricing = compute_price(p.variant_prices, p.variant_compare_at, rule,
                            p.variant_inventory)
    if not pricing.ok:
        # Go through .skip_reason, NOT .outcome.value. The two vocabularies are not the
        # same: PriceOutcome.BELOW_FLOOR is "below_floor" while the declared skip reason
        # is "below_price_floor", and .skip_reason exists precisely to translate. Emitting
        # the raw outcome wrote 114 rows under a code that is not a member of
        # InternalPlatformSkipReason, so the Skipped Products filter could never match it.
        code = pricing.skip_reason.value if pricing.skip_reason else pricing.outcome.value
        return FilterVerdict(False, code, pricing.reason, pricing)

    # Meaningful markdown. Products at or near full price are not what STS shoppers
    # come for, and a thin discount is not worth the consignment slot.
    if pricing.compare_at and pricing.compare_at > 0 and pricing.price:
        discount = (pricing.compare_at - pricing.price) / pricing.compare_at * HUNDRED
        if discount < rule.min_discount_pct:
            return FilterVerdict(
                False, "discount_too_low",
                f"discount {discount:.1f}% below {rule.min_discount_pct}%", pricing,
            )

    return FilterVerdict(True, None, pricing.reason, pricing)


def is_tagged(p: SourceProduct, trigger_tag: str) -> bool:
    return any(t.upper() == trigger_tag.upper() for t in p.tags)


def needs_tagging(p: SourceProduct, trigger_tag: str, allow: Allowlists,
                  rule: PricingRule = DEFAULT_PRICING, *,
                  taxonomy: "TypeTaxonomy") -> bool:
    return (qualifies(p, allow, rule, taxonomy=taxonomy).qualified
            and not is_tagged(p, trigger_tag))


@dataclass(frozen=True, slots=True)
class ScheduledActions:
    """What a scheduled pass is permitted to execute."""
    tag: bool
    delist: bool
    # Untag a product Syncio has not delivered yet. Deliberately NOT folded into `delist`:
    # that flag permits a productDelete on Shop The Sample, this one cannot reach a
    # destination store at all. See auto_untag_awaiting_sync in the source poller.
    untag_awaiting: bool = False

    @property
    def any(self) -> bool:
        return self.tag or self.delist or self.untag_awaiting


def plan_scheduled_actions(*, auto_submit: bool, auto_delist: bool,
                           execute_deletes: bool, delists: bool,
                           auto_untag_awaiting_sync: bool = False) -> ScheduledActions:
    """Which halves of a scheduled pass may write. Pure, so the flags are testable.

    auto_submit and auto_delist are INDEPENDENT, and this function exists because they
    were not. The poller returned early on `not auto_submit` before reaching its delist
    branch, so the daily pass could never act on auto_delist whatever it held - and it
    logged "planning only" while doing it, which is what kept the dead flag hidden for as
    long as it was. Extracting the decision means the branch that ships is the branch the
    tests drive, rather than a re-implementation of it in a test file.

    `execute` is deliberately NOT an input. It gates every write from the pass, scheduled
    or manual, and folding it in here would let a caller satisfy this function and still
    need a second check - one switch, one place.

    untag_awaiting depends on neither `delists` nor execute_deletes. It is a source-side
    untag of a product that has no destination product to delete, so gating it on the
    delete switches would mean an operator could only get it by also arming the
    irreversible half - which is the opposite of what it is for.
    """
    return ScheduledActions(
        tag=auto_submit,
        delist=delists and auto_delist and execute_deletes,
        untag_awaiting=auto_untag_awaiting_sync,
    )


def is_delist_candidate(verdict: FilterVerdict, *,
                        delist_on_no_inventory: bool,
                        delist_on_not_active: bool = False) -> bool:
    """Has this tagged product stopped qualifying in a way that warrants delisting?

    Selling out is not such a way, by default. Stock comes back - the same parent is
    restocked, or a size run is topped up - and delisting on it means untagging the
    source, letting Syncio tear the product off Shop The Sample, and then rebuilding the
    whole listing days later when the item returns. Measured 2026-07-29: 583 of the 1,363
    products live on STS were at zero stock at that instant, so this decides the fate of
    43% of the live footprint.

    no_inventory is checked FIRST in qualifies(), so a zero-stock product never reveals
    whether its price would also have failed. That is fine and deliberate: when stock
    returns the next scan re-evaluates everything from scratch.

    Every other rejection - an unmapped type, an underivable gender, a price that left
    the band - describes the product itself rather than a transient state, and still
    delists.
    """
    if verdict.qualified:
        return False
    if verdict.rejected_by == "no_inventory" and not delist_on_no_inventory:
        return False
    # Same reasoning as no_inventory, and the stakes are higher. Delisting untags the
    # source AND deletes the destination product, for which Shopify has no undelete. A
    # DRAFT is usually a deliberate, temporary park - we drafted eight products on
    # 2026-08-21 purely to hide unedited photos - and deleting their STS listings would
    # turn a reversible action into an irreversible one. Off by default; flip
    # delist_on_not_active only if a source draft really should destroy the STS product.
    if verdict.rejected_by == "not_active" and not delist_on_not_active:
        return False
    return True


def needs_delisting(p: SourceProduct, trigger_tag: str, allow: Allowlists,
                    rule: PricingRule = DEFAULT_PRICING, *,
                    taxonomy: "TypeTaxonomy",
                    delist_on_no_inventory: bool = True,
                    delist_on_not_active: bool = False) -> bool:
    if not is_tagged(p, trigger_tag):
        return False
    return is_delist_candidate(
        qualifies(p, allow, rule, taxonomy=taxonomy),
        delist_on_no_inventory=delist_on_no_inventory,
        delist_on_not_active=delist_on_not_active,
    )


def is_awaiting_sync(listed_at: datetime | None, dest_product_gid: str | None) -> bool:
    """Tagged on the source, not yet delivered by Syncio.

    The one definition. It matches ledger.awaiting_sync()'s SQL exactly - `listed_at IS
    NOT NULL AND dest_product_gid IS NULL` - and deliberately does not look at
    current_status, for the reason recorded on mark_delisted(): status and delivery drift
    apart, and on 2026-08-03 reading one for the other reported 155 freshly deleted
    products as waiting on Syncio.
    """
    return listed_at is not None and dest_product_gid is None


def pre_delivery_untag_due(
    *,
    verdict: FilterVerdict,
    listed_at: datetime | None,
    dest_product_gid: str | None,
    ineligible_since: datetime | None,
    now: datetime,
    soak_minutes: int,
) -> bool:
    """Should this tagged-but-undelivered product come back off the source now?

    Syncio takes 1 to 3 days to copy a tagged product across. A product that sells out or
    drifts out of the price band inside that window is delivered anyway and lands on Shop
    The Sample already unqualified. Untagging before delivery is what prevents that.

    Deliberately NOT routed through is_delist_candidate(). That function exempts
    no_inventory, because delisting a product already LIVE on STS means untagging the
    source, letting Syncio tear the listing down, and rebuilding it days later when stock
    returns - 43% of the live footprint was at zero stock when that was measured. None of
    that applies here: there is no destination product yet, so there is nothing to tear
    down and nothing to rebuild. Selling out is grounds on this path and is not on that
    one, and the two must not share a predicate.

    Reversible in a way the delist path is not: the whole action is one tagsRemove. When
    the product qualifies again the next scan returns it to ready_for_listing.

    The soak is in MINUTES against a stored timestamp, not in cycles against a counter.
    delist_strikes is bumped only by the daily pass so its unit stays days; this decision
    runs on the five-minute scan, and a timestamp also survives a poller restart, a missed
    cycle, and a change to interval_seconds. 0 disables the soak, matching the convention
    every cap in this module uses.
    """
    if verdict.qualified:
        return False
    if not is_awaiting_sync(listed_at, dest_product_gid):
        return False
    if soak_minutes <= 0:
        return True
    if ineligible_since is None:
        # First failing scan starts the clock. Never act on the same pass that set it, or
        # the soak is a no-op for every product whose row has not been written yet.
        return False
    return (now - ineligible_since) >= timedelta(minutes=soak_minutes)


def delist_cause(p: SourceProduct, allow: Allowlists,
                 rule: PricingRule = DEFAULT_PRICING, *,
                 taxonomy: "TypeTaxonomy") -> str:
    """Why a tagged product stopped qualifying.

    Caps are split on this. Sold-out is the normal high-volume case (every item that
    sells drops below stock 1), so budgeting it against the same cap as an
    allowlist/price change means routine sales trip the cap, ops raise it, and it
    protects nothing on the day the filter actually breaks.
    """
    if p.total_inventory < 1:
        return "sold_out"
    return qualifies(p, allow, rule, taxonomy=taxonomy).rejected_by or "unknown"


# ---------------------------------------------------------------------------
# Desired destination state
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class DesiredTags:
    add: tuple[str, ...]
    remove: tuple[str, ...]


def desired_tags(product_type: str | None, current_tags: Sequence[str],
                 trigger_tag: str, *, taxonomy: "TypeTaxonomy",
                 replace: bool = False) -> DesiredTags:
    """Tags to add and remove on the destination.

    Two modes, and which one applies is decided by where the product is in its life,
    not by a setting.

    replace=False (default, ADDITIVE) - the remove list is scoped strictly to tags this
    automation owns, so anything else survives by construction. Measured 2026-07-23: a
    blanket replace would have destroyed tags on 196 of 250 sampled destination products,
    including `stock:low` (100), `arrival:new` (23) and the correct live category tags
    `SHOE` (52), `JACKET` (37) and `ACC` (6) - moving products out of collections they
    already occupied. That is why an established product is never stripped.

    replace=True - the destination tag set becomes exactly {trigger tag, gender tag(s),
    category tag}. Used ONLY on a freshly delivered product that has not been normalized
    yet, where every tag present was copied from 1nventory by Syncio and describes
    1nventory's operations rather than this storefront: SPO, BFCM, MEM2025 and the like.
    The measurement above does not apply to those products - they have no Shop The Sample
    history to destroy, because they arrived seconds ago.

    The trigger tag is always in `want`, so a replace can never remove it. That matters
    more than it looks: assess_ownership reads the trigger tag to decide a product is
    ours, and stripping it would orphan the product from the automation permanently.
    Syncio's own linkage is a metafield, not a tag, so it is unaffected either way.
    """
    want: list[str] = [trigger_tag.upper()]
    # A tuple, not a single value: "Does Not Apply" resolves to both MEN and WOMEN, so a
    # non-gendered product lands in both collections rather than neither.
    for tag in taxonomy.gender_for(product_type) or ():
        want.append(tag)
    category = taxonomy.category_for(product_type)
    if category:
        want.append(category)
    want_set = {t.upper() for t in want}

    current_upper = {t.upper(): t for t in current_tags}
    add = tuple(t for t in want if t not in current_upper)

    remove = tuple(
        original
        for upper, original in current_upper.items()
        if upper not in want_set and (replace or upper in OWNED_TAG_PREFIXES)
    )
    return DesiredTags(add=add, remove=remove)


# ---------------------------------------------------------------------------
# Inventory location cleanup
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class InventoryLevel:
    inventory_item_gid: str
    location_gid: str
    available: int


@dataclass(frozen=True, slots=True)
class LocationZeroing:
    inventory_item_gid: str
    location_gid: str
    from_qty: int


@dataclass(frozen=True, slots=True)
class LocationPlan:
    zeroings: tuple[LocationZeroing, ...]
    skipped: bool
    reason: str


def plan_location_cleanup(levels: Sequence[InventoryLevel],
                          keep_location_gid: str) -> LocationPlan:
    """Zero every location except the keep location.

    Syncio has a bug where, on first creation, it sometimes writes the product's
    inventory to the wrong location instead of the mapped one. Any quantity outside
    the keep location is therefore bad data by definition - including when it is the
    ONLY quantity the product has.

    That last case is why there is no "would this leave it at zero?" guard here. If
    Lakewood holds 0 and 129 Lafayette holds 3, the 3 is precisely the misplaced
    inventory, not stock worth protecting. Zeroing it takes the product to zero
    briefly, and Syncio's inventory sync - which is hardcoded on and cannot be
    disabled - then republishes the correct quantity from the source link onto the
    mapped location. An earlier revision skipped this case and left the bug in place.

    The one hard stop that remains is an unresolved keep location: with no location
    to keep, EVERY location qualifies as non-keep and a blind pass would wipe the
    catalog. That is a cycle-level misconfiguration, not a per-product condition.
    """
    if not keep_location_gid:
        return LocationPlan((), True, "keep location unresolved; refusing to zero anything")
    if not levels:
        return LocationPlan((), True, "no inventory levels supplied")

    zeroings: list[LocationZeroing] = [
        LocationZeroing(lvl.inventory_item_gid, lvl.location_gid, lvl.available)
        for lvl in levels
        if lvl.location_gid != keep_location_gid and lvl.available != 0
    ]

    if not zeroings:
        return LocationPlan((), False, "no phantom inventory outside the keep location")
    total = sum(z.from_qty for z in zeroings)
    return LocationPlan(
        tuple(zeroings), False,
        f"clearing {total} phantom units across {len(zeroings)} location entries",
    )


# ---------------------------------------------------------------------------
# Blast-radius caps
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class SafetyCaps:
    max_actions_per_cycle: int = 250
    max_variant_writes_per_cycle: int = 600
    # Deletes attributable to a filter/allowlist change: tight.
    max_deletes_per_cycle: int = 0
    # Deletes attributable to a sale: generous, because this is the normal case.
    max_sold_out_deletes_per_cycle: int = 50
    max_pct_of_footprint_changed: float = 10.0
    max_units_zeroed_per_cycle: int = 400
    min_candidate_set_size: int = 50
    max_candidate_set_shrink_pct: float = 50.0
    # Pre-delivery untags in one scan. This is the main numeric guard on that path, and it
    # matters most because the trigger set is EVERY rejection reason, taxonomy gaps
    # included: dropping one type mapping from listing options makes every product of that
    # type fail qualification at once. check_taxonomy_health() catches a collapsed
    # taxonomy, this catches a single mapping deleted by hand. 0 disables, like every cap
    # here.
    max_pre_delivery_untags_per_cycle: int = 25
    # Stock-reconciliation blast radius. Distinct from max_units_zeroed_per_cycle, which
    # counts Shopify inventory-level writes on the destination; this counts STATE ROWS whose
    # stored stock the scan is about to correct downward because their SKUs are no longer on
    # Shopify. Reconciliation is the one place the pipeline treats absence as signal, so it
    # gets its own ceiling rather than sharing one. 0 disables, like every cap here.
    max_rows_zeroed_per_cycle: int = 50


def check_reconcile_cap(rows_zeroed: int, caps: SafetyCaps) -> str | None:
    """Breach message if a reconciliation pass wants to zero too many rows, else None.

    A mass disappearance is a broken read until a human says otherwise. This is the guard
    that keeps `absence -> zero` from becoming the SPO mapping wipe: there, an unexpectedly
    small input set was treated as authoritative and sibling rows were destroyed.

    Note the caller must ALSO have established that the scan completed. This cap bounds the
    damage from a scan that completed but was wrong; it cannot detect one that was truncated,
    because a truncated scan is what the abort paths in _cycle already catch.
    """
    if caps.max_rows_zeroed_per_cycle > 0 and rows_zeroed > caps.max_rows_zeroed_per_cycle:
        return (
            f"max_rows_zeroed_per_cycle: {rows_zeroed} > "
            f"{caps.max_rows_zeroed_per_cycle}; refusing to zero stock this cycle"
        )
    return None


def check_caps(
    action_count: int,
    delete_count: int,
    sold_out_delete_count: int,
    variant_writes: int,
    units_zeroed: int,
    footprint: int,
    caps: SafetyCaps,
    pre_delivery_untag_count: int = 0,
) -> str | None:
    """Return a breach message, or None if the cycle may proceed.

    Module-level and pure so it is testable without constructing a poller - unlike
    daily_sellercloud_sync_poller._check_gates, which touches self only for constants
    and therefore needs a whole poller to exercise.

    MUST be evaluated on the FULL action list BEFORE the first write. Delist is
    untag-then-delete with mandatory ordering, so a mid-cycle abort would leave
    products untagged on the source with their destination products still live -
    manufacturing exactly the orphan state we are trying to avoid.
    """
    # 0 disables. ONE convention, no exceptions - every cap in this function, including
    # both delete caps (2026-07-29, on request).
    #
    # This previously inverted for the delete caps, where 0 meant "none permitted". That
    # asymmetry was itself the hazard: two adjacent settings holding the same value and
    # meaning opposite things is a comment nobody re-reads under pressure. A single rule
    # that holds everywhere is safer than a special case that is correct on paper.
    #
    # What this means for deletes: the numeric ceiling is no longer what stops them.
    # Whether a productDelete ever fires is gated by execute_deletes (off), auto_delist
    # (off), delist_soak_cycles (two consecutive DAILY failures), and a human pressing
    # Delist. Those are the guards; the cap was a fifth one that also made routine
    # sell-through trip an alarm.
    if caps.max_actions_per_cycle > 0 and action_count > caps.max_actions_per_cycle:
        return f"max_actions_per_cycle: {action_count} > {caps.max_actions_per_cycle}"
    if caps.max_deletes_per_cycle > 0 and delete_count > caps.max_deletes_per_cycle:
        return f"max_deletes_per_cycle: {delete_count} > {caps.max_deletes_per_cycle}"
    if (caps.max_sold_out_deletes_per_cycle > 0
            and sold_out_delete_count > caps.max_sold_out_deletes_per_cycle):
        return (
            f"max_sold_out_deletes_per_cycle: {sold_out_delete_count} > "
            f"{caps.max_sold_out_deletes_per_cycle}"
        )
    if (caps.max_pre_delivery_untags_per_cycle > 0
            and pre_delivery_untag_count > caps.max_pre_delivery_untags_per_cycle):
        return (
            f"max_pre_delivery_untags_per_cycle: {pre_delivery_untag_count} > "
            f"{caps.max_pre_delivery_untags_per_cycle}"
        )
    if (caps.max_variant_writes_per_cycle > 0
            and variant_writes > caps.max_variant_writes_per_cycle):
        return f"max_variant_writes_per_cycle: {variant_writes} > {caps.max_variant_writes_per_cycle}"
    if (caps.max_units_zeroed_per_cycle > 0
            and units_zeroed > caps.max_units_zeroed_per_cycle):
        return f"max_units_zeroed_per_cycle: {units_zeroed} > {caps.max_units_zeroed_per_cycle}"
    # 0 disables, like every cap above it. Note this reads the opposite way to how it
    # looks: "0% of the footprint may change" would be the stricter reading, and it is NOT
    # what this does. That is the price of one uniform convention, and it is worth paying -
    # a single rule that holds for all six beats five that agree and one that inverts.
    if footprint > 0 and caps.max_pct_of_footprint_changed > 0:
        pct = action_count / footprint * 100
        if pct > caps.max_pct_of_footprint_changed:
            return (
                f"max_pct_of_footprint_changed: {pct:.1f}% of {footprint} > "
                f"{caps.max_pct_of_footprint_changed}%"
            )
    return None


@dataclass(frozen=True, slots=True)
class SyncioCapacity:
    """How much of Syncio's delivery budget is free right now."""

    in_flight: int
    ceiling: int
    remaining: int
    blocked: bool
    message: str


def check_syncio_capacity(products_in_flight: int, max_in_flight: int) -> SyncioCapacity:
    """Room left before we out-run Syncio.

    Tagging past what Syncio can deliver does not make anything arrive sooner - it just
    builds an invisible queue and makes "awaiting Syncio" useless as a health signal. So
    both the scheduled pass and the manual button budget against the same ceiling.

    Counted in PRODUCTS, whatever their size run. An earlier revision budgeted in variants
    on the theory that a 12-size run is more work for Syncio than a one-size bag; in
    practice a small product cap is easier to reason about while the pipeline is being
    watched by hand, and one product with a deep size run should not consume a whole batch.

    A ceiling of 0 or less means the gate is disabled, not that nothing may be submitted.
    """
    if max_in_flight <= 0:
        return SyncioCapacity(products_in_flight, max_in_flight, 0, False,
                              "Syncio capacity gate disabled")

    remaining = max(0, max_in_flight - products_in_flight)
    if remaining == 0:
        return SyncioCapacity(
            products_in_flight, max_in_flight, 0, True,
            f"Syncio still has {products_in_flight} of {max_in_flight} products "
            "outstanding; nothing new until they land",
        )
    return SyncioCapacity(
        products_in_flight, max_in_flight, remaining, False,
        f"{remaining} of {max_in_flight} products free "
        f"({products_in_flight} awaiting Syncio)",
    )


@dataclass(frozen=True, slots=True)
class SubmitCooldown:
    """Whether a NEW batch may go out yet, independent of how large it may be."""

    allowed: bool
    message: str
    retry_after_hours: float = 0.0


def check_submit_cooldown(
    products_in_flight: int,
    last_submit_at: datetime | None,
    now: datetime,
    cooldown_hours: int,
) -> SubmitCooldown:
    """Pace successive batches. Allowed when EITHER arm holds.

        products_in_flight == 0        the previous batch fully landed, go again
        last submit >= cooldown_hours  Syncio is stuck or slow; do not block forever

    max_products_in_flight caps the SIZE of one batch and says nothing about cadence, so
    without this the button can be pressed repeatedly and pile batch on batch before
    Syncio has delivered any of them.

    Drain-first is the primary rule; the hours arm is an escape hatch. Syncio has no
    completion callback and its documented window is 1-3 days, so a pure drain-first gate
    would block indefinitely the first time a delivery silently failed - which is exactly
    the stale_awaiting_sync condition the Overview already reports.

    `now` is a parameter rather than read from the clock so this stays pure and testable.
    cooldown_hours <= 0 disables the gate, matching check_syncio_capacity's convention
    that a non-positive ceiling means "off", not "nothing may pass".
    """
    if cooldown_hours <= 0:
        return SubmitCooldown(True, "Submit cooldown disabled")
    if products_in_flight <= 0:
        return SubmitCooldown(True, "Syncio has delivered everything outstanding")
    if last_submit_at is None:
        # Variants in flight but nothing in the ledger: a backfill populated the state
        # rather than this pipeline tagging it. Nothing to pace against.
        return SubmitCooldown(True, "No previous submit on record")

    elapsed_hours = (now - last_submit_at).total_seconds() / 3600
    if elapsed_hours >= cooldown_hours:
        return SubmitCooldown(
            True,
            f"{products_in_flight} products still awaiting Syncio, but the last submit "
            f"was {elapsed_hours:.0f}h ago; proceeding anyway",
        )
    remaining = cooldown_hours - elapsed_hours
    return SubmitCooldown(
        False,
        f"{products_in_flight} products still awaiting Syncio. Waiting for them to land, "
        f"or {remaining:.1f}h more since the last submit",
        remaining,
    )


# ---------------------------------------------------------------------------
# Lifecycle status derivation
# ---------------------------------------------------------------------------

# Statuses the scan pass may write. NOT InternalPlatformStatus, which is the ledger-row
# vocabulary (pending/success/failed/skipped) for internal_platform_submissions. These are
# internal_platform_state.current_status values and are constrained by chk_ip_state_status.
PENDING: Final[str] = "pending"
PENDING_NORMALIZATION: Final[str] = "pending_normalization"
READY_FOR_LISTING: Final[str] = "ready_for_listing"
PENDING_DELISTING: Final[str] = "pending_delisting"
SKIPPED: Final[str] = "skipped"


def derive_scan_status(
    *,
    tagged: bool,
    qualified: bool,
    rejected_by: str | None,
    soak_reached: bool,
    current_status: str | None = None,
) -> tuple[str, str | None] | None:
    """Status the SCAN may set for one product, or None to leave the row alone.

    Returning None is the important case. Once a product is tagged its status belongs to
    the tag/normalize/delist paths - mark_listed, mark_normalized, and the destination
    poller which owns 'listed'. A scan that recomputed status for tagged rows would
    overwrite the destination poller's work every five minutes.

    So the scan only judges what it can see from the SOURCE side: whether the trigger tag
    is present, and whether the product currently qualifies.

    The one exception is a tagged product still sitting at the table default, PENDING.
    That happens when something outside this pipeline applied the trigger tag - a human
    tagging by hand on 1nventory - so refresh_product_facts inserted the row but the rule
    above declined to classify it, leaving it PENDING forever. It is tagged and Syncio has
    not delivered it, which is precisely pending_normalization. Guarded on the current
    status so it can never overwrite a real one.
    """
    if not tagged:
        if qualified:
            return (READY_FOR_LISTING, None)
        return (SKIPPED, rejected_by)
    if not qualified and soak_reached:
        return (PENDING_DELISTING, rejected_by)
    if current_status == PENDING:
        return (PENDING_NORMALIZATION, None)
    return None


def fit_to_capacity(
    candidates: Sequence[tuple[str, int]],
    remaining_products: int,
) -> tuple[list[str], int, int]:
    """Take candidates in order until the product budget is spent.

    `candidates` is (parent_sku, variant_count). Returns the SKUs that fit, the total
    variants they carry, and how many were held back.

    The variant count no longer gates anything - a product is admitted whatever its size
    run - but it is still summed and returned, because "5 products / 37 variants" is what
    the submit report and the Syncio queue are actually made of.
    """
    taken: list[str] = []
    used = 0
    for sku, variants in candidates:
        if len(taken) >= max(remaining_products, 0):
            break
        taken.append(sku)
        used += variants
    return taken, used, len(candidates) - len(taken)


# A scan-completeness gate was built here on 2026-07-29 and removed the same day, because
# tracing the writes showed it guarded nothing this pipeline can suffer.
#
# Every write is keyed on a product the scan RETURNED: refresh_product_facts,
# apply_delist_strikes, apply_scan_statuses and to_delist are all built inside the loop
# over scanned products, and state_map is only ever read as .get(parent) for one of them.
# Nothing iterates the state table, and nothing computes a set difference, so absence from
# a scan is never signal. A truncated read means less work done, never wrong work - the
# next cycle picks the product up.
#
# This is also why the two candidate-set proxies below are safely zeroed: the SPO mapping
# wipe they were modelled on happened because an unexpectedly small input set was treated
# as AUTHORITATIVE. Here it is treated as incomplete by construction. The one input that
# can still do damage is a degraded taxonomy, because it makes the products we DO read
# fail qualification - and that is guarded directly by check_taxonomy_health.


def check_candidate_set(current_size: int, previous_size: int | None,
                        caps: SafetyCaps) -> str | None:
    """Guard against an incomplete source scan being read as a mass drop-out.

    A truncated or partially-failed catalog read is indistinguishable from "these
    3,000 products all stopped qualifying". This is the shape of the SPO mapping
    wipe incident, where an unexpectedly small input set was treated as authoritative.

    Both halves take 0 to mean disabled, matching every other cap. They are a PROXY,
    though: they infer a broken input from the shape of the output, so they also fire on
    a genuine sell-through and stay silent on a half-loaded taxonomy that still leaves
    120 products qualifying. check_taxonomy_health() and the scan-completeness assertion
    test the inputs directly, and are what make zeroing these two safe.
    """
    if 0 < caps.min_candidate_set_size and current_size < caps.min_candidate_set_size:
        return (
            f"candidate set {current_size} below min_candidate_set_size "
            f"{caps.min_candidate_set_size}; source read likely incomplete"
        )
    if previous_size and previous_size > 0 and caps.max_candidate_set_shrink_pct > 0:
        shrink = (previous_size - current_size) / previous_size * 100
        if shrink > caps.max_candidate_set_shrink_pct:
            return (
                f"candidate set shrank {shrink:.1f}% ({previous_size} -> {current_size}), "
                f"over max_candidate_set_shrink_pct {caps.max_candidate_set_shrink_pct}%"
            )
    return None
