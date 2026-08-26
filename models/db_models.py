from enum import StrEnum
from typing import Any, Dict, List

import uuid

from tortoise import fields
from tortoise.models import Model


class SubmissionStatus(StrEnum):
    QUEUED = "queued"
    PENDING = "pending"
    PROCESSING = "processing"
    # The platform accepted the submission and a human still owes it something. eBay is
    # the first: publishing a listing does not attach its images, which an operator does
    # by uploading a File Exchange file by hand. Distinct from PENDING, which means "not
    # sent yet" and drives the submit button and the dashboard's unsent-work badges.
    AWAITING_ACTION = "awaiting_action"
    SUCCESS = "success"
    FAILED = "failed"


TERMINAL_STATUSES = {SubmissionStatus.SUCCESS, SubmissionStatus.FAILED}
# Work the system is still carrying. AWAITING_ACTION is in flight because the submission is
# not finished, even though nothing automated will move it: the listing's "pending" count is
# what tells an operator there is something left to do.
IN_FLIGHT_STATUSES = {
    SubmissionStatus.QUEUED,
    SubmissionStatus.PENDING,
    SubmissionStatus.PROCESSING,
    SubmissionStatus.AWAITING_ACTION,
}


class Template(Model):

    id = fields.CharField(pk=True, max_length=100)
    name = fields.CharField(
        max_length=100, unique=True, description="Template name (database identifier)"
    )
    display_name = fields.CharField(max_length=200, description="Human-readable name for UI")
    description = fields.TextField(null=True, description="Optional template description")

    field_definitions = fields.JSONField(
        description="Field definitions based on FieldDefinition model"
    )

    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)
    is_active = fields.BooleanField(default=True, description="Whether template is active/visible")

    class Meta:
        table = "templates"
        ordering = ["name"]

    def __str__(self):
        return f"Template({self.name} - {self.display_name})"

    @property
    def field_count(self) -> int:
        return len(self.field_definitions) if self.field_definitions else 0

    def get_field_by_name(self, field_name: str) -> Dict[str, Any] | None:
        if not self.field_definitions:
            return None

        return next(
            (field for field in self.field_definitions if field.get("name") == field_name),
            None,
        )

    def add_field(self, field_definition: Dict[str, Any]) -> None:
        if not self.field_definitions:
            self.field_definitions = []

        if "order" not in field_definition:
            max_order = max([f.get("order", 0) for f in self.field_definitions], default=0)
            field_definition["order"] = max_order + 1

        self.field_definitions.append(field_definition)

    def remove_field(self, field_name: str) -> bool:
        if not self.field_definitions:
            return False

        original_length = len(self.field_definitions)
        self.field_definitions = [f for f in self.field_definitions if f.get("name") != field_name]

        return len(self.field_definitions) < original_length

    def reorder_fields(self, field_order: List[str]) -> None:
        if not self.field_definitions:
            return

        field_map = {f.get("name"): f for f in self.field_definitions}

        reordered_fields = []
        for i, field_name in enumerate(field_order):
            if field_name in field_map:
                field_def = field_map[field_name].copy()
                field_def["order"] = i
                reordered_fields.append(field_def)

        existing_names = set(field_order)
        for field_def in self.field_definitions:
            if field_def.get("name") not in existing_names:
                field_def["order"] = len(reordered_fields)
                reordered_fields.append(field_def)

        self.field_definitions = reordered_fields


class Batch(Model):

    id = fields.IntField(pk=True)
    comment = fields.TextField(null=True, description="Batch description/comment")
    assigned_to = fields.CharField(
        max_length=100,
        index=True,
        null=True,
        description="User ID assigned to this batch",
    )
    priority = fields.CharField(
        max_length=10,
        default="medium",
        index=True,
        description="Batch priority: low, medium, high",
    )
    created_by = fields.CharField(max_length=100, description="User ID who created this batch")

    status = fields.CharField(
        max_length=20,
        default="new",
        index=True,
        description="Batch status: new, in_progress, completed",
    )
    total_listings = fields.IntField(default=0, description="Total number of listings in batch")
    submitted_listings = fields.IntField(default=0, description="Number of submitted listings")

    photography_batch_id = fields.IntField(null=True, description="Reference to photography batch")

    platform_submission_statuses = fields.JSONField(
        default=dict,
        description="Denormalized per-product per-platform submission status",
    )

    # Frozen merchandise-value snapshot taken once, after create_batch commits. Never
    # recomputed, and deliberately outside update_batch_counts() - see
    # migrations/add_batch_total_value.sql. total_value is non-null so it cannot sort
    # ahead of real values under ORDER BY total_value DESC; value_computed_at is what
    # separates "not computed" from "worth nothing".
    total_value = fields.DecimalField(
        max_digits=14,
        decimal_places=2,
        default=0,
        description="Sum over products of (physical qty x SitePrice) at batch creation",
    )
    product_values = fields.JSONField(
        default=dict,
        description="Per-parent-SKU breakdown behind total_value",
    )
    value_computed_at = fields.DatetimeField(
        null=True,
        description="When the value snapshot was taken; null means never computed",
    )

    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "batches"
        ordering = ["-created_at"]

    def __str__(self):
        return f"Batch({self.id} - {self.comment[:50]}...)"

    @property
    def progress_percentage(self) -> float:
        if self.total_listings == 0:
            return 0.0
        return (self.submitted_listings / self.total_listings) * 100

    @property
    def is_completed(self) -> bool:
        return self.total_listings > 0 and self.submitted_listings == self.total_listings


class Listing(Model):

    id = fields.UUIDField(pk=True, default=uuid.uuid4)
    product_id = fields.CharField(
        max_length=200, index=True, description="Product ID from SellerCloud"
    )
    info_product_id = fields.CharField(
        max_length=255,
        null=True,
        description="Full SellerCloud product ID, including variations",
    )

    assigned_to = fields.CharField(
        max_length=100,
        index=True,
        null=True,
        description="User ID assigned to this listing",
    )
    data = fields.JSONField(default=dict, description="Form data based on template JSON schema")
    original_data = fields.JSONField(
        null=True,
        description="Snapshot of `data` at creation time; write-once, never updated",
    )

    ai_response = fields.JSONField(null=True, description="AI generated response or suggestions")
    ai_description = fields.TextField(null=True, description="AI generated description")
    original_description = fields.TextField(
        null=True, description="Original SellerCloud description"
    )
    original_title = fields.TextField(
        null=True,
        description="Title at creation, before the title template rewrites it; write-once",
    )
    title_auto_update = fields.BooleanField(
        default=True,
        description=(
            "Whether the title still tracks the title template. Cleared the first time an "
            "operator takes the field over, so the edit survives a reload"
        ),
    )
    submitted = fields.BooleanField(
        default=False, description="Whether the listing has been submitted"
    )
    submitted_at = fields.DatetimeField(
        null=True, description="Timestamp when the listing was submitted"
    )
    submitted_by = fields.CharField(
        max_length=100, null=True, description="User ID who submitted this listing"
    )
    error = fields.TextField(
        null=True, description="Error traceback from post-submission operations"
    )
    upload_status = fields.CharField(
        max_length=20,
        default="pending",
        description="Image upload status: pending (uploading) or uploaded (ready)",
    )
    created_by = fields.CharField(max_length=100, description="User ID who created this listing")

    batch = fields.ForeignKeyField(
        "models.Batch",
        related_name="listings",
        null=True,
        on_delete=fields.SET_NULL,
        description="Batch this listing belongs to",
    )

    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "listings"
        ordering = ["-created_at"]

    def __str__(self):
        return f"Listing({self.id} - Product: {self.product_id})"

    @property
    def is_completed(self) -> bool:
        return self.submitted and self.submitted_at is not None

    async def get_submission_summary(self) -> Dict[str, Any]:
        submissions = await self.submissions.all()

        if not submissions:
            if self.submitted:
                return {
                    "total_platforms": 1,
                    "successful": 1,
                    "failed": 0,
                    "pending": 0,
                    "platforms": {
                        "sellercloud": {
                            "status": "success",
                            "submitted_at": (
                                self.submitted_at.isoformat() if self.submitted_at else None
                            ),
                            "submitted_by": self.submitted_by,
                        }
                    },
                }
            return {
                "total_platforms": 0,
                "successful": 0,
                "failed": 0,
                "pending": 0,
                "platforms": {},
            }

        platforms = {}
        for sub in submissions:
            if (
                sub.platform_id not in platforms
                or sub.attempt_number > platforms[sub.platform_id].attempt_number
            ):
                platforms[sub.platform_id] = sub

        successful = sum(1 for s in platforms.values() if s.status == SubmissionStatus.SUCCESS)
        failed = sum(1 for s in platforms.values() if s.status == SubmissionStatus.FAILED)
        in_flight = sum(1 for s in platforms.values() if s.status in IN_FLIGHT_STATUSES)

        return {
            "total_platforms": len(platforms),
            "successful": successful,
            "failed": failed,
            "pending": in_flight,
            "platforms": {
                pid: {
                    "status": sub.status,
                    "submitted_at": sub.submitted_at.isoformat() if sub.submitted_at else None,
                    "submitted_by": sub.submitted_by,
                    "error": sub.error,
                    "attempt_number": sub.attempt_number,
                    "external_id": sub.external_id,
                }
                for pid, sub in platforms.items()
            },
        }

    async def has_successful_submission(self, platform_id: str = None) -> bool:
        if platform_id:
            return await self.submissions.filter(platform_id=platform_id, status="success").exists()
        return await self.submissions.filter(status="success").exists()


class ListingSubmission(Model):

    id = fields.IntField(pk=True)
    listing = fields.ForeignKeyField(
        "models.Listing",
        related_name="submissions",
        on_delete=fields.SET_NULL,
        null=True,
        description="The listing this submission belongs to",
    )
    platform_id = fields.CharField(
        max_length=50,
        index=True,
        description="Platform identifier (sellercloud, grailed, ebay, etc.)",
    )
    status = fields.CharField(
        max_length=20,
        default="pending",
        index=True,
        description=(
            "Submission status: queued, pending, processing, awaiting_action, "
            "success, failed"
        ),
    )

    submitted_by = fields.CharField(
        max_length=100,
        null=True,
        description="User ID who initiated the submission",
    )
    submitted_at = fields.DatetimeField(
        null=True,
        description="When the submission completed (set by trigger on status change)",
    )

    error = fields.TextField(
        null=True,
        description="Technical error message/traceback if failed",
    )
    error_display = fields.TextField(
        null=True,
        description="Human-friendly error message shown in UI",
    )

    platform_status = fields.CharField(
        max_length=50,
        null=True,
        description="Granular platform-specific progress within the 'processing' status",
    )

    platform_meta = fields.JSONField(
        null=True,
        description="Platform-specific tracking data (e.g., {product_import_id: 123})",
    )

    attempt_number = fields.IntField(
        default=1,
        description="Which attempt this is (for retry tracking)",
    )

    external_id = fields.JSONField(
        null=True,
        description="ID/reference(s) from the external platform after successful submission",
    )

    reviewed_at = fields.DatetimeField(
        null=True,
        description="When a failed manual-fallback submission was triaged. Reviewing "
        "also moves the row to 'success', so this is the audit trail of why a "
        "submission succeeded without the platform ever accepting it",
    )
    reviewed_by = fields.CharField(
        max_length=100,
        null=True,
        description="User ID who marked the failed submission as reviewed",
    )

    completed_at = fields.DatetimeField(
        null=True,
        description="When an operator confirmed the manual step this submission was "
        "waiting on. Deliberately NOT reviewed_at: that records a FAILED submission "
        "being triaged, this records a submission the platform accepted that still "
        "needed a human. Conflating them would make the two indistinguishable in "
        "reporting",
    )
    completed_by = fields.CharField(
        max_length=100,
        null=True,
        description="User ID who confirmed the manual step",
    )

    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "listing_submissions"
        ordering = ["-created_at"]
        unique_together = [("listing", "platform_id", "attempt_number")]

    def __str__(self):
        return f"ListingSubmission({self.listing_id} -> {self.platform_id}: {self.status})"


class AppSettings(Model):

    id = fields.IntField(pk=True)
    field_templates = fields.JSONField(
        default=dict,
        description="Field templates mapping field names to template configs: {field_name: {template: '...'}}",
    )
    app_variables = fields.JSONField(
        default=[{"id": "max_batches", "name": "Maximum Batch Size", "value": 50}],
        description="Application configuration variables",
    )

    platform_settings = fields.JSONField(
        default={},
        description=(
            "Platform-specific settings keyed by platform_id. "
            "Common keys: enabled, price_multiplier, shipping. "
            "manual_fallback (bool) - when true the platform shows in the "
            "Submissions Dashboard and exposes a manual 'Submit Now' batch action. "
            "min_batch_size (int) - minimum pending count before the auto-poller "
            "groups a batch. Manual 'Submit Now' bypasses this threshold. "
            "(e.g. SPO reads platform_settings.spo.min_batch_size)."
        ),
    )

    platforms = fields.JSONField(
        default=["sellercloud", "grailed"],
        description="List of enabled platform IDs for submission",
    )

    strict_template_validation = fields.BooleanField(
        default=False,
        description=(
            "When true, saving a field template rejects any {placeholder} that is not in the "
            "valid field list (throws at save time)."
        ),
    )

    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "app_settings"

    def __str__(self):
        return f"AppSettings({self.id})"


class InternalPlatformAction(StrEnum):
    """Actions the consignment pipeline can take against a parent product.

    Written into internal_platform_submissions.action, which carries a CHECK
    constraint - a typo here would create a private namespace no query ever finds.
    """

    LIST = "list"           # tag on source; Syncio delivers 1-3 days later
    NORMALIZE = "normalize"  # vendor + tags on destination
    REPRICE = "reprice"      # price / compare-at on destination
    LOCATION = "location"    # zero phantom non-Lakewood inventory
    UNTAG = "untag"          # remove trigger tag on source (first half of delist)
    DELETE = "delete"        # productDelete on destination (irreversible)


class InternalPlatformStatus(StrEnum):
    """Terminal-ish states for a ledger row.

    Deliberately NOT SubmissionStatus: there is no human queuer and no overlapping
    cycles here, so queued/processing have no meaning. `skipped` is the state
    SubmissionStatus lacks, and without it the two documented skip-and-flag
    behaviours (unmapped product type, location guard trip) would have to lie as
    either success or failed.
    """

    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class InternalPlatformSkipReason(StrEnum):
    """Why we deliberately declined to act. Not an error - nothing failed.

    Populates the flagged report (WHERE skip_reason IS NOT NULL), which is how an
    operator sees what the automation chose not to touch.
    """

    # Selection: the source product does not qualify for STS.
    NO_INVENTORY = "no_inventory"
    UNMAPPED_PRODUCT_TYPE = "unmapped_product_type"
    UNDERIVABLE_GENDER = "underivable_gender"
    BELOW_PRICE_FLOOR = "below_price_floor"
    NO_COMPARE_AT = "no_compare_at"
    # The discount band has TWO edges and both reject. TOO_HIGH means the markdown is so
    # steep it cannot be brought inside the 80% cap without an implausible price bump;
    # TOO_LOW means the product sits near full price and is not worth a consignment slot
    # (under the 15% floor). The low edge was missing here while qualifies() emitted a
    # code for it anyway - measured 2026-07-28, 584 products, the fourth largest
    # rejection category, filed under a reason the Skipped Products filter did not know
    # existed and so could never surface.
    DISCOUNT_TOO_HIGH = "discount_too_high"
    DISCOUNT_TOO_LOW = "discount_too_low"
    NO_PRICED_VARIANTS = "no_priced_variants"
    # Normalization: we reached the destination product but declined to write.
    NOT_OURS = "not_ours"
    CAS_MISMATCH = "cas_mismatch"
    # NOTE: location_guard / inventory_would_zero were removed. They skipped the exact
    # case the location pass exists to fix - stock sitting only at a wrong location is
    # Syncio's creation bug, not inventory worth protecting. See
    # internal_platform_rules.plan_location_cleanup.
    # No variant SKU resolves to a registered parent in the products DB. We do not
    # list products SkuBase does not know about - they cannot be normalized safely
    # and would escape the destination ownership guard.
    UNREGISTERED_PARENT = "unregistered_parent"
    # Every variant SKU on this product has been reassigned (merged) onto another parent.
    # Distinct from UNREGISTERED_PARENT: SkuBase knows these SKUs perfectly well, they
    # have simply stopped describing this product. Resolving them would key the product's
    # state row under a DIFFERENT garment's parent, which is how three rows came to hold
    # the wrong source_product_gid. See internal_platform_products.load_reassigned.
    REASSIGNED_SKU = "reassigned_sku"


class InternalPlatform(Model):
    """A consignment target driven through a third-party sync tool, not an API.

    source_store / dest_store are LOOKUP KEYS into config.toml [shopify.stores.*].
    They must never be interpolated into a URL host: the token exchange POSTs the
    client secret to https://{store}.myshopify.com, so a writable DB value reaching
    the host would exfiltrate that secret.
    """

    id = fields.CharField(pk=True, max_length=50)
    name = fields.CharField(max_length=255)

    source_store = fields.CharField(
        max_length=60, description="Config lookup key for the source store, not a hostname"
    )
    dest_store = fields.CharField(
        max_length=60, description="Config lookup key for the destination store, not a hostname"
    )
    trigger_tag = fields.CharField(
        max_length=64,
        description="Tag applied on source to trigger sync. Flows into Shopify's query: "
        "mini-language, which GraphQL variables do not protect, so it is regex-validated",
    )

    dest_location_gid = fields.TextField(
        null=True,
        description="Resolved Shopify location GID. Never match a location by display "
        "name: a rename would make every location 'not Lakewood' and zero the catalog",
    )
    dest_location_name = fields.CharField(max_length=255, null=True)

    enabled = fields.BooleanField(default=False)

    config = fields.JSONField(
        default=dict,
        description="Reserved for per-platform settings. Until populated, numeric rules "
        "live in internal_platform_rules.py so there is one source of truth",
    )

    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "internal_platforms"

    def __str__(self):
        return f"InternalPlatform({self.id}: {self.source_store} -> {self.dest_store})"


class InternalPlatformState(Model):
    """Current state per (platform, parent_sku). Narrow, hot, bounded at catalog size.

    Loaded into a dict once per cycle. This is what replaces a per-product idempotency
    query (~72k queries/day -> 24), and it carries the in-flight claim so a crash
    cannot be mistaken for work in progress forever.
    """

    id = fields.BigIntField(pk=True)

    internal_platform = fields.ForeignKeyField(
        "models.InternalPlatform",
        related_name="states",
        on_delete=fields.CASCADE,
        source_field="internal_platform_id",
    )
    parent_sku = fields.CharField(
        max_length=110,
        description="No FK: parent_products lives in the products DB. Same treatment as "
        "listings.product_id",
    )

    # Shopify GIDs as text. Shopify returns GIDs and Syncio's metafield holds one;
    # round-tripping through bigint invites the ID-namespace confusion we are guarding
    # against.
    source_product_gid = fields.TextField(null=True)
    dest_product_gid = fields.TextField(null=True)

    # Storefront addressing, for the platform links on the product page.
    #
    # The handle builds the URL; `online` records whether Shopify actually serves it.
    # They are separate because on the SOURCE store they disagree for most of the
    # catalog: a sold-out product is unpublished from the Online Store channel, so its
    # handle still exists but the page 404s. Measured 2026-08-13, 2,523 of 14,985 source
    # products are online (16.8%) against 99% on the destination store.
    #
    # Stored rather than fetched per view, so the product page needs no Shopify call.
    source_handle = fields.TextField(null=True)
    source_online = fields.BooleanField(null=True)
    dest_handle = fields.TextField(null=True)
    dest_online = fields.BooleanField(null=True)

    current_status = fields.CharField(max_length=30, default="pending")

    inflight_action = fields.CharField(
        max_length=20,
        null=True,
        description="Set while an action is being attempted. Backed by a partial unique "
        "index, and swept by the stale-recovery pass - without that sweep a crash would "
        "block this key permanently",
    )
    inflight_since = fields.DatetimeField(null=True)

    listed_at = fields.DatetimeField(null=True)
    normalize_done_at = fields.DatetimeField(null=True)
    location_done_at = fields.DatetimeField(null=True)
    delisted_at = fields.DatetimeField(null=True)

    last_source_updated_at = fields.TextField(null=True)
    last_dest_updated_at = fields.TextField(null=True)
    desired_hash = fields.TextField(
        null=True, description="Hash of the desired state last successfully written"
    )

    delist_strikes = fields.IntField(
        default=0,
        description="Consecutive cycles failing qualification. Delist fires only at the "
        "soak threshold, so a transient bad read cannot trigger deletes",
    )

    ineligible_since = fields.DatetimeField(
        null=True,
        description="When a tagged product last started failing qualification. Drives the "
        "pre-delivery untag, which runs on the five-minute scan; delist_strikes cannot "
        "carry it because only the DAILY pass may bump that counter",
    )

    # Shopify-derived facts, refreshed by the source scan on every pass. Denormalised
    # here rather than joined at read time: the database is remote at ~0.57s a round trip
    # and the Products endpoint was just optimised by removing round trips.
    title = fields.TextField(null=True)
    image_url = fields.TextField(null=True)
    product_type = fields.TextField(null=True)
    inventory = fields.IntField(default=0)
    source_price = fields.DecimalField(max_digits=12, decimal_places=2, null=True)
    source_compare_at = fields.DecimalField(max_digits=12, decimal_places=2, null=True)
    sts_price = fields.DecimalField(max_digits=12, decimal_places=2, null=True)
    # [{sku, size, price, compare_at, inventory}] - read whole, never joined or aggregated
    # across products, so JSONB rather than a child table and a second round trip.
    variants = fields.JSONField(null=True)

    variant_count = fields.IntField(
        default=0,
        description="Variants on the source product. Syncio's throughput limit is "
        "variants per day, so the submit gate sums this across everything awaiting "
        "delivery. Refreshed on every scan rather than written once at tag time",
    )

    skip_reason = fields.CharField(max_length=40, null=True)
    last_error = fields.TextField(null=True)

    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "internal_platform_state"
        unique_together = [("internal_platform", "parent_sku")]

    def __str__(self):
        return f"InternalPlatformState({self.parent_sku}: {self.current_status})"


class InternalPlatformSubmission(Model):
    """Append-only action history. Audit, not control.

    A row is written ONLY when an API call was actually made. An evaluation that
    results in no call is not an action - that rule is the difference between
    ~0.2 GB/year and ~31 GB/year.
    """

    id = fields.BigIntField(pk=True)

    internal_platform_id = fields.CharField(max_length=50, index=True)
    parent_sku = fields.CharField(max_length=110)

    action = fields.CharField(max_length=20)
    status = fields.CharField(max_length=20, default="pending")
    skip_reason = fields.CharField(max_length=40, null=True)

    source_product_gid = fields.TextField(null=True)
    dest_product_gid = fields.TextField(null=True)

    payload = fields.JSONField(
        null=True,
        description="Whitelisted {mutation, variables, before}. The `before` pre-image is "
        "the only rollback material a delete will ever have and must be committed before "
        "the mutation. Never the header map - it holds the access token",
    )
    result = fields.JSONField(
        null=True, description="Whitelisted {userErrors, ids, cost}. Never a raw response"
    )

    error = fields.TextField(null=True)

    actor = fields.CharField(max_length=100, null=True)
    triggered_by = fields.CharField(max_length=20, default="scheduler")

    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "internal_platform_submissions"
        ordering = ["-created_at"]

    def __str__(self):
        return (
            f"InternalPlatformSubmission({self.parent_sku} -> "
            f"{self.internal_platform_id}.{self.action}: {self.status})"
        )


# ---------------------------------------------------------------------------
# eBay item aspects
# ---------------------------------------------------------------------------
# Reference data (EbayCategory, EbayAspectValues, EbayCategoryAspect) is loaded from the
# offline dump in API/data/ebay/ by scripts/ebay_load_dump_to_db.py and is replaced
# wholesale on a tree-version reload. Operator data (EbayCategoryAspectConfig,
# EbayTypeAspectValue) is never touched by a reload.
#
# See docs/plans/2026-08-05-feat-ebay-aspect-mapping-plan.md


class EbayCategory(Model):

    # Composite PK (marketplace_id, category_id) in SQL. Tortoise has no composite-pk
    # support, so category_id carries pk=True here purely to stop it generating an `id`
    # column that the table does not have. Always filter on marketplace_id as well.
    category_id = fields.CharField(pk=True, max_length=32, description="eBay's own category id")
    marketplace_id = fields.CharField(max_length=20, default="EBAY_US")
    tree_version = fields.CharField(max_length=20)
    name = fields.CharField(max_length=255, description="Leaf name, e.g. 'Dress Shirts'")
    path = fields.JSONField(default=list, description="Ancestor chain, root first")
    is_leaf = fields.BooleanField(default=True)

    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "pm_ebay_categories"

    def __str__(self):
        return f"EbayCategory({self.category_id}: {self.name})"


class EbayAspectValues(Model):

    # Content hash of the value list. 39,753 distinct lists cover 9.1M values, so the
    # lists are stored once and referenced rather than repeated per (category, aspect).
    values_id = fields.CharField(pk=True, max_length=32)
    values_json = fields.JSONField(description="The allowed-value list")
    # Denormalised so callers can choose inline options vs typeahead without reading the
    # JSONB. 91.5% of lists hold <= 200 values; the largest holds 79,116.
    value_count = fields.IntField()

    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "pm_ebay_aspect_values"

    def __str__(self):
        return f"EbayAspectValues({self.values_id}: {self.value_count} values)"


class EbayCategoryAspect(Model):

    id = fields.UUIDField(pk=True, default=uuid.uuid4)
    marketplace_id = fields.CharField(max_length=20, default="EBAY_US")
    category_id = fields.CharField(max_length=32)
    tree_version = fields.CharField(max_length=20)
    aspect_name = fields.CharField(max_length=255)

    is_required = fields.BooleanField(default=False, description="eBay's aspectRequired")
    mode = fields.CharField(max_length=20, description="SELECTION_ONLY or FREE_TEXT")
    data_type = fields.CharField(max_length=20, description="STRING, NUMBER or DATE")
    cardinality = fields.CharField(max_length=20, description="SINGLE or MULTI")
    usage = fields.CharField(max_length=20, null=True, description="RECOMMENDED or OPTIONAL")
    # eBay's aspectMaxLength, present on only 11% of aspects. There is no corresponding
    # minimum: eBay publishes no lower bound and no numeric bounds at all.
    max_length = fields.IntField(null=True)
    variations = fields.BooleanField(default=False)
    values_id = fields.CharField(max_length=32, null=True)
    sort_order = fields.IntField(default=0)
    # eBay's aspectConstraint verbatim. The typed columns above are lifted out for querying;
    # this keeps every other key so one eBay adds later survives the load rather than being
    # dropped at compaction, which is what happened to aspectMaxLength and four others.
    constraint_json = fields.JSONField(default=dict)
    # Hash of the eBay-derived definition at load time, for detecting what moved under an
    # operator's configuration after a reload. Same primitive as
    # InternalPlatformState.desired_hash.
    definition_hash = fields.CharField(max_length=32, null=True)

    class Meta:
        table = "pm_ebay_category_aspects"
        unique_together = (("marketplace_id", "category_id", "aspect_name"),)
        ordering = ["sort_order"]

    def __str__(self):
        return f"EbayCategoryAspect({self.category_id}.{self.aspect_name})"


class EbayAspectSettings(Model):

    # Keyed on the ASPECT NAME, not on (category, aspect). eBay is consistent about a given
    # name almost everywhere: across the categories a Lux type maps to, data_type and
    # aspectApplicableTo never differ between categories, and mode/cardinality/required
    # differ for only 22 of 125 shared names. What is not consistent is the allowed values
    # (59 of 125 differ, and Brand has 61 distinct lists across 62 categories), so values
    # are never stored here and are always resolved per category.
    #
    # Every column below is something eBay does not publish. Nothing eBay-derived is stored,
    # so a tree reload cannot be masked by a stale operator copy.
    id = fields.UUIDField(pk=True, default=uuid.uuid4)
    marketplace_id = fields.CharField(max_length=20, default="EBAY_US")
    aspect_name = fields.CharField(max_length=255)

    enabled = fields.BooleanField(default=True)
    source = fields.CharField(
        max_length=20,
        default="type_based",
        description=(
            "Where the value comes from: 'mapped_field' (from the listing, never on the "
            "form), 'form' (per-listing field), 'type_based' (once per product type)"
        ),
    )
    mapped_field = fields.CharField(max_length=255, null=True, description="Template field name")
    mapped_table = fields.CharField(max_length=255, null=True)
    mapped_column = fields.CharField(max_length=255, null=True)

    display_name = fields.CharField(max_length=255, null=True)
    # The column this aspect becomes in the SellerCloud import file (Size -> EbaySize).
    # Free text: no convention is settled yet. NULL means "not set" and the service derives
    # a default at read time, so a later bulk map can tell defaults from operator edits.
    sellercloud_field = fields.CharField(max_length=255, null=True)
    ai_tagging = fields.BooleanField(default=False)
    ui_size = fields.IntField(null=True)
    # eBay publishes no minimum and no pattern of any kind. `max` is absent on purpose: the
    # only bound eBay gives is aspectMaxLength, which is read from the reference row.
    min_length = fields.IntField(null=True)
    regex = fields.TextField(null=True)
    # Aspect-wide default, every category. Nothing sets it today; the resolver falls back
    # to it when the category below has no entry of its own.
    default_value = fields.JSONField(null=True)
    # The one field on this row that is NOT aspect level: {"15687": "Men"}, keyed by eBay
    # category id. Scalar for SINGLE cardinality, array for MULTI. Held here rather than in
    # a table of its own because this row is already the operator's half of an aspect and
    # is already exempt from the wholesale replacement a tree reload performs on the
    # reference tables. Written one key at a time with jsonb_set, never read-modify-write.
    category_defaults = fields.JSONField(default=dict)

    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "pm_ebay_aspect_settings"
        unique_together = (("marketplace_id", "aspect_name"),)

    def __str__(self):
        return f"EbayAspectSettings({self.aspect_name}={self.source})"


class EbayReferenceLoad(Model):

    # One row per reference load. Until this existed a reload printed its report to stdout
    # and lost it, so nothing could tell an operator what had moved beneath their settings.
    id = fields.UUIDField(pk=True, default=uuid.uuid4)
    marketplace_id = fields.CharField(max_length=20)
    tree_version_from = fields.CharField(max_length=20, null=True)
    tree_version_to = fields.CharField(max_length=20)
    categories = fields.IntField(default=0)
    aspects = fields.IntField(default=0)
    value_lists = fields.IntField(default=0)
    added = fields.IntField(default=0)
    removed = fields.IntField(default=0)
    changed = fields.IntField(default=0)
    status = fields.CharField(max_length=20, default="completed")
    refused_reason = fields.TextField(null=True)
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "pm_ebay_reference_loads"
        ordering = ["-created_at"]


class EbayReferenceLoadChange(Model):

    # Only changes touching a CONFIGURED aspect. A full tree diff would be hundreds of
    # thousands of rows and nobody would read it.
    id = fields.UUIDField(pk=True, default=uuid.uuid4)
    load = fields.ForeignKeyField("models.EbayReferenceLoad", related_name="changes")
    category_id = fields.CharField(max_length=32)
    aspect_name = fields.CharField(max_length=255)
    verb = fields.CharField(max_length=24)
    was = fields.TextField(null=True)
    now_value = fields.TextField(null=True)
    acknowledged = fields.BooleanField(default=False)
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "pm_ebay_reference_load_changes"


class EbayTypeAspectValue(Model):

    # Keyed on (Lux product type, eBay category). The type is in the key because many Lux
    # types share one eBay category and the aspects that distinguish them would otherwise
    # collide. The category is in the key so that remapping a type to a different category
    # yields an empty set (the submit gate asks again) instead of carrying stale answers
    # onto a different aspect set.
    id = fields.UUIDField(pk=True, default=uuid.uuid4)
    product_type_id = fields.UUIDField(description="listingoptions_types.id")
    marketplace_id = fields.CharField(max_length=20, default="EBAY_US")
    category_id = fields.CharField(max_length=32)
    aspect_name = fields.CharField(max_length=255)
    value = fields.JSONField(description="Scalar for SINGLE cardinality, array for MULTI")

    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "pm_ebay_type_aspect_values"
        unique_together = (
            ("product_type_id", "marketplace_id", "category_id", "aspect_name"),
        )

    def __str__(self):
        return f"EbayTypeAspectValue({self.product_type_id}.{self.aspect_name})"
