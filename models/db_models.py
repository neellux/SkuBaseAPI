from enum import StrEnum
from typing import Any, Dict, List

import uuid

from tortoise import fields
from tortoise.models import Model


class SubmissionStatus(StrEnum):
    QUEUED = "queued"
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCESS = "success"
    FAILED = "failed"


TERMINAL_STATUSES = {SubmissionStatus.SUCCESS, SubmissionStatus.FAILED}
IN_FLIGHT_STATUSES = {
    SubmissionStatus.QUEUED,
    SubmissionStatus.PENDING,
    SubmissionStatus.PROCESSING,
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

    ai_response = fields.JSONField(null=True, description="AI generated response or suggestions")
    ai_description = fields.TextField(null=True, description="AI generated description")
    original_description = fields.TextField(
        null=True, description="Original SellerCloud description"
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
        description="Submission status: pending, success, failed",
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
        description="Platform-specific settings: {platform_id: {enabled, price_multiplier, shipping}}",
    )

    platforms = fields.JSONField(
        default=["sellercloud", "grailed"],
        description="List of enabled platform IDs for submission",
    )

    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "app_settings"

    def __str__(self):
        return f"AppSettings({self.id})"
