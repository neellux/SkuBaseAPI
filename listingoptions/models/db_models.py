from tortoise.models import Model
from tortoise import fields
from tortoise.contrib.postgres.indexes import GinIndex
import uuid
from enum import Enum


class Gender(str, Enum):
    MENS = "Mens"
    WOMENS = "Womens"
    BOYS = "Boys"
    GIRLS = "Girls"
    UNISEX = "Unisex"
    DOES_NOT_APPLY = "Does Not Apply"


class ReportingCategory(str, Enum):
    JEANS = "Jeans"
    HOODIES_SWEATSHIRTS = "Hoodies & Sweatshirts"
    T_SHIRTS = "T-Shirts"
    BOTTOMS = "Bottoms"
    OUTERWEAR = "Outerwear"
    TOPS = "Tops"
    SUITING = "Suiting"
    ACCESSORIES = "Accessories"
    SNEAKERS = "Sneakers"
    FOOTWEAR = "Footwear"
    BAGS = "Bags"


class Platform(Model):

    id = fields.CharField(max_length=50, pk=True)
    name = fields.CharField(max_length=255, null=False)
    icon = fields.TextField(null=True)
    icon_mime_type = fields.CharField(max_length=50, null=True)

    class Meta:
        table = "listingoptions_platforms"


class Schema(Model):

    table = fields.CharField(max_length=255, pk=True)
    column_schema = fields.JSONField(default=list)
    list_schema = fields.JSONField(default=list)
    display_name = fields.CharField(max_length=255, null=True)
    primary_business_column = fields.CharField(max_length=255, null=True)
    list_type = fields.CharField(max_length=50, default="default")
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "listingoptions_schema"


class BaseTableModel(Model):

    id = fields.UUIDField(pk=True, default=uuid.uuid4)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        abstract = True


class DefaultListModel(Model):

    primary_id = fields.UUIDField(null=True)
    platform_value = fields.CharField(max_length=255)
    platform_id = fields.ForeignKeyField("listingoptions.Platform", related_name="default_lists")
    primary_table_column = fields.CharField(max_length=255)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        abstract = True


class SizingListModel(Model):

    sizing_scheme = fields.CharField(max_length=255)
    platform_value = fields.CharField(max_length=255)
    platform = fields.ForeignKeyField("listingoptions.Platform", related_name="sizing_lists")
    value = fields.CharField(max_length=255)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        abstract = True


class SizingScheme(Model):

    id = fields.UUIDField(pk=True, default=uuid.uuid4)
    sizing_scheme = fields.CharField(
        max_length=50,
        index=True,
        description="The name of the sizing scheme (e.g., 'Alpha Sizes', 'EU Shoe Sizes')",
    )
    size = fields.TextField(
        description="A specific size within the scheme (e.g., 'S', 'M', 'L', '42', 'UK 8')"
    )
    order = fields.IntField(
        column_name="order",
        description="The display order of this size within its scheme.",
    )
    sizing_types = fields.JSONField(
        null=True,
        description="List of applicable types for this sizing scheme (e.g., ['Tops', 'Bottoms'])",
    )

    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "listingoptions_sizing_schemes"
        unique_together = (("sizing_scheme", "size"),)
        ordering = ["sizing_scheme", "order"]
        indexes = [GinIndex(fields={"sizing_types"})]

    def __str__(self):
        return f"{self.sizing_scheme} - {self.size} (Order: {self.order})"


class SizingList(Model):

    id = fields.UUIDField(pk=True, default=uuid.uuid4)
    sizing_scheme_entry = fields.ForeignKeyField(
        "listingoptions.SizingScheme",
        related_name="platform_mappings",
        description="The specific size entry (e.g., 'Alpha Sizes - M') this mapping refers to.",
        on_delete=fields.CASCADE,
    )
    platform = fields.ForeignKeyField(
        "listingoptions.Platform",
        related_name="sizing_list_mappings",
        description="The platform this mapping is for.",
        on_delete=fields.CASCADE,
    )
    platform_value = fields.CharField(
        max_length=255,
        description="The platform-specific value for the given size (e.g., 'Medium', '42 EU').",
    )
    sizing_type = fields.CharField(
        max_length=255,
        description="The sizing type (e.g., 'Tops', 'Bottoms') for this mapping.",
        index=True,
    )

    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "listingoptions_sizing_lists"
        unique_together = (("sizing_scheme_entry", "platform", "sizing_type"),)
        ordering = [
            "sizing_scheme_entry__sizing_scheme",
            "sizing_scheme_entry__order",
            "platform__name",
            "platform_value",
        ]

    def __str__(self):
        return f"SizingList Entry ID: {self.id} - SchemeEntry: {self.sizing_scheme_entry_id} - Platform: {self.platform_id} -> {self.platform_value}"


class ConfigSpoValueList(Model):

    id = fields.UUIDField(pk=True, default=uuid.uuid4)
    value_list = fields.CharField(
        max_length=100,
        description="Human-readable name of the ShopSimon value list (e.g. 'Mens Clothing Bottoms Size').",
    )
    list_code = fields.CharField(
        max_length=100,
        index=True,
        description="ShopSimon list slug (e.g. 'mens-clothing-bottoms-size-values').",
    )
    label = fields.CharField(
        max_length=255,
        description="Display label as it appears in SPO data (e.g. '30\"\" Waist').",
    )
    value_code = fields.CharField(
        max_length=100,
        description="Value code SPO's import file expects (e.g. '30-inch').",
    )

    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "config_spo_value_lists"

    def __str__(self) -> str:
        return f"{self.list_code}: {self.label} -> {self.value_code}"


class ParentType(Model):

    id = fields.UUIDField(pk=True, default=uuid.uuid4)
    division = fields.CharField(max_length=255, index=True)
    dept_code = fields.IntField(index=True)
    dept = fields.CharField(max_length=255, index=True)
    class_code = fields.IntField(unique=True, index=True)
    class_name = fields.CharField(max_length=255, column_name="class", index=True)
    gender = fields.CharEnumField(Gender, max_length=20, index=True)
    reporting_category = fields.CharEnumField(ReportingCategory, max_length=50, index=True)

    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "listingoptions_types_parents"
        unique_together = (
            ("division", "dept", "class_name"),
            ("division", "dept_code", "class_code"),
        )
        ordering = ["dept_code", "division", "class_code"]

    def __str__(self):
        return f"{self.division} -> {self.dept} -> {self.class_name}"
