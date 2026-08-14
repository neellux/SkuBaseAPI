from pydantic import BaseModel, Field, validator, model_validator
from typing import List, Optional, Union, Literal, Dict, Any
from datetime import datetime
from decimal import Decimal
import re


class PlatformMapping(BaseModel):

    platform_id: str = Field(
        ..., description="Platform identifier (sellercloud, ebay, amazon, etc)"
    )
    field_id: str = Field(..., description="Field ID/name in the platform's API")
    is_custom: bool = Field(
        default=False,
        description="For SellerCloud: if true, send as CustomColumn instead of AdvancedInfo",
    )
    platform_tags: Optional[List[str]] = Field(
        default=None, description="Platform-specific tags for this field mapping"
    )


class FieldDefinition(BaseModel):

    name: str = Field(..., description="Field name in the DB (snake_case for product_info column)")
    display_name: str = Field(..., description="Label shown in the UI")
    type: Literal[
        "text",
        "number",
        "bool",
        "text_list",
        "rich_text",
    ] = Field(..., description="Data type")
    order: int = Field(default=999, description="Display order of the field, lower is first.")
    is_required: bool = Field(default=False, description="Form-level requirement")
    is_unique: bool = Field(default=False, description="Ensures values in the field are unique")

    display_in_form: bool = Field(
        default=True,
        description="Whether to display this field in form inputs",
    )
    default: Optional[Union[str, int, float, bool, List[str]]] = Field(
        None, description="Default value"
    )
    min: Optional[Union[int, float]] = Field(None, description="Minimum value/length")
    max: Optional[Union[int, float]] = Field(None, description="Maximum value/length")
    regex: Optional[str] = Field(None, description="Regex pattern for text fields")
    regex_error_message: Optional[str] = Field(None, description="Regex error message")

    options: Optional[List[Union[str, int, float]]] = Field(
        None, description="Predefined options for 'text' or 'number' type"
    )
    multiselect: bool = Field(
        False, description="Allow multiple selections for 'text' type with options"
    )
    platform_tags: Optional[List[str]] = Field(
        None, description="Tags from the source platform, e.g., SellerCloud"
    )
    platforms: Optional[List[PlatformMapping]] = Field(
        default=None,
        description="Platform-specific field mappings for syncing to external systems (SellerCloud, eBay, etc)",
    )
    ai_tagging: bool = Field(
        default=False, description="Whether AI tagging is enabled for this field"
    )
    use_raw_fallback: bool = Field(
        default=True,
        description=(
            "When no per-platform template is configured for this field, fall back to the field's "
            "raw mapped value (today's pass-through behaviour). When false, the field is omitted."
        ),
    )
    ui_size: Optional[int] = Field(
        default=12, description="MUI grid size for the field in the UI (1-12)"
    )

    mapped_table: Optional[str] = Field(
        None, description="Source table name from external listing options API"
    )
    mapped_column: Optional[str] = Field(
        None, description="Source column name from external listing options API"
    )

    @validator("ui_size")
    def validate_ui_size(cls, v):
        if v is not None and not (1 <= v <= 12):
            raise ValueError("ui_size must be between 1 and 12")
        return v

    @validator("name")
    def validate_name(cls, v):
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", v):
            raise ValueError("Field name must be a valid identifier")
        return v

    @validator("regex")
    def validate_regex(cls, v):
        if v:
            try:
                re.compile(v)
            except re.error:
                raise ValueError("Invalid regex pattern")
        return v

    @validator("is_unique")
    def check_is_unique(cls, v, values):
        if v and values.get("multiselect"):
            raise ValueError("is_unique cannot be true for multiselect fields")
        return v

    @validator("options")
    def check_options(cls, v, values):
        if v and values.get("type") not in ["text", "text_list", "number"]:
            raise ValueError(
                "options are only supported for 'text', 'text_list', and 'number' types"
            )
        return v

    @validator("multiselect")
    def check_multiselect(cls, v, values):
        if v:
            if not values.get("options"):
                raise ValueError("multiselect requires having options")
            if values.get("type") != "text_list":
                raise ValueError("multiselect requires the type to be 'text_list'")
        return v

    @model_validator(mode="after")
    def check_platform_list_constraints(self) -> "FieldDefinition":
        if self.type == "platform_list":
            if self.is_unique:
                raise ValueError("is_unique is not applicable for platform_list type")
            if self.options:
                raise ValueError("options are not applicable for platform_list type")
            if self.multiselect:
                raise ValueError("multiselect is not applicable for platform_list type")
            if self.min is not None:
                raise ValueError("min is not applicable for platform_list type")
            if self.max is not None:
                raise ValueError("max is not applicable for platform_list type")
            if self.regex is not None:
                raise ValueError("regex is not applicable for platform_list type")
            if self.default is not None and self.default != []:
                raise ValueError("Default for platform_list can only be an empty list or None.")
        return self


class CreateTemplateRequest(BaseModel):

    name: str = Field(..., description="Template name (database identifier)")
    display_name: str = Field(..., description="Human-readable name for UI")
    description: Optional[str] = Field(None, description="Optional template description")
    field_definitions: List[FieldDefinition] = Field(
        default=[], description="Initial field definitions"
    )

    @validator("name")
    def validate_name(cls, v):
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", v):
            raise ValueError("Template name must be a valid identifier")
        return v.lower()


class UpdateTemplateRequest(BaseModel):

    display_name: Optional[str] = Field(None, description="Human-readable name for UI")
    description: Optional[str] = Field(None, description="Template description")
    is_active: Optional[bool] = Field(None, description="Whether template is active")


class UpdateTemplateWithFieldsRequest(BaseModel):

    display_name: Optional[str] = Field(None, description="Human-readable name for UI")
    description: Optional[str] = Field(None, description="Template description")
    is_active: Optional[bool] = Field(None, description="Whether template is active")
    field_definitions: Optional[List[FieldDefinition]] = Field(
        None, description="Complete field definitions list"
    )


class AddFieldToTemplateRequest(BaseModel):

    template_name: str = Field(..., description="Template name")
    field: FieldDefinition = Field(..., description="Field definition to add")


class UpdateTemplateFieldRequest(BaseModel):

    template_name: str = Field(..., description="Template name")
    field_name: str = Field(..., description="Name of field to update")
    update_data: Dict[str, Any] = Field(..., description="Field properties to update")


class ReorderTemplateFieldsRequest(BaseModel):

    template_name: str = Field(..., description="Template name")
    field_order: List[str] = Field(..., description="Ordered list of field names")


class TemplateResponse(BaseModel):

    id: str
    name: str
    display_name: str
    description: Optional[str]
    field_definitions: List[Dict[str, Any]]
    field_count: int
    is_active: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class ProductFieldSearchResponse(BaseModel):

    id: str = Field(..., description="Field ID")
    tags: List[str] = Field(default=[], description="Field tags (e.g., 'custom')")
    display_name: Optional[str] = Field(None, description="Suggested display name")

    class Config:
        from_attributes = True


class CreateListingRequest(BaseModel):

    product_id: str = Field(..., description="Product ID from SellerCloud")
    info_product_id: Optional[str] = Field(
        None, description="Full SellerCloud product ID, including variations"
    )
    assigned_to: Optional[str] = Field(None, description="User ID assigned to this listing")
    template_id: Optional[str] = Field(None, description="Template ID for the listing form")
    data: Dict[str, Any] = Field(default_factory=dict, description="Initial form data")


class UpdateListingRequest(BaseModel):

    assigned_to: Optional[str] = Field(None, description="User ID assigned to this listing")
    data: Optional[Dict[str, Any]] = Field(None, description="Form data")
    # Served here so the listing page does not need a round trip of its own before it can
    # ask for the schema. The client cannot know a type's default category without asking,
    # and the schema request cannot be issued until it does -- which would insert a fourth
    # serialized wave into every listing open, paid even on schema cache hits.
    ebay_categories: Optional[List[Dict[str, Any]]] = Field(
        None,
        description=(
            "eBay categories this listing's product type maps to, default first. "
            "Empty when the type maps nowhere."
        ),
    )
    ai_response: Optional[Dict[str, Any]] = Field(None, description="AI response data")
    ai_description: Optional[str] = Field(None, description="AI generated description")
    submitted: Optional[bool] = Field(None, description="Submission status")
    submitted_by: Optional[str] = Field(None, description="User ID who submitted this listing")
    title_auto_update: Optional[bool] = Field(
        None, description="Whether the title still tracks the title template"
    )


class SubmitListingRequest(BaseModel):

    platforms: Optional[List[str]] = Field(
        None, description="List of platform IDs to submit to. If None, uses app_settings defaults"
    )


class SaveSizeMappingRequest(BaseModel):

    sizing_scheme_entry_id: str = Field(..., description="UUID of the sizing_schemes entry")
    platform_id: str = Field(..., description="Platform ID (e.g. 'grailed')")
    platform_value: Optional[str] = Field(
        None, description="Platform-specific size value. Null = delete mapping."
    )
    sizing_type: Optional[str] = Field(None, description="Sizing type (e.g. 'Shoes', 'Clothing')")


class ListingResponse(BaseModel):

    id: str = Field(..., description="Listing UUID")
    product_id: str = Field(..., description="Product ID from SellerCloud")
    info_product_id: Optional[str] = Field(
        None, description="Full SellerCloud product ID, including variations"
    )
    assigned_to: Optional[str] = Field(None, description="User ID assigned to this listing")
    assigned_to_name: Optional[str] = Field(None, description="Name of assigned user")
    data: Dict[str, Any] = Field(..., description="Form data")
    # Served here so the listing page does not need a round trip of its own before it can
    # ask for the schema. The client cannot know a type's default category without asking,
    # and the schema request cannot be issued until it does -- which would insert a fourth
    # serialized wave into every listing open, paid even on schema cache hits.
    ebay_categories: Optional[List[Dict[str, Any]]] = Field(
        None,
        description=(
            "eBay categories this listing's product type maps to, default first. "
            "Empty when the type maps nowhere."
        ),
    )
    ai_response: Optional[Dict[str, Any]] = Field(None, description="AI response data")
    ai_description: Optional[str] = Field(None, description="AI generated description")
    original_description: Optional[str] = Field(
        None, description="Original SellerCloud description"
    )
    original_title: Optional[str] = Field(
        None, description="Title at creation, before the title template rewrote it"
    )
    title_auto_update: bool = Field(
        True,
        description=(
            "Whether the title still tracks the title template. False once an operator has "
            "taken the field over, which is what stops the template overwriting their edit "
            "the next time the listing is opened."
        ),
    )
    submitted: bool = Field(..., description="Whether listing is submitted")
    submitted_at: Optional[datetime] = Field(None, description="Submission timestamp")
    submitted_by: Optional[str] = Field(None, description="User ID who submitted this listing")
    submitted_by_name: Optional[str] = Field(None, description="Name of user who submitted")
    submitted_platforms: Optional[List[str]] = Field(
        None, description="List of platform IDs with successful submissions"
    )
    upload_status: str = Field("pending", description="Image upload status: pending or uploaded")
    created_by: str = Field(..., description="Creator user ID")
    created_by_name: Optional[str] = Field(None, description="Name of creator user")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")

    class Config:
        from_attributes = True


class ProductConfirmationData(BaseModel):

    product: Dict[str, Any] = Field(..., description="Product data from SellerCloud")
    existing_listing_id: Optional[str] = Field(
        None,
        description=(
            "ID of the listing this product already has, submitted or not, if one exists. "
            "The caller opens it instead of creating a second listing for the same parent."
        ),
    )


class ListingSchemaResponse(BaseModel):

    json_schema: Dict[str, Any] = Field(..., description="JSON Schema for the form")
    ui_schema: Dict[str, Any] = Field(..., description="UI Schema for the form")
    template_info: Dict[str, Any] = Field(..., description="Template metadata")


class BatchProductConfirmationData(BaseModel):

    product_id: str = Field(..., description="Product ID from request")
    product: Optional[Dict[str, Any]] = Field(None, description="Product data from SellerCloud")
    existing_listing_id: Optional[str] = Field(
        None, description="The ID of an existing draft listing if one is found"
    )
    error: Optional[str] = Field(None, description="Error message if product not found or invalid")
    status: Literal["success", "existing_draft", "not_found", "error"] = Field(
        ..., description="Status of the product confirmation"
    )


class BatchConfirmationRequest(BaseModel):

    product_ids: List[str] = Field(
        ..., min_items=1, max_items=1000, description="List of product IDs to confirm"
    )

    @validator("product_ids")
    def validate_product_ids(cls, v):
        cleaned_ids = [pid.strip() for pid in v if pid.strip()]
        if not cleaned_ids:
            raise ValueError("At least one valid product ID is required")
        return cleaned_ids


class BatchConfirmationResponse(BaseModel):

    products: List[BatchProductConfirmationData] = Field(
        ..., description="List of product confirmation data"
    )
    total_count: int = Field(..., description="Total number of products requested")
    success_count: int = Field(..., description="Number of successfully found products")
    existing_draft_count: int = Field(..., description="Number of products with existing drafts")
    error_count: int = Field(..., description="Number of products with errors")


class CreateBatchRequest(BaseModel):

    product_ids: List[str] = Field(
        ...,
        min_items=1,
        max_items=1000,
        description="List of product IDs to include in the batch",
    )
    comment: Optional[str] = Field(None, description="Comment for the batch")
    assigned_to: Optional[str] = Field(None, description="User ID assigned to this batch")
    priority: Literal["low", "medium", "high"] = Field(
        default="medium", description="Batch priority"
    )
    photography_batch_id: Optional[int] = Field(None, description="Reference to photography batch")

    @validator("product_ids")
    def validate_product_ids(cls, v):
        cleaned_ids = [pid.strip() for pid in v if pid.strip()]
        if not cleaned_ids:
            raise ValueError("At least one valid product ID is required")
        return cleaned_ids


class UpdateBatchRequest(BaseModel):

    comment: Optional[str] = Field(None, description="Comment for the batch")
    assigned_to: Optional[str] = Field(None, description="User ID assigned to this batch")
    priority: Optional[Literal["low", "medium", "high"]] = Field(None, description="Batch priority")


class BatchResponse(BaseModel):

    id: int = Field(..., description="Batch ID")
    comment: Optional[str] = Field(None, description="Batch description/comment")
    assigned_to: Optional[str] = Field(None, description="User ID assigned to this batch")
    assigned_to_name: Optional[str] = Field(None, description="Name of assigned user")
    priority: str = Field(..., description="Batch priority")
    status: Literal["new", "in_progress", "completed"] = Field(..., description="Batch status")
    created_by: str = Field(..., description="Creator user ID")
    created_by_name: Optional[str] = Field(None, description="Name of creator user")
    total_listings: int = Field(..., description="Total number of listings in batch")
    submitted_listings: int = Field(..., description="Number of submitted listings")
    photography_batch_id: Optional[int] = Field(None, description="Reference to photography batch")
    progress_percentage: float = Field(..., description="Completion percentage")
    total_value: Decimal = Field(
        Decimal(0),
        description="Merchandise value snapshot at creation (physical qty x SitePrice)",
    )
    value_computed_at: Optional[datetime] = Field(
        None,
        description="When the value snapshot was taken; null means it has not been computed, "
        "which is not the same as a batch genuinely worth 0",
    )
    product_values: Dict[str, Any] = Field(
        default_factory=dict,
        description="Per-parent-SKU breakdown behind total_value: "
        "{sku: {value, qty, children, priced}}. Drives the card's value hover card; "
        "thumbnails are built client-side from the SKU, so no image data travels here.",
    )
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")
    listings: List[ListingResponse] = Field(default=[], description="Listings in this batch")

    class Config:
        from_attributes = True


class BatchListResponse(BaseModel):

    id: int = Field(..., description="Batch ID")
    comment: Optional[str] = Field(None, description="Batch description/comment")
    assigned_to: Optional[str] = Field(None, description="User ID assigned to this batch")
    assigned_to_name: Optional[str] = Field(None, description="Name of assigned user")
    priority: str = Field(..., description="Batch priority")
    status: Literal["new", "in_progress", "completed"] = Field(..., description="Batch status")
    total_listings: int = Field(..., description="Total number of listings in batch")
    submitted_listings: int = Field(..., description="Number of submitted listings")
    photography_batch_id: Optional[int] = Field(None, description="Reference to photography batch")
    progress_percentage: float = Field(..., description="Completion percentage")
    total_value: Decimal = Field(
        Decimal(0),
        description="Merchandise value snapshot at creation (physical qty x SitePrice)",
    )
    value_computed_at: Optional[datetime] = Field(
        None,
        description="When the value snapshot was taken; null means it has not been computed, "
        "which is not the same as a batch genuinely worth 0",
    )
    product_values: Dict[str, Any] = Field(
        default_factory=dict,
        description="Per-parent-SKU breakdown behind total_value: "
        "{sku: {value, qty, children, priced}}. Drives the card's value hover card; "
        "thumbnails are built client-side from the SKU, so no image data travels here.",
    )
    created_at: datetime = Field(..., description="Creation timestamp")

    class Config:
        from_attributes = True


class NextOpenBatchResponse(BaseModel):

    batch: Optional[BatchResponse] = Field(
        None,
        description=(
            "Next open batch with its listings, the same shape as /listings/batch/detail, "
            "so the caller can render it without a second round trip. Null when no other "
            "open batch exists"
        ),
    )
    wrapped: bool = Field(
        False,
        description="True when the walk ran past the oldest open batch and came back to the newest",
    )


class BatchFilterOptionsResponse(BaseModel):

    users: List[Dict[str, str]] = Field(..., description="Available users with id and name")
    priorities: List[str] = Field(..., description="Available priorities")
    statuses: List[str] = Field(..., description="Available statuses")


class ProductFailureDetail(BaseModel):
    product_id: str = Field(..., description="Product ID that failed")
    error_type: str = Field(..., description="Type of error encountered")
    error_message: str = Field(..., description="Detailed error message")


class BatchCreationErrorResponse(BaseModel):
    error: str = Field(..., description="High-level error message")
    total_products: int = Field(..., description="Total products attempted")
    failed_count: int = Field(..., description="Number of failed products")
    failed_products: List[ProductFailureDetail] = Field(
        ..., description="Details of each failed product"
    )
    timestamp: datetime = Field(..., description="When the error occurred")


class ChildProductData(BaseModel):

    id: str = Field(..., description="Full product ID including size variant")
    parent_id: str = Field(..., description="Parent product ID")
    size: str = Field(..., description="Size extracted from product ID")
    is_active: bool = Field(
        True, description="False for a disabled variant, shown read-only in the UI"
    )


class ChildrenResponse(BaseModel):

    children: List[ChildProductData] = Field(..., description="List of child products")
    product_type: Optional[str] = Field(None, description="Product type from SellerCloud")
    sizing_scheme: Optional[str] = Field(
        None, description="Sizing scheme from SellerCloud CustomColumns"
    )


class SizeEntry(BaseModel):

    id: str
    size: str


class SizingSchemeData(BaseModel):

    sizing_scheme: str = Field(..., description="Sizing scheme name")
    sizes: List[str] = Field(..., description="Available sizes in order")
    size_entries: Optional[List[SizeEntry]] = Field(
        None, description="Size entries with IDs for mapping lookups"
    )


class SizingSchemesResponse(BaseModel):

    schemes: List[SizingSchemeData] = Field(..., description="List of available sizing schemes")
    sizing_type: Optional[str] = Field(
        None, description="Sizing type from the ProductType (types.sizing_types)"
    )


class ProductTypeInfoResponse(BaseModel):

    gender: Optional[str] = Field(None, description="Gender from types_parents table")
    item_weight_oz: Optional[float] = Field(
        None, description="Item weight in ounces from types table"
    )


class UpdateSettingsRequest(BaseModel):

    field_templates: Optional[Dict[str, Any]] = Field(
        None,
        description="Per-platform field templates: {platform_id: {field_name: template_string}}",
    )
    strict_template_validation: Optional[bool] = Field(
        None,
        description="When true, reject templates that reference placeholders not in the valid field list.",
    )


class SettingsResponse(BaseModel):

    id: int = Field(..., description="Settings ID")
    field_templates: Dict[str, Any] = Field(
        default_factory=dict,
        description="Per-platform field templates: {platform_id: {field_name: template_string}}",
    )
    strict_template_validation: bool = Field(
        default=False,
        description="Whether save-time strict placeholder validation is enabled.",
    )
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")

    class Config:
        from_attributes = True


class UpdateAppVariablesRequest(BaseModel):

    app_variables: List[Dict[str, Any]] = Field(
        ..., description="Application configuration variables array with id, name, value objects"
    )


class AppVariablesResponse(BaseModel):

    app_variables: List[Dict[str, Any]] = Field(
        ..., description="Application configuration variables array with id, name, value objects"
    )
    updated_at: datetime = Field(..., description="Last update timestamp")

    class Config:
        from_attributes = True


class UpdatePlatformSettingsRequest(BaseModel):
    platform_settings: Dict[str, Any] = Field(
        ..., description="Platform-specific settings dictionary"
    )


class PlatformSettingsResponse(BaseModel):
    platform_settings: Dict[str, Any] = Field(
        default_factory=dict, description="Platform-specific settings"
    )
    updated_at: datetime = Field(..., description="Last update timestamp")

    class Config:
        from_attributes = True


class PlatformMeta(BaseModel):
    id: str = Field(..., description="Platform ID (e.g., 'sellercloud', 'grailed')")
    name: str = Field(..., description="Platform name")
    icon: Optional[str] = Field(None, description="Base64 encoded icon")
    icon_mime_type: Optional[str] = Field(
        None, description="MIME type of the icon (e.g., 'image/svg+xml')"
    )
    settings: Dict[str, Any] = Field(
        default_factory=dict,
        description="Platform settings from app_settings.platform_settings (manual_fallback, allow_resubmit, requires_images, etc.)",
    )


class PlatformMetaResponse(BaseModel):
    platforms: List[PlatformMeta] = Field(default_factory=list)


class EnabledPlatformsResponse(BaseModel):
    platforms: List[str] = Field(default_factory=list, description="List of enabled platform IDs")
    updated_at: datetime = Field(..., description="Last update timestamp")


class UpdateEnabledPlatformsRequest(BaseModel):
    platforms: List[str] = Field(..., description="List of platform IDs to enable")


class ListingSubmissionResponse(BaseModel):
    id: int = Field(..., description="Submission ID")
    listing_id: str = Field(..., description="Listing UUID")
    platform_id: str = Field(..., description="Platform identifier")
    status: Literal["queued", "pending", "processing", "success", "failed"] = Field(
        ..., description="Submission status"
    )
    submitted_by: Optional[str] = Field(None, description="User ID who submitted")
    submitted_by_name: Optional[str] = Field(None, description="Name of user who submitted")
    submitted_at: Optional[datetime] = Field(None, description="When submission was attempted")
    error_display: Optional[str] = Field(None, description="Human-friendly error message")
    platform_status: Optional[str] = Field(None, description="Platform-specific sub-status")
    attempt_number: int = Field(..., description="Attempt number for retries")
    external_id: Optional[list] = Field(None, description="External platform reference ID(s)")
    created_at: datetime = Field(..., description="Record creation timestamp")

    class Config:
        from_attributes = True


class SubmissionSummary(BaseModel):
    total_platforms: int = Field(..., description="Total platforms with submissions")
    successful: int = Field(..., description="Number of successful submissions")
    failed: int = Field(..., description="Number of failed submissions")
    pending: int = Field(..., description="Number of pending submissions")
    platforms: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict, description="Latest status per platform"
    )


class ImportSummary(BaseModel):
    import_id: int = Field(..., description="Platform-side import id (e.g. SPO product_import_id)")
    platform_id: str = Field(..., description="Platform identifier this import belongs to")
    submission_count: int = Field(..., description="Number of ListingSubmissions in this import")
    sku_count: int = Field(
        0, description="Total SKU/product rows across this import's listings"
    )
    file_name: Optional[str] = Field(
        None, description="Uploaded SPO product file name (spo_products_*.xlsx)"
    )
    batch_number: Optional[int] = Field(
        None,
        description="Grailed sequential batch number (1, 2, 3, ...); UI shows it zero-padded to 6 digits",
    )
    status_counts: Dict[str, int] = Field(
        default_factory=dict, description="Count of submissions per status"
    )
    created_at: Optional[datetime] = Field(
        None,
        description="When the import was uploaded to the platform (falls back to earliest submission created_at for legacy imports)",
    )
    updated_at: Optional[datetime] = Field(None, description="Latest submission updated_at")


class SubmissionsDashboardResponse(BaseModel):
    platform_id: str = Field(..., description="Platform identifier")
    pending_count: int = Field(..., description="ListingSubmissions awaiting upload")
    processing_count: int = Field(..., description="ListingSubmissions in flight")
    failed_count: int = Field(..., description="ListingSubmissions in terminal failure")
    success_count: int = Field(..., description="ListingSubmissions completed successfully")
    min_batch_size: int = Field(
        ...,
        description="Per-platform minimum pending count before auto-batch from platform_settings",
    )
    imports: List[ImportSummary] = Field(default_factory=list)
    total_imports: int = Field(0, description="Total imports for this platform")
    page: int = Field(1, description="Current page (1-indexed)")
    page_size: int = Field(50, description="Imports per page")
    platform_pending_counts: Dict[str, int] = Field(
        default_factory=dict,
        description="Pending submission count per platform_id, for tab badges",
    )


class ImportListingDetail(BaseModel):
    submission_id: int
    listing_id: Optional[str] = None
    product_id: Optional[str] = None
    title: Optional[str] = None
    status: str
    platform_status: Optional[str] = None
    error_display: Optional[str] = None
    sku_errors: Optional[Dict[str, str]] = Field(
        None,
        description="Per-child-SKU error map ({sku: error}); lets the UI group "
        "identical errors across sizes instead of repeating them.",
    )
    skus: List[str] = Field(
        default_factory=list,
        description="Child SKUs submitted for this listing (data.child_size_overrides keys)",
    )
    updated_skus: List[str] = Field(
        default_factory=list,
        description="SKUs that were already on the sheet and refreshed in place "
        "(grailed updated_references), rather than added as new rows",
    )
    updated_at: Optional[datetime] = None
    reviewed_at: Optional[datetime] = Field(
        None,
        description="Set when a failed submission was manually reviewed. Such a row "
        "reports status 'success'; this field is what marks it as manually resolved",
    )


class ImportDetailResponse(BaseModel):
    import_id: int
    platform_id: str
    batch_number: Optional[int] = Field(
        None, description="Grailed sequential batch number (shown zero-padded to 6 digits)"
    )
    submissions: List[ImportListingDetail] = Field(default_factory=list)
    status_counts: Dict[str, int] = Field(default_factory=dict)


class CreateBatchResponse(BaseModel):
    platform: str = Field(..., description="Platform identifier the batch was created for")
    submission_count: int = Field(..., description="Number of submissions included in the batch")
    product_import_id: Optional[int] = Field(
        None, description="Platform-side import id, if a batch was uploaded"
    )


class AddProductRequest(BaseModel):

    child_sku: str = Field(..., description="Full child SKU (e.g., PRD-001/S)")
    title: str = Field(..., description="Product title")
    upc: Optional[str] = Field(None, description="UPC barcode (8, 12, or 13 digits)")
    mpn: Optional[str] = Field(None, description="Manufacturer Part Number")
    brand_code: Optional[str] = Field(None, description="Brand code")
    type_code: Optional[str] = Field(None, description="Product type code")
    serial_number: Optional[int] = Field(None, description="Serial number (1-9999)")
    company_code: int = Field(..., description="Company code (required)")

    @validator("child_sku")
    def validate_child_sku_format(cls, v):
        if "/" not in v:
            raise ValueError(
                "Child SKU must contain '/' to separate parent and size (e.g., PRD-001/S)"
            )
        parts = v.split("/", 1)
        if not parts[0] or not parts[1]:
            raise ValueError("Both parent SKU and size must be non-empty")
        return v


class AddProductResponse(BaseModel):

    success: bool
    child_sku: str
    parent_sku: Optional[str] = None
    size: Optional[str] = None
    is_primary: bool = False
    parent_created: bool = False
    errors: Optional[List[Dict[str, Any]]] = None


class AddSizeRequest(BaseModel):

    parent_sku: str = Field(..., description="Parent product SKU (must not contain '/')")
    size: str = Field(..., min_length=1, description="Size value (e.g., '32', 'M', 'XL')")
    upc: str = Field(..., description="UPC barcode (8, 12, or 13 digits)")
    cost_price: float = Field(..., gt=0, description="Cost price (must be > 0)")

    @validator("parent_sku")
    def validate_parent_sku(cls, v):
        if "/" in v:
            raise ValueError("Parent SKU must not contain '/'")
        return v

    @validator("upc")
    def validate_upc(cls, v):
        if not re.match(r"^\d{8}$|^\d{12}$|^\d{13}$", v):
            raise ValueError("UPC must be 8, 12, or 13 digits")
        return v


class AddSizeResponse(BaseModel):

    success: bool
    new_child_sku: Optional[str] = None
    parent_sku: Optional[str] = None
    size: Optional[str] = None
    error: Optional[str] = None


class NextUpcResponse(BaseModel):

    success: bool
    upc: Optional[str] = None
    error: Optional[str] = None


class CostPriceResponse(BaseModel):

    success: bool
    cost_price: Optional[float] = None
    error: Optional[str] = None


class CheckBrandMpnResponse(BaseModel):

    success: bool
    exists: bool = False
    sku: Optional[str] = None
    error: Optional[str] = None


class CountriesResponse(BaseModel):

    success: bool
    countries: List[str] = []
    error: Optional[str] = None


class PlatformLink(BaseModel):

    platform_id: str = Field(..., description="listingoptions_platforms.id, for the icon")
    name: str
    url: str = Field(..., description="Shopify admin product URL")
    online: bool = Field(
        ...,
        description=(
            "False when the product is not currently served by that storefront, which on "
            "1nventory means sold out and unpublished. The admin URL still resolves; this "
            "is a status hint the UI marks the icon with."
        ),
    )


class PlatformLinksResponse(BaseModel):

    success: bool
    parent_sku: Optional[str] = None
    links: List[PlatformLink] = []
    error: Optional[str] = None


class CreateSkuSize(BaseModel):

    size: str = Field(..., min_length=1, description="Size value")
    unit_price: float = Field(..., gt=0, description="Unit cost (SiteCost)")
    upc: Optional[str] = Field(
        None, description="Manual UPC (12-13 digits); blank/omitted means auto-generate"
    )

    @validator("upc")
    def validate_upc(cls, v):
        if v is None:
            return v
        digits = re.sub(r"[^0-9]", "", v)
        if digits == "":
            return None
        if not re.fullmatch(r"\d{12,13}", digits):
            raise ValueError("UPC must be 12 or 13 digits")
        # GS1 mod-10 check digit (payload weighted 3,1,... from the right).
        total = sum(
            int(ch) * (3 if (len(digits) - 1 - i) % 2 == 1 else 1)
            for i, ch in enumerate(digits[:-1])
        )
        if (10 - (total % 10)) % 10 != int(digits[-1]):
            raise ValueError("Invalid UPC check digit")
        return digits


class CreateSkuRequest(BaseModel):

    company_code: int = Field(..., description="Company code")
    brand: str = Field(..., min_length=1, description="Brand name")
    brand_code: str = Field(..., min_length=1, max_length=10, description="Brand code")
    mpn: str = Field(..., min_length=1, description="Manufacturer Part Number")
    title: str = Field(..., min_length=1, description="Product title")
    product_type: str = Field(..., min_length=1, description="Product type")
    type_code: str = Field(..., min_length=1, max_length=10, description="Type code")
    sizing_scheme: str = Field(..., min_length=1, description="Sizing scheme")
    style_name: str = Field(..., min_length=1, description="Style name")
    brand_color: str = Field(..., min_length=1, description="Brand color")
    color: str = Field(..., min_length=1, description="Standard color")
    retail_price: float = Field(..., gt=0, description="Retail price (SellerCloud ListPrice)")
    country_of_origin: str = Field(
        ..., min_length=1, description="Country of origin (SC COUNTRY_OF_ORIGIN column)"
    )
    season: Optional[str] = Field(
        None, description="Season (SC FASHION_SEASON column); upper-cased, max 10 chars"
    )
    sizes: List[CreateSkuSize] = Field(..., min_items=1, description="Sizes with unit price")

    @validator("season")
    def validate_season(cls, v):
        if v is None:
            return None
        v = v.strip().upper()
        return v[:10] or None


class CreateSkuResponse(BaseModel):

    success: bool
    parent_sku: Optional[str] = None
    children: Optional[List[str]] = None
    failures: Optional[List[Dict[str, str]]] = None
    sellercloud_warning: Optional[str] = None
    error: Optional[str] = None


class BulkAddSizesRequest(BaseModel):

    parent_sku: str = Field(..., description="Parent product SKU (must not contain '/')")
    sizes: List[CreateSkuSize] = Field(
        ..., min_items=1, description="Sizes with unit price and optional UPC (blank = auto)"
    )

    @validator("parent_sku")
    def validate_parent_sku(cls, v):
        if "/" in v:
            raise ValueError("Parent SKU must not contain '/'")
        return v


class BulkAddSizeFailure(BaseModel):

    size: str
    error: str


class BulkAddSizesResponse(BaseModel):

    success: bool
    parent_sku: Optional[str] = None
    children: Optional[List[str]] = None
    failures: Optional[List[BulkAddSizeFailure]] = None
    error: Optional[str] = None


class ReassignAddSizeRequest(BaseModel):

    parent_sku: str = Field(..., description="Parent product SKU (must not contain '/')")
    size: str = Field(..., min_length=1, description="Size value (e.g., '32', 'M', 'XL')")

    @validator("parent_sku")
    def validate_parent_sku(cls, v):
        if "/" in v:
            raise ValueError("Parent SKU must not contain '/'")
        return v


class ReassignAddSizeResponse(BaseModel):

    success: bool
    new_child_sku: Optional[str] = None
    parent_sku: Optional[str] = None
    size: Optional[str] = None
    error: Optional[str] = None


class UpdateParentProductRequest(BaseModel):
    title: Optional[str] = Field(
        None, min_length=1, max_length=500, description="Updated product title"
    )
    product_type: Optional[str] = Field(
        None, max_length=200, description="Product type (e.g., Sneaker, Shirt)"
    )
    sizing_scheme: Optional[str] = Field(
        None, max_length=200, description="Sizing scheme (e.g., S_SHOE_MEN)"
    )
    style_name: Optional[str] = Field(None, max_length=500, description="Style name")
    brand_color: Optional[str] = Field(None, max_length=200, description="Brand color")
    color: Optional[str] = Field(None, max_length=200, description="Standard color")
    mpn: Optional[str] = Field(
        None, min_length=1, max_length=200, description="Manufacturer Part Number"
    )
    brand: Optional[str] = Field(None, max_length=200, description="Brand name")


class UpdateParentProductResponse(BaseModel):
    success: bool
    sku: str
    title: Optional[str] = None
    product_type: Optional[str] = None
    sizing_scheme: Optional[str] = None
    style_name: Optional[str] = None
    brand_color: Optional[str] = None
    color: Optional[str] = None
    mpn: Optional[str] = None
    brand: Optional[str] = None
    sellercloud_warning: Optional[str] = None


class ReassignChildRequest(BaseModel):
    child_sku: str = Field(..., description="The child SKU to reassign")
    new_parent_sku: str = Field(..., description="The new parent SKU")
    target_child_sku: str = Field(
        ..., description="The destination child SKU to transfer inventory to"
    )

    @validator("new_parent_sku")
    def validate_parent_sku(cls, v):
        if "/" in v:
            raise ValueError("Parent SKU must not contain '/' separator")
        return v

    @validator("target_child_sku")
    def validate_target_child_sku(cls, v):
        if "/" not in v:
            raise ValueError("Target child SKU must contain '/' separator")
        return v


class ReassignChildResponse(BaseModel):
    success: bool
    assignment_id: Optional[int] = None
    job_id: Optional[int] = None
    child_sku: str
    old_parent_sku: Optional[str] = None
    new_parent_sku: str
    target_child_sku: str
    transfer_result: Optional[Dict[str, Any]] = None
    message: str


class ProductSearchResult(BaseModel):

    sku: str
    title: Optional[str] = None
    mpn: Optional[str] = None
    brand: Optional[str] = None
    size: Optional[str] = None
    is_primary: Optional[bool] = None
    parent_sku: Optional[str] = None
    child_count: Optional[int] = None
    is_parent: bool = Field(..., description="True if parent product, False if child")


class ProductSearchResponse(BaseModel):

    results: List[ProductSearchResult]
    total: int
    exact_match: bool = False


class SelectedChildUpc(BaseModel):

    upc: str
    is_primary_upc: bool
    upc_type: Optional[str] = None


class SelectedChild(BaseModel):
    """Child-specific data for the SKU the user requested (when that SKU was a child)."""

    sku: str
    size: Optional[str] = None
    is_primary: Optional[bool] = None
    parent_sku: Optional[str] = None
    primary_upc: Optional[str] = None
    all_upcs: List[SelectedChildUpc] = Field(default_factory=list)
    keywords: List[str] = Field(default_factory=list)


class ProductDetailsResponse(BaseModel):
    """
    Unified parent-shaped response. Top-level fields describe the parent;
    `children` is the parent's child list. When the requested SKU was a
    child, `selected_child` carries that child's per-row data (size, UPCs,
    keywords). When the requested SKU was a parent itself,
    `selected_child` is `None`.
    """

    success: bool
    sku: str
    is_parent: Optional[bool] = Field(
        None,
        description="True for resolved products. None on redirect/not-found responses.",
    )
    title: Optional[str] = None
    mpn: Optional[str] = None
    brand: Optional[str] = None
    type_code: Optional[str] = None
    serial_number: Optional[int] = None
    company_code: Optional[int] = None
    product_type: Optional[str] = None
    sizing_scheme: Optional[str] = None
    style_name: Optional[str] = None
    brand_color: Optional[str] = None
    color: Optional[str] = None
    child_count: Optional[int] = None
    children: Optional[List[Dict[str, Any]]] = None
    selected_child: Optional[SelectedChild] = Field(
        None,
        description="Populated when the requested SKU was a child; carries that child's size, UPCs, keywords, and primary flag.",
    )
    redirect_to: Optional[str] = Field(
        None,
        description="If the requested SKU is a reassigned secondary SKU, the live primary SKU it now maps to.",
    )
    error: Optional[str] = None


class BulkMappingItem(BaseModel):

    old_child_sku: str = Field(..., description="Source child SKU to reassign")
    new_child_sku: str = Field(..., description="Target child SKU for inventory transfer")


class BulkReassignRequest(BaseModel):

    old_parent_sku: str = Field(..., description="Source parent SKU")
    new_parent_sku: str = Field(..., description="Target parent SKU")
    mappings: List[BulkMappingItem] = Field(..., description="List of child mappings")


class BulkReassignResponse(BaseModel):

    success: bool
    bulk_assignment_id: Optional[int] = None
    total_mappings: Optional[int] = None
    failed_mappings: Optional[List[Dict[str, Any]]] = None
    status: Optional[str] = None
    error: Optional[str] = None


class BulkAssignmentStatusResponse(BaseModel):

    assignment_id: int
    old_child_sku: str
    new_child_sku: Optional[str] = None
    status: str
    completed_jobs: Optional[int] = None
    total_jobs: Optional[int] = None


class BulkReassignStatusResponse(BaseModel):

    success: bool
    bulk_assignment_id: Optional[int] = None
    old_parent_sku: Optional[str] = None
    new_parent_sku: Optional[str] = None
    status: Optional[str] = None
    total: Optional[int] = None
    completed: Optional[int] = None
    failed: Optional[int] = None
    current_sku: Optional[str] = None
    created_at: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    assignments: Optional[List[BulkAssignmentStatusResponse]] = None
    error: Optional[str] = None


# ============================================================================
# UPC Management Models
# ============================================================================


class AddUPCRequest(BaseModel):

    sku: str = Field(..., min_length=1, description="Child SKU")
    upc: str = Field(..., pattern=r"^\d{8}$|^\d{12,13}$", description="UPC (8, 12, or 13 digits)")


class AddUPCResponse(BaseModel):

    success: bool
    sku: str
    upc: str
    is_primary: bool = False
    upc_type: Optional[str] = None
    error: Optional[str] = None


class SetPrimaryUPCRequest(BaseModel):

    sku: str = Field(..., min_length=1, description="Child SKU")
    upc: str = Field(..., min_length=1, description="UPC to set as primary")


class SetPrimaryUPCResponse(BaseModel):

    success: bool
    sku: str
    old_primary_upc: Optional[str] = None
    new_primary_upc: str
    message: Optional[str] = None


class DeleteUPCRequest(BaseModel):

    sku: str = Field(..., min_length=1, description="Child SKU")
    upc: str = Field(..., min_length=1, description="UPC to delete")


class DeleteUPCResponse(BaseModel):

    success: bool
    sku: str
    upc: str
    error: Optional[str] = None


# ============================================================================
# Keyword Management Models
# ============================================================================


class AddKeywordRequest(BaseModel):

    sku: str = Field(..., min_length=1, description="Child SKU")
    keyword: str = Field(..., pattern=r"^[A-Za-z0-9_./+#&@()-]{6,20}$", description="Keyword (6-20 chars; letters, digits, or symbols _./+#&@()-)")


class AddKeywordResponse(BaseModel):

    success: bool
    sku: str
    keyword: str
    error: Optional[str] = None


class DeleteKeywordRequest(BaseModel):

    sku: str = Field(..., min_length=1, description="Child SKU")
    keyword: str = Field(..., min_length=1, description="Keyword to delete")


class DeleteKeywordResponse(BaseModel):

    success: bool
    sku: str
    keyword: str
    error: Optional[str] = None


# ============================================================================
# Bulk Import Models
# ============================================================================


class BulkImportItem(BaseModel):

    row: int
    sku: str
    value: str
    action: str = Field(..., description="Primary, Secondary, Keyword, or Delete")
    classification: Optional[str] = Field(
        None,
        description="Resolved action: add_primary, add_secondary, add_keyword, "
                     "noop, promote_primary, swap_primary, swap_secondary, "
                     "swap_keyword, delete_upc, delete_keyword",
    )
    source_sku: Optional[str] = Field(
        None, description="SKU that currently owns the value (for swaps)",
    )


class BulkImportValidationError(BaseModel):

    row: int
    sku: Optional[str] = None
    value: Optional[str] = None
    field: str
    message: str


class BulkImportValidateResponse(BaseModel):

    valid: bool
    errors: List[BulkImportValidationError] = []
    items: List[BulkImportItem] = []
    file_data: Optional[str] = None
    donors: Dict[str, Dict[str, int]] = Field(
        default_factory=dict,
        description="SKUs that lose UPCs without receiving any in return (one-way transfers). "
                     "Keys are SKUs, values are {losses: int, gains: int}.",
    )
    auto_promotions: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="SKUs where a secondary UPC will be auto-promoted to primary because the "
                     "current primary is being moved away and no explicit replacement was provided. "
                     "Each entry: {sku, previous_primary, candidates: [upcs]}.",
    )
    noops: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Rows that will be skipped because the UPC/keyword is already on the target SKU "
                     "in the desired state. Each entry: {row, sku, value, action}. Safe to re-import; "
                     "the UI should warn the user and offer a download.",
    )
    transfers: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Per-row transfer list — one entry per swap (Primary UPC, Secondary UPC, or "
                     "Keyword moving between SKUs). Each entry: {row, value_type, value, from_sku, "
                     "to_sku}. Surfaced to the user so they see every move explicitly.",
    )


class BulkImportRequest(BaseModel):

    items: List[BulkImportItem]


class BulkImportResultItem(BaseModel):

    row: int
    sku: str
    value: str
    action: str
    classification: Optional[str] = None
    success: bool
    error: Optional[str] = None
    operation_id: Optional[int] = None


class BulkImportResponse(BaseModel):

    success: bool
    total_items: int
    successful_count: int
    failed_count: int
    results: List[BulkImportResultItem] = []
    async_job: bool = Field(
        False,
        description="True when the import was enqueued as a background job because "
                     "items exceeded the synchronous threshold. Poll /bulk_import/jobs/{job_id}.",
    )
    job_id: Optional[int] = None


class BulkImportJobStatusResponse(BaseModel):

    job_id: int
    status: str  # pending, processing, completed, failed
    total_items: int
    processed_items: int
    successful_count: int
    failed_count: int
    results: List[BulkImportResultItem] = []
    error_message: Optional[str] = None
    created_at: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None


# ---------------------------------------------------------------------------
# Internal platforms (consignment pipeline: 1nventory -> Syncio -> Shop The Sample)
# ---------------------------------------------------------------------------


class InternalPlatformStoreStatus(BaseModel):
    store_key: str = Field(..., description="Config lookup key, e.g. 'xuh30f-dr'")
    role: str = Field(..., description="'source' or 'destination'")
    granted_scopes: List[str] = Field(
        default_factory=list, description="Scopes the Shopify app actually holds"
    )
    required_scopes: List[str] = Field(
        default_factory=list, description="Scopes this pipeline needs on this store"
    )
    missing_scopes: List[str] = Field(
        default_factory=list, description="required - granted; non-empty means blocked"
    )
    reachable: bool = Field(True, description="False if the token request failed")
    error: Optional[str] = Field(None, description="Why the store could not be reached")


class InternalPlatformPollerStatus(BaseModel):
    name: str = Field(..., description="Poller identifier")
    enabled: bool = Field(..., description="Whether the poller starts at all")
    execute: bool = Field(..., description="False means dry-run: plans but never writes")
    execute_deletes: Optional[bool] = Field(
        None, description="Source poller only; deletes are gated separately"
    )
    cadence: str = Field("", description="Human-readable schedule")


class InternalPlatformOverviewResponse(BaseModel):
    platform_id: str = Field(..., description="Internal platform identifier")
    name: str = Field("", description="Display name")
    source_store: str = Field("", description="Source store config key")
    dest_store: str = Field("", description="Destination store config key")
    trigger_tag: str = Field("", description="Tag that drives the sync")
    platform_enabled: bool = Field(
        False, description="Platform row enabled flag; false means nothing runs"
    )

    # The effective write posture, so an operator reads it rather than inferring it from a
    # Submit button that returns 409. Three separate switches because they fail at three
    # different points and a single "read-only" boolean would hide which one is engaged.
    writes_allowed: bool = Field(
        True,
        description="[shopify] allow_writes. False means this environment cannot write "
        "to Shopify by any caller, including maintenance scripts",
    )
    source_poller_enabled: bool = Field(
        False, description="Source poller runs its scheduled scan"
    )
    source_poller_execute: bool = Field(
        False, description="Source poller may write; false makes POST /submit return 409"
    )
    dest_poller_enabled: bool = Field(
        False, description="Destination poller runs its scheduled reconcile"
    )
    dest_poller_execute: bool = Field(
        False, description="Destination poller may write vendor, tags and prices"
    )

    tracked: int = Field(0, description="Parent SKUs tracked in state")
    live: int = Field(0, description="Products confirmed on the destination")
    awaiting_sync: int = Field(
        0, description="Tagged on source, Syncio has not delivered yet"
    )
    stale_awaiting_sync: int = Field(
        0, description="Awaiting sync beyond the alert window (needs a human)"
    )
    failed: int = Field(0, description="State rows in a failed state")
    flagged: int = Field(0, description="State rows skipped and flagged")
    orphaned_delists: int = Field(
        0, description="Source untagged but the destination action failed"
    )

    status_counts: Dict[str, int] = Field(
        default_factory=dict, description="State rows grouped by current_status"
    )
    skip_reason_counts: Dict[str, int] = Field(
        default_factory=dict, description="State rows grouped by skip_reason"
    )
    recent_activity: Dict[str, int] = Field(
        default_factory=dict, description="'{action}.{status}' counts for the window"
    )
    activity_window_minutes: int = Field(
        1440, description="Window used for recent_activity"
    )

    products_in_flight: int = Field(
        0, description="Products tagged on source and awaiting Syncio delivery"
    )
    max_products_in_flight: int = Field(
        0, description="Ceiling the submit gate budgets against; 0 means disabled"
    )
    submit_blocked: bool = Field(
        False, description="True when the Syncio capacity gate would refuse a submit"
    )
    submit_gate_message: str = Field("", description="Human-readable capacity state")
    auto_submit: bool = Field(
        False, description="Whether the scheduled pass tags automatically"
    )
    can_submit_for_real: bool = Field(
        False, description="False means a submit would be a dry-run (execute=false)"
    )
    ready_for_listing: int = Field(
        0, description="Qualify but not yet tagged; waiting on a Submit click"
    )
    pending_delisting: int = Field(
        0, description="Stopped qualifying and soaked; waiting on a Delist click"
    )
    can_delist_for_real: bool = Field(
        False, description="False means Delist would refuse (execute_deletes=false)"
    )
    auto_delist: bool = Field(
        False, description="Whether the scheduled pass delists automatically"
    )

    stores: List[InternalPlatformStoreStatus] = Field(default_factory=list)
    pollers: List[InternalPlatformPollerStatus] = Field(default_factory=list)
    blockers: List[str] = Field(
        default_factory=list,
        description="Human-readable reasons the pipeline is not currently writing",
    )


class InternalPlatformStateRow(BaseModel):
    parent_sku: str = Field(..., description="Parent SKU this row tracks")

    # Shopify-derived facts, refreshed by the source scan on every pass. Null until a scan
    # has seen the product, so the UI renders these cells empty rather than wrong.
    title: str | None = Field(None, description="Product title on 1nventory")
    image_url: str | None = Field(None, description="featuredImage URL")
    product_type: str | None = Field(None, description="Lux product type")
    inventory: int = Field(0, description="Total stock across variants")
    source_price: Decimal | None = Field(None, description="MIN variant price on 1nventory")
    source_compare_at: Decimal | None = Field(
        None, description="MAX variant compare-at; the denominator for both discounts"
    )
    sts_price: Decimal | None = Field(
        None, description="Computed destination price; null when pricing does not resolve"
    )
    variant_count: int = Field(0, description="Variants this product carries")
    variants: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Per-variant sku/size/price/compare_at/inventory, as the scan saw them",
    )

    current_status: str = Field(
        ...,
        description="pending|ready_for_listing|pending_normalization|listed|"
                    "pending_delisting|delisted|failed|skipped",
    )
    source_product_gid: Optional[str] = Field(None, description="Shopify GID on the source store")
    dest_product_gid: Optional[str] = Field(None, description="Shopify GID on the destination")
    inflight_action: Optional[str] = Field(None, description="Action currently claimed, if any")
    skip_reason: Optional[str] = Field(None, description="Why the automation declined to act")
    last_error: Optional[str] = Field(None, description="Most recent failure detail")
    delist_strikes: int = Field(0, description="Consecutive cycles failing qualification")
    listed_at: Optional[datetime] = None
    normalize_done_at: Optional[datetime] = None
    location_done_at: Optional[datetime] = None
    delisted_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class InternalPlatformProductsResponse(BaseModel):
    platform_id: str
    items: List[InternalPlatformStateRow] = Field(default_factory=list)
    total: int = Field(0, description="Total rows matching the filters")
    page: int = Field(1, description="Current page (1-indexed)")
    page_size: int = Field(50, description="Rows per page")


class InternalPlatformSubmissionRow(BaseModel):
    id: int = Field(..., description="Ledger row id")
    parent_sku: str
    action: str = Field(..., description="list|normalize|reprice|location|untag|delete")
    status: str = Field(..., description="pending|success|failed|skipped")
    skip_reason: Optional[str] = None
    source_product_gid: Optional[str] = None
    dest_product_gid: Optional[str] = None
    error: Optional[str] = Field(None, description="Redacted failure detail")
    actor: Optional[str] = None
    triggered_by: str = Field("scheduler", description="scheduler|manual|backfill")
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class InternalPlatformActivityResponse(BaseModel):
    platform_id: str
    items: List[InternalPlatformSubmissionRow] = Field(default_factory=list)
    total: int = Field(0, description="Total rows matching the filters")
    page: int = Field(1, description="Current page (1-indexed)")
    page_size: int = Field(50, description="Rows per page")


class InternalPlatformSubmissionDetail(InternalPlatformSubmissionRow):
    payload: Optional[Dict[str, Any]] = Field(
        None, description="Whitelisted request payload, includes the pre-image on deletes"
    )
    result: Optional[Dict[str, Any]] = Field(None, description="Whitelisted response")


class InternalPlatformProductDetailResponse(BaseModel):
    platform_id: str
    parent_sku: str
    state: Optional[InternalPlatformStateRow] = Field(
        None, description="Current state; null if only history exists"
    )
    timeline: List[InternalPlatformSubmissionDetail] = Field(
        default_factory=list, description="Every action for this SKU, newest first"
    )
    status_counts: Dict[str, int] = Field(default_factory=dict)


class InternalPlatformSubmitResponse(BaseModel):
    platform_id: str = Field(..., description="Internal platform the submit ran for")
    submitted: int = Field(0, description="Products tagged on the source store")
    variants_submitted: int = Field(0, description="Variants those products carry")
    held_back: int = Field(
        0, description="Qualifying products trimmed to fit Syncio's remaining budget"
    )
    blocked: bool = Field(False, description="True when the Syncio capacity gate refused")
    gate_message: str = Field("", description="Human-readable capacity state")
    products_in_flight: int = Field(0, description="Products awaiting Syncio delivery")
    max_products_in_flight: int = Field(0, description="Ceiling the gate budgets against")


class InternalPlatformDelistResponse(BaseModel):
    platform_id: str = Field(..., description="Internal platform the delist ran for")
    untagged: int = Field(0, description="Products untagged on the source store")
    deleted: int = Field(0, description="Destination products deleted. IRREVERSIBLE")
    failed: int = Field(0, description="Products whose delist did not complete")
    still_pending: int = Field(0, description="Queued for delisting after this run")
    blocked: bool = Field(False, description="True when the run refused to act")
    gate_message: str = Field("", description="Why it refused, when it did")
