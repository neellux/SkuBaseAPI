from pydantic import BaseModel, Field, validator, model_validator
from typing import List, Optional, Union, Literal, Dict, Any
from datetime import datetime
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
    ai_response: Optional[Dict[str, Any]] = Field(None, description="AI response data")
    ai_description: Optional[str] = Field(None, description="AI generated description")
    submitted: Optional[bool] = Field(None, description="Submission status")
    submitted_by: Optional[str] = Field(None, description="User ID who submitted this listing")


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
    ai_response: Optional[Dict[str, Any]] = Field(None, description="AI response data")
    ai_description: Optional[str] = Field(None, description="AI generated description")
    original_description: Optional[str] = Field(
        None, description="Original SellerCloud description"
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
        None, description="The ID of an existing draft listing if one is found"
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
    created_at: datetime = Field(..., description="Creation timestamp")

    class Config:
        from_attributes = True


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
        None, description="Field templates mapping field names to template configs"
    )


class SettingsResponse(BaseModel):

    id: int = Field(..., description="Settings ID")
    field_templates: Dict[str, Any] = Field(
        default_factory=dict, description="Field templates mapping field names to template configs"
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


class ProductDetailsResponse(BaseModel):

    success: bool
    sku: str
    is_parent: Optional[bool] = Field(None, description="True if parent product, False if child")
    title: Optional[str] = None
    mpn: Optional[str] = None
    brand: Optional[str] = None
    type_code: Optional[str] = None
    serial_number: Optional[int] = None
    company_code: Optional[int] = None
    size: Optional[str] = None
    is_primary: Optional[bool] = None
    parent_sku: Optional[str] = None
    primary_upc: Optional[str] = None
    all_upcs: Optional[List[Dict[str, Any]]] = None
    keywords: Optional[List[str]] = None
    product_type: Optional[str] = None
    sizing_scheme: Optional[str] = None
    style_name: Optional[str] = None
    brand_color: Optional[str] = None
    color: Optional[str] = None
    child_count: Optional[int] = None
    children: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None
    redirect_to: Optional[str] = Field(
        None,
        description="When the requested SKU was reassigned, the SKU the UI should redirect to.",
    )


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
    keyword: str = Field(..., pattern=r"^\d{6,}$", description="Numeric keyword (6+ digits)")


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
