from pydantic import BaseModel, Field, validator, model_validator
from typing import List, Dict, Any, Optional, Union, Literal
from uuid import UUID
from datetime import datetime
import re
from pydantic import computed_field


class ColumnDefinition(BaseModel):

    name: str = Field(..., description="Column name in the DB")
    display_name: str = Field(..., description="Label shown in the UI")
    type: Literal["text", "number", "bool", "text_list", "platform_list"] = Field(
        ..., description="Data type"
    )
    order: int = Field(default=999, description="Display order of the column, lower is first.")
    is_required: bool = Field(default=False, description="Form-level requirement")
    is_unique: bool = Field(default=False, description="Ensures values in the column are unique")
    is_primary_column: bool = Field(
        default=False, description="Indicates if this is the primary business column"
    )
    display_on_ui: bool = Field(
        default=True,
        description="Whether to display this column in the main table view",
    )
    display_in_form: bool = Field(
        default=True,
        description="Whether to display this column in form inputs",
    )
    default: Optional[Union[str, int, float, bool, List[str]]] = Field(
        None, description="Default value"
    )
    min: Optional[Union[int, float]] = Field(None, description="Minimum value/length")
    max: Optional[Union[int, float]] = Field(None, description="Maximum value/length")
    regex: Optional[str] = Field(None, description="Regex pattern for text fields")
    regex_error_message: Optional[str] = Field(None, description="Regex error message")
    fuzzy_check: bool = Field(default=False, description="Enable fuzzy duplicate detection")
    options: Optional[List[Union[str, int, float]]] = Field(
        None, description="Predefined options for 'text' or 'number' type"
    )
    multiselect: bool = Field(
        False, description="Allow multiple selections for 'text' type with options"
    )

    @validator("name")
    def validate_name(cls, v):
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", v):
            raise ValueError("Column name must be a valid identifier")
        return v.lower()

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
    def check_platform_list_constraints(self) -> "ColumnDefinition":
        if self.type == "platform_list":
            if self.is_unique:
                raise ValueError("is_unique is not applicable for platform_list type")
            if self.fuzzy_check:
                raise ValueError("fuzzy_check is not applicable for platform_list type")
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


class ListSchemaDefinition(BaseModel):

    platform_id: str = Field(..., description="Platform identifier (maps to Platform.id)")
    list_type: Literal["default", "sizing"] = Field(..., description="Type of list")
    display_name: str = Field(
        ...,
        description="User-friendly display name for this specific list configuration (e.g., 'Farfetch EU Sizing', 'Brand X Default Codes')",
    )
    enabled: bool = Field(
        default=True,
        description="Whether this list configuration is currently active and usable",
    )

    min_length: Optional[int] = Field(
        None, ge=0, description="Minimum length for the 'platform_value' string."
    )
    max_length: Optional[int] = Field(
        None, ge=0, description="Maximum length for the 'platform_value' string."
    )
    regex: Optional[str] = Field(
        None, description="Regex pattern that 'platform_value' must match."
    )
    regex_error_message: Optional[str] = Field(
        None, description="Custom error message for regex mismatch."
    )

    @validator("regex")
    def validate_platform_value_regex(cls, v):
        if v:
            try:
                re.compile(v)
            except re.error:
                raise ValueError("Invalid regex pattern for platform_value")
        return v

    @validator("max_length")
    def check_max_length_gte_min_length(cls, v, values):
        min_length = values.get("min_length")
        if v is not None and min_length is not None:
            if v < min_length:
                raise ValueError(
                    "max_length must be greater than or equal to min_length for platform_value"
                )
        return v


class ListSchemaDefinitionUpdate(BaseModel):

    display_name: Optional[str] = Field(
        None,
        description="User-friendly display name for this specific list configuration",
    )
    enabled: Optional[bool] = Field(
        None,
        description="Whether this list configuration is currently active and usable",
    )
    min_length: Optional[int] = Field(
        None, ge=0, description="Minimum length for the 'platform_value' string."
    )
    max_length: Optional[int] = Field(
        None, ge=0, description="Maximum length for the 'platform_value' string."
    )
    regex: Optional[str] = Field(
        None, description="Regex pattern that 'platform_value' must match."
    )
    regex_error_message: Optional[str] = Field(
        None, description="Custom error message for regex mismatch."
    )

    @validator("regex")
    def validate_platform_value_regex(cls, v):
        if v:
            try:
                re.compile(v)
            except re.error:
                raise ValueError("Invalid regex pattern for platform_value")
        return v


class CreateTableRequest(BaseModel):

    table_name: str = Field(..., description="Name of the table to create")
    display_name: str = Field(..., description="UI display name for the table")
    primary_business_column: str = Field(..., description="Primary business column name")
    primary_business_display_name: str = Field(
        ..., description="Display name for primary business column"
    )
    list_type: Literal["default", "sizing"] = Field(
        default="default", description="Default list type for the table"
    )

    @validator("table_name")
    def validate_table_name(cls, v):
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", v):
            raise ValueError("Table name must be a valid identifier")
        return v.lower()

    @validator("primary_business_column")
    def validate_primary_business_column(cls, v):
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", v):
            raise ValueError("Primary business column name must be a valid identifier")
        return v.lower()


class AddColumnRequest(BaseModel):

    table_name: str = Field(..., description="Name of the table")
    column: ColumnDefinition = Field(..., description="Column definition")


class ColumnDefinitionUpdate(BaseModel):

    display_name: Optional[str] = Field(None, description="Label shown in the UI")
    display_on_ui: Optional[bool] = Field(
        None, description="Whether to display this column in the main table view"
    )
    display_in_form: Optional[bool] = Field(
        None, description="Whether to display this column in form inputs"
    )
    order: Optional[int] = Field(None, description="Display order of the column")
    default: Optional[Union[str, int, float, bool, List[str]]] = Field(
        None, description="Default value"
    )
    min: Optional[Union[int, float]] = Field(None, description="Minimum value/length")
    max: Optional[Union[int, float]] = Field(None, description="Maximum value/length")
    regex: Optional[str] = Field(None, description="Regex pattern for text fields")
    regex_error_message: Optional[str] = Field(None, description="Regex error message")
    options: Optional[List[str]] = Field(None, description="Predefined options for 'text' type")


class UpdateColumnRequest(BaseModel):

    table_name: str = Field(..., description="Name of the table")
    column_name: str = Field(..., description="Name of the column to update")
    update_data: ColumnDefinitionUpdate = Field(..., description="Fields to update")


class TableSchema(BaseModel):

    table: str
    display_name: Optional[str]
    primary_business_column: Optional[str]
    column_schema: List[ColumnDefinition]
    list_schema: List[ListSchemaDefinition]
    list_type: str
    json_schema: Optional[Dict[str, Any]] = Field(None, description="JSON schema for RJSF")
    ui_schema: Optional[Dict[str, Any]] = Field(None, description="UI schema for RJSF")
    created_at: datetime
    updated_at: datetime


class RecordData(BaseModel):

    data: Dict[str, Any] = Field(..., description="Record data as key-value pairs")


class DefaultListEntry(BaseModel):

    primary_id: Optional[UUID] = None
    platform_value: str = Field(..., description="Value as displayed on platform")
    platform_id: str = Field(..., description="Platform ID")
    primary_table_column: str = Field(..., description="Column being mapped")


class SizingListEntry(BaseModel):

    sizing_scheme: str = Field(..., description="Name of the size chart/scheme")
    platform_value: str = Field(..., description="Value as displayed on platform")
    platform: str = Field(..., description="Platform ID")
    value: str = Field(..., description="Internal value")


class DefaultListResponse(DefaultListEntry):

    id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class SizingListResponse(SizingListEntry):

    id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class PlatformResponse(BaseModel):

    id: str
    name: str
    icon: Optional[str] = None
    icon_mime_type: Optional[str] = None


class FuzzyCheckRequest(BaseModel):

    table_name: str = Field(..., description="Name of the table")
    column_name: str = Field(..., description="Name of the column")
    value: str = Field(..., description="Value to check for duplicates")
    threshold: float = Field(default=0.3, ge=0.0, le=1.0, description="Similarity threshold")


class FuzzyCheckResponse(BaseModel):

    similar_values: List[str] = Field(
        default_factory=list,
        description="List of values that are similar to the input.",
    )
    exact_matches: List[str] = Field(
        default_factory=list,
        description="List of values that have an exact match in the DB.",
    )

    @computed_field
    @property
    def exact_match(self) -> bool:
        return len(self.exact_matches) > 0


class SuccessResponse(BaseModel):

    success: bool = True
    message: str


class ErrorResponse(BaseModel):

    success: bool = False
    error: str
    details: Optional[str] = None


class PaginationParams(BaseModel):

    page: int = Field(default=1, ge=1, description="Page number")
    page_size: int = Field(default=50, ge=1, le=1000, description="Number of items per page")


class PaginatedResponse(BaseModel):

    items: List[Dict[str, Any]]
    total: int
    page: int
    page_size: int
    total_pages: int


class TableSchemaResponse(BaseModel):

    table: str
    display_name: Optional[str]
    primary_business_column: Optional[str]
    column_schema: List[ColumnDefinition]
    list_schema: List[ListSchemaDefinition]
    list_type: str
    json_schema: Optional[Dict[str, Any]] = Field(None, description="JSON schema for RJSF")
    ui_schema: Optional[Dict[str, Any]] = Field(None, description="UI schema for RJSF")
    created_at: datetime
    updated_at: datetime


class SizingSchemeEntryBase(BaseModel):
    size: str = Field(
        ...,
        description="A specific size within the scheme (e.g., 'S', 'M', 'L', '42', 'UK 8')",
    )
    order: int = Field(..., description="The display order of this size within its scheme.")

    model_config = {"from_attributes": True}

    @validator("size")
    def validate_size_content(cls, v):
        if not v:
            raise ValueError("Size value cannot be empty.")
        if " " in v or "/" in v:
            raise ValueError("Size value cannot contain spaces or forward slashes ('/').")
        return v


class SizingSchemeEntryWithId(SizingSchemeEntryBase):
    id: UUID


class SizingSchemeEntryCreate(SizingSchemeEntryBase):
    pass


class SizingSchemeEntryDB(SizingSchemeEntryBase):
    id: UUID
    sizing_scheme: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class SizingSchemeListedName(BaseModel):

    name: str


class SizeWithSchemesResponse(BaseModel):

    size: str
    sizing_schemes: List[str]


class SizingSchemeDetailResponse(BaseModel):

    sizing_scheme: str = Field(..., description="The name of the sizing scheme.")
    sizes: List[SizingSchemeEntryWithId] = Field(..., description="List of sizes with their order.")
    sizing_types: Optional[List[str]] = Field(
        None, description="List of applicable types for this sizing scheme."
    )


class AllSizingSchemesResponse(BaseModel):

    schemes: List[SizingSchemeDetailResponse] = Field(
        ..., description="List of all sizing schemes with their sizes."
    )


class UpdateSizeOrderRequest(BaseModel):

    new_sizing_scheme: Optional[str] = Field(
        None,
        max_length=50,
        description="Optional new name for the sizing scheme. If provided, the scheme will be renamed.",
    )
    sizes: List[SizingSchemeEntryBase] = Field(
        ..., description="A list of sizes with their new order."
    )
    sizing_types: Optional[List[str]] = Field(
        None, description="Updated list of applicable types for this sizing scheme."
    )


class FullSizingSchemeCreate(BaseModel):

    sizing_scheme: str = Field(..., max_length=50, description="The name of the new sizing scheme.")
    sizes: List[SizingSchemeEntryBase] = Field(
        ..., description="The initial list of sizes and their orders for the scheme."
    )
    sizing_types: Optional[List[str]] = Field(
        None, description="List of applicable types for this sizing scheme."
    )


class SizingListPlatformEntryBase(BaseModel):

    sizing_scheme_entry_id: UUID = Field(
        ...,
        description="The ID of the specific SizingScheme entry (e.g., for 'Alpha Sizes - M').",
    )
    platform_id: str = Field(..., description="The ID of the platform.")
    platform_value: str = Field(..., description="The platform-specific sizing value.")
    sizing_type: str = Field(..., description="The sizing type for this mapping.")


class SizingListPlatformEntryCreate(SizingListPlatformEntryBase):

    pass


class SizingListPlatformEntryUpdate(BaseModel):

    platform_value: Optional[str] = Field(
        None, description="The new platform-specific sizing value."
    )
    sizing_type: Optional[str] = Field(None, description="The new sizing type for this mapping.")


class SizingListPlatformEntryDB(SizingListPlatformEntryBase):

    id: UUID
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class SizingListPlatformEntryDetail(SizingListPlatformEntryDB):

    sizing_scheme_name: str = Field(
        ..., description="Name of the sizing scheme (e.g., 'Alpha Sizes')."
    )
    size_value: str = Field(..., description="The specific size (e.g., 'M').")
    size_order: int = Field(..., description="Order of the size within its scheme.")
    platform_name: str = Field(..., description="Name of the platform.")


class PaginatedSizingListPlatformEntryResponse(BaseModel):

    items: List[SizingListPlatformEntryDetail]
    total: int
    page: int
    page_size: int
    total_pages: int


class DefaultListInternalValuesUpdate(BaseModel):

    platform_id: str = Field(..., description="Platform ID")
    platform_value: str = Field(..., description="Platform value (key)")
    internal_values: List[str] = Field(
        ...,
        description="Complete set of internal values that should be linked to this platform_value",
    )
    confirmed: bool = Field(
        default=False,
        description="If true, confirms re-mapping of conflicting internal values.",
    )
    sizing_type: Optional[str] = Field(
        default=None,
        description="Sizing type scope (only for sizes table)",
    )


class ParentTypeResponse(BaseModel):

    id: UUID = Field(..., description="Unique identifier for the parent type")
    division: str = Field(..., description="Division name")
    dept_code: int = Field(..., description="Department code")
    dept: str = Field(..., description="Department name")
    class_code: int = Field(..., description="Class code")
    class_name: str = Field(..., description="Class name")
    gender: str = Field(..., description="Gender category")
    reporting_category: str = Field(..., description="Reporting category")

    model_config = {"from_attributes": True}


class BulkSearchByNameRequest(BaseModel):
    names: List[str]
    search_alias: bool = True


class BulkSearchTypeNameRequest(BaseModel):
    types: List[str]


class BulkGetProductTypesByClassRequest(BaseModel):
    class_names: List[str]


class BulkGetSizingSchemesByProductTypeRequest(BaseModel):
    product_types: List[str]


class FailedAlias(BaseModel):
    alias: str
    existing_color: str
    existing_alias: str


class AddAliasResponse(BaseModel):
    record_found: bool
    data: Optional[Dict[str, Any]] = None
    added_aliases: List[str] = []
    failed_aliases: List[FailedAlias] = []
