# SkuBase API Reference

**Version:** 1.0.0  
**Base URL:** `http://localhost:5521`  
**Response Format:** ORJSON  

---

## Table of Contents

- [Authentication & Middleware](#authentication--middleware)
- [Main API](#main-api)
  - [Root](#root)
  - [Products](#products-products)
  - [Listings](#listings-listings)
  - [Public API](#public-api-api)
  - [Templates](#templates-templates)
  - [Product Images](#product-images-productsimages)
  - [Settings](#settings-settings)
- [Listing Options API](#listing-options-api-listingoptions)
  - [Tables](#tables-listingoptionstables)
  - [Lists](#lists-listingoptionslists)
  - [Platforms](#platforms-listingoptionsplatforms)
  - [Sizing Schemes](#sizing-schemes-listingoptionssizing_schemes)
  - [Sizing Lists](#sizing-lists-listingoptionssizing_lists)
  - [Parent Types](#parent-types-listingoptionsparent_types)
  - [Listing Options Public API](#listing-options-public-api-apilistingoptions)
- [Data Models](#data-models)
  - [Database Models](#database-models)
  - [Request/Response Models (Main)](#requestresponse-models-main)
  - [Request/Response Models (Listing Options)](#requestresponse-models-listing-options)
- [Error Reference](#error-reference)
- [Background Services](#background-services)
- [Constants & Configuration](#constants--configuration)

---

## Authentication & Middleware

### AuthMiddleware

Cookie-based authentication via external auth service. All routes require authentication except paths starting with `/api`.

**Request flow:**
1. Extracts `Cookie` header from request
2. Forwards to auth service with `X-REQUEST-PATH`, `X-REQUEST-METHOD`, `X-APP-SHORT-NAME`
3. Auth service returns user object with permissions
4. User data stored in `request.state.user`

**Error responses:**
- `401 Unauthorized` — Missing Cookie header
- `403 Forbidden` — Insufficient access permissions
- `500 Internal Server Error` — Auth service unreachable

**WebSocket authentication:** Uses same cookie-based flow via `verify_websocket_scope_auth()`.

### CORSMiddleware

Handles CORS preflight (OPTIONS) and simple requests. Origins configured via `config.toml` `cors.allowed_origins` (default: `["*"]`).

### GZipMiddleware

Compresses responses larger than 1000 bytes.

### Permissions

| Permission | Section |
|---|---|
| `view_batches` | View batch pages |
| `manage_batches` | Full batch CRUD + delete action |
| `manage_settings` | App settings access |
| `manage_templates` | Template management |
| `create_products` | Product creation |
| `manage_records` | Listing options record management |
| `manage_tables` | Listing options table schema management |
| `manage_platforms` | Platform configuration |
| `upsert_records` | Record create/update action |
| `create_sizing_schemes` | Sizing scheme creation action |
| `manage_classes` | Parent type/class management |
| `edit_record_names` | Edit primary column values action |
| `edit_sizing_schemes` | Sizing scheme editing action |

---

## Main API

### Root

#### `GET /`

Returns API info.

**Auth:** Yes  
**Response:** `{"message": "Listing API", "version": "1.0.0"}`

---

#### `GET /app_settings`

Returns user permissions (sections and actions).

**Auth:** Yes  
**Response:** `{"sections": [...], "actions": [...]}`

---

#### `GET /app_users`

Returns list of app users filtered by app-specific roles (excluding dev roles).

**Auth:** Yes  
**Response:** `[{"id": "user_id", "name": "User Name"}, ...]`

---

### Products (`/products`)

#### `POST /products`

Add a new product (child SKU). Automatically creates parent if it doesn't exist. Validates and adds brand color alias if color and brand_color differ.

**Auth:** Yes  
**Request Body:** `AddProductRequest`

| Field | Type | Required | Description |
|---|---|---|---|
| `child_sku` | string | Yes | Full child SKU with `/` separator (e.g., `PRD-001/S`) |
| `title` | string | Yes | Product title |
| `upc` | string | No | UPC barcode (8, 12, or 13 digits) |
| `mpn` | string | No | Manufacturer Part Number |
| `brand_code` | string | No | Brand code |
| `type_code` | string | No | Product type code |
| `serial_number` | int | No | Serial number (1-9999) |
| `company_code` | int | Yes | Company code |

**Response:** `AddProductResponse` — `{success, child_sku, parent_sku, size, is_primary, parent_created, errors}`  
**Errors:** `400` invalid input, `500` server error

---

#### `POST /products/add_size`

Add a new size variant to an existing parent product. Creates child on SellerCloud and registers in local database.

**Auth:** Yes  
**Request Body:** `AddSizeRequest`

| Field | Type | Required | Validation |
|---|---|---|---|
| `parent_sku` | string | Yes | Must not contain `/` |
| `size` | string | Yes | Min length 1 |
| `upc` | string | Yes | 8, 12, or 13 digits |
| `cost_price` | float | Yes | Must be > 0 |

**Response:** `AddSizeResponse` — `{success, new_child_sku, parent_sku, size, error}`  
**Errors:** `400` invalid input, `404` parent not found, `500` server error

---

#### `POST /products/reassign_add_size`

Add a size variant with auto-generated 77777 UPC for reassignment (no SellerCloud product created).

**Auth:** Yes  
**Request Body:** `ReassignAddSizeRequest`

| Field | Type | Required | Validation |
|---|---|---|---|
| `parent_sku` | string | Yes | Must not contain `/` |
| `size` | string | Yes | Min length 1 |

**Response:** `ReassignAddSizeResponse` — `{success, new_child_sku, parent_sku, size, error}`  
**Errors:** `400` invalid input, `500` server error

---

#### `PUT /products/update_product_info`

Update a parent product's info (title, product_type, sizing_scheme, etc.).

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required | Description |
|---|---|---|---|
| `parent_sku` | string | Yes | Parent product SKU |
| `skip_brand_color_update` | bool | No | Skip brand color alias update |

**Request Body:** `UpdateParentProductRequest`

| Field | Type | Required |
|---|---|---|
| `title` | string | No |
| `product_type` | string | No |
| `sizing_scheme` | string | No |
| `style_name` | string | No |
| `brand_color` | string | No |
| `color` | string | No |
| `mpn` | string | No |
| `brand` | string | No |

**Response:** `UpdateParentProductResponse` — `{success, sku, ...updated fields}`  
**Errors:** `400` validation, `404` not found, `500` server error

---

#### `GET /products/reassign/preview`

Preview reassignment details before executing. Returns inventory breakdown by warehouse/bin and planned jobs.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `child_sku` | string | Yes |
| `new_parent_sku` | string | Yes |
| `target_child_sku` | string | Yes |

**Response:** Dict with inventory preview, warehouse/bin breakdown, planned jobs  
**Errors:** `400` invalid SKUs, `500` server error

---

#### `PUT /products/reassign`

Reassign a child product to a new parent. Transfers inventory, UPCs, keywords. Transfer flow: delete from source FIRST, then add to target. Placeholder UPC handling for 77777 UPCs skips alias operations.

**Auth:** Yes  
**Request Body:** `ReassignChildRequest`

| Field | Type | Required | Validation |
|---|---|---|---|
| `child_sku` | string | Yes | — |
| `new_parent_sku` | string | Yes | Must not contain `/` |
| `target_child_sku` | string | Yes | Must contain `/` |

**Response:** `ReassignChildResponse` — `{success, assignment_id, job_id, child_sku, old_parent_sku, new_parent_sku, target_child_sku, transfer_result, message}`  
**Errors:** `400` invalid input, `500` server error

---

#### `GET /products/product_types`

Get all available product types from the listing options database.

**Auth:** Yes  
**Response:** `{"product_types": [...]}`

---

#### `GET /products/colors`

Get all available colors from the listing options database.

**Auth:** Yes  
**Response:** `{"colors": [...]}`

---

#### `GET /products/brands`

Get all available brands from the listing options database.

**Auth:** Yes  
**Response:** `{"brands": [...]}`

---

#### `GET /products/search`

Search for products by SKU prefix.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required | Default | Validation |
|---|---|---|---|---|
| `q` | string | Yes | — | min_length=1 |
| `is_parent` | bool | No | — | Filter parent/child |
| `limit` | int | No | 50 | 1-200 |

**Response:** `ProductSearchResponse` — `{results: [ProductSearchResult], total, exact_match}`  
**Errors:** `400` invalid query

---

#### `GET /products/details`

Get detailed information about a product. Supports both parent SKUs (e.g., `PRD-001`) and child SKUs (e.g., `PRD-001/S`).

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `sku` | string | Yes |

**Response:** `ProductDetailsResponse` — includes classification fields (product_type, sizing_scheme, style_name, brand_color, color), child-specific fields (size, primary_upc, all_upcs), parent-specific fields (child_count, children)  
**Errors:** `404` not found, `500` server error

---

#### `GET /products/reassign/bulk/preview`

Get preview of bulk reassignment with auto-matched size mappings between old and new parent's children.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `old_parent_sku` | string | Yes |
| `new_parent_sku` | string | Yes |

**Response:** Dict with mapping suggestions based on size matching  
**Errors:** `400` invalid SKUs, `404` parent not found, `500` server error

---

#### `POST /products/reassign/bulk`

Create a bulk reassignment to move all children from one parent to another. Returns immediately with `bulk_assignment_id` for status polling.

**Auth:** Yes  
**Request Body:** `BulkReassignRequest`

| Field | Type | Required |
|---|---|---|
| `old_parent_sku` | string | Yes |
| `new_parent_sku` | string | Yes |
| `mappings` | List[BulkMappingItem] | Yes |

**Response:** `BulkReassignResponse` — `{success, bulk_assignment_id, total_mappings, failed_mappings, status, error}`  
**Errors:** `400` invalid mappings, `500` server error

---

#### `GET /products/reassign/bulk/status`

Get status of a bulk reassignment including progress on individual assignments.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `bulk_id` | int | Yes |

**Response:** `BulkReassignStatusResponse` — `{success, bulk_assignment_id, old_parent_sku, new_parent_sku, status, total, completed, failed, current_sku, assignments: [...]}`  
**Errors:** `404` not found, `500` server error

---

#### `POST /products/reassign/bulk/process`

Process the next pending assignment in a bulk reassignment. Call repeatedly until status is `completed` or `failed`.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `bulk_id` | int | Yes |

**Response:** Dict with processing result  
**Errors:** `404` not found, `500` server error

---

### Listings (`/listings`)

#### `GET /listings/product/confirm`

Get product confirmation data. Validates product images on GCS.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `product_id` | string | Yes |

**Response:** `ProductConfirmationData` — `{product: {...}, existing_listing_id}`  
**Errors:** `404` product not found, `500` server error

---

#### `POST /listings`

Create a new listing. Requires authenticated user.

**Auth:** Yes  
**Request Body:** `CreateListingRequest`

| Field | Type | Required |
|---|---|---|
| `product_id` | string | Yes |
| `info_product_id` | string | No |
| `assigned_to` | string | No |
| `template_id` | string | No |
| `data` | Dict | No |

**Response:** `ListingResponse`  
**Errors:** `400` invalid input, `500` server error

---

#### `GET /listings/images`

Get listing images.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `listing_id` | string | Yes |

**Response:** `List[str]` — image URLs

---

#### `GET /listings/children`

Get listing children with size variants.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `listing_id` | string | Yes |
| `product_type` | string | No |

**Response:** `ChildrenResponse` — `{children: [ChildProductData], product_type, sizing_scheme}`

---

#### `GET /listings/sizing_schemes`

Get sizing schemes for a product type. Uses custom SQL query.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `product_type` | string | Yes |

**Response:** `SizingSchemesResponse` — `{schemes: [SizingSchemeData], sizing_type}`

---

#### `GET /listings/platform_size_records`

Get platform-specific size records.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `platform_id` | string | Yes |
| `sizing_type` | string | No |

**Response:** Dict with platform size records

---

#### `POST /listings/save_size_mapping`

Save a single size mapping. Send null `platform_value` to delete.

**Auth:** Yes  
**Request Body:** `SaveSizeMappingRequest`

| Field | Type | Required | Description |
|---|---|---|---|
| `sizing_scheme_entry_id` | string | Yes | UUID of sizing_schemes entry |
| `platform_id` | string | Yes | Platform ID (e.g., `grailed`) |
| `platform_value` | string | No | Null = delete mapping |
| `sizing_type` | string | No | E.g., `Shoes`, `Clothing` |

**Response:** Dict with saved mapping  
**Errors:** `400` invalid input, `500` server error

---

#### `GET /listings/product_type_info`

Get product type info (gender, item weight).

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `product_type` | string | Yes |

**Response:** `ProductTypeInfoResponse` — `{gender, item_weight_oz}`

---

#### `GET /listings/detail`

Get listing details with user data.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `listing_id` | string | Yes |

**Response:** `ListingResponse`  
**Errors:** `404` not found

---

#### `PUT /listings`

Update listing data.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `listing_id` | string | Yes |

**Request Body:** `UpdateListingRequest`

| Field | Type | Required |
|---|---|---|
| `assigned_to` | string | No |
| `data` | Dict | No |
| `ai_response` | Dict | No |
| `ai_description` | string | No |
| `submitted` | bool | No |
| `submitted_by` | string | No |

**Response:** `ListingResponse`  
**Errors:** `404` not found, `500` server error

---

#### `DELETE /listings`

Delete a listing. Batch listing counts are automatically updated via database trigger.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `listing_id` | string | Yes |

**Response:** `{"success": true}`  
**Errors:** `404` not found, `500` server error

---

#### `GET /listings/submission_status`

Get per-platform submission status for a listing (used for polling). `queued` counts as complete from the user's perspective (submission accepted, poller handles it). Only `pending` and `processing` are actively in-flight.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `listing_id` | string | Yes |

**Response:** Dict with per-platform status

---

#### `POST /listings/submit`

Submit listing to platforms. Complex multi-platform submission flow.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `listing_id` | string | Yes |
| `skip_brand_color_update` | bool | No |

**Request Body:** `SubmitListingRequest`

| Field | Type | Required | Description |
|---|---|---|---|
| `platforms` | List[str] | No | Platform IDs. If null, uses app_settings defaults |

**Business Logic:**
1. **MPN conflict check:** Blocks submit if brand+mpn already belongs to another parent
2. **Child size validation:** Validates no duplicate sizes or empty sizes
3. **Brand color alias:** Validates and applies brand color alias upfront
4. **Size mapping check:** Validates size mappings for non-SellerCloud platforms
5. **Submission records created inside transaction with row-level locking** to serialize concurrent requests
6. **In-flight guard:** Skips platforms with active submission (queued/pending/processing)
7. **Resubmit guard:** Skips platforms that don't allow resubmission after success
8. **Initial status per platform:** `batch_submit` platforms start as `queued` (poller collects into batches); `requires_images` platforms start as `queued` when images pending; everything else starts as `pending` (submitted immediately)
9. **Parallel platform submissions** via `asyncio.gather`
10. **Post-submission operations:** Child size updates, parent product field updates, product info column sync (fire-and-forget background tasks)

**Response:** Dict with submission results per platform  
**Errors:** `400` validation (MPN conflict, size duplicates, missing mappings), `404` listing not found, `500` server error

---

#### `POST /listings/disable_product`

Disable a child product (marks as inactive in both SellerCloud and product DB).

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `product_id` | string | Yes |

**Response:** `{"success": true}`  
**Errors:** `500` server error

---

#### `GET /listings`

Get all listings with pagination.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required | Default |
|---|---|---|---|
| `assigned_to` | string | No | — |
| `submitted` | bool | No | — |
| `page` | int | No | 1 |
| `page_size` | int | No | 50 |

**Response:** `List[ListingResponse]`

---

#### `GET /listings/schema`

Get listing form schema (JSON Schema + UI Schema for RJSF).

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `template_id` | string | Yes |

**Response:** `ListingSchemaResponse` — `{json_schema, ui_schema, template_info}`

---

#### `POST /listings/batch/confirm`

Batch product confirmation (multiple products at once).

**Auth:** Yes  
**Request Body:** `BatchConfirmationRequest`

| Field | Type | Required | Validation |
|---|---|---|---|
| `product_ids` | List[str] | Yes | 1-1000 items |

**Response:** `BatchConfirmationResponse` — `{products: [...], total_count, success_count, existing_draft_count, error_count}`

---

#### `POST /listings/batch`

Create a new batch of listings. Requires authenticated user. Processes products concurrently (semaphore limit: 10).

**Auth:** Yes  
**Request Body:** `CreateBatchRequest`

| Field | Type | Required | Default | Validation |
|---|---|---|---|---|
| `product_ids` | List[str] | Yes | — | 1-1000 items |
| `comment` | string | No | — | — |
| `assigned_to` | string | No | — | — |
| `priority` | string | No | `medium` | `low`, `medium`, `high` |
| `photography_batch_id` | int | No | — | — |

**Response:** `BatchResponse`  
**Errors:** `400` batch size exceeds max (configurable, default 50), `400` `BatchCreationError` with failed product details

---

#### `GET /listings/batch/detail`

Get batch details with listings. Adds user data.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `batch_id` | int | Yes |

**Response:** `BatchResponse`  
**Errors:** `404` not found

---

#### `GET /listings/batches/filter_options`

Get batch filter options (available users, priorities, statuses).

**Auth:** Yes  
**Response:** `BatchFilterOptionsResponse` — `{users: [{id, name}], priorities: [...], statuses: [...]}`

---

#### `GET /listings/batches`

Get batches with multi-field filtering.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required | Default |
|---|---|---|---|
| `assigned_to` | string | No | — |
| `priority` | string | No | — |
| `status` | string | No | — |
| `date_from` | string | No | — |
| `date_to` | string | No | — |
| `search` | string | No | — |
| `page` | int | No | 1 |
| `page_size` | int | No | 50 |

**Response:** `List[BatchListResponse]`

---

#### `PUT /listings/batch`

Update batch properties.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `batch_id` | int | Yes |

**Request Body:** `UpdateBatchRequest` — `{comment, assigned_to, priority}`  
**Response:** `BatchListResponse`  
**Errors:** `404` not found, `500` server error

---

#### `DELETE /listings/batch`

Delete a batch.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `batch_id` | int | Yes |

**Response:** `{"success": true}`  
**Errors:** `404` not found, `500` server error

---

### Public API (`/api`)

#### `POST /api/create_batch`

Public endpoint (no authentication). Creates batch with system user.

**Auth:** No  
**Request Body:** `CreateBatchRequest` (same as authenticated batch creation)  
**Response:** `BatchResponse`  
**Errors:** `400` `BatchCreationError` with `{error, total_products, failed_count, failed_products: [{product_id, error_type, error_message}], timestamp}`, `500` server error

---

### Templates (`/templates`)

#### `GET /templates/list`

Get all templates (active only by default).

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required | Default |
|---|---|---|---|
| `active_only` | bool | No | true |

**Response:** `List[TemplateResponse]`

---

#### `GET /templates/product_fields`

Get available product fields from SellerCloud. Returns array of field objects with tags.

**Auth:** Yes  
**Response:** `List[ProductFieldSearchResponse]` — `[{id, tags, display_name}]`

---

#### `GET /templates/search_product_fields`

Search product fields by query string.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required | Default |
|---|---|---|---|
| `q` | string | No | `""` |

**Response:** `List[ProductFieldSearchResponse]`

---

#### `GET /templates/template_fields`

Get available fields from template definitions only.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required | Default |
|---|---|---|---|
| `template_id` | string | No | `"default"` |

**Response:** Dict with field definitions

---

#### `GET /templates/listingoptions_meta`

Get metadata about listing option tables.

**Auth:** Yes  
**Response:** Dict with table metadata

---

#### `GET /templates/detail`

Get template by ID.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `template_id` | string | Yes |

**Response:** `TemplateResponse`  
**Errors:** `404` not found

---

#### `POST /templates/create`

Create a new template. Validates template name format (must be a valid identifier).

**Auth:** Yes  
**Request Body:** `CreateTemplateRequest`

| Field | Type | Required | Validation |
|---|---|---|---|
| `name` | string | Yes | Valid identifier `^[a-zA-Z_][a-zA-Z0-9_]*$`, lowercased |
| `display_name` | string | Yes | — |
| `description` | string | No | — |
| `field_definitions` | List[FieldDefinition] | No | Default `[]` |

**Response:** `TemplateResponse`  
**Errors:** `400` name validation

---

#### `PUT /templates`

Update template metadata.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `template_name` | string | Yes |

**Request Body:** `UpdateTemplateRequest` — `{display_name, description, is_active}`  
**Response:** `TemplateResponse`  
**Errors:** `404` not found

---

#### `PUT /templates/with_fields`

Update template with complete field definitions.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `template_name` | string | Yes |

**Request Body:** `UpdateTemplateWithFieldsRequest` — `{display_name, description, is_active, field_definitions}`  
**Response:** `TemplateResponse`  
**Errors:** `404` not found

---

#### `POST /templates/add_field`

Add a field to a template.

**Auth:** Yes  
**Request Body:** `AddFieldToTemplateRequest` — `{template_name, field: FieldDefinition}`  
**Response:** `TemplateResponse`  
**Errors:** `400` field already exists, `404` template not found

---

#### `PUT /templates/update_field`

Update a field in a template.

**Auth:** Yes  
**Request Body:** `UpdateTemplateFieldRequest` — `{template_name, field_name, update_data}`  
**Response:** `TemplateResponse`  
**Errors:** `404` template/field not found

---

#### `DELETE /templates/field`

Remove a field from a template.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `template_name` | string | Yes |
| `field_name` | string | Yes |

**Response:** `TemplateResponse`  
**Errors:** `404` template/field not found

---

#### `POST /templates/reorder_fields`

Reorder template fields.

**Auth:** Yes  
**Request Body:** `ReorderTemplateFieldsRequest` — `{template_name, field_order: [field_names]}`  
**Response:** `TemplateResponse`  
**Errors:** `404` template not found

---

#### `DELETE /templates`

Delete a template.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `template_name` | string | Yes |

**Response:** `{"success": true}`  
**Errors:** `404` not found

---

### Product Images (`/products/images`)

#### `GET /products/images`

Get all images and washtags for a product.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `product_id` | string | Yes |

**Response:** Dict with images and washtags metadata/URLs

---

#### `POST /products/images/save`

Save product images: handles reorder, upload, and delete in one request. Uses optimistic concurrency control with advisory locks.

**Auth:** Yes  
**Content-Type:** `multipart/form-data`

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `product_id` | string | Yes | — | Product identifier |
| `updated_at` | string | No | — | Optimistic concurrency timestamp |
| `new_order` | JSON string | No | `"[]"` | Current index positions in desired order (e.g., `["2","1","3"]`) |
| `deleted_indices` | JSON string | No | `"[]"` | Indices to delete (e.g., `[4, 5]`) |
| `image_type` | string | No | `"image"` | `image` or `washtag` |
| `files` | List[UploadFile] | No | `[]` | New image files (appended after reordered images) |

**Validation:**
- Allowed content types: `image/jpeg`, `image/png`, `image/webp`
- Max file size: 30MB
- Max product images: 8
- Max washtag images: 3
- Max concurrent resize operations: 3

**Notes:** MD5 hash is computed on raw upload but replaced by full-size MD5 after image resize processing.

**Response:** Dict with saved image metadata  
**Errors:** `400` invalid content type/size/JSON, `409` concurrency conflict, `500` server error

---

### Settings (`/settings`)

#### `GET /settings/field_templates`

Get field templates from AppSettings. Auto-creates settings if not exist.

**Auth:** Yes  
**Response:** `SettingsResponse` — `{id, field_templates, created_at, updated_at}`

---

#### `PUT /settings/field_templates`

Update field templates. Validates platform and field names.

**Auth:** Yes  
**Request Body:** `UpdateSettingsRequest` — `{field_templates}`

**Validation:**
- Allowed platforms: `sellercloud`, `grailed`, `ebay`
- Allowed fields: `title`, `description`
- Supports both platform-based format and legacy flat format

**Response:** `SettingsResponse`  
**Errors:** `400` invalid platform/field names

---

#### `GET /settings/variables`

Get app variables with default `max_batches=50`.

**Auth:** Yes  
**Response:** `AppVariablesResponse` — `{app_variables: [{id, name, value}], updated_at}`

---

#### `PUT /settings/variables`

Update app variables.

**Auth:** Yes  
**Request Body:** `UpdateAppVariablesRequest` — `{app_variables: [{id, name, value}]}`  
**Response:** `AppVariablesResponse`

---

#### `GET /settings/platform_settings`

Get platform-specific settings.

**Auth:** Yes  
**Response:** `PlatformSettingsResponse` — `{platform_settings: {platform_id: {...}}, updated_at}`

---

#### `PUT /settings/platform_settings`

Update platform-specific settings. Re-reads from DB after save to confirm persistence.

**Auth:** Yes  
**Request Body:** `UpdatePlatformSettingsRequest` — `{platform_settings}`  
**Response:** `PlatformSettingsResponse`

---

#### `GET /settings/platform_meta`

Get platform metadata (icons, names) for enabled platforms only.

**Auth:** Yes  
**Response:** `PlatformMetaResponse` — `{platforms: [{id, name, icon, icon_mime_type}]}`

---

#### `GET /settings/platforms`

Get list of enabled platforms from app_settings (default: `["sellercloud", "grailed"]`).

**Auth:** Yes  
**Response:** `EnabledPlatformsResponse` — `{platforms: [...], updated_at}`

---

#### `PUT /settings/platforms`

Update list of enabled platforms.

**Auth:** Yes  
**Request Body:** `UpdateEnabledPlatformsRequest` — `{platforms: [...]}`  
**Response:** `EnabledPlatformsResponse`

---

## Listing Options API (`/listingoptions`)

### Tables (`/listingoptions/tables`)

All tables are stored with `listingoptions_` prefix in the default database.

#### `POST /listingoptions/tables/create`

Create a new table with primary business column.

**Auth:** Yes  
**Request Body:** `CreateTableRequest`

| Field | Type | Required | Validation |
|---|---|---|---|
| `table_name` | string | Yes | Valid identifier, lowercased |
| `display_name` | string | Yes | — |
| `primary_business_column` | string | Yes | Valid identifier, lowercased |
| `primary_business_display_name` | string | Yes | — |
| `list_type` | string | No | `default` or `sizing` |

**Response:** `SuccessResponse`  
**Errors:** `400` invalid name, `409` table exists, `500` server error

---

#### `POST /listingoptions/tables/add_column`

Add column to existing table. Handles type conversion and indexing.

**Auth:** Yes  
**Request Body:** `AddColumnRequest` — `{table_name, column: ColumnDefinition}`  
**Response:** `SuccessResponse`  
**Errors:** `400` invalid column, `404` table not found, `409` column exists

---

#### `PUT /listingoptions/tables/update_column`

Update column metadata in schema.

**Auth:** Yes  
**Request Body:** `UpdateColumnRequest` — `{table_name, column_name, update_data: ColumnDefinitionUpdate}`  
**Response:** `SuccessResponse`  
**Errors:** `404` table/column not found

---

#### `POST /listingoptions/tables/reorder_columns`

Reorder table columns.

**Auth:** Yes  
**Request Body:** `{table_name, ordered_column_names: [...]}`  
**Response:** `SuccessResponse`

---

#### `GET /listingoptions/tables/list`

List all managed tables.

**Auth:** Yes  
**Response:** List of Schema entries with metadata

---

#### `GET /listingoptions/tables/schema`

Get table schema with RJSF support (JSON Schema + UI Schema).

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `table_name` | string | Yes |

**Response:** `TableSchemaResponse` — `{table, display_name, primary_business_column, column_schema, list_schema, list_type, json_schema, ui_schema, created_at, updated_at}`  
**Errors:** `404` table not found

---

#### `GET /listingoptions/tables/records`

Get records with pagination, filtering, and search. Supports type conversion on retrieval.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required | Default |
|---|---|---|---|
| `table_name` | string | Yes | — |
| `page` | int | No | 1 |
| `page_size` | int | No | 50 (max 1000) |
| `search` | string | No | — |
| `sort_by` | string | No | — |
| `sort_order` | string | No | `asc` |
| `filters` | JSON string | No | — |

**Response:** `PaginatedResponse` — `{items, total, page, page_size, total_pages}`

---

#### `POST /listingoptions/tables/records`

Create new record with platform mappings.

**Auth:** Yes  
**Request Body:** `{table_name, data: RecordData, ...}`  
**Response:** Dict with created record  
**Errors:** `400` validation, `409` duplicate (unique constraint)

---

#### `PUT /listingoptions/tables/records`

Update record with platform mappings.

**Auth:** Yes  
**Request Body:** `{table_name, record_id, data: RecordData, ...}`  
**Response:** Dict with updated record  
**Errors:** `404` not found, `409` duplicate

---

#### `DELETE /listingoptions/tables/records`

Delete record with cascading.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `table_name` | string | Yes |
| `record_id` | string | Yes |

**Response:** `SuccessResponse`  
**Errors:** `404` not found

---

#### `GET /listingoptions/tables/fuzzy_check`

Check for fuzzy duplicates using PostgreSQL trigram similarity.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required | Default |
|---|---|---|---|
| `table_name` | string | Yes | — |
| `column_name` | string | Yes | — |
| `value` | string | Yes | — |
| `threshold` | float | No | 0.3 (0.0-1.0) |

**Response:** `FuzzyCheckResponse` — `{similar_values, exact_matches, exact_match (computed)}`

---

#### `POST /listingoptions/tables/fuzzy_check_list`

Batch check multiple values for duplicates.

**Auth:** Yes  
**Request Body:** `{table_name, values: [...]}`  
**Response:** Dict with batch fuzzy check results

---

#### `POST /listingoptions/tables/enable_fuzzy_primary_columns`

Enable fuzzy checking (trigram GIN indexes) for all primary business columns.

**Auth:** Yes  
**Response:** `SuccessResponse`

---

#### `GET /listingoptions/tables/exists`

Check if table exists.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `table_name` | string | Yes |

**Response:** `{"exists": bool}`

---

#### `GET /listingoptions/tables/column_exists`

Check if column exists in table.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `table_name` | string | Yes |
| `column_name` | string | Yes |

**Response:** `{"exists": bool}`

---

#### `GET /listingoptions/tables/primary_values`

Get primary column values for dropdown (limited to 10,000).

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `table_name` | string | Yes |

**Response:** List of primary column values

---

#### `GET /listingoptions/tables/platform_values`

Get platform values for dropdown.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `table_name` | string | Yes |
| `platform_id` | string | Yes |

**Response:** List of platform values

---

#### `POST /listingoptions/tables/list_schemas`

Add list schema definition to table.

**Auth:** Yes  
**Request Body:** `{table_name, list_schema: ListSchemaDefinition}`  
**Response:** `SuccessResponse`

---

#### `GET /listingoptions/tables/list_schemas`

Get list schema definitions for table.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `table_name` | string | Yes |

**Response:** List of `ListSchemaDefinition`

---

#### `PUT /listingoptions/tables/list_schemas`

Update list schema definition.

**Auth:** Yes  
**Request Body:** `{table_name, platform_id, list_type, update_data: ListSchemaDefinitionUpdate}`  
**Response:** `SuccessResponse`

---

#### `DELETE /listingoptions/tables/list_schemas`

Delete list schema.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `table_name` | string | Yes |
| `platform_id` | string | Yes |
| `list_type` | string | Yes |

**Response:** `SuccessResponse`

---

#### `GET /listingoptions/tables/records_lookup`

Lookup record ID by column value.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `table_name` | string | Yes |
| `column_name` | string | Yes |
| `value` | string | Yes |

**Response:** Dict with record ID

---

#### `GET /listingoptions/tables/records/mappings`

Get platform mappings for a record.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `table_name` | string | Yes |
| `record_id` | string | Yes |

**Response:** Dict with platform mappings

---

#### `GET /listingoptions/tables/platform_list_options`

Get all platform dropdown values.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `table_name` | string | Yes |

**Response:** Dict with platform dropdown options

---

#### `GET /listingoptions/tables/types/check_code`

Check type code uniqueness.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `class_code` | string | Yes |
| `old_class_code` | string | No |

**Response:** Dict with conflict info

---

#### `GET /listingoptions/tables/export`

Export table to Excel.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `table_name` | string | Yes |

**Response:** StreamingResponse (Excel file, `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet`)

---

### Lists (`/listingoptions/lists`)

#### `GET /listingoptions/lists/records`

Get mapping list records with pagination.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required | Default |
|---|---|---|---|
| `table_name` | string | Yes | — |
| `list_type` | string | No | — |
| `platform_id` | string | No | — |
| `page` | int | No | 1 |
| `page_size` | int | No | 50 |
| `search` | string | No | — |

**Response:** `PaginatedResponse`

---

#### `POST /listingoptions/lists/default`

Add entry to default list.

**Auth:** Yes  
**Request Body:** `DefaultListEntry` — `{primary_id, platform_value, platform_id, primary_table_column}`  
**Response:** Dict with created entry

---

#### `POST /listingoptions/lists/sizing`

Add entry to sizing list.

**Auth:** Yes  
**Request Body:** `SizingListEntry` — `{sizing_scheme, platform_value, platform, value}`  
**Response:** Dict with created entry

---

#### `PUT /listingoptions/lists/default`

Update default list entry.

**Auth:** Yes  
**Request Body:** `{table_name, entry_id, platform_value, ...}`  
**Response:** Dict with updated entry

---

#### `PUT /listingoptions/lists/default/internal_values`

Synchronize internal values for platform mapping. Accepts conflict confirmation with `confirmed` field.

**Auth:** Yes  
**Request Body:** `DefaultListInternalValuesUpdate`

| Field | Type | Required | Description |
|---|---|---|---|
| `platform_id` | string | Yes | Platform ID |
| `platform_value` | string | Yes | Platform value key |
| `internal_values` | List[str] | Yes | Complete set of internal values to link |
| `confirmed` | bool | No | Confirm re-mapping of conflicting values |
| `sizing_type` | string | No | Sizing type scope (only for sizes table) |

**Response:** Dict with sync result  
**Errors:** `409` conflict requires confirmation

---

#### `PUT /listingoptions/lists/sizing`

Update sizing list entry.

**Auth:** Yes  
**Request Body:** `{table_name, entry_id, ...}`  
**Response:** Dict with updated entry

---

#### `DELETE /listingoptions/lists/entry`

Delete list entry.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `table_name` | string | Yes |
| `entry_id` | int | Yes |
| `list_type` | string | Yes |

**Response:** `SuccessResponse`

---

#### `GET /listingoptions/lists/platforms`

Get platforms for table.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `table_name` | string | Yes |

**Response:** List of platforms

---

#### `GET /listingoptions/lists/entry`

Get specific list entry by ID.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `table_name` | string | Yes |
| `entry_id` | int | Yes |
| `list_type` | string | Yes |

**Response:** Dict with entry data

---

#### `POST /listingoptions/lists/bulk_import`

Bulk import CSV/JSON entries. Supports CSV single-column upload for default lists only. Processes in chunks of 1000 with conflict handling.

**Auth:** Yes  
**Request Body:** multipart form or JSON  
**Response:** Dict with import results (created, skipped, errors)

---

#### `POST /listingoptions/lists/create_mapping_table`

Explicitly create mapping table with indexes.

**Auth:** Yes  
**Request Body:** `{table_name, list_type}`  
**Response:** `SuccessResponse`

---

#### `GET /listingoptions/lists/export`

Export list entries to Excel.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `table_name` | string | Yes |
| `list_type` | string | No |
| `platform_id` | string | No |

**Response:** StreamingResponse (Excel file)

---

### Platforms (`/listingoptions/platforms`)

#### `GET /listingoptions/platforms/list`

List all platforms.

**Auth:** Yes  
**Response:** List of `PlatformResponse` — `[{id, name, icon, icon_mime_type}]`

---

#### `GET /listingoptions/platforms/get`

Get specific platform by ID.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `platform_id` | string | Yes |

**Response:** `PlatformResponse`  
**Errors:** `404` not found

---

#### `POST /listingoptions/platforms/create`

Create new platform with optional icon.

**Auth:** Yes  
**Request Body:** `{id, name, icon (base64), icon_mime_type}`

**Validation:** Icon max 5KB file size, base64 encoded  
**Response:** `PlatformResponse`  
**Errors:** `409` platform already exists

---

#### `PUT /listingoptions/platforms/update`

Update platform details.

**Auth:** Yes  
**Request Body:** `{platform_id, name, icon, icon_mime_type}`  
**Response:** `PlatformResponse`  
**Errors:** `404` not found

---

#### `DELETE /listingoptions/platforms/delete`

Delete platform.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `platform_id` | string | Yes |

**Response:** `SuccessResponse`  
**Errors:** `404` not found

---

#### `GET /listingoptions/platforms/exists`

Check platform existence.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `platform_id` | string | Yes |

**Response:** `{"exists": bool}`

---

### Sizing Schemes (`/listingoptions/sizing_schemes`)

#### `GET /listingoptions/sizing_schemes/sizes`

Get sizing scheme options for dropdown.

**Auth:** Yes  
**Response:** List of scheme names

---

#### `POST /listingoptions/sizing_schemes`

Create sizing scheme with sizes. Transaction with duplicate checks.

**Auth:** Yes  
**Request Body:** `FullSizingSchemeCreate`

| Field | Type | Required | Validation |
|---|---|---|---|
| `sizing_scheme` | string | Yes | Max 50 chars |
| `sizes` | List[SizingSchemeEntryBase] | Yes | Each: no spaces or `/` in size value |
| `sizing_types` | List[str] | No | — |

**Constraint:** `(sizing_scheme, size)` must be unique  
**Response:** Dict with created scheme  
**Errors:** `400` duplicate size, `409` scheme exists

---

#### `GET /listingoptions/sizing_schemes`

List all sizing schemes (with optional details).

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required | Default |
|---|---|---|---|
| `include_details` | bool | No | false |

**Response:** List of scheme names or `AllSizingSchemesResponse`

---

#### `GET /listingoptions/sizing_schemes/detail`

Get sizing scheme details by name.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `sizing_scheme` | string | Yes |

**Response:** `SizingSchemeDetailResponse` — `{sizing_scheme, sizes: [{id, size, order}], sizing_types}`  
**Errors:** `404` not found

---

#### `PUT /listingoptions/sizing_schemes`

Replace all sizes and orders (optionally renames scheme).

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `sizing_scheme` | string | Yes |

**Request Body:** `UpdateSizeOrderRequest` — `{new_sizing_scheme, sizes, sizing_types}`  
**Response:** Dict with updated scheme  
**Errors:** `404` not found, `409` name conflict

---

#### `DELETE /listingoptions/sizing_schemes`

Delete entire sizing scheme.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `sizing_scheme` | string | Yes |

**Response:** `SuccessResponse`  
**Errors:** `404` not found

---

#### `POST /listingoptions/sizing_schemes/sizes`

Add size to scheme.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `sizing_scheme` | string | Yes |

**Request Body:** `SizingSchemeEntryBase` — `{size, order}`  
**Response:** `SizingSchemeEntryDB`  
**Errors:** `409` duplicate size

---

#### `GET /listingoptions/sizing_schemes/sizes/detail`

Get specific size entry from scheme.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `sizing_scheme` | string | Yes |
| `size` | string | Yes |

**Response:** `SizingSchemeEntryDB`  
**Errors:** `404` not found

---

#### `PUT /listingoptions/sizing_schemes/sizes`

Update size value/order.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `sizing_scheme` | string | Yes |
| `size` | string | Yes |

**Request Body:** `{new_size, new_order}`  
**Response:** `SizingSchemeEntryDB`  
**Errors:** `404` not found, `409` conflict

---

#### `DELETE /listingoptions/sizing_schemes/sizes`

Delete size from scheme.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `sizing_scheme` | string | Yes |
| `size` | string | Yes |

**Response:** `SuccessResponse`  
**Errors:** `404` not found

---

#### `GET /listingoptions/sizing_schemes/entries_by_name`

Get all scheme entries with IDs.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `sizing_scheme` | string | Yes |

**Response:** List of entries with IDs, ordered by `order`

---

#### `GET /listingoptions/sizing_schemes/sizing_type_options`

Get available sizing types from types table schema.

**Auth:** Yes  
**Response:** List of sizing type strings

---

#### `GET /listingoptions/sizing_schemes/platform_default_sizes`

Get default sizes by platform from sizes_default_list table.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `platform_id` | string | Yes |

**Response:** Dict with platform default sizes

---

#### `GET /listingoptions/sizing_schemes/export`

Export sizing schemes to Excel.

**Auth:** Yes  
**Response:** StreamingResponse (Excel file) with columns: Sizing Scheme, Size, Sizing Types

---

### Sizing Lists (`/listingoptions/sizing_lists`)

#### `POST /listingoptions/sizing_lists`

Create sizing list platform entry. Validates scheme entry and platform exist.

**Auth:** Yes  
**Request Body:** `SizingListPlatformEntryCreate`

| Field | Type | Required |
|---|---|---|
| `sizing_scheme_entry_id` | UUID | Yes |
| `platform_id` | string | Yes |
| `platform_value` | string | Yes |
| `sizing_type` | string | Yes |

**Response:** `SizingListPlatformEntryDetail`  
**Errors:** `400` invalid references, `409` duplicate

---

#### `GET /listingoptions/sizing_lists`

List all sizing list entries with pagination and filters. Window function for total count.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required | Default |
|---|---|---|---|
| `page` | int | No | 1 |
| `page_size` | int | No | 50 |
| `sizing_scheme_name` | string | No | — |
| `platform_id` | string | No | — |
| `size_value` | string | No | — |
| `platform_value` | string | No | — |
| `sizing_type` | string | No | — |

**Response:** `PaginatedSizingListPlatformEntryResponse` — `{items: [SizingListPlatformEntryDetail], total, page, page_size, total_pages}`

---

#### `GET /listingoptions/sizing_lists/detail`

Get specific sizing list entry by ID.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `entry_id` | UUID | Yes |

**Response:** `SizingListPlatformEntryDetail`  
**Errors:** `404` not found

---

#### `PUT /listingoptions/sizing_lists/update`

Update sizing list entry (platform_value and/or sizing_type).

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `entry_id` | UUID | Yes |

**Request Body:** `SizingListPlatformEntryUpdate` — `{platform_value, sizing_type}`  
**Response:** `SizingListPlatformEntryDetail`  
**Errors:** `404` not found, `409` duplicate after update

---

#### `DELETE /listingoptions/sizing_lists/delete`

Delete sizing list entry.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `entry_id` | UUID | Yes |

**Response:** `SuccessResponse`  
**Errors:** `404` not found

---

### Parent Types (`/listingoptions/parent_types`)

#### `GET /listingoptions/parent_types`

Get parent types with pagination and search.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required | Default |
|---|---|---|---|
| `search` | string | No | — |
| `page` | int | No | 1 |
| `page_size` | int | No | 50 |
| `fetch_all` | bool | No | false |

**Response:** `PaginatedResponse` of `ParentTypeResponse`

---

#### `GET /listingoptions/parent_types/divisions`

Get list of divisions.

**Auth:** Yes  
**Response:** List of division strings

---

#### `GET /listingoptions/parent_types/genders`

Get gender enum values.

**Auth:** Yes  
**Response:** `["Mens", "Womens", "Boys", "Girls", "Unisex", "Does Not Apply"]`

---

#### `GET /listingoptions/parent_types/reporting_categories`

Get reporting category values.

**Auth:** Yes  
**Response:** `["Jeans", "Hoodies & Sweatshirts", "T-Shirts", "Bottoms", "Outerwear", "Tops", "Suiting", "Accessories", "Sneakers", "Footwear", "Bags"]`

---

#### `GET /listingoptions/parent_types/departments`

Get departments by division.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `division` | string | Yes |

**Response:** List of department objects

---

#### `GET /listingoptions/parent_types/classes`

Get classes by division and department.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `division` | string | Yes |
| `dept` | string | Yes |

**Response:** List of class objects

---

#### `GET /listingoptions/parent_types/hierarchy`

Get parent hierarchy tree.

**Auth:** Yes  
**Response:** Dict with hierarchical structure

---

#### `DELETE /listingoptions/parent_types`

Delete parent type.

**Auth:** Yes  
**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `parent_type_id` | UUID | Yes |

**Response:** `SuccessResponse`  
**Errors:** `404` not found

---

#### `PUT /listingoptions/parent_types`

Update parent type. Validates class name prefix matches gender.

**Auth:** Yes  
**Request Body:** `{parent_type_id, division, dept_code, class_name, class_code_suffix, gender, reporting_category}`

**Validation:** Class name must start with gender-specific prefix (e.g., `M_` for Mens)  
**Response:** `ParentTypeResponse`  
**Errors:** `400` validation, `404` not found, `409` code conflict

---

#### `POST /listingoptions/parent_types`

Create parent type.

**Auth:** Yes  
**Request Body:** `{division, dept_code, dept, class_name, class_code_suffix, gender, reporting_category}`  
**Response:** `ParentTypeResponse`  
**Errors:** `400` validation, `409` duplicate

---

### Listing Options Public API (`/api/listingoptions`)

All endpoints in this section are **unauthenticated** (mounted under `/api`).

#### `GET /api/listingoptions/records`

Search records by primary column.

**Query Parameters:**

| Param | Type | Required |
|---|---|---|
| `table_name` | string | Yes |
| `search` | string | No |
| `limit` | int | No |

**Response:** List of matching records

---

#### `GET /api/listingoptions/brand_by_name`

Get brand by exact name.

**Query Parameters:** `name` (string, required)  
**Response:** Brand record or null

---

#### `GET /api/listingoptions/color_by_name`

Get color by exact name.

**Query Parameters:** `name` (string, required)  
**Response:** Color record or null

---

#### `GET /api/listingoptions/type_by_name`

Get type by exact name.

**Query Parameters:** `name` (string, required)  
**Response:** Type record or null

---

#### `POST /api/listingoptions/bulk_brands_by_name`

Get multiple brands by name.

**Request Body:** `BulkSearchByNameRequest` — `{names: [...], search_alias: true}`  
**Response:** Dict mapping names to records

---

#### `POST /api/listingoptions/bulk_colors_by_name`

Get multiple colors by name.

**Request Body:** `BulkSearchByNameRequest`  
**Response:** Dict mapping names to records

---

#### `POST /api/listingoptions/bulk_types_by_name`

Get multiple types by name.

**Request Body:** `BulkSearchTypeNameRequest` — `{types: [...]}`  
**Response:** Dict mapping names to records

---

#### `GET /api/listingoptions/get_table`

Export table records.

**Query Parameters:** `table_name` (string, required)  
**Response:** List of all records

---

#### `GET /api/listingoptions/product_types_by_class`

Get product types by class name.

**Query Parameters:** `class_name` (string, required)  
**Response:** List of product types

---

#### `POST /api/listingoptions/bulk_product_types_by_class`

Bulk get product types by class names.

**Request Body:** `BulkGetProductTypesByClassRequest` — `{class_names: [...]}`  
**Response:** Dict mapping class names to product types

---

#### `GET /api/listingoptions/sizing_schemes_by_product_type`

Get sizing schemes by product type.

**Query Parameters:** `product_type` (string, required)  
**Response:** List of sizing schemes

---

#### `POST /api/listingoptions/bulk_sizing_schemes_by_product_type`

Bulk get sizing schemes by product types.

**Request Body:** `BulkGetSizingSchemesByProductTypeRequest` — `{product_types: [...]}`  
**Response:** Dict mapping product types to sizing schemes

---

#### `GET /api/listingoptions/get_classes`

Get all classes (parent types).

**Response:** List of all parent type records

---

#### `GET /api/listingoptions/get_types`

Get all types.

**Response:** List of all type records

---

#### `GET /api/listingoptions/get_brands`

Get all brands.

**Response:** List of all brand records

---

#### `GET /api/listingoptions/get_sizes`

Get sizes with their associated sizing schemes.

**Response:** List of sizes mapped to scheme names

---

#### `GET /api/listingoptions/tables/list`

List all tables (public access).

**Response:** List of Schema entries

---

## Data Models

### Database Models

#### Template (table: `templates`)

| Field | Type | Constraints | Description |
|---|---|---|---|
| `id` | CharField | PK, max 100 | Template ID |
| `name` | CharField | unique, max 100 | Template name (identifier) |
| `display_name` | CharField | max 200 | Human-readable name |
| `description` | TextField | nullable | Description |
| `field_definitions` | JSONField | — | Array of FieldDefinition objects |
| `is_active` | BooleanField | default true | Active/visible flag |
| `created_at` | DatetimeField | auto | — |
| `updated_at` | DatetimeField | auto | — |

**Methods:** `field_count` (property), `get_field_by_name()`, `add_field()`, `remove_field()`, `reorder_fields()`

---

#### Batch (table: `batches`)

| Field | Type | Constraints | Description |
|---|---|---|---|
| `id` | IntField | PK | Batch ID |
| `comment` | TextField | nullable | Description/comment |
| `assigned_to` | CharField | indexed, nullable, max 100 | Assigned user ID |
| `priority` | CharField | indexed, default `medium`, max 10 | `low`, `medium`, `high` |
| `created_by` | CharField | max 100 | Creator user ID |
| `status` | CharField | indexed, default `new`, max 20 | `new`, `in_progress`, `completed` |
| `total_listings` | IntField | default 0 | Total listings count |
| `submitted_listings` | IntField | default 0 | Submitted listings count |
| `photography_batch_id` | IntField | nullable | Photography batch reference |
| `platform_submission_statuses` | JSONField | default {} | `{product_id: {platform_id: status}}` |
| `created_at` | DatetimeField | auto | — |
| `updated_at` | DatetimeField | auto | — |

**Properties:** `progress_percentage`, `is_completed`

---

#### Listing (table: `listings`)

| Field | Type | Constraints | Description |
|---|---|---|---|
| `id` | UUIDField | PK, auto-generated | Listing UUID |
| `product_id` | CharField | indexed, max 200 | SellerCloud product ID |
| `info_product_id` | CharField | nullable, max 255 | Full SC product ID with variations |
| `assigned_to` | CharField | indexed, nullable, max 100 | Assigned user ID |
| `data` | JSONField | default {} | Form data (template schema) |
| `ai_response` | JSONField | nullable | AI suggestions |
| `ai_description` | TextField | nullable | AI generated description |
| `original_description` | TextField | nullable | Original SellerCloud description |
| `submitted` | BooleanField | default false | Submission flag |
| `submitted_at` | DatetimeField | nullable | Submission timestamp |
| `submitted_by` | CharField | nullable, max 100 | Submitter user ID |
| `error` | TextField | nullable | Post-submission error traceback |
| `upload_status` | CharField | default `pending`, max 20 | `pending` or `uploaded` |
| `created_by` | CharField | max 100 | Creator user ID |
| `batch` | ForeignKey(Batch) | nullable, SET_NULL | Batch relationship |
| `created_at` | DatetimeField | auto | — |
| `updated_at` | DatetimeField | auto | — |

**Methods:** `is_completed` (property), `get_submission_summary()`, `has_successful_submission()`

---

#### ListingSubmission (table: `listing_submissions`)

| Field | Type | Constraints | Description |
|---|---|---|---|
| `id` | IntField | PK | Submission ID |
| `listing` | ForeignKey(Listing) | nullable, SET_NULL | Parent listing |
| `platform_id` | CharField | indexed, max 50 | Platform identifier |
| `status` | CharField | indexed, default `pending`, max 20 | `queued`, `pending`, `processing`, `success`, `failed` |
| `submitted_by` | CharField | nullable, max 100 | Submitter user ID |
| `submitted_at` | DatetimeField | nullable | Completion timestamp |
| `error` | TextField | nullable | Technical error/traceback |
| `error_display` | TextField | nullable | Human-friendly error for UI |
| `platform_status` | CharField | nullable, max 50 | Granular platform progress (e.g., `products_processing`, `offers_processing`, `listed`) |
| `platform_meta` | JSONField | nullable | Transient tracking data (e.g., `{product_import_id: 123}`) |
| `attempt_number` | IntField | default 1 | Retry tracking |
| `external_id` | JSONField | nullable | Platform reference ID(s) after success |
| `created_at` | DatetimeField | auto | — |
| `updated_at` | DatetimeField | auto | — |

**Unique constraint:** `(listing, platform_id, attempt_number)`

---

#### AppSettings (table: `app_settings`)

| Field | Type | Default | Description |
|---|---|---|---|
| `id` | IntField | PK | — |
| `field_templates` | JSONField | `{}` | `{field_name: {template: '...'}}` |
| `app_variables` | JSONField | `[{id: "max_batches", name: "Maximum Batch Size", value: 50}]` | Config variables |
| `platform_settings` | JSONField | `{}` | `{platform_id: {enabled, price_multiplier, shipping}}` |
| `platforms` | JSONField | `["sellercloud", "grailed"]` | Enabled platform IDs |
| `created_at` | DatetimeField | auto | — |
| `updated_at` | DatetimeField | auto | — |

---

#### SubmissionStatus (Enum)

| Value | Description |
|---|---|
| `queued` | Accepted, waiting for poller (e.g., batch_submit or requires_images) |
| `pending` | Ready for submission |
| `processing` | Actively being submitted |
| `success` | Completed successfully |
| `failed` | Failed |

**Status groups:** `TERMINAL_STATUSES = {success, failed}`, `IN_FLIGHT_STATUSES = {queued, pending, processing}`

---

#### Platform (table: `listingoptions_platforms`)

| Field | Type | Constraints |
|---|---|---|
| `id` | CharField | PK, max 50 |
| `name` | CharField | max 255, not null |
| `icon` | TextField | nullable (base64) |
| `icon_mime_type` | CharField | nullable, max 50 |

---

#### Schema (table: `listingoptions_schema`)

| Field | Type | Description |
|---|---|---|
| `table` | CharField | PK, max 255 |
| `column_schema` | JSONField | Array of ColumnDefinition |
| `list_schema` | JSONField | Array of ListSchemaDefinition |
| `display_name` | CharField | nullable, max 255 |
| `primary_business_column` | CharField | nullable, max 255 |
| `list_type` | CharField | default `default`, max 50 |
| `created_at` | DatetimeField | auto |
| `updated_at` | DatetimeField | auto |

---

#### SizingScheme (table: `listingoptions_sizing_schemes`)

| Field | Type | Constraints |
|---|---|---|
| `id` | UUIDField | PK |
| `sizing_scheme` | CharField | indexed, max 50 |
| `size` | TextField | — |
| `order` | IntField | — |
| `sizing_types` | JSONField | nullable, GIN indexed |
| `created_at` | DatetimeField | auto |
| `updated_at` | DatetimeField | auto |

**Unique constraint:** `(sizing_scheme, size)`  
**Ordering:** `[sizing_scheme, order]`

---

#### SizingList (table: `listingoptions_sizing_lists`)

| Field | Type | Constraints |
|---|---|---|
| `id` | UUIDField | PK |
| `sizing_scheme_entry` | FK(SizingScheme) | CASCADE |
| `platform` | FK(Platform) | CASCADE |
| `platform_value` | CharField | max 255 |
| `sizing_type` | CharField | indexed, max 255 |
| `created_at` | DatetimeField | auto |
| `updated_at` | DatetimeField | auto |

**Unique constraint:** `(sizing_scheme_entry, platform, sizing_type)`

---

#### ParentType (table: `listingoptions_types_parents`)

| Field | Type | Constraints |
|---|---|---|
| `id` | UUIDField | PK |
| `division` | CharField | indexed, max 255 |
| `dept_code` | IntField | indexed |
| `dept` | CharField | indexed, max 255 |
| `class_code` | IntField | unique, indexed |
| `class_name` | CharField | indexed, max 255 (column: `class`) |
| `gender` | CharEnumField(Gender) | indexed, max 20 |
| `reporting_category` | CharEnumField(ReportingCategory) | indexed, max 50 |
| `created_at` | DatetimeField | auto |
| `updated_at` | DatetimeField | auto |

**Unique constraints:** `(division, dept, class_name)`, `(division, dept_code, class_code)`

**Gender values:** `Mens`, `Womens`, `Boys`, `Girls`, `Unisex`, `Does Not Apply`

**Reporting categories:** `Jeans`, `Hoodies & Sweatshirts`, `T-Shirts`, `Bottoms`, `Outerwear`, `Tops`, `Suiting`, `Accessories`, `Sneakers`, `Footwear`, `Bags`

---

### Request/Response Models (Main)

See `API/models/api_models.py` for full Pydantic definitions. Key models:

| Model | Type | Key Fields |
|---|---|---|
| `PlatformMapping` | Request | platform_id, field_id, is_custom, platform_tags |
| `FieldDefinition` | Shared | name, display_name, type, order, is_required, is_unique, options, multiselect, platforms, ai_tagging, ui_size (1-12), mapped_table, mapped_column |
| `CreateTemplateRequest` | Request | name (valid identifier), display_name, description, field_definitions |
| `UpdateTemplateRequest` | Request | display_name, description, is_active |
| `UpdateTemplateWithFieldsRequest` | Request | display_name, description, is_active, field_definitions |
| `AddFieldToTemplateRequest` | Request | template_name, field |
| `UpdateTemplateFieldRequest` | Request | template_name, field_name, update_data |
| `ReorderTemplateFieldsRequest` | Request | template_name, field_order |
| `TemplateResponse` | Response | id, name, display_name, description, field_definitions, field_count, is_active, timestamps |
| `ProductFieldSearchResponse` | Response | id, tags, display_name |
| `CreateListingRequest` | Request | product_id, info_product_id, assigned_to, template_id, data |
| `UpdateListingRequest` | Request | assigned_to, data, ai_response, ai_description, submitted, submitted_by |
| `SubmitListingRequest` | Request | platforms (optional list) |
| `SaveSizeMappingRequest` | Request | sizing_scheme_entry_id, platform_id, platform_value (null=delete), sizing_type |
| `ListingResponse` | Response | id, product_id, info_product_id, assigned_to, data, ai fields, submission fields, timestamps |
| `ProductConfirmationData` | Response | product, existing_listing_id |
| `ListingSchemaResponse` | Response | json_schema, ui_schema, template_info |
| `BatchConfirmationRequest` | Request | product_ids (1-1000) |
| `BatchConfirmationResponse` | Response | products, total_count, success_count, existing_draft_count, error_count |
| `CreateBatchRequest` | Request | product_ids (1-1000), comment, assigned_to, priority, photography_batch_id |
| `UpdateBatchRequest` | Request | comment, assigned_to, priority |
| `BatchResponse` | Response | id, comment, assigned_to, priority, status, totals, progress, listings, timestamps |
| `BatchListResponse` | Response | Summary without listings array |
| `BatchFilterOptionsResponse` | Response | users, priorities, statuses |
| `AddProductRequest` | Request | child_sku (must contain `/`), title, upc, mpn, brand_code, type_code, serial_number, company_code |
| `AddSizeRequest` | Request | parent_sku (no `/`), size, upc (8/12/13 digits), cost_price (>0) |
| `ReassignChildRequest` | Request | child_sku, new_parent_sku (no `/`), target_child_sku (must have `/`) |
| `BulkReassignRequest` | Request | old_parent_sku, new_parent_sku, mappings |
| `BulkReassignStatusResponse` | Response | success, bulk_assignment_id, status, total, completed, failed, assignments |
| `ProductSearchResponse` | Response | results, total, exact_match |
| `ProductDetailsResponse` | Response | sku, is_parent, title, mpn, brand, product_type, sizing_scheme, children, etc. |
| `UpdateSettingsRequest` | Request | field_templates |
| `SettingsResponse` | Response | id, field_templates, timestamps |
| `UpdateAppVariablesRequest` | Request | app_variables |
| `AppVariablesResponse` | Response | app_variables, updated_at |
| `UpdatePlatformSettingsRequest` | Request | platform_settings |
| `PlatformSettingsResponse` | Response | platform_settings, updated_at |
| `PlatformMeta` | Response | id, name, icon, icon_mime_type |
| `EnabledPlatformsResponse` | Response | platforms, updated_at |
| `ListingSubmissionResponse` | Response | id, listing_id, platform_id, status, timestamps, error_display, external_id |
| `SubmissionSummary` | Response | total_platforms, successful, failed, pending, platforms |

---

### Request/Response Models (Listing Options)

See `API/listingoptions/models/api_models.py` for full Pydantic definitions. Key models:

| Model | Type | Key Fields |
|---|---|---|
| `ColumnDefinition` | Shared | name, display_name, type, order, is_required, is_unique, is_primary_column, display_on_ui, display_in_form, fuzzy_check, options, multiselect |
| `ListSchemaDefinition` | Shared | platform_id, list_type (`default`/`sizing`), display_name, enabled, min/max_length, regex |
| `ListSchemaDefinitionUpdate` | Request | All fields optional |
| `CreateTableRequest` | Request | table_name, display_name, primary_business_column, primary_business_display_name, list_type |
| `AddColumnRequest` | Request | table_name, column |
| `UpdateColumnRequest` | Request | table_name, column_name, update_data |
| `RecordData` | Request | data (Dict) |
| `DefaultListEntry` | Request | primary_id, platform_value, platform_id, primary_table_column |
| `SizingListEntry` | Request | sizing_scheme, platform_value, platform, value |
| `PlatformResponse` | Response | id, name, icon, icon_mime_type |
| `FuzzyCheckResponse` | Response | similar_values, exact_matches, exact_match (computed) |
| `PaginatedResponse` | Response | items, total, page, page_size, total_pages |
| `SuccessResponse` | Response | success=true, message |
| `ErrorResponse` | Response | success=false, error, details |
| `SizingSchemeEntryBase` | Shared | size (no spaces/slashes), order |
| `FullSizingSchemeCreate` | Request | sizing_scheme (max 50), sizes, sizing_types |
| `UpdateSizeOrderRequest` | Request | new_sizing_scheme, sizes, sizing_types |
| `SizingSchemeDetailResponse` | Response | sizing_scheme, sizes, sizing_types |
| `SizingListPlatformEntryCreate` | Request | sizing_scheme_entry_id, platform_id, platform_value, sizing_type |
| `SizingListPlatformEntryUpdate` | Request | platform_value, sizing_type |
| `SizingListPlatformEntryDetail` | Response | All base fields + sizing_scheme_name, size_value, size_order, platform_name |
| `DefaultListInternalValuesUpdate` | Request | platform_id, platform_value, internal_values, confirmed, sizing_type |
| `ParentTypeResponse` | Response | id, division, dept_code, dept, class_code, class_name, gender, reporting_category |
| `BulkSearchByNameRequest` | Request | names, search_alias |
| `BulkSearchTypeNameRequest` | Request | types |
| `BulkGetProductTypesByClassRequest` | Request | class_names |
| `BulkGetSizingSchemesByProductTypeRequest` | Request | product_types |
| `AddAliasResponse` | Response | record_found, data, added_aliases, failed_aliases |

---

## Error Reference

### HTTP Status Codes

| Code | Meaning | Common Triggers |
|---|---|---|
| `400` | Bad Request | Validation failures, invalid input, MPN conflicts, size duplicates |
| `401` | Unauthorized | Missing Cookie header (AuthMiddleware) |
| `403` | Forbidden | Insufficient permissions (AuthMiddleware) |
| `404` | Not Found | Resource doesn't exist (product, listing, template, batch, scheme) |
| `409` | Conflict | Duplicate resource (table, column, record, platform, sizing scheme entry), concurrency conflict (image save) |
| `500` | Internal Server Error | Unhandled exceptions, external service failures |

### Custom Exceptions

#### BatchCreationError

Raised during batch creation when product processing fails.

**Structure:**
```json
{
  "error": "Batch creation failed: X of Y products failed",
  "total_products": 10,
  "failed_count": 3,
  "failed_products": [
    {
      "product_id": "PRD-001",
      "error_type": "not_found",
      "error_message": "Product not found in SellerCloud"
    }
  ],
  "timestamp": "2026-04-06T12:00:00"
}
```

### Validation Errors

Pydantic model validators return `422 Unprocessable Entity` with detailed field-level errors:
- Field name must be valid identifier: `^[a-zA-Z_][a-zA-Z0-9_]*$`
- UPC must be 8, 12, or 13 digits
- Child SKU must contain `/`
- Parent SKU must not contain `/`
- `ui_size` must be 1-12
- `cost_price` must be > 0
- `multiselect` requires `options` and `text_list` type
- `is_unique` incompatible with `multiselect`
- Size value cannot contain spaces or `/`

### Global Exception Handler

All unhandled exceptions return `500` with `{"error": "Internal server error"}`. Full error is logged server-side.

---

## Background Services

### SubmissionPoller

Manages submission lifecycle for all platforms.

**Poll cycle:**
1. **Recover stale submissions:** Mark submissions pending >10 minutes as failed
2. **Process queued submissions:** Transition to `pending` when listing images are `uploaded`
3. **Auto-submit:** Submit new listings based on platform auto_submit config (max 50 per cycle, semaphore: 1)

**Configuration:** Poll interval from `config.toml` `[submission_poller]` section

---

### SPOPoller (ShopSimon / Mirakl)

Batches pending SPO submissions into XLSX uploads.

**Poll cycle:**
1. **Recover stale processing:** Mark submissions exceeding 24-hour timeout as failed
2. **Resume products_complete:** Continue offer uploads for completed product imports
3. **Batch upload pending:** Collect pending submissions into single XLSX batch (max 200 per batch, max 40 polls per submission)

**File format:** Mirakl P41 product file + OF01 offer CSV  
**Terminal statuses:** `COMPLETE`, `FAILED`, `CANCELLED`, `REJECTED`  
**Max error display:** 500 characters

---

### SellerCloud Token Refresh

Both SellerCloud services (external + internal) run background token refresh:
- Check interval: 30 seconds
- Refresh buffer: 5 minutes before expiry
- Main service max retries: 20,000
- Internal service max retries: 3

---

### Spreadsheet Service (Listing Options)

Google AppScript integration for spreadsheet synchronization.

- Rate limit: 30 seconds between updates per table
- Async periodic checker: 30-second intervals
- Payload: `{secret, action: "updateSheets", tableName, data[]}`
- Non-error response: "Table configs not found" (means table not configured in sheets)

---

## Constants & Configuration

### Database Connections

| Connection | Source | Usage |
|---|---|---|
| `default` | `config.toml [database]` | Main app (templates, batches, listings, submissions, settings) + listing options tables |
| `product_db` | `config.toml [product_database]` | Product info, parent/child products, assignments |
| `photography_db` | `config.toml [photography_database]` | Photography batches |

### Image Processing

| Constant | Value |
|---|---|
| `MAX_FILE_SIZE` | 30 MB |
| `ALLOWED_CONTENT_TYPES` | `image/jpeg`, `image/png`, `image/webp` |
| `MAX_PRODUCT_IMAGES` | 8 |
| `MAX_WASHTAG_IMAGES` | 3 |
| `MAX_CONCURRENT_RESIZE` | 3 |
| `PILImage.MAX_IMAGE_PIXELS` | 25,000,000 (decompression bomb guard) |

### AI Service

| Constant | Value |
|---|---|
| `MAX_IMAGE_SIDE` | 1024 px |
| `MAX_IMAGE_SIZE_MB` | 5 MB |
| `MAX_IMAGES_TO_SEND` | 8 |

### Listing Options

| Constant | Value |
|---|---|
| Table prefix | `listingoptions_` |
| Primary values limit | 10,000 |
| Bulk import chunk size | 1,000 |
| Pagination max page_size | 1,000 |
| Icon max size | 5 KB |
| Sizing scheme name max | 50 chars |
| Fuzzy threshold default | 0.3 |
