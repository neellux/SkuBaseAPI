# SellerCloud Field Mapping Documentation

This document describes all fields that are updated on SellerCloud when a listing form is submitted via `submit_listing_to_sellercloud()` in `sellercloud_service.py`.

---

## Normal Fields (via `/Catalog/AdvancedInfo`)

| Form Field | SellerCloud Field | Type | Required | Transformation |
|------------|-------------------|------|----------|----------------|
| `brand_name` | `BrandName` | text | Yes | None |
| `manufacturer_sku` | `ManufacturerSKU` | text | Yes | None |
| `product_type` | `ProductTypeName` | text | Yes | Field name override |
| `list_price` | `ListPrice` | number | Yes | None (also used in ProductName) |
| `product_name` | `ProductName` | text | Yes | Appends ` SIZE {size} ${list_price}` |
| `long_description` | `LongDescription` | rich_text | No | Template populated, HTML cleaned |
| `shipping_weight` | `PackageWeightLbs` | number | No | `= shipping_weight // 16` |
| `shipping_weight` | `PackageWeightOz` | number | No | `= shipping_weight % 16` |
| `test_id` | `LocationNotes` | text | No | None |

---

## Custom Column Fields (via `/Products/CustomColumns`)

| Form Field | SellerCloud Field | Type | Required | Transformation |
|------------|-------------------|------|----------|----------------|
| `style_name` | `STYLE_NAME` | text | Yes | None |
| `color` | `COLOR` | text | Yes | None |
| `brand_color` | `BRAND_COLOR` | text | Yes | Also triggers alias API call |
| `country_of_origin` | `COUNTRY_OF_ORIGIN` | text | No | None |
| `material` | `MATERIAL` | text | No | None (transformed in template only) |

---

## Auto-Generated Fields (not from form input)

| SellerCloud Field | Type | Source | Transformation |
|-------------------|------|--------|----------------|
| `GENDER` | custom | `product_type` | Fetched via `get_gender_from_product_type()` API |
| `HTMLDESCRIPTION_FIXED` | custom | `long_description` | Copy of populated `LongDescription` |
| `SIZE` | custom | Product ID | Extracted from child product ID (last segment after `/`) |
| `SIZING_SCHEME` | custom | Form (if present) | Hardcoded as custom field |

---

## Complete Dependency Graph

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           FORM SUBMISSION                                │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  product_type ──────────────────┬──────────────────────────────────────│
│       │                         │                                       │
│       ▼                         ▼                                       │
│   GENDER (API fetch)      ProductTypeName                               │
│       │                                                                 │
│       ▼                                                                 │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │              LongDescription Template Population                 │   │
│  │  ┌─────────────────────────────────────────────────────────┐    │   │
│  │  │  {GENDER}     → GENDER_MAPPING applied                  │    │   │
│  │  │  {MATERIAL}   → Lines wrapped in <div>, Main:→Shell:    │    │   │
│  │  │  {brand_name}, {style_name}, {color}, etc. → as-is      │    │   │
│  │  └─────────────────────────────────────────────────────────┘    │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                         │                                               │
│                         ▼                                               │
│               LongDescription (HTML cleaned)                            │
│                         │                                               │
│                         ├──────────────────────────────────────────────│
│                         ▼                                               │
│               HTMLDESCRIPTION_FIXED (copy)                              │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│  shipping_weight (oz) ─────────┬───────────────────────────────────────│
│                                │                                        │
│                    ┌───────────┴───────────┐                           │
│                    ▼                       ▼                           │
│           PackageWeightLbs          PackageWeightOz                    │
│           (weight // 16)            (weight % 16)                      │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│  color + brand_color ──────────────────────────────────────────────────│
│       │                                                                 │
│       ▼                                                                 │
│  add_color_alias() API call (if color != brand_color)                  │
│       │                                                                 │
│       ├──► COLOR (custom field)                                        │
│       └──► BRAND_COLOR (custom field)                                  │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│  product_name + list_price + SIZE ─────────────────────────────────────│
│       │                                                                 │
│       ▼                                                                 │
│  ProductName = "{product_name} SIZE {size} ${list_price}"              │
│  (per child product)                                                    │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│  Child Product ID (e.g., "ABC-123/M") ─────────────────────────────────│
│       │                                                                 │
│       ▼                                                                 │
│  SIZE = "M" (last segment after "/")                                   │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## API Endpoints Used

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/Catalog/AdvancedInfo` | PUT | Update normal fields |
| `/Products/CustomColumns` | PUT | Update custom fields |
| `/type_by_name` (Listing Options API) | GET | Fetch gender from product type |
| `/add_aliases_to_color` (Listing Options API) | PUT | Add brand color alias |

---

## Special Processing Rules

| Rule | Details |
|------|---------|
| **Skip** | `ID` field never sent |
| **Override** | `ProductType` → `ProductTypeName` |
| **HTML Clean** | `<li><p>x</p></li>` → `<li>x</li>`, empty `<p>` removed |
| **Gender Map** | `Mens`→`Men's `, `Womens`→`Women's `, `Boys`→`Boy's `, `Girls`→`Girl's `, `Unisex`→`Unisex `, `Does Not Apply`→`` |
| **Material Transform** | (in template) Lines split by `\n`, `Main:` → `Shell:`, wrapped in `<div>` |
| **Trim** | All string values are `.strip()`ed |

---

## Hardcoded Constants

### CUSTOM_COLUMN_FIELDS
Fields that are always treated as custom columns regardless of platform mapping:
- `SIZING_SCHEME`
- `GENDER`
- `HTMLDESCRIPTION_FIXED`

### FIELD_NAME_OVERRIDES
Field name transformations applied before sending to SellerCloud:
- `ProductType` → `ProductTypeName`

### SKIP_FIELDS
Fields that are never sent to SellerCloud:
- `ID`

### GENDER_MAPPING
Used when populating the LongDescription template:
| Input | Output |
|-------|--------|
| `Mens` | `Men's ` |
| `Womens` | `Women's ` |
| `Boys` | `Boy's ` |
| `Girls` | `Girl's ` |
| `Unisex` | `Unisex ` |
| `Does Not Apply` | `` (empty) |

---

## Update Flow

1. **Validate required fields** (`ProductType`, `COLOR`, `BRAND_COLOR`)
2. **Fetch GENDER** from Listing Options API based on `ProductType`
3. **Add color alias** (if `COLOR` != `BRAND_COLOR` and not skipped)
4. **Populate LongDescription template** with form data
5. **Set HTMLDESCRIPTION_FIXED** to same value as populated `LongDescription`
6. **Fetch child products** for the parent product ID
7. **For each child product:**
   - Build normal fields list
   - Build custom fields list (including `SIZE` from product ID)
   - Modify `ProductName` to include size and price
   - Send updates via both API endpoints
