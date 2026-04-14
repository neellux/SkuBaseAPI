# Brainstorm: Upload Status & Per-Platform Submission Lifecycle

**Date:** 2026-03-18
**Status:** Draft

---

## What We're Building

A proper submission lifecycle for listings that handles:

1. **Upload status tracking** — listings start with `upload_status = 'pending'` while images are still being uploaded by an external system. The external app writes directly to the DB to flip status to `'uploaded'`.

2. **Per-platform submission statuses** — each platform submission goes through a lifecycle: `queued → pending → processing → success / failed`. This replaces the current instant success/fail model.

3. **Auto-queue per platform** — configurable per platform. When `upload_status` becomes `'uploaded'`, auto-submit platforms are queued and a background poller picks them up for submission.

4. **Resubmit control** — configurable per platform. Some platforms (e.g., SellerCloud) may not allow resubmission after success. Others (e.g., Grailed) may allow it.

5. **Human-friendly error display** — platform avatars in ListingView detail view show submission status on hover, with friendly error messages for failed submissions.

6. **SPO (ShopSimon) integration** — new platform with async multi-step submission: Products (P41) → poll (P42) → Offers (OF01) → poll (OF02) → confirm listed. Has its own background poller for status tracking.

7. **Platform sub-status tracking** — `platform_status` column on `listing_submissions` for granular platform-specific progress within the `processing` status.

---

## Why This Approach

**Current pain points:**
- No concept of "images not ready yet" — listings can be submitted before images are uploaded
- Submissions are instant fire-and-forget; no tracking for platforms that take time to accept/reject
- Grailed has a 31% failure rate with no clear error display to users
- `submitted_at` on `listing_submissions` is never populated
- No way to prevent premature submissions or auto-queue when ready

**Approach chosen: Extend existing schema (Approach A)**
- Add `upload_status` column to `listings` table
- Expand `listing_submissions` statuses from 2 (success/failed) to 5 (queued/pending/processing/success/failed)
- Add `error_display` column to `listing_submissions`
- Add per-platform config flags to existing `platform_settings` JSONB
- Keep multi-row pattern (one row per attempt) for audit logging

---

## Key Decisions

### 1. Upload Status on Listings Table
- **Column:** `upload_status VARCHAR(20) DEFAULT 'pending'`
- **Values:** `pending` (images uploading) → `uploaded` (images ready)
- **Trigger:** External app writes directly to DB
- **Impact:** Submissions blocked when `upload_status = 'pending'`

### 2. Submission Status Lifecycle (per-platform, per-attempt)
```
queued → pending → processing → success
                              → failed
```
- **queued** — user clicked Submit while `upload_status = 'pending'` (images not ready). Record created in `listing_submissions` with status `queued`. Background poller transitions to `pending` when images are ready.
- **pending** — submitted to platform, awaiting initial acknowledgment
- **processing** — platform acknowledged receipt, decision pending (can take hours/days)
- **success** — platform accepted the listing
- **failed** — platform rejected or technical error occurred

### 3. Multi-Row Audit Pattern (Kept)
- One row per `(listing_id, platform_id, attempt_number)` — existing pattern preserved
- Each attempt captures its own status, error, error_display, timestamps
- "Current status" = latest attempt_number for that listing+platform
- Submission blocked when latest status is `pending` or `processing` (in-flight)
- Submission blocked when latest status is `success` AND `allow_resubmit = false`

### 4. Per-Platform Configuration
Stored in `app_settings.platform_settings` JSONB:
```json
{
  "sellercloud": {
    "auto_submit": true,
    "allow_resubmit": false,
    ...existing fields...
  },
  "grailed": {
    "auto_submit": false,
    "allow_resubmit": true,
    ...existing fields...
  }
}
```

### 5. Background Poller
- **Runs inside FastAPI app** as an asyncio background task (on_startup)
- **Poll interval** stored in `config.toml` under a new `[submission_poller]` section
- **Scope:** Two jobs: (1) transition `queued` submissions when images are ready, (2) auto-submit for configured platforms. Platform response checking (for `processing` status) is a separate concern handled per-service.
- **Flow:**
  1. **Queued → Pending:** Find `listing_submissions` with `status = 'queued'` whose listing has `upload_status = 'uploaded'` → transition to `pending` and submit to platform
  2. **Auto-submit:** Find listings where `upload_status = 'uploaded'` + platforms with `auto_submit = true` that have no existing submission → create record with `status = 'pending'` and submit
  3. On success: update to `success` (or `processing` for slow platforms like SPO)
  4. On failure: mark `failed` with `error` and `error_display`, move on
- **Concurrency:** Configurable (start at 1) — supports parallel listing processing
- **No auto-retry:** Failed submissions stay failed; user retries from UI

### 6. Error Display
- **DB:** New `error_display TEXT` column on `listing_submissions` for human-readable messages
- **DB:** Existing `error TEXT` column kept for technical/backend error details
- **UI:** Platform avatars in ListingView detail view show status on hover via tooltip
- **UI:** Failed status shows friendly error from `error_display`
- **UI:** Submit button enabled when status is `failed` (allows retry)

### 7. UI Scope
- **Detail view only** (ListingView.js) — platform status indicators on platform avatars
- **Not** in list/batch views (upload_status also detail-view only)
- Hover tooltip shows: platform name, current status, error message (if failed)
- No retry button in tooltip — user uses main Submit flow

### 8. Platform Sub-Status (`platform_status` column)
- **Column:** `platform_status VARCHAR(50)` on `listing_submissions`
- **Purpose:** Granular platform-specific progress within the `processing` main status
- **Main `status`** stays simple: `pending`, `processing`, `success`, `failed`
- **`platform_status`** tracks sub-steps per platform:
  - **SPO:** `products_uploading` → `products_processing` → `products_complete` → `offers_uploading` → `offers_processing` → `listed`
  - **SellerCloud:** `null` (instant success/fail, no sub-steps)
  - **Grailed:** `null` or `uploading_images` → `listing_created` (if steps emerge)
- **UI:** Tooltip shows `platform_status` when status is `processing` for extra context

### 9. SPO (ShopSimon / Mirakl) Integration
- **Platform config:** `auto_submit: false`, `allow_resubmit: false` (products can't be modified once published)
- **Batch import model:** Multiple pending SPO submissions are batched into a single XLSX upload to reduce API calls and simplify tracking.
- **Submission flow:**
  1. User submits listing(s) to SPO → submission record(s) created with `status = 'pending'`
  2. **SPO batch poller** runs every X seconds (configurable), collects all `pending` SPO submissions
  3. Generates **one XLSX** containing all products from all pending listings
  4. Uploads via **P41** → gets one `import_id`
  5. All related submissions updated: `status = 'processing'`, `platform_status = 'products_processing'`, `platform_meta = {product_import_id: <id>}`
  6. Poller checks **P42** using the shared `import_id`
  7. On P42 COMPLETE:
     - Fetch **P44** error report (Excel with failed SKUs)
     - Map failed SKUs back to specific listing submissions → mark as `failed` with `error_display`
     - Mark remaining submissions as `platform_status = 'products_complete'`
     - Generate batch offers CSV for successful submissions → upload via **OF01**
     - Update: `platform_status = 'offers_processing'`, store `offer_import_id` in `platform_meta`
  8. Poller checks **OF02** for offer import status
  9. On OF02 COMPLETE → same per-SKU error parsing via **OF03** → mark failures, set successes to `status = 'success'`, `platform_status = 'listed'`
  10. On any FAILED step → `error_display` populated from parsed error report, `status = 'failed'`
- **SPO batch poller:** Separate background task in FastAPI (not the main submission poller)
  - Interval configurable in config.toml: `[spo_poller]`
  - **Batch pending:** Collects all `status = 'pending'` SPO submissions, batches into one import
  - **Track processing:** Groups `status = 'processing'` by shared `import_id` in `platform_meta`, checks P42/OF02 per import batch
  - **Error mapping:** Parses P44/OF03 Excel error reports, matches failed child SKUs → listing submissions via `child_size_overrides`
- **Data mapping:** Follows same pattern as grailed_service.py
  - Template-mapped fields: iterate `field_definitions` where `platform_id = "spo"` to get `field_id` and `platform_tags`
  - Already mapped in template: `brand_name` → `designer`, `manufacturer_sku` → `sku`, `product_type` → `category`, `list_price` → `msrp`, `shipping_weight` → `weight`, `title` → `title`, `description` → `description`
  - User will add: `standard_color` → `normalized-color`, `brand_color` → `designer-color`
  - Category path: `listing_options_service.get_platform_type(product_type, "spo")` — from types table
  - Sizes: Pre-mapped by user, stored as `"footwear-size US 3"` — split on first space for column name and value
  - Images: Derived in spo_service from `listing.product_id` + GCS base URL (`https://storage.googleapis.com/lux_products/{product_id}/{n}_fullsize.jpg`)
  - `variantId`: `listing.product_id` (parent SKU)
  - `final-sale`: Default `false`
  - Children: One XLSX row per child SKU from `child_size_overrides`
- **Offer submission (OF01):** After P42 COMPLETE, submit offers as CSV
  - `Offer Sku` = child SKU
  - `Product ID` = child SKU (repeated)
  - `Product ID Type` = `"SHOP_SKU"` (hardcoded)
  - `Offer Price` = `list_price` from form data (direct, no multiplier)
  - `Offer Quantity` = `1` (hardcoded for now)
  - `Offer State` = `"New"` (hardcoded)
  - Discount fields: skipped for now
- **Config:** Uses existing `[spo]` section in config.toml for API endpoint and key

### 10. Batch Integration
- **No batch-level "Submit All"** — submissions are per-listing from BatchView with platform selection
- **Upload status guard** applies per-listing (same as ListingView)
- **Per-platform per-listing status on batches table** — denormalized JSONB column updated by DB trigger
  - Structure: `{product_id: {platform_id: latest_status}}`
  - e.g., `platform_submission_statuses: {"SKU-001": {"sellercloud": "success", "grailed": "pending"}, "SKU-002": {"sellercloud": "failed"}}`
  - Trigger fires on `listing_submissions` INSERT/UPDATE, updates the listing's product_id entry for that platform with the latest attempt's status
  - Follows existing pattern of `submitted_listings` trigger on batches

---

## Schema Changes

### listings table
```sql
ALTER TABLE listings ADD COLUMN upload_status VARCHAR(20) NOT NULL DEFAULT 'pending';
```

### listing_submissions table
```sql
ALTER TABLE listing_submissions ADD COLUMN error_display TEXT;
ALTER TABLE listing_submissions ADD COLUMN platform_status VARCHAR(50);
ALTER TABLE listing_submissions ADD COLUMN platform_meta JSONB;
-- Status values expanded: 'queued', 'pending', 'processing', 'success', 'failed'
-- platform_status: platform-specific sub-state (e.g., 'products_processing', 'offers_uploading', 'listed')
-- platform_meta: transient platform-specific tracking data (e.g., SPO import IDs)
-- external_id: remains for the product's actual ID on the external platform
-- No enum constraint change needed (VARCHAR columns already accommodate these)
```

### batches table
```sql
ALTER TABLE batches ADD COLUMN platform_submission_statuses JSONB DEFAULT '{}';
-- Structure: {product_id: {platform_id: latest_status}}
-- DB trigger on listing_submissions INSERT/UPDATE updates this
```

### app_settings.platform_settings JSONB
```json
{
  "<platform_id>": {
    "auto_submit": false,
    "allow_resubmit": true,
    ...existing fields...
  }
}
```

### config.toml
```toml
[submission_poller]
enabled = true
interval_seconds = 60
max_concurrent = 1

[spo_poller]
enabled = true
interval_seconds = 30
max_polls_per_submission = 40
```

---

## Submission Guard Logic

Before creating a new submission attempt for platform X on listing Y:

1. **In-flight check:** No existing submission with status `queued`, `pending`, or `processing` for this listing+platform
2. **Resubmit check:** If latest submission status is `success` AND `platform_settings[X].allow_resubmit = false`, block
3. **If all pass:** Create new row with `attempt_number = MAX(existing) + 1`
   - If `listing.upload_status = 'pending'` → status = `queued` (will be picked up by poller when images ready)
   - If `listing.upload_status = 'uploaded'` → status = `pending` (submit immediately)

---

## Migration Strategy

### Existing listings (973 records)
```sql
-- All existing listings have images already uploaded
ALTER TABLE listings ADD COLUMN upload_status VARCHAR(20) NOT NULL DEFAULT 'pending';
UPDATE listings SET upload_status = 'uploaded';
```

### Existing submissions (217 records)
- Already have `success`/`failed` statuses — compatible with new lifecycle
- New columns (`error_display`, `platform_status`) default to `NULL` — no migration needed

### Batch trigger
- `listing_submissions` has no `batch_id` — trigger must JOIN through `listings` to get `batch_id` and `product_id`

## Implementation Notes

- **Weight conversion:** Form stores `shipping_weight` in OZ. SPO wants lbs. Convert in spo_service: `oz / 16`
- **`external_id`** = the product's actual ID on the external platform (e.g., Grailed listing refs, SPO product SKU on Mirakl)
- **`platform_meta`** = transient platform-specific tracking data (e.g., SPO's `{product_import_id: 123, offer_import_id: 456}`). Used by pollers to track async operations.

---

## Open Questions

_None — all questions resolved through brainstorm dialogue._

---

## Resolved Questions

1. **Upload trigger:** External app writes directly to DB (no webhook/polling needed on our side)
2. **Auto-queue mechanism:** Background poller (user-built) watches for upload_status changes
3. **Status lifecycle:** queued → pending → processing → success/failed
4. **Async platform status:** Varies by platform — each service handles its own update mechanism
5. **Row model:** Keep multi-row (one per attempt) for audit trail
6. **Error format:** Separate `error_display` TEXT column for human-friendly messages
7. **Config location:** `platform_settings` JSONB in `app_settings` table
8. **UI scope:** Detail view only (ListingView.js platform avatars on hover)
9. **Retry UX:** View error in tooltip, submit via main button when status is failed
10. **Resubmit control:** Configurable per platform via `allow_resubmit` flag
11. **Batch submission:** Per-listing (no batch-level submit all), upload_status guard applies per-listing
12. **Batch counts:** Denormalized per-platform counts on batches table via DB trigger
13. **Upload status in BatchView:** Not shown — detail view only
14. **Poller host:** Inside FastAPI app as asyncio background task
15. **Poll interval:** Stored in config.toml, not DB
16. **Poller scope:** Only auto-submit queuing. Platform response checking is separate per-service.
17. **Concurrency:** Configurable, start at 1
18. **Auto-retry:** None. Failed = failed. User retries from UI.
19. **SPO poller:** Separate SPO-specific background task, not part of main submission poller
20. **SPO flow:** Products (P41) → poll (P42) → Offers (OF01) → poll (OF02) → listed
21. **SPO resubmit:** `allow_resubmit: false` — products can't be modified once published on Mirakl
22. **Platform sub-status:** New `platform_status` VARCHAR(50) column for granular platform-specific progress
23. **SPO data mapping:** Follows grailed_service pattern — template mappings + derived fields in spo_service
24. **SPO category:** From types table via `listing_options_service.get_platform_type`
25. **SPO sizes:** Pre-mapped by user as "column-name value" — split on first space in spo_service
26. **SPO images/variantId/final-sale:** Handled in spo_service code (not template-mapped)
27. **SPO offers:** Hardcoded qty=1, price=list_price, no discounts. Submitted via OF01 after products approved.
28. **Queued timing:** Record created when user clicks Submit while images pending. Poller transitions queued→pending when images ready.
29. **Existing data migration:** All 973 existing listings get `upload_status = 'uploaded'`. Existing submissions unchanged.
30. **Weight conversion:** OZ→lbs in spo_service (`/16`).
31. **external_id vs platform_meta:** `external_id` = product's ID on external platform. New `platform_meta` JSONB column for transient tracking data (SPO import IDs).
32. **SPO batch imports:** Multiple pending SPO submissions batched into single XLSX/CSV uploads. One import_id per batch.
33. **SPO partial failure:** Parse P44/OF03 Excel error reports per-SKU. Failed SKUs → specific submissions marked failed. Others → success.
