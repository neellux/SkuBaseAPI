# Upload Status & Per-Platform Submission Lifecycle — Implementation Reference

**Implemented:** 2026-03-19
**Origin:** [Brainstorm](../brainstorms/2026-03-18-upload-status-platform-submissions-brainstorm.md) | [Plan](../plans/2026-03-19-feat-upload-status-platform-submission-lifecycle-plan.md)

---

## Overview

Proper submission lifecycle management for listings across multiple marketplace platforms. Listings track image upload readiness, each platform submission follows a stateful lifecycle, and background pollers handle async workflows. All platform-specific behavior is driven by configurable flags in `app_settings.platform_settings` — no platform names are hardcoded.

---

## Platform Settings (Configurable via DB)

All platform behavior is controlled by flags in `app_settings.platform_settings` JSONB:

| Flag | Type | Default | Description |
|---|---|---|---|
| `requires_images` | bool | `false` | If true + `upload_status='pending'` → submission starts as `queued` (waits for images) |
| `batch_submit` | bool | `false` | If true → skip individual submission; platform-specific batch poller handles it |
| `allow_resubmit` | bool | `true` | If false → block new submission attempts after `success` |
| `auto_submit` | bool | `false` | If true → submission poller auto-creates submissions for uploaded listings |

**Current Platform Config:**

| Platform | requires_images | batch_submit | allow_resubmit | auto_submit |
|---|---|---|---|---|
| sellercloud | false | false | false | false |
| grailed | true | false | true | false |
| spo | true | true | false | false |

**Managed via SQL function:**
```sql
SELECT set_platform_setting('spo', 'batch_submit', 'true');
SELECT set_platform_setting('all', 'auto_submit', 'false');
```

---

## Status Lifecycle

### Submission Statuses (`listing_submissions.status`)

```
queued ──► pending ──► processing ──► success
                                  ──► failed
```

| Status | Meaning | Who transitions | Frontend behavior |
|---|---|---|---|
| `queued` | Accepted, waiting for images | Submit endpoint (when `requires_images` + images pending) | Amber badge, stops polling (static) |
| `pending` | Ready to submit / actively submitting | Submit endpoint or submission poller | Blue pulse + spinner, polls 1.5s |
| `processing` | Submitted to platform, awaiting response | Platform service / SPO poller | Blue + sync icon, polls 15s |
| `success` | Platform accepted | Platform service / SPO poller | Green checkmark, polling stops |
| `failed` | Rejected or error | Platform service / SPO poller / stale recovery | Red error icon + `error_display` tooltip |

### Upload Status (`listings.upload_status`)

| Value | Meaning | Set by |
|---|---|---|
| `pending` | Images still being uploaded | Default on creation |
| `uploaded` | Images ready | External app (direct DB write) |

### Platform Sub-Status (`listing_submissions.platform_status`)

SPO-specific granular progress within `processing`:

```
products_uploading → products_processing → products_complete → offers_processing → listed
```

Other platforms: `null` (instant success/fail).

---

## Architecture

### Files Created/Modified

**New Files (5):**

| File | Purpose |
|---|---|
| `API/migrations/add_upload_status_and_submission_lifecycle.sql` | Schema changes, triggers, indexes, seed data |
| `API/services/base_poller.py` | Abstract base class for background pollers |
| `API/services/submission_poller.py` | Queued→pending transitions, auto-submit, stale recovery |
| `API/services/spo_service.py` | Mirakl P41/P42/P44/OF01/OF02/OF03 API integration |
| `API/services/spo_poller.py` | SPO batch import lifecycle management |

**Modified Files (9):**

| File | Changes |
|---|---|
| `API/models/db_models.py` | `SubmissionStatus` StrEnum, `upload_status` on Listing, `error_display`/`platform_status`/`platform_meta` on ListingSubmission, `platform_submission_statuses` on Batch |
| `API/models/api_models.py` | 5-value status Literal, `upload_status`/`error_display`/`platform_status` in responses |
| `API/routes/listing_routes.py` | Transactional submission guard, per-platform initial status, `batch_submit` dispatch, `error_display` on failures |
| `API/services/listing_service.py` | `upload_status` in `_to_response()` |
| `API/app.py` | Poller startup/shutdown hooks |
| `API/config.toml` | `[submission_poller]` and `[spo_poller]` sections |
| `UI/src/utils/submissionService.js` | Epoch guard, recursive setTimeout, adaptive intervals, listener-scoped lifecycle |
| `UI/src/components/SubmissionProgressDialog.js` | Queued/processing status icons and text |
| `UI/src/components/ListingView.js` | 5 visual states on avatars, `submitInFlightRef`, non-terminal guard |

---

## Data Flow

### Manual Submission (user clicks Submit)

```
POST /listings/submit
│
├── Load platform_settings from app_settings
├── Lock listing row (SELECT FOR UPDATE) — serializes concurrent requests
│
├── Per platform:
│   ├── In-flight guard: reject if status ∈ {queued, pending, processing}
│   ├── Resubmit guard: reject if status=success AND allow_resubmit=false
│   ├── Determine initial_status:
│   │   ├── "queued"  if requires_images=true AND upload_status="pending"
│   │   └── "pending" otherwise
│   └── Create ListingSubmission row
│
├── If all platforms are queued → return {status: "queued"}
│
└── Fire background task for non-batch platforms:
    ├── Skip platforms with batch_submit=true
    ├── Submit sellercloud/grailed in parallel
    ├── Set error_display="Failed to submit" on any failure
    └── Run post-submission ops (child sizes, parent fields, product_info)
```

### Queued → Pending (submission poller, every 60s)

```
SubmissionPoller._process_queued_submissions():
│
├── Find listings with upload_status="uploaded"
├── Lock queued submissions (SELECT FOR UPDATE SKIP LOCKED)
├── Transition queued → pending
│
└── Fire platform submissions in parallel (semaphore-limited):
    ├── Skip batch_submit=true platforms
    └── Submit via platform service (sellercloud/grailed)
```

### Auto-Submit (submission poller, every 60s)

```
SubmissionPoller._auto_submit_new():
│
├── For each platform with auto_submit=true:
│   ├── Find uploaded listings with no existing submission (NOT EXISTS query)
│   ├── Limit to max_auto_submit_per_cycle (default 50)
│   ├── Create pending submission records
│   └── Submit in parallel (skip batch_submit=true)
```

### SPO Batch Import (spo poller, every 30s)

```
SpoPoller._batch_upload_pending():
│
├── Collect pending SPO submissions (max 200, FOR UPDATE SKIP LOCKED)
├── Transition to processing + platform_status="products_uploading"
├── Build product rows from all listings' form data
├── Generate single XLSX (run_in_executor — CPU-bound)
├── Upload via P41 → import_id
└── Store import_id in platform_meta

SpoPoller._check_processing():
│
├── Group processing submissions by product_import_id
├── Poll P42 per import batch
│   ├── COMPLETE → parse P44 errors → map to listings → upload offers (OF01)
│   ├── FAILED → mark all submissions failed
│   └── Still processing → wait for next cycle
│
├── Group by offer_import_id
├── Poll OF02
│   ├── COMPLETE → parse OF03 errors → mark success/fail
│   └── FAILED → mark submissions failed
```

### Stale Recovery (submission poller, every 60s)

```
SubmissionPoller._recover_stale_submissions():
│
├── Find submissions in "pending" status for > 10 minutes
└── Mark as failed with error_display="Submission timed out — please retry"

SpoPoller._recover_stale_processing():
│
├── Find SPO submissions in "processing" for > 24 hours
└── Mark as failed with error_display="Import timed out after 24 hours"

SpoPoller._resume_products_complete():
│
├── Find SPO submissions stuck at platform_status="products_complete" (crash recovery)
└── Retry offer upload
```

---

## Database Changes

### New Columns

| Table | Column | Type | Default | Purpose |
|---|---|---|---|---|
| `listings` | `upload_status` | VARCHAR(20) NOT NULL | `'pending'` | Image upload readiness |
| `listing_submissions` | `error_display` | TEXT | NULL | Human-friendly error for UI |
| `listing_submissions` | `platform_status` | VARCHAR(50) | NULL | Platform-specific sub-state |
| `listing_submissions` | `platform_meta` | JSONB | NULL | Transient tracking data (import IDs) |
| `batches` | `platform_submission_statuses` | JSONB | `'{}'` | Denormalized {product_id: {platform_id: status}} |

### Constraints

| Constraint | Table | Definition |
|---|---|---|
| `chk_listings_upload_status` | listings | `CHECK (upload_status IN ('pending', 'uploaded'))` |
| `chk_listing_submissions_status` | listing_submissions | `CHECK (status IN ('queued', 'pending', 'processing', 'success', 'failed'))` |
| `idx_listing_submissions_inflight` | listing_submissions | `UNIQUE (listing_id, platform_id) WHERE status IN ('queued', 'pending', 'processing')` |

### Triggers

| Trigger | Table | Event | Purpose |
|---|---|---|---|
| `set_submitted_at_on_completion` | listing_submissions | BEFORE UPDATE | Sets `submitted_at` when reaching terminal state from any non-terminal |
| `update_batch_platform_statuses` | listing_submissions | AFTER INSERT/UPDATE OF status | Updates `batches.platform_submission_statuses` JSONB |

### Indexes

| Index | Columns | Notes |
|---|---|---|
| `idx_listings_upload_status` | `(upload_status)` | Poller: find uploaded listings |
| `idx_listing_submissions_status_platform` | `(status, platform_id)` | Poller: find pending/queued by platform |
| `idx_listing_submissions_inflight` | `(listing_id, platform_id)` WHERE non-terminal | Partial unique — prevents duplicate in-flight |

### Utility Function

```sql
-- Add/update any platform setting dynamically
SELECT set_platform_setting('platform_id_or_all', 'key', 'value');

-- Examples:
SELECT set_platform_setting('spo', 'batch_submit', 'true');
SELECT set_platform_setting('all', 'auto_submit', 'false');
```

---

## Frontend

### Submission Service (`submissionService.js`)

- **Singleton** with recursive `setTimeout` polling (not `setInterval`)
- **Epoch guard**: prevents stale poll responses from overwriting fresh data
- **Adaptive intervals**: 1.5s (pending), 10s (queued), 15s (processing)
- **Terminal statuses**: `{success, failed, queued}` — stops polling when all platforms reach one
- **Listener-scoped lifecycle**: stops polling when last subscriber for a listing unsubscribes
- **API**: `subscribe(listingId, callback)` returns unsubscribe function

### Platform Avatars (`ListingView.js`)

| Status | Border | Background | Badge | Tooltip |
|---|---|---|---|---|
| `queued` | amber | light amber tint | `CloudQueueIcon` (amber) | "Platform — Queued (waiting for images)" |
| `pending` | blue, pulsing | default | `CircularProgress` | "Submitting..." |
| `processing` | blue | default | `SyncIcon` (spinning) | "Platform — Processing: {platform_status}" |
| `success` | green | default | `CheckCircleIcon` | "Platform — Submitted" |
| `failed` | red | default | `ErrorIcon` | "Platform — {error_display}" |

### Submit Button

- **Disabled** when: any submission in `{queued, pending, processing}`, saving, or validation errors
- **Text**: "Submit" / "Re-submit" / "Retry Failed" / "Submitting..." / "Saving..."
- **Double-click guard**: `submitInFlightRef` (useRef, synchronous — React state is async)

### Submission Progress Dialog

- Shows per-platform status with icons (queued=CloudQueue, processing=Sync spinning, pending=spinner, success=check, failed=error)
- Error details shown for failed platforms using `error_display`
- Non-closable while actively submitting

---

## Configuration

### config.toml

```toml
[submission_poller]
enabled = true           # false to disable queued transitions + auto-submit
interval_seconds = 60    # seconds between poll cycles
max_concurrent = 1       # max parallel platform submissions per cycle
max_auto_submit_per_cycle = 50  # cap on auto-submit listings per cycle

[spo_poller]
enabled = false          # enable when SPO integration is ready
interval_seconds = 30    # seconds between poll cycles
max_polls_per_submission = 40   # max P42/OF02 checks before timeout
max_batch_size = 200     # max submissions per XLSX batch
stale_processing_timeout_minutes = 1440  # 24h before marking stuck processing as failed
```

### Deployment Strategy

1. Deploy with both pollers `enabled = false`
2. Run migration on test DB, verify
3. Run migration on prod DB
4. Enable `submission_poller` → monitor logs for 1-4 hours
5. Enable `spo_poller` → monitor SPO imports

---

## Migration Checklist

**Pre-migration verification queries:**
```sql
-- Verify no unexpected status values
SELECT status, COUNT(*) FROM listing_submissions GROUP BY status;

-- Verify external_id is castable to JSONB
SELECT id, external_id FROM listing_submissions
WHERE external_id IS NOT NULL AND external_id !~ '^\s*[\[{"]';

-- Check listings count
SELECT COUNT(*) FROM listings;
```

**Migration file:** `API/migrations/add_upload_status_and_submission_lifecycle.sql`

**Post-migration verification:**
```sql
-- All listings should be 'uploaded'
SELECT upload_status, COUNT(*) FROM listings GROUP BY upload_status;

-- New columns exist
SELECT column_name FROM information_schema.columns
WHERE table_name = 'listing_submissions' AND column_name IN ('error_display', 'platform_status', 'platform_meta');

-- Platform settings seeded
SELECT platform_settings FROM app_settings LIMIT 1;

-- Partial unique index exists
SELECT indexname FROM pg_indexes WHERE indexname = 'idx_listing_submissions_inflight';
```

---

## Adding a New Platform

To add a new marketplace platform:

1. **Create platform service** following `grailed_service.py` pattern (or `spo_service.py` for batch platforms)
2. **Set platform settings:**
   ```sql
   SELECT set_platform_setting('newplatform', 'requires_images', 'true');
   SELECT set_platform_setting('newplatform', 'batch_submit', 'false');
   SELECT set_platform_setting('newplatform', 'allow_resubmit', 'true');
   SELECT set_platform_setting('newplatform', 'auto_submit', 'false');
   ```
3. **Add to enabled platforms:** Update `app_settings.platforms` array
4. **Register platform metadata** in the listing_options DB `platforms` table (name, icon)
5. **Add dispatch** in `_run_submissions_background()` for non-batch platforms, or create a dedicated poller for batch platforms
6. **Add field mappings** in the template's `field_definitions[].platforms` array
