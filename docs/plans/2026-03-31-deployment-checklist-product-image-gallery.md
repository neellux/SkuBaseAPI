# Deployment Checklist: Product Image Gallery with Reordering, Upload & Metadata

**Date:** 2026-03-31
**Status:** Draft -- requires review before execution
**Estimated total deployment time:** 45-60 minutes (including verification pauses)
**Maintenance window required:** No (see rationale below)

---

## Summary of Changes

This deployment touches two services and one shared database:

| Component | Change Type | Risk Level |
|-----------|-------------|------------|
| Photography DB (`lux_photography`) | Schema migration + data backfill on `productimages` table | HIGH |
| Photography API | Code update (~15-20 lines across 3 files) to use new columns | MEDIUM |
| Listing Tool API | New 4th DB connection, new GCS service, new endpoints, new dependencies | MEDIUM |
| Listing Tool UI | New React component (additive) | LOW |

**Shared database:** `lux_photography` on `a288413-akamai-prod-1934030-default.g2a.akamaidb.net:28582`
**Shared table:** `productimages` (written by both Photography API and Listing Tool API)

---

## Deployment Order (5 Steps -- Order Is Critical)

The correct order eliminates downtime and avoids breaking either service:

```
Step 1: Add new columns (image_data, washtag_data_json) -- DO NOT drop old columns
Step 2: Backfill new columns from old columns
Step 3: Deploy Photography API code (reads/writes new columns)
Step 4: Deploy Listing Tool API + UI (connects to lux_photography, uses new columns)
Step 5: Drop old columns (image_ids, washtag_ids) -- only after both services confirmed working
```

**Why this order:**
- Steps 1-2 are additive. Old columns remain. Photography API continues working on old columns.
- Step 3 switches Photography API to new columns. Old columns still exist as safety net.
- Step 4 is safe because new columns already exist and are populated.
- Step 5 is the only destructive step and happens last, after full verification.

**Why no maintenance window:**
- Steps 1-2 add columns and backfill -- no lock contention on reads, minimal on writes.
- The `productimages` table is written only during batch processing (not high-frequency OLTP).
- Steps 3-4 are rolling deploys of application code.
- Step 5 (column drop) acquires a brief ACCESS EXCLUSIVE lock but is instant (no rewrite).
- The only risk is a Photography API batch running mid-migration and writing to old columns after Step 3. Mitigation: verify no active batches before Step 3.

---

## Pre-Deploy (Required -- All Must Pass)

### 1. Verify No Active Batches

Before starting, confirm no Photography API batch processing is in progress. An active batch writing to `image_ids`/`washtag_ids` during migration would create inconsistency.

```sql
-- Run against lux_photography
-- Check for batches that might be actively processing
SELECT id, status, created_at, updated_at
FROM batch
WHERE status NOT IN ('completed', 'error', 'cancelled')
ORDER BY created_at DESC
LIMIT 10;
-- Expected: 0 rows, or only batches in safe states
```

If active batches exist: STOP. Wait for them to complete or coordinate with the Photography team.

### 2. Baseline Record Counts (Save These Values)

```sql
-- Run against lux_photography
-- Total productimages records
SELECT COUNT(*) AS total_records FROM productimages;

-- Records by image_source
SELECT image_source, COUNT(*) AS cnt
FROM productimages
GROUP BY image_source
ORDER BY image_source;

-- Records with non-null image_ids
SELECT COUNT(*) AS records_with_image_ids
FROM productimages
WHERE image_ids IS NOT NULL AND array_length(image_ids, 1) > 0;

-- Records with non-null washtag_ids
SELECT COUNT(*) AS records_with_washtag_ids
FROM productimages
WHERE washtag_ids IS NOT NULL AND array_length(washtag_ids, 1) > 0;

-- Total image_ids entries across all records (for backfill verification)
SELECT COALESCE(SUM(array_length(image_ids, 1)), 0) AS total_image_id_entries
FROM productimages
WHERE image_ids IS NOT NULL;

-- Total washtag_ids entries across all records
SELECT COALESCE(SUM(array_length(washtag_ids, 1)), 0) AS total_washtag_id_entries
FROM productimages
WHERE washtag_ids IS NOT NULL;
```

**Record these six values. They are your verification baseline.**

- [ ] `total_records` = ______
- [ ] `batch_creation` count = ______
- [ ] `upload` count = ______
- [ ] `records_with_image_ids` = ______
- [ ] `records_with_washtag_ids` = ______
- [ ] `total_image_id_entries` = ______
- [ ] `total_washtag_id_entries` = ______

### 3. Check for Known Data Issues

The duplicate approved images bug (documented in `/home/ubuntu/Luxemporium/PhotoManagementNew/docs/duplicate_approved_images_bug.md`) can cause inflated `image_ids` arrays. Verify the current state:

```sql
-- Check for productimages records with inflated counts
SELECT pi_upload.product_id,
       pi_batch.product_images_count AS batch_count,
       pi_upload.product_images_count AS upload_count,
       pi_upload.product_images_count - pi_batch.product_images_count AS inflated_by
FROM productimages pi_upload
JOIN productimages pi_batch
    ON pi_batch.product_id = pi_upload.product_id
    AND pi_batch.image_source = 'batch_creation'
WHERE pi_upload.image_source = 'upload'
  AND pi_upload.product_images_count > pi_batch.product_images_count
ORDER BY inflated_by DESC;
```

- [ ] Count of inflated records = ______ (known: ~7 products, acceptable)
- [ ] No new unexpected inflated records since last check

### 4. Verify Database Connectivity from Listing Tool API Host

The Listing Tool API will add a 4th Tortoise ORM connection to `lux_photography`. Verify connectivity before deploying:

```bash
# From the Listing Tool API host
psql "postgres://lux_photography:WYnCk7ZYNhFh\!Ih*3h9S@a288413-akamai-prod-1934030-default.g2a.akamaidb.net:28582/lux_photography" \
  -c "SELECT 1 AS connectivity_check;"
```

- [ ] Connection successful

### 5. Verify GCS Service Account Access

```bash
# From the Listing Tool API host, verify the service account JSON exists
ls -la /path/to/service-account-2.json

# Test GCS access (read-only check)
# gsutil ls gs://lux_products/ | head -5
```

- [ ] GCS service account file exists and is readable
- [ ] GCS bucket `lux_products` is accessible

### 6. Verify Dependencies Available

```bash
# From the Listing Tool API environment
pip install --dry-run gcloud-aio-storage Pillow
```

- [ ] `gcloud-aio-storage` installable
- [ ] `Pillow` installable

### 7. Database Backup

```bash
# Take a logical backup of the productimages table before any schema changes
pg_dump "postgres://lux_photography:WYnCk7ZYNhFh\!Ih*3h9S@a288413-akamai-prod-1934030-default.g2a.akamaidb.net:28582/lux_photography" \
  -t productimages \
  --data-only \
  -f productimages_backup_$(date +%Y%m%d_%H%M%S).sql
```

- [ ] Backup file created and verified (check file size is non-zero)
- [ ] Backup file stored in a safe location outside the deployment directory

---

## Deploy Steps

### Step 1: Add New Columns (Non-Destructive)

**What:** Add `image_data` (JSONB) and `washtag_data_json` (JSONB) columns to `productimages`. Do NOT drop old columns yet.

**Method:** Run the migration via `aerich` from the Photography API directory, or execute the SQL directly.

**Direct SQL (preferred for controlled deployment):**

```sql
-- Run against lux_photography
BEGIN;

ALTER TABLE productimages
    ADD COLUMN IF NOT EXISTS image_data JSONB DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS washtag_data_json JSONB DEFAULT '[]'::jsonb;

COMMIT;
```

**Estimated runtime:** < 5 seconds (adding nullable JSONB columns with defaults is metadata-only in PostgreSQL, no table rewrite)

**Verification immediately after:**

```sql
-- Confirm columns exist
SELECT column_name, data_type, column_default, is_nullable
FROM information_schema.columns
WHERE table_name = 'productimages'
  AND column_name IN ('image_data', 'washtag_data_json', 'image_ids', 'washtag_ids')
ORDER BY column_name;
-- Expected: 4 rows (both old and new columns exist)
```

- [ ] Step 1 complete
- [ ] Both old columns (`image_ids`, `washtag_ids`) still exist
- [ ] Both new columns (`image_data`, `washtag_data_json`) now exist
- [ ] Photography API is still working (spot-check the UI or a known endpoint)

### Step 2: Backfill New Columns from Old Columns

**What:** Populate `image_data` from `image_ids` and `washtag_data_json` from `washtag_ids`. For each UUID in the old array, create a JSONB object with `id`, `shot_type` (looked up from the `image`/`washtag` table), and `md5_hash` (null for existing records).

**Option A: Simple backfill (UUID-only, no shot_type lookup):**

This is faster and safer. Shot types can be enriched later:

```sql
-- Backfill image_data from image_ids
UPDATE productimages
SET image_data = (
    SELECT jsonb_agg(
        jsonb_build_object('id', elem, 'shot_type', NULL, 'md5_hash', NULL)
    )
    FROM unnest(image_ids) AS elem
)
WHERE image_ids IS NOT NULL
  AND array_length(image_ids, 1) > 0
  AND (image_data IS NULL OR image_data = '[]'::jsonb);

-- Backfill washtag_data_json from washtag_ids
UPDATE productimages
SET washtag_data_json = (
    SELECT jsonb_agg(
        jsonb_build_object('id', elem, 'shot_type', NULL, 'md5_hash', NULL)
    )
    FROM unnest(washtag_ids) AS elem
)
WHERE washtag_ids IS NOT NULL
  AND array_length(washtag_ids, 1) > 0
  AND (washtag_data_json IS NULL OR washtag_data_json = '[]'::jsonb);
```

**Option B: Full backfill with shot_type lookup (recommended if time permits):**

```sql
-- Backfill image_data with shot_type from the image table
UPDATE productimages pi
SET image_data = sub.new_data
FROM (
    SELECT pi2.id AS pi_id,
           jsonb_agg(
               jsonb_build_object(
                   'id', elem,
                   'shot_type', img.shot_type,
                   'md5_hash', NULL
               ) ORDER BY ord
           ) AS new_data
    FROM productimages pi2
    CROSS JOIN LATERAL unnest(pi2.image_ids) WITH ORDINALITY AS t(elem, ord)
    LEFT JOIN image img ON img.id::text = elem AND img.status = 'approved'
    WHERE pi2.image_ids IS NOT NULL
      AND array_length(pi2.image_ids, 1) > 0
      AND (pi2.image_data IS NULL OR pi2.image_data = '[]'::jsonb)
    GROUP BY pi2.id
) sub
WHERE pi.id = sub.pi_id;

-- Backfill washtag_data_json with shot_type from the washtag table
UPDATE productimages pi
SET washtag_data_json = sub.new_data
FROM (
    SELECT pi2.id AS pi_id,
           jsonb_agg(
               jsonb_build_object(
                   'id', elem,
                   'shot_type', wt.shot_type,
                   'md5_hash', NULL
               ) ORDER BY ord
           ) AS new_data
    FROM productimages pi2
    CROSS JOIN LATERAL unnest(pi2.washtag_ids) WITH ORDINALITY AS t(elem, ord)
    LEFT JOIN washtag wt ON wt.id::text = elem
    WHERE pi2.washtag_ids IS NOT NULL
      AND array_length(pi2.washtag_ids, 1) > 0
      AND (pi2.washtag_data_json IS NULL OR pi2.washtag_data_json = '[]'::jsonb)
    GROUP BY pi2.id
) sub
WHERE pi.id = sub.pi_id;
```

**Estimated runtime:** Depends on table size. For thousands of rows: < 30 seconds. For tens of thousands: 1-5 minutes. Run `SELECT COUNT(*) FROM productimages WHERE image_ids IS NOT NULL` first to gauge.

**Verification immediately after:**

```sql
-- Count records where backfill succeeded
SELECT COUNT(*) AS records_with_image_data
FROM productimages
WHERE image_data IS NOT NULL AND image_data != '[]'::jsonb;
-- Expected: should match baseline records_with_image_ids

SELECT COUNT(*) AS records_with_washtag_data
FROM productimages
WHERE washtag_data_json IS NOT NULL AND washtag_data_json != '[]'::jsonb;
-- Expected: should match baseline records_with_washtag_ids

-- Verify element counts match
SELECT COALESCE(SUM(jsonb_array_length(image_data)), 0) AS total_image_data_entries
FROM productimages
WHERE image_data IS NOT NULL AND image_data != '[]'::jsonb;
-- Expected: should match baseline total_image_id_entries

SELECT COALESCE(SUM(jsonb_array_length(washtag_data_json)), 0) AS total_washtag_data_entries
FROM productimages
WHERE washtag_data_json IS NOT NULL AND washtag_data_json != '[]'::jsonb;
-- Expected: should match baseline total_washtag_id_entries

-- Verify no records were missed (have old data but no new data)
SELECT COUNT(*) AS missed_image_backfills
FROM productimages
WHERE image_ids IS NOT NULL
  AND array_length(image_ids, 1) > 0
  AND (image_data IS NULL OR image_data = '[]'::jsonb);
-- Expected: 0

SELECT COUNT(*) AS missed_washtag_backfills
FROM productimages
WHERE washtag_ids IS NOT NULL
  AND array_length(washtag_ids, 1) > 0
  AND (washtag_data_json IS NULL OR washtag_data_json = '[]'::jsonb);
-- Expected: 0

-- Spot-check: compare a few records manually
SELECT product_id, image_ids, image_data, washtag_ids, washtag_data_json
FROM productimages
WHERE image_ids IS NOT NULL AND array_length(image_ids, 1) > 0
ORDER BY updated_at DESC
LIMIT 5;
-- Verify: each UUID in image_ids appears as an "id" value in image_data
```

- [ ] Step 2 complete
- [ ] `records_with_image_data` matches baseline `records_with_image_ids`
- [ ] `records_with_washtag_data` matches baseline `records_with_washtag_ids`
- [ ] `total_image_data_entries` matches baseline `total_image_id_entries`
- [ ] `total_washtag_data_entries` matches baseline `total_washtag_id_entries`
- [ ] `missed_image_backfills` = 0
- [ ] `missed_washtag_backfills` = 0
- [ ] Spot-check confirms correct mapping
- [ ] Photography API is still working (old columns untouched)

### Step 3: Deploy Photography API Code

**What:** Deploy the updated Photography API that reads/writes `image_data` and `washtag_data_json` instead of `image_ids` and `washtag_ids`.

**Pre-condition:** Verify no active batches AGAIN before this step.

```sql
SELECT id, status, created_at, updated_at
FROM batch
WHERE status NOT IN ('completed', 'error', 'cancelled')
ORDER BY created_at DESC
LIMIT 5;
-- Expected: 0 rows
```

**Files changed (verify these are in the deploy):**

| File | Path |
|------|------|
| DB model | `/home/ubuntu/Luxemporium/PhotoManagementNew/API/models/db_models.py` (lines 378-393) |
| GCS upload processor | `/home/ubuntu/Luxemporium/PhotoManagementNew/API/utils/gcs_upload_processor.py` (lines 317-318, 345-356, 463-476) |
| GCS product uploader | `/home/ubuntu/Luxemporium/PhotoManagementNew/API/utils/gcs_product_uploader.py` (lines 123, 130, 172, 182, 410, 413, 457, 468) |
| Washtag AI processor | `/home/ubuntu/Luxemporium/PhotoManagementNew/API/utils/washtag_ai_processor.py` (line 31) |

**Deploy command:** (adjust to your deployment process)

```bash
# Example: restart the Photography API service
cd /home/ubuntu/Luxemporium/PhotoManagementNew/API
# git pull / deploy steps here
# systemctl restart photography-api  (or equivalent)
```

**Verification immediately after:**

```bash
# Check Photography API is responding
curl -s -o /dev/null -w "%{http_code}" https://photography-api.luxinternal.com/
# Expected: 200 or appropriate health check response
```

```sql
-- Verify the Photography API model no longer references old columns
-- (This is a code verification, not a SQL check -- verify in the deployed code)
```

- [ ] Step 3 complete
- [ ] No active batches were running during deploy
- [ ] Photography API is responding to health checks
- [ ] Photography API logs show no startup errors
- [ ] Code review confirms no references to `image_ids` or `washtag_ids` in deployed code

### Step 4: Deploy Listing Tool API and UI

**What:** Deploy the Listing Tool API with the new `lux_photography` database connection, GCS image service, new endpoints, and new dependencies. Deploy the UI with `ProductImageGallery` component.

**4a. Deploy Listing Tool API:**

**Pre-deployment config check:**

Verify `config.toml` (or `config prod.toml`) contains the new `[photography_database]` section:

```toml
[photography_database]
db_engine = "postgres"
db_user = "lux_photography"
db_password = "..."
db_host = "a288413-akamai-prod-1934030-default.g2a.akamaidb.net"
db_port = 28582
db_name = "lux_photography"
```

Verify `config.py` constructs the 4th connection URL and adds it to `TORTOISE_ORM_CONFIG`:

```python
PHOTOGRAPHY_DB_CONFIG = config.get("photography_database", {})
PHOTOGRAPHY_DB_URL = f"postgres://..."

TORTOISE_ORM_CONFIG = {
    "connections": {
        "default": DB_URL,
        "listing_options": DB_URL_2,
        "product_db": PRODUCT_DB_URL,
        "photography_db": PHOTOGRAPHY_DB_URL,  # <-- new
    },
    ...
}
```

**Deploy command:**

```bash
cd "/home/ubuntu/Luxemporium/Listing Tool New/API"
pip install -r requirements.txt  # installs gcloud-aio-storage, Pillow
# Deploy / restart service
```

**4b. Deploy Listing Tool UI:**

```bash
cd "/home/ubuntu/Luxemporium/Listing Tool New/UI"
npm install  # if any new dependencies
npm run build
# Deploy built assets
```

**Verification immediately after:**

```bash
# Check Listing Tool API is responding
curl -s -o /dev/null -w "%{http_code}" https://listingapi.luxinternal.com/
# Expected: 200

# Check new endpoints exist (will return 401/403 without auth, but that confirms routing works)
curl -s -o /dev/null -w "%{http_code}" https://listingapi.luxinternal.com/products/images?product_id=test
# Expected: 401 or 403 (not 404)
```

```sql
-- From the Listing Tool API logs or directly, verify the photography_db connection is active
-- Check pg_stat_activity for new connections from the Listing Tool API host
SELECT usename, datname, client_addr, state, query
FROM pg_stat_activity
WHERE datname = 'lux_photography'
  AND usename = 'lux_photography'
ORDER BY backend_start DESC
LIMIT 10;
-- Expected: connections from both Photography API and Listing Tool API hosts
```

- [ ] Step 4a complete -- Listing Tool API deployed
- [ ] Step 4b complete -- Listing Tool UI deployed
- [ ] Listing Tool API is responding to health checks
- [ ] New endpoints return non-404 responses
- [ ] Listing Tool API logs show successful connection to `lux_photography`
- [ ] UI loads without console errors
- [ ] `ProductImageGallery` component renders for a known product

### Step 5: Drop Old Columns (Destructive -- Only After Full Verification)

**What:** Remove `image_ids` and `washtag_ids` columns from `productimages`.

**Pre-conditions (ALL must be true):**
- [ ] Photography API has been running on new columns for at least 1 hour with no errors
- [ ] At least one batch has completed successfully using the new columns (or: manual test of batch processing confirms new columns are written correctly)
- [ ] Listing Tool API gallery is confirmed working
- [ ] No code in either service references `image_ids` or `washtag_ids`

**Execute:**

```sql
-- Run against lux_photography
BEGIN;

-- Final safety check: verify no data exists only in old columns
SELECT COUNT(*) AS orphaned_old_data
FROM productimages
WHERE (image_ids IS NOT NULL AND array_length(image_ids, 1) > 0)
  AND (image_data IS NULL OR image_data = '[]'::jsonb);
-- MUST be 0. If not 0, ABORT and re-run backfill.

ALTER TABLE productimages DROP COLUMN IF EXISTS image_ids;
ALTER TABLE productimages DROP COLUMN IF EXISTS washtag_ids;

COMMIT;
```

**Estimated runtime:** < 1 second (column drop is metadata-only, no table rewrite)

**Verification:**

```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'productimages'
  AND column_name IN ('image_ids', 'washtag_ids', 'image_data', 'washtag_data_json')
ORDER BY column_name;
-- Expected: 2 rows (only image_data and washtag_data_json)

-- Verify total record count unchanged
SELECT COUNT(*) AS total_records FROM productimages;
-- Expected: matches baseline total_records
```

- [ ] Step 5 complete
- [ ] Old columns (`image_ids`, `washtag_ids`) are gone
- [ ] New columns (`image_data`, `washtag_data_json`) remain
- [ ] Total record count unchanged
- [ ] Photography API still healthy
- [ ] Listing Tool API still healthy

---

## Post-Deploy Verification (Within 5 Minutes of Each Step)

### Data Integrity Invariants

These must remain true throughout and after the deployment:

```sql
-- INVARIANT 1: Total record count unchanged
SELECT COUNT(*) FROM productimages;

-- INVARIANT 2: No NULL new columns where old columns had data
-- (After Step 2, before Step 5)
SELECT COUNT(*)
FROM productimages
WHERE (image_ids IS NOT NULL AND array_length(image_ids, 1) > 0)
  AND (image_data IS NULL OR image_data = '[]'::jsonb);
-- Must be 0

-- INVARIANT 3: Element counts match between old and new
-- (After Step 2, before Step 5)
SELECT
    COALESCE(SUM(array_length(image_ids, 1)), 0) AS old_image_count,
    COALESCE(SUM(jsonb_array_length(image_data)), 0) AS new_image_count
FROM productimages;
-- old_image_count must equal new_image_count

-- INVARIANT 4: Every UUID in image_data actually exists as a UUID string
SELECT pi.product_id, elem->>'id' AS image_id
FROM productimages pi,
     jsonb_array_elements(pi.image_data) AS elem
WHERE elem->>'id' IS NOT NULL
  AND elem->>'id' != 'manual'
  AND NOT EXISTS (
      SELECT 1 FROM image i WHERE i.id::text = elem->>'id'
  )
LIMIT 10;
-- Expected: 0 rows (all referenced image UUIDs exist)

-- INVARIANT 5: Records per image_source unchanged
SELECT image_source, COUNT(*) FROM productimages GROUP BY image_source;
```

### Functional Smoke Tests (After Step 4)

Run these from a browser or via `curl` with valid auth:

1. **Gallery loads for a known product:**
   - Navigate to ManageProducts page in the Listing Tool UI
   - Select a product that has images (pick one from the spot-check in Step 2)
   - Verify the gallery component renders with correct image count
   - [ ] Gallery displays images

2. **Image reorder works:**
   - Drag image from position 1 to position 3
   - Verify GCS URLs update (check `1_300.jpg`, `2_300.jpg`, `3_300.jpg` accessibility)
   - Verify `productimages.image_data` is updated in the database
   - [ ] Reorder works end-to-end

3. **Image upload works:**
   - Upload a test JPEG image
   - Verify all 3 resolutions appear in GCS (`{index}_300.jpg`, `{index}_600.jpg`, `{index}_1500.jpg`)
   - Verify the `image_data` array has a new entry with `"id": "manual"`
   - [ ] Upload works end-to-end

4. **Image delete works:**
   - Delete a test image (the one you just uploaded)
   - Verify GCS blobs are removed
   - Verify `image_data` array is updated and subsequent indices shifted
   - [ ] Delete works end-to-end

5. **Photography API batch processing still works:**
   - If possible, trigger a small test batch
   - Verify `image_data` and `washtag_data_json` are written correctly
   - [ ] Batch processing works with new columns

---

## Rollback Plan

### Rollback Scenarios by Step

| Failed At | Rollback Action | Data Loss Risk |
|-----------|----------------|----------------|
| Step 1 (add columns) | Drop new columns: `ALTER TABLE productimages DROP COLUMN image_data, DROP COLUMN washtag_data_json;` | None |
| Step 2 (backfill) | Set new columns back to empty: `UPDATE productimages SET image_data = '[]', washtag_data_json = '[]';` then drop columns | None |
| Step 3 (Photography API deploy) | Redeploy previous Photography API commit. Old columns still exist, so old code works immediately. | None |
| Step 4 (Listing Tool deploy) | Redeploy previous Listing Tool API/UI. New columns and Photography API are fine either way. | None |
| Step 5 (drop old columns) | Cannot undo column drop without backup. Restore from the `pg_dump` backup taken in pre-deploy. | LOW (backup exists) |

### Full Rollback Procedure (Worst Case: Roll Back Everything)

```bash
# 1. Redeploy previous Photography API code
cd /home/ubuntu/Luxemporium/PhotoManagementNew/API
git checkout <previous-commit-sha>
# Restart service

# 2. Redeploy previous Listing Tool API code
cd "/home/ubuntu/Luxemporium/Listing Tool New/API"
git checkout <previous-commit-sha>
# Restart service

# 3. Redeploy previous Listing Tool UI
cd "/home/ubuntu/Luxemporium/Listing Tool New/UI"
git checkout <previous-commit-sha>
npm run build
# Deploy built assets

# 4. If Step 5 was executed (columns dropped), restore old columns from backup:
psql "postgres://lux_photography:...@a288413-akamai-prod-1934030-default.g2a.akamaidb.net:28582/lux_photography" \
  -f productimages_backup_<timestamp>.sql

# 5. If Step 5 was NOT executed, simply drop the new columns:
psql "postgres://lux_photography:...@a288413-akamai-prod-1934030-default.g2a.akamaidb.net:28582/lux_photography" \
  -c "ALTER TABLE productimages DROP COLUMN IF EXISTS image_data, DROP COLUMN IF EXISTS washtag_data_json;"
```

### Can We Roll Back?

- **Before Step 5:** YES -- fully reversible. Old columns still exist. Redeploy old code.
- **After Step 5:** YES with backup -- restore `productimages` data from `pg_dump` backup, then redeploy old code.
- **After Step 5 without backup:** PARTIAL -- can recreate columns and re-backfill from `image`/`washtag` tables, but any data written by the new code (manual uploads with `"id": "manual"`) cannot be reconstructed.

---

## Post-Deploy Monitoring (First 24 Hours)

### Metrics and Logs to Watch

| What | Where to Check | Alert Condition |
|------|----------------|-----------------|
| Photography API error rate | Application logs / monitoring dashboard | Any 500 errors mentioning `image_ids`, `washtag_ids`, `image_data`, or `washtag_data_json` |
| Listing Tool API error rate | Application logs / monitoring dashboard | Any 500 errors on `/products/images/*` endpoints |
| Database connection count | `pg_stat_activity` on `lux_photography` | Unexpected spike (Listing Tool adds a new connection pool) |
| GCS upload failures | Listing Tool API logs | Any GCS 403/500 errors |
| Batch processing results | Photography API batch completion logs | Any batch completing with 0 images uploaded |
| Advisory lock contention | PostgreSQL logs | Any `pg_advisory_lock` timeouts |

### Monitoring Queries (Run at +1h, +4h, +24h)

```sql
-- Check for any productimages records with empty new columns
-- (Should only be records that also have empty old columns, i.e. error records)
SELECT COUNT(*) AS empty_image_data_count
FROM productimages
WHERE (image_data IS NULL OR image_data = '[]'::jsonb)
  AND product_images_count > 0;
-- Expected: 0 (unless these are pre-existing error records)

-- Check for records written by the new code (after deploy)
SELECT id, product_id, image_source, image_data, washtag_data_json, updated_at
FROM productimages
WHERE updated_at > NOW() - INTERVAL '1 hour'
ORDER BY updated_at DESC
LIMIT 10;
-- Verify: image_data is JSONB array, not NULL or empty

-- Check for any records with "manual" uploads via the gallery
SELECT product_id, image_data
FROM productimages
WHERE image_data::text LIKE '%"manual"%'
ORDER BY updated_at DESC
LIMIT 10;
-- Informational: shows gallery upload activity

-- Verify database connection count is reasonable
SELECT datname, usename, COUNT(*) AS connection_count
FROM pg_stat_activity
WHERE datname = 'lux_photography'
GROUP BY datname, usename
ORDER BY connection_count DESC;
-- Expected: connection count within normal range (Photography API pool + Listing Tool pool)
```

### Console Spot-Checks (Run 1 hour after deploy)

```bash
# Verify a known product's images are accessible on GCS
PRODUCT_ID="<pick a known product from pre-deploy spot-check>"
for i in 1 2 3; do
  for res in 300 600 1500; do
    code=$(curl -s -o /dev/null -w "%{http_code}" \
      "https://storage.googleapis.com/lux_products/${PRODUCT_ID}/${i}_${res}.jpg")
    echo "${i}_${res}.jpg -> $code"
  done
done
# Expected: 200 for all existing images
```

---

## Data Invariants Summary

These are the invariants that must be true at every checkpoint:

| # | Invariant | Verification |
|---|-----------|-------------|
| 1 | Total `productimages` record count is unchanged | `SELECT COUNT(*) FROM productimages` matches baseline |
| 2 | Records per `image_source` unchanged | `SELECT image_source, COUNT(*) ...` matches baseline |
| 3 | No old data orphaned without new data | `missed_image_backfills` = 0, `missed_washtag_backfills` = 0 |
| 4 | Element counts match old-to-new | `SUM(array_length(image_ids))` = `SUM(jsonb_array_length(image_data))` |
| 5 | All UUIDs in `image_data` reference real `image` records (except `"manual"`) | Referential integrity query returns 0 rows |
| 6 | Photography API continues to process batches | No batch errors post-deploy |
| 7 | Listing Tool API can read/write `productimages` | Gallery loads, CRUD operations work |

---

## Known Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Active batch during Step 3 writes to old columns | Data inconsistency: new code reads new columns but batch wrote old | Check for active batches before Step 3. If a batch starts during deploy, it will fail fast on missing column reference (if Step 5 done) or write to old columns (if Step 5 not done). Old columns still exist until Step 5 so data is preserved. |
| Duplicate approved images inflate `image_data` | Incorrect image count in gallery | Known issue (167 products). Gallery should handle gracefully. Not a new risk. |
| Two services writing `productimages` concurrently | Race condition on `image_data` | Advisory locks on product_id prevent simultaneous writes from Photography API batch and Listing Tool gallery. |
| Listing Tool API connection pool exhausts `lux_photography` connections | Database unavailable for Photography API | Configure Listing Tool pool size conservatively (max 5-10 connections). Monitor `pg_stat_activity`. |
| Backfill query runs long on large table | Table locks, degraded performance | The `UPDATE ... WHERE` only touches rows with non-null arrays. Use `EXPLAIN ANALYZE` on a test DB first. If > 100K rows, batch in chunks of 10K. |
| `pg_dump` backup is incomplete or corrupt | Cannot rollback after Step 5 | Verify backup file size is > 0 and can be parsed: `pg_restore --list productimages_backup_*.sql` or `head -20 productimages_backup_*.sql` |

---

## Signoff

| Role | Name | Approved | Date |
|------|------|----------|------|
| Database Owner | | [ ] | |
| Photography API Owner | | [ ] | |
| Listing Tool API Owner | | [ ] | |
| Deploying Engineer | | [ ] | |

---

## Appendix: File Reference

| File | Service | Change Description |
|------|---------|-------------------|
| `/home/ubuntu/Luxemporium/PhotoManagementNew/API/models/db_models.py` (lines 378-393) | Photography API | Replace `image_ids`/`washtag_ids` ArrayField with `image_data`/`washtag_data_json` JSONField |
| `/home/ubuntu/Luxemporium/PhotoManagementNew/API/utils/gcs_upload_processor.py` (lines 317-318, 345-356, 463-476) | Photography API | Read/write new columns instead of old |
| `/home/ubuntu/Luxemporium/PhotoManagementNew/API/utils/gcs_product_uploader.py` (lines 123-182, 410-468) | Photography API | Write new columns instead of old |
| `/home/ubuntu/Luxemporium/PhotoManagementNew/API/utils/washtag_ai_processor.py` (line 31) | Photography API | Extract IDs from JSONB instead of array |
| `/home/ubuntu/Luxemporium/Listing Tool New/API/config.py` | Listing Tool API | Add `photography_db` connection |
| `/home/ubuntu/Luxemporium/Listing Tool New/API/app.py` | Listing Tool API | Register Tortoise ORM connection |
| `/home/ubuntu/Luxemporium/Listing Tool New/API/models/photography_models.py` | Listing Tool API | New file: ProductImages model for photography_db |
| `/home/ubuntu/Luxemporium/Listing Tool New/API/services/image_service.py` | Listing Tool API | New file: GCS operations service |
| `/home/ubuntu/Luxemporium/Listing Tool New/API/utils/image_processor.py` | Listing Tool API | New file: Pillow resolution processing |
| `/home/ubuntu/Luxemporium/Listing Tool New/API/routes/product_routes.py` | Listing Tool API | New endpoints: GET/POST/DELETE /products/images/* |
| `/home/ubuntu/Luxemporium/Listing Tool New/UI/src/components/ProductImageGallery.js` | Listing Tool UI | New file: gallery component |
| `/home/ubuntu/Luxemporium/Listing Tool New/UI/src/pages/ManageProducts.js` | Listing Tool UI | Integrate gallery component |
| `/home/ubuntu/Luxemporium/Listing Tool New/UI/src/utils/productImages.js` | Listing Tool UI | Add URL builder helpers |
