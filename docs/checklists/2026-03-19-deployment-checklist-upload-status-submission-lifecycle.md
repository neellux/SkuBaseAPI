# Deployment Checklist: Upload Status & Per-Platform Submission Lifecycle

**Plan:** `docs/plans/2026-03-19-feat-upload-status-platform-submission-lifecycle-plan.md`
**Date prepared:** 2026-03-19
**Target databases:** `lux_listing` (prod), `lux_listing_test` (test)
**DB host:** `a288413-akamai-prod-1934030-default.g2a.akamaidb.net:28582`

---

## PHASE 0 -- Pre-Deployment Audit (Read-Only, Run Against Prod)

Run all queries below against the **production** database (`lux_listing`) BEFORE touching anything. Save every result. Any deviation from expected values = STOP.

### 0.1 Baseline Counts

```sql
-- Q1: Total listings count (plan references "all 973 existing listings")
SELECT COUNT(*) AS total_listings FROM listings;
-- EXPECTED: ~973 (record the exact number: ________)

-- Q2: Listings by submitted status
SELECT submitted, COUNT(*) FROM listings GROUP BY submitted;
-- Save both rows. These counts must be unchanged after migration.

-- Q3: Total listing_submissions count and breakdown by status
SELECT status, COUNT(*) FROM listing_submissions GROUP BY status;
-- Save all rows. Only 'pending', 'success', 'failed' should exist today.

-- Q4: Total listing_submissions count (absolute)
SELECT COUNT(*) AS total_submissions FROM listing_submissions;
-- Record: ________

-- Q5: Submissions by platform
SELECT platform_id, status, COUNT(*)
FROM listing_submissions
GROUP BY platform_id, status
ORDER BY platform_id, status;
-- Save full result set.

-- Q6: Batch counts baseline
SELECT COUNT(*) AS total_batches FROM batches;
SELECT id, total_listings, submitted_listings FROM batches ORDER BY id DESC LIMIT 20;
-- Save for post-deploy comparison.

-- Q7: app_settings row count (should be exactly 1)
SELECT COUNT(*) FROM app_settings;
-- EXPECTED: 1

-- Q8: Current platforms list
SELECT platforms, platform_settings FROM app_settings LIMIT 1;
-- Record the current value.
```

### 0.2 Schema Pre-Checks (Verify Columns Do Not Already Exist)

```sql
-- Q9: Verify listings.upload_status does NOT exist yet
SELECT column_name FROM information_schema.columns
WHERE table_name = 'listings' AND column_name = 'upload_status';
-- EXPECTED: 0 rows

-- Q10: Verify listing_submissions new columns do NOT exist yet
SELECT column_name FROM information_schema.columns
WHERE table_name = 'listing_submissions'
AND column_name IN ('error_display', 'platform_status', 'platform_meta');
-- EXPECTED: 0 rows

-- Q11: Verify batches.platform_submission_statuses does NOT exist yet
SELECT column_name FROM information_schema.columns
WHERE table_name = 'batches' AND column_name = 'platform_submission_statuses';
-- EXPECTED: 0 rows

-- Q12: Check current external_id column type (should be varchar, will be migrated to jsonb)
SELECT data_type FROM information_schema.columns
WHERE table_name = 'listing_submissions' AND column_name = 'external_id';
-- Record current type: ________
-- If already JSONB: the ALTER TYPE step is a no-op (safe).
-- If VARCHAR: migration will cast. Check for non-JSON values first:

SELECT id, external_id FROM listing_submissions
WHERE external_id IS NOT NULL
  AND external_id !~ '^\s*[\[{"\d]';
-- EXPECTED: 0 rows (all existing values must be valid JSON or NULL).
-- WARNING: If any rows returned, the CAST in migration WILL FAIL. Fix data first.
```

### 0.3 Check Constraint Pre-Check

```sql
-- Q13: Verify no CHECK constraint already exists on listing_submissions.status
SELECT conname FROM pg_constraint
WHERE conrelid = 'listing_submissions'::regclass
AND conname = 'chk_listing_submissions_status';
-- EXPECTED: 0 rows (migration will create it)

-- Q14: Verify current listing_submissions.status values are all valid under new constraint
SELECT DISTINCT status FROM listing_submissions
WHERE status NOT IN ('queued', 'pending', 'processing', 'success', 'failed');
-- EXPECTED: 0 rows
-- CRITICAL: If any rows returned, the CHECK constraint will fail to apply.
-- Fix invalid status values BEFORE running migration.
```

### 0.4 Trigger Pre-Check

```sql
-- Q15: Existing triggers on listing_submissions
SELECT tgname, tgtype FROM pg_trigger
WHERE tgrelid = 'listing_submissions'::regclass AND NOT tgisinternal;
-- Record all trigger names. Expected: trigger_set_submitted_at

-- Q16: Existing triggers on listings
SELECT tgname, tgtype FROM pg_trigger
WHERE tgrelid = 'listings'::regclass AND NOT tgisinternal;
-- Record all trigger names.

-- Q17: Verify the function we will replace exists
SELECT proname FROM pg_proc WHERE proname = 'set_submitted_at_on_completion';
-- EXPECTED: 1 row
```

### 0.5 Stuck Submissions Check

```sql
-- Q18: Any submissions stuck in 'pending' for over 1 hour? (should be 0 at deploy time)
SELECT id, listing_id, platform_id, created_at
FROM listing_submissions
WHERE status = 'pending'
AND created_at < NOW() - INTERVAL '1 hour';
-- EXPECTED: 0 rows
-- WARNING: If rows exist, these will become subject to the stale recovery logic.
-- Decide whether to manually fail them before migration or let the poller handle them.
```

---

## PHASE 1 -- Migration Execution

### 1.0 Prerequisites

- [ ] All pre-deploy audit queries executed and results saved
- [ ] Q12 external_id data validated (no non-JSON varchar values)
- [ ] Q14 confirmed no invalid status values exist
- [ ] Q18 no stuck pending submissions (or decision made on how to handle them)
- [ ] Database backup taken (or point-in-time recovery confirmed available)
- [ ] Low-traffic window selected (no active user submissions in progress)
- [ ] Application NOT running (or placed in maintenance mode) to prevent mid-migration submissions

### 1.1 Run Migration on TEST First

```bash
# Connect to lux_listing_test and run the migration
psql -h a288413-akamai-prod-1934030-default.g2a.akamaidb.net -p 28582 \
     -U lux_listing_test -d lux_listing_test \
     -f API/migrations/add_upload_status_and_submission_lifecycle.sql
```

- [ ] Migration completes without errors on test DB
- [ ] Run verification queries (section 2) against test DB
- [ ] Smoke-test the application against test DB with new code

### 1.2 Run Migration on PROD

```bash
# Connect to lux_listing (prod) and run the migration
psql -h a288413-akamai-prod-1934030-default.g2a.akamaidb.net -p 28582 \
     -U lux_listing -d lux_listing \
     -f API/migrations/add_upload_status_and_submission_lifecycle.sql
```

- [ ] Migration completes without errors on prod DB

### 1.3 Migration Execution Order (Within the SQL File)

The migration runs inside a single `BEGIN...COMMIT` transaction. Internal order:

| Step | Operation | Risk Level | Notes |
|------|-----------|------------|-------|
| 1 | `ALTER TABLE listings ADD COLUMN upload_status` | Low | `IF NOT EXISTS`, default 'pending', idempotent |
| 2 | `UPDATE listings SET upload_status = 'uploaded'` | **MEDIUM** | Touches ALL rows. On 973 rows this is <1s. |
| 3 | `ADD CONSTRAINT chk_listings_upload_status` | Low | CHECK constraint, validates existing data |
| 4 | `CREATE INDEX idx_listings_upload_status` | Low | `IF NOT EXISTS` |
| 5 | `ALTER TABLE listing_submissions ADD COLUMN error_display` | Low | Nullable, no default |
| 6 | `ALTER TABLE listing_submissions ADD COLUMN platform_status` | Low | Nullable, no default |
| 7 | `ALTER TABLE listing_submissions ADD COLUMN platform_meta` | Low | Nullable, no default |
| 8 | `DROP CONSTRAINT chk_listing_submissions_status` | Low | `IF EXISTS` |
| 9 | `ADD CONSTRAINT chk_listing_submissions_status` | **MEDIUM** | Will fail if invalid status values exist (see Q14) |
| 10 | `ALTER COLUMN external_id TYPE JSONB` | **HIGH** | Type change on existing column. Safe if all values are valid JSON or NULL (see Q12). Acquires ACCESS EXCLUSIVE lock briefly. |
| 11 | `CREATE INDEX idx_listing_submissions_status_platform` | Low | Composite index for poller queries |
| 12 | `ALTER TABLE batches ADD COLUMN platform_submission_statuses` | Low | Default '{}', `IF NOT EXISTS` |
| 13 | `CREATE OR REPLACE FUNCTION set_submitted_at_on_completion()` | Low | Replaces existing function |
| 14 | `CREATE OR REPLACE FUNCTION update_batch_platform_statuses()` | Low | New function |
| 15 | `DROP/CREATE TRIGGER trigger_update_batch_platform_statuses` | Low | New trigger on listing_submissions |

**Total estimated runtime:** <30 seconds for ~973 listings + existing submissions.

---

## PHASE 2 -- Post-Migration Verification (Within 5 Minutes of Migration)

Run these against the database you just migrated.

### 2.1 Schema Verification

```sql
-- V1: Confirm upload_status column exists and has correct default
SELECT column_name, data_type, column_default, is_nullable
FROM information_schema.columns
WHERE table_name = 'listings' AND column_name = 'upload_status';
-- EXPECTED: varchar, 'pending'::character varying, NO

-- V2: Confirm all existing listings were backfilled to 'uploaded'
SELECT upload_status, COUNT(*) FROM listings GROUP BY upload_status;
-- EXPECTED: single row: uploaded, <same count as Q1>
-- CRITICAL: If 'pending' rows exist, the backfill failed.

-- V3: Confirm new listing_submissions columns
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'listing_submissions'
AND column_name IN ('error_display', 'platform_status', 'platform_meta')
ORDER BY column_name;
-- EXPECTED: 3 rows: error_display (text, YES), platform_meta (jsonb, YES), platform_status (varchar, YES)

-- V4: Confirm external_id is now JSONB
SELECT data_type FROM information_schema.columns
WHERE table_name = 'listing_submissions' AND column_name = 'external_id';
-- EXPECTED: jsonb

-- V5: Confirm batches.platform_submission_statuses exists
SELECT column_name, data_type, column_default
FROM information_schema.columns
WHERE table_name = 'batches' AND column_name = 'platform_submission_statuses';
-- EXPECTED: jsonb, '{}'::jsonb

-- V6: Confirm CHECK constraints
SELECT conname, pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid IN ('listings'::regclass, 'listing_submissions'::regclass)
AND contype = 'c';
-- EXPECTED: chk_listings_upload_status and chk_listing_submissions_status
```

### 2.2 Data Integrity Verification

```sql
-- V7: Listing count unchanged
SELECT COUNT(*) FROM listings;
-- MUST MATCH Q1 baseline exactly.

-- V8: Submission count unchanged
SELECT COUNT(*) FROM listing_submissions;
-- MUST MATCH Q4 baseline exactly.

-- V9: Submission status breakdown unchanged
SELECT status, COUNT(*) FROM listing_submissions GROUP BY status;
-- MUST MATCH Q3 baseline exactly.

-- V10: No data corruption in existing fields
SELECT COUNT(*) FROM listing_submissions WHERE error_display IS NOT NULL;
-- EXPECTED: 0 (new column, should all be NULL)

SELECT COUNT(*) FROM listing_submissions WHERE platform_status IS NOT NULL;
-- EXPECTED: 0

SELECT COUNT(*) FROM listing_submissions WHERE platform_meta IS NOT NULL;
-- EXPECTED: 0

-- V11: Batch counts unchanged
SELECT id, total_listings, submitted_listings FROM batches ORDER BY id DESC LIMIT 20;
-- Compare with Q6 baseline. Must match.

-- V12: Batch platform_submission_statuses initialized
SELECT COUNT(*) FROM batches WHERE platform_submission_statuses IS NULL;
-- EXPECTED: 0 (all should have default '{}')
```

### 2.3 Trigger Verification

```sql
-- V13: Triggers on listing_submissions
SELECT tgname FROM pg_trigger
WHERE tgrelid = 'listing_submissions'::regclass AND NOT tgisinternal
ORDER BY tgname;
-- EXPECTED: trigger_set_submitted_at, trigger_update_batch_platform_statuses

-- V14: Functions exist
SELECT proname FROM pg_proc
WHERE proname IN ('set_submitted_at_on_completion', 'update_batch_platform_statuses')
ORDER BY proname;
-- EXPECTED: 2 rows

-- V15: Test the updated submitted_at trigger handles 'processing' -> 'success'
-- (Dry run -- do NOT commit)
BEGIN;
INSERT INTO listing_submissions (listing_id, platform_id, status, attempt_number)
SELECT id, 'test_trigger', 'processing', 999 FROM listings LIMIT 1;

UPDATE listing_submissions SET status = 'success'
WHERE platform_id = 'test_trigger' AND attempt_number = 999;

SELECT submitted_at IS NOT NULL AS trigger_fired
FROM listing_submissions
WHERE platform_id = 'test_trigger' AND attempt_number = 999;
-- EXPECTED: true
ROLLBACK;

-- V16: Test the batch platform statuses trigger
-- (Dry run -- do NOT commit)
BEGIN;
-- Pick a listing that has a batch
SELECT l.id AS listing_id, l.product_id, l.batch_id
FROM listings l WHERE l.batch_id IS NOT NULL LIMIT 1;
-- Use the listing_id from above:
-- INSERT INTO listing_submissions (listing_id, platform_id, status, attempt_number)
-- VALUES ('<listing_id>', 'test_trigger', 'pending', 998);
-- SELECT platform_submission_statuses FROM batches WHERE id = <batch_id>;
-- EXPECTED: should contain {"<product_id>": {"test_trigger": "pending"}}
ROLLBACK;
```

### 2.4 Index Verification

```sql
-- V17: New indexes exist
SELECT indexname FROM pg_indexes
WHERE tablename IN ('listings', 'listing_submissions')
AND indexname IN ('idx_listings_upload_status', 'idx_listing_submissions_status_platform')
ORDER BY indexname;
-- EXPECTED: 2 rows
```

---

## PHASE 3 -- Application Deployment

### 3.0 Config Dependencies

Before deploying the new application code, the following config sections MUST exist in `config.toml` (or `config prod.toml`).

**Required new sections:**

```toml
[submission_poller]
enabled = false          # START DISABLED -- enable after smoke test
interval_seconds = 60
max_concurrent = 1

[spo_poller]
enabled = false          # START DISABLED -- enable after smoke test
interval_seconds = 30
max_polls_per_submission = 40
stale_processing_timeout_minutes = 1440

[spo]
api_endpoint = "https://marketplace.sspo.com/api"
api_key = "<production_api_key>"
```

**CRITICAL:** Both pollers MUST start as `enabled = false`. This is a staged rollout:

| Stage | submission_poller.enabled | spo_poller.enabled | Duration |
|-------|--------------------------|-------------------|----------|
| Deploy | `false` | `false` | Until smoke test passes |
| Stage 1 | `true` | `false` | 1-4 hours, monitor logs |
| Stage 2 | `true` | `true` | After Stage 1 is stable |

- [ ] `[submission_poller]` section added to prod config with `enabled = false`
- [ ] `[spo_poller]` section added to prod config with `enabled = false`
- [ ] `[spo]` section confirmed present with production API key (already exists in `config.toml`)
- [ ] Production config does NOT yet have `[spo_poller]` or `[submission_poller]` -- absence is handled gracefully by code defaults, but explicit `enabled = false` is safer

### 3.1 Deploy Application Code

| Step | Action | Estimated Downtime |
|------|--------|-------------------|
| 1 | Stop the running FastAPI process | ~5s |
| 2 | Deploy new code (git pull / copy) | ~30s |
| 3 | Verify config.toml has new sections | Manual check |
| 4 | Start the FastAPI process | ~10s |

- [ ] Application starts without errors in logs
- [ ] No import errors for new modules (`submission_poller`, `spo_poller`, `spo_service`)
- [ ] Startup log shows "Submission poller disabled" (since `enabled = false`)
- [ ] Startup log shows "SPO poller disabled" (since `enabled = false`)
- [ ] SellerCloud service initializes successfully (existing behavior)
- [ ] SellerCloud Internal service initializes successfully (existing behavior)

### 3.2 Post-Deploy Smoke Test (Pollers Still Disabled)

Test that the existing submission flow still works with the schema changes:

- [ ] Open an existing listing in the UI -- no errors
- [ ] `GET /listings/submission_status` returns results with new fields (`error_display`, `platform_status` as null)
- [ ] `GET /listings/detail` returns `upload_status` field (should be "uploaded" for all existing listings)
- [ ] Submit a test listing to SellerCloud -- completes as `success`
- [ ] Submit a test listing to Grailed -- completes or fails normally
- [ ] Check `listing_submissions` -- new rows have correct status, new columns are NULL
- [ ] Platform avatars render correctly in UI (no JS errors in console)
- [ ] Verify the `submitted_at` trigger still fires on `pending -> success`

### 3.3 Enable Submission Poller (Stage 1)

```toml
[submission_poller]
enabled = true
interval_seconds = 60
max_concurrent = 1
```

- [ ] Restart application (or use hot-reload if `reload = true`)
- [ ] Startup log shows "Submission poller started" (or equivalent)
- [ ] Watch logs for 5 minutes -- poller cycles complete without errors
- [ ] Verify poller does NOT create spurious submissions (no `auto_submit` configured yet)
- [ ] Check DB: no unexpected new rows in `listing_submissions`

### 3.4 Enable SPO Poller (Stage 2 -- After Stage 1 Stable for 1-4 Hours)

```toml
[spo_poller]
enabled = true
interval_seconds = 30
max_polls_per_submission = 40
stale_processing_timeout_minutes = 1440
```

- [ ] Restart application
- [ ] Startup log shows "SPO poller started" (or equivalent)
- [ ] Watch logs for 5 minutes -- poller cycles complete without errors
- [ ] Since no SPO submissions exist yet, poller should log "no pending SPO submissions" each cycle
- [ ] Submit a test listing to SPO platform to validate end-to-end flow

---

## PHASE 4 -- Monitoring Plan (First 24 Hours)

### 4.1 Log Monitoring

| What to Watch | Log Pattern | Alert Condition |
|---------------|-------------|-----------------|
| Submission poller errors | `"Submission poller error:"` | Any occurrence |
| SPO poller errors | `"SPO poller error:"` or `"spo_poller"` | Any occurrence |
| Stale submission recovery | `"Submission timed out"` | More than 5 in 1 hour |
| Trigger errors | `"trigger"` + `"error"` in postgres logs | Any occurrence |
| DB connection errors | `"connection"` + `"refused"` or `"timeout"` | Any occurrence |
| Background task unhandled exceptions | `"Task exception was never retrieved"` | Any occurrence |
| External_id cast failures | `"jsonb"` + `"invalid"` | Any occurrence (should be 0 post-migration) |

### 4.2 Database Monitoring Queries

Run these at +1h, +4h, +12h, +24h after deploy:

```sql
-- M1: Submission status distribution (should evolve naturally)
SELECT status, COUNT(*) FROM listing_submissions GROUP BY status ORDER BY status;

-- M2: Any submissions stuck in non-terminal state too long?
SELECT id, listing_id, platform_id, status, platform_status, created_at,
       NOW() - created_at AS age
FROM listing_submissions
WHERE status IN ('queued', 'pending', 'processing')
AND created_at < NOW() - INTERVAL '30 minutes'
ORDER BY created_at;
-- EXPECTED: 0 rows (except SPO 'processing' which can take hours)
-- For SPO: 'processing' up to 24 hours is normal, but check platform_status is progressing

-- M3: upload_status distribution
SELECT upload_status, COUNT(*) FROM listings GROUP BY upload_status;
-- Initially: all 'uploaded'. New listings may have 'pending'.

-- M4: Batch trigger working? (only if submissions happened since deploy)
SELECT id, platform_submission_statuses
FROM batches
WHERE platform_submission_statuses != '{}'
ORDER BY updated_at DESC LIMIT 10;

-- M5: Error display populated correctly for failures?
SELECT id, platform_id, status, error_display, LEFT(error, 100) AS error_preview
FROM listing_submissions
WHERE status = 'failed'
AND created_at > NOW() - INTERVAL '24 hours'
ORDER BY created_at DESC LIMIT 20;
-- Check: error_display should be human-readable, error should have traceback

-- M6: SPO-specific: any processing submissions with stale platform_status?
SELECT id, listing_id, platform_status, platform_meta, created_at
FROM listing_submissions
WHERE platform_id = 'spo' AND status = 'processing'
ORDER BY created_at;
```

### 4.3 What to Watch For

**First Hour:**
- Application starts cleanly, no crash loops
- Existing submission flow (SellerCloud, Grailed) works unchanged
- No foreign key or constraint violations in logs
- Poller logs show clean cycles (no errors, correct interval timing)

**Hours 1-4:**
- No gradual memory increase from poller tasks
- `asyncio.create_task` is not leaking tasks (check with `len(asyncio.all_tasks())` if possible)
- Frontend polling intervals are correct (1.5s for pending, not hammering the API)
- No 500 errors on `/listings/submission_status` or `/listings/detail`

**Hours 4-24:**
- SPO submissions (if any) progressing through lifecycle correctly
- `platform_status` values make sense: `products_uploading -> products_processing -> offers_processing -> listed`
- Stale recovery is NOT firing for healthy submissions (would indicate the timeout is too aggressive)
- `batches.platform_submission_statuses` JSONB is growing correctly, not corrupted
- No data in `platform_meta` is excessively large (should be small import_id references)

**Known Risk Indicators (Escalate Immediately):**

| Indicator | Likely Cause | Action |
|-----------|-------------|--------|
| `"constraint chk_listing_submissions_status"` errors | Code path setting invalid status string | Fix code, do not disable constraint |
| `trigger_update_batch_platform_statuses` errors blocking INSERTs | Bug in trigger function (NULL handling) | See rollback section for trigger-only rollback |
| SPO poller running continuously without sleeping | `_shutdown_event` not working, or exception in sleep | Restart app, disable poller |
| `listing_submissions` row count growing rapidly | Auto-submit creating duplicates | Disable `submission_poller`, investigate |
| `SELECT FOR UPDATE` deadlocks in logs | Poller and submit endpoint racing | Expected occasionally; if frequent, increase poller interval |

---

## PHASE 5 -- Rollback Plan

### 5.1 Rollback Decision Matrix

| Scenario | Rollback Scope | Data Loss Risk |
|----------|---------------|----------------|
| Migration SQL fails mid-transaction | None needed -- transaction rolls back automatically | None |
| App code crashes on startup | Roll back code only, DB changes are backward-compatible | None |
| Poller creating bad data | Disable poller in config, keep DB changes | None |
| Trigger blocking submissions | Drop trigger only (see 5.3) | None |
| Complete rollback needed | Full DB + code rollback (see 5.4) | New columns/data lost |

### 5.2 Quick Fix: Disable Pollers Without Redeployment

If pollers are causing issues, the fastest fix is config change + restart:

```toml
[submission_poller]
enabled = false

[spo_poller]
enabled = false
```

Then restart the application. All existing submission functionality continues to work. The pollers are additive features.

### 5.3 Rollback: Drop New Trigger Only (If Blocking Submissions)

If `trigger_update_batch_platform_statuses` is causing INSERT/UPDATE failures:

```sql
-- Emergency: drop only the new trigger
DROP TRIGGER IF EXISTS trigger_update_batch_platform_statuses ON listing_submissions;
DROP FUNCTION IF EXISTS update_batch_platform_statuses();
```

This leaves all other schema changes intact. The `batches.platform_submission_statuses` column will stop updating but causes no harm.

### 5.4 Full Database Rollback SQL

Run this ONLY if a complete rollback to pre-migration state is required. This is destructive and will lose any data written to new columns since migration.

```sql
BEGIN;

-- === REVERT LISTINGS TABLE ===
-- Drop CHECK constraint
ALTER TABLE listings DROP CONSTRAINT IF EXISTS chk_listings_upload_status;
-- Drop index
DROP INDEX IF EXISTS idx_listings_upload_status;
-- Drop column (WARNING: loses upload_status data)
ALTER TABLE listings DROP COLUMN IF EXISTS upload_status;

-- === REVERT LISTING_SUBMISSIONS TABLE ===
-- Drop new trigger
DROP TRIGGER IF EXISTS trigger_update_batch_platform_statuses ON listing_submissions;
DROP FUNCTION IF EXISTS update_batch_platform_statuses();

-- Drop new CHECK constraint
ALTER TABLE listing_submissions DROP CONSTRAINT IF EXISTS chk_listing_submissions_status;

-- Drop new index
DROP INDEX IF EXISTS idx_listing_submissions_status_platform;

-- Drop new columns (WARNING: loses error_display, platform_status, platform_meta data)
ALTER TABLE listing_submissions DROP COLUMN IF EXISTS error_display;
ALTER TABLE listing_submissions DROP COLUMN IF EXISTS platform_status;
ALTER TABLE listing_submissions DROP COLUMN IF EXISTS platform_meta;

-- Revert external_id from JSONB back to VARCHAR(255)
-- WARNING: This will fail if any external_id values are complex JSON objects.
-- Only safe if values are simple strings or NULL.
ALTER TABLE listing_submissions ALTER COLUMN external_id TYPE VARCHAR(255)
    USING CASE WHEN external_id IS NULL THEN NULL ELSE external_id::text END;

-- === REVERT BATCHES TABLE ===
ALTER TABLE batches DROP COLUMN IF EXISTS platform_submission_statuses;

-- === RESTORE ORIGINAL submitted_at TRIGGER ===
CREATE OR REPLACE FUNCTION set_submitted_at_on_completion()
RETURNS TRIGGER AS $$
BEGIN
    -- Original: Only set submitted_at when status changes from 'pending' to 'success' or 'failed'
    IF OLD.status = 'pending' AND NEW.status IN ('success', 'failed') THEN
        NEW.submitted_at = CURRENT_TIMESTAMP;
    END IF;
    -- Always update updated_at on any change
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMIT;
```

### 5.5 Code-Only Rollback

If the database migration is fine but the new application code is broken:

1. Deploy the previous version of the code
2. The old code will simply ignore new columns (`error_display`, `platform_status`, `platform_meta`, `upload_status`, `platform_submission_statuses`) -- Tortoise ORM only reads columns it knows about
3. The old `set_submitted_at_on_completion` trigger is a superset of the old behavior (also handles `processing -> success/failed`) -- this is backward-compatible
4. The new `trigger_update_batch_platform_statuses` will continue firing but only writes to `platform_submission_statuses` which the old code ignores
5. The CHECK constraint on `listing_submissions.status` will NOT block old code because old code only writes `pending`, `success`, `failed` -- all valid values

**Code-only rollback is safe and non-destructive.**

### 5.6 Rollback Verification Queries

After any rollback, run:

```sql
-- RV1: Confirm columns reverted (full rollback only)
SELECT column_name FROM information_schema.columns
WHERE table_name = 'listings' AND column_name = 'upload_status';
-- EXPECTED after full rollback: 0 rows

-- RV2: Confirm triggers reverted
SELECT tgname FROM pg_trigger
WHERE tgrelid = 'listing_submissions'::regclass AND NOT tgisinternal;
-- EXPECTED after full rollback: only trigger_set_submitted_at

-- RV3: Listing count unchanged
SELECT COUNT(*) FROM listings;
-- MUST match original baseline

-- RV4: Submission count unchanged
SELECT COUNT(*) FROM listing_submissions;
-- MUST match original baseline

-- RV5: Submit a test listing to verify the original flow works
-- (Manual test in UI)
```

---

## PHASE 6 -- Post-Rollout Configuration (After 24 Hours Stable)

These are optional steps to enable the full feature set once the deployment is confirmed stable.

### 6.1 Enable Auto-Submit for Specific Platforms

Update `platform_settings` in `app_settings` via `PUT /settings/platform_settings`:

```json
{
  "spo": {
    "auto_submit": true,
    "allow_resubmit": true
  },
  "grailed": {
    "auto_submit": false,
    "allow_resubmit": true
  },
  "sellercloud": {
    "auto_submit": false,
    "allow_resubmit": true
  }
}
```

- [ ] Only enable `auto_submit` after confirming the submission poller is stable
- [ ] Monitor for unexpected submission volume after enabling

### 6.2 Add SPO to Enabled Platforms

Update `platforms` in `app_settings` to include `"spo"`:

```json
["sellercloud", "grailed", "spo"]
```

- [ ] SPO platform appears in the UI platform selection
- [ ] Test a manual SPO submission end-to-end before enabling auto_submit

---

## Summary Checklist (Print This Page)

### Pre-Deploy (Required)
- [ ] Baseline queries Q1-Q18 executed and results saved
- [ ] Q12: external_id values are valid JSON or NULL
- [ ] Q14: No invalid status values in listing_submissions
- [ ] Q18: No stuck pending submissions (or handled)
- [ ] Database backup confirmed
- [ ] Low-traffic window selected

### Migration
- [ ] Migration succeeds on `lux_listing_test`
- [ ] Verification queries pass on test
- [ ] Migration succeeds on `lux_listing` (prod)
- [ ] Verification queries V1-V17 pass on prod

### Application Deploy
- [ ] Config updated with `[submission_poller]` and `[spo_poller]` (both `enabled = false`)
- [ ] Application deploys and starts without errors
- [ ] Existing SellerCloud/Grailed submission flow works (smoke test)
- [ ] Frontend renders without errors

### Staged Poller Enablement
- [ ] Stage 1: `submission_poller.enabled = true` -- monitor 1-4 hours
- [ ] Stage 2: `spo_poller.enabled = true` -- monitor 1-4 hours

### Monitoring (24 Hours)
- [ ] +1h check: queries M1-M6, log review
- [ ] +4h check: queries M1-M6, log review
- [ ] +12h check: queries M1-M6, log review
- [ ] +24h check: queries M1-M6, final sign-off

### Rollback Ready
- [ ] Rollback SQL saved and accessible
- [ ] Team knows: config change + restart disables pollers immediately
- [ ] Team knows: code-only rollback is safe (DB changes are backward-compatible)
