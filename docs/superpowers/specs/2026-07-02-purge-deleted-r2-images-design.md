# Purge deleted products' images from R2 — Design

**Date:** 2026-07-02
**Status:** Approved (pending spec review)

## Problem

Both scraper pipelines (`fashionbroda`, `woodtableguy`) only ever *add* data. When a
seller removes an album from Yupoo, the `validate` spider marks the product
`is_active=False, is_deleted=True` in Supabase — but the product's images stay in
Cloudflare R2 **forever**. Over time this orphaned storage is the single biggest source
of unnecessary R2 cost.

Goal: reclaim R2 storage by deleting the images belonging to products that are already
marked `is_deleted=True`, on every cron run, for both pipelines. Keep the DB rows.

## Scope

- **In scope:** deleting R2 objects for `is_deleted=True` products; a batch-delete
  endpoint on both Workers; a new purge script per project; a new end-of-run cron step;
  removing the dangerous `cleanup-delete-all` Worker block.
- **Out of scope:** purging soft-deleted DB rows (rows are kept); cleaning images
  orphaned by slug/brand *changes* (separate leak, not addressed here); image
  resize/quality reduction.

## Key decisions (from brainstorming)

1. **Delete R2, keep DB row** — soft-deleted products remain in Postgres.
2. **No schema change** — idempotency comes from R2 state, not a DB flag. Once an
   image is deleted, the next run's HEAD finds it gone and skips it.
3. **Separate end-of-run step** — runs after everything else in the cron job.
4. **HEAD then delete** — check existence with HEAD (metadata only), delete what is
   present, and log how many were still there. HEAD, **never GET** (GET bills egress).
5. **Delete via the Worker** — reuse the existing `X-Auth-Token` auth; no new R2
   credentials, no `boto3`.

## Why URL-derived keys (not prefix reconstruction)

The DB stores the exact CDN URLs that were written for each product. Stripping the CDN
base off a stored URL yields the **exact R2 key**, e.g.

```
https://wtg888.reps.cheap/products/nike/foo-abc123/images/01.jpg
  → key: products/nike/foo-abc123/images/01.jpg
```

This deletes precisely the objects the product owns, with zero brand/slug
reconstruction and none of the mismatch risk that a prefix rebuild would carry.

## Per-project constants

| | fashionbroda (fbd) | woodtableguy (wtg) |
|---|---|---|
| Table | `fashionbroda_products` | `woodtableguy_products` |
| Worker base | `https://fbd.imageuploads.workers.dev` | `https://wtg.imageuploads.workers.dev` |
| CDN base | `https://cdn.reps.cheap` | `https://wtg888.reps.cheap` |
| Auth token env | `WORKER_AUTH_TOKEN` | `WORKER2_AUTH_TOKEN` |
| Image columns | `product_image_urls`, `product_cover_image`, `size_chart_image_urls` | `product_image_urls`, `product_cover_image` |

Note: `woodtableguy` has no `size_chart_image_urls` column.

## Components

### 1. Worker changes (both `workers/fbd/src/index.ts`, `workers/wtg/src/index.ts`)

- **Add `POST /delete-keys`:**
  - Auth via `X-Auth-Token` (existing check).
  - Body: `{"keys": ["products/.../01.jpg", ...]}`.
  - Delete in chunks of ≤1000 via `env.<bucket>.delete(chunk)`.
  - Respond `{ "deleted": <count> }` (200).
  - Reject empty/malformed body with 400.
- **Keep HEAD** — reused by the purge script for the existence check.
- **Remove the `cleanup-delete-all` block** from both Workers (dangerous full-bucket
  wipe; no longer needed).
- Redeploy both Workers.

### 2. DB read helper (`.../scripts/read_db.py`, both projects)

Add `read_deleted_products()`:
- Paginated (batch 1000), same pattern as `read_db()`.
- Filter `is_deleted=True` (no `is_active` filter).
- Select `id` + the project's image columns.
- Return `{id: row}`.

### 3. Purge script (`.../scripts/purge_deleted_r2.py`, per project)

Flow:
1. Load env: Worker base URL, CDN base, auth token (raise on missing, matching the
   existing `WORKER_AUTH_TOKEN` guard pattern).
2. `rows = read_deleted_products()`.
3. For each product, collect all image URLs (array columns + cover + size-chart where
   present), strip the CDN base + leading `/` → R2 keys.
4. For each key: **HEAD** via the Worker. If present (200) → mark for deletion and
   count "still present". If 404 → skip.
5. Batch the present keys and call `POST /delete-keys` (chunks ≤1000).
6. Log a summary: products scanned, keys checked, keys still present, keys deleted.
7. `--dry-run` flag: do the HEAD checks and report, but skip the delete call.
8. `--limit N` flag: process only the first N deleted products (testing).

Failure handling: best-effort with retries on transient errors (reuse the existing
`RETRY_LIMIT`/`RETRY_DELAY` pattern from the image scripts). A key that fails to delete
is simply retried on the next cron run — no state to track.

### 4. Wiring `validate` → purge

Per the discussion, the purge runs as its **own end-of-run cron step**, not inside the
`validate` spider. `validate` continues to only mark products deleted. This keeps the
spider focused and the purge independently runnable/testable.

Add to both workflows (`.github/workflows/cron-jobs.yml`, `cron-job2.yml`) as the
**last step** (after new-album upload):

```yaml
- name: Purge deleted albums' images from R2
  run: python3 <project>/.../scripts/purge_deleted_r2.py
```

The Worker auth token is already present in each job's `env` block.

## Cost characteristics

- **Savings:** frees all R2 storage held by deleted products (the main win).
- **Ongoing ops:** with no DB flag, every run re-checks (HEAD) every `is_deleted=True`
  product. After the first purge these HEADs return 404 and nothing is deleted. HEAD is
  a cheap Class B op; this is bounded-cheap and grows slowly with the deleted backlog.
  Accepted trade-off in exchange for no schema change.
- **First run** clears the entire existing backlog automatically (all deleted rows are
  processed). Run once locally with `--dry-run` first to verify keys.

## Testing

- **URL → key conversion:** unit-test stripping each CDN base (fbd and wtg), including
  leading slash and already-key inputs.
- **Key collection:** a product with array + cover (+ size-chart for fbd), and one with
  empty/None fields, yields the expected key set.
- **Worker `POST /delete-keys`:** vitest — auth rejection, empty-body 400, chunking >1000
  keys, returns deleted count (extend `workers/*/test/index.spec.ts`).
- **Dry-run:** asserts no delete call is issued.
- Manual: `--dry-run` against real R2 on a small `--limit`, verify reported keys match a
  known deleted product.

## Rollout

1. Apply Worker changes, deploy both Workers.
2. Add `read_deleted_products()` + purge script per project.
3. Run purge locally with `--dry-run` (both projects), verify.
4. Run once for real to clear the backlog.
5. Add the cron step to both workflows.
