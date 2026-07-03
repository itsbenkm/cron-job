# Orphan Sweep — Design

**Date:** 2026-07-03
**Status:** Draft (pending approval)

## Problem

The woodtableguy R2 bucket holds ~129.8k objects / 381 GB, but active products only
reference ~63.9k keys — meaning **~65,875 objects (~51%) are orphans**: images no
product references, left behind when active products were re-slugged (the fixed
"conflicting slugs" bug). The existing `purge_deleted_r2` feature can't reach them —
it deletes by *stored URL*, and orphans have no stored URL. fashionbroda appears clean
(102k objects < 122.8k referenced keys) but this must be verified, not assumed.

Goal: reclaim orphaned R2 storage by deleting every object that **no active product
references**, per project, as a **local-only, manually-run, dry-run-by-default**
operation.

## Approach — delete by absence

```
KEEP   = every R2 key referenced by an active product   (from read_clean_db)
BUCKET = every key physically in the bucket              (from Worker GET /list-keys)
ORPHANS = BUCKET − KEEP   → delete
```

Surviving objects are exactly those a live product points to. Orphans from slug changes
AND any stray deleted-product leftovers are both cleaned in one pass.

## Key decisions

1. **Local-only.** Never wired into cron/`.yml`. Run by hand from the dev machine using
   the local `.env`.
2. **Dry-run by default.** Running bare performs the LIST + diff and prints counts +
   samples but deletes nothing. Deletion requires an explicit `--apply` flag.
3. **List via Worker.** Add `GET /list-keys` to both Workers, reusing `X-Auth-Token`.
   No new R2 credentials.
4. **Reuse existing pieces.** KEEP is built from `read_clean_db()` (already returns the
   image columns for active products) using the `cdn_url_to_r2_key` / `collect_keys`
   pure functions already in `purge_deleted_r2.py`. No change to `read_db.py`.
5. **Both projects.** A `sweep_orphans.py` twin per project. wtg is the real reclaim;
   fbd is run in dry-run first to confirm it's clean (expect ~0 orphans).

## Per-project constants

| | fbd | wtg |
|---|---|---|
| Table (via read_clean_db) | fashionbroda_products | woodtableguy_products |
| Worker base | https://fbd.imageuploads.workers.dev | https://wtg.imageuploads.workers.dev |
| CDN base | https://cdn.reps.cheap | https://wtg888.reps.cheap |
| Token env | WORKER_AUTH_TOKEN | WORKER2_AUTH_TOKEN |
| Bucket binding | env.fbd | env.wtg |

## Components

### 1. Worker `GET /list-keys` (both fbd + wtg)

- Auth via `X-Auth-Token` (existing check).
- Optional query param `?cursor=<cursor>`.
- Calls `env.<bucket>.list({ cursor, limit: 1000 })`.
- Returns `200 {"keys": string[], "cursor": string | null, "truncated": boolean}`
  where `keys` = the page's object keys, `cursor` = next cursor when `truncated`,
  else `null`.
- Redeploy both Workers.
- vitest: seed N objects, page through with cursor, assert all keys returned and
  `truncated`/`cursor` behave (including the >1000 multi-page case, seeded modestly).

### 2. `sweep_orphans.py` (per project)

Config from env (raise on missing token, like the purge script). Flow:

1. **Build KEEP:** `read_clean_db()` → for each active product, `collect_keys(row, CDN)`
   → add to a `set()`. Log `keep_count`.
2. **List BUCKET:** page `GET /list-keys` following the cursor to exhaustion; accumulate
   keys into a list. Log `bucket_count`.
3. **Diff:** `orphans = [k for k in bucket if k not in keep]`. Log `orphan_count` and
   `matched = bucket_count - orphan_count`.
4. **Report (always):** print `keep_count`, `bucket_count`, `orphan_count`, the delete
   fraction `orphan_count / bucket_count`, plus a sample of ~10 orphan keys and ~10
   kept keys for eyeballing.
5. **Guardrails before any delete:**
   - Default is dry-run — deletes only when `--apply` is passed.
   - Abort (even with `--apply`) if `keep_count == 0` (KEEP build failed).
   - Abort if delete fraction > 0.95 unless `--force` — a normalization break would
     flag ~everything as orphan; this catches it. Message names the fraction.
6. **Delete (only with `--apply` and guardrails passed):** `POST /delete-keys` in
   chunks of ≤1000. Log objects deleted; warn if `deleted != len(orphans)`.

Flags: `--apply` (perform deletion), `--force` (override the >95% safety abort),
`--limit N` (cap orphans processed, for cautious first deletes).

### 3. Tests

- **Pure diff:** `compute_orphans(bucket_keys, keep_set)` returns exactly the keys not in
  KEEP; covers overlap, full-orphan, no-orphan, and duplicate-key cases. Import-safe
  (no env at module load).
- **Key normalization:** reuse/verify `cdn_url_to_r2_key` on wtg + fbd CDN bases,
  including keys with spaces / `$` / quotes (the real slug shapes).
- **Worker:** vitest for `GET /list-keys` (auth 401, single page, multi-page cursor).

## Safety rationale

Deleting by *absence* is higher-stakes than by presence: an incomplete or mis-normalized
KEEP set deletes live images. Mitigations: robust pagination on both the DB read and the
bucket list; identical normalization of DB-derived and R2-listed keys; dry-run default;
the `keep_count==0` and `>95%` aborts; sample output for manual spot-check; and `--limit`
for a cautious first real delete.

## Rollout (manual, local)

1. Deploy both Workers (`npm run deploy`).
2. **wtg dry-run:** expect ~63,935 keep / ~65,875 orphan (~51%). Spot-check samples.
3. **wtg `--apply`** (optionally `--limit` first) → reclaim ~150–190 GB.
4. **fbd dry-run:** expect ~0 orphans → confirms clean; `--apply` only if meaningful.

## Out of scope

Image resize/quality reduction (separate lever for ongoing growth); any cron wiring.
