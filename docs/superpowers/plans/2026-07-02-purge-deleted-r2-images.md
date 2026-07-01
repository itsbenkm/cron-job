# Purge Deleted Products' Images from R2 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** On every cron run, delete the R2 images belonging to products already marked `is_deleted=True`, for both the `fashionbroda` and `woodtableguy` pipelines, keeping the DB rows.

**Architecture:** A new per-project Python script queries all `is_deleted=True` products, derives each image's exact R2 key by stripping the CDN base off the stored URL, HEAD-checks each key via the project's Cloudflare Worker, then batch-deletes the present ones through a new `POST /delete-keys` Worker endpoint. The script runs as the last step of each cron workflow. No DB schema change — R2 state is the idempotency source.

**Tech Stack:** Python 3.12 (scrapy projects, `requests`, `supabase-py`), Cloudflare Workers (TypeScript, R2 bindings, vitest), GitHub Actions.

## Global Constraints

- Existence check uses **HEAD only, never GET** (GET bills R2 egress).
- Delete via the Worker using the existing `X-Auth-Token` auth — no new R2 credentials, no `boto3`.
- Batch delete in chunks of **≤1000 keys** per Worker call.
- Pure helper functions must have **no import-time side effects** (no env reads / raises at module top) so they are unit-testable.
- Per-project constants:
  - fbd: table `fashionbroda_products`, Worker `https://fbd.imageuploads.workers.dev`, CDN `https://cdn.reps.cheap`, token env `WORKER_AUTH_TOKEN`, image cols `product_image_urls`, `product_cover_image`, `size_chart_image_urls`.
  - wtg: table `woodtableguy_products`, Worker `https://wtg.imageuploads.workers.dev`, CDN `https://wtg888.reps.cheap`, token env `WORKER2_AUTH_TOKEN`, image cols `product_image_urls`, `product_cover_image` (no size-chart).

---

### Task 1: fbd Worker — `POST /delete-keys` endpoint + remove `cleanup-delete-all`

**Files:**
- Modify: `workers/fbd/src/index.ts`
- Modify: `workers/fbd/vitest.config.mts`
- Test: `workers/fbd/test/index.spec.ts` (replace stale boilerplate)

**Interfaces:**
- Produces: `POST /delete-keys` with header `X-Auth-Token`, body `{"keys": string[]}` → `200 {"deleted": number}`; `400` on invalid/empty body; `401` on bad token.

- [ ] **Step 1: Give the test env an auth token binding**

In `workers/fbd/vitest.config.mts`, add a `miniflare` bindings block so `env.AUTH_TOKEN` is defined in tests:

```ts
import { defineWorkersConfig } from "@cloudflare/vitest-pool-workers/config";

export default defineWorkersConfig({
	test: {
		poolOptions: {
			workers: {
				wrangler: { configPath: "./wrangler.jsonc" },
				miniflare: {
					bindings: { AUTH_TOKEN: "test-token" },
				},
			},
		},
	},
});
```

- [ ] **Step 2: Replace the stale boilerplate tests with real ones**

Overwrite `workers/fbd/test/index.spec.ts`:

```ts
import { SELF } from "cloudflare:test";
import { describe, it, expect } from "vitest";

const AUTH = { "X-Auth-Token": "test-token" };

describe("fbd worker /delete-keys", () => {
	it("deletes an existing key and HEAD then returns 404", async () => {
		const key = "products/x/y/01.jpg";
		await SELF.fetch(`https://example.com/${key}`, {
			method: "PUT",
			headers: { ...AUTH, "Content-Type": "image/jpeg" },
			body: new Uint8Array([1, 2, 3]),
		});

		const before = await SELF.fetch(`https://example.com/${key}`, { method: "HEAD", headers: AUTH });
		expect(before.status).toBe(200);

		const res = await SELF.fetch("https://example.com/delete-keys", {
			method: "POST",
			headers: { ...AUTH, "Content-Type": "application/json" },
			body: JSON.stringify({ keys: [key] }),
		});
		expect(res.status).toBe(200);
		expect(await res.json()).toEqual({ deleted: 1 });

		const after = await SELF.fetch(`https://example.com/${key}`, { method: "HEAD", headers: AUTH });
		expect(after.status).toBe(404);
	});

	it("rejects an unauthorized request with 401", async () => {
		const res = await SELF.fetch("https://example.com/delete-keys", {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ keys: ["a"] }),
		});
		expect(res.status).toBe(401);
	});

	it("rejects an empty keys array with 400", async () => {
		const res = await SELF.fetch("https://example.com/delete-keys", {
			method: "POST",
			headers: { ...AUTH, "Content-Type": "application/json" },
			body: JSON.stringify({ keys: [] }),
		});
		expect(res.status).toBe(400);
	});
});
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cd workers/fbd && npm test -- --run`
Expected: FAIL — `/delete-keys` returns 405 (`deleted` assertion fails / 200 expected).

- [ ] **Step 4: Add the endpoint and remove the cleanup block**

In `workers/fbd/src/index.ts`, delete the entire `// ── CLEANUP — one-time delete of ALL objects` block (the `if (request.method === 'DELETE' && key === 'cleanup-delete-all')` branch). Then, immediately before the final `return new Response('Method not allowed', { status: 405 });`, insert:

```ts
		// ── POST /delete-keys — batch delete R2 objects by exact key ──
		if (request.method === 'POST' && key === 'delete-keys') {
			let parsed: { keys?: unknown };
			try {
				parsed = await request.json();
			} catch {
				return new Response('Invalid JSON body', { status: 400 });
			}
			const keys = parsed?.keys;
			if (!Array.isArray(keys) || keys.length === 0 || !keys.every((k) => typeof k === 'string')) {
				return new Response('Body must be { keys: string[] } with at least one key', { status: 400 });
			}

			let deleted = 0;
			for (let i = 0; i < keys.length; i += 1000) {
				const chunk = keys.slice(i, i + 1000) as string[];
				await env.fbd.delete(chunk);
				deleted += chunk.length;
			}

			return new Response(JSON.stringify({ deleted }), {
				status: 200,
				headers: { 'Content-Type': 'application/json' },
			});
		}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd workers/fbd && npm test -- --run`
Expected: PASS (3 passed).

- [ ] **Step 6: Commit**

```bash
git add workers/fbd/src/index.ts workers/fbd/test/index.spec.ts workers/fbd/vitest.config.mts
git commit -m "feat(fbd worker): add POST /delete-keys, remove cleanup-delete-all"
```

---

### Task 2: wtg Worker — `POST /delete-keys` endpoint + remove `cleanup-delete-all`

**Files:**
- Modify: `workers/wtg/src/index.ts`
- Modify: `workers/wtg/vitest.config.mts`
- Test: `workers/wtg/test/index.spec.ts` (replace stale boilerplate)

**Interfaces:**
- Produces: identical `POST /delete-keys` contract as Task 1, backed by the `wtg` bucket.

Note: in `workers/wtg/src/index.ts` the key variable is named `r2Key` (not `key`) and the bucket binding is `env.wtg`.

- [ ] **Step 1: Add the test auth token binding**

Edit `workers/wtg/vitest.config.mts` to match the fbd version — add the same `miniflare: { bindings: { AUTH_TOKEN: "test-token" } }` block inside `poolOptions.workers`.

- [ ] **Step 2: Replace the stale boilerplate tests**

Overwrite `workers/wtg/test/index.spec.ts` with the same file content as Task 1 Step 2, changing only the top `describe` label to `"wtg worker /delete-keys"`.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cd workers/wtg && npm test -- --run`
Expected: FAIL — `/delete-keys` returns 405.

- [ ] **Step 4: Add the endpoint and remove the cleanup block**

In `workers/wtg/src/index.ts`, delete the `if (request.method === 'DELETE' && r2Key === 'cleanup-delete-all')` block. Then, immediately before the final `return new Response('Method not allowed', { status: 405 });`, insert:

```ts
		// ── POST /delete-keys — batch delete R2 objects by exact key ──
		if (request.method === 'POST' && r2Key === 'delete-keys') {
			let parsed: { keys?: unknown };
			try {
				parsed = await request.json();
			} catch {
				return new Response('Invalid JSON body', { status: 400 });
			}
			const keys = parsed?.keys;
			if (!Array.isArray(keys) || keys.length === 0 || !keys.every((k) => typeof k === 'string')) {
				return new Response('Body must be { keys: string[] } with at least one key', { status: 400 });
			}

			let deleted = 0;
			for (let i = 0; i < keys.length; i += 1000) {
				const chunk = keys.slice(i, i + 1000) as string[];
				await env.wtg.delete(chunk);
				deleted += chunk.length;
			}

			return new Response(JSON.stringify({ deleted }), {
				status: 200,
				headers: { 'Content-Type': 'application/json' },
			});
		}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd workers/wtg && npm test -- --run`
Expected: PASS (3 passed).

- [ ] **Step 6: Commit**

```bash
git add workers/wtg/src/index.ts workers/wtg/test/index.spec.ts workers/wtg/vitest.config.mts
git commit -m "feat(wtg worker): add POST /delete-keys, remove cleanup-delete-all"
```

---

### Task 3: fbd — `read_deleted_products()` + `purge_deleted_r2.py`

**Files:**
- Modify: `fashionbroda/fashionbroda_cj/fashionbroda_cj/scripts/read_db.py`
- Create: `fashionbroda/fashionbroda_cj/fashionbroda_cj/scripts/purge_deleted_r2.py`
- Test: `fashionbroda/fashionbroda_cj/fashionbroda_cj/scripts/test_purge_deleted_r2.py`

**Interfaces:**
- Consumes: `read_deleted_products()` from `read_db.py` (added here).
- Produces:
  - `read_deleted_products() -> dict[str, dict]` — `{id: {id, product_image_urls, product_cover_image, size_chart_image_urls}}` for all `is_deleted=True`.
  - `cdn_url_to_r2_key(url: str, cdn_base: str) -> str`
  - `collect_keys(row: dict, cdn_base: str) -> list[str]`

- [ ] **Step 1: Write failing unit tests for the pure functions**

Create `fashionbroda/fashionbroda_cj/fashionbroda_cj/scripts/test_purge_deleted_r2.py`:

```python
from fashionbroda_cj.scripts.purge_deleted_r2 import cdn_url_to_r2_key, collect_keys

CDN = "https://cdn.reps.cheap"


def test_strips_cdn_base_to_key():
    url = "https://cdn.reps.cheap/products/nike/foo-abc123/product/01.jpg"
    assert cdn_url_to_r2_key(url, CDN) == "products/nike/foo-abc123/product/01.jpg"


def test_handles_leading_slash_only_input():
    assert cdn_url_to_r2_key("/products/x/y/01.jpg", CDN) == "products/x/y/01.jpg"


def test_collect_keys_gathers_all_image_fields():
    row = {
        "id": "1",
        "product_image_urls": [f"{CDN}/products/x/y/product/01.jpg"],
        "product_cover_image": f"{CDN}/products/x/y/cover.jpg",
        "size_chart_image_urls": [f"{CDN}/products/x/y/size-chart/01.jpg"],
    }
    assert collect_keys(row, CDN) == [
        "products/x/y/product/01.jpg",
        "products/x/y/cover.jpg",
        "products/x/y/size-chart/01.jpg",
    ]


def test_collect_keys_ignores_empty_fields():
    row = {"id": "1", "product_image_urls": None, "product_cover_image": None, "size_chart_image_urls": []}
    assert collect_keys(row, CDN) == []
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd fashionbroda/fashionbroda_cj && pip install pytest -q && python -m pytest fashionbroda_cj/scripts/test_purge_deleted_r2.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named '...purge_deleted_r2'`.

- [ ] **Step 3: Create the purge script (pure functions have no import-time side effects)**

Create `fashionbroda/fashionbroda_cj/fashionbroda_cj/scripts/purge_deleted_r2.py`:

```python
#!/usr/bin/env python3
"""
Purge R2 images for products marked is_deleted=True (fashionbroda).

- Reads all is_deleted=True rows from the DB.
- Derives each image's exact R2 key by stripping the CDN base off the stored URL.
- HEAD-checks each key via the Worker (HEAD only — never GET, to avoid egress cost).
- Batch-deletes the present keys via the Worker's POST /delete-keys.

Idempotent: once an object is deleted, its next HEAD is 404 and it is skipped.
No DB flag is used — R2 state is the source of truth.

Known limitation: keys are rebuilt from the stored CDN URLs. If a product's images
were re-uploaded under a different key after a slug/brand change, only the currently
stored URLs are purged (the separate slug-change orphan case is out of scope).
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import requests
from dotenv import load_dotenv

from fashionbroda_cj.scripts.paths import LOGS_DIR
from fashionbroda_cj.scripts.read_db import read_deleted_products

WORKER_BASE_URL = "https://fbd.imageuploads.workers.dev"
CDN_BASE_URL = "https://cdn.reps.cheap"
RETRY_LIMIT = 3
RETRY_DELAY = 2

log = logging.getLogger(__name__)


# ── Pure helpers (no side effects — unit tested) ─────────────────────────────

def cdn_url_to_r2_key(url: str, cdn_base: str) -> str:
    """Strip the CDN base (and any leading slash) to yield the exact R2 key."""
    prefix = cdn_base.rstrip("/") + "/"
    if url.startswith(prefix):
        return url[len(prefix):]
    return url.lstrip("/")


def collect_keys(row: dict, cdn_base: str) -> list[str]:
    """Collect every image R2 key for a product row."""
    urls: list[str] = []
    urls.extend(row.get("product_image_urls") or [])
    if row.get("product_cover_image"):
        urls.append(row["product_cover_image"])
    urls.extend(row.get("size_chart_image_urls") or [])
    return [cdn_url_to_r2_key(u, cdn_base) for u in urls if u]


# ── Worker I/O ───────────────────────────────────────────────────────────────

def worker_head_exists(key: str, headers: dict) -> bool:
    """HEAD a key via the Worker. True if it exists (200), False on 404/error."""
    url = f"{WORKER_BASE_URL}/{key}"
    for attempt in range(RETRY_LIMIT):
        try:
            r = requests.head(url, headers=headers, timeout=30)
            if r.status_code == 200:
                return True
            if r.status_code == 404:
                return False
            log.warning(f"HEAD {key} got {r.status_code}")
        except Exception as e:
            log.warning(f"HEAD {key} attempt {attempt + 1} failed: {e}")
            time.sleep(RETRY_DELAY)
    return False


def worker_delete_keys(keys: list[str], headers: dict) -> int:
    """Delete keys via POST /delete-keys in chunks of 1000. Returns count deleted."""
    total = 0
    for i in range(0, len(keys), 1000):
        chunk = keys[i:i + 1000]
        url = f"{WORKER_BASE_URL}/delete-keys"
        for attempt in range(RETRY_LIMIT):
            try:
                r = requests.post(
                    url,
                    json={"keys": chunk},
                    headers={**headers, "Content-Type": "application/json"},
                    timeout=60,
                )
                if r.status_code == 200:
                    total += r.json().get("deleted", 0)
                    break
                log.warning(f"delete-keys got {r.status_code}: {r.text[:200]}")
            except Exception as e:
                log.warning(f"delete-keys attempt {attempt + 1} failed: {e}")
                time.sleep(RETRY_DELAY)
    return total


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Purge deleted products' images from R2")
    parser.add_argument("--dry-run", action="store_true", help="HEAD-check and report, but do not delete")
    parser.add_argument("--limit", type=int, default=None, help="Process only first N deleted products")
    args = parser.parse_args()

    load_dotenv()
    token = os.getenv("WORKER_AUTH_TOKEN")
    if not token:
        raise EnvironmentError("WORKER_AUTH_TOKEN is not set.")
    headers = {"X-Auth-Token": token}

    Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(Path(LOGS_DIR) / "purge_deleted_r2.log")],
    )

    rows = list(read_deleted_products().values())
    if args.limit:
        rows = rows[: args.limit]
    log.info(f"Loaded {len(rows)} deleted products")

    all_keys: list[str] = []
    for row in rows:
        all_keys.extend(collect_keys(row, CDN_BASE_URL))
    log.info(f"Collected {len(all_keys)} candidate keys")

    present = [k for k in all_keys if worker_head_exists(k, headers)]
    log.info(f"{len(present)} keys still present in R2")

    if args.dry_run:
        log.info("Dry run — no deletions performed")
        for k in present:
            log.info(f"  would delete: {k}")
        return

    deleted = worker_delete_keys(present, headers)
    log.info(f"Deleted {deleted} objects from R2")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Add `read_deleted_products()` to `read_db.py`**

Append to `fashionbroda/fashionbroda_cj/fashionbroda_cj/scripts/read_db.py` (before the `if __name__` block):

```python
def read_deleted_products():
    batch_size = 1000
    start = 0
    all_rows = []

    while True:
        end = start + batch_size - 1
        data = (
            supabase.table("fashionbroda_products")
            .select(
                """
                id,
                product_image_urls,
                product_cover_image,
                size_chart_image_urls
                """
            )
            .eq("is_deleted", True)
            .eq("is_active", False)
            .range(start, end)
            .execute()
        )
        if not data.data:
            break
        all_rows.extend(data.data)
        print(f"Fetched deleted rows {start} to {end} -> {len(data.data)} rows")
        start += batch_size

    return {row["id"]: row for row in all_rows}
```

- [ ] **Step 5: Run the unit tests to verify they pass**

Run: `cd fashionbroda/fashionbroda_cj && python -m pytest fashionbroda_cj/scripts/test_purge_deleted_r2.py -v`
Expected: PASS (4 passed). The pure-function import must not require env vars — confirms no import-time side effects.

- [ ] **Step 6: Commit**

```bash
git add fashionbroda/fashionbroda_cj/fashionbroda_cj/scripts/read_db.py \
        fashionbroda/fashionbroda_cj/fashionbroda_cj/scripts/purge_deleted_r2.py \
        fashionbroda/fashionbroda_cj/fashionbroda_cj/scripts/test_purge_deleted_r2.py
git commit -m "feat(fbd): add purge_deleted_r2 script and read_deleted_products"
```

---

### Task 4: wtg — `read_deleted_products()` + `purge_deleted_r2.py`

**Files:**
- Modify: `woodtableguy/wtg/wtg/scripts/read_db.py`
- Create: `woodtableguy/wtg/wtg/scripts/purge_deleted_r2.py`
- Test: `woodtableguy/wtg/wtg/scripts/test_purge_deleted_r2.py`

**Interfaces:**
- Same function names/contracts as Task 3, but: table `woodtableguy_products`, Worker `https://wtg.imageuploads.workers.dev`, CDN `https://wtg888.reps.cheap`, token env `WORKER2_AUTH_TOKEN`, and **no `size_chart_image_urls`** column. `collect_keys` keeps the same body (a missing `size_chart_image_urls` key yields `[]`, so it is harmless and identical).

- [ ] **Step 1: Write failing unit tests**

Create `woodtableguy/wtg/wtg/scripts/test_purge_deleted_r2.py`:

```python
from wtg.scripts.purge_deleted_r2 import cdn_url_to_r2_key, collect_keys

CDN = "https://wtg888.reps.cheap"


def test_strips_cdn_base_to_key():
    url = "https://wtg888.reps.cheap/products/nike/foo-abc123/images/01.jpg"
    assert cdn_url_to_r2_key(url, CDN) == "products/nike/foo-abc123/images/01.jpg"


def test_handles_leading_slash_only_input():
    assert cdn_url_to_r2_key("/products/x/y/01.jpg", CDN) == "products/x/y/01.jpg"


def test_collect_keys_gathers_images_and_cover():
    row = {
        "id": "1",
        "product_image_urls": [f"{CDN}/products/x/y/images/01.jpg"],
        "product_cover_image": f"{CDN}/products/x/y/cover.jpg",
    }
    assert collect_keys(row, CDN) == [
        "products/x/y/images/01.jpg",
        "products/x/y/cover.jpg",
    ]


def test_collect_keys_ignores_empty_fields():
    row = {"id": "1", "product_image_urls": None, "product_cover_image": None}
    assert collect_keys(row, CDN) == []
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd woodtableguy/wtg && pip install pytest -q && python -m pytest wtg/scripts/test_purge_deleted_r2.py -v`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Create the purge script**

Create `woodtableguy/wtg/wtg/scripts/purge_deleted_r2.py` with the **same content as Task 3 Step 3**, changing exactly these lines:
- Module docstring: replace `(fashionbroda)` with `(woodtableguy)`.
- `WORKER_BASE_URL = "https://wtg.imageuploads.workers.dev"`
- `CDN_BASE_URL = "https://wtg888.reps.cheap"`
- Imports: `from wtg.scripts.paths import LOGS_DIR` and `from wtg.scripts.read_db import read_deleted_products`.
- In `main()`: `token = os.getenv("WORKER2_AUTH_TOKEN")` and the error message `"WORKER2_AUTH_TOKEN is not set."`.

Everything else (the `cdn_url_to_r2_key`, `collect_keys`, `worker_head_exists`, `worker_delete_keys`, argparse, and main flow) is identical. `collect_keys` keeps the `row.get("size_chart_image_urls") or []` line unchanged — it resolves to `[]` for wtg.

Note: `LOGS_DIR` in `wtg/scripts/paths.py` is a `Path`; `Path(LOGS_DIR)` is still valid.

- [ ] **Step 4: Add `read_deleted_products()` to `read_db.py`**

Append to `woodtableguy/wtg/wtg/scripts/read_db.py` (before the `if __name__` block):

```python
def read_deleted_products():
    batch_size = 1000
    start = 0
    all_rows = []

    while True:
        end = start + batch_size - 1
        data = (
            supabase.table("woodtableguy_products")
            .select(
                """
                id,
                product_image_urls,
                product_cover_image
                """
            )
            .eq("is_deleted", True)
            .eq("is_active", False)
            .range(start, end)
            .execute()
        )
        if not data.data:
            break
        all_rows.extend(data.data)
        print(f"Fetched deleted rows {start} to {end} -> {len(data.data)} rows")
        start += batch_size

    return {row["id"]: row for row in all_rows}
```

- [ ] **Step 5: Run the unit tests to verify they pass**

Run: `cd woodtableguy/wtg && python -m pytest wtg/scripts/test_purge_deleted_r2.py -v`
Expected: PASS (4 passed).

- [ ] **Step 6: Commit**

```bash
git add woodtableguy/wtg/wtg/scripts/read_db.py \
        woodtableguy/wtg/wtg/scripts/purge_deleted_r2.py \
        woodtableguy/wtg/wtg/scripts/test_purge_deleted_r2.py
git commit -m "feat(wtg): add purge_deleted_r2 script and read_deleted_products"
```

---

### Task 5: Wire the purge step into both cron workflows

**Files:**
- Modify: `.github/workflows/cron-jobs.yml` (fashionbroda)
- Modify: `.github/workflows/cron-job2.yml` (woodtableguy)

**Interfaces:**
- Consumes: `purge_deleted_r2.py` scripts from Tasks 3 & 4; the Worker tokens already present in each job's `env`.

- [ ] **Step 1: Add the fashionbroda purge step**

In `.github/workflows/cron-jobs.yml`, after the `Insert new albums into DB` step and before the `Pipeline failed` step, add:

```yaml
      # ── Part 3 — Reclaim R2 storage for deleted products ────────────────────

      - name: Purge deleted albums' images from R2
        run: python3 fashionbroda/fashionbroda_cj/fashionbroda_cj/scripts/purge_deleted_r2.py
```

(`WORKER_AUTH_TOKEN` is already in the job `env` block — no change needed there.)

- [ ] **Step 2: Add the woodtableguy purge step**

In `.github/workflows/cron-job2.yml`, after the `Upload new albums to Supabase` step and before the `Pipeline failed` step, add:

```yaml
      - name: Purge deleted albums' images from R2
        run: python3 woodtableguy/wtg/wtg/scripts/purge_deleted_r2.py
```

(`WORKER2_AUTH_TOKEN` is already in the job `env` block.)

- [ ] **Step 3: Validate the YAML parses**

Run: `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/cron-jobs.yml')); yaml.safe_load(open('.github/workflows/cron-job2.yml')); print('ok')"`
Expected: `ok`

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/cron-jobs.yml .github/workflows/cron-job2.yml
git commit -m "ci: run purge_deleted_r2 as final step in both cron jobs"
```

---

## Manual Rollout (after all tasks land)

Only one manual step is required — the cron cannot deploy Workers for itself:

1. Deploy both Workers: `cd workers/fbd && npm run deploy`, then `cd workers/wtg && npm run deploy`.

No separate one-time backlog run is needed: the purge step processes **all** `is_deleted=True AND is_active=False` products on every run, so the existing backlog is cleaned automatically the first time the cron job runs after deploy.

Optional safety check before the first cron run: dry-run each purge locally (needs `.env` with `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, and the Worker token):
- `python3 fashionbroda/fashionbroda_cj/fashionbroda_cj/scripts/purge_deleted_r2.py --dry-run --limit 5`
- `python3 woodtableguy/wtg/wtg/scripts/purge_deleted_r2.py --dry-run --limit 5`

## Self-Review Notes

- Spec coverage: Worker endpoint (T1/T2), no-column idempotency (T3/T4 via HEAD), backlog cleared by full `is_deleted=True` query (T3/T4 + rollout), end-of-run step (T5), `cleanup-delete-all` removed (T1/T2), HEAD-only (T3/T4), dry-run (T3/T4). All covered.
- The purge script reads env inside `main()` only, so the unit tests importing pure functions need no secrets.
