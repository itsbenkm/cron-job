#!/usr/bin/env python3
"""
Purge R2 images for products marked is_deleted=True (woodtableguy).

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

from wtg.scripts.paths import LOGS_DIR

WORKER_BASE_URL = "https://wtg.imageuploads.workers.dev"
CDN_BASE_URL = "https://wtg888.reps.cheap"
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
    token = os.getenv("WORKER2_AUTH_TOKEN")
    if not token:
        raise EnvironmentError("WORKER2_AUTH_TOKEN is not set.")
    headers = {"X-Auth-Token": token}

    Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(Path(LOGS_DIR) / "purge_deleted_r2.log")],
    )

    from wtg.scripts.read_db import read_deleted_products

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
