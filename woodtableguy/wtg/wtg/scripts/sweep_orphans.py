#!/usr/bin/env python3
"""
Sweep orphaned images from the R2 bucket (woodtableguy).

Deletes by ABSENCE: any object in the bucket that no ACTIVE product references is an
orphan (e.g. images left behind when a product was re-slugged). This is the inverse of
purge_deleted_r2.py, which deletes by presence from deleted products.

    KEEP    = every R2 key referenced by an active product (via read_clean_db)
    BUCKET  = every key in the bucket (via Worker GET /list-keys)
    ORPHANS = BUCKET - KEEP   -> delete

Dry-run by default: prints counts + samples and deletes NOTHING. Pass --apply to delete.
Guardrails abort if KEEP is empty, or if >95% of the bucket would be deleted (a sign the
KEEP set was built wrong), unless --force overrides. --limit caps a cautious first run.

Local-only: run by hand from a machine with the project .env. Not wired into cron.
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
from wtg.scripts.purge_deleted_r2 import collect_keys, worker_delete_keys

WORKER_BASE_URL = "https://wtg.imageuploads.workers.dev"
CDN_BASE_URL = "https://wtg888.reps.cheap"
RETRY_LIMIT = 3
RETRY_DELAY = 2
FRACTION_ABORT = 0.95
PRODUCTS_PREFIX = "products/"

log = logging.getLogger(__name__)


# ── Pure diff (unit tested, no side effects) ─────────────────────────────────

def compute_orphans(bucket_keys: list[str], keep: set[str]) -> list[str]:
    """Return the bucket keys that are not referenced by any active product."""
    return [k for k in bucket_keys if k not in keep]


def partition_deletable(orphans: list[str], prefix: str = PRODUCTS_PREFIX) -> tuple[list[str], list[str]]:
    """Split orphans into (deletable under prefix, protected outside prefix).

    Only keys under `products/` are ever deletable — this shields root/non-product
    assets (e.g. icon.png) from the sweep even if no product references them.
    """
    deletable = [k for k in orphans if k.startswith(prefix)]
    protected = [k for k in orphans if not k.startswith(prefix)]
    return deletable, protected


# ── Worker I/O ───────────────────────────────────────────────────────────────

def list_bucket_keys(headers: dict) -> list[str]:
    """Page through the Worker's GET /list-keys until the cursor is exhausted."""
    keys: list[str] = []
    cursor = None
    page = 0
    while True:
        params = {"cursor": cursor} if cursor else {}
        resp = None
        for attempt in range(RETRY_LIMIT):
            try:
                r = requests.get(
                    f"{WORKER_BASE_URL}/list-keys", headers=headers, params=params, timeout=60
                )
                if r.status_code == 200:
                    resp = r.json()
                    break
                log.warning(f"list-keys page {page} got {r.status_code}: {r.text[:200]}")
                time.sleep(RETRY_DELAY)
            except Exception as e:
                log.warning(f"list-keys page {page} attempt {attempt + 1} failed: {e}")
                time.sleep(RETRY_DELAY)
        if resp is None:
            raise RuntimeError(
                f"Failed to list bucket at page {page} after {RETRY_LIMIT} attempts — aborting."
            )
        keys.extend(resp.get("keys") or [])
        page += 1
        if page % 20 == 0:
            log.info(f"  listed {len(keys)} keys so far ...")
        if not resp.get("truncated"):
            break
        cursor = resp.get("cursor")
    return keys


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Delete orphaned R2 objects not referenced by any active product"
    )
    parser.add_argument(
        "--apply", action="store_true", help="Actually delete (default: dry-run, deletes nothing)"
    )
    parser.add_argument(
        "--force", action="store_true", help="Override the >95%%-of-bucket safety abort"
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Delete at most N orphans (cautious first run)"
    )
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
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(Path(LOGS_DIR) / "sweep_orphans.log"),
        ],
    )

    from wtg.scripts.read_db import read_active_products

    # ── KEEP: keys referenced by active products (NO brand filter) ──
    log.info("Building KEEP set from active products ...")
    keep: set[str] = set()
    for row in read_active_products().values():
        keep.update(collect_keys(row, CDN_BASE_URL))
    log.info(f"KEEP: {len(keep)} keys referenced by active products")

    # ── BUCKET: every key in the bucket ──
    log.info("Listing bucket keys via Worker GET /list-keys ...")
    bucket = list_bucket_keys(headers)
    log.info(f"BUCKET: {len(bucket)} objects in bucket")

    # ── Diff ──
    all_orphans = compute_orphans(bucket, keep)
    orphans, protected = partition_deletable(all_orphans)
    matched = len(bucket) - len(all_orphans)
    frac = (len(orphans) / len(bucket)) if bucket else 0.0
    log.info(
        f"KEEP={len(keep)}  BUCKET={len(bucket)}  KEPT(matched)={matched}  "
        f"ORPHANS={len(all_orphans)}  DELETABLE(under {PRODUCTS_PREFIX})={len(orphans)}  "
        f"PROTECTED={len(protected)}  ({frac:.1%} of bucket)"
    )
    if protected:
        log.info(f"Protected {len(protected)} non-product orphan(s) — will NOT be deleted:")
        for k in protected[:10]:
            log.info(f"  protected: {k}")

    # ── Samples for eyeballing ──
    log.info("Sample ORPHAN keys (up to 10):")
    for k in orphans[:10]:
        log.info(f"  orphan: {k}")
    kept_sample = [k for k in bucket if k in keep][:10]
    log.info("Sample KEPT keys (up to 10):")
    for k in kept_sample:
        log.info(f"  keep:   {k}")

    # ── Guardrails ──
    if len(keep) == 0:
        log.error(
            "KEEP set is empty — refusing to delete anything "
            "(read_clean_db returned no keys)."
        )
        return
    if not orphans:
        log.info("No orphans found — bucket is clean. Nothing to do.")
        return

    if not args.apply:
        log.info(
            "DRY RUN — no deletions performed. Re-run with --apply to delete the orphans above."
        )
        return

    if frac > FRACTION_ABORT and not args.force:
        log.error(
            f"Refusing to delete {frac:.1%} of the bucket (> {FRACTION_ABORT:.0%}) — this usually "
            f"means the KEEP set was built wrong. Re-check, or pass --force to override."
        )
        return

    to_delete = orphans[: args.limit] if args.limit else orphans
    log.info(f"Deleting {len(to_delete)} orphaned objects ...")
    deleted = worker_delete_keys(to_delete, headers)
    log.info(f"Deleted {deleted} objects from R2")
    if deleted != len(to_delete):
        log.warning(
            f"Expected to delete {len(to_delete)} but Worker reported {deleted} — some deletes failed."
        )


if __name__ == "__main__":
    main()
