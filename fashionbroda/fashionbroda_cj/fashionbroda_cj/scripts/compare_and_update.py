#!/usr/bin/env python3
"""
DB Sync
-------
- Reads transformed_products.json (CDN URLs, latest product data)
- Reads current DB state via read_clean_db()
- Compares each product field by field
- Updates fashionbroda_products and product_data tables where changes are found
- Logs exactly what changed per product to a timestamped log file
-------
This is the 5th script to run.
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

# Allow running this script directly from the scripts/ directory
# by adding the project root (fashionbroda_cj/) to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from fashionbroda_cj.scripts.read_db import read_clean_db, supabase

# ── Paths ──────────────────────────────────────────────────────────────────────

DATA_DIR = "fashionbroda_cj/fashionbroda_cj/data"
LOGS_DIR = "fashionbroda_cj/fashionbroda_cj/logs"
TRANSFORMED_JSON = f"{DATA_DIR}/transformed_products.json"

# ── Logging ───────────────────────────────────────────────────────────────────

Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)

# Timestamped log file per run so history is never overwritten
log_filename = datetime.now().strftime("db_sync_%Y-%m-%d_%H-%M-%S.log")
log_filepath = f"{LOGS_DIR}/{log_filename}"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_filepath),
    ],
)
log = logging.getLogger(__name__)

# ── DB columns we care about ───────────────────────────────────────────────────

# Top-level fields in fashionbroda_products to check for changes
PRODUCT_FIELDS = [
    "product_cover_image",
    "product_image_urls",
    "size_chart_image_urls",
]

# Fields inside product_data table to check for changes
PRODUCT_DATA_FIELDS = [
    "price",
    "style_code",
    "fabric",
    "fit",
    "sizes",
    "features",
]

# ── Helpers ────────────────────────────────────────────────────────────────────


def utc_now() -> str:
    """Returns current UTC time as ISO 8601 string for updated_at columns."""
    return datetime.now(timezone.utc).isoformat()


def update_product(product_id: str, changes: dict) -> bool:
    """
    Updates fashionbroda_products row with the given changes.
    Always stamps updated_at with current UTC time.
    """
    try:
        supabase.table("fashionbroda_products").update(
            {**changes, "updated_at": utc_now()}
        ).eq("id", product_id).execute()
        return True
    except Exception as e:
        log.error(f"  ✗ Failed to update fashionbroda_products [{product_id}]: {e}")
        return False


def update_product_data(product_id: str, changes: dict) -> bool:
    """
    Updates product_data row linked to the given product_id.
    Always stamps updated_at with current UTC time.
    """
    try:
        supabase.table("product_data").update({**changes, "updated_at": utc_now()}).eq(
            "product_id", product_id
        ).execute()
        return True
    except Exception as e:
        log.error(f"  ✗ Failed to update product_data [{product_id}]: {e}")
        return False


def normalize_json_product(raw: dict) -> dict:
    """
    Normalizes a product entry from transformed_products.json into a flat
    structure that maps directly to DB column names.

    Key mappings:
      product_image_url  → product_image_urls  (JSON key → DB column)
      size_chart_url     → size_chart_image_urls      (JSON key → DB column)

    Only extracts fields that exist as DB columns — extra scraper fields
    like craft, construction, note, care_recommendation, weidian etc. are
    intentionally excluded since they are not part of the DB schema.
    """
    raw_product_data = raw.get("product_data") or {}

    return {
        # Top-level product fields
        "product_cover_image": raw.get("product_cover_image"),
        "product_image_urls": raw.get("product_image_url"),  # JSON → DB name
        "size_chart_image_urls": raw.get("size_chart_url"),  # JSON → DB name
        # product_data fields — only the core DB columns, nothing extra
        "product_data": {
            "price": raw_product_data.get("price"),
            "style_code": raw_product_data.get("style_code"),
            "fabric": raw_product_data.get("fabric"),
            "fit": raw_product_data.get("fit"),
            "sizes": raw_product_data.get("sizes"),
            "features": raw_product_data.get("features"),
        },
    }


def compare_values(field: str, json_val, db_val) -> bool:
    """
    Returns True if the two values are different and an update is needed.
    Handles list comparison for array fields like product_image_urls and sizes.
    """
    # Treat None and empty list as equivalent to avoid false positives
    if not json_val and not db_val:
        return False
    return json_val != db_val


# ── Core sync logic ────────────────────────────────────────────────────────────


def sync_product(product_id: str, json_product: dict, db_product: dict) -> dict:
    """
    Compares a single product from the JSON against its DB row.
    Detects changes in both top-level fields and product_data fields.
    Applies updates directly to Supabase where changes are found.
    Returns a summary dict of what changed.
    """
    slug = db_product.get("slug", product_id)
    changes = {}  # top-level product changes
    pd_changes = {}  # product_data changes

    normalized = normalize_json_product(json_product)
    db_pd = db_product.get("product_data") or {}

    # ── Check top-level product fields ────────────────────────────
    for field in PRODUCT_FIELDS:
        json_val = normalized.get(field)
        db_val = db_product.get(field)

        if compare_values(field, json_val, db_val):
            changes[field] = {"from": db_val, "to": json_val}
            log.info(f"  [{slug}] {field} changed")

            # Log detailed diff for image arrays
            if isinstance(json_val, list) and isinstance(db_val, list):
                log.info(f"    was: {len(db_val)} images → now: {len(json_val)} images")
            elif field == "product_cover_image":
                # product_cover_image column was recently added so DB value
                # will be null for all existing products — this is expected
                log.info(f"    was: {db_val} → now: {json_val}")

    # ── Check product_data fields ──────────────────────────────────
    json_pd = normalized.get("product_data") or {}

    for field in PRODUCT_DATA_FIELDS:
        json_val = json_pd.get(field)
        db_val = db_pd.get(field)

        if compare_values(field, json_val, db_val):
            pd_changes[field] = {"from": db_val, "to": json_val}
            log.info(
                f"  [{slug}] product_data.{field} changed: {db_val!r} → {json_val!r}"
            )

    # ── Apply top-level updates ────────────────────────────────────
    if changes:
        flat_changes = {field: meta["to"] for field, meta in changes.items()}
        ok = update_product(product_id, flat_changes)
        if ok:
            log.info(f"  ✓ fashionbroda_products updated [{slug}]")

    # ── Apply product_data updates ─────────────────────────────────
    if pd_changes:
        flat_pd_changes = {field: meta["to"] for field, meta in pd_changes.items()}
        ok = update_product_data(product_id, flat_pd_changes)
        if ok:
            log.info(f"  ✓ product_data updated [{slug}]")

    return {**changes, **{f"product_data.{k}": v for k, v in pd_changes.items()}}


# ── Main ───────────────────────────────────────────────────────────────────────


def main():
    log.info("=" * 60)
    log.info("DB Sync started")
    log.info(f"Log file: {log_filepath}")
    log.info("=" * 60)

    # Load transformed_products.json
    log.info(f"Loading {TRANSFORMED_JSON} ...")
    with open(TRANSFORMED_JSON, "r", encoding="utf-8") as f:
        transformed: dict = json.load(f)

    log.info(f"Loaded {len(transformed)} products from JSON")

    # Load current DB state
    log.info("Reading DB ...")
    db_products = read_clean_db()
    log.info(f"Loaded {len(db_products)} active products from DB")

    # Track results
    updated = []  # products that had at least one change
    skipped = []  # products with no changes
    not_in_db = []  # products in JSON but not in DB (new — not handled here)
    not_in_json = []  # products in DB but not in JSON

    # Products in DB but missing from JSON
    for db_id in db_products:
        if db_id not in transformed:
            not_in_json.append(db_id)

    # Compare every product in the JSON against the DB
    for product_id, json_product in transformed.items():
        if product_id not in db_products:
            # This product is in the JSON but not in the DB yet
            # New products are handled by the new album processor — skip here
            not_in_db.append(product_id)
            continue

        log.info(f"\nChecking: {json_product.get('slug', product_id)}")
        db_product = db_products[product_id]

        changes = sync_product(product_id, json_product, db_product)

        if changes:
            updated.append(product_id)
        else:
            skipped.append(product_id)
            log.info("  ✓ No changes")

    # ── Final summary ──────────────────────────────────────────────
    log.info(f"\n{'=' * 60}")
    log.info("DB Sync complete")
    log.info(f"  ✓ Updated:       {len(updated)} products")
    log.info(f"  - No changes:    {len(skipped)} products")
    log.info(
        f"  ? Not in DB:     {len(not_in_db)} products (new — use new_album_processor)"
    )
    log.info(
        f"  ? Not in JSON:   {len(not_in_json)} products (may have been removed from seller)"
    )
    log.info(f"Log saved to: {log_filepath}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
