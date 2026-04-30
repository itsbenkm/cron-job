#!/usr/bin/env python3
"""
WTG DB Sync
-----------
- Reads album_data_updated_cdn.json (CDN URLs, latest product data)
- Reads current DB state via read_clean_db()
- Compares each product field by field across all columns
- Updates woodtableguy_products and woodtableguy_product_data tables where changes are found
- Logs exactly what changed per product to a timestamped log file
-----------
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from wtg.scripts.read_db import read_clean_db, supabase

# ── Paths ─────────────────────────────────────────────────────────────────────

DATA_DIR = Path(__file__).resolve().parent.parent / "data2"
LOGS_DIR = Path(__file__).resolve().parent.parent / "logs"
TRANSFORMED_JSON = DATA_DIR / "album_data_updated_cdn.json"

# ── Logging ───────────────────────────────────────────────────────────────────

LOGS_DIR.mkdir(parents=True, exist_ok=True)

log_filename = datetime.now().strftime("db_sync_%Y-%m-%d_%H-%M-%S.log")
log_filepath = LOGS_DIR / log_filename

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_filepath),
    ],
)
log = logging.getLogger(__name__)

# ── DB columns to check ───────────────────────────────────────────────────────

# Top-level columns in woodtableguy_products to compare
# Maps JSON key -> DB column name where they differ
PRODUCT_FIELDS = {
    "brands": "brands",  # JSON: brand -> DB: brands
    "slug": "slug",
    "yupoo_album_url": "yupoo_album_url",
    "product_cover_image": "product_cover_image",
    "product_image_urls": "product_image_urls",
}

# Fields inside woodtableguy_product_data to compare
# All names match between JSON product_data and DB
PRODUCT_DATA_FIELDS = [
    "price",
    "product_title",
    "sizes",
]

# ── Helpers ───────────────────────────────────────────────────────────────────


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def update_product(product_id: str, changes: dict) -> bool:
    try:
        supabase.table("woodtableguy_products").update(
            {**changes, "updated_at": utc_now()}
        ).eq("id", product_id).execute()
        return True
    except Exception as e:
        log.error(f"  ✗ Failed to update woodtableguy_products [{product_id}]: {e}")
        return False


def update_product_data(product_id: str, changes: dict) -> bool:
    try:
        supabase.table("woodtableguy_product_data").update(
            {**changes, "updated_at": utc_now()}
        ).eq("product_id", product_id).execute()
        return True
    except Exception as e:
        log.error(f"  ✗ Failed to update woodtableguy_product_data [{product_id}]: {e}")
        return False


def normalize_json_product(raw: dict) -> dict:
    """
    Normalizes a product entry from album_data_updated_cdn.json into a flat
    structure that maps directly to DB column names.
    """
    raw_product_data = raw.get("product_data") or {}

    return {
        # JSON "brand" → DB "brands"
        "brands": raw.get("brand"),
        "slug": raw.get("slug"),
        "yupoo_album_url": raw.get("yupoo_album_url"),
        "product_cover_image": raw.get("product_cover_image"),
        "product_image_urls": raw.get("product_image_urls"),
        "product_data": {
            "price": raw_product_data.get("price"),
            "product_title": raw_product_data.get("product_title"),
            "sizes": raw_product_data.get("sizes"),
        },
    }


def compare_values(json_val, db_val) -> bool:
    """Returns True if values differ and an update is needed."""
    if not json_val and not db_val:
        return False
    return json_val != db_val


# ── Core sync logic ───────────────────────────────────────────────────────────


def sync_product(product_id: str, json_product: dict, db_product: dict) -> dict:
    """
    Compares a single product from JSON against its DB row across all columns.
    Applies updates to Supabase where changes are found.
    Returns a summary dict of what changed.
    """

    slug = db_product.get("slug", product_id)
    changes = {}
    pd_changes = {}

    normalized = normalize_json_product(json_product)
    raw_pd = db_product.get("woodtableguy_product_data") or {}
    db_pd = raw_pd[0] if isinstance(raw_pd, list) else raw_pd

    # ── Check all top-level product fields ────────────────────────
    for json_key, db_col in PRODUCT_FIELDS.items():
        json_val = normalized.get(json_key)
        db_val = db_product.get(db_col)

        if compare_values(json_val, db_val):
            changes[db_col] = {"from": db_val, "to": json_val}
            if isinstance(json_val, list) and isinstance(db_val, list):
                log.info(
                    f"  [{slug}] {db_col} changed: {len(db_val)} images → {len(json_val)} images"
                )
            else:
                log.info(f"  [{slug}] {db_col} changed: {db_val!r} → {json_val!r}")

    # ── Check all product_data fields ─────────────────────────────
    json_pd = normalized.get("product_data") or {}

    for field in PRODUCT_DATA_FIELDS:
        json_val = json_pd.get(field)
        db_val = db_pd.get(field)

        if compare_values(json_val, db_val):
            pd_changes[field] = {"from": db_val, "to": json_val}
            log.info(
                f"  [{slug}] product_data.{field} changed: {db_val!r} → {json_val!r}"
            )

    # ── Apply top-level updates ───────────────────────────────────
    if changes:
        flat_changes = {col: meta["to"] for col, meta in changes.items()}

        # If slug changed but new slug already exists in DB — skip slug update
        # to avoid violating the unique constraint
        if "slug" in flat_changes and flat_changes["slug"] != db_product.get("slug"):
            existing = (
                supabase.table("woodtableguy_products")
                .select("id")
                .eq("slug", flat_changes["slug"])
                .execute()
            )
            if existing.data:
                log.warning(
                    f"  ⚠ [{slug}] slug '{flat_changes['slug']}' already exists in DB — skipping slug update"
                )
                flat_changes.pop("slug")

        # Only update if there are still changes after potentially dropping slug
        if flat_changes:
            ok = update_product(product_id, flat_changes)
            if ok:
                log.info(f"  ✓ woodtableguy_products updated [{slug}]")

    # ── Apply product_data updates ────────────────────────────────
    if pd_changes:
        flat_pd_changes = {field: meta["to"] for field, meta in pd_changes.items()}
        ok = update_product_data(product_id, flat_pd_changes)
        if ok:
            log.info(f"  ✓ woodtableguy_product_data updated [{slug}]")

    return {**changes, **{f"product_data.{k}": v for k, v in pd_changes.items()}}


# ── Main ──────────────────────────────────────────────────────────────────────


def main():
    log.info("=" * 60)
    log.info("WTG DB Sync started")
    log.info(f"Log file: {log_filepath}")
    log.info("=" * 60)

    log.info(f"Loading {TRANSFORMED_JSON} ...")
    with open(TRANSFORMED_JSON, "r", encoding="utf-8") as f:
        transformed: dict = json.load(f)

    log.info(f"Loaded {len(transformed)} products from JSON")

    log.info("Reading DB ...")
    db_products = read_clean_db()
    log.info(f"Loaded {len(db_products)} active products from DB")

    updated = []
    skipped = []
    not_in_db = []
    not_in_json = []

    for db_id in db_products:
        if db_id not in transformed:
            not_in_json.append(db_id)

    for product_id, json_product in transformed.items():
        if product_id not in db_products:
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

    log.info(f"\n{'=' * 60}")
    log.info("WTG DB Sync complete")
    log.info(f"  ✓ Updated:       {len(updated)} products")
    log.info(f"  - No changes:    {len(skipped)} products")
    log.info(
        f"  ? Not in DB:     {len(not_in_db)} products (new — use new album processor)"
    )
    log.info(
        f"  ? Not in JSON:   {len(not_in_json)} products (may have been removed from seller)"
    )
    log.info(f"Log saved to: {log_filepath}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
