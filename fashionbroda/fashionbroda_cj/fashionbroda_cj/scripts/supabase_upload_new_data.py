#!/usr/bin/env python3
"""
Supabase Upload New Data
-------------------------
Final step in the cron job pipeline — inserts newly discovered albums
that were not previously in the DB into fashionbroda_products and product_data.

Pipeline position:
  discover spider → new_album_processor.py → new_album_data_processed.json → THIS SCRIPT → DB

Key features:
- Reads new_album_data_processed.json (CDN URLs, processed product data)
- Inserts into fashionbroda_products then product_data (linked by product_id)
- Upsert on slug — unique constraint at DB level prevents duplicates on re-run
- Only extracts core DB columns — extra scraper fields like weidian, weight etc. are excluded
- Logs successes and failures to a timestamped log file in logs/
- Handles exceptions per product so one failure never blocks the rest
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from supabase import Client, create_client

# ── Path fix — allows running from scripts/ or anywhere ───────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# ── Config ─────────────────────────────────────────────────────────────────────

SELLER_ID = "68315cdb-5674-4305-b20f-99ab05c5c526"

DATA_DIR = "fashionbroda/fashionbroda_cj/fashionbroda_cj/data"
LOGS_DIR = "fashionbroda/fashionbroda_cj/fashionbroda_cj/logs"
INPUT_JSON = f"{DATA_DIR}/new_album_data_processed.json"

# ── Supabase client ────────────────────────────────────────────────────────────

load_dotenv()
supabase: Client = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_SERVICE_ROLE_KEY"),
)

# ── Logging ───────────────────────────────────────────────────────────────────

Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)

log_filename = datetime.now().strftime("supabase_upload_%Y-%m-%d_%H-%M-%S.log")
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


# ── Upload ─────────────────────────────────────────────────────────────────────


def upload_to_supabase():
    log.info("=" * 60)
    log.info("Supabase Upload New Data started")
    log.info(f"Log file: {log_filepath}")
    log.info("=" * 60)

    # ── Load JSON ──────────────────────────────────────────────────
    log.info(f"Reading {INPUT_JSON} ...")
    try:
        with open(INPUT_JSON, "r", encoding="utf-8") as f:
            all_products = json.load(f)
    except FileNotFoundError:
        log.error(f"JSON file not found: {INPUT_JSON}")
        raise SystemExit("JSON file not found")
    except json.JSONDecodeError as e:
        log.error(f"Invalid JSON format: {e}")
        raise SystemExit("Invalid JSON file")
    except Exception as e:
        log.error(f"Unexpected error reading JSON file: {e}")
        raise SystemExit("Error reading JSON file")

    # Handle both list and dict shaped JSON
    products = (
        all_products if isinstance(all_products, list) else list(all_products.values())
    )

    if not products:
        log.info("No new products to insert — exiting.")
        return

    log.info(f"Found {len(products)} new products to insert\n")

    # ── Counters ───────────────────────────────────────────────────
    fashionbroda_success = 0
    fashionbroda_failed = 0
    product_data_success = 0
    product_data_failed = 0

    # ── Loop and insert ────────────────────────────────────────────
    for product in products:
        slug = product.get("slug", "unknown")

        try:
            # ── Step 1: Insert into fashionbroda_products ──────────
            # Map JSON keys to exact DB column names:
            #   product_image_url  (JSON) → product_image_urls  (DB)
            #   size_chart_url     (JSON) → size_chart_image_urls (DB)
            fashionbroda_row = {
                "seller_id": SELLER_ID,
                "brands": product.get("brands"),
                "slug": slug,
                "is_active": True,
                "is_deleted": False,
                "yupoo_album_url": product.get("yupoo_album_url"),
                "product_cover_image": product.get("product_cover_image"),
                "product_image_urls": product.get("product_image_url") or [],
                "size_chart_image_urls": product.get("size_chart_url") or [],
            }

            # Upsert on slug — DB unique constraint prevents duplicates on re-run
            fashionbroda_response = (
                supabase.table("fashionbroda_products")
                .upsert(fashionbroda_row, on_conflict="slug")
                .execute()
            )

            if not fashionbroda_response.data:
                log.error(f"  ✗ Failed to insert fashionbroda_products: {slug}")
                fashionbroda_failed += 1
                continue

            log.info(f"  ✓ Inserted fashionbroda_products: {slug}")
            fashionbroda_success += 1

            # Get the autogenerated product id from the insert response
            # to link the product_data row back to it
            product_id = fashionbroda_response.data[0].get("id")
            log.info(f"  ↳ product_id: {product_id}")

            # ── Step 2: Insert into product_data ──────────────────
            # Only extract core DB columns — weidian, weight, craft,
            # construction, note etc. are intentionally excluded
            raw_pd = product.get("product_data") or {}
            product_data_row = {
                "product_id": product_id,
                "price": raw_pd.get("price"),
                "style_code": raw_pd.get("style_code"),
                "fabric": raw_pd.get("fabric"),
                "fit": raw_pd.get("fit"),
                "sizes": raw_pd.get("sizes"),
                "features": raw_pd.get("features"),
            }

            product_data_response = (
                supabase.table("product_data")
                .upsert(product_data_row, on_conflict="product_id")
                .execute()
            )

            if not product_data_response.data:
                log.error(f"  ✗ Failed to insert product_data: {slug}")
                product_data_failed += 1
                continue

            log.info(f"  ✓ Inserted product_data: {slug}")
            product_data_success += 1

        except Exception as e:
            log.error(f"  ✗ Exception for {slug}: {e}")
            fashionbroda_failed += 1
            continue

    # ── Summary ────────────────────────────────────────────────────
    log.info(f"\n{'=' * 60}")
    log.info("Upload complete")
    log.info(f"  ✓ fashionbroda_products inserted: {fashionbroda_success}")
    log.info(f"  ✓ product_data inserted:          {product_data_success}")
    log.info(f"  ✗ fashionbroda_products failed:   {fashionbroda_failed}")
    log.info(f"  ✗ product_data failed:            {product_data_failed}")
    log.info(f"Log saved to: {log_filepath}")
    log.info("=" * 60)


if __name__ == "__main__":
    upload_to_supabase()
