import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# ── Path fix — allows running from scripts/ or anywhere ───────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv
from supabase import Client, create_client
from wtg.scripts.paths import LOGS_DIR as SHARED_LOGS_DIR
from wtg.scripts.paths import get_data_path

# ── Config ─────────────────────────────────────────────────────────────────────

SELLER_ID = "16bd8883-cd2f-45aa-ac85-686b66e06791"

LOGS_DIR = str(SHARED_LOGS_DIR)
INPUT_JSON = get_data_path("new_album_data_updated_cdn.json")

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


# ── Helpers ────────────────────────────────────────────────────────────────────


def insert_woodtableguy_product(row: dict) -> tuple[bool, str | None]:
    """
    Upserts a row into woodtableguy_products.
    Returns (success, product_id or None).
    """
    slug = row.get("slug", "unknown")
    try:
        result = (
            supabase.table("woodtableguy_products")
            .upsert(row, on_conflict="slug")
            .execute()
        )
        if not result.data:
            log.error(
                f"  ✗ Upsert returned no data for woodtableguy_products [{slug}] — row may not have been written"
            )
            return False, None

        product_id = result.data[0].get("id")
        if not product_id:
            log.error(f"  ✗ No product_id returned after upsert [{slug}]")
            return False, None

        log.info(f"  ✓ Inserted woodtableguy_products: {slug}")
        log.info(f"  ↳ product_id: {product_id}")

        # Warn if image URLs are empty — catches key mismatch bugs early
        if not row.get("product_image_urls"):
            log.warning(f"  ⚠ [{slug}] product_image_urls is empty — check JSON key")

        return True, product_id

    except Exception as e:
        log.error(f"  ✗ Exception inserting woodtableguy_products [{slug}]: {e}")
        return False, None


def insert_woodtableguy_product_data(row: dict, slug: str) -> bool:
    """
    Upserts a row into woodtableguy_product_data.
    Returns True if successful.
    """
    try:
        result = (
            supabase.table("woodtableguy_product_data")
            .upsert(row, on_conflict="product_id")
            .execute()
        )
        if not result.data:
            log.error(
                f"  ✗ Upsert returned no data for woodtableguy_product_data [{slug}] — row may not have been written"
            )
            return False

        log.info(f"  ✓ Inserted woodtableguy_product_data: {slug}")
        return True

    except Exception as e:
        log.error(f"  ✗ Exception inserting woodtableguy_product_data [{slug}]: {e}")
        return False


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
    woodtableguy_products_success = 0
    woodtableguy_products_failed = 0
    woodtableguy_product_data_success = 0
    woodtableguy_product_data_failed = 0

    # ── Loop and insert ────────────────────────────────────────────
    for product in products:
        slug = product.get("slug", "unknown")

        # ── Step 1: Insert into woodtableguy_products ──────────────
        woodtableguy_products_row = {
            "seller_id": SELLER_ID,
            "brands": product.get("brand"),
            "slug": slug,
            "is_active": True,
            "is_deleted": False,
            "yupoo_album_url": product.get("yupoo_album_url"),
            "product_cover_image": product.get("product_cover_image"),
            "product_image_urls": product.get("product_image_urls") or [],
        }

        ok, product_id = insert_woodtableguy_product(woodtableguy_products_row)
        if not ok:
            woodtableguy_products_failed += 1
            continue

        woodtableguy_products_success += 1

        # ── Step 2: Insert into woodtableguy_product_data ──────────
        raw_pd = product.get("product_data") or {}
        woodtableguy_product_data_row = {
            "product_id": product_id,
            "price": raw_pd.get("price"),
            "product_title": raw_pd.get("product_title"),
            "sizes": raw_pd.get("sizes"),
        }

        ok = insert_woodtableguy_product_data(woodtableguy_product_data_row, slug)
        if not ok:
            woodtableguy_product_data_failed += 1
            continue

        woodtableguy_product_data_success += 1

    # ── Summary ────────────────────────────────────────────────────
    log.info(f"\n{'=' * 60}")
    log.info("Upload complete")
    log.info(
        f"  ✓ woodtableguy_products inserted:      {woodtableguy_products_success}"
    )
    log.info(
        f"  ✓ woodtableguy_product_data inserted:  {woodtableguy_product_data_success}"
    )
    log.info(f"  ✗ woodtableguy_products failed:        {woodtableguy_products_failed}")
    log.info(
        f"  ✗ woodtableguy_product_data failed:    {woodtableguy_product_data_failed}"
    )
    log.info(f"Log saved to: {log_filepath}")
    log.info("=" * 60)


if __name__ == "__main__":
    upload_to_supabase()
