import argparse
import json
import os
import re
import time
from pathlib import Path

import boto3
import httpx
from botocore.config import Config
from dotenv import load_dotenv

# ── Load .env then validate required credentials ──────────────────────────────
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(ENV_PATH)


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"Missing required environment variable '{name}'. Expected in {ENV_PATH}."
        )
    return value


ACCOUNT_ID = require_env("ACCOUNT_ID")
ACCESS_KEY_ID = require_env("ACCESS_KEY_ID")
SECRET_ACCESS_KEY = require_env("SECRET_ACCESS_KEY")
BUCKET_NAME = require_env("BUCKET_NAME")
PUBLIC_BASE_URL = require_env("PUBLIC_BASE_URL")

# ── File paths ────────────────────────────────────────────────────────────────
DATA_DIR = "wtg/wtg/data"
INPUT_JSON = f"{DATA_DIR}/album_data.json"
OUTPUT_JSON = f"{DATA_DIR}/album_data_updated_cdn.json"
CHECKPOINT_FILE = f"{DATA_DIR}/checkpoint.json"

MAX_RETRIES = 3

# ── R2 client ─────────────────────────────────────────────────────────────────
s3 = boto3.client(
    "s3",
    endpoint_url=f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY,
    config=Config(signature_version="s3v4"),
    region_name="auto",
)


def load_checkpoint() -> set:
    """Load set of already completed product IDs."""
    if Path(CHECKPOINT_FILE).exists():
        with open(CHECKPOINT_FILE, "r") as f:
            return set(json.load(f))
    return set()


def save_checkpoint(completed: set):
    """Persist completed product IDs to checkpoint file."""
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(list(completed), f)


def slugify_brand(brand: str) -> str:
    """Simple brand slugify for R2 path."""
    brand = brand.lower()
    brand = re.sub(r"[^a-z0-9\s-]", "", brand)
    brand = re.sub(r"[\s-]+", "-", brand).strip("-")
    return brand


def image_exists_in_r2(r2_key: str) -> bool:
    """Check if a JPEG already exists in R2 at the given key."""
    try:
        head = s3.head_object(Bucket=BUCKET_NAME, Key=r2_key)
        content_type = head.get("ContentType", "")
        return "jpeg" in content_type or "jpg" in content_type
    except s3.exceptions.ClientError:
        return False
    except Exception:
        return False


def upload_image_to_r2(
    image_bytes: bytes, r2_key: str, content_type: str = "image/jpeg"
):
    """Upload raw bytes to R2."""
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=r2_key,
        Body=image_bytes,
        ContentType=content_type,
    )


def download_and_upload(
    client: httpx.Client,
    yupoo_url: str,
    r2_key: str,
    album_url: str,
    label: str,
) -> str | None:
    """
    Download image from Yupoo and upload to R2 with retry logic.
    Returns the R2 public URL on success, None on failure.
    """
    r2_public_url = f"{PUBLIC_BASE_URL}/{r2_key}"

    # Check if already exists in R2 — skip download if so
    if image_exists_in_r2(r2_key):
        print(f"    {label} ↩ Already in R2, skipping")
        return r2_public_url

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = client.get(
                yupoo_url,
                headers={"Referer": album_url},
                timeout=30,
            )
            response.raise_for_status()
            upload_image_to_r2(response.content, r2_key)
            print(f"    {label} ✓ {r2_key}")
            return r2_public_url

        except Exception as e:
            print(f"    {label} attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(1 * attempt)  # backoff: 1s, 2s, 3s

    print(f"    {label} ✗ Skipping after {MAX_RETRIES} failed attempts")
    return None


def process_product(product: dict, client: httpx.Client) -> tuple[dict, bool]:
    """
    Download all images for a product from Yupoo and upload to R2.
    Returns updated product dict and a boolean indicating full success.
    """
    slug = product["slug"]
    brand = slugify_brand(product.get("brand", "unknown"))
    album_url = product.get("yupoo_album_url", "")
    image_urls = product.get("product_image_urls", [])
    total = len(image_urls)
    all_succeeded = True

    # Process product images
    updated_urls = []
    for i, yupoo_url in enumerate(image_urls, start=1):
        r2_key = f"products/{brand}/{slug}/images/{i:02}.jpg"
        result = download_and_upload(
            client, yupoo_url, r2_key, album_url, label=f"[{i}/{total}]"
        )
        if result:
            updated_urls.append(result)
        else:
            all_succeeded = False

        time.sleep(0.1)

    # Process cover image
    cover_url = product.get("product_cover_image")
    updated_cover = cover_url  # default to original if fails
    if cover_url:
        cover_key = f"products/{brand}/{slug}/cover.jpg"
        result = download_and_upload(
            client, cover_url, cover_key, album_url, label="[cover]"
        )
        if result:
            updated_cover = result
        else:
            all_succeeded = False

    product["product_image_urls"] = updated_urls
    product["product_cover_image"] = updated_cover
    return product, all_succeeded


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--limit", type=int, default=None, help="Limit number of products to process"
    )
    args = parser.parse_args()

    print(f"Loading {INPUT_JSON}...")
    with open(INPUT_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    products = list(data.values()) if isinstance(data, dict) else data
    total_products = len(products)
    print(f"Loaded {total_products} products.")

    completed = load_checkpoint()
    print(f"Already completed: {len(completed)} products. Resuming...")

    if Path(OUTPUT_JSON).exists():
        with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
            output_data = json.load(f)
    else:
        output_data = {}

    with httpx.Client(follow_redirects=True) as client:
        products_to_process = products[: args.limit] if args.limit else products

        for idx, product in enumerate(products_to_process, start=1):
            product_id = product["id"]

            if product_id in completed:
                print(f"[{idx}/{total_products}] Skipping {product_id} (already done)")
                continue

            print(
                f"\n[{idx}/{total_products}] Processing {product_id} — {product.get('slug', 'no-slug')}"
            )

            updated_product, all_succeeded = process_product(product, client)

            output_data[product_id] = {
                **product,
                "product_image_urls": updated_product["product_image_urls"],
                "product_cover_image": updated_product["product_cover_image"],
            }

            # Write output JSON after every product
            with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)

            # Only mark complete if all images succeeded
            if all_succeeded:
                completed.add(product_id)
                save_checkpoint(completed)
                print(
                    f"  ✓ Product {product_id} complete. Progress: {len(completed)}/{total_products}"
                )
            else:
                print(
                    f"  ⚠ Product {product_id} had failed images — will retry on next run"
                )

    print(f"\n✅ Done. Updated JSON written to {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
