#!/usr/bin/env python3


"""
R2 Image Migrator
-----------------
- Reads products JSON
- For each product, checks R2 via Worker if images exist and are JPEG
- Downloads missing/WebP images from Yupoo (with referer header)
- Converts all images to JPEG
- Uploads to R2 via Worker
- Outputs transformed JSON with cdn.reps.cheap
-----------------

This is the 4th script to run, it takes the data from slug.json, which is the output of fashionbroda.py, and migrates the images to R2,
and outputs a new JSON file with the same shape as slug.json but with the image URLs replaced with the CDN URLs,
so that we can diff the output with the existing DB data to see what has changed and needs to be updated in the DB.

"""

import argparse
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from pathlib import Path

import requests
from PIL import Image

# ── Configuration ─────────────────────────────────────────────────────────────

WORKER_BASE_URL = "https://fbd.imageuploads.workers.dev"
CDN_BASE_URL = "https://cdn.reps.cheap"

YUPOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

MAX_WORKERS = 4  # parallel product workers
RETRY_LIMIT = 3
RETRY_DELAY = 2  # seconds between retries

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "migrator.log"),
    ],
)
log = logging.getLogger(__name__)

# ── Worker helpers ─────────────────────────────────────────────────────────────


def worker_head(r2_key: str) -> requests.Response | None:
    """HEAD request to check if a file exists in R2 and its content-type."""
    url = f"{WORKER_BASE_URL}/{r2_key}"
    for attempt in range(RETRY_LIMIT):
        try:
            r = requests.head(url, timeout=30)
            return r
        except Exception as e:
            log.warning(f"HEAD {r2_key} attempt {attempt + 1} failed: {e}")
            time.sleep(RETRY_DELAY)
    return None


def worker_put(r2_key: str, image_bytes: bytes) -> bool:
    """PUT a JPEG image to R2 via the worker."""
    url = f"{WORKER_BASE_URL}/{r2_key}"
    for attempt in range(RETRY_LIMIT):
        try:
            r = requests.put(
                url,
                data=image_bytes,
                headers={"Content-Type": "image/jpeg"},
                timeout=60,
            )
            if r.status_code in (200, 201, 204):
                return True
            log.warning(f"PUT {r2_key} got {r.status_code}: {r.text[:200]}")
        except Exception as e:
            log.warning(f"PUT {r2_key} attempt {attempt + 1} failed: {e}")
            time.sleep(RETRY_DELAY)
    return False


# ── Image helpers ──────────────────────────────────────────────────────────────


def download_image(url: str, referer: str) -> bytes | None:
    """Download an image from Yupoo with hotlink-bypass headers."""
    headers = {**YUPOO_HEADERS, "Referer": referer}
    for attempt in range(RETRY_LIMIT):
        try:
            # Small delay to avoid hammering Yupoo and triggering rate limits
            time.sleep(0.3)
            r = requests.get(url, headers=headers, timeout=30)
            if r.status_code == 200:
                return r.content
            log.warning(f"Download {url} got {r.status_code}")
        except Exception as e:
            log.warning(f"Download {url} attempt {attempt + 1} failed: {e}")
            # Longer wait on connection reset before retrying
            time.sleep(RETRY_DELAY * 2 if "Connection reset" in str(e) else RETRY_DELAY)
    return None


def to_jpeg(raw_bytes: bytes) -> bytes | None:
    """Convert any image format to JPEG bytes."""
    try:
        img = Image.open(BytesIO(raw_bytes))
        # Convert palette images with transparency to RGBA first
        # to avoid Pillow warning and preserve correct colors
        if img.mode == "P":
            img = img.convert("RGBA")
        if img.mode in ("RGBA", "LA"):
            img = img.convert("RGB")
        elif img.mode != "RGB":
            img = img.convert("RGB")
        out = BytesIO()
        img.save(out, format="JPEG", quality=95, optimize=True)
        return out.getvalue()
    except Exception as e:
        log.error(f"JPEG conversion failed: {e}")
        return None


def is_jpeg_in_r2(r2_key: str) -> bool:
    """Returns True if the file exists in R2 AND is already a JPEG."""
    resp = worker_head(r2_key)
    if resp is None:
        return False
    if resp.status_code == 404:
        return False
    if resp.status_code == 200:
        ct = resp.headers.get("Content-Type", "")
        return "jpeg" in ct or "jpg" in ct
    return False


# ── Core per-image logic ───────────────────────────────────────────────────────


def ensure_jpeg_in_r2(
    r2_key: str,
    source_url: str,
    referer: str,
) -> bool:
    """
    Check R2:
      - If file exists and is JPEG → skip
      - Otherwise → download, convert, upload
    Returns True on success.
    """
    if is_jpeg_in_r2(r2_key):
        log.info(f"  ✓ Already JPEG: {r2_key}")
        return True

    log.info(f"  ↓ Downloading: {source_url}")
    raw = download_image(source_url, referer)
    if raw is None:
        log.error(f"  ✗ Failed to download: {source_url}")
        return False

    jpeg = to_jpeg(raw)
    if jpeg is None:
        log.error(f"  ✗ Failed to convert: {source_url}")
        return False

    log.info(f"  ↑ Uploading: {r2_key}")
    ok = worker_put(r2_key, jpeg)
    if ok:
        log.info(f"  ✓ Uploaded: {r2_key}")
    else:
        log.error(f"  ✗ Upload failed: {r2_key}")
    return ok


# ── Per-product processor ──────────────────────────────────────────────────────


def process_product(product: dict) -> dict | None:
    """
    Process one product entry. Returns the transformed output dict.
    """
    slug = product["slug"]
    brand = product["brands"].lower().replace(" ", "-")
    album_url = product["yupoo_album_url"]

    log.info(f"\n{'=' * 60}")
    log.info(f"Processing: {slug}")

    # ── Build R2 key prefix ────────────────────────────────────────
    prefix_product = f"products/{brand}/{slug}/product"
    prefix_size_chart = f"products/{brand}/{slug}/size-chart"
    prefix_cover = f"products/{brand}/{slug}/cover"

    # ── Product images ─────────────────────────────────────────────
    product_cdn_urls = []
    for idx, src_url in enumerate(product.get("product_image_url") or [], start=1):
        num = f"{idx:02d}"
        r2_key = f"{prefix_product}/{num}.jpg"
        cdn_url = f"{CDN_BASE_URL}/{r2_key}"
        ensure_jpeg_in_r2(r2_key, src_url, album_url)
        product_cdn_urls.append(cdn_url)

    # ── Size chart images ──────────────────────────────────────────
    size_chart_cdn_urls = []
    for idx, src_url in enumerate(product.get("size_chart_url") or [], start=1):
        num = f"{idx:02d}"
        r2_key = f"{prefix_size_chart}/{num}.jpg"
        cdn_url = f"{CDN_BASE_URL}/{r2_key}"
        ensure_jpeg_in_r2(r2_key, src_url, album_url)
        size_chart_cdn_urls.append(cdn_url)

    # ── Cover image ────────────────────────────────────────────────
    cover_src = product.get("product_cover_image")
    cover_cdn_url = None
    if cover_src:
        r2_key = f"{prefix_cover}/cover.jpg"
        cover_cdn_url = f"{CDN_BASE_URL}/{r2_key}"
        ensure_jpeg_in_r2(r2_key, cover_src, album_url)

    # ── Build output dict ──────────────────────────────────────────
    # Mirrors the shape of slug.json exactly, with CDN URLs replacing
    # the raw Yupoo URLs so the output can be diffed against the DB.
    output = {
        "product_id": product["product_id"],
        "brands": product["brands"],
        "slug": slug,
        "yupoo_album_url": album_url,
        "product_cover_image": cover_cdn_url,
        "product_image_url": product_cdn_urls,
        "size_chart_url": size_chart_cdn_urls,
        "product_data": product.get("product_data", {}),
    }

    return output


# ── Main ───────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Migrate Yupoo images to R2")
    parser.add_argument(
        "input_json",
        nargs="?",
        default="fashionbroda/fashionbroda_cj/fashionbroda_cj/data/slug.json",
        help="Path to input products JSON file (default: cron job data dir)",
    )
    parser.add_argument(
        "output_json",
        nargs="?",
        default="fashionbroda/fashionbroda_cj/fashionbroda_cj/data/transformed_products.json",
        help="Path to write transformed JSON output (default: cron job data dir)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=MAX_WORKERS,
        help=f"Parallel product workers (default: {MAX_WORKERS})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only process first N products (for testing)",
    )
    args = parser.parse_args()

    # Load input
    log.info(f"Loading {args.input_json} ...")
    with open(args.input_json, "r", encoding="utf-8") as f:
        raw_data: dict = json.load(f)

    products = list(raw_data.values())
    if args.limit:
        products = products[: args.limit]

    log.info(f"Found {len(products)} products to process")

    # Use a dict keyed by product_id to mirror the shape of slug.json exactly.
    results: dict = {}
    failed = []

    # Ensure output dir exists before we start writing
    Path(args.output_json).parent.mkdir(parents=True, exist_ok=True)

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_product, p): p["slug"] for p in products}
        for future in as_completed(futures):
            slug = futures[future]
            try:
                result = future.result()
                if result:
                    # Key by product_id just like slug.json
                    results[result["product_id"]] = result
                    # Write after every completed product so progress is never
                    # lost if the script is interrupted mid-run.
                    with open(args.output_json, "w", encoding="utf-8") as f:
                        json.dump(results, f, ensure_ascii=False, indent=2)
                    log.info(f"  ✎ Output updated ({len(results)} products written)")
                else:
                    failed.append(slug)
            except Exception as e:
                log.error(f"Product {slug} raised exception: {e}")
                failed.append(slug)

    log.info(f"\n{'=' * 60}")
    log.info(f"Done. ✓ {len(results)} succeeded, ✗ {len(failed)} failed")
    if failed:
        log.warning(f"Failed slugs: {failed}")


if __name__ == "__main__":
    main()
