#!/usr/bin/env python3

"""
WTG R2 Image Migrator
---------------------
- Reads album_data.json
- For each product, checks R2 via Worker if images exist and are JPEG
- Downloads missing images from Yupoo (with referer header)
- Converts all images to JPEG
- Uploads to R2 via Worker
- Outputs album_data_updated_cdn.json with wtg888.reps.cheap URLs
"""

import argparse
import json
import logging
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from pathlib import Path

import requests
from PIL import Image

# ── Configuration ─────────────────────────────────────────────────────────────

WORKER_BASE_URL = "https://wtg.imageuploads.workers.dev"
CDN_BASE_URL = "https://wtg888.reps.cheap"

YUPOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

MAX_WORKERS = 20
RETRY_LIMIT = 3
RETRY_DELAY = 2

DATA_DIR = Path(__file__).resolve().parent.parent / "data2"
INPUT_JSON = DATA_DIR / "album_data.json"
OUTPUT_JSON = DATA_DIR / "album_data_updated_cdn.json"

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "wtg_migrator.log"),
    ],
)
log = logging.getLogger(__name__)

# ── Worker helpers ────────────────────────────────────────────────────────────


def worker_head(r2_key: str) -> requests.Response | None:
    """HEAD request to check if a file exists in R2 and its content-type."""
    url = f"{WORKER_BASE_URL}/{r2_key}"
    for attempt in range(RETRY_LIMIT):
        try:
            return requests.head(url, timeout=30)
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


# ── Image helpers ─────────────────────────────────────────────────────────────


def download_image(url: str, referer: str) -> bytes | None:
    """Download an image from Yupoo with hotlink-bypass headers."""
    headers = {**YUPOO_HEADERS, "Referer": referer}
    for attempt in range(RETRY_LIMIT):
        try:
            time.sleep(0.3)
            r = requests.get(url, headers=headers, timeout=30)
            if r.status_code == 200:
                return r.content
            log.warning(f"Download {url} got {r.status_code}")
        except Exception as e:
            log.warning(f"Download {url} attempt {attempt + 1} failed: {e}")
            time.sleep(RETRY_DELAY * 2 if "Connection reset" in str(e) else RETRY_DELAY)
    return None


def to_jpeg(raw_bytes: bytes) -> bytes | None:
    """Convert any image format to JPEG bytes."""
    try:
        img = Image.open(BytesIO(raw_bytes))
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
    if resp is None or resp.status_code == 404:
        return False
    if resp.status_code == 200:
        ct = resp.headers.get("Content-Type", "")
        return "jpeg" in ct or "jpg" in ct
    return False


# ── Core per-image logic ──────────────────────────────────────────────────────


def ensure_jpeg_in_r2(r2_key: str, source_url: str, referer: str) -> bool:
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


# ── Brand slugify ─────────────────────────────────────────────────────────────


def slugify_brand(brand: str) -> str:
    brand = brand.lower()
    brand = re.sub(r"[^a-z0-9\s-]", "", brand)
    brand = re.sub(r"[\s-]+", "-", brand).strip("-")
    return brand


# ── Per-product processor ─────────────────────────────────────────────────────


def process_product(product: dict) -> dict | None:
    """Process one product — ensure all images are in R2, return transformed dict."""
    slug = product["slug"]
    brand = slugify_brand(product.get("brand", "unknown"))
    album_url = product.get("yupoo_album_url", "")

    log.info(f"\n{'=' * 60}")
    log.info(f"Processing: {slug}")

    prefix = f"products/{brand}/{slug}"

    # ── Product images ────────────────────────────────────────────
    cdn_image_urls = []
    for idx, src_url in enumerate(product.get("product_image_urls") or [], start=1):
        r2_key = f"{prefix}/images/{idx:02d}.jpg"
        cdn_url = f"{CDN_BASE_URL}/{r2_key}"
        ensure_jpeg_in_r2(r2_key, src_url, album_url)
        cdn_image_urls.append(cdn_url)

    # ── Cover image ───────────────────────────────────────────────
    cover_src = product.get("product_cover_image")
    cover_cdn_url = None
    if cover_src:
        r2_key = f"{prefix}/cover.jpg"
        cover_cdn_url = f"{CDN_BASE_URL}/{r2_key}"
        ensure_jpeg_in_r2(r2_key, cover_src, album_url)

    return {
        "id": product["id"],
        "brand": product.get("brand", ""),
        "slug": slug,
        "yupoo_album_url": album_url,
        "product_cover_image": cover_cdn_url,
        "product_image_urls": cdn_image_urls,
        "product_data": product.get("product_data", {}),
    }


# ── Main ──────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Migrate WTG Yupoo images to R2")
    parser.add_argument(
        "input_json",
        nargs="?",
        default=str(INPUT_JSON),
        help=f"Path to input JSON (default: {INPUT_JSON})",
    )
    parser.add_argument(
        "output_json",
        nargs="?",
        default=str(OUTPUT_JSON),
        help=f"Path to output JSON (default: {OUTPUT_JSON})",
    )
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    log.info(f"Loading {args.input_json} ...")
    with open(args.input_json, "r", encoding="utf-8") as f:
        raw_data: dict = json.load(f)

    products = list(raw_data.values())
    if args.limit:
        products = products[: args.limit]

    log.info(f"Found {len(products)} products to process")

    results: dict = {}
    failed: list = []

    Path(args.output_json).parent.mkdir(parents=True, exist_ok=True)

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_product, p): p["slug"] for p in products}
        for future in as_completed(futures):
            slug = futures[future]
            try:
                result = future.result()
                if result:
                    results[result["id"]] = result
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
