#!/usr/bin/env python3
"""
One-off recovery: re-upload images for active brand='Brands' fashionbroda products
whose R2 objects the orphan sweep wrongly deleted (before the read_active_products fix).

For each affected product it re-scrapes the Yupoo album for fresh origin image URLs
(using the same selectors as the fashionbroda spider), then reuses
r2_img_download.process_product to download, convert to JPEG, and upload them back to
R2 under the exact keys the DB already references (products/brands/{slug}/...).

Idempotent: process_product HEAD-checks each key first and skips any that already exist,
so re-running only fills what's still missing. Safe to run more than once.
"""

import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import requests
from scrapy.http import HtmlResponse

from fashionbroda_cj.scripts.read_db import supabase
from fashionbroda_cj.scripts.r2_img_download import process_product, YUPOO_HEADERS
from fashionbroda_cj.spiders.fashionbroda import get_product_image_cover

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


def fetch_album(url: str):
    for attempt in range(3):
        try:
            r = requests.get(url, headers=YUPOO_HEADERS, timeout=30)
            if r.status_code == 200:
                return r
            log.warning(f"GET {url} -> {r.status_code}")
        except Exception as e:
            log.warning(f"GET {url} attempt {attempt + 1} failed: {e}")
        time.sleep(2)
    return None


def load_brands_products() -> list[dict]:
    rows: list[dict] = []
    start = 0
    while True:
        d = (
            supabase.table("fashionbroda_products")
            .select("id,brands,slug,yupoo_album_url")
            .eq("is_active", True)
            .eq("is_deleted", False)
            .eq("brands", "Brands")
            .range(start, start + 999)
            .execute()
            .data
        )
        if not d:
            break
        rows.extend(d)
        start += 1000
    return rows


def main():
    rows = load_brands_products()
    log.info(f"Recovering images for {len(rows)} active 'Brands' products")

    ok = failed = 0
    for row in rows:
        slug = row.get("slug", row["id"])
        url = row.get("yupoo_album_url")
        if not url:
            log.warning(f"  {slug}: no yupoo_album_url — skip")
            failed += 1
            continue

        r = fetch_album(url)
        if r is None:
            log.error(f"  {slug}: album fetch failed — skip (album may be gone)")
            failed += 1
            continue

        resp = HtmlResponse(url=url, body=r.content, encoding="utf-8")
        imgs = resp.xpath('//img[contains(@class,"image__portrait")]/@data-origin-src').getall()
        scharts = resp.xpath('//img[contains(@class,"image__landscape")]/@data-origin-src').getall()
        cover = get_product_image_cover(resp)

        product = {
            "product_id": row["id"],
            "brands": row["brands"],
            "slug": slug,
            "yupoo_album_url": url,
            "product_cover_image": cover,
            "product_image_url": imgs or [],
            "size_chart_url": scharts or [],
            "product_data": {},
        }

        n = len(imgs or []) + len(scharts or []) + (1 if cover else 0)
        log.info(
            f"  {slug}: {len(imgs or [])} product imgs, {len(scharts or [])} size-charts, "
            f"cover={'yes' if cover else 'no'} — uploading up to {n}"
        )
        if n == 0:
            log.warning(f"  {slug}: album returned no images — skip")
            failed += 1
            continue

        try:
            process_product(product)
            ok += 1
        except Exception as e:
            log.error(f"  {slug}: upload failed: {e}")
            failed += 1

    log.info(f"Done. recovered={ok}  failed={failed}")


if __name__ == "__main__":
    main()
