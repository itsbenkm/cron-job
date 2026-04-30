"""
This spider scrapes the albums that have been verified to exist by the validate spider, it uses the album URLs from the DB to scrape the album data,
This spider will be used to check for any changes in album data and to check for new images, this will be used to update the existing info in the DB
"""

import json
import re
import unicodedata
import uuid
from pathlib import Path
from typing import Optional

import scrapy

from wtg.scripts.read_db import read_db

# *----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def get_product_image_cover(response) -> Optional[str]:
    # 1. JSON-LD first (best)
    for script in response.css('script[type="application/ld+json"]::text').getall():
        try:
            data = json.loads(script)

            for item in data.get("@graph", [data]):
                if item.get("@type") == "ImageGallery":
                    images = item.get("image", [])

                    if isinstance(images, str) and images.strip():
                        return response.urljoin(images.strip())

                    if isinstance(images, list) and images:
                        first = images[0]

                        if isinstance(first, str):
                            return response.urljoin(first.strip())

                        if isinstance(first, dict):
                            url = first.get("url")
                            if url:
                                return response.urljoin(url.strip())

        except (json.JSONDecodeError, AttributeError, TypeError):
            continue

    # 2. og:image
    og = response.css('meta[property="og:image"]::attr(content)').get()

    if og:
        return response.urljoin(og.strip())

    # 3. DOM fallback
    src = response.css(".showalbumheader__gallerycover img::attr(src)").get()

    if src:
        return response.urljoin(src.strip())

    return None


# *----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def normalize_fullwidth(text: str) -> str:
    result = []
    for ch in text:
        cp = ord(ch)
        # Fullwidth ASCII variants FF01–FF5E → shift to ASCII
        if 0xFF01 <= cp <= 0xFF5E:
            result.append(chr(cp - 0xFEE0))
        # Fullwidth space → regular space
        elif cp == 0x3000:
            result.append(" ")
        else:
            result.append(ch)
    return "".join(result)


# *----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def fallback_slug(category: Optional[str] = None, price=None) -> str:
    parts = []
    if category:
        cat = normalize_fullwidth(category)
        cat = (
            unicodedata.normalize("NFKD", cat).encode("ascii", "ignore").decode("utf-8")
        )
        cat = cat.lower()
        cat = re.sub(r"[^a-z0-9\s-]", "", cat)
        cat = re.sub(r"[\s-]+", "-", cat).strip("-")
        if cat:
            parts.append(cat)
    if price is not None:
        price_str = str(int(price)) if price == int(price) else str(price)
        parts.append(price_str)
    if parts:
        return "-".join(parts)
    # Last resort — never returns None
    return "product-" + uuid.uuid4().hex[:8]


# *----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def generate_slug(
    text: Optional[str], category: Optional[str] = None, price=None
) -> str:
    if text:
        text = normalize_fullwidth(text)
        text = (
            unicodedata.normalize("NFKD", text)
            .encode("ascii", "ignore")
            .decode("utf-8")
        )
        slug = text.lower()
        slug = re.sub(r"[^a-z0-9\s-]", "", slug)
        slug = re.sub(r"[\s-]+", "-", slug).strip("-")
        if slug and len(slug) >= 2:
            return slug
    # Empty or too short — fall back to category + price
    return fallback_slug(category, price)


# *----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


class WoodtableguySpider(scrapy.Spider):
    name = "woodtableguy"
    allowed_domains = ["woodtableguy888.x.yupoo.com"]

    custom_settings = {
        "DOWNLOAD_DELAY": 0.15,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 10,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output_data = {}
        self.output_path = (
            Path(__file__).resolve().parent.parent / "data2" / "album_data.json"
        )

    def closed(self, reason):
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with self.output_path.open("w", encoding="utf-8") as file:
            json.dump(self.output_data, file, ensure_ascii=False, indent=2)

    async def start(self):
        self.products = read_db()
        for product in self.products.values():
            if not product.get("yupoo_album_url"):
                continue
            yield scrapy.Request(
                url=product["yupoo_album_url"],
                callback=self.parse_album,
                meta={
                    "product_id": product["id"],
                    "brand": product[
                        "brands"
                    ],  # note: singular key "brand" for clarity
                    "yupoo_album_url": product["yupoo_album_url"],
                },
            )

    # *----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    def parse_album(self, response):
        # Extract from meta
        product_id = response.meta.get("product_id")
        brand = response.meta.get("brand")
        yupoo_album_url = response.meta.get("yupoo_album_url")

        # Cover image
        product_cover_image = get_product_image_cover(response)

        # Product image URLs
        product_image_urls = response.css(
            ".image__imagewrap img::attr(data-origin-src)"
        ).getall()

        raw_header = response.css("span.showalbumheader__gallerytitle::text").get()
        price = None
        header = None
        size_data = None

        if raw_header:
            # Normalize fullwidth characters FIRST (fixes ＄ vs $ issue)
            normalized = normalize_fullwidth(raw_header)

            # 1. Extract Price — handles: 12$, 30$, 15$~23$, 39-45$, 12.99$
            if "$" in normalized:
                before_dollar = normalized[
                    : normalized.rindex("$")
                ]  # rindex captures full price range
                price_candidates = re.findall(r"\d+(?:\.\d+)?", before_dollar)
                price_candidates = [float(p) for p in price_candidates]
                if price_candidates:
                    price = max(price_candidates)
                    price = int(price) if price == int(price) else price

            # 2. Clean Header
            header = re.sub(
                r"[\u4e00-\u9fff\u3000-\u303f\uff00-\uffef]", "", normalized
            )
            if "$" in header:
                header = header.split("$", 1)[
                    -1
                ]  # still split on FIRST $ to drop price prefix
            header = re.sub(r"[#~＆&|【】「」『』（）()\-]+", " ", header)
            header = " ".join(header.split()).strip()

        # 3. Generate Slug (with fallback to brand + price)
        slug = generate_slug(header, category=brand, price=price)

        # 4. Extract Sizes
        texts = response.css(
            "div.showalbumheader__gallerysubtitle.htmlwrap__main ::text"
        ).getall()

        # Clean text nodes (remove empty + whitespace junk)
        cleaned = [t.strip() for t in texts if t.strip()]

        # Try to isolate the line that actually contains size info
        size_line = next(
            (t for t in cleaned if re.search(r"(?i)(sizes?|尺[寸码]|规格)", t)),
            "",
        )

        # Fallback to full text if no labeled size line found
        all_subtitle_text = size_line if size_line else " ".join(cleaned)

        # Normalize unicode/fullwidth characters
        all_subtitle_text = normalize_fullwidth(all_subtitle_text)

        # Fix broken decimals like "37 .5" -> "37.5"
        all_subtitle_text = re.sub(r"(\d)\s*\.\s*(\d)", r"\1.\2", all_subtitle_text)

        size_data = None

        # --- Primary extraction (label-based) ---
        match = re.search(
            r"(?i)(?:sizes?|尺[寸码]|规格)\s*[:：]?\s*(.+)",
            all_subtitle_text,
        )

        if match:
            extracted = re.findall(
                r"\b\d+(?:\.\d+)?\b|\b(?:XS|S|M|L|XL|XXL|\d+XL)\b",
                match.group(1),
                re.IGNORECASE,
            )
            size_data = [s.upper() for s in extracted] if extracted else None

        # --- Fallback (pattern-based detection) ---
        if not size_data:
            fallback = re.findall(
                r"\b(?:3[4-9]|4[0-9]|50)(?:\.\d+)?\b",
                all_subtitle_text,
            )
            size_data = fallback if len(fallback) >= 2 else None

        # --- Deduplicate ---
        if size_data:
            size_data = list(dict.fromkeys(size_data))

        product_data = {
            "price": price,
            "product_title": header if header else None,
            "sizes": size_data,
        }

        item = {
            "id": product_id,
            "brand": brand,
            "slug": slug,
            "yupoo_album_url": yupoo_album_url,
            "product_cover_image": product_cover_image,
            "product_image_urls": product_image_urls,
            "product_data": product_data,
        }

        if product_id:
            self.output_data[str(product_id)] = item

        yield item
