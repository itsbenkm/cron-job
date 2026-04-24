"""
This spider discovers new albums by crawling the category pages on the seller's Yupoo site, comparing against existing DB entries, and scraping only the new albums.
It runs after the validate spider has cleaned up any removed albums, so it can focus on finding genuinely new content to add to the DB.
It uses the same parsing logic as fashionbroda.py to extract album data, but only for albums that are not already in the DB based on their generated slugs.
This way we avoid redundant scraping and only process truly new albums, keeping our dataset fresh without unnecessary duplication.

It is the 6th script to run in the cron job
"""

import scrapy

from fashionbroda_cj.scripts.read_db import album_urls
from fashionbroda_cj.scripts.slug import generate_slug
from fashionbroda_cj.spiders.fashionbroda import FashionbrodaSpider


class DiscoverSpider(scrapy.Spider):
    name = "discover"
    allowed_domains = ["fashionbroda.x.yupoo.com"]
    custom_settings = {
        "CONCURRENT_REQUESTS": 10,
        "DOWNLOAD_DELAY": 0.2,
        "FEEDS": {
            "/home/b3n/Desktop/seller_cron_jobs/fashionbroda/fashionbroda_cj/fashionbroda_cj/data/new_album_data.json": {
                "format": "json",
                "encoding": "utf8",
                "indent": 2,
                "overwrite": True,
            }
        },
    }

    categories = {
        "https://fashionbroda.x.yupoo.com/categories/4832953": "Brands",
        "https://fashionbroda.x.yupoo.com/categories/4867662": "Chrome Hearts",
        "https://fashionbroda.x.yupoo.com/categories/4867669": "Acne Studios",
        "https://fashionbroda.x.yupoo.com/categories/4867658": "Louis Vuitton",
        "https://fashionbroda.x.yupoo.com/categories/4867666": "Balenciaga",
        "https://fashionbroda.x.yupoo.com/categories/4873422": "Moncler",
        "https://fashionbroda.x.yupoo.com/categories/4867656": "Miu Miu",
        "https://fashionbroda.x.yupoo.com/categories/4867659": "Gucci",
        "https://fashionbroda.x.yupoo.com/categories/4867657": "Maison Margiela",
        "https://fashionbroda.x.yupoo.com/categories/4867661": "Dior",
        "https://fashionbroda.x.yupoo.com/categories/4965942": "Loro Piana",
        "https://fashionbroda.x.yupoo.com/categories/4867654": "Ralph Lauren",
        "https://fashionbroda.x.yupoo.com/categories/4965938": "Thom Browne",
        "https://fashionbroda.x.yupoo.com/categories/4867655": "Prada",
        "https://fashionbroda.x.yupoo.com/categories/5001542": "Ami",
        "https://fashionbroda.x.yupoo.com/categories/4867665": "Burberry",
        "https://fashionbroda.x.yupoo.com/categories/4965946": "Brunello Cucinelli",
        "https://fashionbroda.x.yupoo.com/categories/4867663": "Celine",
        "https://fashionbroda.x.yupoo.com/categories/4873402": "Bottega Veneta",
        "https://fashionbroda.x.yupoo.com/categories/4873424": "Canada Goose",
        "https://fashionbroda.x.yupoo.com/categories/4874408": "Loewe",
        "https://fashionbroda.x.yupoo.com/categories/5037752": "Chanel",
        "https://fashionbroda.x.yupoo.com/categories/4867653": "Stone Island",
        "https://fashionbroda.x.yupoo.com/categories/4875314": "Saint Laurent",
        "https://fashionbroda.x.yupoo.com/categories/4902833": "Other Brands",
        "https://fashionbroda.x.yupoo.com/categories/0": "Uncategorized Album",
    }

    def start_requests(self):
        """
        Step 1 — Load all slugs and album URLs from the DB into memory.
        This is done once at startup so every comparison later is a fast
        in-memory set lookup. Nothing is scraped yet at this point.
        """
        db_rows = list(album_urls().values())

        # Store all slugs from the DB — this is our definitive skip list.
        self.db_slugs = {row["slug"] for row in db_rows if row.get("slug")}

        self.logger.info(f"[DISCOVER] Loaded {len(self.db_slugs)} slugs from DB.")

        # Step 2 — Start collecting all album URLs from every category.
        # We store them all in memory first before doing any slug comparison.
        # discovered_albums is a list of (full_album_url, brand) tuples.
        self.discovered_albums = []

        # Track how many category pages are still pending collection so we
        # know when collection is complete and comparison can begin.
        self.pending_category_pages = 0

        for category_url, brand in self.categories.items():
            self.pending_category_pages += 1
            yield scrapy.Request(
                category_url,
                callback=self.collect_album_urls,
                meta={"brand": brand},
            )

    def collect_album_urls(self, response):
        """
        Step 2 (continued) — Collect every album URL from category pages
        into self.discovered_albums without scraping any albums yet.
        Follows pagination until all pages for this category are exhausted.
        Only once ALL category pages across ALL categories are done does it
        move to Step 3 — the slug comparison and album scraping phase.
        """
        brand = response.meta.get("brand")

        for album in response.css(".categories__children a"):
            album_url = album.attrib.get("href")
            if not album_url:
                continue

            full_album_url = response.urljoin(album_url.strip())
            # Store the full URL and its brand together for later processing.
            self.discovered_albums.append((full_album_url, brand))

        # Follow pagination — each next page is also a collection step,
        # not a scraping step, so we keep pending_category_pages open.
        next_page = response.css("a[title='next page']::attr(href)").get()
        if next_page:
            yield response.follow(
                next_page,
                callback=self.collect_album_urls,
                meta={"brand": brand},
            )
        else:
            # This category (including all its paginated pages) is fully collected.
            self.pending_category_pages -= 1
            self.logger.info(
                f"[DISCOVER] Category '{brand}' fully collected. "
                f"Pending categories: {self.pending_category_pages}"
            )

            # Only proceed to comparison once every category page is done.
            if self.pending_category_pages == 0:
                self.logger.info(
                    f"[DISCOVER] All categories collected. "
                    f"Total albums found: {len(self.discovered_albums)}. "
                    f"Starting slug comparison..."
                )
                yield from self.compare_and_scrape()

    def compare_and_scrape(self):
        """
        Step 3 — Compare every discovered album's slug against the DB slugs.
        Go through the full discovered list from first to last:
          - Generate the slug for each album URL
          - If the slug matches one in the DB → skip it
          - If the slug is new → schedule it for scraping
        This runs entirely in memory after all URLs are collected,
        so there is no async race condition with partially loaded data.
        """
        new_count = 0
        skipped_count = 0
        # Dedup within the discovered list itself in case the same album
        # appears under multiple categories.
        seen_slugs = set()

        for full_album_url, brand in self.discovered_albums:
            slug = generate_slug(brand, full_album_url)

            # Skip if slug already exists in the DB.
            if slug in self.db_slugs:
                skipped_count += 1
                continue

            # Skip if we already queued this slug from another category.
            if slug in seen_slugs:
                continue

            seen_slugs.add(slug)
            new_count += 1

            self.logger.info(f"[DISCOVER] New album: {slug} → {full_album_url}")

            # Schedule the album page for scraping, passing brand and slug.
            yield scrapy.Request(
                full_album_url,
                callback=self.parse_album,
                meta={"brands": brand, "slug": slug, "product_id": None},
            )

        self.logger.info(
            f"[DISCOVER] Comparison done. "
            f"New: {new_count} | Skipped (in DB): {skipped_count}"
        )

    def parse_album(self, response):
        """
        Step 4 — Scrape the album page and yield the product item.
        Only albums that passed the slug comparison in Step 3 reach here.
        Reuses FashionbrodaSpider.parse_album() for field extraction and
        injects the slug into the output immediately after the brands field.

        Note: 404s on some album URLs are expected — Yupoo category pages
        can contain stale links to deleted albums. These are harmless.
        """
        slug = response.meta.get("slug") or generate_slug(
            response.meta.get("brands"), response.url
        )

        for item in FashionbrodaSpider.parse_album(self, response):
            # Rebuild item with slug injected right after brands.
            # Final field order:
            # product_id → brands → slug → yupoo_album_url → product_cover_image
            # → product_image_url → size_chart_url → product_data
            ordered_item = {}
            for key, value in item.items():
                ordered_item[key] = value
                if key == "brands":
                    ordered_item["slug"] = slug
            yield ordered_item
