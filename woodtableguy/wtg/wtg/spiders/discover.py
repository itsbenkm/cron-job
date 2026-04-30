import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import scrapy

from wtg.scripts.read_db import album_urls
from wtg.spiders.woodtableguy import WoodtableguySpider, generate_slug

DATA_DIR = Path(__file__).resolve().parent.parent / "data2"


class DiscoverSpider(scrapy.Spider):
    name = "discover"
    allowed_domains = ["woodtableguy888.x.yupoo.com"]
    # start_urls = ["https://woodtableguy888.x.yupoo.com/categories"]

    custom_settings = {
        "CONCURRENT_REQUESTS": 10,
        "DOWNLOAD_DELAY": 0.2,
        "FEEDS": {
            f"{DATA_DIR}/new_album_data.json": {
                "format": "json",
                "encoding": "utf8",
                "indent": 2,
                "overwrite": True,
            }
        },
    }
    categories = {
        "https://woodtableguy888.x.yupoo.com/categories/4632996": "Nike Air Force AF1",
        "https://woodtableguy888.x.yupoo.com/categories/4632999": "Nike Air Jordon 4 AJ4",
        "https://woodtableguy888.x.yupoo.com/categories/4633144": "Nike DUNK SB",
        "https://woodtableguy888.x.yupoo.com/categories/4636083": "New Balance NB",
        "https://woodtableguy888.x.yupoo.com/categories/4636256": "Nike Air Jordon 1 AJ1",
        "https://woodtableguy888.x.yupoo.com/categories/4636302": "Travis Scott x Jordan Nike x Jumpman Jack TR",
        "https://woodtableguy888.x.yupoo.com/categories/4637901": "Adidas Yeezy",
        "https://woodtableguy888.x.yupoo.com/categories/4641432": "Alexander McQueen",
        "https://woodtableguy888.x.yupoo.com/categories/4643192": "Balenciaga",
        "https://woodtableguy888.x.yupoo.com/categories/4646173": "Nike Air Jordon 5 AJ5",
        "https://woodtableguy888.x.yupoo.com/categories/4646245": "OFF-WHITE",
        "https://woodtableguy888.x.yupoo.com/categories/4659585": "Nike Air Jordon 3 AJ3",
        "https://woodtableguy888.x.yupoo.com/categories/4665456": "Nike Air Jordon 11 AJ11",
        "https://woodtableguy888.x.yupoo.com/categories/4689740": "UGG",
        "https://woodtableguy888.x.yupoo.com/categories/4707874": "jjjjound x Asics",
        "https://woodtableguy888.x.yupoo.com/categories/4752860": "socks x Bag",
        "https://woodtableguy888.x.yupoo.com/categories/4753017": "Nike Sacai",
        "https://woodtableguy888.x.yupoo.com/categories/4809621": "Timberland",
        "https://woodtableguy888.x.yupoo.com/categories/4809655": "Nike MAX Air",
        "https://woodtableguy888.x.yupoo.com/categories/4809664": "Golden Goose GGDB",
        "https://woodtableguy888.x.yupoo.com/categories/4809666": "LANVIN",
        "https://woodtableguy888.x.yupoo.com/categories/4809676": "Gucci",
        "https://woodtableguy888.x.yupoo.com/categories/4809678": "Nike Air Jordon 6 AJ6",
        "https://woodtableguy888.x.yupoo.com/categories/4809684": "HERMES",
        "https://woodtableguy888.x.yupoo.com/categories/4809687": "MIHARA YASUHIRO",
        "https://woodtableguy888.x.yupoo.com/categories/4825777": "BAPE x jjjjound",
        "https://woodtableguy888.x.yupoo.com/categories/4840397": "Recommended purchase",
        "https://woodtableguy888.x.yupoo.com/categories/4856779": "Louis Vuitton LV",
        "https://woodtableguy888.x.yupoo.com/categories/4856781": "Nike Air Jordan 14 AJ14",
        "https://woodtableguy888.x.yupoo.com/categories/4856784": "Nike Kobe",
        "https://woodtableguy888.x.yupoo.com/categories/4856788": "Slippers",
        "https://woodtableguy888.x.yupoo.com/categories/4867007": "Nike ZoomX Vaporfly",
        "https://woodtableguy888.x.yupoo.com/categories/4875052": "Chanel",
        "https://woodtableguy888.x.yupoo.com/categories/4875054": "ON",
        "https://woodtableguy888.x.yupoo.com/categories/4883743": "Nike ACG GTX SE",
        "https://woodtableguy888.x.yupoo.com/categories/4889263": "RICK OWENS/RO",
        "https://woodtableguy888.x.yupoo.com/categories/4902496": "LOEWE",
        "https://woodtableguy888.x.yupoo.com/categories/4905315": "AMIRI Skel",
        "https://woodtableguy888.x.yupoo.com/categories/4905316": "HOKA",
        "https://woodtableguy888.x.yupoo.com/categories/4919026": "PRADA",
        "https://woodtableguy888.x.yupoo.com/categories/4921685": "KEEN",
        "https://woodtableguy888.x.yupoo.com/categories/4921688": "Birkenstock",
        "https://woodtableguy888.x.yupoo.com/categories/4923136": "lululemon",
        "https://woodtableguy888.x.yupoo.com/categories/4933916": "THE NORTH FACE THE",
        "https://woodtableguy888.x.yupoo.com/categories/4942895": "Bottega Veneta",
        "https://woodtableguy888.x.yupoo.com/categories/4963038": "DESCENTE",
        "https://woodtableguy888.x.yupoo.com/categories/4982782": "GG-CC",
        "https://woodtableguy888.x.yupoo.com/categories/4982784": "Nike Air Jordan 17 AJ17",
        "https://woodtableguy888.x.yupoo.com/categories/4984677": "Altra OLYMPUS",
        "https://woodtableguy888.x.yupoo.com/categories/4985584": "Maison Margiela",
        "https://woodtableguy888.x.yupoo.com/categories/4985586": "Reebok",
        "https://woodtableguy888.x.yupoo.com/categories/4985588": "Brooks",
        "https://woodtableguy888.x.yupoo.com/categories/4991198": "VALENTINO",
        "https://woodtableguy888.x.yupoo.com/categories/4992161": "LACOSTE",
        "https://woodtableguy888.x.yupoo.com/categories/5003692": "KAILAS",
        "https://woodtableguy888.x.yupoo.com/categories/5010041": "Nike Air Jordon 12 AJ12",
        "https://woodtableguy888.x.yupoo.com/categories/5010047": "Crocs",
        "https://woodtableguy888.x.yupoo.com/categories/5010104": "Adidas Superstar XLG",
        "https://woodtableguy888.x.yupoo.com/categories/5010131": "OLD",
        "https://woodtableguy888.x.yupoo.com/categories/5010134": "SMFK",
        "https://woodtableguy888.x.yupoo.com/categories/5010138": "KAALIXTO",
        "https://woodtableguy888.x.yupoo.com/categories/5010149": "MLB",
        "https://woodtableguy888.x.yupoo.com/categories/5011095": "FENDI",
        "https://woodtableguy888.x.yupoo.com/categories/5013732": "Loro Piana Zegan",
        "https://woodtableguy888.x.yupoo.com/categories/5019179": "Making",
        "https://woodtableguy888.x.yupoo.com/categories/5020797": "Mizuno",
        "https://woodtableguy888.x.yupoo.com/categories/5021614": "CAT",
        "https://woodtableguy888.x.yupoo.com/categories/5025812": "MIU MIU",
        "https://woodtableguy888.x.yupoo.com/categories/5030075": "Under Armour",
        "https://woodtableguy888.x.yupoo.com/categories/5030076": "Nike Bubble jet",
        "https://woodtableguy888.x.yupoo.com/categories/5039680": "Christian Louboutin CL",
        "https://woodtableguy888.x.yupoo.com/categories/5047078": "Nike Ja2",
        "https://woodtableguy888.x.yupoo.com/categories/5048253": "Nike Sabrina 3",
        "https://woodtableguy888.x.yupoo.com/categories/5049118": "Nike Vapor 12",
        "https://woodtableguy888.x.yupoo.com/categories/5049119": "Canada Goose",
        "https://woodtableguy888.x.yupoo.com/categories/5050958": "Nike Air Jordon 40 AJ40",
        "https://woodtableguy888.x.yupoo.com/categories/5051432": "Nike Killshot 2",
        "https://woodtableguy888.x.yupoo.com/categories/5052822": "xVESSEL",
        "https://woodtableguy888.x.yupoo.com/categories/5055568": "Roger Vivier",
        "https://woodtableguy888.x.yupoo.com/categories/5058049": "patagonia",
        "https://woodtableguy888.x.yupoo.com/categories/5058410": "Bag",
        "https://woodtableguy888.x.yupoo.com/categories/5070325": "Moncler",
        "https://woodtableguy888.x.yupoo.com/categories/5088438": "SMILEREPUBLIC",
        "https://woodtableguy888.x.yupoo.com/categories/5088439": "MARNI Pablo",
        "https://woodtableguy888.x.yupoo.com/categories/5097214": "VERSACE",
        "https://woodtableguy888.x.yupoo.com/categories/5097215": "Brunello Cucinelli",
        "https://woodtableguy888.x.yupoo.com/categories/5097390": "Burberry",
        "https://woodtableguy888.x.yupoo.com/categories/5101719": "Figurine",
        "https://woodtableguy888.x.yupoo.com/categories/5106009": "Converse",
        "https://woodtableguy888.x.yupoo.com/categories/5110696": "Polo Ralph Lauren",
        "https://woodtableguy888.x.yupoo.com/categories/5111275": "Lecco",
        "https://woodtableguy888.x.yupoo.com/categories/5126890": "Nike Air Jordon 10 AJ10",
        "https://woodtableguy888.x.yupoo.com/categories/5148793": "Saucony",
        "https://woodtableguy888.x.yupoo.com/categories/5149391": "Nike ReactX",
        "https://woodtableguy888.x.yupoo.com/categories/5162254": "Christian Dior",
        "https://woodtableguy888.x.yupoo.com/categories/5202472": "Alaia",
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
                meta={
                    "brand": brand,
                    "yupoo_album_url": full_album_url,
                    "slug": slug,
                    "product_id": None,
                },
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
        brand = response.meta.get("brand") or response.meta.get("brands")
        slug = response.meta.get("slug") or generate_slug(brand, response.url)

        for item in WoodtableguySpider.parse_album(self, response):
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
