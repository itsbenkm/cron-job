"""
This spider validates whether the albums in the DB exist on the seller's page on yupoo, if not it, updates the DB.
It makes requests to the album URLs in the DB, if it gets a 404 response, it means the album has been removed, and we will update the DB to set is_active to false and is_deleted to true for those albums.

This is the first script that will run in the cron job, and it will help us keep the DB clean by removing the albums that have been removed from the seller's page,
so that we don't waste resources on scraping albums that no longer exist, and also to keep the data accurate for the users.
"""

# import json
import os
from datetime import datetime, timezone

import scrapy
from dotenv import load_dotenv
from supabase import Client, create_client

from fashionbroda_cj.scripts.read_db import read_db

load_dotenv()

supabase: Client = create_client(
    os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY")
)


class ValidateSpider(scrapy.Spider):
    name = "validate"
    allowed_domains = ["fashionbroda.x.yupoo.com"]
    # we use this to catch 404 errors, which means the album has been removed, this is a scrapy feature, we will handle the 404 errors in the parse method
    handle_httpstatus_list = [404]
    # start_urls = ["https://fashionbroda.x.yupoo.com/categories"]
    custom_settings = {
        "DOWNLOAD_DELAY": 0.15,  # Adjust the delay as needed
        "CONCURRENT_REQUESTS_PER_DOMAIN": 10,  # Limit concurrent requests to the same domain
    }

    async def start(self):
        self.products = read_db()
        self.removed_albums = []
        for product in self.products.values():
            if not product.get("yupoo_album_url"):
                continue
            yield scrapy.Request(
                # we pass the url we want the spider to crawl
                url=product["yupoo_album_url"],
                callback=self.validate_album,
                meta={"product_id": product["id"]},
            )

    def validate_album(self, response):
        product_id = response.meta["product_id"]
        if response.status == 404:  # 404 Not Found
            # Add validation logic here
            self.removed_albums.append(product_id)

    # *--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    # close the spider and update the db, we will set is_active to false and is_deleted to true for the removed albums
    def closed(self, reason):
        if self.removed_albums:
            self.update_db()

    def utc_now(self):
        return datetime.now(timezone.utc).isoformat()

    def update_db(self):
        supabase.table("fashionbroda_products").update(
            {"is_active": False, "is_deleted": True, "updated_at": self.utc_now()}
        ).in_("id", self.removed_albums).execute()
