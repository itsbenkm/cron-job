"""
This spider scrapes the albums that have been verified to exist by the validate spider, it uses the album URLs from the DB to scrape the album data,
This spider will be used to check for any changes in album data and to check for new images, this will be used to update the existing info in the DB

"""

import re

import scrapy

from fashionbroda_cj.scripts.read_db import read_db


class FashionbrodaSpider(scrapy.Spider):
    name = "fashionbroda"
    allowed_domains = ["fashionbroda.x.yupoo.com"]
    custom_settings = {
        "DOWNLOAD_DELAY": 0.15,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 10,
        "FEEDS": {
            "/home/b3n/Desktop/seller_cron_jobs/fashionbroda/fashionbroda_cj/fashionbroda_cj/data/album_data.json": {
                "format": "json",
                "encoding": "utf8",
                "indent": 2,
                "overwrite": True,
            }
        },
    }

    async def start(self):
        # preload the db data
        self.products = read_db()
        for product in self.products.values():
            if not product.get("yupoo_album_url"):
                continue
            yield scrapy.Request(
                # pass the album urls from the DB to the scraper, since they are the ones to be scraped
                url=product["yupoo_album_url"],
                callback=self.parse_album,
                meta={"product_id": product["id"], "brands": product["brands"]},
            )

    def parse_album(self, response):
        product_cover_image = response.css("img.autocover::attr(data-origin-src)").get()
        if not product_cover_image:
            product_cover_image = None
        # Get individual product image links from the album page
        product_image_url = response.xpath(
            '//img[contains(@class,"image__portrait")]/@data-origin-src'
        ).getall()
        if not product_image_url:
            product_image_url = None
        # Get the sizing-data-sheet
        size_chart_url = response.xpath(
            '//img[contains(@class,"image__landscape")]/@data-origin-src'
        ).getall()
        if not size_chart_url:
            size_chart_url = None
        raw_description = response.xpath("//meta[@name='description']/@content").get()

        product_description = raw_description.split("\n") if raw_description else []

        # Create a dictionary to hold the product description data
        product_data = {}

        # create a loop to split the product description data into key-value pairs
        for data in product_description:
            # check if the data contains a colon
            if ":" in data:
                # split the data into key and value
                key, value = data.split(":", 1)  # split ONLY once

                # strip convert key to lowercase and replace bullet points with whitespace then strip the whitespace
                # Remove all bullet-like Unicode characters
                clean_key = (
                    re.sub(r"[•●・◦◘○◉⦿⦾▪▫]", "", key).lower().strip().replace(" ", "_")
                )

                # strip whitespace from value
                clean_value = value.strip()

                # Try to convert price to int
                # check whether the clean_key is 'price', using the equality operator (==)
                if clean_key == "price":
                    # Filter out non-numeric characters (like '$', '¥', spaces) so only digits remain
                    # This is done using a generator expression inside the join() method, which iterates over each character in the clean_value string and includes only those that are digits, effectively removing any currency symbols or formatting characters.
                    # then joins them into one clean numeric string that can be converted to an integer, this is important to ensure that we are working with clean and consistent price data, which can be useful for analysis and comparison later on in the data processing pipeline
                    numeric_string = "".join(
                        character for character in clean_value if character.isdigit()
                    )

                    # try to convert the cleaned price string to an integer
                    try:
                        # convert the clean numerical string to an integer
                        clean_value = int(numeric_string)
                    # raise a value error if conversion fails
                    except ValueError:
                        # instead of failing, just pass the value as is to the json output
                        pass

                # specific handling for 'sizes' to convert string to list
                if clean_key in ("sizes", "size") and isinstance(clean_value, str):
                    clean_value = [
                        s for s in re.split(r"[,\-/\s]+", clean_value.strip()) if s
                    ]

                # unified empty check: set value to None if it is empty string or empty list
                if clean_value == "" or clean_value == []:
                    clean_value = None

                # add the key-value pair to the product_data dictionary
                # strip whitespace, convert key to lowercase and replace spaces with underscores
                product_data[clean_key] = clean_value

                # *-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        yield {
            "product_id": response.meta["product_id"],
            "brands": response.meta["brands"],
            "yupoo_album_url": response.url,
            "product_cover_image": product_cover_image,
            "product_image_url": product_image_url,
            "size_chart_url": size_chart_url,
            "product_data": product_data,
        }
