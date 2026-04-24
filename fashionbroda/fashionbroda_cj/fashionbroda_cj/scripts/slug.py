"""
This is the 3rd script to run, it also houses the slug generation function
It takes in the json data from fashionbroda.py and creates slugs for the albums that have been crawled
"""

import hashlib
import json
import re


def normalize_category(category: str) -> str:
    # normalize category for stable slug output
    text = (category or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


def album_hash_from_url(album_url: str) -> str:
    # keep hash format consistent with existing pipeline
    return hashlib.sha1(album_url.encode("utf-8")).hexdigest()[:10]


def generate_slug(category: str, album_url: str) -> str:
    brand = normalize_category(category)
    if not brand:
        raise ValueError("Missing category")
    if not album_url:
        raise ValueError("Missing album_url")
    return f"{brand}-{album_hash_from_url(album_url)}"


def main() -> None:
    # Keep the file-processing behavior opt-in so importing generate_slug does not run it.
    json_file_path = "/home/b3n/Desktop/seller_cron_jobs/fashionbroda/fashionbroda_cj/fashionbroda_cj/data/album_data.json"

    try:
        # Load scraped album data only when this script is run directly.
        with open(json_file_path, "r", encoding="utf-8") as json_file:
            data = json.load(json_file)

    except FileNotFoundError:
        print(f"JSON file not found at: {json_file_path}")
        raise SystemExit("JSON file not found")
    except json.JSONDecodeError as e:
        print(f"Invalid JSON format: {e}")
        raise SystemExit("Invalid JSON file")
    except Exception as e:
        print(f"Unexpected error reading JSON file: {e}")
        raise SystemExit("Error reading JSON file")

    output_data = {}

    for item in data:
        try:
            row_id = item["product_id"]

            # Generate a stable slug from the category name and album URL.
            slug = generate_slug(item.get("brands"), item.get("yupoo_album_url"))

            ordered_item = {}

            for key, value in item.items():
                ordered_item[key] = value

                if key == "brands":
                    ordered_item["slug"] = slug

            output_data[row_id] = ordered_item

            print(f"Generated slug: {slug} for category: {item.get('brands')}")

        except ValueError as e:
            print(
                f"Error generating slug for item with category "
                f"'{item.get('brands')}' and album URL "
                f"'{item.get('yupoo_album_url')}': {e}"
            )

    output_path = "/home/b3n/Desktop/seller_cron_jobs/fashionbroda/fashionbroda_cj/fashionbroda_cj/data/slug.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2)

    print("Slug generation complete.")


if __name__ == "__main__":
    main()
