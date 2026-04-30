"""
This script contains the functions that read the DB
"""

# import json
import os

from dotenv import load_dotenv
from supabase import Client, create_client

load_dotenv()

supabase: Client = create_client(
    os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY")
)

# ********************************************************************************************************************************


def read_db():
    # ---------------------------------------------------
    # BATCH SETTINGS
    # ---------------------------------------------------
    batch_size = 1000
    start = 0
    all_rows = []

    while True:
        end = start + batch_size - 1

        data = (
            supabase.table("woodtableguy_products")
            .select(
                """
        id,
        brands,
        yupoo_album_url
    """
            )
            .eq("is_active", True)
            .eq("is_deleted", False)
            .range(start, end)
            # .limit(100)
            .execute()
        )

        # stop when no more rows are returned
        if not data.data:
            break

        all_rows.extend(data.data)

        print(f"Fetched rows {start} to {end} -> {len(data.data)} rows")

        start += batch_size

    # ------------------------------------------------------------------------------------------
    # result = {row["id"]: row for row in all_rows}
    # with open("products.json", "w", encoding="utf-8") as f:
    #    json.dump(result, f, indent=4, default=str)

    # return result

    # ------------------------------------------------------------------------------------------
    result = {row["id"]: row for row in all_rows}

    print(f"Total rows loaded: {len(all_rows)}")
    print(f"Rows in result dict: {len(result)}")

    return result

    # *--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def read_clean_db():
    # ---------------------------------------------------
    # BATCH SETTINGS
    # ---------------------------------------------------
    batch_size = 1000
    start = 0
    all_rows = []

    while True:
        end = start + batch_size - 1

        data = (
            supabase.table("woodtableguy_products")
            .select("""
                id,
                brands,
                slug,
                yupoo_album_url,
                product_cover_image,
                product_image_urls,
                updated_at,
                woodtableguy_product_data (
                    price,
                    product_title,
                    sizes,
                    updated_at
                )
            """)
            .eq("is_active", True)
            .eq("is_deleted", False)
            .range(start, end)
            .execute()
        )

        if not data.data:
            break

        all_rows.extend(data.data)
        print(f"Fetched rows {start} to {end} -> {len(data.data)} rows")

        start += batch_size

    result = {row["id"]: row for row in all_rows}
    return result


# *--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def album_urls():
    # ---------------------------------------------------
    # BATCH SETTINGS
    # ---------------------------------------------------
    batch_size = 1000
    start = 0
    all_rows = []

    while True:
        end = start + batch_size - 1

        data = (
            supabase.table("woodtableguy_products")
            .select(
                """
                id,
                slug,
                yupoo_album_url
                """
            )
            .eq("is_active", True)
            .eq("is_deleted", False)
            .range(start, end)
            .execute()
        )

        if not data.data:
            break

        all_rows.extend(data.data)
        print(f"Fetched rows {start} to {end} -> {len(data.data)} rows")
        start += batch_size

    result = {row["id"]: row for row in all_rows}
    return result


# *--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    read_db()
    read_clean_db()
    album_urls()
