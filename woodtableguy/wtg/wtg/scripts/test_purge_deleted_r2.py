from wtg.scripts.purge_deleted_r2 import cdn_url_to_r2_key, collect_keys

CDN = "https://wtg888.reps.cheap"


def test_strips_cdn_base_to_key():
    url = "https://wtg888.reps.cheap/products/nike/foo-abc123/images/01.jpg"
    assert cdn_url_to_r2_key(url, CDN) == "products/nike/foo-abc123/images/01.jpg"


def test_handles_leading_slash_only_input():
    assert cdn_url_to_r2_key("/products/x/y/01.jpg", CDN) == "products/x/y/01.jpg"


def test_collect_keys_gathers_images_and_cover():
    row = {
        "id": "1",
        "product_image_urls": [f"{CDN}/products/x/y/images/01.jpg"],
        "product_cover_image": f"{CDN}/products/x/y/cover.jpg",
    }
    assert collect_keys(row, CDN) == [
        "products/x/y/images/01.jpg",
        "products/x/y/cover.jpg",
    ]


def test_collect_keys_ignores_empty_fields():
    row = {"id": "1", "product_image_urls": None, "product_cover_image": None}
    assert collect_keys(row, CDN) == []
