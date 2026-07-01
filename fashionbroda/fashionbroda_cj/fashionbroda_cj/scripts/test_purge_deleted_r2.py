from fashionbroda_cj.scripts.purge_deleted_r2 import cdn_url_to_r2_key, collect_keys

CDN = "https://cdn.reps.cheap"


def test_strips_cdn_base_to_key():
    url = "https://cdn.reps.cheap/products/nike/foo-abc123/product/01.jpg"
    assert cdn_url_to_r2_key(url, CDN) == "products/nike/foo-abc123/product/01.jpg"


def test_handles_leading_slash_only_input():
    assert cdn_url_to_r2_key("/products/x/y/01.jpg", CDN) == "products/x/y/01.jpg"


def test_collect_keys_gathers_all_image_fields():
    row = {
        "id": "1",
        "product_image_urls": [f"{CDN}/products/x/y/product/01.jpg"],
        "product_cover_image": f"{CDN}/products/x/y/cover.jpg",
        "size_chart_image_urls": [f"{CDN}/products/x/y/size-chart/01.jpg"],
    }
    assert collect_keys(row, CDN) == [
        "products/x/y/product/01.jpg",
        "products/x/y/cover.jpg",
        "products/x/y/size-chart/01.jpg",
    ]


def test_collect_keys_ignores_empty_fields():
    row = {"id": "1", "product_image_urls": None, "product_cover_image": None, "size_chart_image_urls": []}
    assert collect_keys(row, CDN) == []
