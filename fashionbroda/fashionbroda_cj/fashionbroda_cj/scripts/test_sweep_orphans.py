from fashionbroda_cj.scripts.sweep_orphans import compute_orphans, partition_deletable
from fashionbroda_cj.scripts.purge_deleted_r2 import cdn_url_to_r2_key

CDN = "https://cdn.reps.cheap"


def test_compute_orphans_returns_only_unreferenced_keys():
    bucket = ["products/a/product/1.jpg", "products/b/product/2.jpg", "products/c/cover.jpg"]
    keep = {"products/a/product/1.jpg", "products/c/cover.jpg"}
    assert compute_orphans(bucket, keep) == ["products/b/product/2.jpg"]


def test_compute_orphans_empty_when_all_referenced():
    bucket = ["products/a/product/1.jpg", "products/b/product/2.jpg"]
    keep = {"products/a/product/1.jpg", "products/b/product/2.jpg"}
    assert compute_orphans(bucket, keep) == []


def test_compute_orphans_all_when_keep_empty():
    bucket = ["products/a/product/1.jpg", "products/b/product/2.jpg"]
    assert compute_orphans(bucket, set()) == bucket


def test_partition_deletable_protects_non_product_keys():
    orphans = ["icon.png", "products/a/product/1.jpg", "robots.txt", "products/b/cover.jpg"]
    deletable, protected = partition_deletable(orphans)
    assert deletable == ["products/a/product/1.jpg", "products/b/cover.jpg"]
    assert protected == ["icon.png", "robots.txt"]


def test_key_derivation_matches_bucket_key_with_spaces():
    url = "https://cdn.reps.cheap/products/moncler/moncler jacket-3875c57ebf/product/22.jpg"
    assert (
        cdn_url_to_r2_key(url, CDN)
        == "products/moncler/moncler jacket-3875c57ebf/product/22.jpg"
    )
