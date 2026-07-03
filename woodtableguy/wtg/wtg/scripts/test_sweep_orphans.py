from wtg.scripts.sweep_orphans import compute_orphans, partition_deletable
from wtg.scripts.purge_deleted_r2 import cdn_url_to_r2_key

CDN = "https://wtg888.reps.cheap"


def test_compute_orphans_returns_only_unreferenced_keys():
    bucket = ["products/a/1.jpg", "products/b/2.jpg", "products/c/3.jpg"]
    keep = {"products/a/1.jpg", "products/c/3.jpg"}
    assert compute_orphans(bucket, keep) == ["products/b/2.jpg"]


def test_compute_orphans_empty_when_all_referenced():
    bucket = ["products/a/1.jpg", "products/b/2.jpg"]
    keep = {"products/a/1.jpg", "products/b/2.jpg"}
    assert compute_orphans(bucket, keep) == []


def test_compute_orphans_all_when_keep_empty():
    bucket = ["products/a/1.jpg", "products/b/2.jpg"]
    assert compute_orphans(bucket, set()) == bucket


def test_partition_deletable_protects_non_product_keys():
    orphans = ["icon.png", "products/a/1.jpg", "favicon.ico", "products/b/2.jpg"]
    deletable, protected = partition_deletable(orphans)
    assert deletable == ["products/a/1.jpg", "products/b/2.jpg"]
    assert protected == ["icon.png", "favicon.ico"]


def test_key_derivation_matches_bucket_key_with_spaces_and_specials():
    # A real wtg slug carries spaces and '$'; the derived key must match the bucket key
    # byte-for-byte, or a live image would look like an orphan.
    url = "https://wtg888.reps.cheap/products/balenciaga/50$ LT Balenciaga 3XL ZL 1.7-1b277a78e1/images/01.jpg"
    assert (
        cdn_url_to_r2_key(url, CDN)
        == "products/balenciaga/50$ LT Balenciaga 3XL ZL 1.7-1b277a78e1/images/01.jpg"
    )
