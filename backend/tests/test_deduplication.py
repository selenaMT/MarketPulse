from app.pipelines.news_ingestion_pipeline import NewsIngestionPipeline
from app.utils.url import canonicalize_url


def test_canonicalize_url_normalizes_tracking_and_slash():
    assert canonicalize_url("https://example.com/a/") == "https://example.com/a"
    assert (
        canonicalize_url("https://example.com/a?utm_source=x&utm_medium=y")
        == "https://example.com/a"
    )
    assert canonicalize_url("https://example.com/a?ref=abc&utm_source=x") == "https://example.com/a?ref=abc"


def test_pipeline_dedupe_uses_canonical_url_rules():
    articles = [
        {"url": "https://example.com/a"},
        {"url": "https://example.com/a/"},
        {"url": "https://example.com/a?utm_source=x"},
        {"url": "https://example.com/a?ref=abc"},
        {"url": "https://example.com/A"},
    ]

    deduped, duplicate_count = NewsIngestionPipeline._dedupe_by_url(articles)

    assert len(deduped) == 3
    assert duplicate_count == 2
