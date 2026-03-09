from app.repositories.article_repository import ArticleRepository


def test_to_row_uses_normalized_source_fields():
    repository = ArticleRepository.__new__(ArticleRepository)

    row = repository._to_row(
        {
            "source_id": "reuters",
            "source_name": "Reuters",
            "title": "Inflation cools",
            "description": "Price pressure eases.",
            "content": "Markets reprice rate expectations.",
            "url": "https://example.com/articles/inflation-cools",
            "published_at": "2026-03-09T00:00:00Z",
            "embedding": [0.1, 0.2],
        }
    )

    assert row["source_name"] == "Reuters"
    assert row["source_article_id"] == "reuters"


def test_to_row_still_supports_nested_source_payload():
    repository = ArticleRepository.__new__(ArticleRepository)

    row = repository._to_row(
        {
            "source": {"id": "ap", "name": "Associated Press"},
            "title": "Jobs report surprises",
            "url": "https://example.com/articles/jobs-report",
        }
    )

    assert row["source_name"] == "Associated Press"
    assert row["source_article_id"] == "ap"


def test_normalize_source_filters_dedupes_and_trims():
    repository = ArticleRepository.__new__(ArticleRepository)

    normalized = repository._normalize_source_filters(
        source_name=" Reuters ",
        source_names=["BBC News", "reuters", " ", "The Guardian"],
    )

    assert normalized == ["Reuters", "BBC News", "The Guardian"]
