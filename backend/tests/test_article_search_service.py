from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.services.article_search_service import ArticleSearchService


class FakeEmbeddingService:
    def __init__(self, vector: list[float]) -> None:
        self.vector = vector
        self.calls: list[list[str]] = []

    def embed(self, texts: list[str]) -> list[list[float]]:
        self.calls.append(texts)
        return [self.vector]


class FakeArticleRepository:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.calls: list[dict[str, object]] = []

    def search_similar(
        self,
        query_embedding: list[float],
        limit: int = 20,
        min_published_at: datetime | None = None,
        source_name: str | None = None,
    ) -> list[dict[str, object]]:
        self.calls.append(
            {
                "query_embedding": query_embedding,
                "limit": limit,
                "min_published_at": min_published_at,
                "source_name": source_name,
            }
        )
        return self.rows


def test_search_by_keywords_embeds_input_and_calls_repository():
    embedding_service = FakeEmbeddingService(vector=[0.1, 0.2, 0.3])
    expected_rows = [
        {
            "article_id": "abc",
            "canonical_url": "https://example.com/a",
            "title": "Inflation cools",
            "published_at": datetime(2026, 3, 9, tzinfo=timezone.utc),
            "source_name": "Reuters",
            "similarity": 0.88,
        }
    ]
    article_repository = FakeArticleRepository(rows=expected_rows)
    service = ArticleSearchService(
        embedding_service=embedding_service,
        article_repository=article_repository,
    )

    min_published_at = datetime(2026, 3, 1, tzinfo=timezone.utc)
    rows = service.search_by_keywords(
        keywords="inflation expectations",
        limit=5,
        min_published_at=min_published_at,
        source_name="Reuters",
    )

    assert rows == expected_rows
    assert embedding_service.calls == [["inflation expectations"]]
    assert article_repository.calls == [
        {
            "query_embedding": [0.1, 0.2, 0.3],
            "limit": 5,
            "min_published_at": min_published_at,
            "source_name": "Reuters",
        }
    ]


def test_search_by_keywords_rejects_blank_keywords():
    service = ArticleSearchService(
        embedding_service=FakeEmbeddingService(vector=[0.1]),
        article_repository=FakeArticleRepository(rows=[]),
    )

    with pytest.raises(ValueError, match="non-empty"):
        service.search_by_keywords("   ")
