"""Service for semantic article search using keyword queries."""

from __future__ import annotations

from datetime import datetime

from app.repositories.article_repository import ArticleRepository
from app.services.embedding_service import EmbeddingService


class ArticleSearchService:
    """Coordinates query embedding generation and vector retrieval."""

    def __init__(
        self,
        embedding_service: EmbeddingService,
        article_repository: ArticleRepository,
    ) -> None:
        self._embedding_service = embedding_service
        self._article_repository = article_repository

    def search_by_keywords(
        self,
        keywords: str,
        *,
        limit: int = 20,
        min_published_at: datetime | None = None,
        source_name: str | None = None,
        source_names: list[str] | None = None,
    ) -> list[dict[str, object]]:
        normalized_keywords = keywords.strip()
        if not normalized_keywords:
            raise ValueError("keywords must be non-empty")

        query_embedding = self._embedding_service.embed([normalized_keywords])[0]
        return self._article_repository.search_similar(
            query_embedding=query_embedding,
            limit=limit,
            min_published_at=min_published_at,
            source_name=source_name,
            source_names=source_names,
        )
