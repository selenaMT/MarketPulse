"""News ingestion pipeline: fetch articles, embed text, then persist (TODO)."""

from __future__ import annotations

from typing import Any

from app.models.embedding import MAX_BATCH_SIZE
from app.repositories.article_repository import ArticleRepository
from app.services.embedding_service import EmbeddingService
from app.services.fetchers.newsapi_source import NewsApiSource
from app.utils.url import canonicalize_url


class NewsIngestionPipeline:
    """Minimal 2-stage ingestion pipeline for hackathon speed."""

    def __init__(
        self,
        fetchers: list[NewsApiSource],
        embedding_service: EmbeddingService,
        article_repository: ArticleRepository | None = None,
    ) -> None:
        self._fetchers = fetchers
        self._embedding_service = embedding_service
        self._article_repository = article_repository

    def run(self, **fetch_params: Any) -> dict[str, Any]:
        # Stage 1: fetch orchestration.
        fetched_articles, fetch_errors, fetch_error_messages = self._fetch_articles(**fetch_params)
        deduped_articles, duplicate_count = self._dedupe_by_url(fetched_articles)

        # Stage 2: embed fetched results.
        embedded_articles, skipped_count, embedding_errors = self._embed_articles(deduped_articles)

        # Stage 3: store to DB.
        (
            persisted_count,
            inserted_count,
            updated_count,
            invalid_url_count,
            persistence_errors,
            persistence_error_message,
        ) = self._persist_articles(embedded_articles)

        return {
            "fetched_count": len(fetched_articles),
            "deduped_count": len(deduped_articles),
            "duplicate_count": duplicate_count,
            "embedded_count": len(embedded_articles),
            "persisted_count": persisted_count,
            "inserted_count": inserted_count,
            "updated_count": updated_count,
            "invalid_url_count": invalid_url_count,
            "skipped_count": skipped_count,
            "errors_count": fetch_errors + embedding_errors + persistence_errors,
            "fetch_errors_count": fetch_errors,
            "embedding_errors_count": embedding_errors,
            "persistence_errors_count": persistence_errors,
            "fetch_error_messages": fetch_error_messages,
            "persistence_error": persistence_error_message,
            "articles": embedded_articles,
        }

    def _fetch_articles(self, **fetch_params: Any) -> tuple[list[dict[str, Any]], int, list[str]]:
        all_articles: list[dict[str, Any]] = []
        errors_count = 0
        error_messages: list[str] = []
        for fetcher in self._fetchers:
            try:
                all_articles.extend(fetcher.fetch(**fetch_params))
            except Exception as exc:
                errors_count += 1
                error_messages.append(str(exc))
        return all_articles, errors_count, error_messages

    @staticmethod
    def _dedupe_by_url(articles: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
        deduped: list[dict[str, Any]] = []
        seen_urls: set[str] = set()

        for article in articles:
            raw_url = article.get("url")
            if not isinstance(raw_url, str) or not raw_url.strip():
                deduped.append(article)
                continue

            normalized_url = canonicalize_url(raw_url)
            if not normalized_url:
                deduped.append(article)
                continue

            if normalized_url in seen_urls:
                continue

            seen_urls.add(normalized_url)
            deduped.append(article)

        duplicate_count = len(articles) - len(deduped)
        return deduped, duplicate_count

    def _embed_articles(
        self, articles: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], int, int]:
        candidates: list[tuple[int, str]] = []
        for idx, article in enumerate(articles):
            text = self._article_to_text(article)
            if text:
                candidates.append((idx, text))

        if not candidates:
            return [], len(articles), 0

        index_to_vector: dict[int, list[float]] = {}
        errors_count = 0
        for chunk in self._chunk(candidates, MAX_BATCH_SIZE):
            chunk_indexes = [idx for idx, _ in chunk]
            chunk_texts = [text for _, text in chunk]
            try:
                chunk_vectors = self._embedding_service.embed(chunk_texts)
                for article_idx, vector in zip(chunk_indexes, chunk_vectors):
                    index_to_vector[article_idx] = vector
            except Exception:
                # Fallback to per-article embedding so one bad article does not kill the run.
                for article_idx, text in chunk:
                    try:
                        vectors = self._embedding_service.embed([text])
                        if vectors:
                            index_to_vector[article_idx] = vectors[0]
                    except Exception:
                        errors_count += 1

        embedded_articles: list[dict[str, Any]] = []
        for idx, article in enumerate(articles):
            vector = index_to_vector.get(idx)
            if vector is None:
                continue
            enriched_article = dict(article)
            enriched_article["embedding"] = vector
            embedded_articles.append(enriched_article)

        skipped_count = len(articles) - len(embedded_articles)
        return embedded_articles, skipped_count, errors_count

    def _persist_articles(
        self, articles: list[dict[str, Any]]
    ) -> tuple[int, int, int, int, int, str | None]:
        if not articles:
            return 0, 0, 0, 0, 0, None
        if self._article_repository is None:
            return 0, 0, 0, 0, 0, None
        try:
            persisted_count, invalid_url_count, inserted_count, updated_count = (
                self._article_repository.upsert_many(articles)
            )
            return persisted_count, inserted_count, updated_count, invalid_url_count, 0, None
        except Exception as exc:
            return 0, 0, 0, 0, 1, str(exc)

    @staticmethod
    def _article_to_text(article: dict[str, Any]) -> str | None:
        fields = [
            ("Title", article.get("title")),
            ("Description", article.get("description")),
            ("Content", article.get("content")),
        ]
        parts: list[str] = []
        for label, value in fields:
            text = (value or "").strip()
            if text:
                parts.append(f"{label}: {text}")
        return "\n".join(parts) if parts else None

    @staticmethod
    def _chunk(items: list[tuple[int, str]], size: int) -> list[list[tuple[int, str]]]:
        return [items[i : i + size] for i in range(0, len(items), size)]
