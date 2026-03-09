"""News ingestion pipeline: fetch articles, enrich text, then persist."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from app.models.embedding import MAX_BATCH_SIZE
from app.repositories.article_repository import ArticleRepository
from app.services.embedding_service import EmbeddingService
from app.services.fetchers.newsapi_source import NewsApiSource
from app.services.text_processing_service import TextProcessingService
from app.utils.url import canonicalize_url


class NewsIngestionPipeline:
    """Minimal 2-stage ingestion pipeline for hackathon speed."""

    def __init__(
        self,
        fetchers: list[NewsApiSource],
        embedding_service: EmbeddingService,
        text_processing_service: TextProcessingService | None = None,
        text_processing_max_workers: int = 10,
        article_repository: ArticleRepository | None = None,
    ) -> None:
        self._fetchers = fetchers
        self._embedding_service = embedding_service
        self._text_processing_service = text_processing_service
        self._text_processing_max_workers = max(text_processing_max_workers, 1)
        self._article_repository = article_repository

    def run(self, **fetch_params: Any) -> dict[str, Any]:
        # Stage 1: fetch orchestration.
        fetched_articles, fetch_errors, fetch_error_messages = self._fetch_articles(**fetch_params)
        deduped_articles, duplicate_count = self._dedupe_by_url(fetched_articles)

        # Stage 2: enrich fetched results with embeddings + text processing.
        (
            processed_articles,
            keep_true_articles,
            filtered_out_urls,
            text_processed_count,
            filtered_out_count,
            skipped_count,
            embedding_errors,
            text_processing_errors,
            text_processing_retry_count,
            text_processing_discarded_count,
        ) = self._enrich_articles(deduped_articles)

        # Stage 3: delete filtered rows and store keep=true rows.
        deleted_count, deletion_errors, deletion_error_message = self._delete_filtered_articles(
            filtered_out_urls
        )
        (
            persisted_count,
            inserted_count,
            updated_count,
            invalid_url_count,
            persistence_errors,
            persistence_error_message,
        ) = self._persist_articles(keep_true_articles)

        return {
            "fetched_count": len(fetched_articles),
            "deduped_count": len(deduped_articles),
            "duplicate_count": duplicate_count,
            "embedded_count": len([article for article in processed_articles if article.get("embedding")]),
            "text_processed_count": text_processed_count,
            "filtered_out_count": filtered_out_count,
            "deleted_filtered_count": deleted_count,
            "text_processing_retry_count": text_processing_retry_count,
            "text_processing_discarded_count": text_processing_discarded_count,
            "persisted_count": persisted_count,
            "inserted_count": inserted_count,
            "updated_count": updated_count,
            "invalid_url_count": invalid_url_count,
            "skipped_count": skipped_count,
            "errors_count": (
                fetch_errors
                + embedding_errors
                + text_processing_errors
                + deletion_errors
                + persistence_errors
            ),
            "fetch_errors_count": fetch_errors,
            "embedding_errors_count": embedding_errors,
            "text_processing_errors_count": text_processing_errors,
            "deletion_errors_count": deletion_errors,
            "persistence_errors_count": persistence_errors,
            "fetch_error_messages": fetch_error_messages,
            "deletion_error": deletion_error_message,
            "persistence_error": persistence_error_message,
            "articles": processed_articles,
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

    def _enrich_articles(
        self, articles: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str], int, int, int, int, int, int, int]:
        if not articles:
            return [], [], [], 0, 0, 0, 0, 0, 0, 0

        with ThreadPoolExecutor(max_workers=2) as executor:
            embedding_future = executor.submit(self._embed_articles, articles)
            text_processing_future = executor.submit(self._process_articles, articles)

            index_to_vector, embedding_errors = embedding_future.result()
            (
                index_to_structured_data,
                text_processing_errors,
                text_processing_retry_count,
                text_processing_discarded_count,
            ) = text_processing_future.result()

        processed_articles: list[dict[str, Any]] = []
        keep_true_articles: list[dict[str, Any]] = []
        filtered_out_urls: list[str] = []
        text_processed_count = 0
        filtered_out_count = 0
        for idx, article in enumerate(articles):
            structured_data = index_to_structured_data.get(idx)
            if structured_data is None:
                # Skip DB write when text processing fails.
                continue

            vector = index_to_vector.get(idx)
            text_processed_count += 1
            keep_value = structured_data.get("keep")
            keep_article = keep_value if isinstance(keep_value, bool) else True

            enriched_article = dict(article)
            enriched_article["text_processing"] = structured_data
            if vector is not None:
                enriched_article["embedding"] = vector
            processed_articles.append(enriched_article)

            if not keep_article:
                filtered_out_count += 1
                canonical_url = self._article_to_canonical_url(article)
                if canonical_url:
                    filtered_out_urls.append(canonical_url)
                continue

            if vector is not None:
                keep_true_articles.append(enriched_article)

        skipped_count = len(articles) - len(keep_true_articles) - filtered_out_count
        return (
            processed_articles,
            keep_true_articles,
            filtered_out_urls,
            text_processed_count,
            filtered_out_count,
            skipped_count,
            embedding_errors,
            text_processing_errors,
            text_processing_retry_count,
            text_processing_discarded_count,
        )

    def _embed_articles(self, articles: list[dict[str, Any]]) -> tuple[dict[int, list[float]], int]:
        candidates: list[tuple[int, str]] = []
        for idx, article in enumerate(articles):
            text = self._article_to_text(article)
            if text:
                candidates.append((idx, text))

        if not candidates:
            return {}, 0

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
        return index_to_vector, errors_count

    def _process_articles(
        self, articles: list[dict[str, Any]]
    ) -> tuple[dict[int, dict[str, Any]], int, int, int]:
        if self._text_processing_service is None:
            return {}, 0, 0, 0

        candidates: list[tuple[int, str]] = []
        for idx, article in enumerate(articles):
            text = self._article_to_text(article)
            if text:
                candidates.append((idx, text))

        if not candidates:
            return {}, 0, 0, 0

        first_successes, first_failures = self._process_candidates_once(candidates)
        retry_count = len(first_failures)

        second_successes: dict[int, dict[str, Any]] = {}
        second_failures: list[tuple[int, str]] = []
        if first_failures:
            second_successes, second_failures = self._process_candidates_once(first_failures)

        merged: dict[int, dict[str, Any]] = dict(first_successes)
        merged.update(second_successes)
        discarded_count = len(second_failures)
        return merged, discarded_count, retry_count, discarded_count

    def _process_candidates_once(
        self, candidates: list[tuple[int, str]]
    ) -> tuple[dict[int, dict[str, Any]], list[tuple[int, str]]]:
        if not candidates:
            return {}, []

        candidate_map = {idx: text for idx, text in candidates}
        successes: dict[int, dict[str, Any]] = {}
        failures: list[tuple[int, str]] = []
        max_workers = min(self._text_processing_max_workers, len(candidates))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_idx = {
                executor.submit(self._text_processing_service.process, text): idx
                for idx, text in candidates
            }
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    payload = future.result()
                    if not isinstance(payload, dict):
                        raise ValueError("Text processing output is not a JSON object")
                    successes[idx] = payload
                except Exception:
                    failures.append((idx, candidate_map[idx]))
        return successes, failures

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

    def _delete_filtered_articles(
        self, canonical_urls: list[str]
    ) -> tuple[int, int, str | None]:
        if not canonical_urls:
            return 0, 0, None
        if self._article_repository is None:
            return 0, 0, None
        try:
            deleted = self._article_repository.delete_by_canonical_urls(canonical_urls)
            return deleted, 0, None
        except Exception as exc:
            return 0, 1, str(exc)

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
    def _article_to_canonical_url(article: dict[str, Any]) -> str | None:
        raw_url = article.get("url")
        if not isinstance(raw_url, str):
            return None
        return canonicalize_url(raw_url)

    @staticmethod
    def _chunk(items: list[tuple[int, str]], size: int) -> list[list[tuple[int, str]]]:
        return [items[i : i + size] for i in range(0, len(items), size)]
