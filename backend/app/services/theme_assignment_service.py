"""Semantic assignment service for themes and candidate themes."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from app.models.embedding import MAX_BATCH_SIZE
from app.repositories.theme_repository import ThemeRepository
from app.services.embedding_service import EmbeddingService


class ThemeAssignmentService:
    """Assign article narratives to themes/candidates and promote qualified candidates."""

    def __init__(
        self,
        embedding_service: EmbeddingService,
        theme_repository: ThemeRepository,
        theme_match_threshold: float | None = None,
        candidate_match_threshold: float | None = None,
        promotion_article_count: int | None = None,
        snapshot_min_new_articles: int | None = None,
        snapshot_min_age_hours: int | None = None,
    ) -> None:
        self._embedding_service = embedding_service
        self._theme_repository = theme_repository
        self._theme_match_threshold = (
            theme_match_threshold
            if theme_match_threshold is not None
            else float(os.getenv("THEME_MATCH_THRESHOLD", "0.6"))
        )
        self._candidate_match_threshold = (
            candidate_match_threshold
            if candidate_match_threshold is not None
            else float(os.getenv("CANDIDATE_THEME_MATCH_THRESHOLD", "0.6"))
        )
        self._promotion_article_count = (
            promotion_article_count
            if promotion_article_count is not None
            else int(os.getenv("CANDIDATE_PROMOTION_ARTICLE_COUNT", "3"))
        )
        self._snapshot_min_new_articles = (
            snapshot_min_new_articles
            if snapshot_min_new_articles is not None
            else int(os.getenv("THEME_SNAPSHOT_MIN_NEW_ARTICLES", "4"))
        )
        self._snapshot_min_age_hours = (
            snapshot_min_age_hours
            if snapshot_min_age_hours is not None
            else int(os.getenv("THEME_SNAPSHOT_MIN_AGE_HOURS", "24"))
        )
        self._user_theme_match_threshold = float(os.getenv("USER_THEME_MATCH_THRESHOLD", "0.68"))
        self._user_theme_match_limit = int(os.getenv("USER_THEME_MATCH_LIMIT", "20"))

    def assign_articles(self, articles: list[dict[str, Any]]) -> dict[str, int]:
        stats = {
            "theme_narratives_processed": 0,
            "theme_matched_real": 0,
            "theme_matched_candidate": 0,
            "theme_candidates_created": 0,
            "theme_candidates_promoted": 0,
            "theme_links_upserted": 0,
            "candidate_theme_links_upserted": 0,
            "theme_snapshots_created": 0,
            "user_theme_links_upserted": 0,
            "user_themes_matched": 0,
        }
        work_items: list[dict[str, Any]] = []
        try:
            for article in articles:
                article_id = article.get("article_id")
                if not article_id:
                    continue
                narratives = self._extract_narratives(article)
                if not narratives:
                    continue

                seen_at = self._article_seen_at(article)

                for narrative in narratives:
                    work_items.append(
                        {
                            "article_id": article_id,
                            "narrative": narrative,
                            "seen_at": seen_at,
                        }
                    )

            if not work_items:
                self._theme_repository.commit()
                return stats

            narrative_texts = [str(item["narrative"]) for item in work_items]
            embeddings = self._embed_in_chunks(narrative_texts)
            for item, embedding in zip(work_items, embeddings):
                stats["theme_narratives_processed"] += 1
                assigned = self._assign_single_narrative(
                    article_id=item["article_id"],
                    narrative=str(item["narrative"]),
                    embedding=embedding,
                    seen_at=item["seen_at"],
                    stats=stats,
                )
                if assigned:
                    continue
        except Exception:
            self._theme_repository.rollback()
            raise

        self._theme_repository.commit()
        return stats

    def _embed_in_chunks(self, texts: list[str]) -> list[list[float]]:
        embeddings: list[list[float]] = []
        for start in range(0, len(texts), MAX_BATCH_SIZE):
            chunk = texts[start : start + MAX_BATCH_SIZE]
            embeddings.extend(self._embedding_service.embed(chunk))
        return embeddings

    def _assign_single_narrative(
        self,
        article_id: Any,
        narrative: str,
        embedding: list[float],
        seen_at: datetime,
        stats: dict[str, int],
    ) -> bool:
        _ = narrative
        primary_assigned = False
        best_theme = self._theme_repository.find_best_theme(embedding)
        if best_theme and float(best_theme["similarity"]) >= self._theme_match_threshold:
            inserted = self._theme_repository.upsert_theme_article_link(
                theme_id=best_theme["id"],
                article_id=article_id,
                similarity_score=float(best_theme["similarity"]),
                matched_at=seen_at,
            )
            self._theme_repository.touch_theme_seen(best_theme["id"], seen_at)
            if inserted:
                self._theme_repository.recompute_theme_article_count(best_theme["id"])
                stats["theme_links_upserted"] += 1
                snapshot_created = self._theme_repository.create_snapshot_if_due(
                    theme_id=best_theme["id"],
                    min_new_articles=self._snapshot_min_new_articles,
                    min_age_hours=self._snapshot_min_age_hours,
                )
                if snapshot_created:
                    stats["theme_snapshots_created"] += 1
            stats["theme_matched_real"] += 1
            primary_assigned = True

        if not primary_assigned:
            best_candidate = self._theme_repository.find_best_candidate(embedding)
            if best_candidate and float(best_candidate["similarity"]) >= self._candidate_match_threshold:
                inserted = self._theme_repository.upsert_candidate_article_link(
                    candidate_theme_id=best_candidate["id"],
                    article_id=article_id,
                    similarity_score=float(best_candidate["similarity"]),
                    matched_at=seen_at,
                )
                self._theme_repository.touch_candidate_seen(best_candidate["id"], seen_at)
                candidate_count = 0
                if inserted:
                    candidate_count = self._theme_repository.recompute_candidate_article_count(best_candidate["id"])
                    stats["candidate_theme_links_upserted"] += 1
                stats["theme_matched_candidate"] += 1
                if inserted and candidate_count >= self._promotion_article_count:
                    promoted = self._theme_repository.promote_candidate(best_candidate["id"])
                    if promoted:
                        stats["theme_candidates_promoted"] += 1
                primary_assigned = True
            else:
                candidate = self._theme_repository.create_or_touch_candidate(
                    title=narrative,
                    title_embedding=embedding,
                    observed_at=seen_at,
                )
                inserted = self._theme_repository.upsert_candidate_article_link(
                    candidate_theme_id=candidate["id"],
                    article_id=article_id,
                    similarity_score=1.0,
                    matched_at=seen_at,
                )
                if inserted:
                    count = self._theme_repository.recompute_candidate_article_count(candidate["id"])
                    stats["candidate_theme_links_upserted"] += 1
                    if count == 1:
                        stats["theme_candidates_created"] += 1
                    if count >= self._promotion_article_count and candidate.get("status") == "candidate":
                        promoted = self._theme_repository.promote_candidate(candidate["id"])
                        if promoted:
                            stats["theme_candidates_promoted"] += 1
                self._theme_repository.touch_candidate_seen(candidate["id"], seen_at)

        self._assign_user_themes(
            article_id=article_id,
            embedding=embedding,
            seen_at=seen_at,
            stats=stats,
        )
        return primary_assigned

    def _assign_user_themes(
        self,
        *,
        article_id: Any,
        embedding: list[float],
        seen_at: datetime,
        stats: dict[str, int],
    ) -> None:
        user_themes = self._theme_repository.find_matching_user_themes(
            embedding=embedding,
            min_similarity=self._user_theme_match_threshold,
            limit=self._user_theme_match_limit,
        )
        if not user_themes:
            return

        for theme in user_themes:
            similarity = float(theme.get("similarity", 0.0))
            inserted = self._theme_repository.upsert_theme_article_link(
                theme_id=theme["id"],
                article_id=article_id,
                similarity_score=similarity,
                matched_at=seen_at,
            )
            self._theme_repository.touch_theme_seen(theme["id"], seen_at)
            stats["user_themes_matched"] += 1
            if not inserted:
                continue

            self._theme_repository.recompute_theme_article_count(theme["id"])
            stats["user_theme_links_upserted"] += 1
            snapshot_created = self._theme_repository.create_snapshot_if_due(
                theme_id=theme["id"],
                min_new_articles=self._snapshot_min_new_articles,
                min_age_hours=self._snapshot_min_age_hours,
            )
            if snapshot_created:
                stats["theme_snapshots_created"] += 1

    @staticmethod
    def _extract_narratives(article: dict[str, Any]) -> list[str]:
        metadata = article.get("metadata")
        if not isinstance(metadata, dict):
            return []
        text_processing = metadata.get("text_processing")
        if not isinstance(text_processing, dict):
            return []
        raw_narratives = text_processing.get("narratives")
        if not isinstance(raw_narratives, list):
            return []
        deduped: list[str] = []
        seen_lower: set[str] = set()
        for value in raw_narratives:
            if not isinstance(value, str):
                continue
            normalized = " ".join(value.split()).strip()
            if not normalized:
                continue
            key = normalized.lower()
            if key in seen_lower:
                continue
            seen_lower.add(key)
            deduped.append(normalized)
        return deduped

    @staticmethod
    def _article_seen_at(article: dict[str, Any]) -> datetime:
        published_at = article.get("published_at")
        if isinstance(published_at, datetime):
            return published_at
        return datetime.now(timezone.utc)
