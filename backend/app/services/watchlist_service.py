"""Service layer for user theme watchlist workflows."""

from __future__ import annotations

import os
from typing import Any

from app.repositories.theme_repository import ThemeRepository
from app.repositories.watchlist_repository import WatchlistRepository
from app.services.embedding_service import EmbeddingService


class WatchlistService:
    """Create/list/remove watchlist themes and backfill custom user themes."""

    def __init__(
        self,
        *,
        embedding_service: EmbeddingService | None,
        watchlist_repository: WatchlistRepository,
        theme_repository: ThemeRepository,
        backfill_min_similarity: float | None = None,
        backfill_candidate_min_similarity: float | None = None,
        backfill_source_limit: int | None = None,
    ) -> None:
        self._embedding_service = embedding_service
        self._watchlist_repository = watchlist_repository
        self._theme_repository = theme_repository
        self._backfill_min_similarity = (
            backfill_min_similarity
            if backfill_min_similarity is not None
            else float(os.getenv("WATCHLIST_BACKFILL_MIN_SIMILARITY", "0.72"))
        )
        self._backfill_candidate_min_similarity = (
            backfill_candidate_min_similarity
            if backfill_candidate_min_similarity is not None
            else float(os.getenv("WATCHLIST_BACKFILL_CANDIDATE_MIN_SIMILARITY", "0.72"))
        )
        self._backfill_source_limit = (
            backfill_source_limit
            if backfill_source_limit is not None
            else int(os.getenv("WATCHLIST_BACKFILL_SOURCE_LIMIT", "20"))
        )

    def list_watchlist_themes(self, user_id: Any, limit: int = 50) -> list[dict[str, Any]]:
        return self._watchlist_repository.list_user_watchlist_themes(user_id=user_id, limit=limit)

    def watch_existing_theme(self, user_id: Any, theme_id: Any, alerts_enabled: bool = True) -> dict[str, Any]:
        try:
            theme = self._watchlist_repository.get_theme_for_user(user_id=user_id, theme_id=theme_id)
            if not theme:
                raise ValueError("Theme not found or inaccessible.")

            inserted = self._watchlist_repository.upsert_user_theme_link(
                user_id=user_id,
                theme_id=theme_id,
                alerts_enabled=alerts_enabled,
            )
            self._watchlist_repository.commit()
        except Exception:
            self._watchlist_repository.rollback()
            raise

        return {
            "created_new_theme": False,
            "watchlist_link_created": inserted,
            "theme": self._watchlist_repository.get_theme_for_user(user_id=user_id, theme_id=theme_id),
            "backfill_source_themes_count": 0,
            "backfill_source_candidates_count": 0,
            "backfill_inherited_from_themes": 0,
            "backfill_inherited_from_candidates": 0,
            "snapshot_created": False,
        }

    def create_custom_theme(
        self,
        *,
        user_id: Any,
        canonical_label: str,
        description: str | None = None,
        alerts_enabled: bool = True,
        backfill_min_similarity: float | None = None,
    ) -> dict[str, Any]:
        normalized_label = " ".join((canonical_label or "").split()).strip()
        if not normalized_label:
            raise ValueError("canonical_label must be non-empty.")

        normalized_description = " ".join((description or "").split()).strip() or None
        existing_user_theme = self._watchlist_repository.get_user_owned_theme_by_label(
            user_id=user_id,
            canonical_label=normalized_label,
        )
        if existing_user_theme:
            try:
                inserted = self._watchlist_repository.upsert_user_theme_link(
                    user_id=user_id,
                    theme_id=existing_user_theme["id"],
                    alerts_enabled=alerts_enabled,
                )
                self._watchlist_repository.commit()
            except Exception:
                self._watchlist_repository.rollback()
                raise
            return {
                "created_new_theme": False,
                "watchlist_link_created": inserted,
                "theme": self._watchlist_repository.get_theme_for_user(
                    user_id=user_id,
                    theme_id=existing_user_theme["id"],
                ),
                "backfill_source_themes_count": 0,
                "backfill_source_candidates_count": 0,
                "backfill_inherited_from_themes": 0,
                "backfill_inherited_from_candidates": 0,
                "snapshot_created": False,
            }

        embed_text = normalized_label
        if normalized_description:
            embed_text = f"{normalized_label}\n{normalized_description}"
        if self._embedding_service is None:
            raise ValueError("Embedding service is required to create a custom watchlist theme.")
        embedding = self._embedding_service.embed([embed_text])[0]
        embedding_literal = self._to_vector_literal(embedding)
        min_similarity = (
            backfill_min_similarity
            if backfill_min_similarity is not None
            else self._backfill_min_similarity
        )

        try:
            created_theme = self._watchlist_repository.create_user_theme(
                user_id=user_id,
                canonical_label=normalized_label,
                summary=normalized_description,
                embedding_literal=embedding_literal,
            )
            self._watchlist_repository.upsert_user_theme_link(
                user_id=user_id,
                theme_id=created_theme["id"],
                alerts_enabled=alerts_enabled,
            )

            similar_themes = self._watchlist_repository.find_similar_global_themes(
                embedding_literal=embedding_literal,
                min_similarity=min_similarity,
                limit=self._backfill_source_limit,
                exclude_theme_id=created_theme["id"],
            )
            similar_candidates = self._watchlist_repository.find_similar_candidates(
                embedding_literal=embedding_literal,
                min_similarity=self._backfill_candidate_min_similarity,
                limit=self._backfill_source_limit,
            )

            inherited_from_themes = self._watchlist_repository.inherit_articles_from_themes(
                target_theme_id=created_theme["id"],
                source_theme_ids=[row["id"] for row in similar_themes],
                assignment_method="user_seed_theme_similarity",
            )
            inherited_from_candidates = self._watchlist_repository.inherit_articles_from_candidates(
                target_theme_id=created_theme["id"],
                source_candidate_ids=[row["id"] for row in similar_candidates],
                assignment_method="user_seed_candidate_similarity",
            )

            self._theme_repository.recompute_theme_article_count(created_theme["id"])
            self._theme_repository.refresh_theme_seen_bounds(created_theme["id"])
            summary = self._theme_repository.build_theme_summary(created_theme["id"])
            self._theme_repository.update_theme_summary(created_theme["id"], summary)
            snapshot_created = self._theme_repository.create_snapshot_if_due(
                theme_id=created_theme["id"],
                min_new_articles=1,
                min_age_hours=0,
            )
            self._watchlist_repository.commit()
        except Exception:
            self._watchlist_repository.rollback()
            raise

        return {
            "created_new_theme": True,
            "watchlist_link_created": True,
            "theme": self._watchlist_repository.get_theme_for_user(user_id=user_id, theme_id=created_theme["id"]),
            "backfill_source_themes_count": len(similar_themes),
            "backfill_source_candidates_count": len(similar_candidates),
            "backfill_inherited_from_themes": inherited_from_themes,
            "backfill_inherited_from_candidates": inherited_from_candidates,
            "snapshot_created": bool(snapshot_created),
        }

    def remove_watchlist_theme(self, user_id: Any, theme_id: Any) -> bool:
        try:
            removed = self._watchlist_repository.remove_user_theme_link(user_id=user_id, theme_id=theme_id)
            self._watchlist_repository.commit()
            return removed
        except Exception:
            self._watchlist_repository.rollback()
            raise

    def list_watchlist_theme_articles(
        self,
        *,
        user_id: Any,
        theme_id: Any,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        if not self._watchlist_repository.has_user_theme_link(user_id=user_id, theme_id=theme_id):
            raise ValueError("Watchlist theme not found.")
        return self._watchlist_repository.list_watchlist_theme_articles(
            user_id=user_id,
            theme_id=theme_id,
            limit=limit,
        )

    @staticmethod
    def _to_vector_literal(embedding: list[float]) -> str:
        if not isinstance(embedding, list) or not embedding:
            raise ValueError("embedding must be a non-empty float list")
        return "[" + ",".join(str(float(value)) for value in embedding) + "]"
