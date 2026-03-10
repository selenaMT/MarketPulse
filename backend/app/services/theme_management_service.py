
"""Service layer for theme assignment, ranking, and evolution views."""

from __future__ import annotations

import math
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Any

from app.repositories.theme_repository import ThemeRepository

_NON_ALNUM_PATTERN = re.compile(r"[^a-z0-9\s]+")
_SPACES_PATTERN = re.compile(r"\s+")

DEFAULT_THEME_FUZZY_MATCH_THRESHOLD = 0.2
DEFAULT_THEME_CANDIDATE_CLUSTER_SIMILARITY = 0.6
DEFAULT_THEME_MERGE_SIMILARITY_THRESHOLD = 0.7

DEFAULT_THEME_CANDIDATE_PROMOTION_MIN_ARTICLES = 2
DEFAULT_THEME_CANDIDATE_PROMOTION_MIN_SOURCES = 1
DEFAULT_THEME_CANDIDATE_PROMOTION_MIN_ACTIVE_DAYS = 0
DEFAULT_THEME_CANDIDATE_PROMOTION_MIN_EMBEDDINGS = 0
# Lower than historical default because sparse macro signals often score lower semantically.
DEFAULT_THEME_CANDIDATE_PROMOTION_MIN_COHESION = 0.55


@dataclass(frozen=True)
class ThemeScoreWeights:
    fuzzy: float
    semantic: float
    entity: float
    asset: float
    relationship: float

    @property
    def as_dict(self) -> dict[str, float]:
        return {
            "fuzzy": self.fuzzy,
            "semantic": self.semantic,
            "entity": self.entity,
            "asset": self.asset,
            "relationship": self.relationship,
        }


class ThemeManagementService:
    """Coordinates theme registry updates and read models for APIs."""

    def __init__(
        self,
        theme_repository: ThemeRepository,
        *,
        candidate_promotion_threshold: int | None = None,
        max_signals_per_article: int = 5,
        max_assignments_per_article: int | None = None,
        fuzzy_match_threshold: float = DEFAULT_THEME_FUZZY_MATCH_THRESHOLD,
        fuzzy_token_overlap_threshold: float = 0.65,
        assignment_version: str | None = None,
        assignment_min_score: float | None = None,
        assignment_min_margin: float | None = None,
        semantic_min_similarity: float | None = None,
        semantic_candidate_limit: int | None = None,
        candidate_cluster_similarity_threshold: float | None = None,
        candidate_cluster_entity_overlap_threshold: float | None = None,
        candidate_promotion_min_sources: int | None = None,
        candidate_promotion_min_active_days: int | None = None,
        candidate_promotion_min_cohesion: float | None = None,
        candidate_promotion_min_embeddings: int | None = None,
        merge_similarity_threshold: float | None = None,
        split_min_articles: int | None = None,
        split_low_cohesion_threshold: float | None = None,
        split_stddev_threshold: float | None = None,
    ) -> None:
        self._theme_repository = theme_repository
        self._max_signals_per_article = max(1, int(max_signals_per_article))
        self._max_assignments_per_article = max(
            1,
            int(max_assignments_per_article or self._env_int("THEME_MAX_ASSIGNMENTS_PER_ARTICLE", 2)),
        )
        self._fuzzy_match_threshold = self._bound_float(fuzzy_match_threshold, 0.0, 1.0)
        self._fuzzy_token_overlap_threshold = self._bound_float(
            fuzzy_token_overlap_threshold, 0.0, 1.0
        )

        self._assignment_version = (
            assignment_version or os.getenv("THEME_ASSIGNMENT_VERSION") or "hybrid_v1"
        )
        self._assignment_min_score = self._bound_float(
            assignment_min_score
            if assignment_min_score is not None
            else self._env_float("THEME_ASSIGNMENT_MIN_SCORE", 0.7),
            0.0,
            1.0,
        )
        self._assignment_min_margin = self._bound_float(
            assignment_min_margin
            if assignment_min_margin is not None
            else self._env_float("THEME_ASSIGNMENT_MIN_MARGIN", 0.06),
            0.0,
            1.0,
        )
        self._semantic_min_similarity = self._bound_float(
            semantic_min_similarity
            if semantic_min_similarity is not None
            else self._env_float("THEME_SEMANTIC_MIN_SIMILARITY", 0.3),
            -1.0,
            1.0,
        )
        self._semantic_candidate_limit = max(
            1,
            int(semantic_candidate_limit or self._env_int("THEME_SEMANTIC_CANDIDATE_LIMIT", 12)),
        )
        self._candidate_cluster_similarity_threshold = self._bound_float(
            candidate_cluster_similarity_threshold
            if candidate_cluster_similarity_threshold is not None
            else self._env_float(
                "THEME_CANDIDATE_CLUSTER_SIMILARITY",
                DEFAULT_THEME_CANDIDATE_CLUSTER_SIMILARITY,
            ),
            0.0,
            1.0,
        )
        self._candidate_cluster_entity_overlap_threshold = self._bound_float(
            candidate_cluster_entity_overlap_threshold
            if candidate_cluster_entity_overlap_threshold is not None
            else self._env_float("THEME_CANDIDATE_CLUSTER_ENTITY_OVERLAP", 0.3),
            0.0,
            1.0,
        )
        self._candidate_promotion_min_articles = max(
            1,
            int(
                candidate_promotion_threshold
                or self._env_int(
                    "THEME_CANDIDATE_PROMOTION_MIN_ARTICLES",
                    DEFAULT_THEME_CANDIDATE_PROMOTION_MIN_ARTICLES,
                )
            ),
        )
        self._candidate_promotion_min_sources = max(
            1,
            int(
                candidate_promotion_min_sources
                or self._env_int(
                    "THEME_CANDIDATE_PROMOTION_MIN_SOURCES",
                    DEFAULT_THEME_CANDIDATE_PROMOTION_MIN_SOURCES,
                )
            ),
        )
        self._candidate_promotion_min_active_days = max(
            0,
            int(
                candidate_promotion_min_active_days
                or self._env_int(
                    "THEME_CANDIDATE_PROMOTION_MIN_ACTIVE_DAYS",
                    DEFAULT_THEME_CANDIDATE_PROMOTION_MIN_ACTIVE_DAYS,
                )
            ),
        )
        self._candidate_promotion_min_cohesion = self._bound_float(
            candidate_promotion_min_cohesion
            if candidate_promotion_min_cohesion is not None
            else self._env_float(
                "THEME_CANDIDATE_PROMOTION_MIN_COHESION",
                DEFAULT_THEME_CANDIDATE_PROMOTION_MIN_COHESION,
            ),
            0.0,
            1.0,
        )
        self._candidate_promotion_min_embeddings = max(
            0,
            int(
                candidate_promotion_min_embeddings
                if candidate_promotion_min_embeddings is not None
                else self._env_int(
                    "THEME_CANDIDATE_PROMOTION_MIN_EMBEDDINGS",
                    DEFAULT_THEME_CANDIDATE_PROMOTION_MIN_EMBEDDINGS,
                )
            ),
        )
        self._merge_similarity_threshold = self._bound_float(
            merge_similarity_threshold
            if merge_similarity_threshold is not None
            else self._env_float(
                "THEME_MERGE_SIMILARITY_THRESHOLD",
                DEFAULT_THEME_MERGE_SIMILARITY_THRESHOLD,
            ),
            0.0,
            1.0,
        )
        self._split_min_articles = max(
            2, int(split_min_articles or self._env_int("THEME_SPLIT_MIN_ARTICLES", 12))
        )
        self._split_low_cohesion_threshold = self._bound_float(
            split_low_cohesion_threshold
            if split_low_cohesion_threshold is not None
            else self._env_float("THEME_SPLIT_LOW_COHESION_THRESHOLD", 0.56),
            0.0,
            1.0,
        )
        self._split_stddev_threshold = self._bound_float(
            split_stddev_threshold
            if split_stddev_threshold is not None
            else self._env_float("THEME_SPLIT_STDDEV_THRESHOLD", 0.18),
            0.0,
            1.0,
        )
        self._weights = self._load_score_weights()

    def assign_articles(self, articles: list[dict[str, Any]]) -> dict[str, int | float]:
        theme_index = self._build_theme_indexes()
        candidate_index = self._build_candidate_indexes()
        assigned_articles = 0
        linked_rows = 0
        created_themes = 0
        promoted_candidates = 0
        abstained_articles = 0
        abstained_signals = 0
        candidate_observations = 0

        for article in articles:
            result = self._assign_single_article(
                article,
                theme_index=theme_index,
                candidate_index=candidate_index,
            )
            if int(result["linked_rows"]) > 0:
                assigned_articles += 1
            else:
                abstained_articles += 1
            linked_rows += int(result["linked_rows"])
            created_themes += int(result["created_themes"])
            promoted_candidates += int(result["promoted_candidates"])
            abstained_signals += int(result["abstained_signals"])
            candidate_observations += int(result["candidate_observations"])

        input_articles = len(articles)
        assignment_rate = assigned_articles / input_articles if input_articles else 0.0
        abstain_rate = abstained_articles / input_articles if input_articles else 0.0
        return {
            "input_articles": input_articles,
            "assigned_articles": assigned_articles,
            "linked_rows": linked_rows,
            "created_themes": created_themes,
            "promoted_candidates": promoted_candidates,
            "abstained_articles": abstained_articles,
            "abstained_signals": abstained_signals,
            "candidate_observations": candidate_observations,
            "assignment_rate": round(assignment_rate, 6),
            "abstain_rate": round(abstain_rate, 6),
        }

    def generate_maintenance_recommendations(self) -> dict[str, int]:
        profiles = self._theme_repository.list_theme_profiles_for_maintenance()
        recommendations: list[dict[str, Any]] = []
        merge_lineage_rows: list[dict[str, str]] = []

        for idx, left in enumerate(profiles):
            left_vector = self._normalize_vector(left.get("centroid_embedding"))
            if not left_vector:
                continue
            left_theme_id = str(left["theme_id"])
            left_links = int(left.get("link_count") or 0)
            for right in profiles[idx + 1 :]:
                right_vector = self._normalize_vector(right.get("centroid_embedding"))
                if not right_vector:
                    continue
                right_theme_id = str(right["theme_id"])
                if left_theme_id == right_theme_id:
                    continue
                semantic_similarity = self._cosine_similarity(left_vector, right_vector)
                if semantic_similarity < self._merge_similarity_threshold:
                    continue
                entity_overlap = self._token_overlap(
                    set(self._normalize_str_list(left.get("entity_profile"))),
                    set(self._normalize_str_list(right.get("entity_profile"))),
                )
                confidence = semantic_similarity * 0.88 + entity_overlap * 0.12
                right_links = int(right.get("link_count") or 0)
                source = left if left_links <= right_links else right
                target = right if source is left else left
                recommendations.append(
                    {
                        "recommendation_type": "merge",
                        "source_theme_id": str(source["theme_id"]),
                        "target_theme_id": str(target["theme_id"]),
                        "confidence_score": round(confidence, 6),
                        "status": "suggested",
                        "rationale": (
                            f"Centroids similar ({semantic_similarity:.3f}), entity overlap "
                            f"({entity_overlap:.3f})."
                        ),
                        "payload": {
                            "semantic_similarity": round(semantic_similarity, 6),
                            "entity_overlap": round(entity_overlap, 6),
                        },
                    }
                )
                merge_lineage_rows.append(
                    {
                        "parent_theme_id": str(source["theme_id"]),
                        "child_theme_id": str(target["theme_id"]),
                        "relation_type": "merge_recommended",
                        "note": f"semantic={semantic_similarity:.4f},entity={entity_overlap:.4f}",
                    }
                )

        for row in self._theme_repository.list_theme_cohesion_rows(min_articles=self._split_min_articles):
            avg_similarity = float(row.get("avg_similarity") or 0.0)
            similarity_stddev = float(row.get("similarity_stddev") or 0.0)
            if avg_similarity > self._split_low_cohesion_threshold:
                continue
            if similarity_stddev < self._split_stddev_threshold:
                continue
            confidence = (
                max(self._split_low_cohesion_threshold - avg_similarity, 0.0) * 0.7
                + min(similarity_stddev, 1.0) * 0.3
            )
            recommendations.append(
                {
                    "recommendation_type": "split",
                    "source_theme_id": str(row["theme_id"]),
                    "target_theme_id": None,
                    "confidence_score": round(confidence, 6),
                    "status": "suggested",
                    "rationale": (
                        f"Low cohesion avg={avg_similarity:.3f}, dispersion std={similarity_stddev:.3f}."
                    ),
                    "payload": {
                        "avg_similarity": round(avg_similarity, 6),
                        "similarity_stddev": round(similarity_stddev, 6),
                        "article_count": int(row.get("article_count") or 0),
                    },
                }
            )

        recommendations.sort(key=lambda item: float(item.get("confidence_score") or 0.0), reverse=True)
        inserted = self._theme_repository.replace_theme_maintenance_recommendations(recommendations)
        for row in merge_lineage_rows:
            self._theme_repository.record_theme_lineage(**row)
        return {
            "recommendations": inserted,
            "merge_recommendations": sum(1 for item in recommendations if item["recommendation_type"] == "merge"),
            "split_recommendations": sum(1 for item in recommendations if item["recommendation_type"] == "split"),
        }

    def get_theme_recommendations(
        self,
        *,
        limit: int = 20,
        recommendation_type: str | None = None,
        status: str = "suggested",
    ) -> list[dict[str, Any]]:
        return self._theme_repository.list_theme_maintenance_recommendations(
            limit=limit,
            recommendation_type=recommendation_type,
            status=status,
        )

    def get_hot_themes(self, *, limit: int = 10, lookback_days: int = 30) -> list[dict[str, Any]]:
        metrics = self._theme_repository.list_hot_theme_metrics(lookback_days=lookback_days)
        rows: list[dict[str, Any]] = []
        for row in metrics:
            recency_weighted = float(row.get("recency_weighted_count") or 0.0)
            article_count_3d = int(row.get("article_count_3d") or 0)
            prev_article_count_3d = int(row.get("prev_article_count_3d") or 0)
            acceleration_3d = article_count_3d - prev_article_count_3d
            article_count_7d = int(row.get("article_count_7d") or 0)
            source_diversity = float(row.get("avg_source_count") or 0.0)
            avg_assignment_score = float(row.get("avg_assignment_score") or 0.0)
            hot_score = (
                recency_weighted
                + max(acceleration_3d, 0) * 0.9
                + source_diversity * 0.35
                + avg_assignment_score * 0.75
            )
            rows.append(
                {
                    "theme_id": str(row["theme_id"]),
                    "slug": str(row["slug"]),
                    "canonical_label": str(row["canonical_label"]),
                    "summary": row.get("summary"),
                    "status": str(row["status"]),
                    "discovery_method": str(row["discovery_method"]),
                    "first_seen_at": row.get("first_seen_at"),
                    "last_seen_at": row.get("last_seen_at"),
                    "last_metric_at": row.get("last_metric_at"),
                    "article_count_3d": article_count_3d,
                    "prev_article_count_3d": prev_article_count_3d,
                    "article_count_7d": article_count_7d,
                    "acceleration_3d": acceleration_3d,
                    "source_diversity_score": round(source_diversity, 3),
                    "avg_assignment_score": round(avg_assignment_score, 3),
                    "recency_weighted_count": round(recency_weighted, 3),
                    "hot_score": round(hot_score, 3),
                    "trend": self._resolve_trend(
                        acceleration_3d=acceleration_3d, article_count_7d=article_count_7d
                    ),
                }
            )
        rows.sort(
            key=lambda item: (float(item["hot_score"]), int(item["article_count_7d"])),
            reverse=True,
        )
        return rows[: max(1, int(limit))]

    def get_theme_overview(self, theme_ref: str) -> dict[str, Any] | None:
        theme = self._theme_repository.get_theme_by_ref(theme_ref)
        if theme is None:
            return None
        overview = self._theme_repository.get_theme_overview(str(theme["theme_id"]))
        if overview is None:
            return None
        timeline = self._theme_repository.list_theme_timeline(str(theme["theme_id"]), days=14)
        overview["trend"] = self._trend_from_timeline(timeline)
        return overview

    def get_theme_timeline(self, theme_ref: str, *, days: int) -> list[dict[str, Any]] | None:
        theme = self._theme_repository.get_theme_by_ref(theme_ref)
        if theme is None:
            return None
        return self._theme_repository.list_theme_timeline(str(theme["theme_id"]), days=days)

    def get_related_developments(
        self,
        theme_ref: str,
        *,
        related_limit: int = 8,
        development_limit: int = 10,
    ) -> dict[str, Any] | None:
        theme = self._theme_repository.get_theme_by_ref(theme_ref)
        if theme is None:
            return None
        theme_id = str(theme["theme_id"])
        return {
            "theme_id": theme_id,
            "slug": str(theme["slug"]),
            "canonical_label": str(theme["canonical_label"]),
            "related_themes": self._theme_repository.list_related_themes(theme_id=theme_id, limit=related_limit),
            "developments": self._theme_repository.list_theme_recent_articles(
                theme_id=theme_id,
                limit=development_limit,
            ),
        }

    def _assign_single_article(
        self,
        article: dict[str, Any],
        *,
        theme_index: dict[str, dict[str, Any]],
        candidate_index: dict[str, dict[str, Any]],
    ) -> dict[str, int]:
        metadata = article.get("metadata")
        if not isinstance(metadata, dict):
            return {
                "linked_rows": 0,
                "created_themes": 0,
                "promoted_candidates": 0,
                "abstained_signals": 0,
                "candidate_observations": 0,
            }
        signal_pairs = self._extract_signals(metadata)
        if not signal_pairs:
            return {
                "linked_rows": 0,
                "created_themes": 0,
                "promoted_candidates": 0,
                "abstained_signals": 0,
                "candidate_observations": 0,
            }

        observed_at = self._resolve_observed_at(article)
        context = self._extract_article_context(article)
        semantic_candidates = self._resolve_semantic_candidates(context)
        assigned: dict[str, dict[str, Any]] = {}
        created_themes = 0
        promoted_candidates = 0
        abstained_signals = 0
        candidate_observations = 0

        for signal_text, normalized_signal in signal_pairs:
            best = self._score_best_theme(
                signal_text=signal_text,
                normalized_signal=normalized_signal,
                context=context,
                theme_index=theme_index,
                semantic_candidates=semantic_candidates,
            )
            if best is None:
                promoted = self._cluster_or_promote_candidate(
                    article=article,
                    context=context,
                    signal_text=signal_text,
                    normalized_signal=normalized_signal,
                    observed_at=observed_at,
                    theme_index=theme_index,
                    candidate_index=candidate_index,
                )
                created_themes += int(promoted["created_themes"])
                promoted_candidates += int(promoted["promoted_candidates"])
                candidate_observations += int(promoted["candidate_observations"])
                if promoted.get("theme") is None:
                    abstained_signals += 1
                    continue
                assigned[str(promoted["theme"]["theme_id"])] = promoted["assignment"]
                continue
            theme_id = str(best["theme_id"])
            existing = assigned.get(theme_id)
            if existing is None or float(best["score"]) > float(existing["score"]):
                assigned[theme_id] = best

        if not assigned:
            return {
                "linked_rows": 0,
                "created_themes": created_themes,
                "promoted_candidates": promoted_candidates,
                "abstained_signals": abstained_signals,
                "candidate_observations": candidate_observations,
            }

        ranked = sorted(assigned.items(), key=lambda item: float(item[1]["score"]), reverse=True)[
            : self._max_assignments_per_article
        ]
        for idx, (theme_id, payload) in enumerate(ranked):
            components = payload["components"]
            self._theme_repository.link_article_to_theme(
                article_id=str(article["article_id"]),
                theme_id=theme_id,
                assignment_score=float(payload["score"]),
                assignment_method=str(payload["method"]),
                assignment_version=self._assignment_version,
                assignment_rationale=payload["rationale"],
                alias_score=0.0,
                semantic_score=float(components["semantic"]),
                entity_overlap_score=float(components["entity"]),
                asset_overlap_score=float(components["asset"]),
                relationship_overlap_score=float(components["relationship"]),
                margin_score=float(components["margin"]),
                is_primary=(idx == 0),
                observed_at=observed_at,
                article_embedding=context["embedding"],
                entity_profile=context["entities"],
                asset_profile=context["assets"],
                relationship_profile=context["relationships"],
            )

        return {
            "linked_rows": len(ranked),
            "created_themes": created_themes,
            "promoted_candidates": promoted_candidates,
            "abstained_signals": abstained_signals,
            "candidate_observations": candidate_observations,
        }

    def _resolve_semantic_candidates(self, context: dict[str, Any]) -> dict[str, float]:
        embedding = self._normalize_vector(context.get("embedding"))
        if not embedding:
            return {}
        rows = self._theme_repository.list_nearest_themes(
            embedding=embedding,
            limit=self._semantic_candidate_limit,
            min_similarity=self._semantic_min_similarity,
        )
        return {str(row["theme_id"]): float(row.get("semantic_similarity") or 0.0) for row in rows}

    def _score_best_theme(
        self,
        *,
        signal_text: str,
        normalized_signal: str,
        context: dict[str, Any],
        theme_index: dict[str, dict[str, Any]],
        semantic_candidates: dict[str, float],
    ) -> dict[str, Any] | None:
        fuzzy_theme, fuzzy_score = self._find_fuzzy_theme(normalized_signal, theme_index=theme_index)
        candidate_ids: set[str] = set(semantic_candidates.keys())
        if fuzzy_theme:
            candidate_ids.add(str(fuzzy_theme["theme_id"]))
        if not candidate_ids:
            return None

        ranked: list[dict[str, Any]] = []
        for theme_id in candidate_ids:
            theme = theme_index.get(theme_id)
            if theme is None:
                continue
            fuzzy_match_score = (
                fuzzy_score if fuzzy_theme and str(fuzzy_theme["theme_id"]) == theme_id else 0.0
            )
            semantic_score = float(semantic_candidates.get(theme_id) or 0.0)
            if semantic_score <= 0.0:
                semantic_score = self._cosine_similarity(context["embedding"], theme.get("centroid_embedding"))
            entity_overlap = self._token_overlap(set(context["entities"]), set(theme["entity_profile_set"]))
            asset_overlap = self._token_overlap(set(context["assets"]), set(theme["asset_profile_set"]))
            relationship_overlap = self._token_overlap(
                set(context["relationships"]), set(theme["relationship_profile_set"])
            )
            score = self._score_components(
                fuzzy_match_score=fuzzy_match_score,
                semantic_score=semantic_score,
                entity_overlap=entity_overlap,
                asset_overlap=asset_overlap,
                relationship_overlap=relationship_overlap,
            )
            ranked.append(
                {
                    "theme_id": theme_id,
                    "score": score,
                    "components": {
                        "fuzzy": round(fuzzy_match_score, 6),
                        "semantic": round(semantic_score, 6),
                        "entity": round(entity_overlap, 6),
                        "asset": round(asset_overlap, 6),
                        "relationship": round(relationship_overlap, 6),
                    },
                    "method": f"{self._assignment_version}_{self._resolve_assignment_method(fuzzy_match_score=fuzzy_match_score, semantic_score=semantic_score)}",
                    "rationale": {
                        "signal_text": signal_text,
                        "normalized_signal": normalized_signal,
                        "theme_id": theme_id,
                        "theme_label": theme["canonical_label"],
                        "weights": self._weights.as_dict,
                        "fuzzy_score": round(fuzzy_match_score, 6),
                        "semantic_score": round(semantic_score, 6),
                        "entity_overlap": round(entity_overlap, 6),
                        "asset_overlap": round(asset_overlap, 6),
                        "relationship_overlap": round(relationship_overlap, 6),
                    },
                }
            )

        if not ranked:
            return None
        ranked.sort(key=lambda item: float(item["score"]), reverse=True)
        best = ranked[0]
        second = ranked[1] if len(ranked) > 1 else None
        margin = float(best["score"]) - float(second["score"] if second else 0.0)
        best["components"]["margin"] = round(max(margin, 0.0), 6)
        best["rationale"]["margin_score"] = round(max(margin, 0.0), 6)
        if float(best["score"]) < self._assignment_min_score:
            return None
        if margin < self._assignment_min_margin and float(best["components"]["fuzzy"]) < 0.999:
            return None
        if float(best["components"]["semantic"]) < self._semantic_min_similarity:
            return None
        return best

    def _cluster_or_promote_candidate(
        self,
        *,
        article: dict[str, Any],
        context: dict[str, Any],
        signal_text: str,
        normalized_signal: str,
        observed_at: datetime,
        theme_index: dict[str, dict[str, Any]],
        candidate_index: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        candidate = self._find_candidate_cluster_by_semantics(context=context, candidate_index=candidate_index)
        if candidate is None:
            candidate = self._theme_repository.ensure_theme_candidate(
                display_label=signal_text,
                normalized_label=normalized_signal,
                observed_at=observed_at,
            )

        candidate_id = str(candidate["candidate_id"])
        observed = self._theme_repository.register_theme_candidate_observation(
            candidate_id=candidate_id,
            article_id=str(article["article_id"]),
            source_name=str(article.get("source_name") or "unknown"),
            observed_at=observed_at,
            signal_text=signal_text,
            normalized_signal=normalized_signal,
            entity_names=context["entities"],
            article_embedding=context["embedding"],
        )
        observed_candidate = observed.get("candidate") or candidate
        quality = observed.get("quality") if isinstance(observed.get("quality"), dict) else {}
        candidate_index[candidate_id] = observed_candidate

        promotion = self._promote_candidate_if_ready(
            candidate=observed_candidate,
            quality=quality,
            signal_text=signal_text,
            normalized_signal=normalized_signal,
            observed_at=observed_at,
        )
        if promotion is None:
            return {
                "theme": None,
                "created_themes": 0,
                "promoted_candidates": 0,
                "candidate_observations": 1 if observed.get("observation_inserted") else 0,
            }

        theme = promotion["theme"]
        theme_id = str(theme["theme_id"])
        theme_index[theme_id] = self._with_theme_profile_sets(theme)
        return {
            "theme": theme,
            "created_themes": 1 if promotion.get("created_theme") else 0,
            "promoted_candidates": 1 if promotion.get("promoted_candidate") else 0,
            "candidate_observations": 1 if observed.get("observation_inserted") else 0,
            "assignment": {
                "score": float(promotion.get("score") or 0.74),
                "method": f"{self._assignment_version}_candidate_promotion",
                "components": {
                    "fuzzy": 0.0,
                    "semantic": float(promotion.get("semantic_score") or 0.0),
                    "entity": 0.0,
                    "asset": 0.0,
                    "relationship": 0.0,
                    "margin": 0.0,
                },
                "rationale": promotion.get("rationale")
                if isinstance(promotion.get("rationale"), dict)
                else {},
            },
        }

    def _promote_candidate_if_ready(
        self,
        *,
        candidate: dict[str, Any],
        quality: dict[str, Any],
        signal_text: str,
        normalized_signal: str,
        observed_at: datetime,
    ) -> dict[str, Any] | None:
        promoted_theme_id = candidate.get("promoted_theme_id")
        if candidate.get("status") == "promoted" and promoted_theme_id:
            theme = self._theme_repository.get_theme_by_ref(str(promoted_theme_id))
            if theme is None:
                return None
            return {
                "theme": theme,
                "created_theme": False,
                "promoted_candidate": False,
                "score": 0.72,
                "semantic_score": 0.0,
                "rationale": {"promotion_reason": "already_promoted"},
            }

        article_count = int(quality.get("article_count") or candidate.get("article_count") or 0)
        distinct_sources = int(quality.get("distinct_sources") or 0)
        active_days = int(quality.get("active_days") or 0)
        cohesion_score = float(quality.get("cohesion_score") or 0.0)
        centroid_count = int(quality.get("centroid_count") or candidate.get("centroid_count") or 0)
        if article_count < self._candidate_promotion_min_articles:
            return None
        if distinct_sources < self._candidate_promotion_min_sources:
            return None
        if active_days < self._candidate_promotion_min_active_days:
            return None
        if (
            centroid_count >= self._candidate_promotion_min_embeddings
            and cohesion_score < self._candidate_promotion_min_cohesion
        ):
            return None

        existing_theme = self._theme_repository.get_theme_by_canonical_label(normalized_signal)
        if existing_theme is not None:
            self._theme_repository.mark_candidate_promoted(
                candidate_id=str(candidate["candidate_id"]),
                promoted_theme_id=str(existing_theme["theme_id"]),
            )
            return {
                "theme": existing_theme,
                "created_theme": False,
                "promoted_candidate": True,
                "score": 0.74,
                "semantic_score": cohesion_score,
                "rationale": {
                    "promotion_reason": "matched_existing_theme",
                    "cohesion_score": cohesion_score,
                },
            }

        canonical_label = self._to_display_label(str(candidate.get("display_label") or signal_text))
        created_theme = self._theme_repository.create_theme(
            canonical_label=canonical_label,
            slug_base=self._slugify(canonical_label),
            status="emerging",
            discovery_method="candidate_promotion",
            observed_at=observed_at,
            centroid_embedding=self._normalize_vector(candidate.get("centroid_embedding")),
            entity_profile=self._normalize_str_list(candidate.get("entity_profile")),
            asset_profile=[],
            relationship_profile=[],
        )
        self._theme_repository.mark_candidate_promoted(
            candidate_id=str(candidate["candidate_id"]),
            promoted_theme_id=str(created_theme["theme_id"]),
        )
        return {
            "theme": created_theme,
            "created_theme": True,
            "promoted_candidate": True,
            "score": 0.76,
            "semantic_score": cohesion_score,
            "rationale": {"promotion_reason": "new_theme_created", "cohesion_score": cohesion_score},
        }

    def _find_candidate_cluster_by_semantics(
        self,
        *,
        context: dict[str, Any],
        candidate_index: dict[str, dict[str, Any]],
    ) -> dict[str, Any] | None:
        embedding = self._normalize_vector(context.get("embedding"))
        if not embedding:
            return None
        nearest = self._theme_repository.list_nearest_theme_candidates(
            embedding=embedding,
            limit=8,
            min_similarity=self._candidate_cluster_similarity_threshold,
        )
        if not nearest:
            return None
        entities = set(context["entities"])
        best: dict[str, Any] | None = None
        best_score = 0.0
        for item in nearest:
            semantic_similarity = float(item.get("semantic_similarity") or 0.0)
            candidate_entities = set(self._normalize_str_list(item.get("entity_profile")))
            entity_overlap = self._token_overlap(entities, candidate_entities)
            if entities and candidate_entities and entity_overlap <= 0.0:
                continue
            combined = semantic_similarity * 0.82 + entity_overlap * 0.18
            if combined > best_score:
                best_score = combined
                best = candidate_index.get(str(item["candidate_id"])) or item
        return best

    def _build_theme_indexes(self) -> dict[str, dict[str, Any]]:
        theme_index: dict[str, dict[str, Any]] = {}
        for row in self._theme_repository.list_themes_for_assignment():
            theme_id = str(row["theme_id"])
            theme_payload = self._with_theme_profile_sets(
                {
                    "theme_id": theme_id,
                    "slug": str(row["slug"]),
                    "canonical_label": str(row["canonical_label"]),
                    "summary": row.get("summary"),
                    "status": str(row["status"]),
                    "discovery_method": str(row["discovery_method"]),
                    "first_seen_at": row.get("first_seen_at"),
                    "last_seen_at": row.get("last_seen_at"),
                    "centroid_embedding": self._normalize_vector(row.get("centroid_embedding")),
                    "centroid_count": int(row.get("centroid_count") or 0),
                    "entity_profile": self._normalize_str_list(row.get("entity_profile")),
                    "asset_profile": self._normalize_str_list(row.get("asset_profile")),
                    "relationship_profile": self._normalize_str_list(row.get("relationship_profile")),
                }
            )
            theme_index[theme_id] = theme_payload
        return theme_index

    def _build_candidate_indexes(self) -> dict[str, dict[str, Any]]:
        candidate_index: dict[str, dict[str, Any]] = {}
        for row in self._theme_repository.list_open_theme_candidates():
            candidate_id = str(row["candidate_id"])
            payload = {
                "candidate_id": candidate_id,
                "display_label": row.get("display_label"),
                "normalized_label": row.get("normalized_label"),
                "article_count": int(row.get("article_count") or 0),
                "status": row.get("status"),
                "promoted_theme_id": row.get("promoted_theme_id"),
                "centroid_embedding": self._normalize_vector(row.get("centroid_embedding")),
                "centroid_count": int(row.get("centroid_count") or 0),
                "cohesion_sum": float(row.get("cohesion_sum") or 0.0),
                "cohesion_count": int(row.get("cohesion_count") or 0),
                "entity_profile": self._normalize_str_list(row.get("entity_profile")),
            }
            candidate_index[candidate_id] = payload
        return candidate_index

    def _find_fuzzy_theme(
        self,
        normalized_signal: str,
        *,
        theme_index: dict[str, dict[str, Any]],
    ) -> tuple[dict[str, Any] | None, float]:
        best_theme: dict[str, Any] | None = None
        best_score = 0.0
        signal_tokens = set(normalized_signal.split())
        for theme in theme_index.values():
            normalized_label = self._normalize_label(str(theme["canonical_label"]))
            if not normalized_label:
                continue
            similarity = SequenceMatcher(None, normalized_signal, normalized_label).ratio()
            token_overlap = self._token_overlap(signal_tokens, set(normalized_label.split()))
            combined = max(similarity, token_overlap)
            if combined > best_score:
                best_score = combined
                best_theme = theme
        if best_theme is None:
            return None, 0.0
        if best_score < self._fuzzy_match_threshold and best_score < self._fuzzy_token_overlap_threshold:
            return None, 0.0
        return best_theme, best_score

    def _extract_signals(self, metadata: dict[str, Any]) -> list[tuple[str, str]]:
        text_processing = metadata.get("text_processing")
        values = [
            metadata.get("narratives"),
            metadata.get("macro_signals"),
            text_processing.get("narratives") if isinstance(text_processing, dict) else None,
            text_processing.get("macro_signals") if isinstance(text_processing, dict) else None,
        ]
        raw_signals: list[str] = []
        for value in values:
            if isinstance(value, list):
                raw_signals.extend(
                    signal.strip()
                    for signal in value
                    if isinstance(signal, str) and signal.strip()
                )
        if not raw_signals and isinstance(text_processing, dict):
            fallback_event = text_processing.get("event")
            if isinstance(fallback_event, str) and fallback_event.strip():
                raw_signals.append(fallback_event.strip())
        deduped: list[tuple[str, str]] = []
        seen: set[str] = set()
        for signal in raw_signals:
            normalized = self._normalize_label(signal)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            deduped.append((signal, normalized))
            if len(deduped) >= self._max_signals_per_article:
                break
        return deduped

    def _extract_article_context(self, article: dict[str, Any]) -> dict[str, Any]:
        metadata = article.get("metadata") if isinstance(article.get("metadata"), dict) else {}
        text_processing = (
            metadata.get("text_processing")
            if isinstance(metadata.get("text_processing"), dict)
            else {}
        )
        entities: list[str] = []
        if isinstance(text_processing.get("entities"), list):
            for item in text_processing["entities"]:
                if isinstance(item, dict) and isinstance(item.get("name"), str):
                    entities.append(self._normalize_label(item["name"]))
        assets: list[str] = []
        if isinstance(text_processing.get("asset_impacts"), list):
            for item in text_processing["asset_impacts"]:
                if isinstance(item, dict) and isinstance(item.get("asset"), str):
                    assets.append(self._normalize_label(item["asset"]))
        relationships: list[str] = []
        if isinstance(text_processing.get("relationships"), list):
            for item in text_processing["relationships"]:
                if not isinstance(item, dict):
                    continue
                source = item.get("source")
                relation = item.get("relation")
                target = item.get("target")
                if isinstance(source, str) and isinstance(relation, str) and isinstance(target, str):
                    relationships.append(
                        f"{self._normalize_label(source)}|{self._normalize_label(relation)}|{self._normalize_label(target)}"
                    )
        return {
            "embedding": self._normalize_vector(article.get("embedding")),
            "entities": self._normalize_str_list(entities),
            "assets": self._normalize_str_list(assets),
            "relationships": self._normalize_str_list(relationships),
        }

    @staticmethod
    def _resolve_observed_at(article: dict[str, Any]) -> datetime:
        for field in ("published_at", "created_at"):
            value = article.get(field)
            if isinstance(value, datetime):
                return value
        return datetime.now(timezone.utc)

    @staticmethod
    def _resolve_trend(*, acceleration_3d: int, article_count_7d: int) -> str:
        if article_count_7d <= 0:
            return "inactive"
        if acceleration_3d >= 2:
            return "emerging"
        if acceleration_3d <= -2:
            return "cooling"
        return "steady"

    def _trend_from_timeline(self, timeline: list[dict[str, Any]]) -> str:
        if not timeline:
            return "inactive"
        if len(timeline) == 1:
            return "emerging" if int(timeline[0].get("article_count") or 0) > 0 else "inactive"
        midpoint = len(timeline) // 2
        first_sum = sum(int(row.get("article_count") or 0) for row in timeline[:midpoint])
        second_sum = sum(int(row.get("article_count") or 0) for row in timeline[midpoint:])
        if second_sum - first_sum >= 2:
            return "emerging"
        if second_sum - first_sum <= -2:
            return "cooling"
        return "steady"

    def _score_components(
        self,
        *,
        fuzzy_match_score: float,
        semantic_score: float,
        entity_overlap: float,
        asset_overlap: float,
        relationship_overlap: float,
    ) -> float:
        return round(
            self._bound_float(
                self._weights.fuzzy * fuzzy_match_score
                + self._weights.semantic * semantic_score
                + self._weights.entity * entity_overlap
                + self._weights.asset * asset_overlap
                + self._weights.relationship * relationship_overlap,
                0.0,
                1.0,
            ),
            6,
        )

    def _resolve_assignment_method(self, *, fuzzy_match_score: float, semantic_score: float) -> str:
        if fuzzy_match_score >= self._fuzzy_match_threshold:
            return "fuzzy"
        if semantic_score >= max(self._semantic_min_similarity, 0.5):
            return "semantic"
        return "fuzzy_semantic"

    @staticmethod
    def _normalize_label(label: str) -> str:
        lowered = label.strip().lower()
        lowered = _NON_ALNUM_PATTERN.sub(" ", lowered)
        return _SPACES_PATTERN.sub(" ", lowered).strip()

    @staticmethod
    def _slugify(label: str) -> str:
        return ThemeManagementService._normalize_label(label).replace(" ", "-")

    @staticmethod
    def _to_display_label(raw_signal: str) -> str:
        trimmed = raw_signal.strip()
        if not trimmed:
            return "Untitled Theme"
        if any(char.isupper() for char in trimmed):
            return trimmed
        return " ".join(part.capitalize() for part in trimmed.split())

    @staticmethod
    def _normalize_vector(value: Any) -> list[float] | None:
        if value is None:
            return None
        if isinstance(value, (list, tuple)):
            try:
                return [float(item) for item in value]
            except (TypeError, ValueError):
                return None
        if isinstance(value, str):
            raw = value.strip()
            if not raw.startswith("[") or not raw.endswith("]"):
                return None
            body = raw[1:-1].strip()
            if not body:
                return []
            try:
                return [float(part.strip()) for part in body.split(",") if part.strip()]
            except ValueError:
                return None
        return None

    @staticmethod
    def _normalize_str_list(values: Any) -> list[str]:
        if isinstance(values, list):
            source = values
        elif isinstance(values, tuple):
            source = list(values)
        else:
            return []
        deduped: list[str] = []
        seen: set[str] = set()
        for value in source:
            if not isinstance(value, str):
                continue
            normalized = ThemeManagementService._normalize_label(value)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(normalized)
        return deduped

    @staticmethod
    def _token_overlap(left_tokens: set[str], right_tokens: set[str]) -> float:
        if not left_tokens or not right_tokens:
            return 0.0
        intersection = len(left_tokens.intersection(right_tokens))
        union = len(left_tokens.union(right_tokens))
        return intersection / union if union else 0.0

    @staticmethod
    def _cosine_similarity(left: list[float] | None, right: list[float] | None) -> float:
        if not left or not right:
            return 0.0
        size = min(len(left), len(right))
        if size == 0:
            return 0.0
        numerator = sum(float(left[idx]) * float(right[idx]) for idx in range(size))
        left_norm = math.sqrt(sum(float(left[idx]) * float(left[idx]) for idx in range(size)))
        right_norm = math.sqrt(sum(float(right[idx]) * float(right[idx]) for idx in range(size)))
        denominator = left_norm * right_norm
        return numerator / denominator if denominator > 0 else 0.0

    def _with_theme_profile_sets(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = dict(payload)
        result["entity_profile"] = self._normalize_str_list(result.get("entity_profile"))
        result["asset_profile"] = self._normalize_str_list(result.get("asset_profile"))
        result["relationship_profile"] = self._normalize_str_list(result.get("relationship_profile"))
        result["entity_profile_set"] = set(result["entity_profile"])
        result["asset_profile_set"] = set(result["asset_profile"])
        result["relationship_profile_set"] = set(result["relationship_profile"])
        result["centroid_embedding"] = self._normalize_vector(result.get("centroid_embedding"))
        result["centroid_count"] = int(result.get("centroid_count") or 0)
        return result

    def _load_score_weights(self) -> ThemeScoreWeights:
        values = {
            "fuzzy": self._env_float("THEME_SCORE_WEIGHT_FUZZY", 0.52),
            "semantic": self._env_float("THEME_SCORE_WEIGHT_SEMANTIC", 0.33),
            "entity": self._env_float("THEME_SCORE_WEIGHT_ENTITY", 0.10),
            "asset": self._env_float("THEME_SCORE_WEIGHT_ASSET", 0.03),
            "relationship": self._env_float("THEME_SCORE_WEIGHT_RELATIONSHIP", 0.02),
        }
        total = sum(max(value, 0.0) for value in values.values())
        if total <= 0:
            return ThemeScoreWeights(0.52, 0.33, 0.10, 0.03, 0.02)
        return ThemeScoreWeights(
            fuzzy=values["fuzzy"] / total,
            semantic=values["semantic"] / total,
            entity=values["entity"] / total,
            asset=values["asset"] / total,
            relationship=values["relationship"] / total,
        )

    @staticmethod
    def _env_int(key: str, default: int) -> int:
        raw = os.getenv(key)
        if raw is None or not raw.strip():
            return default
        try:
            return int(raw.strip())
        except ValueError:
            return default

    @staticmethod
    def _env_float(key: str, default: float) -> float:
        raw = os.getenv(key)
        if raw is None or not raw.strip():
            return default
        try:
            return float(raw.strip())
        except ValueError:
            return default

    @staticmethod
    def _bound_float(value: float, lower: float, upper: float) -> float:
        return min(max(float(value), lower), upper)
