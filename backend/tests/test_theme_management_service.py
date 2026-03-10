from __future__ import annotations

from datetime import datetime, timezone

from app.services.theme_management_service import ThemeManagementService


class FakeThemeRepository:
    def __init__(self) -> None:
        self.alias_rows: list[dict] = []
        self.theme_rows: dict[str, dict] = {}
        self.hot_metrics: list[dict] = []
        self.link_calls: list[dict] = []
        self.add_alias_calls: list[dict] = []
        self.create_theme_calls: list[dict] = []
        self.mark_promoted_calls: list[dict] = []
        self.candidate_state: dict[str, dict] = {}
        self.candidate_aliases: dict[str, str] = {}
        self.candidate_observations: dict[str, list[dict]] = {}

    def list_theme_alias_mappings(self) -> list[dict]:
        return list(self.alias_rows)

    def list_theme_candidate_alias_mappings(self) -> list[dict]:
        rows: list[dict] = []
        for normalized_alias, candidate_id in self.candidate_aliases.items():
            candidate = self.candidate_state[candidate_id]
            rows.append(
                {
                    "candidate_id": candidate_id,
                    "alias": candidate.get("display_label"),
                    "normalized_alias": normalized_alias,
                    "display_label": candidate.get("display_label"),
                    "normalized_label": candidate.get("normalized_label"),
                    "article_count": candidate.get("article_count", 0),
                    "status": candidate.get("status", "candidate"),
                    "promoted_theme_id": candidate.get("promoted_theme_id"),
                    "centroid_embedding": candidate.get("centroid_embedding"),
                    "centroid_count": candidate.get("centroid_count", 0),
                    "cohesion_sum": candidate.get("cohesion_sum", 0.0),
                    "cohesion_count": candidate.get("cohesion_count", 0),
                    "entity_profile": candidate.get("entity_profile", []),
                }
            )
        return rows

    def list_nearest_themes(
        self,
        *,
        embedding: list[float],
        limit: int = 12,
        min_similarity: float | None = None,
    ) -> list[dict]:
        return []

    def list_nearest_theme_candidates(
        self,
        *,
        embedding: list[float],
        limit: int = 8,
        min_similarity: float | None = None,
    ) -> list[dict]:
        return []

    def ensure_theme_candidate(
        self,
        *,
        display_label: str,
        normalized_label: str,
        observed_at: datetime | None,
    ) -> dict:
        for candidate_id, candidate in self.candidate_state.items():
            if candidate["normalized_label"] == normalized_label:
                return dict(candidate)
        candidate_id = f"candidate-{len(self.candidate_state) + 1}"
        row = {
            "candidate_id": candidate_id,
            "display_label": display_label,
            "normalized_label": normalized_label,
            "article_count": 0,
            "status": "candidate",
            "promoted_theme_id": None,
            "centroid_embedding": None,
            "centroid_count": 0,
            "cohesion_sum": 0.0,
            "cohesion_count": 0,
            "entity_profile": [],
        }
        self.candidate_state[candidate_id] = row
        return dict(row)

    def add_theme_candidate_alias(self, *, candidate_id: str, alias: str, normalized_alias: str) -> None:
        self.candidate_aliases[normalized_alias] = candidate_id

    def register_theme_candidate_observation(
        self,
        *,
        candidate_id: str,
        article_id: str,
        source_name: str | None,
        observed_at: datetime,
        signal_text: str,
        normalized_signal: str,
        entity_names: list[str] | None,
        article_embedding: list[float] | None,
    ) -> dict:
        candidate = self.candidate_state[candidate_id]
        seen_article_ids = {row["article_id"] for row in self.candidate_observations.get(candidate_id, [])}
        inserted = article_id not in seen_article_ids
        if inserted:
            candidate["article_count"] = int(candidate.get("article_count", 0)) + 1
            self.candidate_observations.setdefault(candidate_id, []).append(
                {
                    "article_id": article_id,
                    "source_name": source_name,
                    "observed_at": observed_at,
                }
            )

        observations = self.candidate_observations.get(candidate_id, [])
        distinct_sources = len({row["source_name"] for row in observations if row.get("source_name")})
        active_days = len({row["observed_at"].date() for row in observations if row.get("observed_at")})
        quality = {
            "article_count": candidate["article_count"],
            "distinct_sources": distinct_sources,
            "active_days": active_days,
            "cohesion_score": 0.9,
            "centroid_count": 2,
        }
        return {
            "candidate": dict(candidate),
            "quality": quality,
            "observation_inserted": inserted,
        }

    def add_theme_alias(self, *, theme_id: str, alias: str, normalized_alias: str, is_primary: bool = False) -> None:
        self.add_alias_calls.append(
            {
                "theme_id": theme_id,
                "alias": alias,
                "normalized_alias": normalized_alias,
                "is_primary": is_primary,
            }
        )
        if not any(row["normalized_alias"] == normalized_alias for row in self.alias_rows):
            theme = self.theme_rows[theme_id]
            self.alias_rows.append(
                {
                    "theme_id": theme_id,
                    "slug": theme["slug"],
                    "canonical_label": theme["canonical_label"],
                    "summary": theme.get("summary"),
                    "status": theme["status"],
                    "discovery_method": theme["discovery_method"],
                    "first_seen_at": theme.get("first_seen_at"),
                    "last_seen_at": theme.get("last_seen_at"),
                    "centroid_embedding": theme.get("centroid_embedding"),
                    "centroid_count": theme.get("centroid_count", 0),
                    "entity_profile": theme.get("entity_profile", []),
                    "asset_profile": theme.get("asset_profile", []),
                    "relationship_profile": theme.get("relationship_profile", []),
                    "alias": alias,
                    "normalized_alias": normalized_alias,
                    "is_primary": is_primary,
                }
            )

    def get_theme_by_ref(self, theme_ref: str) -> dict | None:
        if theme_ref in self.theme_rows:
            return self.theme_rows[theme_ref]
        for row in self.theme_rows.values():
            if row["slug"] == theme_ref:
                return row
        return None

    def get_theme_by_canonical_label(self, normalized_label: str) -> dict | None:
        for row in self.theme_rows.values():
            if row["canonical_label"].strip().lower() == normalized_label:
                return row
        return None

    def create_theme(
        self,
        *,
        canonical_label: str,
        slug_base: str,
        status: str,
        discovery_method: str,
        observed_at: datetime | None,
        centroid_embedding: list[float] | None = None,
        entity_profile: list[str] | None = None,
        asset_profile: list[str] | None = None,
        relationship_profile: list[str] | None = None,
    ) -> dict:
        theme_id = f"theme-{len(self.theme_rows) + 1}"
        row = {
            "theme_id": theme_id,
            "slug": slug_base,
            "canonical_label": canonical_label,
            "summary": None,
            "status": status,
            "discovery_method": discovery_method,
            "first_seen_at": observed_at,
            "last_seen_at": observed_at,
            "centroid_embedding": centroid_embedding,
            "centroid_count": 1 if centroid_embedding else 0,
            "entity_profile": entity_profile or [],
            "asset_profile": asset_profile or [],
            "relationship_profile": relationship_profile or [],
        }
        self.theme_rows[theme_id] = row
        self.create_theme_calls.append(row)
        return row

    def mark_candidate_promoted(self, *, candidate_id: str, promoted_theme_id: str) -> None:
        self.candidate_state[candidate_id]["status"] = "promoted"
        self.candidate_state[candidate_id]["promoted_theme_id"] = promoted_theme_id
        self.mark_promoted_calls.append(
            {
                "candidate_id": candidate_id,
                "promoted_theme_id": promoted_theme_id,
            }
        )

    def link_article_to_theme(self, **kwargs: object) -> None:
        self.link_calls.append(dict(kwargs))

    def list_hot_theme_metrics(self, lookback_days: int) -> list[dict]:
        return list(self.hot_metrics)

    def list_theme_profiles_for_maintenance(self) -> list[dict]:
        return []

    def list_theme_alias_sets(self) -> dict[str, set[str]]:
        return {}

    def list_theme_cohesion_rows(self, *, min_articles: int = 5) -> list[dict]:
        return []

    def replace_theme_maintenance_recommendations(self, recommendations: list[dict]) -> int:
        return len(recommendations)

    def record_theme_lineage(self, **kwargs: object) -> None:
        return None

    def get_theme_overview(self, theme_id: str) -> dict | None:
        return None

    def list_theme_aliases(self, theme_id: str) -> list[str]:
        return []

    def list_theme_timeline(self, theme_id: str, days: int) -> list[dict]:
        return []

    def list_related_themes(self, theme_id: str, limit: int) -> list[dict]:
        return []

    def list_theme_recent_articles(self, theme_id: str, limit: int) -> list[dict]:
        return []


def test_assign_articles_links_existing_alias():
    repository = FakeThemeRepository()
    repository.theme_rows["theme-1"] = {
        "theme_id": "theme-1",
        "slug": "inflation-surprise",
        "canonical_label": "Inflation Surprise",
        "summary": None,
        "status": "active",
        "discovery_method": "seed",
        "first_seen_at": None,
        "last_seen_at": None,
        "centroid_embedding": [0.2, 0.3],
        "centroid_count": 2,
        "entity_profile": [],
        "asset_profile": [],
        "relationship_profile": [],
    }
    repository.alias_rows.append(
        {
            "theme_id": "theme-1",
            "slug": "inflation-surprise",
            "canonical_label": "Inflation Surprise",
            "summary": None,
            "status": "active",
            "discovery_method": "seed",
            "first_seen_at": None,
            "last_seen_at": None,
            "centroid_embedding": [0.2, 0.3],
            "centroid_count": 2,
            "entity_profile": [],
            "asset_profile": [],
            "relationship_profile": [],
            "alias": "Inflation Surprise",
            "normalized_alias": "inflation surprise",
            "is_primary": True,
        }
    )
    service = ThemeManagementService(theme_repository=repository, candidate_promotion_threshold=2)

    summary = service.assign_articles(
        [
            {
                "article_id": "article-1",
                "source_name": "Reuters",
                "published_at": datetime(2026, 3, 10, tzinfo=timezone.utc),
                "created_at": datetime(2026, 3, 10, tzinfo=timezone.utc),
                "embedding": [0.1, 0.2],
                "metadata": {"text_processing": {"narratives": ["Inflation Surprise"], "keep": True}},
            }
        ]
    )

    assert summary["assigned_articles"] == 1
    assert summary["linked_rows"] == 1
    assert repository.link_calls[0]["theme_id"] == "theme-1"
    assert str(repository.link_calls[0]["assignment_method"]).startswith("hybrid_v1_alias_exact")


def test_assign_articles_promotes_candidate_theme_after_quality_gates():
    repository = FakeThemeRepository()
    service = ThemeManagementService(theme_repository=repository, candidate_promotion_threshold=2)
    articles = [
        {
            "article_id": "article-1",
            "source_name": "Reuters",
            "published_at": datetime(2026, 3, 8, tzinfo=timezone.utc),
            "created_at": datetime(2026, 3, 8, tzinfo=timezone.utc),
            "embedding": [0.1, 0.2],
            "metadata": {"text_processing": {"narratives": ["Tariff Escalation"], "keep": True}},
        },
        {
            "article_id": "article-2",
            "source_name": "Bloomberg",
            "published_at": datetime(2026, 3, 9, tzinfo=timezone.utc),
            "created_at": datetime(2026, 3, 9, tzinfo=timezone.utc),
            "embedding": [0.11, 0.21],
            "metadata": {"text_processing": {"narratives": ["Tariff Escalation"], "keep": True}},
        },
    ]

    summary = service.assign_articles(articles)

    assert summary["created_themes"] == 1
    assert summary["promoted_candidates"] == 1
    assert summary["linked_rows"] == 1
    assert len(repository.create_theme_calls) == 1
    assert repository.create_theme_calls[0]["canonical_label"] == "Tariff Escalation"
    assert repository.link_calls[0]["article_id"] == "article-2"


def test_get_hot_themes_orders_by_computed_score():
    repository = FakeThemeRepository()
    repository.hot_metrics = [
        {
            "theme_id": "theme-a",
            "slug": "theme-a",
            "canonical_label": "Theme A",
            "summary": None,
            "status": "active",
            "discovery_method": "seed",
            "first_seen_at": None,
            "last_seen_at": None,
            "last_metric_at": None,
            "recency_weighted_count": 5.0,
            "article_count_3d": 4,
            "prev_article_count_3d": 1,
            "article_count_7d": 7,
            "avg_source_count": 2.0,
            "avg_assignment_score": 0.8,
        },
        {
            "theme_id": "theme-b",
            "slug": "theme-b",
            "canonical_label": "Theme B",
            "summary": None,
            "status": "active",
            "discovery_method": "seed",
            "first_seen_at": None,
            "last_seen_at": None,
            "last_metric_at": None,
            "recency_weighted_count": 6.0,
            "article_count_3d": 2,
            "prev_article_count_3d": 2,
            "article_count_7d": 4,
            "avg_source_count": 1.0,
            "avg_assignment_score": 0.6,
        },
    ]
    service = ThemeManagementService(theme_repository=repository)

    ranked = service.get_hot_themes(limit=2, lookback_days=30)

    assert [row["theme_id"] for row in ranked] == ["theme-a", "theme-b"]
    assert ranked[0]["hot_score"] > ranked[1]["hot_score"]


def test_assign_articles_keeps_macro_signals_backward_compatibility():
    repository = FakeThemeRepository()
    repository.theme_rows["theme-1"] = {
        "theme_id": "theme-1",
        "slug": "fed-policy-shift",
        "canonical_label": "Fed Policy Shift",
        "summary": None,
        "status": "active",
        "discovery_method": "seed",
        "first_seen_at": None,
        "last_seen_at": None,
        "centroid_embedding": [0.2, 0.3],
        "centroid_count": 2,
        "entity_profile": [],
        "asset_profile": [],
        "relationship_profile": [],
    }
    repository.alias_rows.append(
        {
            "theme_id": "theme-1",
            "slug": "fed-policy-shift",
            "canonical_label": "Fed Policy Shift",
            "summary": None,
            "status": "active",
            "discovery_method": "seed",
            "first_seen_at": None,
            "last_seen_at": None,
            "centroid_embedding": [0.2, 0.3],
            "centroid_count": 2,
            "entity_profile": [],
            "asset_profile": [],
            "relationship_profile": [],
            "alias": "Fed Policy Shift",
            "normalized_alias": "fed policy shift",
            "is_primary": True,
        }
    )
    service = ThemeManagementService(theme_repository=repository, candidate_promotion_threshold=2)

    summary = service.assign_articles(
        [
            {
                "article_id": "article-legacy-1",
                "source_name": "Reuters",
                "published_at": datetime(2026, 3, 10, tzinfo=timezone.utc),
                "created_at": datetime(2026, 3, 10, tzinfo=timezone.utc),
                "embedding": [0.1, 0.2],
                "metadata": {"text_processing": {"macro_signals": ["Fed Policy Shift"], "keep": True}},
            }
        ]
    )

    assert summary["assigned_articles"] == 1
    assert summary["linked_rows"] == 1
    assert repository.link_calls[0]["theme_id"] == "theme-1"
