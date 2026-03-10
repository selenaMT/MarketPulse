from __future__ import annotations

from app.pipelines.theme_management_pipeline import ThemeManagementPipeline


class FakeThemeRepository:
    def __init__(self) -> None:
        self.snapshot_lookback_days = None
        self.relation_lookback_days = None

    def list_unlinked_articles(self, limit: int) -> list[dict]:
        return [{"article_id": "a1"}, {"article_id": "a2"}][:limit]

    def refresh_theme_snapshots(self, lookback_days: int) -> int:
        self.snapshot_lookback_days = lookback_days
        return 12

    def rebuild_cooccurrence_relations(self, lookback_days: int) -> int:
        self.relation_lookback_days = lookback_days
        return 5

    def refresh_theme_statuses(self) -> int:
        return 3


class FakeThemeService:
    def assign_articles(self, articles: list[dict]) -> dict[str, int | float]:
        return {
            "input_articles": len(articles),
            "assigned_articles": 1,
            "linked_rows": 2,
            "created_themes": 1,
            "promoted_candidates": 1,
            "abstained_articles": 1,
            "abstained_signals": 3,
            "candidate_observations": 2,
            "assignment_rate": 0.5,
            "abstain_rate": 0.5,
        }


def test_theme_management_pipeline_runs_full_refresh():
    repository = FakeThemeRepository()
    service = FakeThemeService()
    pipeline = ThemeManagementPipeline(
        theme_repository=repository,
        theme_management_service=service,
    )

    result = pipeline.run(
        assignment_limit=10,
        snapshot_lookback_days=120,
        relation_lookback_days=30,
    )

    assert result["assignment_input_count"] == 2
    assert result["assigned_articles"] == 1
    assert result["theme_links_upserted"] == 2
    assert result["created_themes"] == 1
    assert result["promoted_candidates"] == 1
    assert result["abstained_articles"] == 1
    assert result["abstained_signals"] == 3
    assert result["candidate_observations"] == 2
    assert result["assignment_rate"] == 0.5
    assert result["abstain_rate"] == 0.5
    assert result["snapshots_upserted"] == 12
    assert result["relations_upserted"] == 5
    assert result["status_updates"] == 3
    assert result["centroids_rebuilt"] == 0
    assert result["recommendation_count"] == 0
    assert result["merge_recommendations"] == 0
    assert result["split_recommendations"] == 0
    assert repository.snapshot_lookback_days == 120
    assert repository.relation_lookback_days == 30
