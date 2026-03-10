"""Pipeline for theme assignment, snapshot refresh, and relation maintenance."""

from __future__ import annotations

from typing import Any

from app.repositories.theme_repository import ThemeRepository
from app.services.theme_management_service import ThemeManagementService


class ThemeManagementPipeline:
    """Operational workflow for maintaining theme state."""

    def __init__(
        self,
        theme_repository: ThemeRepository,
        theme_management_service: ThemeManagementService,
    ) -> None:
        self._theme_repository = theme_repository
        self._theme_management_service = theme_management_service

    def run(
        self,
        *,
        assignment_limit: int = 500,
        assignment_lookback_days: int | None = None,
        snapshot_lookback_days: int = 90,
        relation_lookback_days: int = 45,
        run_maintenance: bool = False,
        rebuild_centroids: bool = False,
        record_run: bool = True,
    ) -> dict[str, Any]:
        try:
            candidate_articles = self._theme_repository.list_unlinked_articles(
                limit=assignment_limit,
                lookback_days=assignment_lookback_days,
            )
        except TypeError:
            # Backward compatibility for simpler repository test doubles.
            candidate_articles = self._theme_repository.list_unlinked_articles(limit=assignment_limit)

        assignment_summary = self._theme_management_service.assign_articles(candidate_articles)
        centroids_rebuilt = 0
        if rebuild_centroids and hasattr(self._theme_repository, "rebuild_theme_centroids"):
            centroids_rebuilt = int(
                self._theme_repository.rebuild_theme_centroids(lookback_days=assignment_lookback_days)
            )

        snapshots_upserted = self._theme_repository.refresh_theme_snapshots(
            lookback_days=snapshot_lookback_days
        )
        relations_upserted = self._theme_repository.rebuild_cooccurrence_relations(
            lookback_days=relation_lookback_days
        )
        status_updates = self._theme_repository.refresh_theme_statuses()

        maintenance_summary = {"recommendations": 0, "merge_recommendations": 0, "split_recommendations": 0}
        if run_maintenance and hasattr(self._theme_management_service, "generate_maintenance_recommendations"):
            maintenance_summary = self._theme_management_service.generate_maintenance_recommendations()

        assignment_input_count = int(assignment_summary.get("input_articles", len(candidate_articles)))
        assigned_articles = int(assignment_summary.get("assigned_articles", 0))
        abstained_articles = int(assignment_summary.get("abstained_articles", 0))
        if assignment_input_count > 0:
            assignment_rate = assigned_articles / assignment_input_count
            abstain_rate = abstained_articles / assignment_input_count
        else:
            assignment_rate = 0.0
            abstain_rate = 0.0

        result = {
            "assignment_input_count": assignment_input_count,
            "assigned_articles": assigned_articles,
            "theme_links_upserted": int(assignment_summary.get("linked_rows", 0)),
            "created_themes": int(assignment_summary.get("created_themes", 0)),
            "promoted_candidates": int(assignment_summary.get("promoted_candidates", 0)),
            "abstained_articles": abstained_articles,
            "abstained_signals": int(assignment_summary.get("abstained_signals", 0)),
            "candidate_observations": int(assignment_summary.get("candidate_observations", 0)),
            "assignment_rate": round(float(assignment_summary.get("assignment_rate", assignment_rate)), 6),
            "abstain_rate": round(float(assignment_summary.get("abstain_rate", abstain_rate)), 6),
            "snapshots_upserted": int(snapshots_upserted),
            "relations_upserted": int(relations_upserted),
            "status_updates": int(status_updates),
            "centroids_rebuilt": int(centroids_rebuilt),
            "recommendation_count": int(maintenance_summary.get("recommendations", 0)),
            "merge_recommendations": int(maintenance_summary.get("merge_recommendations", 0)),
            "split_recommendations": int(maintenance_summary.get("split_recommendations", 0)),
        }

        if record_run and hasattr(self._theme_repository, "record_theme_sync_run"):
            config_payload = {
                "assignment_limit": assignment_limit,
                "assignment_lookback_days": assignment_lookback_days,
                "snapshot_lookback_days": snapshot_lookback_days,
                "relation_lookback_days": relation_lookback_days,
                "run_maintenance": run_maintenance,
                "rebuild_centroids": rebuild_centroids,
            }
            self._theme_repository.record_theme_sync_run(summary=result, config=config_payload)

        return result
