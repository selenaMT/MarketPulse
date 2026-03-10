"""Repository for theme management, ranking, and evolution tracking."""

from __future__ import annotations

import json
import math
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session


class ThemeRepository:
    """Database access for theme-centric features."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def list_unlinked_articles(self, limit: int, lookback_days: int | None = None) -> list[dict[str, Any]]:
        """Return processed, keep=true articles that are not linked to any theme yet."""
        if limit <= 0:
            return []

        if lookback_days is not None:
            safe_days = max(1, int(lookback_days))
            query = text(
                """
                select
                  a.id::text as article_id,
                  a.source_name,
                  a.published_at,
                  a.created_at,
                  a.embedding,
                  a.metadata as metadata
                from articles a
                where a.metadata ? 'text_processing'
                  and coalesce((a.metadata->'text_processing'->>'keep')::boolean, true) = true
                  and coalesce(a.published_at, a.created_at) >= now() - make_interval(days => :lookback_days)
                  and not exists (
                    select 1
                    from theme_article_links tal
                    where tal.article_id = a.id
                  )
                order by coalesce(a.published_at, a.created_at) asc, a.id asc
                limit :limit
                """
            )
            params = {"limit": limit, "lookback_days": safe_days}
        else:
            query = text(
                """
                select
                  a.id::text as article_id,
                  a.source_name,
                  a.published_at,
                  a.created_at,
                  a.embedding,
                  a.metadata as metadata
                from articles a
                where a.metadata ? 'text_processing'
                  and coalesce((a.metadata->'text_processing'->>'keep')::boolean, true) = true
                  and not exists (
                    select 1
                    from theme_article_links tal
                    where tal.article_id = a.id
                  )
                order by coalesce(a.published_at, a.created_at) asc, a.id asc
                limit :limit
                """
            )
            params = {"limit": limit}

        rows = self._session.execute(query, params).mappings().all()
        return [
            {
                "article_id": str(row["article_id"]),
                "source_name": str(row["source_name"] or "unknown"),
                "published_at": row["published_at"],
                "created_at": row["created_at"],
                "embedding": self._normalize_vector(row.get("embedding")),
                "metadata": row["metadata"] if isinstance(row["metadata"], dict) else {},
            }
            for row in rows
        ]

    def list_theme_alias_mappings(self) -> list[dict[str, Any]]:
        """Return alias to active-theme mappings."""
        query = text(
            """
            select
              t.id::text as theme_id,
              t.slug,
              t.canonical_label,
              t.summary,
              t.status,
              t.discovery_method,
              t.first_seen_at,
              t.last_seen_at,
              t.centroid_embedding,
              t.centroid_count,
              t.entity_profile,
              t.asset_profile,
              t.relationship_profile,
              ta.alias,
              ta.normalized_alias,
              ta.is_primary
            from theme_aliases ta
            inner join themes t on t.id = ta.theme_id
            where t.status <> 'retired'
            order by t.canonical_label asc, ta.is_primary desc, ta.alias asc
            """
        )
        rows = self._session.execute(query).mappings().all()
        normalized_rows: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            payload["centroid_embedding"] = self._normalize_vector(payload.get("centroid_embedding"))
            payload["centroid_count"] = int(payload.get("centroid_count") or 0)
            payload["entity_profile"] = self._normalize_str_list(payload.get("entity_profile"))
            payload["asset_profile"] = self._normalize_str_list(payload.get("asset_profile"))
            payload["relationship_profile"] = self._normalize_str_list(payload.get("relationship_profile"))
            normalized_rows.append(payload)
        return normalized_rows

    def list_themes_for_assignment(self) -> list[dict[str, Any]]:
        """Return active theme profiles for fuzzy + semantic assignment."""
        query = text(
            """
            select
              t.id::text as theme_id,
              t.slug,
              t.canonical_label,
              t.summary,
              t.status,
              t.discovery_method,
              t.first_seen_at,
              t.last_seen_at,
              t.centroid_embedding,
              t.centroid_count,
              t.entity_profile,
              t.asset_profile,
              t.relationship_profile
            from themes t
            where t.status <> 'retired'
            order by t.canonical_label asc
            """
        )
        rows = self._session.execute(query).mappings().all()
        normalized_rows: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            payload["centroid_embedding"] = self._normalize_vector(payload.get("centroid_embedding"))
            payload["centroid_count"] = int(payload.get("centroid_count") or 0)
            payload["entity_profile"] = self._normalize_str_list(payload.get("entity_profile"))
            payload["asset_profile"] = self._normalize_str_list(payload.get("asset_profile"))
            payload["relationship_profile"] = self._normalize_str_list(payload.get("relationship_profile"))
            normalized_rows.append(payload)
        return normalized_rows

    def get_theme_by_ref(self, theme_ref: str) -> dict[str, Any] | None:
        """Resolve a theme by UUID or slug."""
        theme_uuid = self._parse_uuid(theme_ref)
        if theme_uuid is not None:
            query = text(
                """
                select
                  t.id::text as theme_id,
                  t.slug,
                  t.canonical_label,
                  t.summary,
                  t.status,
                  t.discovery_method,
                  t.first_seen_at,
                  t.last_seen_at,
                  t.centroid_embedding,
                  t.centroid_count,
                  t.entity_profile,
                  t.asset_profile,
                  t.relationship_profile,
                  t.created_at,
                  t.updated_at
                from themes t
                where t.id = :theme_id
                """
            )
            row = self._session.execute(query, {"theme_id": str(theme_uuid)}).mappings().first()
            if row is None:
                return None
            payload = dict(row)
            payload["centroid_embedding"] = self._normalize_vector(payload.get("centroid_embedding"))
            payload["centroid_count"] = int(payload.get("centroid_count") or 0)
            payload["entity_profile"] = self._normalize_str_list(payload.get("entity_profile"))
            payload["asset_profile"] = self._normalize_str_list(payload.get("asset_profile"))
            payload["relationship_profile"] = self._normalize_str_list(payload.get("relationship_profile"))
            return payload

        query = text(
            """
            select
              t.id::text as theme_id,
              t.slug,
              t.canonical_label,
              t.summary,
              t.status,
              t.discovery_method,
              t.first_seen_at,
              t.last_seen_at,
              t.centroid_embedding,
              t.centroid_count,
              t.entity_profile,
              t.asset_profile,
              t.relationship_profile,
              t.created_at,
              t.updated_at
            from themes t
            where lower(t.slug) = lower(:theme_slug)
            """
        )
        row = self._session.execute(query, {"theme_slug": theme_ref.strip()}).mappings().first()
        if row is None:
            return None
        payload = dict(row)
        payload["centroid_embedding"] = self._normalize_vector(payload.get("centroid_embedding"))
        payload["centroid_count"] = int(payload.get("centroid_count") or 0)
        payload["entity_profile"] = self._normalize_str_list(payload.get("entity_profile"))
        payload["asset_profile"] = self._normalize_str_list(payload.get("asset_profile"))
        payload["relationship_profile"] = self._normalize_str_list(payload.get("relationship_profile"))
        return payload

    def get_theme_by_alias(self, normalized_alias: str) -> dict[str, Any] | None:
        """Return theme row for a normalized alias when present."""
        query = text(
            """
            select
              t.id::text as theme_id,
              t.slug,
              t.canonical_label,
              t.summary,
              t.status,
              t.discovery_method,
              t.first_seen_at,
              t.last_seen_at,
              t.centroid_embedding,
              t.centroid_count,
              t.entity_profile,
              t.asset_profile,
              t.relationship_profile
            from theme_aliases ta
            inner join themes t on t.id = ta.theme_id
            where ta.normalized_alias = :normalized_alias
            """
        )
        row = self._session.execute(query, {"normalized_alias": normalized_alias}).mappings().first()
        if row is None:
            return None
        payload = dict(row)
        payload["centroid_embedding"] = self._normalize_vector(payload.get("centroid_embedding"))
        payload["centroid_count"] = int(payload.get("centroid_count") or 0)
        payload["entity_profile"] = self._normalize_str_list(payload.get("entity_profile"))
        payload["asset_profile"] = self._normalize_str_list(payload.get("asset_profile"))
        payload["relationship_profile"] = self._normalize_str_list(payload.get("relationship_profile"))
        return payload

    def get_theme_by_canonical_label(self, normalized_label: str) -> dict[str, Any] | None:
        """Return existing theme for canonical label (case-insensitive)."""
        query = text(
            """
            select
              t.id::text as theme_id,
              t.slug,
              t.canonical_label,
              t.summary,
              t.status,
              t.discovery_method,
              t.first_seen_at,
              t.last_seen_at,
              t.centroid_embedding,
              t.centroid_count,
              t.entity_profile,
              t.asset_profile,
              t.relationship_profile
            from themes t
            where lower(t.canonical_label) = :normalized_label
            """
        )
        row = self._session.execute(query, {"normalized_label": normalized_label}).mappings().first()
        if row is None:
            return None
        payload = dict(row)
        payload["centroid_embedding"] = self._normalize_vector(payload.get("centroid_embedding"))
        payload["centroid_count"] = int(payload.get("centroid_count") or 0)
        payload["entity_profile"] = self._normalize_str_list(payload.get("entity_profile"))
        payload["asset_profile"] = self._normalize_str_list(payload.get("asset_profile"))
        payload["relationship_profile"] = self._normalize_str_list(payload.get("relationship_profile"))
        return payload

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
    ) -> dict[str, Any]:
        """Create and return a new theme row."""
        slug = self._build_unique_slug(slug_base)
        observed_time = observed_at or datetime.now(timezone.utc)
        centroid_vector = self._normalize_vector(centroid_embedding)
        centroid_count = 1 if centroid_vector else 0
        entity_values = self._normalize_str_list(entity_profile)
        asset_values = self._normalize_str_list(asset_profile)
        relationship_values = self._normalize_str_list(relationship_profile)
        query = text(
            """
            insert into themes (
              slug,
              canonical_label,
              status,
              discovery_method,
              first_seen_at,
              last_seen_at,
              centroid_embedding,
              centroid_count,
              centroid_updated_at,
              entity_profile,
              asset_profile,
              relationship_profile,
              profile_updated_at,
              created_at,
              updated_at
            )
            values (
              :slug,
              :canonical_label,
              :status,
              :discovery_method,
              :observed_at,
              :observed_at,
              cast(:centroid_embedding as vector),
              :centroid_count,
              :centroid_updated_at,
              cast(:entity_profile as jsonb),
              cast(:asset_profile as jsonb),
              cast(:relationship_profile as jsonb),
              :profile_updated_at,
              now(),
              now()
            )
            returning
              id::text as theme_id,
              slug,
              canonical_label,
              summary,
              status,
              discovery_method,
              first_seen_at,
              last_seen_at,
              centroid_embedding,
              centroid_count,
              entity_profile,
              asset_profile,
              relationship_profile,
              created_at,
              updated_at
            """
        )
        row = self._session.execute(
            query,
            {
                "slug": slug,
                "canonical_label": canonical_label,
                "status": status,
                "discovery_method": discovery_method,
                "observed_at": observed_time,
                "centroid_embedding": self._to_pgvector_literal(centroid_vector),
                "centroid_count": centroid_count,
                "centroid_updated_at": observed_time if centroid_vector else None,
                "entity_profile": self._to_json(entity_values),
                "asset_profile": self._to_json(asset_values),
                "relationship_profile": self._to_json(relationship_values),
                "profile_updated_at": observed_time,
            },
        ).mappings().one()
        self._session.commit()
        payload = dict(row)
        payload["centroid_embedding"] = self._normalize_vector(payload.get("centroid_embedding"))
        payload["centroid_count"] = int(payload.get("centroid_count") or 0)
        payload["entity_profile"] = self._normalize_str_list(payload.get("entity_profile"))
        payload["asset_profile"] = self._normalize_str_list(payload.get("asset_profile"))
        payload["relationship_profile"] = self._normalize_str_list(payload.get("relationship_profile"))
        return payload

    def add_theme_alias(
        self,
        *,
        theme_id: str,
        alias: str,
        normalized_alias: str,
        is_primary: bool = False,
    ) -> None:
        """Attach alias to theme; idempotent on normalized alias."""
        query = text(
            """
            insert into theme_aliases (
              theme_id,
              alias,
              normalized_alias,
              is_primary,
              created_at,
              updated_at
            )
            values (
              :theme_id,
              :alias,
              :normalized_alias,
              :is_primary,
              now(),
              now()
            )
            on conflict (normalized_alias) do nothing
            """
        )
        self._session.execute(
            query,
            {
                "theme_id": theme_id,
                "alias": alias,
                "normalized_alias": normalized_alias,
                "is_primary": is_primary,
            },
        )
        self._session.commit()

    def upsert_theme_candidate(
        self,
        *,
        display_label: str,
        normalized_label: str,
        observed_at: datetime | None,
    ) -> dict[str, Any]:
        """Increment candidate occurrence and return its latest state."""
        observed_time = observed_at or datetime.now(timezone.utc)
        query = text(
            """
            insert into theme_candidates (
              display_label,
              normalized_label,
              article_count,
              status,
              first_seen_at,
              last_seen_at,
              created_at,
              updated_at
            )
            values (
              :display_label,
              :normalized_label,
              1,
              'candidate',
              :observed_at,
              :observed_at,
              now(),
              now()
            )
            on conflict (normalized_label) do update
            set
              article_count = theme_candidates.article_count + 1,
              display_label = case
                when coalesce(theme_candidates.display_label, '') = ''
                then excluded.display_label
                else theme_candidates.display_label
              end,
              status = case
                when theme_candidates.status = 'discarded' then 'candidate'
                else theme_candidates.status
              end,
              last_seen_at = greatest(theme_candidates.last_seen_at, excluded.last_seen_at),
              updated_at = now()
            returning
              id::text as candidate_id,
              display_label,
              normalized_label,
              article_count,
              status,
              promoted_theme_id::text as promoted_theme_id,
              first_seen_at,
              last_seen_at
            """
        )
        row = self._session.execute(
            query,
            {
                "display_label": display_label,
                "normalized_label": normalized_label,
                "observed_at": observed_time,
            },
        ).mappings().one()
        self._session.commit()
        return dict(row)

    def ensure_theme_candidate(
        self,
        *,
        display_label: str,
        normalized_label: str,
        observed_at: datetime | None,
    ) -> dict[str, Any]:
        """Create candidate row if missing and return current state without incrementing counts."""
        observed_time = observed_at or datetime.now(timezone.utc)
        query = text(
            """
            insert into theme_candidates (
              display_label,
              normalized_label,
              article_count,
              status,
              first_seen_at,
              last_seen_at,
              created_at,
              updated_at
            )
            values (
              :display_label,
              :normalized_label,
              0,
              'candidate',
              :observed_at,
              :observed_at,
              now(),
              now()
            )
            on conflict (normalized_label) do update
            set
              display_label = case
                when coalesce(theme_candidates.display_label, '') = '' then excluded.display_label
                else theme_candidates.display_label
              end,
              status = case
                when theme_candidates.status = 'discarded' then 'candidate'
                else theme_candidates.status
              end,
              last_seen_at = greatest(theme_candidates.last_seen_at, excluded.last_seen_at),
              updated_at = now()
            returning
              id::text as candidate_id,
              display_label,
              normalized_label,
              article_count,
              status,
              promoted_theme_id::text as promoted_theme_id,
              first_seen_at,
              last_seen_at,
              centroid_embedding,
              centroid_count,
              cohesion_sum,
              cohesion_count,
              entity_profile
            """
        )
        row = self._session.execute(
            query,
            {
                "display_label": display_label,
                "normalized_label": normalized_label,
                "observed_at": observed_time,
            },
        ).mappings().one()
        self._session.commit()
        payload = dict(row)
        payload["centroid_embedding"] = self._normalize_vector(payload.get("centroid_embedding"))
        payload["centroid_count"] = int(payload.get("centroid_count") or 0)
        payload["cohesion_sum"] = float(payload.get("cohesion_sum") or 0.0)
        payload["cohesion_count"] = int(payload.get("cohesion_count") or 0)
        payload["entity_profile"] = self._normalize_str_list(payload.get("entity_profile"))
        return payload

    def get_theme_candidate_by_id(self, candidate_id: str) -> dict[str, Any] | None:
        query = text(
            """
            select
              c.id::text as candidate_id,
              c.display_label,
              c.normalized_label,
              c.article_count,
              c.status,
              c.promoted_theme_id::text as promoted_theme_id,
              c.first_seen_at,
              c.last_seen_at,
              c.centroid_embedding,
              c.centroid_count,
              c.cohesion_sum,
              c.cohesion_count,
              c.entity_profile
            from theme_candidates c
            where c.id = :candidate_id
            """
        )
        row = self._session.execute(query, {"candidate_id": candidate_id}).mappings().first()
        if row is None:
            return None
        payload = dict(row)
        payload["centroid_embedding"] = self._normalize_vector(payload.get("centroid_embedding"))
        payload["centroid_count"] = int(payload.get("centroid_count") or 0)
        payload["cohesion_sum"] = float(payload.get("cohesion_sum") or 0.0)
        payload["cohesion_count"] = int(payload.get("cohesion_count") or 0)
        payload["entity_profile"] = self._normalize_str_list(payload.get("entity_profile"))
        return payload

    def get_theme_candidate_by_alias(self, normalized_alias: str) -> dict[str, Any] | None:
        query = text(
            """
            select
              c.id::text as candidate_id,
              c.display_label,
              c.normalized_label,
              c.article_count,
              c.status,
              c.promoted_theme_id::text as promoted_theme_id,
              c.first_seen_at,
              c.last_seen_at,
              c.centroid_embedding,
              c.centroid_count,
              c.cohesion_sum,
              c.cohesion_count,
              c.entity_profile
            from theme_candidate_aliases ca
            inner join theme_candidates c on c.id = ca.candidate_id
            where ca.normalized_alias = :normalized_alias
              and c.status in ('candidate', 'promoted')
            """
        )
        row = self._session.execute(query, {"normalized_alias": normalized_alias}).mappings().first()
        if row is None:
            return None
        payload = dict(row)
        payload["centroid_embedding"] = self._normalize_vector(payload.get("centroid_embedding"))
        payload["centroid_count"] = int(payload.get("centroid_count") or 0)
        payload["cohesion_sum"] = float(payload.get("cohesion_sum") or 0.0)
        payload["cohesion_count"] = int(payload.get("cohesion_count") or 0)
        payload["entity_profile"] = self._normalize_str_list(payload.get("entity_profile"))
        return payload

    def add_theme_candidate_alias(
        self,
        *,
        candidate_id: str,
        alias: str,
        normalized_alias: str,
    ) -> None:
        query = text(
            """
            insert into theme_candidate_aliases (
              candidate_id,
              alias,
              normalized_alias,
              created_at,
              updated_at
            )
            values (
              :candidate_id,
              :alias,
              :normalized_alias,
              now(),
              now()
            )
            on conflict (normalized_alias) do nothing
            """
        )
        self._session.execute(
            query,
            {
                "candidate_id": candidate_id,
                "alias": alias,
                "normalized_alias": normalized_alias,
            },
        )
        self._session.commit()

    def list_theme_candidate_alias_mappings(self) -> list[dict[str, Any]]:
        query = text(
            """
            select
              ca.candidate_id::text as candidate_id,
              ca.alias,
              ca.normalized_alias,
              c.display_label,
              c.normalized_label,
              c.article_count,
              c.status,
              c.promoted_theme_id::text as promoted_theme_id,
              c.first_seen_at,
              c.last_seen_at,
              c.centroid_embedding,
              c.centroid_count,
              c.cohesion_sum,
              c.cohesion_count,
              c.entity_profile
            from theme_candidate_aliases ca
            inner join theme_candidates c on c.id = ca.candidate_id
            where c.status in ('candidate', 'promoted')
            order by c.article_count desc, c.last_seen_at desc
            """
        )
        rows = self._session.execute(query).mappings().all()
        payloads: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            payload["centroid_embedding"] = self._normalize_vector(payload.get("centroid_embedding"))
            payload["centroid_count"] = int(payload.get("centroid_count") or 0)
            payload["cohesion_sum"] = float(payload.get("cohesion_sum") or 0.0)
            payload["cohesion_count"] = int(payload.get("cohesion_count") or 0)
            payload["entity_profile"] = self._normalize_str_list(payload.get("entity_profile"))
            payloads.append(payload)
        return payloads

    def list_open_theme_candidates(self, *, limit: int = 2000) -> list[dict[str, Any]]:
        resolved_limit = max(1, int(limit))
        query = text(
            """
            select
              c.id::text as candidate_id,
              c.display_label,
              c.normalized_label,
              c.article_count,
              c.status,
              c.promoted_theme_id::text as promoted_theme_id,
              c.first_seen_at,
              c.last_seen_at,
              c.centroid_embedding,
              c.centroid_count,
              c.cohesion_sum,
              c.cohesion_count,
              c.entity_profile
            from theme_candidates c
            where c.status in ('candidate', 'promoted')
            order by c.article_count desc, c.last_seen_at desc
            limit :limit
            """
        )
        rows = self._session.execute(query, {"limit": resolved_limit}).mappings().all()
        payloads: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            payload["centroid_embedding"] = self._normalize_vector(payload.get("centroid_embedding"))
            payload["centroid_count"] = int(payload.get("centroid_count") or 0)
            payload["cohesion_sum"] = float(payload.get("cohesion_sum") or 0.0)
            payload["cohesion_count"] = int(payload.get("cohesion_count") or 0)
            payload["entity_profile"] = self._normalize_str_list(payload.get("entity_profile"))
            payloads.append(payload)
        return payloads

    def list_nearest_themes(
        self,
        *,
        embedding: list[float],
        limit: int = 12,
        min_similarity: float | None = None,
    ) -> list[dict[str, Any]]:
        vector_literal = self._to_pgvector_literal(embedding)
        if not vector_literal:
            return []
        resolved_limit = max(1, int(limit))
        query = text(
            """
            select
              t.id::text as theme_id,
              (1 - (t.centroid_embedding <=> cast(:embedding as vector)))::double precision as semantic_similarity
            from themes t
            where t.status <> 'retired'
              and t.centroid_embedding is not null
            order by t.centroid_embedding <=> cast(:embedding as vector)
            limit :limit
            """
        )
        rows = self._session.execute(
            query,
            {
                "embedding": vector_literal,
                "limit": resolved_limit,
            },
        ).mappings().all()
        result: list[dict[str, Any]] = []
        for row in rows:
            similarity = float(row.get("semantic_similarity") or 0.0)
            if min_similarity is not None and similarity < float(min_similarity):
                continue
            result.append(
                {
                    "theme_id": str(row["theme_id"]),
                    "semantic_similarity": similarity,
                }
            )
        return result

    def list_nearest_theme_candidates(
        self,
        *,
        embedding: list[float],
        limit: int = 8,
        min_similarity: float | None = None,
    ) -> list[dict[str, Any]]:
        vector_literal = self._to_pgvector_literal(embedding)
        if not vector_literal:
            return []
        resolved_limit = max(1, int(limit))
        query = text(
            """
            select
              c.id::text as candidate_id,
              c.display_label,
              c.normalized_label,
              c.article_count,
              c.status,
              c.promoted_theme_id::text as promoted_theme_id,
              c.centroid_count,
              c.cohesion_sum,
              c.cohesion_count,
              c.entity_profile,
              (1 - (c.centroid_embedding <=> cast(:embedding as vector)))::double precision as semantic_similarity
            from theme_candidates c
            where c.status in ('candidate', 'promoted')
              and c.centroid_embedding is not null
            order by c.centroid_embedding <=> cast(:embedding as vector)
            limit :limit
            """
        )
        rows = self._session.execute(
            query,
            {
                "embedding": vector_literal,
                "limit": resolved_limit,
            },
        ).mappings().all()
        payloads: list[dict[str, Any]] = []
        for row in rows:
            similarity = float(row.get("semantic_similarity") or 0.0)
            if min_similarity is not None and similarity < float(min_similarity):
                continue
            payload = dict(row)
            payload["semantic_similarity"] = similarity
            payload["centroid_count"] = int(payload.get("centroid_count") or 0)
            payload["cohesion_sum"] = float(payload.get("cohesion_sum") or 0.0)
            payload["cohesion_count"] = int(payload.get("cohesion_count") or 0)
            payload["entity_profile"] = self._normalize_str_list(payload.get("entity_profile"))
            payloads.append(payload)
        return payloads

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
    ) -> dict[str, Any]:
        entity_profile_values = self._normalize_str_list(entity_names)
        insert_query = text(
            """
            insert into theme_candidate_observations (
              candidate_id,
              article_id,
              source_name,
              observed_at,
              signal_text,
              normalized_signal,
              entity_names,
              created_at
            )
            values (
              :candidate_id,
              :article_id,
              :source_name,
              :observed_at,
              :signal_text,
              :normalized_signal,
              cast(:entity_names as jsonb),
              now()
            )
            on conflict (candidate_id, article_id) do nothing
            returning id
            """
        )
        inserted = (
            self._session.execute(
                insert_query,
                {
                    "candidate_id": candidate_id,
                    "article_id": article_id,
                    "source_name": source_name,
                    "observed_at": observed_at,
                    "signal_text": signal_text,
                    "normalized_signal": normalized_signal,
                    "entity_names": self._to_json(entity_profile_values),
                },
            ).scalar_one_or_none()
            is not None
        )

        if inserted:
            self._session.execute(
                text(
                    """
                    update theme_candidates
                    set
                      article_count = article_count + 1,
                      first_seen_at = case
                        when first_seen_at is null then :observed_at
                        else least(first_seen_at, :observed_at)
                      end,
                      last_seen_at = case
                        when last_seen_at is null then :observed_at
                        else greatest(last_seen_at, :observed_at)
                      end,
                      updated_at = now()
                    where id = :candidate_id
                    """
                ),
                {
                    "candidate_id": candidate_id,
                    "observed_at": observed_at,
                },
            )

            if entity_profile_values:
                existing = self.get_theme_candidate_by_id(candidate_id)
                existing_profile = self._normalize_str_list(existing.get("entity_profile") if existing else None)
                merged_profile = self._merge_profiles(existing_profile, entity_profile_values)
                self._session.execute(
                    text(
                        """
                        update theme_candidates
                        set
                          entity_profile = cast(:entity_profile as jsonb),
                          updated_at = now()
                        where id = :candidate_id
                        """
                    ),
                    {
                        "candidate_id": candidate_id,
                        "entity_profile": self._to_json(merged_profile),
                    },
                )

            article_vector = self._normalize_vector(article_embedding)
            if article_vector:
                state = self.get_theme_candidate_by_id(candidate_id)
                state_vector = self._normalize_vector(state.get("centroid_embedding") if state else None)
                state_count = int(state.get("centroid_count") or 0) if state else 0
                cohesion_sum = float(state.get("cohesion_sum") or 0.0) if state else 0.0
                cohesion_count = int(state.get("cohesion_count") or 0) if state else 0
                similarity = (
                    self._cosine_similarity(state_vector, article_vector) if state_count > 0 and state_vector else None
                )
                next_vector = self._blend_centroid(state_vector, state_count, article_vector)
                next_count = state_count + 1
                next_cohesion_sum = cohesion_sum + (similarity if similarity is not None else 0.0)
                next_cohesion_count = cohesion_count + (1 if similarity is not None else 0)
                self._session.execute(
                    text(
                        """
                        update theme_candidates
                        set
                          centroid_embedding = cast(:centroid_embedding as vector),
                          centroid_count = :centroid_count,
                          cohesion_sum = :cohesion_sum,
                          cohesion_count = :cohesion_count,
                          updated_at = now()
                        where id = :candidate_id
                        """
                    ),
                    {
                        "candidate_id": candidate_id,
                        "centroid_embedding": self._to_pgvector_literal(next_vector),
                        "centroid_count": next_count,
                        "cohesion_sum": next_cohesion_sum,
                        "cohesion_count": next_cohesion_count,
                    },
                )

        self._session.commit()
        candidate = self.get_theme_candidate_by_id(candidate_id)
        quality = self.get_theme_candidate_quality(candidate_id)
        return {
            "candidate": candidate,
            "quality": quality,
            "observation_inserted": inserted,
        }

    def get_theme_candidate_quality(self, candidate_id: str) -> dict[str, Any]:
        query = text(
            """
            select
              c.id::text as candidate_id,
              c.article_count,
              c.centroid_count,
              c.cohesion_sum,
              c.cohesion_count,
              coalesce(count(distinct nullif(coalesce(o.source_name, ''), '')), 0)::int as distinct_sources,
              coalesce(count(distinct date_trunc('day', o.observed_at)), 0)::int as active_days
            from theme_candidates c
            left join theme_candidate_observations o on o.candidate_id = c.id
            where c.id = :candidate_id
            group by c.id, c.article_count, c.centroid_count, c.cohesion_sum, c.cohesion_count
            """
        )
        row = self._session.execute(query, {"candidate_id": candidate_id}).mappings().first()
        if row is None:
            return {
                "candidate_id": candidate_id,
                "article_count": 0,
                "distinct_sources": 0,
                "active_days": 0,
                "centroid_count": 0,
                "cohesion_score": 0.0,
            }
        cohesion_sum = float(row.get("cohesion_sum") or 0.0)
        cohesion_count = int(row.get("cohesion_count") or 0)
        cohesion_score = cohesion_sum / cohesion_count if cohesion_count > 0 else 0.0
        return {
            "candidate_id": str(row["candidate_id"]),
            "article_count": int(row.get("article_count") or 0),
            "distinct_sources": int(row.get("distinct_sources") or 0),
            "active_days": int(row.get("active_days") or 0),
            "centroid_count": int(row.get("centroid_count") or 0),
            "cohesion_score": float(cohesion_score),
        }

    def mark_candidate_promoted(self, *, candidate_id: str, promoted_theme_id: str) -> None:
        """Mark candidate row promoted to a stable theme."""
        query = text(
            """
            update theme_candidates
            set
              status = 'promoted',
              promoted_theme_id = :promoted_theme_id,
              updated_at = now()
            where id = :candidate_id
            """
        )
        self._session.execute(
            query,
            {
                "candidate_id": candidate_id,
                "promoted_theme_id": promoted_theme_id,
            },
        )
        self._session.commit()

    def link_article_to_theme(
        self,
        *,
        article_id: str,
        theme_id: str,
        assignment_score: float,
        assignment_method: str,
        is_primary: bool,
        observed_at: datetime | None,
        assignment_version: str = "hybrid_v1",
        assignment_rationale: dict[str, Any] | None = None,
        alias_score: float = 0.0,
        semantic_score: float = 0.0,
        entity_overlap_score: float = 0.0,
        asset_overlap_score: float = 0.0,
        relationship_overlap_score: float = 0.0,
        margin_score: float = 0.0,
        article_embedding: list[float] | None = None,
        entity_profile: list[str] | None = None,
        asset_profile: list[str] | None = None,
        relationship_profile: list[str] | None = None,
    ) -> None:
        """Create or update article-theme link and refresh theme seen timestamps."""
        rationale = assignment_rationale if isinstance(assignment_rationale, dict) else {}
        link_query = text(
            """
            insert into theme_article_links (
              theme_id,
              article_id,
              assignment_score,
              assignment_method,
              assignment_version,
              assignment_rationale,
              alias_score,
              semantic_score,
              entity_overlap_score,
              asset_overlap_score,
              relationship_overlap_score,
              margin_score,
              is_primary,
              created_at,
              updated_at
            )
            values (
              :theme_id,
              :article_id,
              :assignment_score,
              :assignment_method,
              :assignment_version,
              cast(:assignment_rationale as jsonb),
              :alias_score,
              :semantic_score,
              :entity_overlap_score,
              :asset_overlap_score,
              :relationship_overlap_score,
              :margin_score,
              :is_primary,
              now(),
              now()
            )
            on conflict (theme_id, article_id) do update
            set
              assignment_score = greatest(theme_article_links.assignment_score, excluded.assignment_score),
              assignment_method = excluded.assignment_method,
              assignment_version = excluded.assignment_version,
              assignment_rationale = excluded.assignment_rationale,
              alias_score = excluded.alias_score,
              semantic_score = excluded.semantic_score,
              entity_overlap_score = excluded.entity_overlap_score,
              asset_overlap_score = excluded.asset_overlap_score,
              relationship_overlap_score = excluded.relationship_overlap_score,
              margin_score = excluded.margin_score,
              is_primary = theme_article_links.is_primary or excluded.is_primary,
              updated_at = now()
            returning (xmax = 0) as inserted
            """
        )
        link_row = self._session.execute(
            link_query,
            {
                "theme_id": theme_id,
                "article_id": article_id,
                "assignment_score": assignment_score,
                "assignment_method": assignment_method,
                "assignment_version": assignment_version,
                "assignment_rationale": self._to_json(rationale),
                "alias_score": float(alias_score),
                "semantic_score": float(semantic_score),
                "entity_overlap_score": float(entity_overlap_score),
                "asset_overlap_score": float(asset_overlap_score),
                "relationship_overlap_score": float(relationship_overlap_score),
                "margin_score": float(margin_score),
                "is_primary": is_primary,
            },
        ).mappings().one()
        inserted = bool(link_row.get("inserted"))

        seen_at = observed_at or datetime.now(timezone.utc)
        touch_query = text(
            """
            update themes
            set
              first_seen_at = case
                when first_seen_at is null then :seen_at
                else least(first_seen_at, :seen_at)
              end,
              last_seen_at = case
                when last_seen_at is null then :seen_at
                else greatest(last_seen_at, :seen_at)
              end,
              updated_at = now()
            where id = :theme_id
            """
        )
        self._session.execute(touch_query, {"theme_id": theme_id, "seen_at": seen_at})

        if inserted:
            article_vector = self._normalize_vector(article_embedding)
            if article_vector:
                state = self._session.execute(
                    text(
                        """
                        select centroid_embedding, centroid_count
                        from themes
                        where id = :theme_id
                        """
                    ),
                    {"theme_id": theme_id},
                ).mappings().first()
                current_centroid = self._normalize_vector(state.get("centroid_embedding") if state else None)
                current_count = int(state.get("centroid_count") or 0) if state else 0
                next_centroid = self._blend_centroid(current_centroid, current_count, article_vector)
                next_count = current_count + 1
                self._session.execute(
                    text(
                        """
                        update themes
                        set
                          centroid_embedding = cast(:centroid_embedding as vector),
                          centroid_count = :centroid_count,
                          centroid_updated_at = now(),
                          updated_at = now()
                        where id = :theme_id
                        """
                    ),
                    {
                        "theme_id": theme_id,
                        "centroid_embedding": self._to_pgvector_literal(next_centroid),
                        "centroid_count": next_count,
                    },
                )

            existing_profile = self._session.execute(
                text(
                    """
                    select entity_profile, asset_profile, relationship_profile
                    from themes
                    where id = :theme_id
                    """
                ),
                {"theme_id": theme_id},
            ).mappings().first()
            if existing_profile is not None:
                next_entity_profile = self._merge_profiles(
                    self._normalize_str_list(existing_profile.get("entity_profile")),
                    self._normalize_str_list(entity_profile),
                )
                next_asset_profile = self._merge_profiles(
                    self._normalize_str_list(existing_profile.get("asset_profile")),
                    self._normalize_str_list(asset_profile),
                )
                next_relationship_profile = self._merge_profiles(
                    self._normalize_str_list(existing_profile.get("relationship_profile")),
                    self._normalize_str_list(relationship_profile),
                )
                self._session.execute(
                    text(
                        """
                        update themes
                        set
                          entity_profile = cast(:entity_profile as jsonb),
                          asset_profile = cast(:asset_profile as jsonb),
                          relationship_profile = cast(:relationship_profile as jsonb),
                          profile_updated_at = now(),
                          updated_at = now()
                        where id = :theme_id
                        """
                    ),
                    {
                        "theme_id": theme_id,
                        "entity_profile": self._to_json(next_entity_profile),
                        "asset_profile": self._to_json(next_asset_profile),
                        "relationship_profile": self._to_json(next_relationship_profile),
                    },
                )

        self._session.execute(
            text(
                """
                insert into theme_assignment_logs (
                  theme_id,
                  article_id,
                  assignment_version,
                  assignment_method,
                  assignment_score,
                  alias_score,
                  semantic_score,
                  entity_overlap_score,
                  asset_overlap_score,
                  relationship_overlap_score,
                  margin_score,
                  rationale,
                  created_at
                )
                values (
                  :theme_id,
                  :article_id,
                  :assignment_version,
                  :assignment_method,
                  :assignment_score,
                  :alias_score,
                  :semantic_score,
                  :entity_overlap_score,
                  :asset_overlap_score,
                  :relationship_overlap_score,
                  :margin_score,
                  cast(:rationale as jsonb),
                  now()
                )
                """
            ),
            {
                "theme_id": theme_id,
                "article_id": article_id,
                "assignment_version": assignment_version,
                "assignment_method": assignment_method,
                "assignment_score": float(assignment_score),
                "alias_score": float(alias_score),
                "semantic_score": float(semantic_score),
                "entity_overlap_score": float(entity_overlap_score),
                "asset_overlap_score": float(asset_overlap_score),
                "relationship_overlap_score": float(relationship_overlap_score),
                "margin_score": float(margin_score),
                "rationale": self._to_json(rationale),
            },
        )
        self._session.commit()

    def refresh_theme_snapshots(self, lookback_days: int) -> int:
        """Recompute daily theme snapshots for a rolling lookback window."""
        days = max(1, int(lookback_days))
        query = text(
            """
            with daily as (
              select
                tal.theme_id,
                date_trunc('day', coalesce(a.published_at, a.created_at)) as bucket_start,
                count(*)::int as article_count,
                count(distinct a.source_name)::int as source_count,
                avg(tal.assignment_score)::double precision as avg_assignment_score,
                avg(
                  case coalesce(a.metadata->'text_processing'->>'market_tone', 'neutral')
                    when 'hawkish' then -0.6
                    when 'dovish' then 0.6
                    when 'risk_on' then 0.5
                    when 'risk_off' then -0.5
                    when 'inflation_up' then -0.4
                    when 'inflation_down' then 0.4
                    when 'growth_up' then 0.5
                    when 'growth_down' then -0.5
                    when 'liquidity_up' then 0.4
                    when 'liquidity_down' then -0.4
                    else 0.0
                  end
                )::double precision as avg_market_tone_score
              from theme_article_links tal
              inner join articles a on a.id = tal.article_id
              where coalesce(a.published_at, a.created_at) >= now() - make_interval(days => :days)
              group by tal.theme_id, date_trunc('day', coalesce(a.published_at, a.created_at))
            ),
            scored as (
              select
                d.theme_id,
                d.bucket_start,
                d.article_count,
                d.source_count,
                (
                  d.article_count
                  - coalesce(
                    avg(d.article_count) over (
                      partition by d.theme_id
                      order by d.bucket_start
                      rows between 3 preceding and 1 preceding
                    ),
                    0
                  )
                )::double precision as momentum_score,
                d.avg_assignment_score,
                d.avg_market_tone_score
              from daily d
            )
            insert into theme_snapshots (
              theme_id,
              bucket_start,
              bucket_granularity,
              article_count,
              source_count,
              momentum_score,
              avg_assignment_score,
              avg_market_tone_score,
              created_at,
              updated_at
            )
            select
              s.theme_id,
              s.bucket_start,
              'day',
              s.article_count,
              s.source_count,
              s.momentum_score,
              s.avg_assignment_score,
              s.avg_market_tone_score,
              now(),
              now()
            from scored s
            on conflict (theme_id, bucket_start, bucket_granularity) do update
            set
              article_count = excluded.article_count,
              source_count = excluded.source_count,
              momentum_score = excluded.momentum_score,
              avg_assignment_score = excluded.avg_assignment_score,
              avg_market_tone_score = excluded.avg_market_tone_score,
              updated_at = now()
            returning id
            """
        )
        rows = self._session.execute(query, {"days": days}).all()
        self._session.commit()
        return len(rows)

    def rebuild_cooccurrence_relations(self, lookback_days: int) -> int:
        """Rebuild co-occurrence graph edges from recent article-theme links."""
        days = max(1, int(lookback_days))
        self._session.execute(
            text("delete from theme_relations where relation_type = 'co_occurrence'")
        )
        insert_query = text(
            """
            insert into theme_relations (
              source_theme_id,
              target_theme_id,
              relation_type,
              relation_score,
              evidence_count,
              last_observed_at,
              created_at,
              updated_at
            )
            select
              left_links.theme_id as source_theme_id,
              right_links.theme_id as target_theme_id,
              'co_occurrence' as relation_type,
              count(*)::double precision as relation_score,
              count(*)::int as evidence_count,
              max(coalesce(a.published_at, a.created_at)) as last_observed_at,
              now(),
              now()
            from theme_article_links left_links
            inner join theme_article_links right_links
              on right_links.article_id = left_links.article_id
             and right_links.theme_id <> left_links.theme_id
            inner join articles a on a.id = left_links.article_id
            where coalesce(a.published_at, a.created_at) >= now() - make_interval(days => :days)
            group by left_links.theme_id, right_links.theme_id
            on conflict (source_theme_id, target_theme_id, relation_type) do update
            set
              relation_score = excluded.relation_score,
              evidence_count = excluded.evidence_count,
              last_observed_at = excluded.last_observed_at,
              updated_at = now()
            returning id
            """
        )
        rows = self._session.execute(insert_query, {"days": days}).all()
        self._session.commit()
        return len(rows)

    def refresh_theme_statuses(self) -> int:
        """Update theme lifecycle states from recency."""
        query = text(
            """
            with status_targets as (
              select
                t.id,
                case
                  when t.last_seen_at is null then t.status
                  when t.last_seen_at >= now() - interval '3 days'
                    then case
                      when t.first_seen_at is not null and t.first_seen_at >= now() - interval '7 days'
                        then 'emerging'
                      else 'active'
                    end
                  when t.last_seen_at >= now() - interval '14 days' then 'cooling'
                  when t.last_seen_at >= now() - interval '45 days' then 'dormant'
                  else 'retired'
                end as next_status
              from themes t
            )
            update themes t
            set
              status = s.next_status,
              updated_at = now()
            from status_targets s
            where t.id = s.id
              and t.status <> s.next_status
            returning t.id
            """
        )
        rows = self._session.execute(query).all()
        self._session.commit()
        return len(rows)

    def list_hot_theme_metrics(self, lookback_days: int) -> list[dict[str, Any]]:
        """Return aggregate metrics per theme for hotness ranking."""
        days = max(1, int(lookback_days))
        query = text(
            """
            select
              t.id::text as theme_id,
              t.slug,
              t.canonical_label,
              t.summary,
              t.status,
              t.discovery_method,
              t.first_seen_at,
              t.last_seen_at,
              coalesce(
                sum(
                  ts.article_count::double precision
                  * exp(
                    -extract(epoch from (now() - ts.bucket_start)) / 86400.0 / 5.0
                  )
                ),
                0.0
              ) as recency_weighted_count,
              coalesce(
                sum(case when ts.bucket_start >= date_trunc('day', now()) - interval '2 days' then ts.article_count else 0 end),
                0
              )::int as article_count_3d,
              coalesce(
                sum(
                  case
                    when ts.bucket_start < date_trunc('day', now()) - interval '2 days'
                     and ts.bucket_start >= date_trunc('day', now()) - interval '5 days'
                    then ts.article_count
                    else 0
                  end
                ),
                0
              )::int as prev_article_count_3d,
              coalesce(
                sum(case when ts.bucket_start >= date_trunc('day', now()) - interval '6 days' then ts.article_count else 0 end),
                0
              )::int as article_count_7d,
              coalesce(avg(ts.source_count), 0.0) as avg_source_count,
              coalesce(avg(ts.avg_assignment_score), 0.0) as avg_assignment_score,
              coalesce(max(ts.bucket_start), t.last_seen_at) as last_metric_at
            from themes t
            left join theme_snapshots ts
              on ts.theme_id = t.id
             and ts.bucket_granularity = 'day'
             and ts.bucket_start >= now() - make_interval(days => :days)
            where t.status <> 'retired'
            group by
              t.id,
              t.slug,
              t.canonical_label,
              t.summary,
              t.status,
              t.discovery_method,
              t.first_seen_at,
              t.last_seen_at
            """
        )
        rows = self._session.execute(query, {"days": days}).mappings().all()
        return [dict(row) for row in rows]

    def get_theme_overview(self, theme_id: str) -> dict[str, Any] | None:
        """Return profile and aggregate stats for one theme."""
        query = text(
            """
            select
              t.id::text as theme_id,
              t.slug,
              t.canonical_label,
              t.summary,
              t.status,
              t.discovery_method,
              t.first_seen_at,
              t.last_seen_at,
              t.created_at,
              t.updated_at,
              coalesce(count(distinct tal.article_id), 0)::int as total_articles,
              coalesce(count(distinct a.source_name), 0)::int as source_diversity,
              coalesce(avg(tal.assignment_score), 0.0) as avg_assignment_score
            from themes t
            left join theme_article_links tal on tal.theme_id = t.id
            left join articles a on a.id = tal.article_id
            where t.id = :theme_id
            group by
              t.id,
              t.slug,
              t.canonical_label,
              t.summary,
              t.status,
              t.discovery_method,
              t.first_seen_at,
              t.last_seen_at,
              t.created_at,
              t.updated_at
            """
        )
        row = self._session.execute(query, {"theme_id": theme_id}).mappings().first()
        return dict(row) if row else None

    def list_theme_aliases(self, theme_id: str) -> list[str]:
        """Return aliases for a theme in deterministic order."""
        query = text(
            """
            select alias
            from theme_aliases
            where theme_id = :theme_id
            order by is_primary desc, alias asc
            """
        )
        rows = self._session.execute(query, {"theme_id": theme_id}).all()
        return [str(row[0]) for row in rows]

    def list_theme_timeline(self, theme_id: str, days: int) -> list[dict[str, Any]]:
        """Return daily snapshots for one theme."""
        lookback_days = max(1, int(days))
        query = text(
            """
            select
              ts.bucket_start,
              ts.article_count,
              ts.source_count,
              ts.momentum_score,
              ts.avg_assignment_score,
              ts.avg_market_tone_score
            from theme_snapshots ts
            where ts.theme_id = :theme_id
              and ts.bucket_granularity = 'day'
              and ts.bucket_start >= now() - make_interval(days => :days)
            order by ts.bucket_start asc
            """
        )
        rows = self._session.execute(
            query,
            {
                "theme_id": theme_id,
                "days": lookback_days,
            },
        ).mappings().all()
        return [dict(row) for row in rows]

    def list_related_themes(self, theme_id: str, limit: int) -> list[dict[str, Any]]:
        """Return strongest related themes."""
        resolved_limit = max(1, int(limit))
        query = text(
            """
            select
              related.id::text as theme_id,
              related.slug,
              related.canonical_label,
              related.status,
              rel.relation_type,
              rel.relation_score,
              rel.evidence_count,
              rel.last_observed_at
            from theme_relations rel
            inner join themes related on related.id = rel.target_theme_id
            where rel.source_theme_id = :theme_id
              and rel.relation_type = 'co_occurrence'
            order by rel.relation_score desc, rel.last_observed_at desc
            limit :limit
            """
        )
        rows = self._session.execute(
            query,
            {
                "theme_id": theme_id,
                "limit": resolved_limit,
            },
        ).mappings().all()
        return [dict(row) for row in rows]

    def list_theme_recent_articles(self, theme_id: str, limit: int) -> list[dict[str, Any]]:
        """Return latest supporting developments for a theme."""
        resolved_limit = max(1, int(limit))
        query = text(
            """
            select
              a.id::text as article_id,
              a.canonical_url,
              a.title,
              a.published_at,
              a.source_name,
              tal.assignment_score,
              tal.assignment_method,
              tal.assignment_version,
              tal.alias_score,
              tal.semantic_score,
              tal.entity_overlap_score,
              tal.asset_overlap_score,
              tal.relationship_overlap_score,
              tal.margin_score,
              tal.assignment_rationale,
              a.metadata->'text_processing'->>'event' as event,
              coalesce(
                a.metadata->'text_processing'->'narratives',
                a.metadata->'text_processing'->'macro_signals',
                '[]'::jsonb
              ) as narratives
            from theme_article_links tal
            inner join articles a on a.id = tal.article_id
            where tal.theme_id = :theme_id
            order by coalesce(a.published_at, a.created_at) desc
            limit :limit
            """
        )
        rows = self._session.execute(
            query,
            {
                "theme_id": theme_id,
                "limit": resolved_limit,
            },
        ).mappings().all()
        developments: list[dict[str, Any]] = []
        for row in rows:
            narratives = row["narratives"]
            if not isinstance(narratives, list):
                narratives = []
            developments.append(
                {
                    "article_id": str(row["article_id"]),
                    "canonical_url": str(row["canonical_url"]),
                    "title": row["title"],
                    "published_at": row["published_at"],
                    "source_name": str(row["source_name"] or "unknown"),
                    "assignment_score": float(row["assignment_score"] or 0.0),
                    "assignment_method": str(row["assignment_method"] or "unknown"),
                    "assignment_version": str(row["assignment_version"] or "hybrid_v1"),
                    "alias_score": float(row["alias_score"] or 0.0),
                    "semantic_score": float(row["semantic_score"] or 0.0),
                    "entity_overlap_score": float(row["entity_overlap_score"] or 0.0),
                    "asset_overlap_score": float(row["asset_overlap_score"] or 0.0),
                    "relationship_overlap_score": float(row["relationship_overlap_score"] or 0.0),
                    "margin_score": float(row["margin_score"] or 0.0),
                    "assignment_rationale": row["assignment_rationale"]
                    if isinstance(row["assignment_rationale"], dict)
                    else {},
                    "event": row["event"],
                    "narratives": [str(signal) for signal in narratives if isinstance(signal, str)],
                }
            )
        return developments

    def list_theme_profiles_for_maintenance(self, *, statuses: list[str] | None = None) -> list[dict[str, Any]]:
        """Return theme profiles for merge/split recommendation analysis."""
        active_statuses = set(statuses or ["emerging", "active", "cooling", "dormant"])
        query = text(
            """
            select
              t.id::text as theme_id,
              t.slug,
              t.canonical_label,
              t.status,
              t.centroid_embedding,
              t.centroid_count,
              t.entity_profile,
              t.asset_profile,
              t.relationship_profile,
              coalesce(link_stats.link_count, 0)::int as link_count
            from themes t
            left join (
              select theme_id, count(*)::int as link_count
              from theme_article_links
              group by theme_id
            ) link_stats on link_stats.theme_id = t.id
            """
        )
        rows = self._session.execute(query).mappings().all()
        payloads: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            if str(payload.get("status")) not in active_statuses:
                continue
            payload["centroid_embedding"] = self._normalize_vector(payload.get("centroid_embedding"))
            payload["centroid_count"] = int(payload.get("centroid_count") or 0)
            payload["entity_profile"] = self._normalize_str_list(payload.get("entity_profile"))
            payload["asset_profile"] = self._normalize_str_list(payload.get("asset_profile"))
            payload["relationship_profile"] = self._normalize_str_list(payload.get("relationship_profile"))
            payload["link_count"] = int(payload.get("link_count") or 0)
            payloads.append(payload)
        return payloads

    def list_theme_alias_sets(self) -> dict[str, set[str]]:
        """Return normalized alias sets keyed by theme_id."""
        query = text(
            """
            select
              ta.theme_id::text as theme_id,
              ta.normalized_alias
            from theme_aliases ta
            """
        )
        rows = self._session.execute(query).mappings().all()
        alias_sets: dict[str, set[str]] = {}
        for row in rows:
            theme_id = str(row["theme_id"])
            alias_sets.setdefault(theme_id, set()).add(str(row["normalized_alias"]))
        return alias_sets

    def list_theme_cohesion_rows(self, *, min_articles: int = 5) -> list[dict[str, Any]]:
        """Return per-theme embedding cohesion stats for split recommendation signals."""
        resolved_min_articles = max(1, int(min_articles))
        query = text(
            """
            select
              t.id::text as theme_id,
              t.slug,
              t.canonical_label,
              t.status,
              count(*)::int as article_count,
              avg((1 - (a.embedding <=> t.centroid_embedding))::double precision) as avg_similarity,
              coalesce(
                stddev_pop((1 - (a.embedding <=> t.centroid_embedding))::double precision),
                0.0
              ) as similarity_stddev
            from themes t
            inner join theme_article_links tal on tal.theme_id = t.id
            inner join articles a on a.id = tal.article_id
            where t.centroid_embedding is not null
              and a.embedding is not null
              and t.status in ('emerging', 'active', 'cooling', 'dormant')
            group by t.id, t.slug, t.canonical_label, t.status
            having count(*) >= :min_articles
            """
        )
        rows = self._session.execute(query, {"min_articles": resolved_min_articles}).mappings().all()
        payloads: list[dict[str, Any]] = []
        for row in rows:
            payloads.append(
                {
                    "theme_id": str(row["theme_id"]),
                    "slug": str(row["slug"]),
                    "canonical_label": str(row["canonical_label"]),
                    "status": str(row["status"]),
                    "article_count": int(row["article_count"] or 0),
                    "avg_similarity": float(row["avg_similarity"] or 0.0),
                    "similarity_stddev": float(row["similarity_stddev"] or 0.0),
                }
            )
        return payloads

    def replace_theme_maintenance_recommendations(self, recommendations: list[dict[str, Any]]) -> int:
        """Replace currently suggested maintenance recommendations with latest outputs."""
        self._session.execute(
            text(
                """
                delete from theme_maintenance_recommendations
                where status = 'suggested'
                """
            )
        )
        inserted = 0
        for item in recommendations:
            self._session.execute(
                text(
                    """
                    insert into theme_maintenance_recommendations (
                      recommendation_type,
                      source_theme_id,
                      target_theme_id,
                      confidence_score,
                      status,
                      rationale,
                      payload,
                      created_at,
                      updated_at
                    )
                    values (
                      :recommendation_type,
                      :source_theme_id,
                      :target_theme_id,
                      :confidence_score,
                      :status,
                      :rationale,
                      cast(:payload as jsonb),
                      now(),
                      now()
                    )
                    """
                ),
                {
                    "recommendation_type": str(item.get("recommendation_type") or "merge"),
                    "source_theme_id": item.get("source_theme_id"),
                    "target_theme_id": item.get("target_theme_id"),
                    "confidence_score": float(item.get("confidence_score") or 0.0),
                    "status": str(item.get("status") or "suggested"),
                    "rationale": item.get("rationale"),
                    "payload": self._to_json(item.get("payload") if isinstance(item.get("payload"), dict) else {}),
                },
            )
            inserted += 1
        self._session.commit()
        return inserted

    def list_theme_maintenance_recommendations(
        self,
        *,
        limit: int = 50,
        recommendation_type: str | None = None,
        status: str = "suggested",
    ) -> list[dict[str, Any]]:
        resolved_limit = max(1, int(limit))
        query = text(
            """
            select
              r.id::text as recommendation_id,
              r.recommendation_type,
              r.source_theme_id::text as source_theme_id,
              source_theme.slug as source_slug,
              source_theme.canonical_label as source_label,
              r.target_theme_id::text as target_theme_id,
              target_theme.slug as target_slug,
              target_theme.canonical_label as target_label,
              r.confidence_score,
              r.status,
              r.rationale,
              r.payload,
              r.created_at,
              r.updated_at
            from theme_maintenance_recommendations r
            inner join themes source_theme on source_theme.id = r.source_theme_id
            left join themes target_theme on target_theme.id = r.target_theme_id
            where r.status = :status
              and (:recommendation_type is null or r.recommendation_type = :recommendation_type)
            order by r.confidence_score desc, r.created_at desc
            limit :limit
            """
        )
        rows = self._session.execute(
            query,
            {
                "status": status,
                "recommendation_type": recommendation_type,
                "limit": resolved_limit,
            },
        ).mappings().all()
        payloads: list[dict[str, Any]] = []
        for row in rows:
            payloads.append(
                {
                    "recommendation_id": str(row["recommendation_id"]),
                    "recommendation_type": str(row["recommendation_type"]),
                    "source_theme_id": str(row["source_theme_id"]),
                    "source_slug": str(row["source_slug"]),
                    "source_label": str(row["source_label"]),
                    "target_theme_id": str(row["target_theme_id"]) if row["target_theme_id"] else None,
                    "target_slug": str(row["target_slug"]) if row["target_slug"] else None,
                    "target_label": str(row["target_label"]) if row["target_label"] else None,
                    "confidence_score": float(row["confidence_score"] or 0.0),
                    "status": str(row["status"]),
                    "rationale": row.get("rationale"),
                    "payload": row.get("payload") if isinstance(row.get("payload"), dict) else {},
                    "created_at": row.get("created_at"),
                    "updated_at": row.get("updated_at"),
                }
            )
        return payloads

    def record_theme_lineage(
        self,
        *,
        parent_theme_id: str,
        child_theme_id: str,
        relation_type: str,
        note: str | None,
    ) -> None:
        query = text(
            """
            insert into theme_lineage (
              parent_theme_id,
              child_theme_id,
              relation_type,
              note,
              created_at
            )
            values (
              :parent_theme_id,
              :child_theme_id,
              :relation_type,
              :note,
              now()
            )
            on conflict (parent_theme_id, child_theme_id, relation_type) do update
            set note = excluded.note
            """
        )
        self._session.execute(
            query,
            {
                "parent_theme_id": parent_theme_id,
                "child_theme_id": child_theme_id,
                "relation_type": relation_type,
                "note": note,
            },
        )
        self._session.commit()

    def rebuild_theme_centroids(self, *, lookback_days: int | None = None) -> int:
        """Rebuild all theme centroids from linked article embeddings."""
        if lookback_days is None:
            where_clause = ""
            params: dict[str, Any] = {}
        else:
            where_clause = "and coalesce(a.published_at, a.created_at) >= now() - make_interval(days => :lookback_days)"
            params = {"lookback_days": max(1, int(lookback_days))}
        query = text(
            f"""
            with centroid_rows as (
              select
                tal.theme_id,
                avg(a.embedding)::vector(1536) as centroid_embedding,
                count(*)::int as centroid_count
              from theme_article_links tal
              inner join articles a on a.id = tal.article_id
              where a.embedding is not null
              {where_clause}
              group by tal.theme_id
            )
            update themes t
            set
              centroid_embedding = c.centroid_embedding,
              centroid_count = c.centroid_count,
              centroid_updated_at = now(),
              updated_at = now()
            from centroid_rows c
            where t.id = c.theme_id
            returning t.id
            """
        )
        rows = self._session.execute(query, params).all()
        self._session.commit()
        return len(rows)

    def count_processed_articles(self, *, lookback_days: int | None = None) -> int:
        if lookback_days is None:
            query = text(
                """
                select count(*)::int as cnt
                from articles a
                where a.metadata ? 'text_processing'
                  and coalesce((a.metadata->'text_processing'->>'keep')::boolean, true) = true
                """
            )
            row = self._session.execute(query).mappings().first()
        else:
            query = text(
                """
                select count(*)::int as cnt
                from articles a
                where a.metadata ? 'text_processing'
                  and coalesce((a.metadata->'text_processing'->>'keep')::boolean, true) = true
                  and coalesce(a.published_at, a.created_at) >= now() - make_interval(days => :days)
                """
            )
            row = self._session.execute(query, {"days": max(1, int(lookback_days))}).mappings().first()
        return int(row["cnt"]) if row else 0

    def record_theme_sync_run(self, *, summary: dict[str, Any], config: dict[str, Any]) -> None:
        self._session.execute(
            text(
                """
                insert into theme_sync_runs (
                  assignment_input_count,
                  assigned_articles,
                  theme_links_upserted,
                  created_themes,
                  promoted_candidates,
                  snapshots_upserted,
                  relations_upserted,
                  status_updates,
                  abstained_articles,
                  abstained_signals,
                  assignment_rate,
                  abstain_rate,
                  recommendation_count,
                  recommendation_applied_count,
                  config,
                  created_at
                )
                values (
                  :assignment_input_count,
                  :assigned_articles,
                  :theme_links_upserted,
                  :created_themes,
                  :promoted_candidates,
                  :snapshots_upserted,
                  :relations_upserted,
                  :status_updates,
                  :abstained_articles,
                  :abstained_signals,
                  :assignment_rate,
                  :abstain_rate,
                  :recommendation_count,
                  :recommendation_applied_count,
                  cast(:config as jsonb),
                  now()
                )
                """
            ),
            {
                "assignment_input_count": int(summary.get("assignment_input_count") or 0),
                "assigned_articles": int(summary.get("assigned_articles") or 0),
                "theme_links_upserted": int(summary.get("theme_links_upserted") or 0),
                "created_themes": int(summary.get("created_themes") or 0),
                "promoted_candidates": int(summary.get("promoted_candidates") or 0),
                "snapshots_upserted": int(summary.get("snapshots_upserted") or 0),
                "relations_upserted": int(summary.get("relations_upserted") or 0),
                "status_updates": int(summary.get("status_updates") or 0),
                "abstained_articles": int(summary.get("abstained_articles") or 0),
                "abstained_signals": int(summary.get("abstained_signals") or 0),
                "assignment_rate": float(summary.get("assignment_rate") or 0.0),
                "abstain_rate": float(summary.get("abstain_rate") or 0.0),
                "recommendation_count": int(summary.get("recommendation_count") or 0),
                "recommendation_applied_count": int(summary.get("recommendation_applied_count") or 0),
                "config": self._to_json(config if isinstance(config, dict) else {}),
            },
        )
        self._session.commit()

    def _build_unique_slug(self, slug_base: str) -> str:
        normalized = slug_base.strip("-") or "theme"
        if not self._slug_exists(normalized):
            return normalized

        suffix = 2
        while True:
            candidate = f"{normalized}-{suffix}"
            if not self._slug_exists(candidate):
                return candidate
            suffix += 1

    def _slug_exists(self, slug: str) -> bool:
        query = text("select 1 from themes where slug = :slug limit 1")
        row = self._session.execute(query, {"slug": slug}).first()
        return row is not None

    @staticmethod
    def _to_json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=True)

    @staticmethod
    def _normalize_vector(value: Any) -> list[float] | None:
        if value is None:
            return None
        if isinstance(value, tuple):
            return [float(item) for item in value]
        if isinstance(value, list):
            try:
                return [float(item) for item in value]
            except (TypeError, ValueError):
                return None
        if isinstance(value, str):
            text_value = value.strip()
            if not text_value.startswith("[") or not text_value.endswith("]"):
                return None
            body = text_value[1:-1].strip()
            if not body:
                return []
            pieces = [piece.strip() for piece in body.split(",")]
            try:
                return [float(piece) for piece in pieces if piece]
            except ValueError:
                return None
        return None

    @staticmethod
    def _to_pgvector_literal(vector: list[float] | None) -> str | None:
        normalized = ThemeRepository._normalize_vector(vector)
        if normalized is None:
            return None
        payload = ",".join(format(float(value), ".10g") for value in normalized)
        return f"[{payload}]"

    @staticmethod
    def _normalize_str_list(value: Any) -> list[str]:
        if isinstance(value, list):
            items = value
        elif isinstance(value, tuple):
            items = list(value)
        else:
            return []
        deduped: list[str] = []
        seen: set[str] = set()
        for item in items:
            if not isinstance(item, str):
                continue
            normalized = item.strip().lower()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(normalized)
        return deduped

    @staticmethod
    def _merge_profiles(existing: list[str], additions: list[str], max_items: int = 40) -> list[str]:
        merged: list[str] = []
        seen: set[str] = set()
        for item in existing + additions:
            normalized = item.strip().lower()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            merged.append(normalized)
            if len(merged) >= max_items:
                break
        return merged

    @staticmethod
    def _dot(left: list[float], right: list[float]) -> float:
        size = min(len(left), len(right))
        return sum(float(left[idx]) * float(right[idx]) for idx in range(size))

    @staticmethod
    def _norm(values: list[float]) -> float:
        return math.sqrt(sum(float(value) * float(value) for value in values))

    @staticmethod
    def _cosine_similarity(left: list[float] | None, right: list[float] | None) -> float:
        if not left or not right:
            return 0.0
        denom = ThemeRepository._norm(left) * ThemeRepository._norm(right)
        if denom <= 0:
            return 0.0
        return ThemeRepository._dot(left, right) / denom

    @staticmethod
    def _blend_centroid(
        current: list[float] | None,
        current_count: int,
        incoming: list[float],
    ) -> list[float]:
        if current is None or current_count <= 0:
            return [float(value) for value in incoming]
        size = min(len(current), len(incoming))
        total = max(current_count, 0)
        return [
            ((float(current[idx]) * total) + float(incoming[idx])) / (total + 1)
            for idx in range(size)
        ]

    @staticmethod
    def _parse_uuid(raw: str) -> uuid.UUID | None:
        try:
            return uuid.UUID(str(raw).strip())
        except (TypeError, ValueError, AttributeError):
            return None
