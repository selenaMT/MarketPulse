"""Repository for user watchlist links and watchlist reads."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session


class WatchlistRepository:
    """Database operations for user-theme watchlist and backfill support."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def commit(self) -> None:
        self._session.commit()

    def rollback(self) -> None:
        self._session.rollback()

    def get_theme_for_user(self, user_id: Any, theme_id: Any) -> dict[str, Any] | None:
        row = self._session.execute(
            text(
                """
                select
                  t.id,
                  t.slug,
                  t.canonical_label,
                  t.summary,
                  t.status,
                  t.discovery_method,
                  t.scope,
                  t.owner_user_id,
                  t.article_count,
                  t.first_seen_at,
                  t.last_seen_at,
                  t.created_at,
                  t.updated_at,
                  t.title_embedding
                from themes t
                where t.id = cast(:theme_id as uuid)
                  and (
                    t.scope = 'global'
                    or (
                      t.scope = 'user'
                      and t.owner_user_id = cast(:user_id as uuid)
                    )
                  )
                limit 1
                """
            ),
            {"theme_id": str(theme_id), "user_id": str(user_id)},
        ).mappings().first()
        return dict(row) if row else None

    def get_user_owned_theme_by_label(self, user_id: Any, canonical_label: str) -> dict[str, Any] | None:
        row = self._session.execute(
            text(
                """
                select
                  id,
                  slug,
                  canonical_label,
                  summary,
                  status,
                  discovery_method,
                  scope,
                  owner_user_id,
                  article_count,
                  first_seen_at,
                  last_seen_at,
                  created_at,
                  updated_at,
                  title_embedding
                from themes
                where scope = 'user'
                  and owner_user_id = cast(:user_id as uuid)
                  and lower(canonical_label) = lower(:canonical_label)
                limit 1
                """
            ),
            {"user_id": str(user_id), "canonical_label": canonical_label},
        ).mappings().first()
        return dict(row) if row else None

    def list_user_watchlist_themes(self, user_id: Any, limit: int = 50) -> list[dict[str, Any]]:
        rows = self._session.execute(
            text(
                """
                select
                  t.id,
                  t.slug,
                  t.canonical_label,
                  t.summary,
                  t.status,
                  t.discovery_method,
                  t.scope,
                  t.owner_user_id,
                  t.article_count,
                  t.first_seen_at,
                  t.last_seen_at,
                  t.created_at,
                  t.updated_at,
                  utl.alerts_enabled,
                  utl.created_at as watchlisted_at,
                  utl.updated_at as watchlist_updated_at
                from user_theme_links utl
                join themes t on t.id = utl.theme_id
                where utl.user_id = cast(:user_id as uuid)
                order by utl.created_at desc
                limit :limit
                """
            ),
            {"user_id": str(user_id), "limit": max(1, int(limit))},
        ).mappings().all()
        return [dict(row) for row in rows]

    def upsert_user_theme_link(
        self,
        user_id: Any,
        theme_id: Any,
        alerts_enabled: bool = True,
    ) -> bool:
        row = self._session.execute(
            text(
                """
                insert into user_theme_links (
                  user_id,
                  theme_id,
                  alerts_enabled,
                  created_at,
                  updated_at
                ) values (
                  cast(:user_id as uuid),
                  cast(:theme_id as uuid),
                  :alerts_enabled,
                  now(),
                  now()
                )
                on conflict (user_id, theme_id) do update set
                  alerts_enabled = excluded.alerts_enabled,
                  updated_at = now()
                returning (xmax = 0) as inserted
                """
            ),
            {
                "user_id": str(user_id),
                "theme_id": str(theme_id),
                "alerts_enabled": bool(alerts_enabled),
            },
        ).mappings().one()
        return bool(row["inserted"])

    def remove_user_theme_link(self, user_id: Any, theme_id: Any) -> bool:
        row = self._session.execute(
            text(
                """
                delete from user_theme_links
                where user_id = cast(:user_id as uuid)
                  and theme_id = cast(:theme_id as uuid)
                returning id
                """
            ),
            {"user_id": str(user_id), "theme_id": str(theme_id)},
        ).mappings().first()
        return bool(row)

    def has_user_theme_link(self, user_id: Any, theme_id: Any) -> bool:
        row = self._session.execute(
            text(
                """
                select 1
                from user_theme_links
                where user_id = cast(:user_id as uuid)
                  and theme_id = cast(:theme_id as uuid)
                limit 1
                """
            ),
            {"user_id": str(user_id), "theme_id": str(theme_id)},
        ).mappings().first()
        return bool(row)

    def create_user_theme(
        self,
        user_id: Any,
        canonical_label: str,
        summary: str | None,
        embedding_literal: str,
    ) -> dict[str, Any]:
        theme_id = uuid.uuid4()
        row = self._session.execute(
            text(
                """
                insert into themes (
                  id,
                  slug,
                  canonical_label,
                  summary,
                  status,
                  discovery_method,
                  scope,
                  owner_user_id,
                  title_embedding,
                  article_count,
                  first_seen_at,
                  last_seen_at,
                  created_at,
                  updated_at
                ) values (
                  :theme_id,
                  :slug,
                  :canonical_label,
                  :summary,
                  'active',
                  'user_created',
                  'user',
                  cast(:owner_user_id as uuid),
                  cast(:embedding as vector),
                  0,
                  null,
                  null,
                  now(),
                  now()
                )
                returning
                  id,
                  slug,
                  canonical_label,
                  summary,
                  status,
                  discovery_method,
                  scope,
                  owner_user_id,
                  article_count,
                  first_seen_at,
                  last_seen_at,
                  created_at,
                  updated_at
                """
            ),
            {
                "theme_id": theme_id,
                "slug": self._build_slug(canonical_label, theme_id),
                "canonical_label": canonical_label,
                "summary": summary,
                "owner_user_id": str(user_id),
                "embedding": embedding_literal,
            },
        ).mappings().one()
        return dict(row)

    def find_similar_global_themes(
        self,
        *,
        embedding_literal: str,
        min_similarity: float,
        limit: int = 20,
        exclude_theme_id: Any | None = None,
    ) -> list[dict[str, Any]]:
        rows = self._session.execute(
            text(
                """
                select
                  t.id,
                  (1 - (t.title_embedding <=> cast(:embedding as vector)))::double precision as similarity
                from themes t
                where t.scope = 'global'
                  and t.title_embedding is not null
                  and coalesce(t.status, 'active') <> 'retired'
                  and (
                    cast(:exclude_theme_id as uuid) is null
                    or t.id <> cast(:exclude_theme_id as uuid)
                  )
                  and (1 - (t.title_embedding <=> cast(:embedding as vector))) >= :min_similarity
                order by t.title_embedding <=> cast(:embedding as vector)
                limit :limit
                """
            ),
            {
                "embedding": embedding_literal,
                "min_similarity": float(min_similarity),
                "limit": max(1, int(limit)),
                "exclude_theme_id": str(exclude_theme_id) if exclude_theme_id else None,
            },
        ).mappings().all()
        return [dict(row) for row in rows]

    def find_similar_candidates(
        self,
        *,
        embedding_literal: str,
        min_similarity: float,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        rows = self._session.execute(
            text(
                """
                select
                  c.id,
                  (1 - (c.title_embedding <=> cast(:embedding as vector)))::double precision as similarity
                from theme_candidates c
                where c.title_embedding is not null
                  and c.status = 'candidate'
                  and (1 - (c.title_embedding <=> cast(:embedding as vector))) >= :min_similarity
                order by c.title_embedding <=> cast(:embedding as vector)
                limit :limit
                """
            ),
            {
                "embedding": embedding_literal,
                "min_similarity": float(min_similarity),
                "limit": max(1, int(limit)),
            },
        ).mappings().all()
        return [dict(row) for row in rows]

    def inherit_articles_from_themes(
        self,
        *,
        target_theme_id: Any,
        source_theme_ids: list[Any],
        assignment_method: str,
    ) -> int:
        normalized = [str(theme_id) for theme_id in source_theme_ids if theme_id]
        if not normalized:
            return 0

        row = self._session.execute(
            text(
                """
                with source_theme_ids as (
                  select unnest(cast(:source_theme_ids as uuid[])) as source_theme_id
                ),
                upserted as (
                  insert into theme_article_links (
                    theme_id,
                    article_id,
                    similarity_score,
                    assignment_score,
                    assignment_method,
                    assignment_version,
                    matched_at,
                    created_at,
                    updated_at
                  )
                  select
                    cast(:target_theme_id as uuid),
                    tal.article_id,
                    tal.similarity_score,
                    tal.assignment_score,
                    :assignment_method,
                    'semantic_v1',
                    tal.matched_at,
                    now(),
                    now()
                  from theme_article_links tal
                  join source_theme_ids sti on sti.source_theme_id = tal.theme_id
                  on conflict (theme_id, article_id) do update set
                    similarity_score = greatest(theme_article_links.similarity_score, excluded.similarity_score),
                    assignment_score = greatest(theme_article_links.assignment_score, excluded.assignment_score),
                    matched_at = greatest(theme_article_links.matched_at, excluded.matched_at),
                    updated_at = now()
                  returning (xmax = 0) as inserted
                )
                select count(*)::integer as inserted_count
                from upserted
                where inserted
                """
            ),
            {
                "target_theme_id": str(target_theme_id),
                "source_theme_ids": normalized,
                "assignment_method": assignment_method,
            },
        ).mappings().one()
        return int(row["inserted_count"] or 0)

    def inherit_articles_from_candidates(
        self,
        *,
        target_theme_id: Any,
        source_candidate_ids: list[Any],
        assignment_method: str,
    ) -> int:
        normalized = [str(candidate_id) for candidate_id in source_candidate_ids if candidate_id]
        if not normalized:
            return 0

        row = self._session.execute(
            text(
                """
                with source_candidate_ids as (
                  select unnest(cast(:source_candidate_ids as uuid[])) as source_candidate_id
                ),
                upserted as (
                  insert into theme_article_links (
                    theme_id,
                    article_id,
                    similarity_score,
                    assignment_score,
                    assignment_method,
                    assignment_version,
                    matched_at,
                    created_at,
                    updated_at
                  )
                  select
                    cast(:target_theme_id as uuid),
                    ctl.article_id,
                    ctl.similarity_score,
                    ctl.similarity_score,
                    :assignment_method,
                    'semantic_v1',
                    ctl.matched_at,
                    now(),
                    now()
                  from candidate_theme_article_links ctl
                  join source_candidate_ids sci on sci.source_candidate_id = ctl.candidate_theme_id
                  on conflict (theme_id, article_id) do update set
                    similarity_score = greatest(theme_article_links.similarity_score, excluded.similarity_score),
                    assignment_score = greatest(theme_article_links.assignment_score, excluded.assignment_score),
                    matched_at = greatest(theme_article_links.matched_at, excluded.matched_at),
                    updated_at = now()
                  returning (xmax = 0) as inserted
                )
                select count(*)::integer as inserted_count
                from upserted
                where inserted
                """
            ),
            {
                "target_theme_id": str(target_theme_id),
                "source_candidate_ids": normalized,
                "assignment_method": assignment_method,
            },
        ).mappings().one()
        return int(row["inserted_count"] or 0)

    def list_watchlist_theme_articles(
        self,
        *,
        user_id: Any,
        theme_id: Any,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        rows = self._session.execute(
            text(
                """
                select
                  a.id as article_id,
                  a.canonical_url,
                  a.title,
                  a.description,
                  a.published_at,
                  a.source_name,
                  tal.similarity_score,
                  tal.assignment_score,
                  tal.assignment_method,
                  tal.matched_at
                from user_theme_links utl
                join theme_article_links tal on tal.theme_id = utl.theme_id
                join articles a on a.id = tal.article_id
                where utl.user_id = cast(:user_id as uuid)
                  and utl.theme_id = cast(:theme_id as uuid)
                order by tal.matched_at desc nulls last, a.published_at desc nulls last
                limit :limit
                """
            ),
            {
                "user_id": str(user_id),
                "theme_id": str(theme_id),
                "limit": max(1, int(limit)),
            },
        ).mappings().all()
        return [dict(row) for row in rows]

    @staticmethod
    def _build_slug(title: str, theme_id: uuid.UUID) -> str:
        sanitized = "".join(ch.lower() if ch.isalnum() else "-" for ch in title).strip("-")
        while "--" in sanitized:
            sanitized = sanitized.replace("--", "-")
        base = sanitized or "theme"
        return f"{base}-{str(theme_id)[:8]}"
