"""Repository for theme assignment, candidate promotion, and summary maintenance."""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session


class ThemeRepository:
    """Database operations for semantic theme matching and link maintenance."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def commit(self) -> None:
        self._session.commit()

    def rollback(self) -> None:
        self._session.rollback()

    def find_best_theme(self, embedding: list[float]) -> dict[str, Any] | None:
        vector = self._to_vector_literal(embedding)
        row = self._session.execute(
            text(
                """
                select
                  id,
                  canonical_label as title,
                  coalesce(status, 'active') as status,
                  (1 - (title_embedding <=> cast(:vector as vector)))::double precision as similarity
                from themes
                where title_embedding is not null
                  and coalesce(status, 'active') <> 'retired'
                order by title_embedding <=> cast(:vector as vector)
                limit 1
                """
            ),
            {"vector": vector},
        ).mappings().first()
        return dict(row) if row else None

    def find_best_candidate(self, embedding: list[float]) -> dict[str, Any] | None:
        vector = self._to_vector_literal(embedding)
        row = self._session.execute(
            text(
                """
                select
                  id,
                  display_label as title,
                  status,
                  promoted_theme_id,
                  (1 - (title_embedding <=> cast(:vector as vector)))::double precision as similarity
                from theme_candidates
                where title_embedding is not null
                  and status = 'candidate'
                order by title_embedding <=> cast(:vector as vector)
                limit 1
                """
            ),
            {"vector": vector},
        ).mappings().first()
        return dict(row) if row else None

    def create_or_touch_candidate(
        self,
        title: str,
        title_embedding: list[float],
        observed_at: datetime,
    ) -> dict[str, Any]:
        normalized = self._normalize_label(title)
        vector = self._to_vector_literal(title_embedding)
        row = self._session.execute(
            text(
                """
                insert into theme_candidates (
                  display_label,
                  normalized_label,
                  title_embedding,
                  status,
                  first_seen_at,
                  last_seen_at,
                  created_at,
                  updated_at
                ) values (
                  :display_label,
                  :normalized_label,
                  cast(:vector as vector),
                  'candidate',
                  :observed_at,
                  :observed_at,
                  now(),
                  now()
                )
                on conflict (normalized_label) do update set
                  display_label = excluded.display_label,
                  title_embedding = coalesce(theme_candidates.title_embedding, excluded.title_embedding),
                  last_seen_at = greatest(coalesce(theme_candidates.last_seen_at, excluded.last_seen_at), excluded.last_seen_at),
                  updated_at = now()
                returning id, display_label, status, promoted_theme_id
                """
            ),
            {
                "display_label": title,
                "normalized_label": normalized,
                "vector": vector,
                "observed_at": observed_at,
            },
        ).mappings().one()
        return dict(row)

    def upsert_theme_article_link(
        self,
        theme_id: Any,
        article_id: Any,
        similarity_score: float,
        matched_at: datetime,
    ) -> bool:
        row = self._session.execute(
            text(
                """
                insert into theme_article_links (
                  theme_id,
                  article_id,
                  similarity_score,
                  assignment_score,
                  matched_at,
                  created_at,
                  updated_at
                ) values (
                  :theme_id,
                  :article_id,
                  :similarity_score,
                  :similarity_score,
                  :matched_at,
                  now(),
                  now()
                )
                on conflict (theme_id, article_id) do update set
                  similarity_score = greatest(theme_article_links.similarity_score, excluded.similarity_score),
                  assignment_score = greatest(theme_article_links.assignment_score, excluded.assignment_score),
                  matched_at = greatest(theme_article_links.matched_at, excluded.matched_at),
                  updated_at = now()
                returning (xmax = 0) as inserted
                """
            ),
            {
                "theme_id": theme_id,
                "article_id": article_id,
                "similarity_score": similarity_score,
                "matched_at": matched_at,
            },
        ).mappings().one()
        return bool(row["inserted"])

    def upsert_candidate_article_link(
        self,
        candidate_theme_id: Any,
        article_id: Any,
        similarity_score: float,
        matched_at: datetime,
    ) -> bool:
        row = self._session.execute(
            text(
                """
                insert into candidate_theme_article_links (
                  candidate_theme_id,
                  article_id,
                  similarity_score,
                  matched_at,
                  created_at,
                  updated_at
                ) values (
                  :candidate_theme_id,
                  :article_id,
                  :similarity_score,
                  :matched_at,
                  now(),
                  now()
                )
                on conflict (candidate_theme_id, article_id) do update set
                  similarity_score = greatest(
                    candidate_theme_article_links.similarity_score,
                    excluded.similarity_score
                  ),
                  matched_at = greatest(
                    candidate_theme_article_links.matched_at,
                    excluded.matched_at
                  ),
                  updated_at = now()
                returning (xmax = 0) as inserted
                """
            ),
            {
                "candidate_theme_id": candidate_theme_id,
                "article_id": article_id,
                "similarity_score": similarity_score,
                "matched_at": matched_at,
            },
        ).mappings().one()
        return bool(row["inserted"])

    def touch_theme_seen(self, theme_id: Any, seen_at: datetime) -> None:
        self._session.execute(
            text(
                """
                update themes
                set
                  first_seen_at = coalesce(first_seen_at, :seen_at),
                  last_seen_at = greatest(coalesce(last_seen_at, :seen_at), :seen_at),
                  updated_at = now()
                where id = :theme_id
                """
            ),
            {"theme_id": theme_id, "seen_at": seen_at},
        )

    def touch_candidate_seen(self, candidate_id: Any, seen_at: datetime) -> None:
        self._session.execute(
            text(
                """
                update theme_candidates
                set
                  first_seen_at = coalesce(first_seen_at, :seen_at),
                  last_seen_at = greatest(coalesce(last_seen_at, :seen_at), :seen_at),
                  updated_at = now()
                where id = :candidate_id
                """
            ),
            {"candidate_id": candidate_id, "seen_at": seen_at},
        )

    def recompute_theme_article_count(self, theme_id: Any) -> int:
        row = self._session.execute(
            text(
                """
                update themes
                set
                  article_count = sub.article_count,
                  updated_at = now()
                from (
                  select count(*)::integer as article_count
                  from theme_article_links
                  where theme_id = :theme_id
                ) as sub
                where themes.id = :theme_id
                returning article_count
                """
            ),
            {"theme_id": theme_id},
        ).mappings().first()
        return int(row["article_count"]) if row else 0

    def recompute_candidate_article_count(self, candidate_id: Any) -> int:
        row = self._session.execute(
            text(
                """
                update theme_candidates
                set
                  article_count = sub.article_count,
                  updated_at = now()
                from (
                  select count(*)::integer as article_count
                  from candidate_theme_article_links
                  where candidate_theme_id = :candidate_id
                ) as sub
                where theme_candidates.id = :candidate_id
                returning article_count
                """
            ),
            {"candidate_id": candidate_id},
        ).mappings().first()
        return int(row["article_count"]) if row else 0

    def promote_candidate(self, candidate_id: Any) -> dict[str, Any] | None:
        candidate = self._session.execute(
            text(
                """
                select id, display_label, title_embedding, article_count, first_seen_at, last_seen_at, status
                from theme_candidates
                where id = :candidate_id
                for update
                """
            ),
            {"candidate_id": candidate_id},
        ).mappings().first()
        if not candidate:
            return None
        if candidate["status"] != "candidate":
            return None

        theme_id = uuid.uuid4()
        slug = self._build_slug(str(candidate["display_label"]), theme_id)
        self._session.execute(
            text(
                """
                insert into themes (
                  id,
                  slug,
                  canonical_label,
                  status,
                  discovery_method,
                  summary,
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
                  'active',
                  'candidate_promotion',
                  null,
                  cast(:title_embedding as vector),
                  :article_count,
                  :first_seen_at,
                  :last_seen_at,
                  now(),
                  now()
                )
                """
            ),
            {
                "theme_id": theme_id,
                "slug": slug,
                "canonical_label": candidate["display_label"],
                "title_embedding": self._to_vector_literal(candidate["title_embedding"], allow_none=True),
                "article_count": int(candidate["article_count"] or 0),
                "first_seen_at": candidate["first_seen_at"],
                "last_seen_at": candidate["last_seen_at"],
            },
        )

        self._session.execute(
            text(
                """
                insert into theme_article_links (
                  theme_id,
                  article_id,
                  similarity_score,
                  assignment_score,
                  matched_at,
                  assignment_method,
                  assignment_version,
                  created_at,
                  updated_at
                )
                select
                  :theme_id,
                  ctl.article_id,
                  ctl.similarity_score,
                  ctl.similarity_score,
                  ctl.matched_at,
                  'candidate_promotion',
                  'semantic_v1',
                  now(),
                  now()
                from candidate_theme_article_links ctl
                where ctl.candidate_theme_id = :candidate_id
                on conflict (theme_id, article_id) do update set
                  similarity_score = greatest(theme_article_links.similarity_score, excluded.similarity_score),
                  assignment_score = greatest(theme_article_links.assignment_score, excluded.assignment_score),
                  matched_at = greatest(theme_article_links.matched_at, excluded.matched_at),
                  updated_at = now()
                """
            ),
            {"theme_id": theme_id, "candidate_id": candidate_id},
        )

        self.recompute_theme_article_count(theme_id)
        summary = self.build_theme_summary(theme_id)
        self._session.execute(
            text(
                """
                update themes
                set summary = :summary, updated_at = now()
                where id = :theme_id
                """
            ),
            {"theme_id": theme_id, "summary": summary},
        )
        self.create_snapshot_if_due(
            theme_id=theme_id,
            min_new_articles=1,
            min_age_hours=0,
        )

        self._session.execute(
            text(
                """
                update theme_candidates
                set
                  status = 'promoted',
                  promoted_theme_id = :theme_id,
                  promoted_at = now(),
                  updated_at = now()
                where id = :candidate_id
                """
            ),
            {"candidate_id": candidate_id, "theme_id": theme_id},
        )

        return {
            "theme_id": theme_id,
            "theme_slug": slug,
            "canonical_label": candidate["display_label"],
        }

    def create_snapshot_if_due(
        self,
        theme_id: Any,
        min_new_articles: int,
        min_age_hours: int,
    ) -> bool:
        params = {
            "theme_id": theme_id,
            "min_new_articles": max(int(min_new_articles), 1),
            "min_age_hours": max(int(min_age_hours), 0),
        }
        row = self._session.execute(
            text(
                """
                with locked_theme as (
                  select
                    t.id,
                    t.slug,
                    t.canonical_label,
                    t.summary,
                    t.status,
                    t.discovery_method,
                    t.article_count,
                    t.title_embedding,
                    t.first_seen_at,
                    t.last_seen_at,
                    t.current_snapshot_version,
                    t.last_snapshot_at
                  from themes t
                  where t.id = :theme_id
                  for update
                ),
                thresholds as (
                  select
                    lt.*,
                    (
                      select count(*)::integer
                      from theme_article_links tal
                      where tal.theme_id = lt.id
                        and tal.matched_at > coalesce(lt.last_snapshot_at, '-infinity'::timestamptz)
                    ) as new_articles_since_snapshot,
                    now() as now_ts
                  from locked_theme lt
                ),
                inserted_snapshot as (
                  insert into historical_themes (
                    theme_id,
                    snapshot_version,
                    snapshot_created_at,
                    slug,
                    canonical_label,
                    summary,
                    status,
                    discovery_method,
                    article_count,
                    title_embedding,
                    first_seen_at,
                    last_seen_at,
                    created_at
                  )
                  select
                    th.id,
                    th.current_snapshot_version + 1,
                    th.now_ts,
                    th.slug,
                    th.canonical_label,
                    th.summary,
                    th.status,
                    th.discovery_method,
                    th.article_count,
                    th.title_embedding,
                    th.first_seen_at,
                    th.last_seen_at,
                    now()
                  from thresholds th
                  where th.new_articles_since_snapshot >= :min_new_articles
                    and (
                      th.last_snapshot_at is null
                      or th.now_ts - th.last_snapshot_at >= make_interval(hours => :min_age_hours)
                    )
                  returning theme_id, snapshot_version, snapshot_created_at
                )
                update themes t
                set
                  current_snapshot_version = isnap.snapshot_version,
                  last_snapshot_at = isnap.snapshot_created_at,
                  updated_at = now()
                from inserted_snapshot isnap
                where t.id = isnap.theme_id
                returning t.id
                """
            ),
            params,
        ).first()
        return row is not None

    def build_theme_summary(self, theme_id: Any, max_articles: int = 5) -> str | None:
        rows = self._session.execute(
            text(
                """
                select a.title, a.description
                from theme_article_links tal
                join articles a on a.id = tal.article_id
                where tal.theme_id = :theme_id
                order by tal.matched_at desc nulls last, a.published_at desc nulls last, a.created_at desc
                limit :max_articles
                """
            ),
            {"theme_id": theme_id, "max_articles": max(max_articles, 1)},
        ).mappings().all()
        if not rows:
            return None

        snippets: list[str] = []
        for row in rows:
            title = self._clean_text(row["title"])
            description = self._clean_text(row["description"])
            if title and description:
                snippets.append(f"{title} ({description})")
            elif title:
                snippets.append(title)
            elif description:
                snippets.append(description)
        if not snippets:
            return None
        summary = "Recent associated coverage: " + "; ".join(snippets[:max_articles])
        return summary[:1600]

    @staticmethod
    def utcnow() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _normalize_label(text_value: str) -> str:
        collapsed = " ".join((text_value or "").strip().split())
        return collapsed.lower()

    @staticmethod
    def _clean_text(text_value: Any) -> str | None:
        if not isinstance(text_value, str):
            return None
        normalized = " ".join(text_value.split()).strip()
        return normalized or None

    @staticmethod
    def _build_slug(title: str, theme_id: uuid.UUID) -> str:
        base = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
        if not base:
            base = "theme"
        return f"{base}-{str(theme_id)[:8]}"

    @staticmethod
    def _to_vector_literal(embedding: list[float] | Any, allow_none: bool = False) -> str | None:
        if embedding is None and allow_none:
            return None
        if not isinstance(embedding, list) or not embedding:
            raise ValueError("embedding must be a non-empty float list")
        return "[" + ",".join(str(float(value)) for value in embedding) + "]"
