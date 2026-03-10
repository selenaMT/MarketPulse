"""Repository for theme assignment, candidate promotion, and summary maintenance."""

from __future__ import annotations

import math
import re
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.exc import ProgrammingError
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

    def list_hot_themes(self, limit: int = 10) -> list[dict[str, Any]]:
        try:
            rows = self._session.execute(
                text(
                    """
                    select
                      id,
                      slug,
                      canonical_label,
                      article_count,
                      status,
                      last_seen_at,
                      updated_at
                    from themes
                    where coalesce(status, 'active') <> 'retired'
                      and coalesce(scope, 'global') = 'global'
                      and article_count > 0
                    order by article_count desc, last_seen_at desc nulls last, updated_at desc
                    limit :limit
                    """
                ),
                {"limit": max(1, int(limit))},
            ).mappings().all()
        except ProgrammingError as exc:
            if not self._is_missing_scope_column(exc):
                raise
            self._session.rollback()
            rows = self._session.execute(
                text(
                    """
                    select
                      id,
                      slug,
                      canonical_label,
                      article_count,
                      status,
                      last_seen_at,
                      updated_at
                    from themes
                    where coalesce(status, 'active') <> 'retired'
                      and article_count > 0
                    order by article_count desc, last_seen_at desc nulls last, updated_at desc
                    limit :limit
                    """
                ),
                {"limit": max(1, int(limit))},
            ).mappings().all()
        return [dict(row) for row in rows]

    def find_best_theme(self, embedding: list[float]) -> dict[str, Any] | None:
        vector = self._to_vector_literal(embedding)
        try:
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
                      and coalesce(scope, 'global') = 'global'
                    order by title_embedding <=> cast(:vector as vector)
                    limit 1
                    """
                ),
                {"vector": vector},
            ).mappings().first()
        except ProgrammingError as exc:
            if not self._is_missing_scope_column(exc):
                raise
            self._session.rollback()
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

    def find_matching_user_themes(
        self,
        embedding: list[float],
        min_similarity: float,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        vector = self._to_vector_literal(embedding)
        try:
            rows = self._session.execute(
                text(
                    """
                    select
                      id,
                      owner_user_id,
                      canonical_label as title,
                      coalesce(status, 'active') as status,
                      (1 - (title_embedding <=> cast(:vector as vector)))::double precision as similarity
                    from themes
                    where title_embedding is not null
                      and scope = 'user'
                      and coalesce(status, 'active') <> 'retired'
                      and exists (
                        select 1
                        from user_theme_links utl
                        where utl.theme_id = themes.id
                      )
                      and (1 - (title_embedding <=> cast(:vector as vector))) >= :min_similarity
                    order by title_embedding <=> cast(:vector as vector)
                    limit :limit
                    """
                ),
                {
                    "vector": vector,
                    "min_similarity": float(min_similarity),
                    "limit": max(1, int(limit)),
                },
            ).mappings().all()
        except ProgrammingError as exc:
            # Backward-compat mode before watchlist schema migration exists.
            if not (self._is_missing_scope_column(exc) or self._is_missing_user_theme_links_table(exc)):
                raise
            self._session.rollback()
            return []
        return [dict(row) for row in rows]

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

    def refresh_theme_seen_bounds(self, theme_id: Any) -> None:
        self._session.execute(
            text(
                """
                update themes
                set
                  first_seen_at = bounds.first_seen_at,
                  last_seen_at = bounds.last_seen_at,
                  updated_at = now()
                from (
                  select
                    min(matched_at) as first_seen_at,
                    max(matched_at) as last_seen_at
                  from theme_article_links
                  where theme_id = :theme_id
                ) as bounds
                where themes.id = :theme_id
                """
            ),
            {"theme_id": theme_id},
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
                returning themes.article_count as article_count
                """
            ),
            {"theme_id": theme_id},
        ).mappings().first()
        return int(row["article_count"]) if row else 0

    def update_theme_summary(self, theme_id: Any, summary: str | None) -> None:
        self._session.execute(
            text(
                """
                update themes
                set
                  summary = :summary,
                  updated_at = now()
                where id = :theme_id
                """
            ),
            {"theme_id": theme_id, "summary": summary},
        )

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
                returning theme_candidates.article_count as article_count
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
                  scope,
                  owner_user_id,
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
                  'global',
                  null,
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
        _ = min_new_articles
        _ = min_age_hours
        theme = self._session.execute(
            text(
                """
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
                """
            ),
            {"theme_id": theme_id},
        ).mappings().first()
        if not theme:
            return False

        theme_article_count = int(theme["article_count"] or 0)
        effective_min_new_articles = max(4, int(math.ceil(theme_article_count * 0.25)))

        baseline_count = 0
        current_snapshot_version = int(theme["current_snapshot_version"] or 0)
        if current_snapshot_version > 0:
            baseline = self._session.execute(
                text(
                    """
                    select article_count
                    from historical_themes
                    where theme_id = :theme_id
                      and snapshot_version = :snapshot_version
                    limit 1
                    """
                ),
                {
                    "theme_id": theme["id"],
                    "snapshot_version": current_snapshot_version,
                },
            ).mappings().first()
            baseline_count = int(baseline["article_count"]) if baseline else 0

        new_article_count = max(theme_article_count - baseline_count, 0)
        if new_article_count < effective_min_new_articles:
            return False
        now_ts = self.utcnow()

        next_snapshot_version = int(theme["current_snapshot_version"] or 0) + 1
        self._session.execute(
            text(
                """
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
                values (
                  :theme_id,
                  :snapshot_version,
                  :snapshot_created_at,
                  :slug,
                  :canonical_label,
                  :summary,
                  :status,
                  :discovery_method,
                  :article_count,
                  :title_embedding,
                  :first_seen_at,
                  :last_seen_at,
                  now()
                )
                """
            ),
            {
                "theme_id": theme["id"],
                "snapshot_version": next_snapshot_version,
                "snapshot_created_at": now_ts,
                "slug": theme["slug"],
                "canonical_label": theme["canonical_label"],
                "summary": theme["summary"],
                "status": theme["status"],
                "discovery_method": theme["discovery_method"],
                "article_count": theme["article_count"],
                "title_embedding": self._to_vector_literal(theme["title_embedding"], allow_none=True),
                "first_seen_at": theme["first_seen_at"],
                "last_seen_at": theme["last_seen_at"],
            },
        )

        new_articles = self._session.execute(
            text(
                """
                select
                  a.title,
                  a.description,
                  a.content
                from theme_article_links tal
                join articles a on a.id = tal.article_id
                where tal.theme_id = :theme_id
                order by tal.matched_at desc nulls last
                limit :max_articles
                """
            ),
            {
                "theme_id": theme["id"],
                "max_articles": min(max(new_article_count, 1), 8),
            },
        ).mappings().all()

        refreshed_summary = self._build_snapshot_summary(
            previous_summary=self._clean_text(theme["summary"]),
            new_articles=[dict(row) for row in new_articles],
        )

        self._session.execute(
            text(
                """
                update themes
                set
                  current_snapshot_version = :snapshot_version,
                  last_snapshot_at = :last_snapshot_at,
                  summary = :summary,
                  updated_at = now()
                where id = :theme_id
                """
            ),
            {
                "theme_id": theme["id"],
                "snapshot_version": next_snapshot_version,
                "last_snapshot_at": now_ts,
                "summary": refreshed_summary,
            },
        )
        return True

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

    def _build_snapshot_summary(
        self,
        previous_summary: str | None,
        new_articles: list[dict[str, Any]],
    ) -> str | None:
        sections: list[str] = []
        baseline = self._clean_text(previous_summary)
        if baseline:
            sections.append(f"Previous summary: {baseline}")

        article_snippets: list[str] = []
        for article in new_articles:
            title = self._clean_text(article.get("title"))
            description = self._clean_text(article.get("description"))
            content = self._clean_text(article.get("content"))
            parts: list[str] = []
            if title:
                parts.append(f"Title: {title}")
            if description:
                parts.append(f"Description: {description}")
            if content:
                parts.append(f"Content: {self._truncate_text(content, 450)}")
            if parts:
                article_snippets.append(" | ".join(parts))

        if article_snippets:
            sections.append("New linked articles: " + " ; ".join(article_snippets))

        if not sections:
            return baseline
        return self._truncate_text(" ".join(sections), 8000)

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
    def _truncate_text(text_value: str, max_chars: int) -> str:
        if len(text_value) <= max_chars:
            return text_value
        if max_chars <= 3:
            return text_value[:max_chars]
        return text_value[: max_chars - 3].rstrip() + "..."

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

        if isinstance(embedding, str):
            text_value = embedding.strip()
            if text_value.startswith("[") and text_value.endswith("]"):
                payload = text_value[1:-1].strip()
                if not payload:
                    if allow_none:
                        return None
                    raise ValueError("embedding must be non-empty")
                vector_values = [float(part.strip()) for part in payload.split(",") if part.strip()]
                if not vector_values:
                    if allow_none:
                        return None
                    raise ValueError("embedding must be non-empty")
                return "[" + ",".join(str(value) for value in vector_values) + "]"
            raise ValueError("embedding string must use [v1,v2,...] format")

        if isinstance(embedding, tuple):
            embedding = list(embedding)
        elif not isinstance(embedding, list) and hasattr(embedding, "__iter__"):
            embedding = list(embedding)

        if not isinstance(embedding, list) or not embedding:
            raise ValueError("embedding must be a non-empty float list")
        return "[" + ",".join(str(float(value)) for value in embedding) + "]"

    @staticmethod
    def _is_missing_scope_column(exc: Exception) -> bool:
        message = str(exc).lower()
        return "scope" in message and "does not exist" in message

    @staticmethod
    def _is_missing_user_theme_links_table(exc: Exception) -> bool:
        message = str(exc).lower()
        return "user_theme_links" in message and "does not exist" in message
