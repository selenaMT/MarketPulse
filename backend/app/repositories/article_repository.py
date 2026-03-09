"""Repository for article persistence and vector retrieval."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.models.article import Article
from app.utils.url import canonicalize_url


class ArticleRepository:
    """Database operations for article records."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def upsert_many(self, articles: list[dict[str, Any]]) -> tuple[int, int]:
        """Insert or update articles using canonical_url as dedupe key."""
        rows: list[dict[str, Any]] = []
        invalid_url_count = 0
        for article in articles:
            try:
                rows.append(self._to_row(article))
            except ValueError:
                invalid_url_count += 1
        if not rows:
            return 0, invalid_url_count

        canonical_urls = [row["canonical_url"] for row in rows]
        existing_before = set(
            self._session.execute(
                select(Article.canonical_url).where(Article.canonical_url.in_(canonical_urls))
            ).scalars()
        )

        stmt = insert(Article.__table__).values(rows)
        excluded = stmt.excluded
        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=[Article.__table__.c.canonical_url],
            set_={
                "source_name": excluded.source_name,
                "source_article_id": excluded.source_article_id,
                "url": excluded.url,
                "title": excluded.title,
                "description": excluded.description,
                "content": excluded.content,
                "author": excluded.author,
                "language": excluded.language,
                "published_at": excluded.published_at,
                "embedding": excluded.embedding,
                "embedding_model": excluded.embedding_model,
                "embedded_at": excluded.embedded_at,
                "metadata": excluded.metadata,
                "raw_payload": excluded.raw_payload,
                "updated_at": func.now(),
            },
        )

        self._session.execute(upsert_stmt)
        self._session.commit()
        updated_count = len(existing_before)
        inserted_count = len(rows) - updated_count
        return len(rows), invalid_url_count, inserted_count, updated_count
        
    def get_unprocessed_articles(self, limit: int = 100) -> list[Article]:
        """Get articles that haven't been processed yet (no 'processed' in metadata)."""
        return self._session.query(Article).filter(
            ~Article.metadata_json.contains({"processed": True})
        ).limit(limit).all()

    def update_article_metadata(self, article_id: str, metadata: dict) -> None:
        """Update the metadata of an article."""
        self._session.query(Article).filter(Article.id == article_id).update(
            {"metadata_json": metadata}
        )
        self._session.commit()
        
    def search_similar(
        self,
        query_embedding: list[float],
        limit: int = 20,
        min_published_at: datetime | None = None,
        source_name: str | None = None,
        source_names: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Find similar articles via cosine distance."""
        similarity = (1 - Article.embedding.cosine_distance(query_embedding)).label("similarity")
        query = (
            select(
                Article.id,
                Article.canonical_url,
                Article.title,
                Article.published_at,
                Article.source_name,
                similarity,
            )
            .where(Article.embedding.is_not(None))
            .order_by(Article.embedding.cosine_distance(query_embedding))
            .limit(max(limit, 1))
        )
        if min_published_at is not None:
            query = query.where(Article.published_at >= min_published_at)
        normalized_sources = self._normalize_source_filters(
            source_name=source_name,
            source_names=source_names,
        )
        if normalized_sources:
            query = query.where(Article.source_name.in_(normalized_sources))

        rows = self._session.execute(query).all()
        return [
            {
                "article_id": row.id,
                "canonical_url": row.canonical_url,
                "title": row.title,
                "published_at": row.published_at,
                "source_name": row.source_name,
                "similarity": float(row.similarity),
            }
            for row in rows
        ]

    def list_sources(self) -> list[dict[str, Any]]:
        """Return distinct article sources with document counts."""
        query = (
            select(
                Article.source_name.label("source_name"),
                func.count(Article.id).label("article_count"),
            )
            .where(Article.source_name.is_not(None))
            .where(func.length(func.trim(Article.source_name)) > 0)
            .group_by(Article.source_name)
            .order_by(func.count(Article.id).desc(), Article.source_name.asc())
        )
        rows = self._session.execute(query).all()
        return [
            {
                "source_name": str(row.source_name),
                "article_count": int(row.article_count),
            }
            for row in rows
        ]

    def _to_row(self, article: dict[str, Any]) -> dict[str, Any]:
        source = article.get("source")
        source_name = self._to_optional_str(article.get("source_name")) or "unknown"
        if isinstance(source, dict):
            source_name = str(source.get("name") or source.get("id") or "unknown")
        elif isinstance(source, str) and source.strip():
            source_name = source.strip()
        source_article_id = self._to_optional_str(article.get("source_article_id"))
        if source_article_id is None:
            source_article_id = self._to_optional_str(article.get("source_id"))
        if source_article_id is None and isinstance(source, dict):
            source_article_id = self._to_optional_str(source.get("id"))
        if source_article_id is None and article.get("id") is not None:
            source_article_id = str(article.get("id"))

        url = str(article.get("url") or "").strip()
        canonical_url = canonicalize_url(url) if url else None
        if not canonical_url:
            raise ValueError("Article is missing URL; cannot build canonical_url")

        published_at = self._parse_datetime(article.get("publishedAt") or article.get("published_at"))
        embedding = article.get("embedding")

        return {
            "source_name": source_name,
            "source_article_id": source_article_id,
            "url": url,
            "canonical_url": canonical_url,
            "title": self._to_optional_str(article.get("title")),
            "description": self._to_optional_str(article.get("description")),
            "content": self._to_optional_str(article.get("content")),
            "author": self._to_optional_str(article.get("author")),
            "language": self._to_optional_str(article.get("language")),
            "published_at": published_at,
            "embedding": embedding if isinstance(embedding, list) and embedding else None,
            "embedding_model": "text-embedding-3-small" if embedding else None,
            "embedded_at": datetime.now(timezone.utc) if embedding else None,
            "metadata": {},
            "raw_payload": article,
        }

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        if isinstance(value, datetime):
            return value
        if not isinstance(value, str) or not value.strip():
            return None

        text = value.strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    @staticmethod
    def _to_optional_str(value: Any) -> str | None:
        if isinstance(value, str):
            stripped = value.strip()
            return stripped or None
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _normalize_source_filters(
        source_name: str | None,
        source_names: list[str] | None,
    ) -> list[str]:
        merged: list[str] = []
        if source_name:
            merged.append(source_name)
        if source_names:
            merged.extend(source_names)

        deduped: list[str] = []
        seen_lower: set[str] = set()
        for source in merged:
            normalized = source.strip()
            if not normalized:
                continue
            key = normalized.lower()
            if key in seen_lower:
                continue
            seen_lower.add(key)
            deduped.append(normalized)
        return deduped
