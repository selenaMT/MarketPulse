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

    def search_similar(
        self,
        query_embedding: list[float],
        limit: int = 20,
        min_published_at: datetime | None = None,
        source_name: str | None = None,
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
        if source_name:
            query = query.where(Article.source_name == source_name)

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

    def _to_row(self, article: dict[str, Any]) -> dict[str, Any]:
        source = article.get("source")
        source_name = "unknown"
        if isinstance(source, dict):
            source_name = str(source.get("name") or source.get("id") or "unknown")
        elif isinstance(source, str) and source.strip():
            source_name = source.strip()

        url = str(article.get("url") or "").strip()
        canonical_url = canonicalize_url(url) if url else None
        if not canonical_url:
            raise ValueError("Article is missing URL; cannot build canonical_url")

        published_at = self._parse_datetime(article.get("publishedAt") or article.get("published_at"))
        embedding = article.get("embedding")

        return {
            "source_name": source_name,
            "source_article_id": str(article.get("id")) if article.get("id") is not None else None,
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
