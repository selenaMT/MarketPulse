"""Repository for article persistence and vector retrieval."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.models.article import Article

TRACKING_PREFIXES = ("utm_", "fbclid", "gclid", "mc_cid", "mc_eid")


class ArticleRepository:
    """Database operations for article records."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def upsert_many(self, articles: list[dict[str, Any]]) -> int:
        """Insert or update articles using canonical_url as dedupe key."""
        rows = [self._to_row(article) for article in articles]
        if not rows:
            return 0

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
        return len(rows)

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
        canonical_url = self._canonicalize_url(url) if url else ""
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
    def _canonicalize_url(url: str) -> str:
        parsed = urlsplit(url.strip())
        if not parsed.scheme or not parsed.netloc:
            return ""
        kept_query_pairs = [
            (k, v)
            for k, v in parse_qsl(parsed.query, keep_blank_values=True)
            if not any(k.lower().startswith(prefix) for prefix in TRACKING_PREFIXES)
        ]
        clean_query = urlencode(kept_query_pairs, doseq=True)
        normalized_path = parsed.path.rstrip("/") or "/"
        return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), normalized_path, clean_query, ""))

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

