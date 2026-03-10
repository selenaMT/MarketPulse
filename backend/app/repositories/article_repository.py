"""Repository for article persistence and vector retrieval."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import and_, delete, func, or_, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.models.article import Article
from app.utils.url import canonicalize_url


class ArticleRepository:
    """Database operations for article records."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def upsert_many(self, articles: list[dict[str, Any]]) -> tuple[int, int, int, int]:
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
                "region": excluded.region,
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
            or_(
                Article.metadata_json.is_(None),
                ~Article.metadata_json.contains({"processed": True}),
            )
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

    def search_similar_for_chat(
        self,
        query_embedding: list[float],
        limit: int = 5,
        min_published_at: datetime | None = None,
        source_name: str | None = None,
        source_names: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Return similar articles with extra fields for grounded chat responses."""
        similarity = (1 - Article.embedding.cosine_distance(query_embedding)).label("similarity")
        query = (
            select(
                Article.id,
                Article.canonical_url,
                Article.title,
                Article.description,
                Article.content,
                Article.published_at,
                Article.source_name,
                Article.region,
                Article.metadata_json,
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
                "description": row.description,
                "content": row.content,
                "published_at": row.published_at,
                "source_name": row.source_name,
                "region": row.region,
                "metadata": row.metadata_json or {},
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

    def delete_by_canonical_urls(self, canonical_urls: list[str]) -> int:
        """Delete articles by canonical URL and return deleted row count."""
        normalized = [url.strip() for url in canonical_urls if isinstance(url, str) and url.strip()]
        if not normalized:
            return 0

        stmt = delete(Article).where(Article.canonical_url.in_(normalized))
        result = self._session.execute(stmt)
        self._session.commit()
        return int(result.rowcount or 0)

    def delete_by_ids(self, article_ids: list[Any]) -> int:
        """Delete articles by primary key and return deleted row count."""
        normalized = [article_id for article_id in article_ids if article_id]
        if not normalized:
            return 0

        stmt = delete(Article).where(Article.id.in_(normalized))
        result = self._session.execute(stmt)
        self._session.commit()
        return int(result.rowcount or 0)

    def backfill_region_from_metadata(self) -> int:
        """Fill region column from metadata.text_processing.region when available."""
        stmt = text(
            """
            update articles
            set region = nullif(metadata->'text_processing'->>'region', '')
            where region is null
              and metadata ? 'text_processing'
            """
        )
        result = self._session.execute(stmt)
        self._session.commit()
        return int(result.rowcount or 0)

    def list_missing_text_processing(
        self,
        limit: int,
        include_existing: bool = False,
        after_created_at: datetime | None = None,
        after_id: Any | None = None,
    ) -> list[dict[str, Any]]:
        """Return candidate rows for text_processing backfill."""
        if limit <= 0:
            return []

        metadata_column = Article.__table__.c.metadata
        query = (
            select(
                Article.id,
                Article.canonical_url,
                Article.title,
                Article.description,
                Article.content,
                Article.region,
                Article.created_at,
            )
            .order_by(Article.created_at.asc(), Article.id.asc())
            .limit(limit)
        )

        if not include_existing:
            query = query.where(
                or_(
                    metadata_column.is_(None),
                    ~metadata_column.op("?")("text_processing"),
                )
            )

        if after_created_at is not None and after_id is not None:
            query = query.where(
                or_(
                    Article.created_at > after_created_at,
                    and_(Article.created_at == after_created_at, Article.id > after_id),
                )
            )

        rows = self._session.execute(query).all()
        return [
            {
                "article_id": row.id,
                "canonical_url": row.canonical_url,
                "title": row.title,
                "description": row.description,
                "content": row.content,
                "region": row.region,
                "created_at": row.created_at,
            }
            for row in rows
        ]

    def apply_text_processing_updates(
        self,
        updates: list[tuple[Any, dict[str, Any]]],
        replace_metadata: bool = False,
    ) -> int:
        """Persist text_processing payload to metadata and set region from payload."""
        if not updates:
            return 0

        updated_count = 0
        for article_id, payload in updates:
            article = self._session.get(Article, article_id)
            if article is None:
                continue

            if replace_metadata:
                metadata = {"text_processing": payload}
            else:
                metadata = dict(article.metadata_json or {})
                metadata["text_processing"] = payload
            article.metadata_json = metadata
            region = self._to_optional_str(payload.get("region"))
            article.region = region
            updated_count += 1

        self._session.commit()
        return updated_count

    def get_by_canonical_urls(self, canonical_urls: list[str]) -> list[dict[str, Any]]:
        """Return article records keyed by canonical URL."""
        normalized = [url.strip() for url in canonical_urls if isinstance(url, str) and url.strip()]
        if not normalized:
            return []

        query = select(
            Article.id,
            Article.canonical_url,
            Article.title,
            Article.description,
            Article.content,
            Article.published_at,
            Article.metadata_json,
        ).where(Article.canonical_url.in_(normalized))
        rows = self._session.execute(query).all()
        return [
            {
                "article_id": row.id,
                "canonical_url": row.canonical_url,
                "title": row.title,
                "description": row.description,
                "content": row.content,
                "published_at": row.published_at,
                "metadata": row.metadata_json or {},
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
        metadata = self._build_metadata(article)
        region = self._extract_region(article)

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
            "region": region,
            "published_at": published_at,
            "embedding": embedding if isinstance(embedding, list) and embedding else None,
            "embedding_model": "text-embedding-3-small" if embedding else None,
            "embedded_at": datetime.now(timezone.utc) if embedding else None,
            "metadata": metadata,
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

    @staticmethod
    def _build_metadata(article: dict[str, Any]) -> dict[str, Any]:
        metadata: dict[str, Any] = {}
        text_processing = article.get("text_processing")
        if isinstance(text_processing, dict):
            metadata["text_processing"] = text_processing
        return metadata

    def _extract_region(self, article: dict[str, Any]) -> str | None:
        direct_region = self._to_optional_str(article.get("region"))
        if direct_region:
            return direct_region

        text_processing = article.get("text_processing")
        if isinstance(text_processing, dict):
            return self._to_optional_str(text_processing.get("region"))
        return None
