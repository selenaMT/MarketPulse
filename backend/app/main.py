"""FastAPI entrypoint for MarketPulse backend."""

from __future__ import annotations

from collections.abc import Generator
from datetime import datetime

from fastapi import Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.repositories.article_repository import ArticleRepository
from app.services.article_search_service import ArticleSearchService
from app.services.embedding_service import EmbeddingService

app = FastAPI(title="MarketPulse API", version="0.1.0")


class SemanticSearchResponseItem(BaseModel):
    article_id: str
    canonical_url: str
    title: str | None = None
    published_at: datetime | None = None
    source_name: str
    similarity: float = Field(ge=-1.0, le=1.0)


def get_db_session() -> Generator[Session, None, None]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@app.get("/health")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/articles/semantic-search", response_model=list[SemanticSearchResponseItem])
def semantic_search_articles(
    keywords: str = Query(..., min_length=1, description="Keyword query text"),
    limit: int = Query(20, ge=1, le=100),
    min_published_at: datetime | None = Query(default=None),
    source_name: str | None = Query(default=None, min_length=1),
    session: Session = Depends(get_db_session),
) -> list[SemanticSearchResponseItem]:
    article_repository = ArticleRepository(session)
    embedding_service = EmbeddingService()
    search_service = ArticleSearchService(
        embedding_service=embedding_service,
        article_repository=article_repository,
    )

    try:
        rows = search_service.search_by_keywords(
            keywords=keywords,
            limit=limit,
            min_published_at=min_published_at,
            source_name=source_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return [
        SemanticSearchResponseItem(
            article_id=str(row["article_id"]),
            canonical_url=str(row["canonical_url"]),
            title=row.get("title"),
            published_at=row.get("published_at"),
            source_name=str(row["source_name"]),
            similarity=float(row["similarity"]),
        )
        for row in rows
    ]
