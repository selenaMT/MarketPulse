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
from app.services.chat_service import ChatService
from app.services.embedding_service import EmbeddingService

app = FastAPI(title="MarketPulse API", version="0.1.0")


class SemanticSearchResponseItem(BaseModel):
    article_id: str
    canonical_url: str
    title: str | None = None
    published_at: datetime | None = None
    source_name: str
    similarity: float = Field(ge=-1.0, le=1.0)


class SourceOptionResponseItem(BaseModel):
    source_name: str
    article_count: int = Field(ge=1)


class ChatAnswerRequest(BaseModel):
    query: str = Field(min_length=1)
    retrieval_limit: int = Field(default=5, ge=1, le=10)
    min_published_at: datetime | None = None
    source_name: str | None = Field(default=None, min_length=1)
    source_names: list[str] | None = None
    model: str | None = None


class ChatAnswerSourceItem(BaseModel):
    index: int = Field(ge=1)
    article_id: str
    canonical_url: str
    title: str | None = None
    published_at: datetime | None = None
    source_name: str
    similarity: float = Field(ge=-1.0, le=1.0)


class ChatAnswerResponse(BaseModel):
    answer: str
    sources: list[ChatAnswerSourceItem]
    model_used: str


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
    source_names: list[str] | None = Query(
        default=None,
        description="Optional repeated source filters, or comma-separated values.",
    ),
    session: Session = Depends(get_db_session),
) -> list[SemanticSearchResponseItem]:
    article_repository = ArticleRepository(session)
    embedding_service = EmbeddingService()
    search_service = ArticleSearchService(
        embedding_service=embedding_service,
        article_repository=article_repository,
    )

    try:
        normalized_source_names = _normalize_source_filters(source_name, source_names)
        rows = search_service.search_by_keywords(
            keywords=keywords,
            limit=limit,
            min_published_at=min_published_at,
            source_names=normalized_source_names,
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


@app.get("/articles/sources", response_model=list[SourceOptionResponseItem])
def list_article_sources(session: Session = Depends(get_db_session)) -> list[SourceOptionResponseItem]:
    article_repository = ArticleRepository(session)
    rows = article_repository.list_sources()
    return [
        SourceOptionResponseItem(
            source_name=str(row["source_name"]),
            article_count=int(row["article_count"]),
        )
        for row in rows
    ]


@app.post("/chat/answer", response_model=ChatAnswerResponse)
def answer_chat(
    payload: ChatAnswerRequest,
    session: Session = Depends(get_db_session),
) -> ChatAnswerResponse:
    article_repository = ArticleRepository(session)
    embedding_service = EmbeddingService()
    chat_service = ChatService(
        embedding_service=embedding_service,
        article_repository=article_repository,
    )

    try:
        normalized_source_names = _normalize_source_filters(payload.source_name, payload.source_names)
        result = chat_service.answer_query(
            payload.query,
            retrieval_limit=payload.retrieval_limit,
            min_published_at=payload.min_published_at,
            source_names=normalized_source_names,
            model=payload.model,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return ChatAnswerResponse(
        answer=str(result["answer"]),
        sources=[
            ChatAnswerSourceItem(
                index=int(row["index"]),
                article_id=str(row["article_id"]),
                canonical_url=str(row["canonical_url"]),
                title=row.get("title"),
                published_at=row.get("published_at"),
                source_name=str(row["source_name"]),
                similarity=float(row["similarity"]),
            )
            for row in result["sources"]
        ],
        model_used=str(result["model_used"]),
    )


def _normalize_source_filters(
    source_name: str | None,
    source_names: list[str] | None,
) -> list[str]:
    merged: list[str] = []
    if source_name:
        merged.append(source_name)
    for raw_value in source_names or []:
        parts = [part.strip() for part in raw_value.split(",")]
        merged.extend([part for part in parts if part])

    deduped: list[str] = []
    seen_lower: set[str] = set()
    for source in merged:
        key = source.lower()
        if key in seen_lower:
            continue
        seen_lower.add(key)
        deduped.append(source)
    return deduped
