"""FastAPI entrypoint for MarketPulse backend."""

from __future__ import annotations

from collections.abc import Generator
from datetime import datetime
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.pipelines.theme_management_pipeline import ThemeManagementPipeline
from app.repositories.article_repository import ArticleRepository
from app.repositories.theme_repository import ThemeRepository
from app.services.article_search_service import ArticleSearchService
from app.services.embedding_service import EmbeddingService
from app.services.theme_management_service import ThemeManagementService

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


class HotThemeResponseItem(BaseModel):
    theme_id: str
    slug: str
    canonical_label: str
    summary: str | None = None
    status: str
    discovery_method: str
    first_seen_at: datetime | None = None
    last_seen_at: datetime | None = None
    last_metric_at: datetime | None = None
    article_count_3d: int = Field(ge=0)
    prev_article_count_3d: int = Field(ge=0)
    article_count_7d: int = Field(ge=0)
    acceleration_3d: int
    source_diversity_score: float
    avg_assignment_score: float
    recency_weighted_count: float
    hot_score: float
    trend: str


class ThemeOverviewResponse(BaseModel):
    theme_id: str
    slug: str
    canonical_label: str
    summary: str | None = None
    status: str
    discovery_method: str
    first_seen_at: datetime | None = None
    last_seen_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    total_articles: int = Field(ge=0)
    source_diversity: int = Field(ge=0)
    avg_assignment_score: float
    aliases: list[str]
    trend: str


class ThemeTimelinePointResponse(BaseModel):
    bucket_start: datetime
    article_count: int = Field(ge=0)
    source_count: int = Field(ge=0)
    momentum_score: float
    avg_assignment_score: float
    avg_market_tone_score: float


class ThemeRelatedThemeResponseItem(BaseModel):
    theme_id: str
    slug: str
    canonical_label: str
    status: str
    relation_type: str
    relation_score: float
    evidence_count: int = Field(ge=0)
    last_observed_at: datetime | None = None


class ThemeDevelopmentResponseItem(BaseModel):
    article_id: str
    canonical_url: str
    title: str | None = None
    published_at: datetime | None = None
    source_name: str
    assignment_score: float
    assignment_method: str
    assignment_version: str
    alias_score: float
    semantic_score: float
    entity_overlap_score: float
    asset_overlap_score: float
    relationship_overlap_score: float
    margin_score: float
    assignment_rationale: dict[str, Any] = Field(default_factory=dict)
    event: str | None = None
    narratives: list[str]


class ThemeRelatedResponse(BaseModel):
    theme_id: str
    slug: str
    canonical_label: str
    related_themes: list[ThemeRelatedThemeResponseItem]
    developments: list[ThemeDevelopmentResponseItem]


class ThemeSyncResponse(BaseModel):
    assignment_input_count: int = Field(ge=0)
    assigned_articles: int = Field(ge=0)
    theme_links_upserted: int = Field(ge=0)
    created_themes: int = Field(ge=0)
    promoted_candidates: int = Field(ge=0)
    abstained_articles: int = Field(ge=0)
    abstained_signals: int = Field(ge=0)
    candidate_observations: int = Field(ge=0)
    assignment_rate: float = Field(ge=0.0, le=1.0)
    abstain_rate: float = Field(ge=0.0, le=1.0)
    snapshots_upserted: int = Field(ge=0)
    relations_upserted: int = Field(ge=0)
    status_updates: int = Field(ge=0)
    centroids_rebuilt: int = Field(ge=0)
    recommendation_count: int = Field(ge=0)
    merge_recommendations: int = Field(ge=0)
    split_recommendations: int = Field(ge=0)


class ThemeRecommendationResponseItem(BaseModel):
    recommendation_id: str
    recommendation_type: str
    source_theme_id: str
    source_slug: str
    source_label: str
    target_theme_id: str | None = None
    target_slug: str | None = None
    target_label: str | None = None
    confidence_score: float
    status: str
    rationale: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime


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


@app.get("/themes/hot", response_model=list[HotThemeResponseItem])
def list_hot_themes(
    limit: int = Query(10, ge=1, le=50),
    lookback_days: int = Query(30, ge=3, le=365),
    session: Session = Depends(get_db_session),
) -> list[HotThemeResponseItem]:
    theme_repository = ThemeRepository(session)
    theme_service = ThemeManagementService(theme_repository=theme_repository)
    rows = theme_service.get_hot_themes(limit=limit, lookback_days=lookback_days)
    return [HotThemeResponseItem(**row) for row in rows]


@app.get("/themes/recommendations", response_model=list[ThemeRecommendationResponseItem])
def list_theme_recommendations(
    limit: int = Query(20, ge=1, le=200),
    recommendation_type: str | None = Query(default=None, pattern="^(merge|split)$"),
    status: str = Query(default="suggested", pattern="^(suggested|applied|dismissed)$"),
    session: Session = Depends(get_db_session),
) -> list[ThemeRecommendationResponseItem]:
    theme_repository = ThemeRepository(session)
    theme_service = ThemeManagementService(theme_repository=theme_repository)
    rows = theme_service.get_theme_recommendations(
        limit=limit,
        recommendation_type=recommendation_type,
        status=status,
    )
    return [ThemeRecommendationResponseItem(**row) for row in rows]


@app.get("/themes/{theme_ref}", response_model=ThemeOverviewResponse)
def get_theme_overview(
    theme_ref: str,
    session: Session = Depends(get_db_session),
) -> ThemeOverviewResponse:
    theme_repository = ThemeRepository(session)
    theme_service = ThemeManagementService(theme_repository=theme_repository)
    payload = theme_service.get_theme_overview(theme_ref)
    if payload is None:
        raise HTTPException(status_code=404, detail="Theme not found")
    return ThemeOverviewResponse(**payload)


@app.get("/themes/{theme_ref}/timeline", response_model=list[ThemeTimelinePointResponse])
def get_theme_timeline(
    theme_ref: str,
    days: int = Query(30, ge=7, le=365),
    session: Session = Depends(get_db_session),
) -> list[ThemeTimelinePointResponse]:
    theme_repository = ThemeRepository(session)
    theme_service = ThemeManagementService(theme_repository=theme_repository)
    payload = theme_service.get_theme_timeline(theme_ref, days=days)
    if payload is None:
        raise HTTPException(status_code=404, detail="Theme not found")
    return [ThemeTimelinePointResponse(**row) for row in payload]


@app.get("/themes/{theme_ref}/related", response_model=ThemeRelatedResponse)
def get_theme_related(
    theme_ref: str,
    related_limit: int = Query(8, ge=1, le=50),
    development_limit: int = Query(10, ge=1, le=50),
    session: Session = Depends(get_db_session),
) -> ThemeRelatedResponse:
    theme_repository = ThemeRepository(session)
    theme_service = ThemeManagementService(theme_repository=theme_repository)
    payload = theme_service.get_related_developments(
        theme_ref,
        related_limit=related_limit,
        development_limit=development_limit,
    )
    if payload is None:
        raise HTTPException(status_code=404, detail="Theme not found")
    return ThemeRelatedResponse(**payload)


@app.post("/themes/sync", response_model=ThemeSyncResponse)
def sync_themes(
    assignment_limit: int = Query(500, ge=1, le=5000),
    assignment_lookback_days: int | None = Query(default=None, ge=1, le=3650),
    snapshot_lookback_days: int = Query(90, ge=7, le=730),
    relation_lookback_days: int = Query(45, ge=7, le=730),
    run_maintenance: bool = Query(default=False),
    rebuild_centroids: bool = Query(default=False),
    record_run: bool = Query(default=True),
    session: Session = Depends(get_db_session),
) -> ThemeSyncResponse:
    theme_repository = ThemeRepository(session)
    theme_service = ThemeManagementService(theme_repository=theme_repository)
    pipeline = ThemeManagementPipeline(
        theme_repository=theme_repository,
        theme_management_service=theme_service,
    )
    result = pipeline.run(
        assignment_limit=assignment_limit,
        assignment_lookback_days=assignment_lookback_days,
        snapshot_lookback_days=snapshot_lookback_days,
        relation_lookback_days=relation_lookback_days,
        run_maintenance=run_maintenance,
        rebuild_centroids=rebuild_centroids,
        record_run=record_run,
    )
    return ThemeSyncResponse(**result)


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
