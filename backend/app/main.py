"""FastAPI entrypoint for MarketPulse backend."""

from __future__ import annotations

from collections.abc import Generator
from datetime import datetime
import uuid

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.models.user import User
from app.repositories.article_repository import ArticleRepository
from app.repositories.theme_repository import ThemeRepository
from app.repositories.user_repository import UserRepository
from app.repositories.watchlist_repository import WatchlistRepository
from app.services.article_search_service import ArticleSearchService
from app.services.chat_service import ChatService
from app.services.embedding_service import EmbeddingService
from app.services.watchlist_service import WatchlistService
from app.utils.auth import (
    Token,
    create_access_token,
    get_password_hash,
    verify_password,
    verify_token,
)

app = FastAPI(title="MarketPulse API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBearer()


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
    id: str
    slug: str
    canonical_label: str
    status: str
    article_count: int = Field(ge=1)
    last_seen_at: datetime | None = None
    updated_at: datetime | None = None


class WatchlistThemeResponseItem(BaseModel):
    id: str
    slug: str
    canonical_label: str
    summary: str | None = None
    status: str
    discovery_method: str
    scope: str
    owner_user_id: str | None = None
    article_count: int = Field(ge=0)
    first_seen_at: datetime | None = None
    last_seen_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    alerts_enabled: bool | None = None
    watchlisted_at: datetime | None = None


class WatchlistThemeCreateRequest(BaseModel):
    theme_id: str | None = None
    canonical_label: str | None = None
    description: str | None = None
    alerts_enabled: bool = True
    backfill_min_similarity: float | None = Field(default=None, ge=0.0, le=1.0)


class WatchlistThemeCreateResponse(BaseModel):
    created_new_theme: bool
    watchlist_link_created: bool
    theme: WatchlistThemeResponseItem
    backfill_source_themes_count: int = Field(ge=0)
    backfill_source_candidates_count: int = Field(ge=0)
    backfill_inherited_from_themes: int = Field(ge=0)
    backfill_inherited_from_candidates: int = Field(ge=0)
    snapshot_created: bool


class WatchlistThemeArticleResponseItem(BaseModel):
    article_id: str
    canonical_url: str
    title: str | None = None
    description: str | None = None
    published_at: datetime | None = None
    source_name: str
    similarity_score: float = Field(ge=0.0, le=1.0)
    assignment_score: float = Field(ge=0.0, le=1.0)
    assignment_method: str
    matched_at: datetime | None = None


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


class UserCreate(BaseModel):
    email: str = Field(..., min_length=1)
    password: str = Field(..., min_length=8)


class UserLogin(BaseModel):
    email: str = Field(..., min_length=1)
    password: str = Field(..., min_length=1)


class UserResponse(BaseModel):
    id: str
    email: str
    is_active: bool


def get_db_session() -> Generator[Session, None, None]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    session: Session = Depends(get_db_session),
) -> User:
    token_data = verify_token(credentials.credentials)
    if token_data is None or token_data.email is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user_repo = UserRepository(session)
    user = user_repo.get_user_by_email(token_data.email)
    if user is None or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


@app.get("/")
def read_root() -> dict[str, str]:
    return {"message": "Hello World"}


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
    session: Session = Depends(get_db_session),
) -> list[HotThemeResponseItem]:
    theme_repository = ThemeRepository(session)
    rows = theme_repository.list_hot_themes(limit=limit)
    return [
        HotThemeResponseItem(
            id=str(row["id"]),
            slug=str(row["slug"]),
            canonical_label=str(row["canonical_label"]),
            status=str(row["status"]),
            article_count=int(row["article_count"]),
            last_seen_at=row.get("last_seen_at"),
            updated_at=row.get("updated_at"),
        )
        for row in rows
    ]


@app.get("/watchlist/themes", response_model=list[WatchlistThemeResponseItem])
def list_watchlist_themes(
    limit: int = Query(50, ge=1, le=200),
    current_user: User = Depends(get_current_user),
    session: Session = Depends(get_db_session),
) -> list[WatchlistThemeResponseItem]:
    watchlist_service = WatchlistService(
        embedding_service=None,
        watchlist_repository=WatchlistRepository(session),
        theme_repository=ThemeRepository(session),
    )
    rows = watchlist_service.list_watchlist_themes(user_id=current_user.id, limit=limit)
    return [_to_watchlist_theme_response_item(row) for row in rows]


@app.post("/watchlist/themes", response_model=WatchlistThemeCreateResponse)
def create_watchlist_theme(
    payload: WatchlistThemeCreateRequest,
    current_user: User = Depends(get_current_user),
    session: Session = Depends(get_db_session),
) -> WatchlistThemeCreateResponse:
    embedding_service = None if payload.theme_id else EmbeddingService()
    watchlist_service = WatchlistService(
        embedding_service=embedding_service,
        watchlist_repository=WatchlistRepository(session),
        theme_repository=ThemeRepository(session),
    )
    try:
        if payload.theme_id:
            try:
                normalized_theme_id = str(uuid.UUID(payload.theme_id))
            except ValueError as exc:
                raise HTTPException(status_code=400, detail="theme_id must be a valid UUID.") from exc
            result = watchlist_service.watch_existing_theme(
                user_id=current_user.id,
                theme_id=normalized_theme_id,
                alerts_enabled=payload.alerts_enabled,
            )
        else:
            result = watchlist_service.create_custom_theme(
                user_id=current_user.id,
                canonical_label=str(payload.canonical_label or ""),
                description=payload.description,
                alerts_enabled=payload.alerts_enabled,
                backfill_min_similarity=payload.backfill_min_similarity,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    theme = result.get("theme")
    if not isinstance(theme, dict):
        raise HTTPException(status_code=500, detail="Failed to create watchlist theme.")
    return WatchlistThemeCreateResponse(
        created_new_theme=bool(result["created_new_theme"]),
        watchlist_link_created=bool(result["watchlist_link_created"]),
        theme=_to_watchlist_theme_response_item(theme),
        backfill_source_themes_count=int(result["backfill_source_themes_count"]),
        backfill_source_candidates_count=int(result["backfill_source_candidates_count"]),
        backfill_inherited_from_themes=int(result["backfill_inherited_from_themes"]),
        backfill_inherited_from_candidates=int(result["backfill_inherited_from_candidates"]),
        snapshot_created=bool(result["snapshot_created"]),
    )


@app.delete("/watchlist/themes/{theme_id}")
def delete_watchlist_theme(
    theme_id: str,
    current_user: User = Depends(get_current_user),
    session: Session = Depends(get_db_session),
) -> dict[str, bool]:
    try:
        normalized_theme_id = str(uuid.UUID(theme_id))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="theme_id must be a valid UUID.") from exc
    watchlist_service = WatchlistService(
        embedding_service=None,
        watchlist_repository=WatchlistRepository(session),
        theme_repository=ThemeRepository(session),
    )
    removed = watchlist_service.remove_watchlist_theme(user_id=current_user.id, theme_id=normalized_theme_id)
    return {"removed": bool(removed)}


@app.get("/watchlist/themes/{theme_id}/articles", response_model=list[WatchlistThemeArticleResponseItem])
def list_watchlist_theme_articles(
    theme_id: str,
    limit: int = Query(50, ge=1, le=200),
    current_user: User = Depends(get_current_user),
    session: Session = Depends(get_db_session),
) -> list[WatchlistThemeArticleResponseItem]:
    try:
        normalized_theme_id = str(uuid.UUID(theme_id))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="theme_id must be a valid UUID.") from exc
    watchlist_service = WatchlistService(
        embedding_service=None,
        watchlist_repository=WatchlistRepository(session),
        theme_repository=ThemeRepository(session),
    )
    try:
        rows = watchlist_service.list_watchlist_theme_articles(
            user_id=current_user.id,
            theme_id=normalized_theme_id,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return [
        WatchlistThemeArticleResponseItem(
            article_id=str(row["article_id"]),
            canonical_url=str(row["canonical_url"]),
            title=row.get("title"),
            description=row.get("description"),
            published_at=row.get("published_at"),
            source_name=str(row["source_name"]),
            similarity_score=float(row["similarity_score"]),
            assignment_score=float(row["assignment_score"]),
            assignment_method=str(row["assignment_method"]),
            matched_at=row.get("matched_at"),
        )
        for row in rows
    ]


@app.post("/auth/register", response_model=UserResponse)
def register_user(user_data: UserCreate, session: Session = Depends(get_db_session)) -> UserResponse:
    user_repo = UserRepository(session)
    existing_user = user_repo.get_user_by_email(user_data.email)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered",
        )

    hashed_password = get_password_hash(user_data.password)
    user = user_repo.create_user(email=user_data.email, hashed_password=hashed_password)
    return UserResponse(id=str(user.id), email=user.email, is_active=user.is_active)


@app.post("/auth/login", response_model=Token)
def login_user(user_data: UserLogin, session: Session = Depends(get_db_session)) -> Token:
    user_repo = UserRepository(session)
    user = user_repo.get_user_by_email(user_data.email)
    if not user or not verify_password(user_data.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Inactive user",
        )

    access_token = create_access_token(data={"sub": user.email})
    return Token(access_token=access_token, token_type="bearer")


@app.get("/auth/me", response_model=UserResponse)
def get_current_user_info(current_user: User = Depends(get_current_user)) -> UserResponse:
    return UserResponse(id=str(current_user.id), email=current_user.email, is_active=current_user.is_active)


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


def _to_watchlist_theme_response_item(row: dict[str, object]) -> WatchlistThemeResponseItem:
    return WatchlistThemeResponseItem(
        id=str(row["id"]),
        slug=str(row["slug"]),
        canonical_label=str(row["canonical_label"]),
        summary=row.get("summary") if isinstance(row.get("summary"), str) else None,
        status=str(row["status"]),
        discovery_method=str(row["discovery_method"]),
        scope=str(row["scope"]),
        owner_user_id=str(row["owner_user_id"]) if row.get("owner_user_id") else None,
        article_count=int(row.get("article_count") or 0),
        first_seen_at=row.get("first_seen_at") if isinstance(row.get("first_seen_at"), datetime) else None,
        last_seen_at=row.get("last_seen_at") if isinstance(row.get("last_seen_at"), datetime) else None,
        created_at=row.get("created_at") if isinstance(row.get("created_at"), datetime) else None,
        updated_at=row.get("updated_at") if isinstance(row.get("updated_at"), datetime) else None,
        alerts_enabled=bool(row["alerts_enabled"]) if row.get("alerts_enabled") is not None else None,
        watchlisted_at=row.get("watchlisted_at") if isinstance(row.get("watchlisted_at"), datetime) else None,
    )
