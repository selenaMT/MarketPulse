"""FastAPI entrypoint for MarketPulse backend."""

from __future__ import annotations

from collections.abc import Generator
from datetime import datetime

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.models.user import User
from app.repositories.article_repository import ArticleRepository
from app.repositories.user_repository import UserRepository
from app.services.article_search_service import ArticleSearchService
from app.services.chat_service import ChatService
from app.services.embedding_service import EmbeddingService
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
