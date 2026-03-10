"""Grounded chatbot service backed by article retrieval + OpenAI synthesis."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from openai import OpenAI

from app.repositories.article_repository import ArticleRepository
from app.services.embedding_service import EmbeddingService

CHAT_SYSTEM_PROMPT = (
    "You are MarketPulse, a macroeconomic news analyst.\n"
    "Answer the user's question using only the provided article context.\n"
    "If the context is insufficient, say so explicitly and avoid unsupported claims.\n"
    "Prefer concise synthesis over repeating the articles.\n"
    "When referring to evidence, cite the source numbers inline like [1] or [2].\n"
    "If the user is making small talk or asks about something irrelevant to macro or markets, reply briefly and warmly, "
    "then gently guide them to browse the linked articles to see whether any topic is relevant to their interest."
)

MAX_HISTORY_MESSAGES = 6
MAX_CONTEXT_CHARS = 280


class ChatService:
    """Retrieves relevant articles and synthesizes an answer with OpenAI."""

    def __init__(
        self,
        embedding_service: EmbeddingService,
        article_repository: ArticleRepository,
        client: OpenAI | None = None,
        default_model: str = "gpt-4o-mini",
    ) -> None:
        self._embedding_service = embedding_service
        self._article_repository = article_repository
        self._client = client or embedding_service._client
        self._default_model = default_model

    def answer_query(
        self,
        query: str,
        *,
        retrieval_limit: int = 5,
        min_published_at: datetime | None = None,
        source_name: str | None = None,
        source_names: list[str] | None = None,
        conversation_history: list[dict[str, str]] | None = None,
        model: str | None = None,
    ) -> dict[str, Any]:
        normalized_query = query.strip()
        if not normalized_query:
            raise ValueError("query must be non-empty")

        recent_context = self._summarize_recent_context(conversation_history)
        retrieval_query = self._build_retrieval_query(
            normalized_query,
            recent_context=recent_context,
        )
        query_embedding = self._embedding_service.embed([retrieval_query])[0]
        articles = self._article_repository.search_similar_for_chat(
            query_embedding=query_embedding,
            limit=retrieval_limit,
            min_published_at=min_published_at,
            source_name=source_name,
            source_names=source_names,
        )
        if not articles:
            return {
                "answer": "I could not find relevant articles in the current MarketPulse dataset.",
                "sources": [],
                "model_used": model or self._default_model,
            }

        response = self._client.responses.create(
            model=model or self._default_model,
            input=[
                {"role": "system", "content": CHAT_SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": self._build_user_prompt(
                        normalized_query,
                        articles,
                        recent_context=recent_context,
                    ),
                },
            ],
        )

        return {
            "answer": response.output_text.strip(),
            "sources": [self._to_source_item(index, article) for index, article in enumerate(articles, start=1)],
            "model_used": response.model,
        }

    def _build_user_prompt(
        self,
        query: str,
        articles: list[dict[str, Any]],
        *,
        recent_context: str,
    ) -> str:
        context_blocks: list[str] = []
        for index, article in enumerate(articles, start=1):
            metadata = article.get("metadata")
            text_processing = metadata.get("text_processing") if isinstance(metadata, dict) else {}
            if not isinstance(text_processing, dict):
                text_processing = {}

            entities = self._join_entity_names(text_processing.get("entities"))
            narratives = self._join_strings(text_processing.get("narratives"))
            snippet = self._build_snippet(article)
            published_at = article.get("published_at")
            published_text = published_at.isoformat() if isinstance(published_at, datetime) else "unknown"

            context_blocks.append(
                "\n".join(
                    [
                        f"[{index}] Title: {article.get('title') or 'Untitled'}",
                        f"Source: {article.get('source_name') or 'unknown'}",
                        f"Published At: {published_text}",
                        f"Region: {article.get('region') or text_processing.get('region') or 'unknown'}",
                        f"Similarity: {article.get('similarity', 0.0):.3f}",
                        f"Event: {text_processing.get('event') or 'n/a'}",
                        f"Narratives: {narratives or 'n/a'}",
                        f"Entities: {entities or 'n/a'}",
                        f"Snippet: {snippet}",
                        f"URL: {article.get('canonical_url') or 'n/a'}",
                    ]
                )
            )

        context_text = "\n\n".join(context_blocks)
        return (
            f"Recent conversation context:\n{recent_context}\n\n"
            f"Latest user question: {query}\n\n"
            f"Article context:\n{context_text}"
        )

    @staticmethod
    def _build_snippet(article: dict[str, Any], max_chars: int = 800) -> str:
        candidates = [article.get("content"), article.get("description"), article.get("title")]
        for candidate in candidates:
            if isinstance(candidate, str):
                normalized = " ".join(candidate.split()).strip()
                if normalized:
                    return normalized[:max_chars]
        return "n/a"

    @staticmethod
    def _join_strings(value: Any) -> str:
        if not isinstance(value, list):
            return ""
        return ", ".join(item.strip() for item in value if isinstance(item, str) and item.strip())

    @staticmethod
    def _join_entity_names(value: Any) -> str:
        if not isinstance(value, list):
            return ""
        names: list[str] = []
        for item in value:
            if isinstance(item, dict):
                name = item.get("name")
                if isinstance(name, str) and name.strip():
                    names.append(name.strip())
            elif isinstance(item, str) and item.strip():
                names.append(item.strip())
        return ", ".join(names)

    @staticmethod
    def _summarize_recent_context(conversation_history: list[dict[str, str]] | None) -> str:
        if not conversation_history:
            return "n/a"

        fragments: list[str] = []
        for item in conversation_history[-MAX_HISTORY_MESSAGES:]:
            role = item.get("role", "").strip().lower()
            content = " ".join(item.get("content", "").split()).strip()
            if role not in {"user", "assistant"} or not content:
                continue
            label = "User asked" if role == "user" else "Assistant answered"
            fragments.append(f"{label} {content.rstrip('.!?')}.")
            summary = " ".join(fragments)
            if len(summary) > MAX_CONTEXT_CHARS:
                break

        summary = " ".join(fragments).strip()
        if not summary:
            return "n/a"
        if len(summary) <= MAX_CONTEXT_CHARS:
            return summary
        return summary[: MAX_CONTEXT_CHARS - 3].rstrip() + "..."

    @staticmethod
    def _build_retrieval_query(query: str, *, recent_context: str) -> str:
        if recent_context == "n/a":
            return query
        return f"{query}\n\nRecent context: {recent_context}"

    @staticmethod
    def _to_source_item(index: int, article: dict[str, Any]) -> dict[str, Any]:
        published_at = article.get("published_at")
        return {
            "index": index,
            "article_id": str(article["article_id"]),
            "canonical_url": str(article["canonical_url"]),
            "title": article.get("title"),
            "published_at": published_at,
            "source_name": str(article["source_name"]),
            "similarity": float(article["similarity"]),
        }
