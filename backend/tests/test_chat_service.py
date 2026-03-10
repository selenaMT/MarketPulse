from __future__ import annotations

from datetime import datetime, timezone

from app.services.chat_service import ChatService


class FakeEmbeddingService:
    def __init__(self, vector: list[float]) -> None:
        self.vector = vector
        self.calls: list[list[str]] = []
        self._client = None

    def embed(self, texts: list[str]) -> list[list[float]]:
        self.calls.append(texts)
        return [self.vector]


class FakeArticleRepository:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.calls: list[dict[str, object]] = []

    def search_similar_for_chat(
        self,
        query_embedding: list[float],
        limit: int = 5,
        min_published_at: datetime | None = None,
        source_name: str | None = None,
        source_names: list[str] | None = None,
    ) -> list[dict[str, object]]:
        self.calls.append(
            {
                "query_embedding": query_embedding,
                "limit": limit,
                "min_published_at": min_published_at,
                "source_name": source_name,
                "source_names": source_names,
            }
        )
        return self.rows


class FakeResponsesApi:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return type("FakeResponse", (), {"output_text": "Grounded answer [1]", "model": "fake-model"})()


class FakeOpenAIClient:
    def __init__(self) -> None:
        self.responses = FakeResponsesApi()


def test_answer_query_embeds_latest_question_and_includes_conversation_history():
    embedding_service = FakeEmbeddingService(vector=[0.1, 0.2, 0.3])
    article_repository = FakeArticleRepository(
        rows=[
            {
                "article_id": "abc",
                "canonical_url": "https://example.com/article",
                "title": "Treasury yields push higher",
                "published_at": datetime(2026, 3, 10, tzinfo=timezone.utc),
                "source_name": "Reuters",
                "similarity": 0.91,
                "region": "US",
                "content": "Yields rose after stronger data and fresh tariff headlines.",
                "metadata": {
                    "text_processing": {
                        "event": "Treasury selloff",
                        "narratives": ["hotter growth", "sticky inflation"],
                        "entities": [{"name": "Federal Reserve"}],
                    }
                },
            }
        ]
    )
    client = FakeOpenAIClient()
    service = ChatService(
        embedding_service=embedding_service,
        article_repository=article_repository,
        client=client,
        default_model="fake-default",
    )

    result = service.answer_query(
        "What changed this morning?",
        retrieval_limit=3,
        source_names=["Reuters"],
        conversation_history=[
            {"role": "user", "content": "Why were bonds weak yesterday?"},
            {"role": "assistant", "content": "Growth data and issuance were the main drivers."},
        ],
    )

    assert result["answer"] == "Grounded answer [1]"
    assert embedding_service.calls == [
        [
            "What changed this morning?\n\nRecent context: User asked Why were bonds weak yesterday. "
            "Assistant answered Growth data and issuance were the main drivers."
        ]
    ]
    assert article_repository.calls == [
        {
            "query_embedding": [0.1, 0.2, 0.3],
            "limit": 3,
            "min_published_at": None,
            "source_name": None,
            "source_names": ["Reuters"],
        }
    ]

    request = client.responses.calls[0]
    assert request["model"] == "fake-default"
    prompt = request["input"][1]["content"]
    assert "Recent conversation context:" in prompt
    assert "User asked Why were bonds weak yesterday." in prompt
    assert "Assistant answered Growth data and issuance were the main drivers." in prompt
    assert "Latest user question: What changed this morning?" in prompt
