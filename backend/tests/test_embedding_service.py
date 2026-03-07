from types import SimpleNamespace

import pytest

from app.models.embedding import EmbeddingRequest
from app.services.embedding_service import EmbeddingService


def make_response(vectors, request_id="req_test", prompt_tokens=None, total_tokens=None):
    data = [SimpleNamespace(embedding=vector) for vector in vectors]
    usage = None
    if prompt_tokens is not None or total_tokens is not None:
        usage = SimpleNamespace(prompt_tokens=prompt_tokens, total_tokens=total_tokens)
    return SimpleNamespace(data=data, usage=usage, id=request_id)


class FakeEmbeddingsAPI:
    def __init__(self, response):
        self._response = response
        self.last_call = None

    def create(self, *, model, input):
        self.last_call = {"model": model, "input": input}
        return self._response


class FakeClient:
    def __init__(self, response):
        self.embeddings = FakeEmbeddingsAPI(response)


def test_init_raises_without_api_key_and_without_client(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with pytest.raises(ValueError, match="OPENAI_API_KEY"):
        EmbeddingService(api_key=None, client=None)


def test_init_allows_injected_client_without_api_key(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    fake_client = FakeClient(make_response([[0.1, 0.2]]))
    service = EmbeddingService(api_key=None, client=fake_client)
    assert service is not None


def test_embed_texts_uses_default_model_when_request_model_missing():
    fake_client = FakeClient(make_response([[0.1, 0.2, 0.3]]))
    service = EmbeddingService(client=fake_client, default_model="model-default")
    request = EmbeddingRequest(texts=["first text"])

    response = service.embed_texts(request)

    assert fake_client.embeddings.last_call["model"] == "model-default"
    assert response.model_used == "model-default"
    assert response.dimension == 3


def test_embed_texts_allows_model_override_and_preserves_metadata_order():
    fake_client = FakeClient(
        make_response(
            vectors=[[1.0, 2.0], [3.0, 4.0]],
            request_id="req_123",
            prompt_tokens=10,
            total_tokens=10,
        )
    )
    service = EmbeddingService(client=fake_client, default_model="model-default")
    request = EmbeddingRequest(
        texts=["a", "b"],
        model="model-override",
        metadata=[{"doc_id": "A"}, {"doc_id": "B"}],
    )

    response = service.embed_texts(request)

    assert fake_client.embeddings.last_call["model"] == "model-override"
    assert response.model_used == "model-override"
    assert response.vectors[0].metadata == {"doc_id": "A"}
    assert response.vectors[1].metadata == {"doc_id": "B"}
    assert response.request_id == "req_123"
    assert response.usage is not None
    assert response.usage.prompt_tokens == 10


def test_embed_texts_handles_missing_usage_and_empty_vectors():
    fake_client = FakeClient(make_response(vectors=[], request_id="req_empty"))
    service = EmbeddingService(client=fake_client)
    request = EmbeddingRequest(texts=["x"])
    fake_client.embeddings._response = SimpleNamespace(data=[], usage=None, id="req_empty")

    response = service.embed_texts(request)

    assert response.dimension == 0
    assert response.usage is None
    assert response.request_id == "req_empty"
