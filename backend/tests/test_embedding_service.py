from types import SimpleNamespace

import pytest

from app.models.embedding import MAX_BATCH_SIZE, MAX_WORDS_PER_TEXT
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


def test_embed_uses_default_model_when_model_missing():
    fake_client = FakeClient(make_response([[0.1, 0.2, 0.3]]))
    service = EmbeddingService(client=fake_client, default_model="model-default")
    response = service.embed(["first text"])

    assert fake_client.embeddings.last_call["model"] == "model-default"
    assert response == [[0.1, 0.2, 0.3]]


def test_embed_allows_model_override():
    fake_client = FakeClient(
        make_response(
            vectors=[[1.0, 2.0], [3.0, 4.0]],
            request_id="req_123",
            prompt_tokens=10,
            total_tokens=10,
        )
    )
    service = EmbeddingService(client=fake_client, default_model="model-default")
    response = service.embed(["a", "b"], model="model-override")

    assert fake_client.embeddings.last_call["model"] == "model-override"
    assert response == [[1.0, 2.0], [3.0, 4.0]]


def test_embed_handles_empty_vectors():
    fake_client = FakeClient(make_response(vectors=[], request_id="req_empty"))
    service = EmbeddingService(client=fake_client)
    fake_client.embeddings._response = SimpleNamespace(data=[], usage=None, id="req_empty")

    response = service.embed(["x"])

    assert response == []


def test_embed_raises_on_empty_or_blank_text():
    fake_client = FakeClient(make_response(vectors=[]))
    service = EmbeddingService(client=fake_client)

    with pytest.raises(ValueError, match="at least one item"):
        service.embed([])

    with pytest.raises(ValueError, match="non-empty"):
        service.embed(["   "])


def test_embed_raises_when_batch_size_exceeds_limit():
    fake_client = FakeClient(make_response(vectors=[]))
    service = EmbeddingService(client=fake_client)
    texts = ["ok"] * (MAX_BATCH_SIZE + 1)

    with pytest.raises(ValueError, match=f"max of {MAX_BATCH_SIZE}"):
        service.embed(texts)


def test_embed_raises_when_single_text_exceeds_word_limit():
    fake_client = FakeClient(make_response(vectors=[]))
    service = EmbeddingService(client=fake_client)
    too_long = "w " * (MAX_WORDS_PER_TEXT + 1)

    with pytest.raises(ValueError, match=f"max allowed is {MAX_WORDS_PER_TEXT}"):
        service.embed([too_long])
