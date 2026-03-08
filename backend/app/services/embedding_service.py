"""Embedding service for generating vector representations of text."""

from __future__ import annotations

import os
from collections.abc import Sequence

from openai import OpenAI

from app.models.embedding import MAX_BATCH_SIZE, MAX_WORDS_PER_TEXT


class EmbeddingService:
    """Minimal service wrapper around OpenAI embeddings API."""

    def __init__(
        self,
        api_key: str | None = None,
        default_model: str = "text-embedding-3-small",
        client: OpenAI | None = None,
    ) -> None:
        resolved_api_key = api_key or os.getenv("OPENAI_API_KEY")
        if client is None and not resolved_api_key:
            raise ValueError("OPENAI_API_KEY not found in environment variables")

        self._client = client or OpenAI(api_key=resolved_api_key)
        self._default_model = default_model

    def embed(
        self,
        texts: Sequence[str],
        model: str | None = None,
    ) -> list[list[float]]:
        """Generate embeddings while preserving input order."""
        normalized_texts = self._validate_texts(texts)
        model_name = model or self._default_model
        api_response = self._client.embeddings.create(model=model_name, input=normalized_texts)
        return [item.embedding for item in api_response.data]

    def embed_texts(
        self,
        texts: Sequence[str],
        model: str | None = None,
    ) -> list[list[float]]:
        """Backward-friendly alias for embed()."""
        return self.embed(texts=texts, model=model)

    @staticmethod
    def _validate_texts(texts: Sequence[str]) -> list[str]:
        if not texts:
            raise ValueError("texts must contain at least one item")
        if len(texts) > MAX_BATCH_SIZE:
            raise ValueError(f"texts batch size exceeds max of {MAX_BATCH_SIZE}")

        normalized: list[str] = []
        for index, text in enumerate(texts):
            if not isinstance(text, str):
                raise TypeError(f"texts[{index}] must be a string")
            stripped = text.strip()
            if not stripped:
                raise ValueError(f"texts[{index}] must be non-empty")
            word_count = len(stripped.split())
            if word_count > MAX_WORDS_PER_TEXT:
                raise ValueError(
                    f"texts[{index}] has {word_count} words; max allowed is {MAX_WORDS_PER_TEXT}"
                )
            normalized.append(stripped)
        return normalized

