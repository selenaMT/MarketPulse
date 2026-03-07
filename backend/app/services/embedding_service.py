"""Embedding service for generating vector representations of text."""

from __future__ import annotations

import os
from typing import Any

from openai import OpenAI

from app.models.embedding import (
    EmbeddingRequest,
    EmbeddingResponse,
    EmbeddingUsage,
    EmbeddingVector,
)


class EmbeddingService:
    """Thin service wrapper around OpenAI embeddings API."""

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

    def embed_texts(self, request: EmbeddingRequest) -> EmbeddingResponse:
        """Generate embeddings while preserving input order."""
        model_name = request.model or self._default_model
        api_response = self._client.embeddings.create(model=model_name, input=request.texts)

        vectors: list[EmbeddingVector] = []
        for index, item in enumerate(api_response.data):
            item_metadata = request.metadata[index] if request.metadata else None
            vectors.append(
                EmbeddingVector(
                    index=index,
                    embedding=item.embedding,
                    metadata=item_metadata,
                )
            )

        usage = self._build_usage(api_response)
        dimension = len(vectors[0].embedding) if vectors else 0
        request_id = getattr(api_response, "id", None)

        return EmbeddingResponse(
            model_used=model_name,
            dimension=dimension,
            vectors=vectors,
            usage=usage,
            request_id=request_id,
        )

    @staticmethod
    def _build_usage(api_response: Any) -> EmbeddingUsage | None:
        usage_data = getattr(api_response, "usage", None)
        if usage_data is None:
            return None

        return EmbeddingUsage(
            prompt_tokens=getattr(usage_data, "prompt_tokens", None),
            total_tokens=getattr(usage_data, "total_tokens", None),
        )

