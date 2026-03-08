"""Plain Python data models for embedding requests and responses."""

from __future__ import annotations

from dataclasses import dataclass, field

EmbeddingMetadata = dict[str, str | int | float | bool | None]
MAX_BATCH_SIZE = 128
MAX_WORDS_PER_TEXT = 2000


@dataclass(slots=True)
class EmbeddingRequest:
    """Input payload for embedding generation."""

    texts: list[str]
    model: str | None = None
    metadata: list[EmbeddingMetadata] | None = None


@dataclass(slots=True)
class EmbeddingUsage:
    """Token usage information returned by the embedding provider."""

    prompt_tokens: int | None = None
    total_tokens: int | None = None


@dataclass(slots=True)
class EmbeddingVector:
    """Single embedding vector and optional metadata."""

    index: int
    embedding: list[float]
    metadata: EmbeddingMetadata | None = None


@dataclass(slots=True)
class EmbeddingResponse:
    """Structured output returned by the embedding service."""

    model_used: str
    dimension: int
    vectors: list[EmbeddingVector] = field(default_factory=list)
    usage: EmbeddingUsage | None = None
    request_id: str | None = None
