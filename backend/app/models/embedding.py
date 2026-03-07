"""Schemas for embedding service input and output contracts."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field, StringConstraints, field_validator, model_validator

EmbeddingMetadata = dict[str, str | int | float | bool | None]
NonEmptyText = Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)]
MAX_WORDS_PER_TEXT = 5000


class EmbeddingRequest(BaseModel):
    """Validated input payload for embedding generation."""

    texts: list[NonEmptyText] = Field(min_length=1, max_length=128)
    model: str | None = None
    metadata: list[EmbeddingMetadata] | None = None

    @field_validator("texts")
    @classmethod
    def validate_word_count_per_text(cls, texts: list[str]) -> list[str]:
        for index, text in enumerate(texts):
            word_count = len(text.split())
            if word_count > MAX_WORDS_PER_TEXT:
                raise ValueError(
                    f"texts[{index}] has {word_count} words; max allowed is {MAX_WORDS_PER_TEXT}"
                )
        return texts

    @model_validator(mode="after")
    def validate_metadata_alignment(self) -> "EmbeddingRequest":
        if self.metadata is not None and len(self.metadata) != len(self.texts):
            raise ValueError("metadata length must match texts length")
        return self


class EmbeddingUsage(BaseModel):
    """Usage information returned by the embedding provider."""

    prompt_tokens: int | None = None
    total_tokens: int | None = None


class EmbeddingVector(BaseModel):
    """Single embedding vector and optional metadata."""

    index: int = Field(ge=0)
    embedding: list[float] = Field(min_length=1)
    metadata: EmbeddingMetadata | None = None


class EmbeddingResponse(BaseModel):
    """Structured output returned by the embedding service."""

    model_used: str
    dimension: int = Field(ge=0)
    vectors: list[EmbeddingVector]
    usage: EmbeddingUsage | None = None
    request_id: str | None = None
