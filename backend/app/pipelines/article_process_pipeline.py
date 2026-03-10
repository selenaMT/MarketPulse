"""Article processing pipeline: enrich stored articles with embeddings and text-processing output."""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import Any
import traceback

# Ensure imports work regardless of where this module is imported from
_current_dir = Path(__file__).resolve().parent
_backend_root = _current_dir.parents[1]
if str(_backend_root) not in sys.path:
    sys.path.insert(0, str(_backend_root))

from app.models.article import Article
from app.repositories.article_repository import ArticleRepository
from app.services.embedding_service import EmbeddingService
from app.services.text_processing_service import TextProcessingService


class ArticleProcessingPipeline:
    """Pipeline to process raw articles into structured macro insights."""

    def __init__(
        self,
        embedding_service: EmbeddingService,
        article_repository: ArticleRepository,
        text_processing_service: TextProcessingService | None = None,
    ) -> None:
        self._embedding_service = embedding_service
        self._article_repository = article_repository
        self._session = article_repository._session
        self._text_processing_service = text_processing_service or TextProcessingService()

    def run(self, limit: int = 100) -> dict[str, Any]:
        """Run the article processing pipeline."""
        # Get articles that haven't been processed yet.
        unprocessed_articles = self._article_repository.get_unprocessed_articles(limit=limit)

        processed_count = 0
        errors: list[str] = []

        for article in unprocessed_articles:
            try:
                self._process_article(article)
                processed_count += 1
            except Exception as e:
                errors.append(f"Error processing article {article.id}: {str(e)}\n{traceback.format_exc()}")

        return {
            "processed_count": processed_count,
            "error_count": len(errors),
            "errors": errors,
        }

    def _process_article(self, article: Article) -> None:
        """Process a single article."""
        # Combine text
        text = self._combine_text(article)
        if not text:
            return

        # Generate embedding if not present
        if article.embedding is None:
            embeddings = self._embedding_service.embed([text])
            article.embedding = embeddings[0]
            article.embedding_model = "text-embedding-3-small"
            article.embedded_at = datetime.utcnow()

        # Run unified structured extraction.
        structured_data = self._text_processing_service.process(text)
        entities = self._extract_entity_names(structured_data)
        narratives = self._extract_narratives(structured_data)
        impact = self._extract_impact(structured_data)

        # Update metadata
        metadata = dict(article.metadata_json or {})
        metadata.update({
            "text_processing": structured_data,
            "entities": entities,
            "narratives": narratives,
            "impact": impact,
            "processed": True,
        })

        # Update the article in the database
        self._session.query(Article).filter(Article.id == article.id).update(
            {"metadata_json": metadata}
        )
        self._session.commit()

    def _combine_text(self, article) -> str:
        """Combine article title, description, and content into a single text."""
        parts = []
        if article.title:
            parts.append(article.title)
        if article.description:
            parts.append(article.description)
        if article.content:
            parts.append(article.content)
        return " ".join(parts)

    @staticmethod
    def _extract_entity_names(structured_data: dict[str, Any]) -> list[str]:
        entities = structured_data.get("entities")
        if not isinstance(entities, list):
            return []

        names: list[str] = []
        for entity in entities:
            if isinstance(entity, dict):
                name = entity.get("name")
                if isinstance(name, str) and name.strip():
                    names.append(name.strip())
            elif isinstance(entity, str) and entity.strip():
                names.append(entity.strip())
        return names

    @staticmethod
    def _extract_narratives(structured_data: dict[str, Any]) -> list[str]:
        narratives = structured_data.get("narratives")
        if not isinstance(narratives, list):
            narratives = structured_data.get("macro_signals")
        if not isinstance(narratives, list):
            return []
        return [narrative.strip() for narrative in narratives if isinstance(narrative, str) and narrative.strip()]

    @staticmethod
    def _extract_impact(structured_data: dict[str, Any]) -> int | None:
        impact = structured_data.get("impact")
        if not isinstance(impact, int):
            return None
        if 0 <= impact <= 100:
            return impact
        return None
