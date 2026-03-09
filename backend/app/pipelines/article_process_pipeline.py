"""Article processing pipeline: enrich articles with embeddings, entities, and macro signals."""
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

from sqlalchemy import cast
from sqlalchemy.dialects.postgresql import JSONB

from openai import OpenAI

from app.models.article import Article
from app.repositories.article_repository import ArticleRepository
from app.services.embedding_service import EmbeddingService


class ArticleProcessingPipeline:
    """Pipeline to process raw articles into structured macro insights."""

    def __init__(
        self,
        embedding_service: EmbeddingService,
        article_repository: ArticleRepository,
        openai_client: OpenAI | None = None,
    ) -> None:
        self._embedding_service = embedding_service
        self._article_repository = article_repository
        self._session = article_repository._session
        self._openai_client = openai_client or OpenAI()

    def run(self, limit: int = 100) -> dict[str, Any]:
        """Run the article processing pipeline."""
        # Get articles that haven't been processed yet
        unprocessed_articles = self._session.query(Article).filter(
            ~Article.metadata_json.op('@>')(cast({"processed": True}, JSONB))
        ).limit(limit).all()

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

    def _process_article(self, article):
        """Process a single article."""
        # Combine text
        text = self._combine_text(article)

        # Generate embedding if not present
        if article.embedding is None:
            embeddings = self._embedding_service.embed([text])
            article.embedding = embeddings[0]
            article.embedding_model = "text-embedding-3-small"
            article.embedded_at = datetime.utcnow()

        # Extract entities using LLM
        entities = self._extract_entities(text)

        # Extract macro signals using LLM
        signals = self._extract_macro_signals(text)

        # Update metadata
        metadata = article.metadata_json.copy()
        metadata.update({
            "entities": entities,
            "macro_signals": signals,
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

    def _extract_macro_signals(self, text: str) -> list[str]:
        """Extract macroeconomic signals from text using LLM."""
        prompt = f"""
        Analyze the following news article text and extract key macroeconomic signals or themes.
        Provide a list of 3-5 key signals/themes in bullet points.

        Text: {text[:2000]}  # Limit text length

        Signals:
        """
        try:
            response = self._openai_client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=200,
            )
            signals_text = response.choices[0].message.content.strip()
            # Parse bullet points
            signals = [line.strip("- ").strip() for line in signals_text.split("\n") if line.strip().startswith("-")]
            return signals[:5]  # Limit to 5
        except Exception as e:
            return [f"Error extracting signals: {str(e)}"]

    def _extract_entities(self, text: str) -> list[str]:
        """Extract key entities from text using LLM."""
        prompt = f"""
        Analyze the following news article text and extract key entities (people, organizations, locations, etc.).
        Provide a list of 3-7 key entities in bullet points.

        Text: {text[:2000]}  # Limit text length

        Entities:
        """
        try:
            response = self._openai_client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=200,
            )
            entities_text = response.choices[0].message.content.strip()
            # Parse bullet points
            entities = [line.strip("- ").strip() for line in entities_text.split("\n") if line.strip().startswith("-")]
            return entities[:7]  # Limit to 7
        except Exception as e:
            return [f"Error extracting entities: {str(e)}"]
