# app/services/entity_embedding_service.py

from __future__ import annotations

from typing import Any

from app.services.embedding_service import EmbeddingService


class EntityEmbeddingService:
    def __init__(self, embedding_service: EmbeddingService) -> None:
        self._embedding_service = embedding_service

    def embed_entities_from_article(
        self,
        article: dict[str, Any],
        model: str | None = None,
        min_confidence: float = 0.0,
        deduplicate: bool = True,
    ) -> list[dict[str, Any]]:
        rows = self._build_rows(
            article=article,
            min_confidence=min_confidence,
            deduplicate=deduplicate,
        )

        if not rows:
            return []

        texts = [row["text_for_embedding"] for row in rows]
        vectors = self._embedding_service.embed(texts=texts, model=model)

        for row, vector in zip(rows, vectors, strict=True):
            row["embedding"] = vector

        return rows

    def _build_rows(
        self,
        article: dict[str, Any],
        min_confidence: float,
        deduplicate: bool,
    ) -> list[dict[str, Any]]:
        entities = article.get("entities", {})
        title = (article.get("title") or "").strip()
        article_url = (article.get("url") or "").strip()
        published_at = article.get("published_at")

        rows: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()

        for item in entities.get("named_entities", []):
            confidence = float(item.get("confidence", 0.0))
            if confidence < min_confidence:
                continue

            canonical_name = (item.get("canonical_name") or item.get("mention") or "").strip()
            mention = (item.get("mention") or "").strip()
            entity_type = (item.get("entity_type") or "UNKNOWN").strip()
            evidence = (item.get("evidence") or "").strip()

            if not canonical_name:
                continue

            key = ("named_entity", canonical_name.lower(), entity_type.upper())
            if deduplicate and key in seen:
                continue
            seen.add(key)

            rows.append(
                {
                    "kind": "named_entity",
                    "canonical_name": canonical_name,
                    "mention": mention,
                    "entity_type": entity_type,
                    "confidence": confidence,
                    "evidence": evidence,
                    "article_title": title,
                    "article_url": article_url,
                    "published_at": published_at,
                    "text_for_embedding": self._build_named_entity_text(
                        canonical_name=canonical_name,
                        mention=mention,
                        entity_type=entity_type,
                        article_title=title,
                        evidence=evidence,
                    ),
                }
            )

        for item in entities.get("financial_concepts", []):
            confidence = float(item.get("confidence", 0.0))
            if confidence < min_confidence:
                continue

            concept = (item.get("concept") or "").strip()
            canonical_label = (item.get("canonical_label") or concept).strip()
            category = (item.get("category") or "UNKNOWN").strip()
            direction = (item.get("direction") or "NEUTRAL").strip()
            evidence = (item.get("evidence") or "").strip()

            if not canonical_label:
                continue

            key = ("financial_concept", canonical_label.lower(), category.upper())
            if deduplicate and key in seen:
                continue
            seen.add(key)

            rows.append(
                {
                    "kind": "financial_concept",
                    "canonical_label": canonical_label,
                    "concept": concept,
                    "category": category,
                    "direction": direction,
                    "confidence": confidence,
                    "evidence": evidence,
                    "article_title": title,
                    "article_url": article_url,
                    "published_at": published_at,
                    "text_for_embedding": self._build_financial_concept_text(
                        canonical_label=canonical_label,
                        concept=concept,
                        category=category,
                        direction=direction,
                        article_title=title,
                        evidence=evidence,
                    ),
                }
            )

        return rows

    @staticmethod
    def _build_named_entity_text(
        *,
        canonical_name: str,
        mention: str,
        entity_type: str,
        article_title: str,
        evidence: str,
    ) -> str:
        parts = [
            f"kind: named_entity",
            f"entity_type: {entity_type}",
            f"canonical_name: {canonical_name}",
        ]
        if mention:
            parts.append(f"mention: {mention}")
        if article_title:
            parts.append(f"article_title: {article_title}")
        if evidence:
            parts.append(f"evidence: {evidence}")
        return " | ".join(parts)

    @staticmethod
    def _build_financial_concept_text(
        *,
        canonical_label: str,
        concept: str,
        category: str,
        direction: str,
        article_title: str,
        evidence: str,
    ) -> str:
        parts = [
            f"kind: financial_concept",
            f"category: {category}",
            f"canonical_label: {canonical_label}",
            f"direction: {direction}",
        ]
        if concept:
            parts.append(f"concept: {concept}")
        if article_title:
            parts.append(f"article_title: {article_title}")
        if evidence:
            parts.append(f"evidence: {evidence}")
        return " | ".join(parts)