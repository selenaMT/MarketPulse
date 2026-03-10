"""Backfill article embeddings using weighted fields: title=1, description=2, content=1."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import and_, or_, select

# Ensure `app` package imports work when running this file directly.
BACKEND_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.db.session import SessionLocal
from app.models.article import Article
from app.models.embedding import MAX_BATCH_SIZE
from app.services.embedding_service import EmbeddingService

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("backfill_embeddings")
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill embeddings with title=1, description=2, content=1.")
    parser.add_argument("--batch-size", type=int, default=100, help="DB fetch batch size.")
    parser.add_argument(
        "--embed-batch-size",
        type=int,
        default=MAX_BATCH_SIZE,
        help=f"Embedding API batch size (max {MAX_BATCH_SIZE}).",
    )
    parser.add_argument("--limit", type=int, default=0, help="Max rows to scan (0 = no cap).")
    parser.add_argument("--model", default=None, help="Optional embedding model override.")
    parser.add_argument("--dry-run", action="store_true", help="Compute only; do not persist embedding updates.")
    parser.add_argument(
        "--only-missing",
        action="store_true",
        help="Only process rows where embedding is currently null.",
    )
    return parser.parse_args()


def article_to_weighted_embedding_text(article: dict[str, Any]) -> str | None:
    title = (article.get("title") or "").strip()
    description = (article.get("description") or "").strip()
    content = (article.get("content") or "").strip()

    parts: list[str] = []
    if title:
        parts.append(f"Title: {title}")
    if description:
        parts.append(f"Description: {description}")
        parts.append(f"Description: {description}")
    if content:
        parts.append(f"Content: {content}")
    return "\n".join(parts) if parts else None


def chunk_items(items: list[tuple[Any, str]], size: int) -> list[list[tuple[Any, str]]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args()
    embed_batch_size = max(1, min(args.embed_batch_size, MAX_BATCH_SIZE))

    logger.info(
        "Starting embedding backfill: batch_size=%s embed_batch_size=%s limit=%s model=%s dry_run=%s only_missing=%s",
        args.batch_size,
        embed_batch_size,
        args.limit,
        args.model or "default",
        args.dry_run,
        args.only_missing,
    )

    if not os.getenv("OPENAI_API_KEY"):
        raise ValueError("OPENAI_API_KEY not found in environment variables")

    db_session = SessionLocal()
    embedding_service = EmbeddingService()

    processed_total = 0
    updated_total = 0
    skipped_empty_text_total = 0
    embedding_error_total = 0
    batch_number = 0

    last_created_at = None
    last_id = None

    try:
        while True:
            remaining = args.limit - processed_total if args.limit > 0 else args.batch_size
            if args.limit > 0 and remaining <= 0:
                logger.info("Reached limit=%s, stopping.", args.limit)
                break
            fetch_size = min(args.batch_size, remaining) if args.limit > 0 else args.batch_size

            query = (
                select(
                    Article.id,
                    Article.title,
                    Article.description,
                    Article.content,
                    Article.created_at,
                )
                .order_by(Article.created_at.asc(), Article.id.asc())
                .limit(fetch_size)
            )
            if args.only_missing:
                query = query.where(Article.embedding.is_(None))
            if last_created_at is not None and last_id is not None:
                query = query.where(
                    or_(
                        Article.created_at > last_created_at,
                        and_(Article.created_at == last_created_at, Article.id > last_id),
                    )
                )

            rows = db_session.execute(query).all()
            if not rows:
                logger.info("No more rows to process, stopping.")
                break

            batch_number += 1
            candidates: list[tuple[Any, str]] = []
            for row in rows:
                text = article_to_weighted_embedding_text(
                    {
                        "title": row.title,
                        "description": row.description,
                        "content": row.content,
                    }
                )
                if text:
                    candidates.append((row.id, text))
                else:
                    skipped_empty_text_total += 1

            id_to_vector: dict[Any, list[float]] = {}
            for chunk in chunk_items(candidates, embed_batch_size):
                chunk_ids = [article_id for article_id, _ in chunk]
                chunk_texts = [text for _, text in chunk]
                try:
                    vectors = embedding_service.embed(chunk_texts, model=args.model)
                    for article_id, vector in zip(chunk_ids, vectors):
                        id_to_vector[article_id] = vector
                except Exception:
                    for article_id, text in chunk:
                        try:
                            vectors = embedding_service.embed([text], model=args.model)
                            if vectors:
                                id_to_vector[article_id] = vectors[0]
                        except Exception:
                            embedding_error_total += 1

            if args.dry_run:
                updated_total += len(id_to_vector)
            else:
                update_time = datetime.now(timezone.utc)
                update_ids = list(id_to_vector.keys())
                if update_ids:
                    articles = db_session.execute(
                        select(Article).where(Article.id.in_(update_ids))
                    ).scalars()
                    for article in articles:
                        vector = id_to_vector.get(article.id)
                        if vector is None:
                            continue
                        article.embedding = vector
                        article.embedding_model = args.model or "text-embedding-3-small"
                        article.embedded_at = update_time
                    db_session.commit()
                updated_total += len(id_to_vector)

            processed_total += len(rows)
            last_created_at = rows[-1].created_at
            last_id = rows[-1].id
            logger.info(
                "Batch %s summary: scanned=%s candidates=%s updated=%s skipped_empty=%s embedding_errors=%s",
                batch_number,
                len(rows),
                len(candidates),
                len(id_to_vector),
                skipped_empty_text_total,
                embedding_error_total,
            )
    finally:
        db_session.close()

    logger.info("Backfill summary:")
    logger.info("processed_total=%s", processed_total)
    logger.info("updated_total=%s", updated_total)
    logger.info("skipped_empty_text_total=%s", skipped_empty_text_total)
    logger.info("embedding_error_total=%s", embedding_error_total)


if __name__ == "__main__":
    main()

