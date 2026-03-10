"""Backfill text_processing + region for existing articles."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from collections import Counter
from typing import Any

from dotenv import load_dotenv

# Ensure `app` package imports work when running this file directly.
BACKEND_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.db.session import SessionLocal
from app.repositories.article_repository import ArticleRepository
from app.services.text_processing_service import TextProcessingService

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("backfill_text_processing")
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill text_processing for existing articles.")
    parser.add_argument("--batch-size", type=int, default=50, help="DB fetch batch size.")
    parser.add_argument("--workers", type=int, default=10, help="Parallel LLM calls.")
    parser.add_argument("--limit", type=int, default=0, help="Max articles to process (0 = no cap).")
    parser.add_argument("--model", default=None, help="Optional OpenAI model override.")
    parser.add_argument("--dry-run", action="store_true", help="Compute only; do not update/delete DB rows.")
    parser.add_argument(
        "--print-failures",
        type=int,
        default=10,
        help="Number of failed article errors to print at the end (0 to disable).",
    )
    return parser.parse_args()


def article_to_text(article: dict[str, Any]) -> str | None:
    fields = [
        ("Title", article.get("title")),
        ("Description", article.get("description")),
        ("Content", article.get("content")),
    ]
    parts: list[str] = []
    for label, value in fields:
        text = (value or "").strip()
        if text:
            parts.append(f"{label}: {text}")
    return "\n".join(parts) if parts else None


def process_one(
    service: TextProcessingService,
    article: dict[str, Any],
    model: str | None,
) -> tuple[Any, dict[str, Any] | None, str | None]:
    article_id = article["article_id"]
    text = article_to_text(article)
    if not text:
        return article_id, None, "empty_text"

    try:
        payload = service.process(text, model=model)
        if not isinstance(payload, dict):
            return article_id, None, "invalid_payload_type"
        return article_id, payload, None
    except Exception as exc:
        return article_id, None, str(exc)


def process_batch(
    service: TextProcessingService,
    articles: list[dict[str, Any]],
    workers: int,
    model: str | None,
) -> tuple[list[tuple[Any, dict[str, Any]]], list[Any], list[Any], list[tuple[Any, str]]]:
    successes: list[tuple[Any, dict[str, Any]]] = []
    keep_false_ids: list[Any] = []
    failed_ids: list[Any] = []
    failed_details: list[tuple[Any, str]] = []

    max_workers = min(max(workers, 1), len(articles)) if articles else 1
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_one, service, article, model) for article in articles]
        for future in as_completed(futures):
            article_id, payload, error = future.result()
            if payload is None:
                failed_ids.append(article_id)
                failed_details.append((article_id, error or "unknown_error"))
                continue

            keep_value = payload.get("keep")
            keep_article = keep_value if isinstance(keep_value, bool) else True
            if keep_article:
                successes.append((article_id, payload))
            else:
                keep_false_ids.append(article_id)
    return successes, keep_false_ids, failed_ids, failed_details


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args()
    logger.info(
        "Starting backfill: batch_size=%s workers=%s limit=%s model=%s dry_run=%s",
        args.batch_size,
        args.workers,
        args.limit,
        args.model or "default",
        args.dry_run,
    )

    if not os.getenv("OPENAI_API_KEY"):
        raise ValueError("OPENAI_API_KEY not found in environment variables")

    db_session = SessionLocal()
    repository = ArticleRepository(db_session)
    service = TextProcessingService()

    processed_total = 0
    updated_total = 0
    deleted_total = 0
    failed_total = 0
    retried_total = 0
    region_backfilled = 0
    seen_failed_ids: set[Any] = set()
    failed_reason_counter: Counter[str] = Counter()
    failed_samples: list[tuple[Any, str]] = []

    try:
        if args.dry_run:
            logger.info("dry_run=true (DB writes/deletes disabled)")
        else:
            region_backfilled = repository.backfill_region_from_metadata()
            logger.info("Initial region backfill complete: updated=%s", region_backfilled)

        batch_number = 0
        while True:
            remaining = args.limit - processed_total if args.limit > 0 else args.batch_size
            if args.limit > 0 and remaining <= 0:
                logger.info("Reached limit=%s, stopping.", args.limit)
                break
            fetch_size = min(args.batch_size, remaining) if args.limit > 0 else args.batch_size
            batch_number += 1
            logger.info("Batch %s: fetching candidates (fetch_size=%s)", batch_number, fetch_size)
            candidates = repository.list_missing_text_processing(limit=fetch_size)
            if not candidates:
                logger.info("Batch %s: no more candidates, stopping.", batch_number)
                break

            pending = [row for row in candidates if row["article_id"] not in seen_failed_ids]
            if not pending:
                logger.info("Batch %s: all candidates are previously failed IDs, stopping.", batch_number)
                break
            logger.info("Batch %s: processing pending=%s", batch_number, len(pending))

            successes, keep_false_ids, failed_ids, failed_details = process_batch(
                service=service,
                articles=pending,
                workers=args.workers,
                model=args.model,
            )
            failed_reason_counter.update(reason for _, reason in failed_details)
            retried_total += len(failed_ids)

            id_to_article = {row["article_id"]: row for row in pending}
            retry_articles = [id_to_article[article_id] for article_id in failed_ids if article_id in id_to_article]
            retry_successes: list[tuple[Any, dict[str, Any]]] = []
            retry_keep_false_ids: list[Any] = []
            final_failed_ids: list[Any] = []
            if retry_articles:
                logger.info("Batch %s: retrying failed items count=%s", batch_number, len(retry_articles))
                retry_successes, retry_keep_false_ids, final_failed_ids, retry_failed_details = process_batch(
                    service=service,
                    articles=retry_articles,
                    workers=args.workers,
                    model=args.model,
                )
                failed_reason_counter.update(reason for _, reason in retry_failed_details)
                failed_samples.extend(retry_failed_details)

            merged_successes = successes + retry_successes
            merged_keep_false_ids = keep_false_ids + retry_keep_false_ids
            failed_total += len(final_failed_ids)
            seen_failed_ids.update(final_failed_ids)

            if not args.dry_run:
                updated_total += repository.apply_text_processing_updates(merged_successes)
                deleted_total += repository.delete_by_ids(merged_keep_false_ids)
            else:
                updated_total += len(merged_successes)
                deleted_total += len(merged_keep_false_ids)

            processed_total += len(pending)

            logger.info(
                "Batch %s summary: processed=%s updated=%s deleted_keep_false=%s failed_after_retry=%s",
                batch_number,
                len(pending),
                len(merged_successes),
                len(merged_keep_false_ids),
                len(final_failed_ids),
            )

    finally:
        db_session.close()

    logger.info("Backfill summary:")
    logger.info("region_backfilled_from_metadata=%s", region_backfilled)
    logger.info("processed_total=%s", processed_total)
    logger.info("updated_total=%s", updated_total)
    logger.info("deleted_keep_false_total=%s", deleted_total)
    logger.info("retried_total=%s", retried_total)
    logger.info("failed_after_retry_total=%s", failed_total)
    if failed_reason_counter:
        logger.warning("failed_reasons:")
        for reason, count in failed_reason_counter.most_common():
            logger.warning("  %sx %s", count, reason)
    if args.print_failures > 0 and failed_samples:
        logger.warning("failed_samples:")
        for article_id, reason in failed_samples[: args.print_failures]:
            logger.warning("  article_id=%s reason=%s", article_id, reason)


if __name__ == "__main__":
    main()
