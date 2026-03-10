"""Assign themes to existing articles in chronological order by published_at."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

# Ensure `app` package imports work when running this file directly.
BACKEND_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.db.session import SessionLocal
from app.repositories.article_repository import ArticleRepository
from app.repositories.theme_repository import ThemeRepository
from app.services.embedding_service import EmbeddingService
from app.services.theme_assignment_service import ThemeAssignmentService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run semantic theme assignment on existing articles ordered by published_at asc."
        )
    )
    parser.add_argument("--batch-size", type=int, default=100, help="Articles per batch.")
    parser.add_argument(
        "--max-articles",
        type=int,
        default=None,
        help="Optional cap on total processed articles.",
    )
    parser.add_argument(
        "--start-published-at",
        default=None,
        help="Optional ISO timestamp lower bound (inclusive).",
    )
    parser.add_argument(
        "--end-published-at",
        default=None,
        help="Optional ISO timestamp upper bound (inclusive).",
    )
    return parser.parse_args()


def parse_optional_datetime(raw_value: str | None, arg_name: str) -> datetime | None:
    if raw_value is None:
        return None
    normalized = raw_value.strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        return datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"{arg_name} must be a valid ISO datetime") from exc


def merge_stats(total: dict[str, int], current: dict[str, int]) -> None:
    for key, value in current.items():
        total[key] = total.get(key, 0) + int(value)


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args()
    batch_size = max(args.batch_size, 1)
    max_articles = args.max_articles if args.max_articles is None else max(args.max_articles, 1)
    start_published_at = parse_optional_datetime(args.start_published_at, "--start-published-at")
    end_published_at = parse_optional_datetime(args.end_published_at, "--end-published-at")

    db_session = SessionLocal()
    article_repository = ArticleRepository(db_session)
    theme_repository = ThemeRepository(db_session)
    embedding_service = EmbeddingService()
    theme_assignment_service = ThemeAssignmentService(
        embedding_service=embedding_service,
        theme_repository=theme_repository,
    )

    aggregate: dict[str, int] = {
        "batches": 0,
        "articles_scanned": 0,
        "theme_narratives_processed": 0,
        "theme_matched_real": 0,
        "theme_matched_candidate": 0,
        "theme_candidates_created": 0,
        "theme_candidates_promoted": 0,
        "theme_links_upserted": 0,
        "candidate_theme_links_upserted": 0,
        "theme_snapshots_created": 0,
        "user_theme_links_upserted": 0,
        "user_themes_matched": 0,
    }

    cursor_published_at: datetime | None = None
    cursor_article_id: Any | None = None

    try:
        print("Theme assignment start (chronological by published_at).")
        if start_published_at:
            print(f"start_published_at={start_published_at.isoformat()}")
        if end_published_at:
            print(f"end_published_at={end_published_at.isoformat()}")
        if max_articles is not None:
            print(f"max_articles={max_articles}")

        while True:
            remaining = None if max_articles is None else max_articles - aggregate["articles_scanned"]
            if remaining is not None and remaining <= 0:
                break
            effective_limit = batch_size if remaining is None else min(batch_size, remaining)

            rows = article_repository.list_theme_assignment_candidates(
                limit=effective_limit,
                after_published_at=cursor_published_at,
                after_id=cursor_article_id,
                start_published_at=start_published_at,
                end_published_at=end_published_at,
            )
            if not rows:
                break

            batch_stats = theme_assignment_service.assign_articles(rows)
            aggregate["batches"] += 1
            aggregate["articles_scanned"] += len(rows)
            merge_stats(aggregate, batch_stats)

            last = rows[-1]
            cursor_published_at = last.get("published_at")
            cursor_article_id = last.get("article_id")

            print(
                f"batch={aggregate['batches']} "
                f"articles={len(rows)} "
                f"scanned_total={aggregate['articles_scanned']} "
                f"real={batch_stats.get('theme_matched_real', 0)} "
                f"candidate={batch_stats.get('theme_matched_candidate', 0)} "
                f"new_candidates={batch_stats.get('theme_candidates_created', 0)} "
                f"promoted={batch_stats.get('theme_candidates_promoted', 0)}"
            )
    finally:
        db_session.close()

    print("\nTheme assignment summary:")
    for key, value in aggregate.items():
        print(f"{key}={value}")


if __name__ == "__main__":
    main()
