"""Search persisted articles by semantic similarity from keyword query."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

# Ensure `app` package imports work when running this file directly.
BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.db.session import SessionLocal
from app.repositories.article_repository import ArticleRepository
from app.services.article_search_service import ArticleSearchService
from app.services.embedding_service import EmbeddingService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Semantic search over stored articles.")
    parser.add_argument("--keywords", required=True, help="Keyword query text.")
    parser.add_argument("--limit", type=int, default=10, help="Number of results to return.")
    parser.add_argument(
        "--source-name", default=None, help="Optional source filter (e.g. Reuters)."
    )
    parser.add_argument(
        "--min-published-at",
        default=None,
        help="Optional lower bound for publish time in ISO format.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    min_published_at = (
        datetime.fromisoformat(args.min_published_at) if args.min_published_at else None
    )
    session = SessionLocal()
    try:
        service = ArticleSearchService(
            embedding_service=EmbeddingService(),
            article_repository=ArticleRepository(session),
        )
        rows = service.search_by_keywords(
            keywords=args.keywords,
            limit=args.limit,
            source_name=args.source_name,
            min_published_at=min_published_at,
        )
    finally:
        session.close()

    if not rows:
        print("No matching articles found.")
        return

    for index, row in enumerate(rows, start=1):
        print(f"{index}. {row['title'] or '(untitled)'}")


if __name__ == "__main__":
    main()
