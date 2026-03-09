"""Run the news ingestion pipeline end-to-end."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Ensure `app` package imports work when running this file directly.
BACKEND_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.db.session import SessionLocal
from app.pipelines.news_ingestion_pipeline import NewsIngestionPipeline
from app.repositories.article_repository import ArticleRepository
from app.services.embedding_service import EmbeddingService
from app.services.fetchers.newsapi_source import NewsApiSource


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest and embed news articles.")
    parser.add_argument("--q", required=True, help="Search query for NewsAPI.")
    parser.add_argument("--from-date", dest="from_param", default=None, help="YYYY-MM-DD or ISO date-time.")
    parser.add_argument("--to", default=None, help="YYYY-MM-DD or ISO date-time.")
    parser.add_argument("--language", default="en", help="NewsAPI language filter.")
    parser.add_argument("--sort-by", default="publishedAt", help="NewsAPI sort option.")
    parser.add_argument("--page-size", type=int, default=50, help="NewsAPI page size.")
    parser.add_argument("--page", type=int, default=1, help="NewsAPI page number.")
    return parser.parse_args()


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args()

    news_api_key = os.getenv("NEWS_API_KEY")
    if not news_api_key:
        raise ValueError("NEWS_API_KEY not found in environment variables")

    fetcher = NewsApiSource(api_key=news_api_key)
    embedding_service = EmbeddingService()
    db_session = SessionLocal()
    article_repository = ArticleRepository(db_session)
    pipeline = NewsIngestionPipeline(
        fetchers=[fetcher],
        embedding_service=embedding_service,
        article_repository=article_repository,
    )

    try:
        result = pipeline.run(
            q=args.q,
            from_param=args.from_param,
            to=args.to,
            language=args.language,
            sort_by=args.sort_by,
            page_size=args.page_size,
            page=args.page,
        )
    finally:
        db_session.close()

    print("Ingestion run summary:")
    print(f"fetched_count={result['fetched_count']}")
    print(f"deduped_count={result['deduped_count']}")
    print(f"duplicate_count={result['duplicate_count']}")
    print(f"embedded_count={result['embedded_count']}")
    print(f"persisted_count={result['persisted_count']}")
    print(f"inserted_count={result['inserted_count']}")
    print(f"updated_count={result['updated_count']}")
    print(f"invalid_url_count={result['invalid_url_count']}")
    print(f"skipped_count={result['skipped_count']}")
    print(f"errors_count={result['errors_count']}")
    print(f"fetch_errors_count={result.get('fetch_errors_count', 0)}")
    print(f"embedding_errors_count={result.get('embedding_errors_count', 0)}")
    print(f"persistence_errors_count={result.get('persistence_errors_count', 0)}")
    for idx, message in enumerate(result.get("fetch_error_messages", []), start=1):
        print(f"fetch_error_{idx}={message}")
    if result.get("persistence_error"):
        print(f"persistence_error={result['persistence_error']}")


if __name__ == "__main__":
    main()
