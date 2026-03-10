"""Backfill database by domain/day using the full ingestion pipeline."""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
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
from app.services.fetchers.newsapi_source import DEFAULT_REPUTABLE_DOMAINS, NewsApiSource
from app.services.text_processing_service import TextProcessingService

DEFAULT_QUERY = ""#"market OR economics OR finance OR investment OR business OR politics OR regulation"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill ingestion for each reputable domain and each completed day "
            "in the last N days using full processing (fetch + embed + text processing + DB upsert)."
        )
    )
    parser.add_argument("--days", type=int, default=11, help="Number of completed days to process.")
    parser.add_argument(
        "--query",
        default=DEFAULT_QUERY,
        help="NewsAPI query for relevant macro/market coverage.",
    )
    parser.add_argument("--language", default="en", help="NewsAPI language filter.")
    parser.add_argument("--sort-by", default="popularity", help="NewsAPI sort option.")
    parser.add_argument("--page-size", type=int, default=15, help="Articles per source/day request.")
    parser.add_argument("--page", type=int, default=1, help="NewsAPI page number.")
    parser.add_argument(
        "--domains",
        default=None,
        help="Optional comma-separated domains. Defaults to built-in reputable domain list.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.1,
        help="Optional sleep between source/day requests.",
    )
    return parser.parse_args()


def completed_days(days: int) -> list[date]:
    safe_days = max(days, 1)
    # Use UTC so day boundaries are explicit and stable.
    today_utc = datetime.now(timezone.utc).date()
    end_day = today_utc - timedelta(days=1)
    start_day = end_day - timedelta(days=safe_days - 1)
    return [start_day + timedelta(days=offset) for offset in range(safe_days)]


def day_window_utc(target_day: date) -> tuple[str, str]:
    day_value = _iso_newsapi_date(target_day)
    return day_value, day_value


def _iso_newsapi_date(value: date) -> str:
    # NewsAPI accepts YYYY-MM-DD.
    return value.strftime("%Y-%m-%d")


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args()

    news_api_key = os.getenv("NEWS_API_KEY")
    if not news_api_key:
        raise ValueError("NEWS_API_KEY not found in environment variables")

    fetcher = NewsApiSource(api_key=news_api_key)
    embedding_service = EmbeddingService()
    text_processing_service = TextProcessingService()
    db_session = SessionLocal()
    article_repository = ArticleRepository(db_session)
    pipeline = NewsIngestionPipeline(
        fetchers=[fetcher],
        embedding_service=embedding_service,
        text_processing_service=text_processing_service,
        article_repository=article_repository,
    )

    days_to_process = completed_days(args.days)
    if isinstance(args.domains, str) and args.domains.strip():
        domain_list = [part.strip() for part in args.domains.split(",") if part.strip()]
    else:
        domain_list = list(DEFAULT_REPUTABLE_DOMAINS)
    total_requests = len(days_to_process) * len(domain_list)

    aggregate = {
        "requests_attempted": 0,
        "fetched_count": 0,
        "deduped_count": 0,
        "duplicate_count": 0,
        "embedded_count": 0,
        "text_processed_count": 0,
        "filtered_out_count": 0,
        "deleted_filtered_count": 0,
        "persisted_count": 0,
        "inserted_count": 0,
        "updated_count": 0,
        "invalid_url_count": 0,
        "skipped_count": 0,
        "errors_count": 0,
        "fetch_errors_count": 0,
        "embedding_errors_count": 0,
        "text_processing_errors_count": 0,
        "deletion_errors_count": 0,
        "persistence_errors_count": 0,
    }

    try:
        print(
            "Backfill start:",
            f"days={len(days_to_process)}",
            f"domains={len(domain_list)}",
            f"total_requests={total_requests}",
            sep=" ",
        )
        print(f"date_range={days_to_process[0]}..{days_to_process[-1]}")

        for day in days_to_process:
            from_param, to_param = day_window_utc(day)
            print(f"\n=== day={day} (from={from_param} to={to_param}) ===")
            for domain in domain_list:
                aggregate["requests_attempted"] += 1
                result = pipeline.run(
                    q=args.query,
                    domains=domain,
                    from_param=from_param,
                    to=to_param,
                    language=args.language,
                    sort_by=args.sort_by,
                    page_size=args.page_size,
                    page=args.page,
                )

                for key in aggregate:
                    if key == "requests_attempted":
                        continue
                    aggregate[key] += int(result.get(key, 0) or 0)

                print(
                    f"[{domain}] "
                    f"fetched={result.get('fetched_count', 0)} "
                    f"persisted={result.get('persisted_count', 0)} "
                    f"errors={result.get('errors_count', 0)}"
                )
                fetch_errors = result.get("fetch_error_messages", []) or []
                if fetch_errors:
                    print(f"[{domain}] fetch_error={fetch_errors[0]}")
                if result.get("persistence_error"):
                    print(f"[{domain}] persistence_error={result['persistence_error']}")
                if result.get("deletion_error"):
                    print(f"[{domain}] deletion_error={result['deletion_error']}")

                if args.sleep_seconds > 0:
                    time.sleep(args.sleep_seconds)
    finally:
        db_session.close()

    print("\nBackfill summary:")
    for key, value in aggregate.items():
        print(f"{key}={value}")


if __name__ == "__main__":
    main()
