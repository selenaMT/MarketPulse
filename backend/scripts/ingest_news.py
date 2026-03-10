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
from app.pipelines.theme_management_pipeline import ThemeManagementPipeline
from app.repositories.article_repository import ArticleRepository
from app.repositories.theme_repository import ThemeRepository
from app.services.embedding_service import EmbeddingService
from app.services.fetchers.newsapi_source import NewsApiSource
from app.services.text_processing_service import TextProcessingService
from app.services.theme_management_service import ThemeManagementService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest and embed news articles.")
    parser.add_argument("--q", required=True, help="Search query for NewsAPI.")
    parser.add_argument("--from-date", dest="from_param", default=None, help="YYYY-MM-DD or ISO date-time.")
    parser.add_argument("--to", default=None, help="YYYY-MM-DD or ISO date-time.")
    parser.add_argument("--language", default="en", help="NewsAPI language filter.")
    parser.add_argument("--sort-by", default="publishedAt", help="NewsAPI sort option.")
    parser.add_argument("--page-size", type=int, default=50, help="NewsAPI page size.")
    parser.add_argument("--page", type=int, default=1, help="NewsAPI page number.")
    parser.add_argument(
        "--skip-theme-sync",
        action="store_true",
        help="Skip theme assignment/snapshot refresh after ingestion.",
    )
    parser.add_argument(
        "--theme-assignment-limit",
        type=int,
        default=500,
        help="Maximum unlinked articles for theme assignment when theme sync runs.",
    )
    parser.add_argument(
        "--theme-assignment-lookback-days",
        type=int,
        default=None,
        help="Optional lookback window for theme assignment candidates.",
    )
    parser.add_argument(
        "--theme-snapshot-lookback-days",
        type=int,
        default=90,
        help="Theme snapshot lookback window in days.",
    )
    parser.add_argument(
        "--theme-relation-lookback-days",
        type=int,
        default=45,
        help="Theme relation lookback window in days.",
    )
    parser.add_argument(
        "--theme-run-maintenance",
        action="store_true",
        help="Run merge/split recommendation maintenance as part of sync.",
    )
    parser.add_argument(
        "--theme-rebuild-centroids",
        action="store_true",
        help="Rebuild all theme centroids after assignment.",
    )
    parser.add_argument(
        "--theme-no-record-run",
        action="store_true",
        help="Skip writing theme sync metrics to theme_sync_runs.",
    )
    return parser.parse_args()


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

        theme_result = None
        if not args.skip_theme_sync:
            theme_repository = ThemeRepository(db_session)
            theme_service = ThemeManagementService(theme_repository=theme_repository)
            theme_pipeline = ThemeManagementPipeline(
                theme_repository=theme_repository,
                theme_management_service=theme_service,
            )
            theme_result = theme_pipeline.run(
                assignment_limit=args.theme_assignment_limit,
                assignment_lookback_days=args.theme_assignment_lookback_days,
                snapshot_lookback_days=args.theme_snapshot_lookback_days,
                relation_lookback_days=args.theme_relation_lookback_days,
                run_maintenance=args.theme_run_maintenance,
                rebuild_centroids=args.theme_rebuild_centroids,
                record_run=not args.theme_no_record_run,
            )
    finally:
        db_session.close()

    print("Ingestion run summary:")
    print(f"fetched_count={result['fetched_count']}")
    print(f"deduped_count={result['deduped_count']}")
    print(f"duplicate_count={result['duplicate_count']}")
    print(f"embedded_count={result['embedded_count']}")
    print(f"text_processed_count={result.get('text_processed_count', 0)}")
    print(f"filtered_out_count={result.get('filtered_out_count', 0)}")
    print(f"deleted_filtered_count={result.get('deleted_filtered_count', 0)}")
    print(f"text_processing_retry_count={result.get('text_processing_retry_count', 0)}")
    print(f"text_processing_discarded_count={result.get('text_processing_discarded_count', 0)}")
    print(f"persisted_count={result['persisted_count']}")
    print(f"inserted_count={result['inserted_count']}")
    print(f"updated_count={result['updated_count']}")
    print(f"invalid_url_count={result['invalid_url_count']}")
    print(f"skipped_count={result['skipped_count']}")
    print(f"errors_count={result['errors_count']}")
    print(f"fetch_errors_count={result.get('fetch_errors_count', 0)}")
    print(f"embedding_errors_count={result.get('embedding_errors_count', 0)}")
    print(f"text_processing_errors_count={result.get('text_processing_errors_count', 0)}")
    print(f"deletion_errors_count={result.get('deletion_errors_count', 0)}")
    print(f"persistence_errors_count={result.get('persistence_errors_count', 0)}")
    for idx, message in enumerate(result.get("fetch_error_messages", []), start=1):
        print(f"fetch_error_{idx}={message}")
    if result.get("persistence_error"):
        print(f"persistence_error={result['persistence_error']}")
    if result.get("deletion_error"):
        print(f"deletion_error={result['deletion_error']}")
    if not args.skip_theme_sync and theme_result is not None:
        print("theme_sync=enabled")
        print(f"theme_assignment_input_count={theme_result['assignment_input_count']}")
        print(f"theme_assigned_articles={theme_result['assigned_articles']}")
        print(f"theme_links_upserted={theme_result['theme_links_upserted']}")
        print(f"theme_created_themes={theme_result['created_themes']}")
        print(f"theme_promoted_candidates={theme_result['promoted_candidates']}")
        print(f"theme_abstained_articles={theme_result.get('abstained_articles', 0)}")
        print(f"theme_abstained_signals={theme_result.get('abstained_signals', 0)}")
        print(f"theme_candidate_observations={theme_result.get('candidate_observations', 0)}")
        print(f"theme_assignment_rate={theme_result.get('assignment_rate', 0.0)}")
        print(f"theme_abstain_rate={theme_result.get('abstain_rate', 0.0)}")
        print(f"theme_snapshots_upserted={theme_result['snapshots_upserted']}")
        print(f"theme_relations_upserted={theme_result['relations_upserted']}")
        print(f"theme_status_updates={theme_result['status_updates']}")
        print(f"theme_centroids_rebuilt={theme_result.get('centroids_rebuilt', 0)}")
        print(f"theme_recommendation_count={theme_result.get('recommendation_count', 0)}")
        print(f"theme_merge_recommendations={theme_result.get('merge_recommendations', 0)}")
        print(f"theme_split_recommendations={theme_result.get('split_recommendations', 0)}")
    else:
        print("theme_sync=skipped")


if __name__ == "__main__":
    main()
