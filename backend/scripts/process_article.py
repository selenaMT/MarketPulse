"""Run the article processing pipeline end-to-end."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process articles to extract embeddings, entities, and macro signals.")
    parser.add_argument("--limit", type=int, default=100, help="Maximum number of articles to process.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    
    # Ensure `app` package imports work when running this file directly.
    BACKEND_ROOT = Path(__file__).resolve().parents[1]
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    if str(BACKEND_ROOT) not in sys.path:
        sys.path.insert(0, str(BACKEND_ROOT))
    
    # Load environment variables
    env_file = PROJECT_ROOT / ".env"
    logger.info(f"Loading environment from {env_file}")
    if not env_file.exists():
        logger.warning(f".env file not found at {env_file}")
    load_dotenv(env_file)
    
    # Check required environment variables
    required_vars = ["OPENAI_API_KEY", "DATABASE_URL"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        return
    
    # Import app modules after environment is set up
    try:
        from app.db.session import SessionLocal
        from app.pipelines.article_processing_pipeline import ArticleProcessingPipeline
        from app.repositories.article_repository import ArticleRepository
        from app.services.embedding_service import EmbeddingService
    except ImportError as e:
        logger.error(f"Failed to import app modules: {e}")
        return
    
    db_session = None
    try:
        logger.info("Initializing services...")
        embedding_service = EmbeddingService()
        
        logger.info("Connecting to database...")
        db_session = SessionLocal()
        article_repository = ArticleRepository(db_session)
        
        logger.info("Creating pipeline...")
        pipeline = ArticleProcessingPipeline(
            embedding_service=embedding_service,
            article_repository=article_repository,
        )
        
        logger.info(f"Running pipeline with limit={args.limit}...")
        result = pipeline.run(limit=args.limit)
        
        logger.info(f"Pipeline completed successfully.")
        print(f"\nProcessed {result['processed_count']} articles.")
        if result['error_count'] > 0:
            print(f"Errors encountered: {result['error_count']}")
            for error in result['errors'][:5]:  # Show first 5 errors
                print(f"  - {error}")
            if len(result['errors']) > 5:
                print(f"  ... and {len(result['errors']) - 5} more errors")
    except Exception as e:
        logger.error(f"Pipeline error: {e}", exc_info=True)
        return
    finally:
        if db_session:
            logger.info("Closing database session...")
            db_session.close()


if __name__ == "__main__":
    main()
