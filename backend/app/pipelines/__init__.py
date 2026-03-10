"""Pipeline orchestration layer for multi-step workflows."""

from app.pipelines.news_ingestion_pipeline import NewsIngestionPipeline

__all__ = [
    "NewsIngestionPipeline",
]
