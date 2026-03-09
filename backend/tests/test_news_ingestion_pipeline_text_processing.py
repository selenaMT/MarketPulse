from app.pipelines.news_ingestion_pipeline import NewsIngestionPipeline


class FakeFetcher:
    def __init__(self, articles):
        self._articles = articles

    def fetch(self, **fetch_params):
        return list(self._articles)


class FakeEmbeddingService:
    def embed(self, texts):
        return [[float(index), 0.1] for index, _ in enumerate(texts, start=1)]


class FakeTextProcessingService:
    def process(self, article_text):
        return {
            "event": "US CPI rose higher than expected",
            "entities": [{"name": "US CPI", "type": "economic_indicator"}],
            "region": "US",
            "policy_signal": "hawkish",
            "asset_impacts": [{"asset": "US Treasuries", "direction": "down", "confidence": 1}],
            "relationships": [{"source": "US CPI", "relation": "increases", "target": "rate hike expectations"}],
            "keep": True,
        }


class FakeRepository:
    def __init__(self):
        self.received = None
        self.deleted_urls = []

    def upsert_many(self, articles):
        self.received = list(articles)
        return len(articles), 0, len(articles), 0

    def delete_by_canonical_urls(self, canonical_urls):
        self.deleted_urls.extend(canonical_urls)
        return len(canonical_urls)


def test_pipeline_enriches_articles_with_text_processing_and_embedding():
    articles = [
        {
            "url": "https://example.com/a",
            "title": "Inflation surprise",
            "description": "CPI came in hot.",
            "content": "Treasuries sold off after data.",
        }
    ]
    repository = FakeRepository()
    pipeline = NewsIngestionPipeline(
        fetchers=[FakeFetcher(articles)],
        embedding_service=FakeEmbeddingService(),
        text_processing_service=FakeTextProcessingService(),
        article_repository=repository,
    )

    result = pipeline.run(q="inflation")

    assert result["fetched_count"] == 1
    assert result["embedded_count"] == 1
    assert result["text_processed_count"] == 1
    assert result["filtered_out_count"] == 0
    assert result["text_processing_errors_count"] == 0
    assert repository.received is not None
    assert repository.received[0]["embedding"] == [1.0, 0.1]
    assert repository.received[0]["text_processing"]["policy_signal"] == "hawkish"
    assert result["articles"][0]["text_processing"]["keep"] is True


def test_pipeline_skips_db_write_when_text_processing_fails():
    class BrokenTextProcessingService:
        def process(self, article_text):
            raise RuntimeError("LLM failure")

    articles = [
        {
            "url": "https://example.com/b",
            "title": "Fed update",
            "description": "Officials stay cautious.",
            "content": "Markets digest policy path.",
        }
    ]
    repository = FakeRepository()
    pipeline = NewsIngestionPipeline(
        fetchers=[FakeFetcher(articles)],
        embedding_service=FakeEmbeddingService(),
        text_processing_service=BrokenTextProcessingService(),
        article_repository=repository,
    )

    result = pipeline.run(q="fed")

    assert result["embedded_count"] == 0
    assert result["text_processed_count"] == 0
    assert result["text_processing_errors_count"] == 1
    assert result["persisted_count"] == 0
    assert result["articles"] == []
    assert repository.received is None


def test_pipeline_filters_out_keep_false_and_deletes_existing_row():
    class KeepFalseTextProcessingService:
        def process(self, article_text):
            return {
                "event": "Non-relevant article",
                "entities": [],
                "region": "Global",
                "policy_signal": "neutral",
                "asset_impacts": [],
                "relationships": [],
                "keep": False,
            }

    articles = [
        {
            "url": "https://example.com/c?utm_source=x",
            "title": "Sports update",
            "description": "Team won title.",
            "content": "Celebrations continue.",
        }
    ]
    repository = FakeRepository()
    pipeline = NewsIngestionPipeline(
        fetchers=[FakeFetcher(articles)],
        embedding_service=FakeEmbeddingService(),
        text_processing_service=KeepFalseTextProcessingService(),
        article_repository=repository,
    )

    result = pipeline.run(q="sports")

    assert result["filtered_out_count"] == 1
    assert result["deleted_filtered_count"] == 1
    assert result["persisted_count"] == 0
    assert len(result["articles"]) == 1
    assert result["articles"][0]["text_processing"]["keep"] is False
    assert repository.deleted_urls == ["https://example.com/c"]
    assert repository.received is None


def test_pipeline_retries_failed_text_processing_once():
    class FlakyTextProcessingService:
        def __init__(self):
            self.calls = 0

        def process(self, article_text):
            self.calls += 1
            if self.calls == 1:
                raise ValueError("invalid json")
            return {
                "event": "Retry succeeded",
                "entities": [],
                "region": "US",
                "policy_signal": "neutral",
                "asset_impacts": [],
                "relationships": [],
                "keep": True,
            }

    articles = [
        {
            "url": "https://example.com/d",
            "title": "Fed commentary",
            "description": "Policy update",
            "content": "More details",
        }
    ]
    repository = FakeRepository()
    flaky_service = FlakyTextProcessingService()
    pipeline = NewsIngestionPipeline(
        fetchers=[FakeFetcher(articles)],
        embedding_service=FakeEmbeddingService(),
        text_processing_service=flaky_service,
        article_repository=repository,
    )

    result = pipeline.run(q="fed")

    assert result["text_processing_retry_count"] == 1
    assert result["text_processing_discarded_count"] == 0
    assert result["text_processing_errors_count"] == 0
    assert result["persisted_count"] == 1
    assert repository.received is not None
