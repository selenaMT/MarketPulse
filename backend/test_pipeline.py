from app.pipelines.news_entity_pipeline import run_news_entity_pipeline
from app.pipelines.relation_scoring_pipeline import run_relation_scoring_pipeline
import json

results = run_news_entity_pipeline(
    query="Federal Reserve OR inflation OR oil OR China OR sanctions",
    from_date="2026-03-01",
    to_date="2026-03-09",
    page_size=3,
)

scored = run_relation_scoring_pipeline(results)

print(json.dumps(scored, indent=2, ensure_ascii=False))