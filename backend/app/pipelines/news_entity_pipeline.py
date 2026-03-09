from app.services.fetchers.newsapi_fetcher import fetch_news, simplify_articles
from app.services.openai_entity_extractor import extract_entities


def run_news_entity_pipeline(
    query: str,
    from_date: str,
    to_date: str,
    page_size: int = 10,
) -> list[dict]:
    raw_articles = fetch_news(
        query=query,
        from_date=from_date,
        to_date=to_date,
        page_size=page_size,
    )

    articles = simplify_articles(raw_articles)

    results = []
    for article in articles:
        text = article.get("text_for_extraction", "").strip()
        if not text:
            continue

        entities = extract_entities(text)

        results.append({
            **article,
            "entities": entities
        })

    return results