from pathlib import Path
from dotenv import load_dotenv
import os
import requests

# load .env from backend root
env_path = Path(__file__).resolve().parents[3] / ".env"
load_dotenv(env_path)

NEWS_API_KEY = os.getenv("NEWS_API_KEY")
NEWS_API_URL = "https://newsapi.org/v2/everything"

NEWS_API_KEY = os.getenv("NEWS_API_KEY")
NEWS_API_URL = "https://newsapi.org/v2/everything"


def fetch_news(
    query: str,
    from_date: str,
    to_date: str,
    language: str = "en",
    sort_by: str = "publishedAt",
    page_size: int = 20,
    page: int = 1,
):
    if not NEWS_API_KEY:
        raise ValueError("NEWS_API_KEY not found in .env")

    params = {
        "q": query,
        "from": from_date,
        "to": to_date,
        "language": language,
        "sortBy": sort_by,
        "pageSize": page_size,
        "page": page,
        "apiKey": NEWS_API_KEY,
    }

    response = requests.get(NEWS_API_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    if data.get("status") != "ok":
        raise ValueError(f"NewsAPI error: {data}")

    return data.get("articles", [])


def simplify_articles(articles: list[dict]) -> list[dict]:
    cleaned = []
    for idx, article in enumerate(articles, start=1):
        cleaned.append(
            {
                "article_id": f"article_{idx}",
                "source": article.get("source", {}).get("name"),
                "author": article.get("author"),
                "title": article.get("title"),
                "description": article.get("description"),
                "content": article.get("content"),
                "published_at": article.get("publishedAt"),
                "url": article.get("url"),
                "text_for_extraction": "\n".join(
                    x
                    for x in [
                        article.get("title"),
                        article.get("description"),
                        article.get("content"),
                    ]
                    if x
                ),
            }
        )
    return cleaned