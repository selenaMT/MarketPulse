from __future__ import annotations

import inspect
from typing import Any

from newsapi import NewsApiClient

DEFAULT_REPUTABLE_DOMAINS: tuple[str, ...] = (
    "bloomberg.com",
    #"fortune.com",
    #"wsj.com",
    "bbc.co.uk",
    "apnews.com",
    "cnbc.com",
    "cnn.com",
    "marketwatch.com",
    "washingtonpost.com",
    "straitstimes.com",
)


class NewsApiSource:
    """Minimal NewsAPI /v2/everything source adapter using newsapi-python SDK."""

    def __init__(
        self,
        api_key: str,
        client: NewsApiClient | None = None,
        default_domains: tuple[str, ...] = DEFAULT_REPUTABLE_DOMAINS,
    ) -> None:
        if not api_key:
            raise ValueError("NEWS_API_KEY is required")

        self._client = client or NewsApiClient(api_key=api_key)
        self._default_domains = ",".join(default_domains)

    def fetch(
        self,
        *,
        q: str | None = None,
        search_in: str | None = None,
        q_in_title: str | None = None,
        sources: str | None = None,
        domains: str | None = None,
        exclude_domains: str | None = None,
        from_param: str | None = None,
        to: str | None = None,
        language: str | None = None,
        sort_by: str | None = None,
        page_size: int | None = None,
        page: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Fetch articles from NewsAPI /v2/everything.

        Includes all official query params for this endpoint and forwards only
        the parameters supported by the installed SDK version.
        """
        # Default to reputable domains when no explicit source/domain filter is provided.
        effective_domains = domains
        if (
            sources is None
            and effective_domains is None
            and exclude_domains is None
            and self._default_domains
        ):
            effective_domains = self._default_domains

        sdk_params = {
            "q": q,
            "search_in": search_in,       # some SDK versions support this
            "qintitle": q_in_title,       # some SDK versions use this name
            "sources": sources,
            "domains": effective_domains,
            "exclude_domains": exclude_domains,
            "from_param": from_param,
            "to": to,
            "language": language,
            "sort_by": sort_by,
            "page_size": page_size,
            "page": page,
        }
        accepted_params = set(inspect.signature(self._client.get_everything).parameters.keys())
        filtered_params = {
            key: value
            for key, value in sdk_params.items()
            if value is not None and key in accepted_params
        }

        payload = self._client.get_everything(**filtered_params)
        if payload.get("status") != "ok":
            message = payload.get("message", "Unknown NewsAPI error")
            code = payload.get("code", "unknown_error")
            context = (
                f"page={filtered_params.get('page')}, "
                f"page_size={filtered_params.get('page_size')}, "
                f"sort_by={filtered_params.get('sort_by')}, "
                f"sources={filtered_params.get('sources')}, "
                f"domains={filtered_params.get('domains')}"
            )
            raise RuntimeError(f"NewsAPI request failed (code={code}): {message}. {context}")

        raw_articles = payload.get("articles", [])
        return [self._normalize_article(article) for article in raw_articles]

    @staticmethod
    def _normalize_article(article: dict[str, Any]) -> dict[str, Any]:
        source_data = article.get("source") or {}
        return {
            "source_id": source_data.get("id"),
            "source_name": source_data.get("name"),
            "author": article.get("author"),
            "title": article.get("title"),
            "description": article.get("description"),
            "url": article.get("url"),
            "url_to_image": article.get("urlToImage"),
            "published_at": article.get("publishedAt"),
            "content": article.get("content"),
        }
