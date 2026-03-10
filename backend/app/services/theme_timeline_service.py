"""Theme timeline service: snapshot retrieval + timeline synthesis."""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any

from openai import OpenAI

from app.repositories.theme_repository import ThemeRepository

TIMELINE_EVENT_SCHEMA: dict[str, Any] = {
    "type": "json_schema",
    "name": "theme_timeline_events",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "events": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "snapshot_version": {"type": "integer"},
                        "event_description": {"type": "string"},
                    },
                    "required": ["snapshot_version", "event_description"],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["events"],
        "additionalProperties": False,
    },
}

TIMELINE_OVERVIEW_SCHEMA: dict[str, Any] = {
    "type": "json_schema",
    "name": "theme_timeline_overview",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {"overview": {"type": "string"}},
        "required": ["overview"],
        "additionalProperties": False,
    },
}

TIMELINE_SYSTEM_PROMPT = (
    "You generate concise timeline node events for a macro theme.\n"
    "For each snapshot in order, infer what changed vs prior context.\n"
    "Summaries may include markers: 'Previous summary:' and 'New linked articles:'.\n"
    "Use prior event context + new coverage to produce the current event description.\n"
    "Keep each event description factual, specific, and <= 45 words.\n"
    "Return JSON only in the required schema."
)

OVERVIEW_SYSTEM_PROMPT = (
    "You summarize a full theme evolution timeline.\n"
    "Use the event descriptions in chronological order.\n"
    "Write one compact paragraph with 2-4 sentences, <= 110 words, factual tone.\n"
    "Return JSON only in the required schema."
)


class ThemeTimelineService:
    """Build timeline nodes with event descriptions and article references."""

    def __init__(
        self,
        *,
        theme_repository: ThemeRepository,
        client: OpenAI | None = None,
        default_model: str = "gpt-4o-mini",
    ) -> None:
        self._theme_repository = theme_repository
        self._default_model = default_model
        if client is not None:
            self._client = client
        else:
            api_key = os.getenv("OPENAI_API_KEY")
            self._client = OpenAI(api_key=api_key) if api_key else None

    def build_timeline(self, theme_id: Any, limit: int = 50) -> dict[str, Any]:
        theme = self._theme_repository.get_theme_by_id(theme_id)
        if not theme:
            raise ValueError("Theme not found.")
        if str(theme.get("scope") or "global") != "global":
            raise ValueError("Theme not available for public timeline.")

        snapshots = self._theme_repository.list_theme_snapshots(theme_id=theme_id, limit=limit)
        if not snapshots:
            snapshots = [self._build_fallback_snapshot(theme)]

        linked_articles = self._theme_repository.list_theme_linked_articles(
            theme_id=theme_id,
            limit=max(500, int(theme.get("article_count") or 0) * 3),
        )

        event_map = self._generate_event_map(snapshots, theme)
        nodes = self._build_nodes(snapshots, linked_articles, event_map)
        theme_overview = self._generate_theme_overview(theme=theme, nodes=nodes)
        return {"theme": theme, "theme_overview": theme_overview, "nodes": nodes}

    def _build_nodes(
        self,
        snapshots: list[dict[str, Any]],
        linked_articles: list[dict[str, Any]],
        event_map: dict[int, str],
    ) -> list[dict[str, Any]]:
        nodes: list[dict[str, Any]] = []
        previous_snapshot_ts: datetime | None = None
        previous_event_ts: datetime | None = None

        for snapshot in snapshots:
            snapshot_version = int(snapshot.get("snapshot_version") or 0)
            snapshot_created_at = snapshot.get("snapshot_created_at")
            snapshot_ts = snapshot_created_at if isinstance(snapshot_created_at, datetime) else None

            window_articles = self._slice_articles_for_window(
                linked_articles=linked_articles,
                window_start=previous_snapshot_ts,
                window_end=snapshot_ts,
            )
            article_refs = self._build_article_refs(window_articles, max_items=5)
            event_ts, period_label = self._derive_article_time_labels(
                window_articles=window_articles,
                previous_event_ts=previous_event_ts,
                snapshot_ts=snapshot_ts,
            )

            nodes.append(
                {
                    "snapshot_version": snapshot_version,
                    "event_at": event_ts,
                    "period_label": period_label,
                    "article_count": int(snapshot.get("article_count") or 0),
                    "event_description": event_map.get(
                        snapshot_version,
                        self._heuristic_event_description(
                            summary=self._clean_text(snapshot.get("summary")),
                        ),
                    ),
                    "related_articles": article_refs,
                }
            )

            if snapshot_ts is not None:
                previous_snapshot_ts = snapshot_ts
            if event_ts is not None:
                previous_event_ts = event_ts

        return nodes

    def _generate_event_map(self, snapshots: list[dict[str, Any]], theme: dict[str, Any]) -> dict[int, str]:
        fallback = self._generate_fallback_event_map(snapshots)
        if self._client is None:
            return fallback

        payload_snapshots: list[dict[str, Any]] = []
        for snapshot in snapshots:
            payload_snapshots.append(
                {
                    "snapshot_version": int(snapshot.get("snapshot_version") or 0),
                    "snapshot_created_at": (
                        snapshot["snapshot_created_at"].isoformat()
                        if isinstance(snapshot.get("snapshot_created_at"), datetime)
                        else None
                    ),
                    "summary": self._clean_text(snapshot.get("summary")),
                    "article_count": int(snapshot.get("article_count") or 0),
                }
            )

        user_prompt = json.dumps(
            {
                "theme": {
                    "id": str(theme.get("id")),
                    "canonical_label": theme.get("canonical_label"),
                    "status": theme.get("status"),
                },
                "snapshots": payload_snapshots,
                "instructions": (
                    "Generate one event_description per snapshot_version. "
                    "Use sequential context so each event reflects progression."
                ),
            },
            ensure_ascii=True,
        )

        try:
            response = self._client.responses.create(
                model=self._default_model,
                input=[
                    {"role": "system", "content": TIMELINE_SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                text={"format": TIMELINE_EVENT_SCHEMA},
            )
            parsed = json.loads(response.output_text)
            events = parsed.get("events") if isinstance(parsed, dict) else None
            if not isinstance(events, list):
                return fallback

            mapped: dict[int, str] = dict(fallback)
            for event in events:
                if not isinstance(event, dict):
                    continue
                version = event.get("snapshot_version")
                description = self._clean_text(event.get("event_description"))
                if isinstance(version, int) and description:
                    mapped[version] = self._truncate(description, 260)
            return mapped
        except Exception:
            return fallback

    def _generate_fallback_event_map(self, snapshots: list[dict[str, Any]]) -> dict[int, str]:
        mapped: dict[int, str] = {}
        for snapshot in snapshots:
            version = int(snapshot.get("snapshot_version") or 0)
            summary = self._clean_text(snapshot.get("summary"))
            mapped[version] = self._heuristic_event_description(summary=summary)
        return mapped

    def _generate_theme_overview(self, *, theme: dict[str, Any], nodes: list[dict[str, Any]]) -> str:
        fallback = self._heuristic_theme_overview(theme=theme, nodes=nodes)
        if self._client is None:
            return fallback

        events_payload = [
            {
                "snapshot_version": int(node.get("snapshot_version") or 0),
                "period_label": str(node.get("period_label") or ""),
                "event_description": str(node.get("event_description") or ""),
            }
            for node in nodes
        ]
        user_prompt = json.dumps(
            {
                "theme": {
                    "canonical_label": theme.get("canonical_label"),
                    "status": theme.get("status"),
                    "article_count": int(theme.get("article_count") or 0),
                },
                "events": events_payload,
                "instructions": (
                    "Summarize the whole theme trajectory based on timeline event content only."
                ),
            },
            ensure_ascii=True,
        )
        try:
            response = self._client.responses.create(
                model=self._default_model,
                input=[
                    {"role": "system", "content": OVERVIEW_SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                text={"format": TIMELINE_OVERVIEW_SCHEMA},
            )
            parsed = json.loads(response.output_text)
            overview = self._clean_text(parsed.get("overview") if isinstance(parsed, dict) else None)
            return self._truncate(overview or fallback, 700)
        except Exception:
            return fallback

    def _heuristic_theme_overview(self, *, theme: dict[str, Any], nodes: list[dict[str, Any]]) -> str:
        label = self._clean_text(theme.get("canonical_label")) or "This theme"
        descriptions = [
            self._clean_text(node.get("event_description"))
            for node in nodes
            if self._clean_text(node.get("event_description"))
        ]
        if not descriptions:
            return f"{label} remains active, but detailed event summaries are not yet available."
        if len(descriptions) == 1:
            return self._truncate(f"{label}: {descriptions[0]}", 700)
        return self._truncate(
            f"{label}: {descriptions[0]} Then, {descriptions[-1]}",
            700,
        )

    def _heuristic_event_description(self, summary: str | None) -> str:
        if not summary:
            return "Theme evolution checkpoint recorded; detailed coverage summary unavailable."

        previous_summary, new_coverage = self._split_snapshot_summary(summary)
        if new_coverage:
            return self._truncate(f"New coverage: {new_coverage}", 240)
        if previous_summary:
            return self._truncate(f"Context refresh: {previous_summary}", 240)
        return self._truncate(summary, 240)

    def _slice_articles_for_window(
        self,
        *,
        linked_articles: list[dict[str, Any]],
        window_start: datetime | None,
        window_end: datetime | None,
    ) -> list[dict[str, Any]]:
        selected: list[dict[str, Any]] = []
        for article in linked_articles:
            matched_at = article.get("matched_at")
            matched_ts = matched_at if isinstance(matched_at, datetime) else None
            if matched_ts is None:
                continue

            if window_start is not None and matched_ts <= window_start:
                continue
            if window_end is not None and matched_ts > window_end:
                continue
            selected.append(article)
        return selected

    def _derive_article_time_labels(
        self,
        *,
        window_articles: list[dict[str, Any]],
        previous_event_ts: datetime | None,
        snapshot_ts: datetime | None,
    ) -> tuple[datetime | None, str]:
        published_times = [
            article["published_at"]
            for article in window_articles
            if isinstance(article.get("published_at"), datetime)
        ]
        if published_times:
            start = min(published_times)
            end = max(published_times)
            if start.date() == end.date():
                return end, f"Published on {end.strftime('%b %d, %Y')}"
            return end, f"Published {start.strftime('%b %d, %Y')} to {end.strftime('%b %d, %Y')}"

        if snapshot_ts is not None and previous_event_ts is not None:
            return snapshot_ts, f"{previous_event_ts.strftime('%b %d, %Y')} to {snapshot_ts.strftime('%b %d, %Y')}"
        if snapshot_ts is not None:
            return snapshot_ts, f"Up to {snapshot_ts.strftime('%b %d, %Y')}"
        if previous_event_ts is not None:
            return previous_event_ts, f"After {previous_event_ts.strftime('%b %d, %Y')}"
        return None, "Unknown period"

    def _build_article_refs(self, window_articles: list[dict[str, Any]], max_items: int) -> list[dict[str, Any]]:
        sorted_articles = sorted(
            window_articles,
            key=lambda row: (
                self._to_sort_timestamp(row.get("published_at")),
                self._to_sort_timestamp(row.get("matched_at")),
            ),
            reverse=True,
        )
        refs: list[dict[str, Any]] = []
        for row in sorted_articles:
            refs.append(
                {
                    "article_id": str(row.get("article_id") or ""),
                    "title": self._clean_text(row.get("title")) or "(untitled)",
                    "canonical_url": str(row.get("canonical_url") or ""),
                    "source_name": self._clean_text(row.get("source_name")) or "Unknown source",
                    "published_at": row.get("published_at")
                    if isinstance(row.get("published_at"), datetime)
                    else None,
                }
            )
            if len(refs) >= max(1, int(max_items)):
                break
        return refs

    @staticmethod
    def _split_snapshot_summary(summary: str) -> tuple[str | None, str | None]:
        normalized = " ".join(summary.split()).strip()
        if not normalized:
            return None, None

        marker_previous = "Previous summary:"
        marker_new = "New linked articles:"
        lower = normalized.lower()
        idx_new = lower.rfind(marker_new.lower())
        idx_prev: int = -1
        if idx_new != -1:
            idx_prev = lower.rfind(marker_previous.lower(), 0, idx_new)
        else:
            idx_prev = lower.rfind(marker_previous.lower())

        if idx_new != -1:
            new_coverage = normalized[idx_new + len(marker_new) :].strip(" .;|")
            previous_summary = (
                normalized[idx_prev + len(marker_previous) : idx_new].strip(" .;|")
                if idx_prev != -1
                else None
            )
            return previous_summary or None, new_coverage or None

        if idx_prev != -1:
            previous_summary = normalized[idx_prev + len(marker_previous) :].strip(" .;|")
            return previous_summary or None, None

        return normalized, None

    @staticmethod
    def _truncate(value: str, max_len: int) -> str:
        if len(value) <= max_len:
            return value
        if max_len <= 3:
            return value[:max_len]
        return value[: max_len - 3].rstrip() + "..."

    @staticmethod
    def _clean_text(value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        normalized = " ".join(value.split()).strip()
        return normalized or None

    @staticmethod
    def _to_sort_timestamp(value: Any) -> float:
        if not isinstance(value, datetime):
            return -1.0
        return value.timestamp()

    @staticmethod
    def _build_fallback_snapshot(theme: dict[str, Any]) -> dict[str, Any]:
        created_at = (
            theme.get("last_snapshot_at")
            or theme.get("updated_at")
            or theme.get("created_at")
        )
        return {
            "snapshot_version": int(theme.get("current_snapshot_version") or 0),
            "snapshot_created_at": created_at if isinstance(created_at, datetime) else None,
            "summary": theme.get("summary"),
            "article_count": int(theme.get("article_count") or 0),
            "status": theme.get("status"),
            "discovery_method": theme.get("discovery_method"),
            "canonical_label": theme.get("canonical_label"),
            "slug": theme.get("slug"),
        }
