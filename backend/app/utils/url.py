"""URL normalization helpers for deduplication."""

from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

TRACKING_PREFIXES = ("utm_", "fbclid", "gclid", "mc_cid", "mc_eid")


def canonicalize_url(url: str) -> str | None:
    """Return normalized URL for dedupe/upsert, or None when invalid."""
    parsed = urlsplit(url.strip())
    if not parsed.scheme or not parsed.netloc:
        return None

    kept_query_pairs = [
        (k, v)
        for k, v in parse_qsl(parsed.query, keep_blank_values=True)
        if not any(k.lower().startswith(prefix) for prefix in TRACKING_PREFIXES)
    ]
    clean_query = urlencode(kept_query_pairs, doseq=True)
    normalized_path = parsed.path.rstrip("/") or "/"
    return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), normalized_path, clean_query, ""))

