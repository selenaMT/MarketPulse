"""LLM-based text processing service for macro/financial signal extraction."""

from __future__ import annotations

import json
import os

from openai import OpenAI

MARKET_TONE_VOCAB = [
    "hawkish",
    "dovish",

    "risk_on",
    "risk_off",

    "inflation_up",
    "inflation_down",

    "growth_up",
    "growth_down",

    "liquidity_up",
    "liquidity_down",

    "uncertain",
    "neutral",
]

ASSET_IMPACT_DIRECTION_VOCAB = [
    "up",
    "down",
    "flat",
    "volatile",
    "steepen",
    "widen",
    "tighten",
    "uncertain",
]

TEXT_PROCESSING_SCHEMA = {
    "type": "json_schema",
    "name": "marketpulse_text_processing",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "reasoning_1": {
                "type": ["string", "null"],
                "description": "Optional extra reasoning field; use null when not needed.",
            },

            "event": {
                "type": "string",
                "description": (
                    "A concise factual sentence describing the primary macroeconomic or financial "
                    "development reported in the article. Prefer fewer than 15 words (not compulsory)."
                ),
            },
            "entities": {
                "type": "array",
                "description": "List of economically relevant entities mentioned in the article.",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "Entity name exactly as commonly used.",
                        },
                        "type": {
                            "type": "string",
                            "description": "Entity category.",
                            "enum": [
                                "organization",
                                "country",
                                "economic_indicator",
                                "economic_concept",
                                "business_concept",
                                "company",
                                "financial_asset",
                                "economic_asset",
                                "commodity",
                                "person",
                                "currency",
                                "sector",
                            ],
                        },
                    },
                    "required": ["name", "type"],
                    "additionalProperties": False,
                },
            },
            "region": {
                "type": "string",
                "description": "Primary geographic scope of the event.",
            },
            "reasoning_2": {
                "type": ["string", "null"],
                "description": "Optional extra reasoning field; use null when not needed.",
            },
            "market_tone": {
                "type": "string",
                "description": (
                    "Macro policy or market tone category implied by the article."
                ),
                "enum": MARKET_TONE_VOCAB,
            },
            "macro_signals": {
                "type": "array",
                "description": "Top 3-5 macroeconomic themes implied by the article.",
                "items": {
                    "type": "string",
                },
            },
            "asset_impacts": {
                "type": "array",
                "description": (
                    "Most relevant assets potentially affected by the event."
                    "You can either use direct references from the article, or use your own inference and logics"
                ),
                "items": {
                    "type": "object",
                    "properties": {
                        "asset": {
                            "type": "string",
                            "description": "Name of the affected asset or asset class.",
                        },
                        "direction": {
                            "type": "string",
                            "description": "Expected market direction for the asset.",
                            "enum": ASSET_IMPACT_DIRECTION_VOCAB,
                        },
                        "confidence": {
                            "type": "integer",
                            "minimum": 0,
                            "maximum": 100,
                            "description": (
                                "Confidence score for this impact. Higher means the impact is "
                                "more directly supported by the article."
                                "If the article explicitly states the impact, the score should be close to 100."
                                "If it's your inference based on the article, the score should reflect how strongly the inference can be made using information from article and general knowledge"
                            ),
                        },
                    },
                    "required": ["asset", "direction", "confidence"],
                    "additionalProperties": False,
                },
            },
            "relationships": {
                "type": "array",
                "description": "Causal or influence relationships implied by the article.",
                "items": {
                    "type": "object",
                    "properties": {
                        "source": {"type": "string", "description": "Source entity or concept."},
                        "relation": {"type": "string", "description": "Relationship verb/phrase."},
                        "target": {"type": "string", "description": "Target entity or concept."},
                    },
                    "required": ["source", "relation", "target"],
                    "additionalProperties": False,
                },
            },
            "keep": {
                "type": "boolean",
                "description": (
                    "Whether the article is relevant to macroeconomic, financial market, "
                    "geopolitical, or corporate economic developments."
                ),
            },
            "reasoning_3": {
                "type": ["string", "null"],
                "description": "Optional extra reasoning field; use null when not needed.",
            },
        },
        "required": [
            "event",
            "entities",
            "region",
            "market_tone",
            "macro_signals",
            "asset_impacts",
            "relationships",
            "keep",
            "reasoning_1",
            "reasoning_2",
            "reasoning_3",
        ],
        "additionalProperties": False,
    },
}

_MARKET_TONE_VOCAB_TEXT = ", ".join(MARKET_TONE_VOCAB)
_ASSET_DIRECTION_VOCAB_TEXT = ", ".join(ASSET_IMPACT_DIRECTION_VOCAB)

SYSTEM_PROMPT = (
    "You extract essential macro/financial information from news articles.\n"
    "Follow these rules strictly:\n"
    "- Return ONLY valid JSON matching the provided schema.\n"
    "- Do not use markdown.\n"
    "- Keep event factual, concise, ideally under 15 words.\n"
    f"- market_tone allowed values: {_MARKET_TONE_VOCAB_TEXT}.\n"
    "- If there is no clear policy implication, use market_tone='uncertain'.\n"
    "- If multiple descriptions are suitable for market_tone, choose the most appropriaten and most informative one.\n"
    "- macro_signals should contain 3-5 themes/narratives when possible.\n"
    "- If uncertain, macro_signals can be an empty list.\n"
    f"- asset_impacts.direction allowed values: {_ASSET_DIRECTION_VOCAB_TEXT}.\n"
    "- keep=true is when the article is relevant to business, governmental, political, economic, financial, geopolitical, legal, market, and investment. Otherwise, keep=false. Be lenient: keep should be false when you are really sure the article is irrelevant to a economics & finance tracker app.\n"
    "- If keep=false, still return valid JSON. Keep other properties empty if you want to, but keep event concise.\n"
    "- If uncertain, prefer empty lists over guessing.\n"
    "- Do not invent information not implied by the article.\n"
    "- reasoning_1, reasoning_2, reasoning_3 are required keys.\n"
    "- Set reasoning_1/reasoning_2/reasoning_3 to null when extra reasoning is not needed.\n"
    "- Only populate a reasoning field with a short string when it adds more quality to analysis and output.\n"
    "- In fields where you should output a list, output only relevant items and keep it within reasonable length.\n"
)


class TextProcessingService:
    """Service wrapper around OpenAI Responses API for structured article extraction."""

    def __init__(
        self,
        api_key: str | None = None,
        default_model: str = "gpt-4o-mini",
        invalid_json_retries: int = 2,
        client: OpenAI | None = None,
    ) -> None:
        resolved_api_key = api_key or os.getenv("OPENAI_API_KEY")
        if client is None and not resolved_api_key:
            raise ValueError("OPENAI_API_KEY not found in environment variables")
        self._client = client or OpenAI(api_key=resolved_api_key)
        self._default_model = default_model
        self._invalid_json_retries = max(invalid_json_retries, 0)

    def process(self, article_text: str, model: str | None = None) -> dict:
        text = article_text.strip()
        if not text:
            raise ValueError("article_text must be non-empty")

        model_name = model or self._default_model
        last_error: Exception | None = None
        for _ in range(self._invalid_json_retries + 1):
            response = self._client.responses.create(
                model=model_name,
                input=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": text},
                ],
                text={"format": TEXT_PROCESSING_SCHEMA},
            )
            try:
                payload = json.loads(response.output_text)
                if not isinstance(payload, dict):
                    raise ValueError("Text processing output is not a JSON object")
                return payload
            except (json.JSONDecodeError, ValueError) as exc:
                last_error = exc
                continue
        raise ValueError("Failed to parse valid JSON from text processing output") from last_error
