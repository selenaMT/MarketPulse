import os
import json
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI

# force load .env from backend root
env_path = Path(__file__).resolve().parents[3] / ".env"
load_dotenv(env_path)

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

ENTITY_SCHEMA = {
    "type": "json_schema",
    "name": "financial_entity_extraction",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "named_entities": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "mention": {"type": "string"},
                        "canonical_name": {"type": "string"},
                        "entity_type": {
                            "type": "string",
                            "enum": [
                                "PERSON",
                                "ORG",
                                "GPE",
                                "LOC",
                                "PRODUCT",
                                "CURRENCY",
                                "COMMODITY",
                                "INDEX",
                                "EVENT",
                                "DATE",
                                "LAW",
                                "OTHER"
                            ]
                        },
                        "evidence": {"type": "string"},
                        "confidence": {"type": "number"}
                    },
                    "required": [
                        "mention",
                        "canonical_name",
                        "entity_type",
                        "evidence",
                        "confidence"
                    ],
                    "additionalProperties": False
                }
            },
            "financial_concepts": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "concept": {"type": "string"},
                        "canonical_label": {"type": "string"},
                        "category": {
                            "type": "string",
                            "enum": [
                                "MACRO",
                                "MONETARY_POLICY",
                                "FISCAL_POLICY",
                                "MARKET_SENTIMENT",
                                "GEOPOLITICS",
                                "TRADE",
                                "SUPPLY_CHAIN",
                                "COMMODITIES",
                                "CREDIT",
                                "LIQUIDITY",
                                "RISK",
                                "LABOR",
                                "ENERGY",
                                "OTHER"
                            ]
                        },
                        "direction": {
                            "type": "string",
                            "enum": ["UP", "DOWN", "NEUTRAL", "UNCERTAIN"]
                        },
                        "evidence": {"type": "string"},
                        "confidence": {"type": "number"}
                    },
                    "required": [
                        "concept",
                        "canonical_label",
                        "category",
                        "direction",
                        "evidence",
                        "confidence"
                    ],
                    "additionalProperties": False
                }
            }
        },
        "required": ["named_entities", "financial_concepts"],
        "additionalProperties": False
    }
}


def extract_entities(article_text: str, model: str = "gpt-5") -> dict:
    response = client.responses.create(
        model=model,
        input=[
            {
                "role": "system",
                "content": (
                    "You extract structured information from financial, macroeconomic, "
                    "business, and geopolitical news articles. "
                    "Return only entities clearly supported by the text. "
                    "Concrete real-world things go into named_entities. "
                    "Abstract finance or geopolitical ideas go into financial_concepts. "
                    "Do not invent facts."
                )
            },
            {
                "role": "user",
                "content": article_text
            }
        ],
        text={"format": ENTITY_SCHEMA}
    )

    return json.loads(response.output_text)