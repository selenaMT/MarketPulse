import os
import json
from dotenv import load_dotenv
from openai import OpenAI

env_path = Path(__file__).resolve().parents[3] / ".env"
load_dotenv(env_path)

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

RELATION_SCHEMA = {
    "type": "json_schema",
    "name": "relation_scoring",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "relation_candidates": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "source_node": {"type": "string"},
                        "target_node": {"type": "string"},
                        "relation": {"type": "string"},
                        "bridge_explanation": {"type": "string"},
                        "supporting_evidence": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "scores": {
                            "type": "object",
                            "properties": {
                                "novelty": {"type": "number"},
                                "bridge_quality": {"type": "number"},
                                "evidence_strength": {"type": "number"},
                                "cross_domain_bonus": {"type": "number"},
                                "obviousness": {"type": "number"},
                                "noise_penalty": {"type": "number"},
                                "final_score": {"type": "number"}
                            },
                            "required": [
                                "novelty",
                                "bridge_quality",
                                "evidence_strength",
                                "cross_domain_bonus",
                                "obviousness",
                                "noise_penalty",
                                "final_score"
                            ],
                            "additionalProperties": False
                        }
                    },
                    "required": [
                        "source_node",
                        "target_node",
                        "relation",
                        "bridge_explanation",
                        "supporting_evidence",
                        "scores"
                    ],
                    "additionalProperties": False
                }
            }
        },
        "required": ["relation_candidates"],
        "additionalProperties": False
    }
}


def score_relations(article_text: str, extracted_entities: dict, model: str = "gpt-5") -> dict:
    response = client.responses.create(
        model=model,
        input=[
            {
                "role": "system",
                "content": (
                    "You identify interesting finance or geopolitics relation candidates from a news article.\n"
                    "Scoring meaning:\n"
                    "- novelty: recent increase vs baseline commonness\n"
                    "- bridge_quality: whether there is a clear intermediate event or concept\n"
                    "- evidence_strength: how much support exists in the article\n"
                    "- cross_domain_bonus: whether it links domains usually separate\n"
                    "- obviousness: whether the relation is too obvious\n"
                    "- noise_penalty: whether it may be accidental co-occurrence\n"
                    "Use values from 0 to 1.\n"
                    "Compute final_score = 0.30*novelty + 0.25*bridge_quality + "
                    "0.20*evidence_strength + 0.15*cross_domain_bonus - "
                    "0.05*obviousness - 0.05*noise_penalty"
                )
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "article_text": article_text,
                        "entities": extracted_entities
                    },
                    ensure_ascii=False
                )
            }
        ],
        text={"format": RELATION_SCHEMA}
    )

    return json.loads(response.output_text)