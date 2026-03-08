from dotenv import load_dotenv
import os
import logging
import re

load_dotenv()
#Get API key from environment variable
api_key = os.getenv("OPENAI_API_KEY")

from langchain_experimental.graph_transformers import LLMGraphTransformer
from langchain_core.documents import Document
from langchain_openai import ChatOpenAI
import requests
from difflib import SequenceMatcher

llm = ChatOpenAI(temperature=0,model="gpt-4o")

graph_transformer = LLMGraphTransformer(llm = llm)

# WIKIDATA deduplication
WIKIDATA_SEARCH_URL = "https://www.wikidata.org/w/api.php"
WIKIDATA_TIMEOUT_SECONDS = 5
WIKIDATA_USER_AGENT = os.getenv(
    "WIKIDATA_USER_AGENT",
    "MarketPulseBot/0.1 (https://github.com/marketpulse; contact: your-email@example.com)",
)
WIKIDATA_BATCH_SIZE = int(os.getenv("WIKIDATA_BATCH_SIZE", "25"))

# Cache for WIKIDATA lookups to avoid redundant API calls
wikidata_cache = {}
logger = logging.getLogger(__name__)
QID_PATTERN = re.compile(r"^Q\d+$", re.IGNORECASE)


def _normalize_entity_name(entity_name):
    if not isinstance(entity_name, str):
        return ""
    return " ".join(entity_name.strip().split())


def _is_wikidata_qid(text):
    return bool(QID_PATTERN.match(text))

def query_wikidata(entity_name):
    """Query WIKIDATA to get the canonical entity ID and information."""
    normalized_name = _normalize_entity_name(entity_name)
    if not normalized_name:
        return None

    cache_key = normalized_name.lower()
    if cache_key in wikidata_cache:
        return wikidata_cache[cache_key]

    # Already a Wikidata QID; avoid extra network call.
    if _is_wikidata_qid(normalized_name):
        qid = normalized_name.upper()
        info = {"wikidata_id": qid, "label": qid, "description": None}
        wikidata_cache[cache_key] = info
        return info
    
    try:
        params = {
            "action": "wbsearchentities",
            "search": normalized_name,
            "language": "en",
            "format": "json",
            "limit": 1,
        }
        response = requests.get(
            WIKIDATA_SEARCH_URL,
            params=params,
            timeout=WIKIDATA_TIMEOUT_SECONDS,
            headers={"User-Agent": WIKIDATA_USER_AGENT},
        )
        response.raise_for_status()
        results = response.json()
        
        if results.get("search"):
            entity = results["search"][0]
            wikidata_info = {
                "wikidata_id": entity.get("id"),
                "label": entity.get("label"),
                "description": entity.get("description")
            }
            wikidata_cache[cache_key] = wikidata_info
            return wikidata_info
        # Negative cache to avoid repeated misses.
        wikidata_cache[cache_key] = None
    except requests.RequestException as e:
        logger.warning("WIKIDATA lookup failed for '%s': %s", normalized_name, e)
    except ValueError as e:
        logger.warning("WIKIDATA response parse failed for '%s': %s", normalized_name, e)
    
    return None


def resolve_entity(node_id):
    """Resolve an entity into canonical graph key + display label."""
    raw = _normalize_entity_name(node_id)
    if not raw:
        return {"key": "", "label": "", "description": None}

    info = query_wikidata(raw)
    if info and info.get("wikidata_id"):
        return {
            "key": info["wikidata_id"],
            "label": info.get("label") or raw,
            "description": info.get("description"),
        }
    return {"key": raw.lower(), "label": raw, "description": None}


def _chunk(items, size):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def resolve_entities_batched(node_ids):
    """Resolve unique entity ids in bounded-size sequential batches."""
    normalized_unique = []
    seen = set()
    for node_id in node_ids:
        normalized = _normalize_entity_name(node_id)
        if not normalized:
            continue
        cache_key = normalized.lower()
        if cache_key in seen:
            continue
        seen.add(cache_key)
        normalized_unique.append(normalized)

    resolved = {}
    if not normalized_unique:
        return resolved

    for batch in _chunk(normalized_unique, max(1, WIKIDATA_BATCH_SIZE)):
        for name in batch:
            try:
                resolved[name.lower()] = resolve_entity(name)
            except Exception as exc:
                logger.warning("Entity resolution failed for '%s': %s", name, exc)
                resolved[name.lower()] = {"key": name.lower(), "label": name, "description": None}

    return resolved

def are_nodes_duplicate(node1_id, node2_id, similarity_threshold=0.85):
    """Check if two nodes represent the same entity using WIKIDATA and string similarity."""
    left = _normalize_entity_name(node1_id)
    right = _normalize_entity_name(node2_id)
    if not left or not right:
        return False

    # First check string similarity
    similarity = SequenceMatcher(None, left.lower(), right.lower()).ratio()
    
    if similarity > similarity_threshold:
        return True
    
    # Query WIKIDATA for both nodes
    wikidata1 = query_wikidata(left)
    wikidata2 = query_wikidata(right)
    
    # If both found in WIKIDATA and have the same ID, they're duplicates
    if wikidata1 and wikidata2 and wikidata1.get("wikidata_id") == wikidata2.get("wikidata_id"):
        return True
    
    return False

def get_canonical_node_id(node_id):
    """Get the canonical node ID, preferring WIKIDATA ID if available."""
    normalized_node_id = _normalize_entity_name(node_id)
    if not normalized_node_id:
        return node_id

    wikidata_info = query_wikidata(normalized_node_id)
    if wikidata_info:
        return wikidata_info.get("wikidata_id", normalized_node_id)
    return normalized_node_id

def extract_knowledge_graph(text):
    documents = [Document(page_content = text)]
    graph_document = graph_transformer.convert_to_graph_documents(documents)
    return graph_document[0]

from pyvis.network import Network
import webbrowser

# Initialize once
net = Network(height="1200px", width="100%", directed=True,
              notebook=False, bgcolor="#222222", font_color="white")

# Track added nodes
added_nodes = set()

def merge_graph(graph_document):
    nodes = graph_document.nodes
    relationships = graph_document.relationships

    # Mapping of duplicate nodes to canonical nodes
    node_mapping = {}
    resolved_map = resolve_entities_batched([node.id for node in nodes])

    # Add new nodes with deduplication.
    # Use canonical key for identity, but display human-readable label.
    for node in nodes:
        normalized_node_id = _normalize_entity_name(node.id)
        if not normalized_node_id:
            continue
        resolved = resolved_map.get(normalized_node_id.lower()) or resolve_entity(normalized_node_id)
        canonical_key = resolved["key"]
        if not canonical_key:
            continue

        node_mapping[node.id] = canonical_key
        if canonical_key in added_nodes:
            continue

        display_label = resolved["label"] or canonical_key
        description = resolved["description"] or ""
        title = f"{display_label}\n{node.type}"
        if description:
            title = f"{title}\n{description}"

        net.add_node(canonical_key, label=display_label, title=title, group=node.type)
        added_nodes.add(canonical_key)

    # Add new edges using deduplicated node IDs
    for rel in relationships:
        try:
            source_id = node_mapping.get(rel.source.id, rel.source.id)
            target_id = node_mapping.get(rel.target.id, rel.target.id)
            
            # Avoid duplicate edges
            if source_id != target_id:  # Don't add self-loops
                net.add_edge(source_id, target_id, label=rel.type.lower())
        except Exception as e:
            print(f"Error adding edge: {e}")
            continue
def render_graph(output_file="knowledge_graph.html"):
    net.set_options("""
        {
            "physics": {
                "forceAtlas2Based": {
                    "gravitationalConstant": -100,
                    "centralGravity": 0.01,
                    "springLength": 200,
                    "springConstant": 0.08
                },
                "minVelocity": 0.75,
                "solver": "forceAtlas2Based"
            }
        }
    """)
    
    # Get absolute path
    abs_path = os.path.abspath(output_file)
    
    try:
        net.save_graph(abs_path)
        print(f"Graph saved to {abs_path}")
        
        # Convert Windows path to proper file:// URL
        file_url = abs_path.replace("\\", "/")
        if file_url[1] == ":":  # Windows drive letter
            file_url = "file:///" + file_url
        else:
            file_url = "file://" + file_url
        
        print(f"Opening: {file_url}")
        webbrowser.open(file_url)
    except Exception as e:
        print(f"Error rendering graph: {e}")
        print(f"File path: {abs_path}")
        print(f"Please manually open: {abs_path}")


    

