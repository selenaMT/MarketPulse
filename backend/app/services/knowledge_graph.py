from dotenv import load_dotenv
import os

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
WIKIDATA_API_URL = "https://www.wikidata.org/w/api.php"
WIKIDATA_SEARCH_URL = "https://www.wikidata.org/w/api.php"

# Cache for WIKIDATA lookups to avoid redundant API calls
wikidata_cache = {}

def query_wikidata(entity_name):
    """Query WIKIDATA to get the canonical entity ID and information."""
    if entity_name in wikidata_cache:
        return wikidata_cache[entity_name]
    
    try:
        params = {
            "action": "wbsearchentities",
            "search": entity_name,
            "language": "en",
            "format": "json"
        }
        response = requests.get(WIKIDATA_SEARCH_URL, params=params, timeout=5)
        results = response.json()
        
        if results.get("search"):
            entity = results["search"][0]
            wikidata_info = {
                "wikidata_id": entity.get("id"),
                "label": entity.get("label"),
                "description": entity.get("description")
            }
            wikidata_cache[entity_name] = wikidata_info
            return wikidata_info
    except Exception as e:
        print(f"WIKIDATA lookup failed for '{entity_name}': {e}")
    
    return None

def are_nodes_duplicate(node1_id, node2_id, similarity_threshold=0.85):
    """Check if two nodes represent the same entity using WIKIDATA and string similarity."""
    # First check string similarity
    similarity = SequenceMatcher(None, node1_id.lower(), node2_id.lower()).ratio()
    
    if similarity > similarity_threshold:
        return True
    
    # Query WIKIDATA for both nodes
    wikidata1 = query_wikidata(node1_id)
    wikidata2 = query_wikidata(node2_id)
    
    # If both found in WIKIDATA and have the same ID, they're duplicates
    if wikidata1 and wikidata2 and wikidata1.get("wikidata_id") == wikidata2.get("wikidata_id"):
        return True
    
    return False

def get_canonical_node_id(node_id):
    """Get the canonical node ID, preferring WIKIDATA ID if available."""
    wikidata_info = query_wikidata(node_id)
    if wikidata_info:
        return wikidata_info.get("wikidata_id", node_id)
    return node_id

def extract_knowledge_graph(text):
    documents = [Document(page_content = text)]
    graph_document = graph_transformer.convert_to_graph_documents(documents)
    return graph_document[0]

from pyvis.network import Network
import os, webbrowser

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

    # Add new nodes with deduplication
    for node in nodes:
        canonical_id = get_canonical_node_id(node.id)
        
        # Check if this node is a duplicate of an existing node
        is_duplicate = False
        for existing_node_id in added_nodes:
            if are_nodes_duplicate(canonical_id, existing_node_id):
                node_mapping[node.id] = existing_node_id
                is_duplicate = True
                print(f"Deduplicated: '{node.id}' -> '{existing_node_id}'")
                break
        
        if not is_duplicate:
            if canonical_id not in added_nodes:
                net.add_node(canonical_id, label=canonical_id, title=node.type, group=node.type)
                added_nodes.add(canonical_id)
                node_mapping[node.id] = canonical_id

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


    

