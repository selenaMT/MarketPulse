from dotenv import load_dotenv
import os

load_dotenv()
#Get API key from environment variable
api_key = os.getenv("OPENAI_API_KEY")

from langchain_experimental.graph_transformers import LLMGraphTransformer
from langchain_core.documents import Document
from langchain_openai import ChatOpenAI

llm = ChatOpenAI(temperature=0,model="gpt-4o")

graph_transformer = LLMGraphTransformer(llm = llm)

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


    # Add new nodes
    for node in nodes:
        if node.id not in added_nodes:
            net.add_node(node.id, label=node.id, title=node.type, group=node.type)
            added_nodes.add(node.id)

    # Add new edges
    for rel in relationships:
        try:
            net.add_edge(rel.source.id, rel.target.id, label=rel.type.lower())
        except:
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
    net.save_graph(output_file)
    print(f"Graph saved to {os.path.abspath(output_file)}")
    webbrowser.open(f"file://{os.path.abspath(output_file)}")


    

