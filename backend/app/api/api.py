import requests
from newsapi import NewsApiClient

from datetime import datetime, timedelta
import json
from openai import OpenAI
from dotenv import load_dotenv
import os
from app.services.knowledge_graph import extract_knowledge_graph, merge_graph, render_graph

def get_news():
    load_dotenv()
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    query = "finance"
    api_key = os.getenv("NEWS_API_KEY")
    client = NewsApiClient(api_key=api_key)

    res = client.get_everything(q=query, sort_by="popularity", page_size=10)
    data = res
    articles = data.get("articles", [])

    print(data.get("status"))
    print(data.get("totalResults"))
    print(articles)

    # client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    for i in range(0,len(articles)):
        if articles[i]["description"] == None:
            continue
    #     print(articles[i])
    #     response = client.embeddings.create(
    #     input=articles[i]["description"],
    #     model="text-embedding-3-small"
    #     ) 
        

    #     print(response.data[0].embedding)

        # Extract and merge knowledge graph
        print(f"Processing article {i}")
        graph_doc = extract_knowledge_graph(articles[i]["description"])
        merge_graph(graph_doc)

    # Render the knowledge graph
    render_graph()
if __name__ == "__main__":
    get_news()
