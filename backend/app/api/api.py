import requests
from datetime import datetime, timedelta
import json
from openai import OpenAI
from dotenv import load_dotenv
import os

# from ..services.knowledge_graph import extract_knowledge_graph, merge_graph, render_graph

def get_gdelt(query):

    gdelt_api_url = "https://api.gdeltproject.org/api/v2/doc/doc?"
    parameters = {
        "query": query,
        "mode": "ArtList",
        "timespan": "2d",
        "maxrecords": 25,
        "format": "json"
    }
    res = requests.get(gdelt_api_url, params=parameters)
    
    gdelt_data = res.json()
    articles = gdelt_data.get("articles", [])

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    results = []

    for i in range(0, len(articles)):
        if articles[i]["title"] == None:
            continue

        response = client.embeddings.create(
        input=articles[i]["title"],
        model="text-embedding-3-small"
        )

        extracted = {
            "desc": articles[i]["title"],
            "embeddings": response.data[0].embedding, 
        }
        results.append(extracted)


    #     print(f"Processing article {i}")
    #     graph_doc = extract_knowledge_graph(articles[i]["description"])
    #     merge_graph(graph_doc)
    

    # render_graph()

    
    return results
    

def get_news(query):
    load_dotenv()

    yesterday = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
    api_key = os.getenv("NEWS_API_KEY")
    news_api_url = (
        f"https://newsapi.org/v2/everything?"
        #f"q={query}&"
        f"apiKey=41ff95c7093e452b883e3c5212301edf&"
        f"domains=www.ft.com"
    )

    print(news_api_url)

    res = requests.get(news_api_url)
    data = res.json()
    articles = data.get("articles", [])

    print(articles[:10])

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    results = []

    # for i in range(0,len(articles)):
    #     if articles[i]["description"] == None:
    #         continue

    #     response = client.embeddings.create(
    #     input=articles[i]["description"],
    #     model="text-embedding-3-small"
    #     )

    #     extracted = {
    #         "desc": articles[i]["description"],
    #         "embeddings": response.data[0].embedding, 
    #     }
    #     results.append(extracted)


    #     print(f"Processing article {i}")
    #     graph_doc = extract_knowledge_graph(articles[i]["description"])
    #     merge_graph(graph_doc)
    

    # render_graph()

    
    return results
        

if __name__ == "__main__":
    # queries = ["Iran", "Crypto", "Economics", "Stocks", "Finance", "Inflation", "Policy"]
    # for query in queries:
    #     news_embeddings = get_gdelt(query)
    #     print(news_embeddings)
    get_news("Economics")
