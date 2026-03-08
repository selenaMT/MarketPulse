import requests
from datetime import datetime, timedelta
import json
from openai import OpenAI
from dotenv import load_dotenv
import os

def get_news():
    load_dotenv()
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    query = "Iran"
    api_key = os.getenv("NEWS_API_KEY")

    url = (
        f"https://newsapi.org/v2/everything?"
        f"q={query}&"
        f"from={yesterday}&"
        f"sortBy=popularity&"
        f"apiKey={api_key}"
    )

    res = requests.get(url)
    data = res.json()
    articles = data.get("articles", [])

    client = OpenAI(api_key=os.getenv("OPEN_AI_KEY"))

    for i in range(0,len(articles)):
        if articles[i]["description"] == None:
            continue
        print(articles[i])
        response = client.embeddings.create(
        input=articles[i]["description"],
        model="text-embedding-3-small"
        ) 
        

        print(response.data[0].embedding)
if __name__ == "__main__":
    get_news()
