from fastapi import FastAPI
import requests
import os

app = FastAPI()

MEILI_URL = "http://meilisearch:7700"
API_KEY = os.getenv("MEILI_MASTER_KEY", "masterKey")

@app.get("/")
def home():
    return {"message": "API is running"}

@app.get("/api/cdc-stream")
def stream():
    return {"message": "CDC stream running"}

@app.get("/search")
def search(q: str):
    response = requests.get(
        f"{MEILI_URL}/indexes/products/search",
        headers={"Authorization": f"Bearer {API_KEY}"},
        params={"q": q}
    )
    return response.json()