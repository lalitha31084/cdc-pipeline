import asyncio
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from sse_starlette.sse import EventSourceResponse
import requests
import os
import json
import redis.asyncio as redis
from fastapi.templating import Jinja2Templates

app = FastAPI()

templates = Jinja2Templates(directory="templates")

MEILI_URL = os.getenv("MEILI_URL", "http://meilisearch:7700")
API_KEY = os.getenv("MEILI_MASTER_KEY", "masterKey")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/cdc-stream")
async def stream(request: Request):
    r = redis.from_url(f"redis://{REDIS_HOST}:6379", decode_responses=True)
    pubsub = r.pubsub()
    await pubsub.subscribe('cdc_events')

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message:
                    yield {
                        "event": "message",
                        "id": "message_id",
                        "retry": 15000,
                        "data": message['data']
                    }
        except asyncio.CancelledError:
            pass
        finally:
            await pubsub.unsubscribe('cdc_events')
            await r.aclose()

    return EventSourceResponse(event_generator(),
        media_type="text/event-stream"
    )

@app.get("/search")
def search(q: str):
    response = requests.get(
        f"{MEILI_URL}/indexes/products/search",
        headers={"Authorization": f"Bearer {API_KEY}"},
        params={"q": q}
    )
    return response.json()
