from fastapi import Depends, FastAPI, HTTPException
import redis.asyncio as redis
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import AsyncGenerator
import asyncio
import httpx
import json

from config import get_settings

settings = get_settings()

redis_client = redis.Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
    password=settings.REDIS_PASSWORD,
)

app = FastAPI(docs_url=None, redoc_url=None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def index():
    return {"message": "Server is up and running"}


async def validate_token(token: str):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                settings.AUTH_URL, headers={"Authorization": f"Bearer {token}"}
            )
            response.raise_for_status()
            result = response.json()
            company_id = result.get("company_id")
            if company_id:
                return company_id
            else:
                raise HTTPException(status_code=401, detail="Invalid token")
        except httpx.HTTPError as e:
            print(f"error = {e}")
            raise HTTPException(status_code=500, detail="Token validation failed")


async def get_current_user(token: str):
    if not token:
        raise HTTPException(status_code=401, detail="Token is required")
    company_id = await validate_token(token)
    return company_id


async def event_stream(company_id: str) -> AsyncGenerator[str, None]:
    print(f"streaming started for company_id: *{company_id}*")
    channel_name = f"LOADS-CHANNEL::-{company_id}"
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(channel_name)

    try:
        yield "retry: 3000\n\n"

        keep_alive_interval = 15  # in seconds
        last_keep_alive = asyncio.get_event_loop().time()

        while True:
            message = await pubsub.get_message(
                ignore_subscribe_messages=True, timeout=1
            )
            if message and message["type"] == "message":
                data = message["data"].decode("utf-8")

                try:
                    payload = json.loads(data)
                    event_type = payload.get("event", "default")
                    event_data = json.dumps(payload.get("data", {}))
                except Exception:
                    continue

                yield f"event: {event_type}\ndata: {event_data}\n\n"

            # Keep-alive ping every N seconds
            now = asyncio.get_event_loop().time()
            if now - last_keep_alive > keep_alive_interval:
                yield ":keep-alive\n\n"
                last_keep_alive = now

            await asyncio.sleep(0.1)
    except asyncio.CancelledError:
        print(f"Disconnected companyd_id: *{company_id}*")
    finally:
        await pubsub.unsubscribe(channel_name)
        await pubsub.close()


@app.get("/live-loads/")
async def live_loads(token: str, company_id: str = Depends(get_current_user)):
    return StreamingResponse(event_stream(company_id), media_type="text/event-stream")
