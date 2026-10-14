import asyncio
import hashlib
import os

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

app = FastAPI(title="AsyncPulse reproducible mock sources")


@app.get("/shops/{shop}")
async def shop_search(
    shop: int, q: str = Query(min_length=1), delay: float | None = Query(default=None, ge=0, le=10)
):
    await asyncio.sleep(delay if delay is not None else float(os.getenv("MOCK_DELAY", "0.5")))
    if q == "rate-limit":
        return JSONResponse(
            {"error": "rate limited"}, status_code=429, headers={"Retry-After": "0.2"}
        )
    if q == "failure":
        return JSONResponse({"error": "unavailable"}, status_code=503)
    if q == "slow":
        await asyncio.sleep(5)
    digest = hashlib.sha256(f"{shop}:{q}".encode()).hexdigest()
    return {
        "shop": shop,
        "query": q,
        "price": 100 + int(digest[:4], 16) % 10000 / 100,
        "currency": "RUB",
    }
