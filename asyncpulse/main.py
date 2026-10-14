import asyncio
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .config import Settings
from .models import SearchRequest, SearchResult
from .scheduler import Scheduler
from .service import SearchService
from .storage import Cache, History


def create_app(settings: Settings | None = None, transport=None):
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app):
        cache = Cache(settings.redis_url, settings.cache_ttl)
        history = History(settings.database_url)
        client = httpx.AsyncClient(
            transport=transport,
            limits=httpx.Limits(
                max_connections=settings.global_limit,
                max_keepalive_connections=settings.global_limit,
            ),
        )
        service = SearchService(settings, Scheduler(settings, client, cache), history)
        app.state.service = service
        try:
            await history.start()
            await service.start()
            yield
        finally:
            await service.close()
            await client.aclose()
            await cache.close()
            await history.close()

    app = FastAPI(title="AsyncPulse", version="0.1.0", lifespan=lifespan)

    def get_job(job_id):
        job = app.state.service.jobs.get(job_id)
        if job is None:
            raise HTTPException(404, "Search not found")
        return job

    def submit(body):
        try:
            return app.state.service.submit(body)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        except asyncio.QueueFull as exc:
            raise HTTPException(503, "Search queue is full", headers={"Retry-After": "1"}) from exc

    @app.get("/health")
    async def health():
        return {"status": "ok", "queue_size": app.state.service.queue.qsize()}

    @app.get("/sources")
    async def sources():
        return {"sources": list(settings.sources)}

    @app.get("/metrics")
    async def metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.post("/search", response_model=SearchResult)
    async def search(body: SearchRequest, request: Request):
        job = submit(body)
        try:
            while not job.done.is_set():
                if await request.is_disconnected():
                    await app.state.service.cancel(job)
                    raise HTTPException(499, "Client disconnected")
                try:
                    await asyncio.wait_for(job.done.wait(), 0.1)
                except TimeoutError:
                    pass
            return job.result
        except asyncio.CancelledError:
            await app.state.service.cancel(job)
            raise

    @app.post("/search/jobs", response_model=SearchResult, status_code=202)
    async def create_job(body: SearchRequest):
        return submit(body).result

    @app.get("/search/{job_id}", response_model=SearchResult)
    async def get_search(job_id: str):
        return get_job(job_id).result

    @app.delete("/search/{job_id}", response_model=SearchResult)
    async def cancel_search(job_id: str):
        job = get_job(job_id)
        await app.state.service.cancel(job)
        return job.result

    @app.websocket("/search/{job_id}/ws")
    async def progress(websocket: WebSocket, job_id: str):
        job = app.state.service.jobs.get(job_id)
        if job is None:
            await websocket.close(code=1008)
            return
        await websocket.accept()

        async def send():
            revision = -1
            while True:
                async with job.changed:
                    await job.changed.wait_for(lambda previous=revision: job.revision != previous)
                    revision = job.revision
                    payload = job.result.model_dump()
                await websocket.send_json(payload)
                if job.done.is_set():
                    return

        async def receive():
            while True:
                message = await websocket.receive_text()
                if message == "cancel":
                    await app.state.service.cancel(job)

        sender = asyncio.create_task(send())
        receiver = asyncio.create_task(receive())
        try:
            done, _ = await asyncio.wait({sender, receiver}, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        except WebSocketDisconnect:
            await app.state.service.cancel(job)
        finally:
            sender.cancel()
            receiver.cancel()
            await asyncio.gather(sender, receiver, return_exceptions=True)
            try:
                await websocket.close()
            except RuntimeError:
                pass

    return app


app = create_app()
