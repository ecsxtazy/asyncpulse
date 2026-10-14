import asyncio
import time

import httpx
import pytest
from fastapi.testclient import TestClient

from asyncpulse.config import Settings
from asyncpulse.main import create_app
from asyncpulse.models import SearchRequest
from asyncpulse.scheduler import AdaptiveGate, Scheduler
from asyncpulse.service import SearchService
from asyncpulse.storage import Cache, History


def settings(**kwargs):
    return Settings(
        sources={f"s{i}": f"http://mock/{i}" for i in range(4)}, requests_per_second=10000, **kwargs
    )


@pytest.fixture
async def components():
    calls = []

    async def handle(request):
        calls.append(request.url.path)
        await asyncio.sleep(0.05)
        return httpx.Response(200, json={"price": 10})

    config = settings()
    client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    cache = Cache(None, 60)
    scheduler = Scheduler(config, client, cache)
    service = SearchService(config, scheduler, History(None))
    await service.start()
    yield service, scheduler, calls
    await service.close()
    await client.aclose()


async def test_concurrency_and_cache(components):
    service, _, calls = components
    started = time.monotonic()
    concurrent = service.submit(SearchRequest(query="phone", use_cache=False))
    await concurrent.done.wait()
    concurrent_time = time.monotonic() - started
    started = time.monotonic()
    sequential = service.submit(SearchRequest(query="phone", mode="sequential", use_cache=False))
    await sequential.done.wait()
    assert time.monotonic() - started > concurrent_time * 2
    job = service.submit(SearchRequest(query="cached"))
    await job.done.wait()
    before = len(calls)
    repeat = service.submit(SearchRequest(query="cached"))
    await repeat.done.wait()
    assert len(calls) == before
    assert all(result.cached for result in repeat.result.results)


async def test_coalescing_survives_one_cancellation(components):
    _, scheduler, calls = components
    first = asyncio.create_task(scheduler.fetch("s0", "same", False))
    second = asyncio.create_task(scheduler.fetch("s0", "same", False))
    await asyncio.sleep(0.01)
    first.cancel()
    await asyncio.gather(first, return_exceptions=True)
    assert (await second).data == {"price": 10}
    assert len(calls) == 1
    assert not scheduler.flights


async def test_cancel_last_waiter_releases_resources(components):
    service, scheduler, _ = components
    job = service.submit(SearchRequest(query="cancel", use_cache=False))
    await asyncio.sleep(0.01)
    await service.cancel(job)
    assert job.result.status == "cancelled"
    assert not scheduler.flights
    assert all(gate.active == 0 for gate in scheduler.gates.values())
    following = service.submit(SearchRequest(query="next"))
    await following.done.wait()
    assert following.result.status == "completed"


async def test_retry_timeout_and_adaptation():
    attempts = 0

    async def handle(request):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(429, headers={"Retry-After": "0"})
        return httpx.Response(200, json={"ok": True})

    config = settings(retries=1, request_timeout=0.01)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        scheduler = Scheduler(config, client, Cache(None, 60))
        result = await scheduler.fetch("s0", "retry", False)
        assert result.data == {"ok": True}
        assert attempts == 2
        assert scheduler.gates["s0"].limit == 1

    async def slow(request):
        await asyncio.sleep(1)
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(slow)) as client:
        scheduler = Scheduler(config, client, Cache(None, 60))
        assert (await scheduler.fetch("s0", "slow", False)).error == "TimeoutError"
        assert scheduler.gates["s0"].active == 0


async def test_gate_growth_and_decrease():
    gate = AdaptiveGate(8, 100)
    for _ in range(4):
        await gate.feedback(True)
    assert gate.limit == 3
    await gate.feedback(False)
    assert gate.limit == 1


async def test_queue_backpressure_and_queued_cancellation():
    config = settings(queue_size=1)
    service = SearchService(config, None, None)
    job = service.submit(SearchRequest(query="a"))
    with pytest.raises(asyncio.QueueFull):
        service.submit(SearchRequest(query="b"))
    await service.cancel(job)
    assert job.done.is_set()
    assert job.result.status == "cancelled"


async def test_search_deadline(components):
    service, scheduler, _ = components
    service.settings.search_timeout = 0.01
    job = service.submit(SearchRequest(query="deadline"))
    await job.done.wait()
    assert job.result.status == "failed"
    assert not scheduler.flights


def test_http_and_websocket():
    async def handle(request):
        await asyncio.sleep(0.02)
        return httpx.Response(200, json={"price": 42})

    app = create_app(settings(), httpx.MockTransport(handle))
    with TestClient(app) as client:
        assert client.get("/health").status_code == 200
        assert (
            client.post("/search", json={"query": "x", "sources": ["missing"]}).status_code == 422
        )
        response = client.post("/search/jobs", json={"query": "x"})
        assert response.status_code == 202
        job_id = response.json()["id"]
        with client.websocket_connect(f"/search/{job_id}/ws") as ws:
            message = ws.receive_json()
            while message["status"] not in {"completed", "failed", "cancelled"}:
                message = ws.receive_json()
            assert message["status"] == "completed"
            assert len(message["results"]) == 4
        assert client.get(f"/search/{job_id}").status_code == 200
        assert client.get("/search/missing").status_code == 404
        assert "asyncpulse_upstream_total" in client.get("/metrics").text


def test_websocket_disconnect_cancels():
    async def handle(request):
        await asyncio.sleep(1)
        return httpx.Response(200, json={})

    with TestClient(create_app(settings(), httpx.MockTransport(handle))) as client:
        job_id = client.post("/search/jobs", json={"query": "x"}).json()["id"]
        with client.websocket_connect(f"/search/{job_id}/ws") as ws:
            ws.receive_json()
        assert client.get(f"/search/{job_id}").json()["status"] == "cancelled"


async def test_global_http_limit():
    active = 0
    peak = 0

    async def handle(request):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0.01)
            return httpx.Response(200, json={})
        finally:
            active -= 1

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        scheduler = Scheduler(settings(global_limit=2), client, Cache(None, 60))
        await asyncio.gather(*(scheduler.fetch(f"s{i % 4}", str(i), False) for i in range(12)))
        assert peak == 2
        assert active == 0
        assert not scheduler.flights


async def test_processing_helpers(tmp_path):
    from asyncpulse.processing import cpu_fingerprint, export_json, fingerprint

    target = tmp_path / "result.json"
    await export_json(target, '{"ok": true}')
    assert target.read_text() == '{"ok": true}'
    assert await cpu_fingerprint(b"example", rounds=100) == fingerprint(b"example", 100)
