import asyncio
import hashlib
import random
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass

import httpx
from prometheus_client import Counter, Gauge, Histogram

from .models import SourceResult

UPSTREAM = Counter("asyncpulse_upstream_total", "Upstream attempts", ["source", "outcome"])
LATENCY = Histogram("asyncpulse_upstream_seconds", "Upstream attempt latency", ["source"])
LIMIT = Gauge("asyncpulse_source_limit", "Adaptive source concurrency", ["source"])


class AdaptiveGate:
    def __init__(self, maximum: int, rate: float):
        self.maximum = maximum
        self.limit = min(2, maximum)
        self.active = 0
        self.successes = 0
        self.condition = asyncio.Condition()
        self.rate_lock = asyncio.Lock()
        self.interval = 1 / rate
        self.next_request = 0.0

    @asynccontextmanager
    async def slot(self):
        async with self.condition:
            await self.condition.wait_for(lambda: self.active < self.limit)
            self.active += 1
        try:
            async with self.rate_lock:
                await asyncio.sleep(max(0, self.next_request - time.monotonic()))
                self.next_request = time.monotonic() + self.interval
            yield
        finally:
            async with self.condition:
                self.active -= 1
                self.condition.notify_all()

    async def feedback(self, healthy: bool):
        async with self.condition:
            if healthy:
                self.successes += 1
                if self.successes >= self.limit * 2:
                    self.limit = min(self.maximum, self.limit + 1)
                    self.successes = 0
            else:
                self.limit = max(1, self.limit // 2)
                self.successes = 0
            self.condition.notify_all()


@dataclass
class Flight:
    task: asyncio.Task
    waiters: int = 0


class Scheduler:
    def __init__(self, settings, client: httpx.AsyncClient, cache):
        self.settings = settings
        self.client = client
        self.cache = cache
        self.semaphore = asyncio.Semaphore(settings.global_limit)
        self.gates = {
            name: AdaptiveGate(settings.source_limit, settings.requests_per_second)
            for name in settings.sources
        }
        self.lock = asyncio.Lock()
        self.flights: dict[str, Flight] = {}

    async def fetch(self, source: str, query: str, use_cache: bool) -> SourceResult:
        key = "asyncpulse:" + hashlib.sha256(f"{source}:{query}".encode()).hexdigest()
        if use_cache:
            cached = await self.cache.get(key)
            if cached is not None:
                return SourceResult(source=source, data=cached, cached=True)
        flight_key = f"{key}:{use_cache}"
        async with self.lock:
            flight = self.flights.get(flight_key)
            if flight is None:
                flight = Flight(asyncio.create_task(self._fetch(source, query, key, use_cache)))
                self.flights[flight_key] = flight
            flight.waiters += 1
        try:
            return (await asyncio.shield(flight.task)).model_copy(deep=True)
        finally:
            async with self.lock:
                flight.waiters -= 1
                if flight.waiters == 0:
                    self.flights.pop(flight_key, None)
                    if not flight.task.done():
                        flight.task.cancel()
            if flight.waiters == 0:
                await asyncio.gather(flight.task, return_exceptions=True)

    async def _fetch(self, source, query, key, use_cache):
        started = time.monotonic()
        gate = self.gates[source]
        error = "upstream unavailable"
        for attempt in range(self.settings.retries + 1):
            delay = min(2, 0.1 * 2**attempt) + random.uniform(0, 0.05)
            try:
                async with gate.slot(), self.semaphore:
                    before = time.monotonic()
                    try:
                        async with asyncio.timeout(self.settings.request_timeout):
                            response = await self.client.get(
                                self.settings.sources[source], params={"q": query}
                            )
                    finally:
                        LATENCY.labels(source).observe(time.monotonic() - before)
                if response.status_code == 429 or response.status_code >= 500:
                    await gate.feedback(False)
                    UPSTREAM.labels(source, str(response.status_code)).inc()
                    error = f"HTTP {response.status_code}"
                    if response.status_code == 429:
                        try:
                            delay = min(
                                5, max(delay, float(response.headers.get("retry-after", 0)))
                            )
                        except ValueError:
                            pass
                else:
                    response.raise_for_status()
                    data = response.json()
                    if not isinstance(data, dict):
                        raise ValueError("upstream must return an object")
                    await gate.feedback(time.monotonic() - before < 0.75)
                    UPSTREAM.labels(source, "success").inc()
                    if use_cache:
                        await self.cache.put(key, data)
                    return SourceResult(
                        source=source, data=data, elapsed_ms=(time.monotonic() - started) * 1000
                    )
            except (TimeoutError, httpx.TransportError) as exc:
                await gate.feedback(False)
                UPSTREAM.labels(source, "timeout_or_network").inc()
                error = type(exc).__name__
            except (httpx.HTTPStatusError, ValueError) as exc:
                error = str(exc)
                UPSTREAM.labels(source, "invalid_response").inc()
                break
            finally:
                LIMIT.labels(source).set(gate.limit)
            if attempt < self.settings.retries:
                await asyncio.sleep(delay)
        return SourceResult(
            source=source, error=error, elapsed_ms=(time.monotonic() - started) * 1000
        )

    async def close(self):
        tasks = [flight.task for flight in self.flights.values()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
