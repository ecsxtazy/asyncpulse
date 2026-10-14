import asyncio
import json
import time
from datetime import UTC, datetime

from redis.asyncio import Redis
from sqlalchemy import Column, DateTime, MetaData, String, Table, Text, insert
from sqlalchemy.ext.asyncio import create_async_engine


class Cache:
    def __init__(self, url: str | None, ttl: int):
        self.redis = Redis.from_url(url, decode_responses=True) if url else None
        self.ttl = ttl
        self.values: dict[str, tuple[float, dict]] = {}
        self.lock = asyncio.Lock()

    async def get(self, key: str) -> dict | None:
        if self.redis:
            value = await self.redis.get(key)
            return json.loads(value) if value else None
        async with self.lock:
            now = time.monotonic()
            self.values = {k: v for k, v in self.values.items() if v[0] > now}
            value = self.values.get(key)
            return value[1] if value else None

    async def put(self, key: str, value: dict):
        if self.redis:
            await self.redis.set(key, json.dumps(value), ex=self.ttl)
        else:
            async with self.lock:
                now = time.monotonic()
                self.values = {k: v for k, v in self.values.items() if v[0] > now}
                if len(self.values) >= 10000:
                    self.values.pop(next(iter(self.values)))
                self.values[key] = (now + self.ttl, value)

    async def close(self):
        if self.redis:
            await self.redis.aclose()


class History:
    def __init__(self, url: str | None):
        self.engine = create_async_engine(url) if url else None
        metadata = MetaData()
        self.table = Table(
            "search_history",
            metadata,
            Column("id", String, primary_key=True),
            Column("created_at", DateTime(timezone=True), nullable=False),
            Column("payload", Text, nullable=False),
        )
        self.metadata = metadata

    async def start(self):
        if self.engine:
            async with self.engine.begin() as connection:
                await connection.run_sync(self.metadata.create_all)

    async def save(self, result):
        if self.engine:
            async with self.engine.begin() as connection:
                await connection.execute(
                    insert(self.table).values(
                        id=result.id,
                        created_at=datetime.now(UTC),
                        payload=result.model_dump_json(),
                    )
                )

    async def close(self):
        if self.engine:
            await self.engine.dispose()
