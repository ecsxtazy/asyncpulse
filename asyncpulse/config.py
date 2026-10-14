import os
from dataclasses import dataclass, field


@dataclass
class Settings:
    sources: dict[str, str] = field(
        default_factory=lambda: {
            f"shop-{i}": f"{os.getenv('MOCK_URL', 'http://127.0.0.1:8001')}/shops/{i}"
            for i in range(10)
        }
    )
    workers: int = 8
    queue_size: int = 64
    global_limit: int = 32
    source_limit: int = 8
    requests_per_second: float = 20
    request_timeout: float = 2
    search_timeout: float = 15
    retries: int = 2
    cache_ttl: int = 60
    max_jobs: int = 1000
    redis_url: str | None = field(default_factory=lambda: os.getenv("REDIS_URL"))
    database_url: str | None = field(default_factory=lambda: os.getenv("DATABASE_URL"))
