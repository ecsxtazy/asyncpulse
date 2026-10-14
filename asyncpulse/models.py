from typing import Literal

from pydantic import BaseModel, Field


class SearchRequest(BaseModel):
    query: str = Field(min_length=1, max_length=200)
    sources: list[str] | None = Field(default=None, min_length=1, max_length=20)
    mode: Literal["concurrent", "sequential"] = "concurrent"
    use_cache: bool = True


class SourceResult(BaseModel):
    source: str
    data: dict | None = None
    error: str | None = None
    cached: bool = False
    elapsed_ms: float = 0


class SearchResult(BaseModel):
    id: str
    query: str
    status: Literal["queued", "running", "completed", "cancelled", "failed"]
    results: list[SourceResult] = Field(default_factory=list)
    elapsed_ms: float = 0
    error: str | None = None
