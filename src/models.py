from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class DocumentIn(BaseModel):
    title: str = Field(..., min_length=1, max_length=512)
    body: str = Field(..., min_length=1)
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class Document(BaseModel):
    id: UUID
    tenant_id: str
    title: str
    body: str
    tags: list[str]
    metadata: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class IndexAck(BaseModel):
    id: UUID
    status: str = "accepted"


class SearchHit(BaseModel):
    id: UUID
    score: float
    title: str
    snippet: str | None = None
    tags: list[str] = Field(default_factory=list)


class FacetBucket(BaseModel):
    value: str
    count: int


class SearchResponse(BaseModel):
    took_ms: int
    total: int
    hits: list[SearchHit]
    cache: str = "miss"
    facets: dict[str, list[FacetBucket]] | None = None
