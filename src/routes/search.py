from __future__ import annotations

import hashlib
import json
import logging
import time

from fastapi import APIRouter, Query, Request

from ..config import settings
from ..deps import state
from ..models import SearchHit, SearchResponse
from ..search import build_search_query

log = logging.getLogger(__name__)
router = APIRouter(tags=["search"])


def _cache_key(tenant_id: str, q: str, size: int, offset: int) -> str:
    raw = f"{tenant_id}|{q}|{size}|{offset}".encode()
    return f"sc:{tenant_id}:{hashlib.sha1(raw).hexdigest()}"


@router.get("/search", response_model=SearchResponse)
async def search(
    request: Request,
    q: str = Query(..., min_length=1, max_length=512),
    size: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0, le=10_000),
) -> SearchResponse:
    """Full-text search scoped to the caller's tenant. Results are cached
    per (tenant, q, size, offset) for a short TTL to absorb hot-query traffic
    — invalidation is TTL-based; staleness is bounded by settings.search_cache_ttl_seconds.
    """
    tenant_id: str = request.state.tenant_id
    started = time.perf_counter()

    key = _cache_key(tenant_id, q, size, offset)
    cached = await state.redis.get(key)
    if cached:
        payload = json.loads(cached)
        payload["cache"] = "hit"
        payload["took_ms"] = int((time.perf_counter() - started) * 1000)
        return SearchResponse(**payload)

    body = build_search_query(tenant_id, q, size, offset)
    resp = await state.opensearch.search(
        index=settings.index_name,
        body=body,
        routing=tenant_id,  # tenant-shard affinity
    )

    hits_raw = resp.get("hits", {})
    total = hits_raw.get("total", {}).get("value", 0)
    hits: list[SearchHit] = []
    for h in hits_raw.get("hits", []):
        src = h.get("_source", {})
        highlight = h.get("highlight", {})
        snippet = None
        if "body" in highlight and highlight["body"]:
            snippet = " … ".join(highlight["body"])
        elif "title" in highlight and highlight["title"]:
            snippet = highlight["title"][0]
        hits.append(
            SearchHit(
                id=h["_id"],
                score=h.get("_score", 0.0),
                title=src.get("title", ""),
                snippet=snippet,
                tags=src.get("tags", []),
            )
        )

    payload = {
        "took_ms": int((time.perf_counter() - started) * 1000),
        "total": total,
        "hits": [h.model_dump(mode="json") for h in hits],
        "cache": "miss",
    }
    await state.redis.set(key, json.dumps(payload), ex=settings.search_cache_ttl_seconds)
    return SearchResponse(**payload)
