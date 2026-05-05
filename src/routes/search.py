from __future__ import annotations

import hashlib
import json
import logging
import time

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

from ..config import settings
from ..deps import state
from ..models import SearchHit, SearchResponse
from ..search import build_search_query

log = logging.getLogger(__name__)
router = APIRouter(tags=["search"])


def _cache_key(
    tenant_id: str,
    q: str,
    size: int,
    offset: int,
    tag_filters: tuple[str, ...] = (),
    facets: bool = False,
) -> str:
    tags_part = ",".join(sorted(tag_filters))
    raw = f"{tenant_id}|{q}|{size}|{offset}|{tags_part}|{int(facets)}".encode()
    return f"sc:{tenant_id}:{hashlib.sha1(raw).hexdigest()}"


@router.get("/search", response_model=SearchResponse)
async def search(
    request: Request,
    q: str = Query(..., min_length=1, max_length=512),
    size: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0, le=10_000),
    tag: list[str] | None = Query(None, description="Filter to docs containing all listed tags"),
    facets: bool = Query(False, description="Include term aggregations (e.g. tag counts)"),
) -> SearchResponse:
    """Full-text search scoped to the caller's tenant. Results are cached
    per (tenant, q, size, offset, tags, facets) for a short TTL to absorb hot-query
    traffic — invalidation is TTL-based; staleness is bounded by
    settings.search_cache_ttl_seconds.

    Supports faceted search via `facets=true` (returns tag bucket counts) and
    tag filtering via repeated `tag=...` query params.
    """
    tenant_id: str = request.state.tenant_id
    started = time.perf_counter()
    tag_filters = tuple(tag or ())

    key = _cache_key(tenant_id, q, size, offset, tag_filters, facets)
    try:
        cached = await state.redis.get(key)
    except Exception:
        log.exception("cache_get_failed")
        cached = None

    if cached:
        payload = json.loads(cached)
        payload["cache"] = "hit"
        payload["took_ms"] = int((time.perf_counter() - started) * 1000)
        return SearchResponse(**payload)

    body = build_search_query(
        tenant_id, q, size, offset,
        tag_filters=list(tag_filters) or None,
        facets=facets,
    )
    try:
        resp = await state.opensearch.search(
            index=settings.index_name,
            body=body,
            routing=tenant_id,  # tenant-shard affinity
        )
    except Exception:
        log.exception("opensearch_search_failed")
        # Graceful degrade: serve stale cache if we have one.
        try:
            stale = await state.redis.get(key + ":stale")
        except Exception:
            stale = None
        if stale:
            payload = json.loads(stale)
            payload["cache"] = "stale"
            payload["took_ms"] = int((time.perf_counter() - started) * 1000)
            return JSONResponse(payload, headers={"X-Cache": "stale-while-error"})
        return JSONResponse(
            {"error": "search_unavailable", "detail": "search backend unavailable"},
            status_code=503,
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

    facet_payload: dict[str, list[dict[str, int]]] | None = None
    if facets:
        aggs = resp.get("aggregations", {})
        facet_payload = {
            name: [{"value": b["key"], "count": b["doc_count"]} for b in agg.get("buckets", [])]
            for name, agg in aggs.items()
        }

    payload = {
        "took_ms": int((time.perf_counter() - started) * 1000),
        "total": total,
        "hits": [h.model_dump(mode="json") for h in hits],
        "cache": "miss",
        "facets": facet_payload,
    }
    serialized = json.dumps(payload)
    try:
        # Hot cache (short TTL) + stale cache (long TTL) for graceful degrade on OS outages.
        await state.redis.set(key, serialized, ex=settings.search_cache_ttl_seconds)
        await state.redis.set(key + ":stale", serialized, ex=settings.search_cache_ttl_seconds * 60)
    except Exception:
        log.exception("cache_set_failed")
    return SearchResponse(**payload)
