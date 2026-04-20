from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter

from ..deps import state

log = logging.getLogger(__name__)
router = APIRouter(tags=["health"])


async def _check_pg() -> bool:
    if state.pg_pool is None:
        return False
    try:
        async with state.pg_pool.acquire() as con:
            await con.fetchval("SELECT 1")
        return True
    except Exception:
        log.exception("pg healthcheck failed")
        return False


async def _check_redis() -> bool:
    if state.redis is None:
        return False
    try:
        return await state.redis.ping()
    except Exception:
        log.exception("redis healthcheck failed")
        return False


async def _check_opensearch() -> bool:
    if state.opensearch is None:
        return False
    try:
        res = await state.opensearch.cluster.health()
        return res.get("status") in ("yellow", "green")
    except Exception:
        log.exception("opensearch healthcheck failed")
        return False


@router.get("/health")
async def health():
    pg, rds, os_ = await asyncio.gather(_check_pg(), _check_redis(), _check_opensearch())
    deps = {"postgres": pg, "redis": rds, "opensearch": os_}
    status = "ok" if all(deps.values()) else "degraded"
    return {"status": status, "dependencies": deps}
