from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI
from opensearchpy import AsyncOpenSearch

from .config import settings
from .deps import state
from .logging_setup import configure_logging
from .middleware import TenantAuthMiddleware, RateLimitMiddleware
from .routes import documents, health, search
from .search import ensure_index

log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    log.info("starting up", extra={"env": settings.app_env})

    state.pg_pool = await asyncpg.create_pool(
        settings.postgres_dsn,
        min_size=1,
        max_size=10,
        command_timeout=settings.postgres_command_timeout_seconds,
    )
    state.redis = redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_timeout=settings.redis_socket_timeout_seconds,
        socket_connect_timeout=settings.redis_socket_timeout_seconds,
    )
    state.opensearch = AsyncOpenSearch(
        hosts=[settings.opensearch_url],
        verify_certs=False,
        ssl_show_warn=False,
        timeout=settings.opensearch_timeout_seconds,
    )
    await ensure_index(state.opensearch)

    try:
        yield
    finally:
        log.info("shutting down")
        if state.pg_pool:
            await state.pg_pool.close()
        if state.redis:
            await state.redis.aclose()
        if state.opensearch:
            await state.opensearch.close()


app = FastAPI(
    title="Distributed Document Search",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(RateLimitMiddleware)
app.add_middleware(TenantAuthMiddleware)

app.include_router(health.router)
app.include_router(documents.router)
app.include_router(search.router)
