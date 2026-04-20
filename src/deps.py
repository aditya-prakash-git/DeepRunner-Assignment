"""Shared dependency instances. Initialized once in the FastAPI lifespan."""
from __future__ import annotations

from typing import Optional

import asyncpg
import redis.asyncio as redis
from opensearchpy import AsyncOpenSearch


class State:
    pg_pool: Optional[asyncpg.Pool] = None
    redis: Optional[redis.Redis] = None
    opensearch: Optional[AsyncOpenSearch] = None


state = State()


def get_pg() -> asyncpg.Pool:
    assert state.pg_pool is not None, "pg pool not initialized"
    return state.pg_pool


def get_redis() -> redis.Redis:
    assert state.redis is not None, "redis not initialized"
    return state.redis


def get_opensearch() -> AsyncOpenSearch:
    assert state.opensearch is not None, "opensearch not initialized"
    return state.opensearch
