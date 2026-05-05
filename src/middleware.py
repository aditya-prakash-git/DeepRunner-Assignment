from __future__ import annotations

import logging

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from .config import settings
from .deps import state

log = logging.getLogger(__name__)

TENANT_HEADER = "X-Tenant-ID"
PUBLIC_PATHS = {"/health", "/health/", "/docs", "/openapi.json", "/redoc"}


class TenantAuthMiddleware(BaseHTTPMiddleware):
    """Resolves X-Tenant-ID, verifies against the tenants table, and attaches
    a `TenantContext` to request.state. Endpoints under PUBLIC_PATHS are skipped.

    NOTE: For the prototype we treat the header as the auth principal. Production
    would require a signed JWT whose claims include the tenant_id — see
    docs/production-readiness.md#security.
    """

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if path in PUBLIC_PATHS or path.startswith("/docs"):
            return await call_next(request)

        tenant_id = request.headers.get(TENANT_HEADER)
        if not tenant_id:
            return JSONResponse(
                {"error": "missing_tenant", "detail": f"{TENANT_HEADER} header required"},
                status_code=401,
            )

        pool = state.pg_pool
        if pool is None:
            return JSONResponse({"error": "not_ready"}, status_code=503)

        row = await pool.fetchrow(
            "SELECT tenant_id, rate_limit_rps FROM tenants WHERE tenant_id = $1",
            tenant_id,
        )
        if row is None:
            return JSONResponse(
                {"error": "unknown_tenant", "detail": "tenant not registered"},
                status_code=403,
            )

        request.state.tenant_id = row["tenant_id"]
        request.state.rate_limit_rps = row["rate_limit_rps"]
        return await call_next(request)


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Per-tenant token bucket stored in Redis.

    Algorithm: atomic Lua script that maintains (tokens, updated_at) per tenant.
    On each request, refill = (now - updated_at) * refill_rate, capped at capacity.
    If tokens >= 1, allow and decrement; otherwise reject with 429. Server-side
    TIME removes client clock skew. Smooth refill avoids the 2x boundary spike of
    a fixed-window counter.

    Returns {allowed, remaining_tokens} so the response can carry
    X-RateLimit-Remaining for client back-pressure.
    """

    LUA_TOKEN_BUCKET = """
    local capacity = tonumber(ARGV[1])
    local refill = tonumber(ARGV[2])
    local requested = tonumber(ARGV[3])

    local t = redis.call('TIME')
    local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

    local data = redis.call('HMGET', KEYS[1], 'tokens', 'updated_at')
    local tokens = tonumber(data[1])
    local updated_at = tonumber(data[2])
    if tokens == nil then
        tokens = capacity
        updated_at = now
    end

    local delta = math.max(0, now - updated_at)
    tokens = math.min(capacity, tokens + delta * refill)

    local allowed = 0
    if tokens >= requested then
        tokens = tokens - requested
        allowed = 1
    end

    redis.call('HMSET', KEYS[1], 'tokens', tokens, 'updated_at', now)
    redis.call('EXPIRE', KEYS[1], math.ceil(capacity / refill) + 1)

    return {allowed, tostring(tokens)}
    """

    async def dispatch(self, request: Request, call_next):
        tenant_id = getattr(request.state, "tenant_id", None)
        if tenant_id is None:
            return await call_next(request)

        refill = getattr(request.state, "rate_limit_rps", settings.default_rate_limit_rps)
        capacity = refill * settings.rate_limit_burst_multiplier

        r = state.redis
        if r is None:
            return await call_next(request)

        key = f"rl:{tenant_id}"
        try:
            result = await r.eval(
                self.LUA_TOKEN_BUCKET, 1, key, str(capacity), str(refill), "1"
            )
        except Exception:
            log.exception("rate_limit_error")
            return await call_next(request)

        allowed, remaining_str = result
        remaining = max(0, int(float(remaining_str)))

        if int(allowed) == 0:
            return JSONResponse(
                {"error": "rate_limited", "detail": f"capacity {capacity}, refill {refill}/s"},
                status_code=429,
                headers={
                    "Retry-After": "1",
                    "X-RateLimit-Limit": str(capacity),
                    "X-RateLimit-Remaining": "0",
                },
            )

        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(capacity)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        return response
