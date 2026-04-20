from __future__ import annotations

import logging
import time

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

    Algorithm: atomic Lua script increments a fixed-window counter keyed on
    tenant + 1-second bucket. Simple, lock-free, and deterministic. Swap for
    a sliding-window script in production if burst smoothing matters.
    """

    LUA_SCRIPT = """
    local current = redis.call('INCR', KEYS[1])
    if current == 1 then
        redis.call('EXPIRE', KEYS[1], ARGV[2])
    end
    return current
    """

    async def dispatch(self, request: Request, call_next):
        tenant_id = getattr(request.state, "tenant_id", None)
        if tenant_id is None:
            return await call_next(request)

        limit = getattr(request.state, "rate_limit_rps", settings.default_rate_limit_rps)
        burst = limit * settings.rate_limit_burst_multiplier

        r = state.redis
        if r is None:
            return await call_next(request)

        bucket = int(time.time())
        key = f"rl:{tenant_id}:{bucket}"
        try:
            count = await r.eval(self.LUA_SCRIPT, 1, key, str(burst), "2")
        except Exception:
            log.exception("rate_limit_error")
            return await call_next(request)

        if int(count) > burst:
            return JSONResponse(
                {"error": "rate_limited", "detail": f"exceeded {burst} req/s"},
                status_code=429,
                headers={"Retry-After": "1"},
            )
        return await call_next(request)
