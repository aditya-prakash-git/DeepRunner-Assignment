"""Warm-cache benchmark — drives the same query repeatedly to measure
the cache-hot read path. With the 30s TTL and tenant-scoped cache, every
request after the first ~1 should be served from Redis.
"""
from __future__ import annotations

import argparse
import asyncio
import statistics
import time

import httpx


async def hammer(client: httpx.AsyncClient, tenant: str, total: int, concurrency: int):
    headers = {"X-Tenant-ID": tenant}
    sem = asyncio.Semaphore(concurrency)
    latencies: list[float] = []
    statuses: dict[int, int] = {}
    cache_mix: dict[str, int] = {}

    async def one(_i: int) -> None:
        async with sem:
            t0 = time.perf_counter()
            r = await client.get("/search?q=earnings&size=10", headers=headers)
            latencies.append((time.perf_counter() - t0) * 1000)
            statuses[r.status_code] = statuses.get(r.status_code, 0) + 1
            try:
                marker = r.json().get("cache", "?")
            except Exception:
                marker = "?"
            cache_mix[marker] = cache_mix.get(marker, 0) + 1

    t0 = time.perf_counter()
    await asyncio.gather(*[one(i) for i in range(total)])
    wall = time.perf_counter() - t0
    return latencies, statuses, cache_mix, wall


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * p
    f, c = int(k), min(int(k) + 1, len(s) - 1)
    return s[f] + (s[c] - s[f]) * (k - f)


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default="http://localhost:8000")
    ap.add_argument("--tenant", default="globex")
    ap.add_argument("--total", type=int, default=2000)
    ap.add_argument("--concurrency", type=int, default=20)
    args = ap.parse_args()

    async with httpx.AsyncClient(base_url=args.base_url, timeout=10.0) as client:
        # warm with one request so the very first cache miss isn't in the sample
        await client.get("/search?q=earnings&size=10", headers={"X-Tenant-ID": args.tenant})
        latencies, statuses, cache_mix, wall = await hammer(
            client, args.tenant, args.total, args.concurrency
        )

    print(f"requests:   {len(latencies)}")
    print(f"throughput: {len(latencies) / wall:.1f} req/s ({wall:.2f}s)")
    print(f"status mix: {statuses}")
    print(f"cache mix:  {cache_mix}")
    if latencies:
        print(
            "latency ms — "
            f"p50={percentile(latencies, 0.50):.1f}  "
            f"p90={percentile(latencies, 0.90):.1f}  "
            f"p95={percentile(latencies, 0.95):.1f}  "
            f"p99={percentile(latencies, 0.99):.1f}  "
            f"max={max(latencies):.1f}  "
            f"mean={statistics.mean(latencies):.1f}"
        )


if __name__ == "__main__":
    asyncio.run(main())
