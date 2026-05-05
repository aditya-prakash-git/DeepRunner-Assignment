"""Latency benchmark for the search service.

Workflow:
  1. Seed `--seed-docs` documents under `--tenant` (skipped with --no-seed).
  2. Wait for the async indexer to drain the queue (best-effort sleep).
  3. Drive `--total` GET /search requests at `--concurrency`, with a 50/50 mix
     of cache-hot (same query) and cache-cold (rotating queries).
  4. Print p50 / p90 / p95 / p99 / max latency, throughput, and status mix.

Usage (from repo root, with the docker-compose stack already up):
    python -m pip install httpx
    python bench/bench_search.py --total 2000 --concurrency 50 --seed-docs 500
"""
from __future__ import annotations

import argparse
import asyncio
import random
import statistics
import string
import time
from dataclasses import dataclass

import httpx

WORDS = (
    "earnings revenue contract invoice quarterly compliance roadmap migration "
    "incident postmortem onboarding latency throughput retention churn pipeline "
    "deployment rollback canary tenant shard index cache cluster region replica "
    "backup snapshot encryption authentication authorization scaling"
).split()


def _random_doc(rng: random.Random) -> dict:
    title_len = rng.randint(3, 7)
    body_len = rng.randint(40, 120)
    return {
        "title": " ".join(rng.choice(WORDS) for _ in range(title_len)),
        "body": " ".join(rng.choice(WORDS) for _ in range(body_len)),
        "tags": rng.sample(WORDS, k=rng.randint(1, 4)),
        "metadata": {"author": "".join(rng.choices(string.ascii_lowercase, k=6))},
    }


@dataclass
class Result:
    status: int
    latency_ms: float


async def seed(client: httpx.AsyncClient, tenant: str, n: int) -> None:
    rng = random.Random(42)
    headers = {"X-Tenant-ID": tenant}
    sem = asyncio.Semaphore(20)

    async def post_one() -> None:
        async with sem:
            await client.post("/documents", json=_random_doc(rng), headers=headers)

    print(f"seeding {n} docs for tenant={tenant} ...")
    await asyncio.gather(*[post_one() for _ in range(n)])
    print("seed complete; waiting 5s for indexer drain")
    await asyncio.sleep(5.0)


async def hammer(
    client: httpx.AsyncClient, tenant: str, total: int, concurrency: int
) -> list[Result]:
    headers = {"X-Tenant-ID": tenant}
    queries = [
        "earnings", "contract", "compliance", "incident", "deployment",
        "tenant", "cache", "shard", "snapshot", "encryption",
    ]
    sem = asyncio.Semaphore(concurrency)
    results: list[Result] = []

    async def one(i: int) -> None:
        # 50/50 hot vs. cold to exercise both cache paths.
        q = queries[0] if i % 2 == 0 else queries[i % len(queries)]
        async with sem:
            t0 = time.perf_counter()
            r = await client.get(f"/search?q={q}&size=10", headers=headers)
            results.append(Result(status=r.status_code, latency_ms=(time.perf_counter() - t0) * 1000))

    await asyncio.gather(*[one(i) for i in range(total)])
    return results


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * p
    f, c = int(k), min(int(k) + 1, len(s) - 1)
    return s[f] + (s[c] - s[f]) * (k - f)


def report(results: list[Result], wall_seconds: float) -> None:
    ok = [r for r in results if r.status == 200]
    latencies = [r.latency_ms for r in ok]
    status_mix: dict[int, int] = {}
    for r in results:
        status_mix[r.status] = status_mix.get(r.status, 0) + 1

    print()
    print(f"requests:   {len(results)}")
    print(f"successful: {len(ok)} ({len(ok) / max(1, len(results)) * 100:.1f}%)")
    print(f"throughput: {len(results) / wall_seconds:.1f} req/s (over {wall_seconds:.2f}s)")
    print(f"status mix: {status_mix}")
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


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default="http://localhost:8000")
    ap.add_argument("--tenant", default="acme")
    ap.add_argument("--total", type=int, default=1000)
    ap.add_argument("--concurrency", type=int, default=50)
    ap.add_argument("--seed-docs", type=int, default=200)
    ap.add_argument("--no-seed", action="store_true")
    args = ap.parse_args()

    timeout = httpx.Timeout(10.0)
    limits = httpx.Limits(max_connections=args.concurrency * 2, max_keepalive_connections=args.concurrency)
    async with httpx.AsyncClient(base_url=args.base_url, timeout=timeout, limits=limits) as client:
        if not args.no_seed:
            await seed(client, args.tenant, args.seed_docs)

        print(f"hammering /search: total={args.total} concurrency={args.concurrency}")
        t0 = time.perf_counter()
        results = await hammer(client, args.tenant, args.total, args.concurrency)
        wall = time.perf_counter() - t0
        report(results, wall)


if __name__ == "__main__":
    asyncio.run(main())
