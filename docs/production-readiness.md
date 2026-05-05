# Production Readiness Analysis

The prototype demonstrates the shape of the system. This document enumerates what separates it from something that can carry a production workload at 99.95% availability.

## 1. Scalability — 100× growth (1B docs, 100k rps)

**Indexing**
- Redis Streams → Kafka when sustained write rate > 50k msg/s; partition key = `tenant_id` for ordered per-tenant updates.
- Indexer workers scale horizontally on consumer group count. Use bulk `_bulk` indexing (batch 500–1000) with back-pressure on queue depth.
- Separate hot/cold tiers in OpenSearch (ILM) — recent indices on NVMe, older on spinning disk / S3-backed searchable snapshots.

**Search**
- Shard sizing: target 20–50 GB per shard; start at 3 primaries and grow by split-index ILM action.
- Dedicated coordinator nodes for search; dedicated master nodes (3 for quorum).
- Cross-cluster search for regional read-locality; writes still go to the primary cluster, then CCR.
- Add a result-ranking layer (learning-to-rank plugin) once query volume justifies it.

**API**
- Stateless — scale horizontally. Autoscale on p95 latency and CPU jointly; CPU alone lies under async workloads.
- Move Redis to a cluster (sharded) and split cache vs. rate-limit / streams into separate clusters so cache eviction doesn't drop stream state.

## 2. Resilience

- **Timeouts** — bounded waits on every dependency. OS client `timeout=0.7s` on the API and `5s` on the indexer; Postgres `command_timeout=0.5s` on the API and `2s` on the indexer; Redis `socket_timeout=0.1s` on the API. No code path can hang on a wedged backend.
- **Dead-letter stream** — the indexer runs `XAUTOCLAIM` against entries idle > 30s and tracks delivery count via `XPENDING`. After `indexer_max_deliveries` retries (default 5) the entry moves to `docs.index.dlq` and is XACKed off the main stream so a poison message cannot block the partition.
- **Graceful degradation on /search** — on an OS exception, the route attempts to serve the long-TTL stale cache with `X-Cache: stale-while-error`; if no stale entry exists, returns 503 with `{error: search_unavailable}`. Cache get/set failures are logged but never break the request.
- **Circuit breakers** (not yet wired) on OpenSearch and Postgres clients — `pybreaker` or hand-rolled, trip on consecutive error threshold, half-open probe. The bounded-timeout + stale-cache combo above already covers most of the blast radius, but a real breaker avoids the "200 retries × 700ms" thundering herd during a sustained OS outage.
- **Retry strategy**: idempotent GETs → 3 attempts with exponential backoff + jitter; POSTs are idempotent via `Idempotency-Key` header (stored in Redis, TTL 24h). Indexer ops are naturally idempotent (XACK after success).
- **Failover**: Postgres via managed service with sync replica; OpenSearch 3-master quorum; Redis with Sentinel or managed cluster mode.

## 3. Security

- **AuthN**: replace `X-Tenant-ID` with short-lived JWTs signed by the identity provider (OAuth2 client-credentials for M2M, OIDC for user flows). Tenant claim is signed — not trustable from the client side.
- **AuthZ**: per-endpoint scopes (`docs:read`, `docs:write`, `docs:admin`). Cross-tenant reads impossible by server-side filter injection.
- **Transport**: TLS 1.3 everywhere; mTLS between services inside the mesh.
- **At-rest encryption**: Postgres TDE; OpenSearch node-level encryption; Redis AUTH + TLS. KMS-managed keys with rotation.
- **Input hardening**: body size limits (1 MB default), query length cap, rejection of queries with excessive `offset` (deep-pagination is a DoS vector — use `search_after` cursors past offset 1000).
- **PII / document sensitivity**: field-level encryption for PII fields; per-tenant at-rest keys for the compliance tier.
- **Supply chain**: SBOM on every build, dependency pinning with hash verification, Dependabot / Renovate + scheduled re-tests.
- **Secret management**: Vault / AWS Secrets Manager — no secrets in env vars baked into images.

## 4. Observability

- **Metrics** (Prometheus/OTel):
  - RED on every endpoint by tenant: requests, errors, duration (p50/p95/p99).
  - Index lag: time from POST→indexed (exported by the worker).
  - Cache hit ratio by tenant and endpoint.
  - Rate-limit rejections by tenant (product signal, not just ops).
  - OS cluster health, shard sizes, JVM heap, indexing rate.
- **Logs**: structured JSON, trace-id correlated, tenant_id as a first-class field. Ship to Loki / Datadog. Retain 30 days hot, 12 months cold.
- **Tracing**: OpenTelemetry spans across API → Redis → OS → PG. Sampled 1% baseline, 100% on errors. Attach tenant_id + route to every span.
- **SLOs** (published): `/search` p95 < 500ms, availability 99.95%. Error budget burn alerts on the 1h and 6h windows.

## 5. Performance

- **OpenSearch**: force-merge cold indices to 1 segment; tune `refresh_interval=5s` on write-heavy indices (we don't need 1s); enable request cache for filtered aggregations.
- **Postgres**: partial indices (`WHERE deleted_at IS NULL`) already in schema. Partition `documents` by `tenant_id` hash once a single table exceeds ~100M rows.
- **Query shaping**: cap `size` at 100; require `search_after` for deep pagination; fuzziness `AUTO` only above 3 chars.
- **Connection pooling**: PgBouncer in front of Postgres (transaction mode).
- **Response compression**: gzip/brotli at the edge.
- **Benchmark targets** to validate before scale-up: 10k qps with p95 < 300ms on warm cache.

### Reproducible local benchmark

The repo ships two self-contained benchmark drivers. After `docker compose up --build` is healthy:

```bash
python -m pip install httpx

# Mixed cold/warm queries — exercises full path including OpenSearch
python bench/bench_search.py --total 500 --concurrency 10 --seed-docs 200 --tenant globex

# Same query, repeatedly — exercises Redis cache short-circuit
python bench/bench_warm_cache.py --total 2000 --concurrency 10 --tenant globex
```

#### Measured results (Windows 11 laptop, Docker Desktop, single-node stack)

After raising `globex` rps to 1000 to remove rate-limiter interference (default acme is 50 rps × 2 burst):

| Workload | total | concurrency | success | throughput | p50 | p95 | p99 | max |
|---|---|---|---|---|---|---|---|---|
| Mixed cold queries (10 distinct terms) | 500 | 10 | 100% | 306 req/s | 29 ms | **45 ms** | 168 ms | 179 ms |
| Same query, warm cache (100% hits) | 2000 | 10 | 100% | 324 req/s | 29 ms | **43 ms** | 53 ms | 96 ms |
| Mixed queries, burst @ concurrency 50 | 500 | 50 | 96% | 189 req/s | 61 ms | 124 ms | 200 ms | 297 ms |

Both unsaturated runs land p95 around 45 ms, an order of magnitude below the 500 ms SLA target. The warm-cache p99 (53 ms) is much tighter than the cold p99 (168 ms) — the Redis short-circuit is doing real work. The third row is the saturation case: at concurrency 50 a single uvicorn worker pair on a laptop can't fan out fast enough, OS connect timeouts trip on ~4% of requests, and the route returns 503 instead of hanging. That is the bounded-timeout fallback path firing — preferable to unbounded queueing under load.

Reproduce with the commands above.

## 6. Operations

- **Deploy**: containers on k8s, blue-green via two Services behind a weighted ingress; `/health` drives readiness probes; 1% canary for 30 min before 100%.
- **Zero-downtime migrations**:
  - Postgres: expand-contract (add column → backfill → switch read → drop old).
  - OpenSearch: write alias + reindex into new mapping → flip alias atomically.
- **Backup/recovery**:
  - Postgres: continuous WAL archiving, PITR, nightly logical dumps.
  - OpenSearch: snapshot to S3 every hour; treat as rebuildable-from-Postgres for worst-case (re-index is authoritative).
  - Redis: cache is rebuildable; streams require AOF + replica (don't lose unacked events).
- **Runbooks**: per-alert, with a single "did you check X" first step. Chaos-test monthly (kill a node in each tier during business hours).

## 7. SLA — path to 99.95% availability

- **Budget**: 21.6 min/month downtime. A single incident > 15 min eats the month's budget.
- **Multi-AZ everything**: API, OS, PG, Redis — no single-AZ components.
- **Read-path independence**: if the write path is down, search still serves. The prototype already separates them; production must keep that discipline.
- **Release gating**: no deploy without a 10-minute "burn" window under canary; automatic rollback on error-rate regression.
- **Game-day**: quarterly region-failure drills with measured recovery time.
- **Dependency SLAs**: downstream SLAs (OS, PG, Redis managed services) must multiply to ≥ 99.99% — 99.95% at the edge needs that headroom.

## 8. Cost

- OpenSearch dominates — tier by recency (hot/warm/cold), use searchable snapshots for cold to push storage to S3.
- Cache aggressively: a 30% cache-hit ratio reduces OS spend ~30%.
- Right-size indexer workers to queue depth (KEDA scaler on stream length).
- Reserved instances for steady-state capacity + on-demand for spillover.
