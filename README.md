# Distributed Document Search Service

Prototype of a multi-tenant, horizontally scalable document search service. Built for the Software Engineer technical assessment.

- [Architecture](docs/architecture.md)
- [Production readiness analysis](docs/production-readiness.md)
- [Experience showcase](docs/experience-showcase.md) (TODOs for you to fill in)

## Stack

FastAPI · OpenSearch · Postgres · Redis · Redis Streams · Docker Compose

## Run locally

```bash
docker compose up --build
```

Services exposed:

| Service      | Port | Notes |
|--------------|------|-------|
| API          | 8000 | OpenAPI docs at `http://localhost:8000/docs` |
| Postgres     | 5432 | `dsearch / dsearch / dsearch` |
| Redis        | 6379 | |
| OpenSearch   | 9200 | security disabled for local only |

Seed tenants (`acme`, `globex`) are inserted on first boot.

## Sample requests

All non-public endpoints require the `X-Tenant-ID` header.

### Index a document (202 — async)

```bash
curl -s -X POST http://localhost:8000/documents \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: acme" \
  -d '{
    "title": "Q1 2026 Earnings",
    "body": "Revenue grew 28% year over year, driven by enterprise adoption.",
    "tags": ["finance", "earnings", "2026"],
    "metadata": {"author": "ada", "confidential": false}
  }'
```

### Search

```bash
curl -s "http://localhost:8000/search?q=earnings&size=10" \
  -H "X-Tenant-ID: acme"
```

Response includes `took_ms`, `total`, hits with highlighted snippets, and a `cache: hit|miss` marker.

### Get a document

```bash
curl -s http://localhost:8000/documents/<uuid> \
  -H "X-Tenant-ID: acme"
```

### Delete a document

```bash
curl -s -X DELETE http://localhost:8000/documents/<uuid> \
  -H "X-Tenant-ID: acme"
```

### Health

```bash
curl -s http://localhost:8000/health
# { "status": "ok", "dependencies": { "postgres": true, "redis": true, "opensearch": true } }
```

### Tenant isolation check (should be 403)

```bash
# Ask for acme's document while presenting globex credentials
curl -i http://localhost:8000/documents/<acme-uuid> \
  -H "X-Tenant-ID: globex"
# HTTP/1.1 404 Not Found
```

### Rate limit check (should be 429)

```bash
for i in $(seq 1 400); do
  curl -s -o /dev/null -w "%{http_code}\n" \
    "http://localhost:8000/search?q=test" \
    -H "X-Tenant-ID: globex"
done | sort | uniq -c
# You'll see a mix of 200s and 429s once burst is exceeded (globex is capped at 50 rps).
```

## Project layout

```
src/
  main.py              FastAPI app + lifespan + middleware wiring
  config.py            Pydantic settings (env-driven)
  deps.py              Shared connection handles (pg, redis, opensearch)
  middleware.py        Tenant auth + Redis token-bucket rate limit
  search.py            OpenSearch mapping + query builder
  models.py            Pydantic request/response schemas
  routes/
    documents.py       POST / GET / DELETE /documents
    search.py          GET /search with Redis result cache
    health.py          GET /health with dependency probes
  workers/
    indexer.py         Redis Streams consumer → OpenSearch upsert/delete
sql/schema.sql         Tenants + documents tables, seed tenants
tests/                 Unit tests (pure-function tests, no infra required)
docs/                  Architecture, production readiness, experience showcase
docker-compose.yml     Full stack
Dockerfile             API + worker image
```

## Running tests

```bash
python -m pip install -r requirements.txt
python -m pytest tests/ -q
```

Tests here are pure-function (query builder, cache key, models). Integration tests would use `testcontainers` against real Postgres/Redis/OpenSearch — described in the production-readiness doc but not included in the prototype to keep the footprint small.

## Assumptions & shortcuts (so the reviewer doesn't have to guess)

- `X-Tenant-ID` header acts as the tenancy principal. A production deployment replaces this with a signed JWT (see `docs/production-readiness.md#security`).
- OpenSearch security is disabled in the compose file — for local only.
- The rate limiter uses a 1-second fixed window; a sliding-window variant is the first upgrade in production.
- The indexer handles at-least-once delivery via XACK. Failed entries stay in XPENDING; a reaper/DLQ is described in docs but not implemented.
- No auth / auth token in the prototype — trust boundary is at the load balancer in this design.
