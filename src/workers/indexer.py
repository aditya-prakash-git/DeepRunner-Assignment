"""Async indexer worker.

Reads operations from the Redis Stream (consumer group), fetches the canonical
row from Postgres, and upserts / deletes in OpenSearch. Uses XACK for at-least-
once semantics. Failed batches are logged; in production we would write them to
a dead-letter stream after N attempts.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import asyncpg
import redis.asyncio as redis
from opensearchpy import AsyncOpenSearch
from opensearchpy.exceptions import NotFoundError

from ..config import settings
from ..logging_setup import configure_logging
from ..search import ensure_index

log = logging.getLogger(__name__)

BATCH_SIZE = 64
BLOCK_MS = 2000


async def _ensure_group(r: redis.Redis) -> None:
    try:
        await r.xgroup_create(
            settings.index_stream, settings.indexer_group, id="0", mkstream=True
        )
    except redis.ResponseError as e:
        if "BUSYGROUP" in str(e):
            return
        raise


async def _handle_upsert(
    pg: asyncpg.Pool, os_client: AsyncOpenSearch, doc_id: str, tenant_id: str
) -> None:
    row = await pg.fetchrow(
        """
        SELECT id, tenant_id, title, body, tags, metadata, created_at, updated_at
        FROM documents
        WHERE id = $1 AND deleted_at IS NULL
        """,
        doc_id,
    )
    if row is None:
        log.warning("upsert_skipped_missing_row", extra={"doc_id": doc_id})
        return

    metadata = row["metadata"]
    if isinstance(metadata, str):
        metadata = json.loads(metadata)

    body: dict[str, Any] = {
        "tenant_id": row["tenant_id"],
        "title": row["title"],
        "body": row["body"],
        "tags": list(row["tags"] or []),
        "metadata": metadata,
        "created_at": row["created_at"].isoformat(),
        "updated_at": row["updated_at"].isoformat(),
    }
    await os_client.index(
        index=settings.index_name,
        id=str(row["id"]),
        body=body,
        routing=tenant_id,
        refresh=False,
    )


async def _handle_delete(os_client: AsyncOpenSearch, doc_id: str, tenant_id: str) -> None:
    try:
        await os_client.delete(
            index=settings.index_name,
            id=doc_id,
            routing=tenant_id,
            refresh=False,
        )
    except NotFoundError:
        pass


async def run() -> None:
    configure_logging()
    log.info("indexer_starting")

    pg = await asyncpg.create_pool(settings.postgres_dsn, min_size=1, max_size=4)
    r = redis.from_url(settings.redis_url, decode_responses=True)
    os_client = AsyncOpenSearch(
        hosts=[settings.opensearch_url], verify_certs=False, ssl_show_warn=False
    )

    await ensure_index(os_client)
    await _ensure_group(r)

    log.info("indexer_ready", extra={"stream": settings.index_stream})

    try:
        while True:
            resp = await r.xreadgroup(
                settings.indexer_group,
                settings.indexer_consumer,
                {settings.index_stream: ">"},
                count=BATCH_SIZE,
                block=BLOCK_MS,
            )
            if not resp:
                continue

            for _stream, entries in resp:
                for entry_id, fields in entries:
                    op = fields.get("op")
                    doc_id = fields.get("id")
                    tenant_id = fields.get("tenant_id")
                    try:
                        if op == "upsert":
                            await _handle_upsert(pg, os_client, doc_id, tenant_id)
                        elif op == "delete":
                            await _handle_delete(os_client, doc_id, tenant_id)
                        else:
                            log.warning("unknown_op", extra={"op": op})
                        await r.xack(settings.index_stream, settings.indexer_group, entry_id)
                    except Exception:
                        log.exception(
                            "index_entry_failed",
                            extra={"entry_id": entry_id, "op": op, "doc_id": doc_id},
                        )
                        # Leave unacked so it is retried by XPENDING/XCLAIM semantics.
    finally:
        await pg.close()
        await r.aclose()
        await os_client.close()


if __name__ == "__main__":
    asyncio.run(run())
