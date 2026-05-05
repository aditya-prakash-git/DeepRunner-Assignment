"""Async indexer worker.

Reads operations from the Redis Stream (consumer group), fetches the canonical
row from Postgres, and upserts / deletes in OpenSearch. Uses XACK for at-least-
once semantics.

Poison-pill handling:
  - The main loop XCLAIMs entries that have been pending and idle past a
    threshold and re-tries them.
  - Each retry is bounded by `settings.indexer_max_deliveries`. After the cap
    is reached, the entry is moved to `settings.index_dlq_stream` and XACKed
    out of the main stream so it does not block the partition.
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
RECLAIM_IDLE_MS = 30_000  # Reclaim entries idle for >30s — a healthy consumer acks faster.


async def _ensure_group(r: redis.Redis) -> None:
    try:
        await r.xgroup_create(
            settings.index_stream, settings.indexer_group, id="0", mkstream=True
        )
    except redis.ResponseError as e:
        if "BUSYGROUP" in str(e):
            return
        raise


async def _route_to_dlq(
    r: redis.Redis, entry_id: str, fields: dict, deliveries: int, reason: str
) -> None:
    """Move a poison entry to the DLQ stream and ack it from the main stream."""
    payload = {
        **fields,
        "original_entry_id": entry_id,
        "deliveries": str(deliveries),
        "reason": reason[:500],
    }
    await r.xadd(settings.index_dlq_stream, payload, maxlen=10_000, approximate=True)
    await r.xack(settings.index_stream, settings.indexer_group, entry_id)
    log.error(
        "indexer_dlq_routed",
        extra={"entry_id": entry_id, "deliveries": deliveries, "reason": reason},
    )


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

    pg = await asyncpg.create_pool(
        settings.postgres_dsn,
        min_size=1,
        max_size=4,
        command_timeout=2.0,
    )
    # Redis socket_timeout must exceed XREADGROUP block duration.
    r = redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_timeout=(BLOCK_MS / 1000) + 1.0,
    )
    os_client = AsyncOpenSearch(
        hosts=[settings.opensearch_url],
        verify_certs=False,
        ssl_show_warn=False,
        timeout=5.0,
    )

    await ensure_index(os_client)
    await _ensure_group(r)

    log.info("indexer_ready", extra={"stream": settings.index_stream})

    async def process(entry_id: str, fields: dict, deliveries: int) -> None:
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
        except Exception as exc:
            log.exception(
                "index_entry_failed",
                extra={
                    "entry_id": entry_id, "op": op, "doc_id": doc_id,
                    "deliveries": deliveries,
                },
            )
            if deliveries >= settings.indexer_max_deliveries:
                await _route_to_dlq(r, entry_id, fields, deliveries, repr(exc))
            # else: leave unacked — XCLAIM loop below will retry it later.

    try:
        while True:
            # 1. Claim stale pending entries from this or any other consumer.
            claimed = await r.xautoclaim(
                settings.index_stream,
                settings.indexer_group,
                settings.indexer_consumer,
                min_idle_time=RECLAIM_IDLE_MS,
                count=BATCH_SIZE,
            )
            # xautoclaim returns (next_cursor, [(entry_id, fields), ...], deleted_ids)
            reclaimed_entries = claimed[1] if isinstance(claimed, (list, tuple)) and len(claimed) > 1 else []
            for entry_id, fields in reclaimed_entries:
                if not fields:
                    # Entry was deleted from the stream while pending — drop it.
                    await r.xack(settings.index_stream, settings.indexer_group, entry_id)
                    continue
                # Look up delivery count from XPENDING.
                pending = await r.xpending_range(
                    settings.index_stream, settings.indexer_group, min=entry_id, max=entry_id, count=1
                )
                deliveries = int(pending[0]["times_delivered"]) if pending else 1
                await process(entry_id, fields, deliveries)

            # 2. New deliveries.
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
                    await process(entry_id, fields, deliveries=1)
    finally:
        await pg.close()
        await r.aclose()
        await os_client.close()


if __name__ == "__main__":
    asyncio.run(run())
