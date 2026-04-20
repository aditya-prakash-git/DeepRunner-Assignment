from __future__ import annotations

import json
import logging
from uuid import UUID, uuid4

from fastapi import APIRouter, HTTPException, Request

from ..config import settings
from ..deps import state
from ..models import Document, DocumentIn, IndexAck

log = logging.getLogger(__name__)
router = APIRouter(prefix="/documents", tags=["documents"])


@router.post("", response_model=IndexAck, status_code=202)
async def create_document(payload: DocumentIn, request: Request) -> IndexAck:
    """Write-through to Postgres (source of truth), then enqueue an
    async indexing job to Redis Streams. Returns 202 once the durable
    write is committed — search visibility is eventual.
    """
    tenant_id: str = request.state.tenant_id
    doc_id = uuid4()

    async with state.pg_pool.acquire() as con:
        await con.execute(
            """
            INSERT INTO documents (id, tenant_id, title, body, tags, metadata)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb)
            """,
            doc_id,
            tenant_id,
            payload.title,
            payload.body,
            payload.tags,
            json.dumps(payload.metadata),
        )

    await state.redis.xadd(
        settings.index_stream,
        {"op": "upsert", "id": str(doc_id), "tenant_id": tenant_id},
        maxlen=100_000,
        approximate=True,
    )

    log.info("document_accepted", extra={"tenant": tenant_id, "doc_id": str(doc_id)})
    return IndexAck(id=doc_id)


@router.get("/{doc_id}", response_model=Document)
async def get_document(doc_id: UUID, request: Request) -> Document:
    tenant_id: str = request.state.tenant_id
    row = await state.pg_pool.fetchrow(
        """
        SELECT id, tenant_id, title, body, tags, metadata, created_at, updated_at
        FROM documents
        WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL
        """,
        doc_id,
        tenant_id,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="document not found")
    d = dict(row)
    # asyncpg returns JSONB as str — normalize.
    if isinstance(d["metadata"], str):
        d["metadata"] = json.loads(d["metadata"])
    return Document(**d)


@router.delete("/{doc_id}", status_code=202)
async def delete_document(doc_id: UUID, request: Request):
    tenant_id: str = request.state.tenant_id
    result = await state.pg_pool.execute(
        """
        UPDATE documents SET deleted_at = now()
        WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL
        """,
        doc_id,
        tenant_id,
    )
    if result.endswith(" 0"):
        raise HTTPException(status_code=404, detail="document not found")

    await state.redis.xadd(
        settings.index_stream,
        {"op": "delete", "id": str(doc_id), "tenant_id": tenant_id},
        maxlen=100_000,
        approximate=True,
    )
    return {"id": str(doc_id), "status": "accepted"}
