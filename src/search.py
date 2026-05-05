"""OpenSearch index setup and query helpers.

Multi-tenancy approach: single shared index with a mandatory `tenant_id` filter
and custom routing on tenant_id so each tenant's docs co-locate on a shard.
For physical isolation (compliance-tier tenants), swap to index-per-tenant via
an alias — see docs/production-readiness.md.
"""
from __future__ import annotations

from typing import Any

from opensearchpy import AsyncOpenSearch

from .config import settings

INDEX_MAPPING: dict[str, Any] = {
    "settings": {
        "index": {
            "number_of_shards": 3,
            "number_of_replicas": 1,
        },
        "analysis": {
            "analyzer": {
                "default": {"type": "standard"},
            }
        },
    },
    "mappings": {
        "properties": {
            "tenant_id": {"type": "keyword"},
            "title": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
            "body": {"type": "text"},
            "tags": {"type": "keyword"},
            "metadata": {"type": "object", "enabled": True},
            "created_at": {"type": "date"},
            "updated_at": {"type": "date"},
        }
    },
}


async def ensure_index(os_client: AsyncOpenSearch) -> None:
    exists = await os_client.indices.exists(index=settings.index_name)
    if not exists:
        await os_client.indices.create(index=settings.index_name, body=INDEX_MAPPING)


def build_search_query(
    tenant_id: str,
    q: str,
    size: int,
    offset: int,
    tag_filters: list[str] | None = None,
    facets: bool = False,
) -> dict[str, Any]:
    filters: list[dict[str, Any]] = [{"term": {"tenant_id": tenant_id}}]
    if tag_filters:
        # AND semantics: every tag must be present on the document.
        filters.extend({"term": {"tags": t}} for t in tag_filters)

    body: dict[str, Any] = {
        "from": offset,
        "size": size,
        "query": {
            "bool": {
                "filter": filters,
                "must": [
                    {
                        "multi_match": {
                            "query": q,
                            "fields": ["title^3", "body", "tags^2"],
                            "fuzziness": "AUTO",
                            "operator": "or",
                        }
                    }
                ],
            }
        },
        "highlight": {
            "fields": {
                "title": {"number_of_fragments": 0},
                "body": {"fragment_size": 150, "number_of_fragments": 2},
            }
        },
    }
    if facets:
        body["aggs"] = {
            "tags": {"terms": {"field": "tags", "size": 20}},
        }
    return body
