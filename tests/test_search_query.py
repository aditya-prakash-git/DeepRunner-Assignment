from src.search import build_search_query


def test_query_includes_tenant_filter():
    q = build_search_query("acme", "hello world", size=10, offset=0)
    assert q["from"] == 0
    assert q["size"] == 10
    filters = q["query"]["bool"]["filter"]
    assert {"term": {"tenant_id": "acme"}} in filters


def test_query_uses_multi_match_with_field_boosts():
    q = build_search_query("globex", "quarterly report", size=5, offset=10)
    must = q["query"]["bool"]["must"]
    assert len(must) == 1
    mm = must[0]["multi_match"]
    assert mm["query"] == "quarterly report"
    assert "title^3" in mm["fields"]
    assert "tags^2" in mm["fields"]
    assert mm["fuzziness"] == "AUTO"


def test_query_pagination():
    q = build_search_query("acme", "x", size=25, offset=50)
    assert q["from"] == 50
    assert q["size"] == 25


def test_query_with_tag_filter_adds_per_tag_term_filters():
    """Tag filter is AND semantics: every tag must match. We emit one
    `term` filter per tag rather than a single `terms` filter (which is OR)."""
    q = build_search_query("acme", "hello", size=10, offset=0, tag_filters=["finance", "2026"])
    filters = q["query"]["bool"]["filter"]
    assert {"term": {"tenant_id": "acme"}} in filters
    assert {"term": {"tags": "finance"}} in filters
    assert {"term": {"tags": "2026"}} in filters


def test_query_with_facets_adds_aggs():
    q = build_search_query("acme", "hello", size=10, offset=0, facets=True)
    assert "aggs" in q
    assert q["aggs"]["tags"]["terms"]["field"] == "tags"


def test_query_without_facets_omits_aggs():
    q = build_search_query("acme", "hello", size=10, offset=0)
    assert "aggs" not in q
