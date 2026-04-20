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
