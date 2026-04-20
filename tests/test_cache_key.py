from src.routes.search import _cache_key


def test_cache_key_is_tenant_scoped():
    k1 = _cache_key("acme", "invoice", 10, 0)
    k2 = _cache_key("globex", "invoice", 10, 0)
    assert k1 != k2
    assert k1.startswith("sc:acme:")
    assert k2.startswith("sc:globex:")


def test_cache_key_is_deterministic():
    assert _cache_key("acme", "q", 10, 0) == _cache_key("acme", "q", 10, 0)


def test_cache_key_changes_on_pagination():
    assert _cache_key("acme", "q", 10, 0) != _cache_key("acme", "q", 10, 10)
    assert _cache_key("acme", "q", 10, 0) != _cache_key("acme", "q", 20, 0)
