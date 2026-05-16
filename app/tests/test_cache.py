"""Tests for ``CacheFactory`` — get/set, TTL, single-flight, retry."""

from __future__ import annotations

import asyncio

import pytest

from databricks_labs_dqx_app.backend.cache import MISS, CacheFactory


@pytest.fixture
def cache() -> CacheFactory:
    return CacheFactory(ttl=10)


# ---------------------------------------------------------------------------
# get / set / delete / clear
# ---------------------------------------------------------------------------


class TestBasicCRUD:
    async def test_get_missing_returns_sentinel(self, cache):
        assert await cache.get("does-not-exist") is MISS

    async def test_set_then_get_round_trips(self, cache):
        await cache.set("k", 42)
        assert await cache.get("k") == 42

    async def test_set_overwrites(self, cache):
        await cache.set("k", "first")
        await cache.set("k", "second")
        assert await cache.get("k") == "second"

    async def test_delete_makes_key_disappear(self, cache):
        await cache.set("k", 1)
        await cache.delete("k")
        assert await cache.get("k") is MISS

    async def test_delete_missing_is_noop(self, cache):
        await cache.delete("never-set")  # must not raise

    async def test_clear_removes_everything(self, cache):
        await cache.set("a", 1)
        await cache.set("b", 2)
        await cache.clear()
        assert await cache.get("a") is MISS
        assert await cache.get("b") is MISS


# ---------------------------------------------------------------------------
# TTL semantics
# ---------------------------------------------------------------------------


class TestTtl:
    async def test_zero_ttl_expires_immediately(self, cache):
        await cache.set("k", "v", ttl=0)
        # Even at t=0, the entry has expires_at == monotonic(), and the
        # check is `monotonic() > expires_at`. We need a tiny sleep to
        # nudge clock past it; otherwise asserting on edge equality is
        # racy. The promise is "expires effectively immediately".
        await asyncio.sleep(0.001)
        assert await cache.get("k") is MISS

    async def test_negative_ttl_treated_as_already_expired(self, cache):
        await cache.set("k", "v", ttl=-1)
        assert await cache.get("k") is MISS

    async def test_per_call_ttl_overrides_default(self, cache):
        # Set with a tiny TTL, wait it out, ensure miss.
        await cache.set("k", "v", ttl=0)
        await asyncio.sleep(0.01)
        assert await cache.get("k") is MISS

    async def test_distinguishes_none_value_from_miss(self, cache):
        await cache.set("k", None)
        assert await cache.get("k") is None  # not MISS!


# ---------------------------------------------------------------------------
# cached() decorator
# ---------------------------------------------------------------------------


class TestCachedDecorator:
    async def test_caches_function_call(self, cache):
        calls = {"n": 0}

        # ``reliable=True`` so the set lands before the second call begins —
        # otherwise the fire-and-forget task can race with the read and the
        # second call legitimately misses.
        @cache.cached("k:simple", ttl=60, reliable=True)
        async def fetch() -> int:
            calls["n"] += 1
            return 42

        assert await fetch() == 42
        assert await fetch() == 42
        assert calls["n"] == 1

    async def test_key_template_uses_arg_names(self, cache):
        @cache.cached("k:{x}", ttl=60, reliable=True)
        async def fetch(x: int) -> int:
            return x * 2

        assert await fetch(1) == 2
        assert await fetch(2) == 4
        # Distinct keys → both should be cached independently.
        assert await cache.get("k:1") == 2
        assert await cache.get("k:2") == 4

    async def test_user_id_attribute_exposed_as_underscore_user(self, cache):
        class Service:
            def __init__(self, user_id: str) -> None:
                self.user_id = user_id

            @cache.cached("svc:{_user}:items", ttl=60, reliable=True)
            async def items(self) -> list[str]:
                return [f"item-for-{self.user_id}"]

        a = Service("alice")
        b = Service("bob")
        assert (await a.items()) == ["item-for-alice"]
        assert (await b.items()) == ["item-for-bob"]
        assert await cache.get("svc:alice:items") == ["item-for-alice"]
        assert await cache.get("svc:bob:items") == ["item-for-bob"]

    async def test_single_flight_collapses_concurrent_misses(self, cache):
        """Ten concurrent callers should still trigger only one fetch.

        This is the contract that prevents thundering-herd to upstream
        services (Unity Catalog, SCIM, etc.) when the cache is cold.
        """
        calls = {"n": 0}

        @cache.cached("hot-key", ttl=60)
        async def fetch() -> str:
            calls["n"] += 1
            await asyncio.sleep(0.05)  # simulate slow upstream
            return "result"

        results = await asyncio.gather(*(fetch() for _ in range(10)))
        assert all(r == "result" for r in results)
        assert calls["n"] == 1

    async def test_reliable_mode_awaits_set(self, cache):
        @cache.cached("rel-key", ttl=60, reliable=True)
        async def fetch() -> str:
            return "v"

        await fetch()
        # In reliable mode, set is awaited inline before fetch returns,
        # so the value is *guaranteed* present immediately.
        assert await cache.get("rel-key") == "v"


# ---------------------------------------------------------------------------
# set_reliable — exponential back-off retry
# ---------------------------------------------------------------------------


class TestSetReliable:
    async def test_succeeds_on_first_try(self, cache):
        await cache.set_reliable("k", "v")
        assert await cache.get("k") == "v"

    async def test_retries_then_succeeds(self, cache, monkeypatch):
        attempts = {"n": 0}
        original_set = cache.set

        async def flaky_set(key, value, ttl=None):
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise RuntimeError("transient")
            await original_set(key, value, ttl=ttl)

        monkeypatch.setattr(cache, "set", flaky_set)
        await cache.set_reliable("k", "v", retries=3)
        assert attempts["n"] == 3
        assert await cache.get("k") == "v"

    async def test_raises_after_exhausting_retries(self, cache, monkeypatch):
        async def always_fails(key, value, ttl=None):
            raise RuntimeError("persistent")

        monkeypatch.setattr(cache, "set", always_fails)
        with pytest.raises(RuntimeError, match="persistent"):
            await cache.set_reliable("k", "v", retries=2)


# ---------------------------------------------------------------------------
# Process-wide app_cache singleton hygiene
# ---------------------------------------------------------------------------


class TestAppCacheSingleton:
    async def test_app_cache_is_a_cache_factory(self):
        from databricks_labs_dqx_app.backend.cache import app_cache

        assert isinstance(app_cache, CacheFactory)

    async def test_clear_resets_locks_too(self, cache):
        # The lock dict only grows on miss-paths (the fast-path returns
        # before _get_lock runs). Force a miss by leaving the cache empty.
        @cache.cached("k", ttl=60, reliable=True)
        async def fetch() -> int:
            return 1

        await fetch()
        assert cache._locks  # noqa: SLF001

        await cache.clear()
        assert not cache._locks  # noqa: SLF001
