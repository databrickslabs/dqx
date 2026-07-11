"""Tests for the 30 s per-token-hash cache on ``get_user_catalog_names`` (P7.1).

The catalog listing drives authorization filtering on list endpoints, so the
cache MUST be keyed by token hash — a shared entry would leak one user's
catalog visibility to another. These tests pin that isolation property, the
TTL behaviour, and the uncached local-dev fallback.
"""

from __future__ import annotations

import asyncio
import hashlib
from types import SimpleNamespace
from unittest.mock import MagicMock

from databricks_labs_dqx_app.backend.cache import app_cache
from databricks_labs_dqx_app.backend.dependencies import get_user_catalog_names


def _ws_with_catalogs(*names: str | None) -> MagicMock:
    ws = MagicMock()
    ws.catalogs.list.return_value = [SimpleNamespace(name=n) for n in names]
    return ws


def _cache_key(token: str) -> str:
    return f"auth:catalogs:{hashlib.sha256(token.encode()).hexdigest()}"


async def _flush_cache_writes() -> None:
    """Let the decorator's fire-and-forget cache-write task run."""
    await asyncio.sleep(0.01)


class TestCacheHit:
    async def test_second_call_within_ttl_does_not_relist(self):
        ws = _ws_with_catalogs("main", "dev")

        first = await get_user_catalog_names(obo_ws=ws, token="token-a")
        await _flush_cache_writes()
        second = await get_user_catalog_names(obo_ws=ws, token="token-a")

        assert first == second == frozenset({"main", "dev"})
        assert ws.catalogs.list.call_count == 1

    async def test_none_catalog_names_are_dropped(self):
        ws = _ws_with_catalogs("main", None)

        result = await get_user_catalog_names(obo_ws=ws, token="token-a")

        assert result == frozenset({"main"})


class TestExpiry:
    async def test_expired_entry_relists(self):
        ws = _ws_with_catalogs("main")

        first = await get_user_catalog_names(obo_ws=ws, token="token-a")
        await _flush_cache_writes()
        # Force expiry through the public cache API: overwrite the entry
        # with a zero TTL so the next read is a MISS.
        await app_cache.set(_cache_key("token-a"), first, ttl=0)
        await asyncio.sleep(0.01)

        ws.catalogs.list.return_value = [SimpleNamespace(name="main"), SimpleNamespace(name="new_catalog")]
        second = await get_user_catalog_names(obo_ws=ws, token="token-a")

        assert ws.catalogs.list.call_count == 2
        assert second == frozenset({"main", "new_catalog"})


class TestPerTokenIsolation:
    async def test_different_tokens_do_not_share_entries(self):
        """Cross-user leakage guard: each token hash gets its own entry."""
        ws_a = _ws_with_catalogs("catalog_a")
        ws_b = _ws_with_catalogs("catalog_b")

        result_a = await get_user_catalog_names(obo_ws=ws_a, token="token-a")
        await _flush_cache_writes()
        result_b = await get_user_catalog_names(obo_ws=ws_b, token="token-b")
        await _flush_cache_writes()

        assert result_a == frozenset({"catalog_a"})
        assert result_b == frozenset({"catalog_b"})
        assert ws_a.catalogs.list.call_count == 1
        assert ws_b.catalogs.list.call_count == 1

        # Re-reads stay isolated: each token still resolves to its own set
        # from cache, with no extra upstream calls.
        assert await get_user_catalog_names(obo_ws=ws_a, token="token-a") == frozenset({"catalog_a"})
        assert await get_user_catalog_names(obo_ws=ws_b, token="token-b") == frozenset({"catalog_b"})
        assert ws_a.catalogs.list.call_count == 1
        assert ws_b.catalogs.list.call_count == 1


class TestLocalDevFallback:
    async def test_no_token_lists_directly_and_is_uncached(self):
        ws = _ws_with_catalogs("main")

        first = await get_user_catalog_names(obo_ws=ws, token=None)
        await _flush_cache_writes()
        second = await get_user_catalog_names(obo_ws=ws, token=None)

        assert first == second == frozenset({"main"})
        assert ws.catalogs.list.call_count == 2
