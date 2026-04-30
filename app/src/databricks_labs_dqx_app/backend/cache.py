"""Async in-memory cache with TTL support.

Provides a singleton ``app_cache`` instance shared across the application,
with a ``cached`` decorator for transparent cache-aside on async functions
and methods.  No external dependencies — uses only stdlib asyncio and time.

Usage::

    from .cache import app_cache

    @app_cache.cached("discovery:{_user}:catalogs", ttl=300)
    async def list_catalogs_async(self) -> list[CatalogInfo]: ...

    @app_cache.cached("auth:sp", ttl=2700)
    async def get_sp_ws() -> WorkspaceClient: ...
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

logger = logging.getLogger(__name__)

DEFAULT_TTL = 300  # 5 minutes
T = TypeVar("T")

MISS = object()  # sentinel distinguishing "not in cache" from a cached None


class CacheFactory:
    """Async in-memory cache with per-key TTL.

    Entries are stored as ``(value, expires_at)`` tuples.  Expiry is
    checked on every read; no background eviction thread is needed for
    an app of this scale.

    Single-flight protection: a per-key asyncio.Lock prevents duplicate
    upstream calls when multiple coroutines miss the cache simultaneously.
    """

    def __init__(self, ttl: int = DEFAULT_TTL, namespace: str = "main") -> None:
        self._ttl = ttl
        self._namespace = namespace
        self._store: dict[str, tuple[Any, float]] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    # -- core operations ------------------------------------------------

    async def get(self, key: str) -> Any:
        """Return the cached value, or the ``_MISS`` sentinel if absent/expired."""
        entry = self._store.get(key)
        if entry is None:
            return MISS
        value, expires_at = entry
        if time.monotonic() > expires_at:
            self._store.pop(key, None)
            return MISS
        return value

    async def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        effective_ttl = ttl if ttl is not None else self._ttl
        self._store[key] = (value, time.monotonic() + effective_ttl)

    async def delete(self, key: str) -> None:
        self._store.pop(key, None)

    async def clear(self) -> None:
        self._store.clear()
        self._locks.clear()

    def _get_lock(self, key: str) -> asyncio.Lock:
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]

    # -- write modes ----------------------------------------------------

    def set_fire_and_forget(self, key: str, value: Any, ttl: int | None = None) -> None:
        """Schedule a cache write without blocking the caller."""
        task = asyncio.create_task(self.set(key, value, ttl=ttl))
        task.add_done_callback(self._log_task_error)

    async def set_reliable(
        self,
        key: str,
        value: Any,
        ttl: int | None = None,
        retries: int = 3,
    ) -> None:
        """Await a cache write, retrying with exponential back-off on failure."""
        for attempt in range(1, retries + 1):
            try:
                await self.set(key, value, ttl=ttl)
                return
            except Exception:
                if attempt == retries:
                    logger.error("Cache set_reliable failed after %d attempts for key=%s", retries, key)
                    raise
                await asyncio.sleep(0.1 * (2 ** (attempt - 1)))

    # -- decorator ------------------------------------------------------

    def cached(
        self,
        key_template: str,
        *,
        ttl: int | None = None,
        reliable: bool = False,
    ) -> Callable[..., Any]:
        """Cache-aside decorator for async functions and methods.

        ``key_template`` is a format string resolved against the function's
        bound arguments (excluding ``self``).  If the first argument has a
        ``user_id`` attribute it is exposed as ``{_user}`` in the template.

        Examples::

            @app_cache.cached("auth:sp", ttl=2700)
            async def get_sp_ws() -> WorkspaceClient: ...

            @app_cache.cached("discovery:{_user}:schemas:{catalog}")
            async def list_schemas_async(self, catalog: str) -> list[SchemaInfo]: ...

            @app_cache.cached("auth:obo:{token_hash}", ttl=2700)
            async def _create_obo_ws(token_hash: str, token: str) -> WorkspaceClient: ...
        """

        def decorator(fn: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
            sig = inspect.signature(fn)

            @functools.wraps(fn)
            async def wrapper(*args: Any, **kwargs: Any) -> T:
                bound = sig.bind(*args, **kwargs)
                bound.apply_defaults()
                key_args: dict[str, Any] = {k: v for k, v in bound.arguments.items() if k != "self"}
                # Expose self.user_id as {_user} when present
                if args and hasattr(args[0], "user_id"):
                    key_args["_user"] = args[0].user_id

                cache_key = key_template.format_map(key_args)

                # Fast path — check before acquiring lock
                hit = await self.get(cache_key)
                if hit is not MISS:
                    logger.debug("Cache HIT  %s", cache_key)
                    return hit  # type: ignore[return-value]

                # Single-flight: only one coroutine fetches per key
                async with self._get_lock(cache_key):
                    # Re-check inside lock (another coroutine may have populated it)
                    hit = await self.get(cache_key)
                    if hit is not MISS:
                        logger.debug("Cache HIT (post-lock) %s", cache_key)
                        return hit  # type: ignore[return-value]

                    logger.debug("Cache MISS %s", cache_key)
                    result = await fn(*args, **kwargs)

                    if reliable:
                        await self.set_reliable(cache_key, result, ttl=ttl)
                    else:
                        self.set_fire_and_forget(cache_key, result, ttl=ttl)

                return result

            return wrapper

        return decorator

    # -- lifecycle ------------------------------------------------------

    async def shutdown(self) -> None:
        await self.clear()
        logger.info("Cache cleared during shutdown")

    # -- internal -------------------------------------------------------

    @staticmethod
    def _log_task_error(task: asyncio.Task[object]) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error("Fire-and-forget cache write failed: %s", exc)


app_cache = CacheFactory()
