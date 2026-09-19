"""Cache with identical semantics whether Redis is present or not.

A Redis outage degrades hit rate and cross-pod sharing; it never takes the
system down. The fallback is exercised by default, since Redis is opt-in.
"""

from __future__ import annotations

import json
import time
from collections import OrderedDict
from contextlib import suppress
from typing import Any, Optional, Tuple

from ..config import SETTINGS
from ..observability import log

try:  # pragma: no cover - optional dependency
    import redis.asyncio as aioredis

    HAVE_REDIS = True
except Exception:  # pragma: no cover
    aioredis = None  # type: ignore
    HAVE_REDIS = False

class Cache:
    """Same interface whether Redis is present or not, so no code path forks."""

    def __init__(self) -> None:
        self._redis: Any = None
        self._local: "OrderedDict[str, Tuple[float, Any]]" = OrderedDict()
        self._max_local = 20_000

    async def connect(self) -> None:
        if SETTINGS.redis_url and HAVE_REDIS:
            try:
                self._redis = aioredis.from_url(
                    SETTINGS.redis_url, encoding="utf-8", decode_responses=True
                )
                await self._redis.ping()
                log.info("cache: redis at %s", SETTINGS.redis_url)
                return
            except Exception as exc:  # pragma: no cover
                log.warning("cache: redis unavailable (%s); using in-process LRU", exc)
                self._redis = None
        log.info("cache: in-process LRU (set NEWSAGG_REDIS_URL for shared caching)")

    async def close(self) -> None:
        if self._redis is not None:
            with suppress(Exception):
                await self._redis.aclose()

    async def get(self, key: str) -> Optional[Any]:
        if self._redis is not None:
            raw = await self._redis.get(key)
            return json.loads(raw) if raw else None
        item = self._local.get(key)
        if not item:
            return None
        expiry, value = item
        if expiry < time.time():
            self._local.pop(key, None)
            return None
        self._local.move_to_end(key)
        return value

    async def set(self, key: str, value: Any, ttl: int) -> None:
        if self._redis is not None:
            await self._redis.set(key, json.dumps(value, default=str), ex=ttl)
            return
        self._local[key] = (time.time() + ttl, value)
        self._local.move_to_end(key)
        while len(self._local) > self._max_local:
            self._local.popitem(last=False)

    async def delete_prefix(self, prefix: str) -> None:
        if self._redis is not None:
            # SCAN, never KEYS - KEYS blocks the Redis event loop.
            async for key in self._redis.scan_iter(match=f"{prefix}*", count=500):
                await self._redis.delete(key)
            return
        for key in [k for k in self._local if k.startswith(prefix)]:
            self._local.pop(key, None)


CACHE = Cache()
