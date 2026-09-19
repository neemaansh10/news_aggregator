"""Rate limiting and retry primitives for talking to other people's servers."""

from __future__ import annotations

import asyncio
import random
import time
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

class TokenBucket:
    """Per-domain politeness limiter."""

    def __init__(self, rate_per_s: float, burst: int) -> None:
        self.rate = rate_per_s
        self.burst = burst
        self.tokens = float(burst)
        self.updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                self.tokens = min(
                    self.burst, self.tokens + (now - self.updated) * self.rate
                )
                self.updated = now
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                await asyncio.sleep((1.0 - self.tokens) / self.rate)


class RateLimiterRegistry:
    def __init__(self, rate_per_s: float = 2.0, burst: int = 5) -> None:
        self._buckets: Dict[str, TokenBucket] = {}
        self._rate, self._burst = rate_per_s, burst

    async def acquire(self, domain: str) -> None:
        bucket = self._buckets.get(domain)
        if bucket is None:
            bucket = self._buckets[domain] = TokenBucket(self._rate, self._burst)
        await bucket.acquire()


RATE_LIMITER = RateLimiterRegistry()


async def retry_async(
    fn: Callable[[], Awaitable[Any]],
    attempts: int = 3,
    base_delay: float = 0.5,
    max_delay: float = 8.0,
    retry_on: Tuple[type, ...] = (Exception,),
) -> Any:
    """Exponential backoff with full jitter (AWS-style) to avoid retry storms."""
    last: Optional[BaseException] = None
    for attempt in range(attempts):
        try:
            return await fn()
        except retry_on as exc:
            last = exc
            if attempt == attempts - 1:
                break
            delay = min(max_delay, base_delay * (2**attempt))
            await asyncio.sleep(random.uniform(0, delay))
    assert last is not None
    raise last
