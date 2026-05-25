"""Verify wire-level redis errors translate to typed RedisCache* exceptions."""
from __future__ import annotations

import asyncio
import sys

sys.path.insert(0, "python/hedge_memory_rag/src")

from hedge_memory_rag.redis_cache import (  # noqa: E402
    RedisCacheConfig,
    RedisCacheConnectError,
    RedisCacheTimeoutError,
    RedisHotCache,
)


async def run() -> None:
    # Use an IP guaranteed not to respond — TEST-NET-1 (RFC 5737)
    cfg = RedisCacheConfig(
        redis_url="redis://192.0.2.1:6379/0",
        connect_timeout_s=0.5,
        socket_timeout_s=0.5,
    )
    cache = RedisHotCache(cfg)
    await cache.start()
    try:
        try:
            await asyncio.wait_for(cache.cache_trade("X", {"a": 1}), timeout=3.0)
        except (RedisCacheConnectError, RedisCacheTimeoutError) as exc:
            print(f"got typed error (cache_trade): {type(exc).__name__}: op={exc.op} key={exc.key}")
        else:
            raise AssertionError("expected a typed error from unreachable redis")

        try:
            await asyncio.wait_for(cache.set_regime("Trending"), timeout=3.0)
        except (RedisCacheConnectError, RedisCacheTimeoutError) as exc:
            print(f"got typed error (set_regime): {type(exc).__name__}: op={exc.op}")
        else:
            raise AssertionError("expected a typed error from unreachable redis")

        try:
            await asyncio.wait_for(cache.get_stability_score(), timeout=3.0)
        except (RedisCacheConnectError, RedisCacheTimeoutError) as exc:
            print(f"got typed error (get_stability_score): {type(exc).__name__}: op={exc.op}")
        else:
            raise AssertionError("expected a typed error from unreachable redis")
    finally:
        await cache.aclose()
    print("wire-level error translation ok")


if __name__ == "__main__":
    asyncio.run(run())
