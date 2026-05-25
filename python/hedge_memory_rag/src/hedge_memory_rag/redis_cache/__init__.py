"""Redis hot cache for the Memory_RAG_Layer (task 33.1).

Implements R19.4 ("THE Memory_RAG_Layer SHALL cache hot read paths in
Redis") with bounded-LRU semantics:

* ``cache_trade(symbol, trade)`` / ``recent_trades(symbol)`` — last
  ``N`` trades per symbol via Redis ``LPUSH`` + ``LTRIM``.
* ``cache_news(symbol, news)`` / ``recent_news(symbol)`` — last ``N``
  news items per symbol via the same ring construction.
* ``set_regime(regime)`` / ``get_regime()`` — current regime with TTL
  for staleness bounding (cache invalidation on write).
* ``set_stability_score(score)`` / ``get_stability_score()`` —
  current Trader_Stability_Score with TTL.

Connection params are resolved from the existing ``HEDGE_REDIS_URL``
environment variable (the same surface the docker-compose stack and
the Hot_Path Rust crates already use); no hardcoded URLs.

Wire-level Redis failures are translated to typed
:class:`RedisCacheError` subclasses so the
``Self_Healing_Supervisor`` (task 41.1, F2) can detect them and emit
``cache.redis.degraded`` (R25.2). Degraded-event emission is *not*
this module's responsibility; we only raise typed exceptions on the
edges.

References
----------
- Requirements §19 — R19.1, R19.4.
- Design § Components § Memory_RAG_Layer (R19) — Redis bullet.
"""

from __future__ import annotations

from .cache import RedisHotCache
from .codec import decode_payload, encode_payload
from .config import (
    DEFAULT_CONNECT_TIMEOUT_S,
    DEFAULT_KEY_NAMESPACE,
    DEFAULT_MARKET_STABILITY_TTL_S,
    DEFAULT_NEWS_PER_SYMBOL,
    DEFAULT_REGIME_TTL_S,
    DEFAULT_SOCKET_TIMEOUT_S,
    DEFAULT_STABILITY_TTL_S,
    DEFAULT_TRADES_PER_SYMBOL,
    ENV_REDIS_URL,
    RedisCacheConfig,
    load_redis_cache_config,
)
from .errors import (
    RedisCacheCodecError,
    RedisCacheConnectError,
    RedisCacheError,
    RedisCacheTimeoutError,
)

__all__ = [
    # Cache
    "RedisHotCache",
    # Config
    "DEFAULT_CONNECT_TIMEOUT_S",
    "DEFAULT_KEY_NAMESPACE",
    "DEFAULT_MARKET_STABILITY_TTL_S",
    "DEFAULT_NEWS_PER_SYMBOL",
    "DEFAULT_REGIME_TTL_S",
    "DEFAULT_SOCKET_TIMEOUT_S",
    "DEFAULT_STABILITY_TTL_S",
    "DEFAULT_TRADES_PER_SYMBOL",
    "ENV_REDIS_URL",
    "RedisCacheConfig",
    "load_redis_cache_config",
    # Codec
    "decode_payload",
    "encode_payload",
    # Errors
    "RedisCacheCodecError",
    "RedisCacheConnectError",
    "RedisCacheError",
    "RedisCacheTimeoutError",
]
