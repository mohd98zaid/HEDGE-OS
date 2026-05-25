"""Connection + bounded-cache configuration for :mod:`hedge_memory_rag.redis_cache`.

The Memory_RAG_Layer Redis cache is configured **only** from the
existing ``HEDGE_REDIS_URL`` environment variable (defined in
``docker-compose.yml`` for every service container) plus a small number
of cache-specific knobs. Hardcoded ``localhost`` / ``redis://`` URLs are
forbidden — task 33.1 explicitly requires "connection params come from
the existing config loader; do not hardcode."

Bounded-cache sizes (``last N trades / news per symbol``) and the
staleness windows for current regime / current Trader_Stability_Score
follow R19.4 ("hot read paths") and the property in task 33.2 ("cache
returns the most recent value for any key within the configured
staleness window"). Sensible defaults are provided so a service can run
without bespoke ENV plumbing in dev.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Final

#: Environment variable holding the Redis URL. Populated by
#: ``docker-compose.yml`` for every Hedge service container as
#: ``redis://redis:6379``. Out-of-container deployments override it.
ENV_REDIS_URL: Final[str] = "HEDGE_REDIS_URL"

#: Fallback URL used **only** when ``HEDGE_REDIS_URL`` is unset. The
#: explicit fallback is the same loopback URL the Hot_Path Rust crates
#: use (`crates/hedge-signals/src/bin/main.rs::DEFAULT_REDIS_URL`) so
#: dev parity with the Rust side is preserved without hardcoding a
#: production value.
_FALLBACK_REDIS_URL: Final[str] = "redis://127.0.0.1:6379"

#: Per-instance key prefix. Keeps Memory_RAG_Layer keys disjoint from
#: ``hedge.hot.*`` Redis Streams used by the Hot_Path on the same
#: Redis instance.
DEFAULT_KEY_NAMESPACE: Final[str] = "hedge:rag:cache"

#: Default ring size for ``last N trades per symbol`` and
#: ``last N news items per symbol``. The retrieval pipeline (task 34.1)
#: consumes the most recent ~50 of each per reasoning event; we keep
#: 100 to absorb burstiness without unbounded growth.
DEFAULT_TRADES_PER_SYMBOL: Final[int] = 100
DEFAULT_NEWS_PER_SYMBOL: Final[int] = 100

#: Staleness windows for the global single-value caches. Both the
#: regime and the Trader_Stability_Score are refreshed continuously by
#: the Warm_AI_Pipeline, so a 5-minute TTL is generous; if the
#: pipeline stops emitting for that long the entries fall out and
#: callers see ``None`` rather than a stale value.
DEFAULT_REGIME_TTL_S: Final[int] = 300
DEFAULT_STABILITY_TTL_S: Final[int] = 300

#: Staleness window for the current `MarketStability` factor written by
#: the Market_Regime_Engine (task 22.1). The factor is consumed by the
#: Risk_Engine via the WarmCache last-known-value path; the TTL bounds
#: how long a stalled producer can leak stale stability values into the
#: Risk_Engine sizing formula. Same five-minute floor as the regime and
#: stability-score caches.
DEFAULT_MARKET_STABILITY_TTL_S: Final[int] = 300

#: Wire-level socket timeout. Five seconds is well above Redis's
#: typical sub-millisecond round-trip and guards against a daemon
#: that has accepted the TCP connection but stalled.
DEFAULT_SOCKET_TIMEOUT_S: Final[float] = 5.0
DEFAULT_CONNECT_TIMEOUT_S: Final[float] = 2.0


@dataclass(frozen=True, slots=True)
class RedisCacheConfig:
    """Resolved configuration for a :class:`RedisHotCache` instance.

    All fields have sensible defaults. The Redis URL is resolved
    via :func:`load_redis_cache_config` from ``HEDGE_REDIS_URL`` so the
    cache integrates with the existing config surface without growing
    a new env var.

    Attributes:
        redis_url: ``redis://[:password@]host:port[/db]`` URL passed
            verbatim to :class:`redis.asyncio.Redis.from_url`.
        namespace: Prefix for every key. Keeps RAG cache keys disjoint
            from Hot_Path Redis Streams keys on a shared Redis.
        trades_per_symbol: Maximum ring length for the
            ``cache_trade`` / ``recent_trades`` API (R19.4).
        news_per_symbol: Maximum ring length for the
            ``cache_news`` / ``recent_news`` API (R19.4).
        regime_ttl_s: TTL applied to the current-regime key.
        stability_ttl_s: TTL applied to the current
            Trader_Stability_Score key.
        socket_timeout_s: Per-operation socket timeout passed to the
            async Redis client. Translates :mod:`redis-py` socket
            timeouts to :class:`RedisCacheTimeoutError`.
        connect_timeout_s: Connect timeout applied during the initial
            handshake.
    """

    redis_url: str
    namespace: str = DEFAULT_KEY_NAMESPACE
    trades_per_symbol: int = DEFAULT_TRADES_PER_SYMBOL
    news_per_symbol: int = DEFAULT_NEWS_PER_SYMBOL
    regime_ttl_s: int = DEFAULT_REGIME_TTL_S
    stability_ttl_s: int = DEFAULT_STABILITY_TTL_S
    market_stability_ttl_s: int = DEFAULT_MARKET_STABILITY_TTL_S
    socket_timeout_s: float = DEFAULT_SOCKET_TIMEOUT_S
    connect_timeout_s: float = DEFAULT_CONNECT_TIMEOUT_S

    def __post_init__(self) -> None:
        if not self.redis_url:
            raise ValueError("redis_url must be a non-empty redis:// URL")
        if self.trades_per_symbol <= 0:
            raise ValueError(
                f"trades_per_symbol must be positive, got {self.trades_per_symbol!r}"
            )
        if self.news_per_symbol <= 0:
            raise ValueError(
                f"news_per_symbol must be positive, got {self.news_per_symbol!r}"
            )
        if self.regime_ttl_s <= 0:
            raise ValueError(f"regime_ttl_s must be positive, got {self.regime_ttl_s!r}")
        if self.stability_ttl_s <= 0:
            raise ValueError(
                f"stability_ttl_s must be positive, got {self.stability_ttl_s!r}"
            )
        if self.market_stability_ttl_s <= 0:
            raise ValueError(
                "market_stability_ttl_s must be positive, got "
                f"{self.market_stability_ttl_s!r}"
            )
        if self.socket_timeout_s <= 0:
            raise ValueError(
                f"socket_timeout_s must be positive, got {self.socket_timeout_s!r}"
            )
        if self.connect_timeout_s <= 0:
            raise ValueError(
                f"connect_timeout_s must be positive, got {self.connect_timeout_s!r}"
            )
        if not self.namespace:
            raise ValueError("namespace must be a non-empty string")


def load_redis_cache_config(
    *,
    namespace: str = DEFAULT_KEY_NAMESPACE,
    trades_per_symbol: int = DEFAULT_TRADES_PER_SYMBOL,
    news_per_symbol: int = DEFAULT_NEWS_PER_SYMBOL,
    regime_ttl_s: int = DEFAULT_REGIME_TTL_S,
    stability_ttl_s: int = DEFAULT_STABILITY_TTL_S,
    market_stability_ttl_s: int = DEFAULT_MARKET_STABILITY_TTL_S,
    socket_timeout_s: float = DEFAULT_SOCKET_TIMEOUT_S,
    connect_timeout_s: float = DEFAULT_CONNECT_TIMEOUT_S,
) -> RedisCacheConfig:
    """Resolve a :class:`RedisCacheConfig` from the process environment.

    The Redis URL is taken from ``HEDGE_REDIS_URL`` (the same env var
    the docker-compose stack and the Rust Hot_Path crates already
    read). The bounded-cache sizes and TTLs are overridable via
    keyword for test harnesses and bespoke deployments; production
    callers pass no kwargs and inherit the defaults.

    Args:
        namespace: Override the default key prefix.
        trades_per_symbol: Override the per-symbol trade ring length.
        news_per_symbol: Override the per-symbol news ring length.
        regime_ttl_s: Override the regime TTL in seconds.
        stability_ttl_s: Override the stability-score TTL in seconds.
        socket_timeout_s: Override the per-operation socket timeout.
        connect_timeout_s: Override the connect timeout.

    Returns:
        A frozen :class:`RedisCacheConfig`.
    """
    redis_url = os.environ.get(ENV_REDIS_URL, _FALLBACK_REDIS_URL)
    return RedisCacheConfig(
        redis_url=redis_url,
        namespace=namespace,
        trades_per_symbol=trades_per_symbol,
        news_per_symbol=news_per_symbol,
        regime_ttl_s=regime_ttl_s,
        stability_ttl_s=stability_ttl_s,
        market_stability_ttl_s=market_stability_ttl_s,
        socket_timeout_s=socket_timeout_s,
        connect_timeout_s=connect_timeout_s,
    )


__all__ = [
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
]
