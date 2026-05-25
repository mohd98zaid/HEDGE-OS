"""Async Redis hot cache for :mod:`hedge_memory_rag` (task 33.1).

This module implements R19.4 ("THE Memory_RAG_Layer SHALL cache hot
read paths in Redis") for the Memory_RAG_Layer. The shape of the cache
follows the design language in § Memory_RAG_Layer:

> Redis (R19.4): hot read-path cache for recent context (last N
> trades, last N news per symbol, current regime, current stability
> score).

Bounded-LRU semantics are realised via two Redis primitives:

* **Per-symbol Redis Lists** (``LPUSH`` + ``LTRIM 0 N-1``) for the
  ``last N trades`` and ``last N news items`` rings. ``LPUSH`` puts
  the newest entry at index 0; ``LTRIM`` evicts beyond ``N``. This is
  bounded LRU at constant amortised cost — Redis serves the trim in
  ``O(N)`` on the discarded tail only.
* **Plain keys with TTL** for the current regime and the current
  Trader_Stability_Score. ``SET ... EX ttl`` overwrites the previous
  value (cache invalidation on write — task 33.1 explicitly requires
  it) and bounds staleness via TTL.

Connection, encoding, and error policy
--------------------------------------

* The Redis URL is resolved via :func:`load_redis_cache_config`
  from ``HEDGE_REDIS_URL`` so the cache integrates with the existing
  config surface; no hardcoded URLs.
* Payloads are encoded with the package's compact JSON codec
  (:mod:`hedge_memory_rag.redis_cache.codec`), matching the design
  rule that non-embedding Warm_AI_Pipeline payloads are JSON
  (embeddings, owned by task 31.1 / Qdrant, are CBOR — out of scope
  here).
* Wire-level Redis failures are translated to typed
  :class:`RedisCacheError` subclasses so the
  :class:`Self_Healing_Supervisor` (task 41.1, F2) can detect them
  and emit ``cache.redis.degraded`` (R25.2). This module **does not**
  emit degraded events itself — that responsibility lives in task
  33.2 (optional smoke test) and the supervisor.
* All public methods are async. The class is **async-safe** — multiple
  coroutines may share one :class:`RedisHotCache` against the shared
  ``redis.asyncio.Redis`` connection pool.

References
----------
- Requirements §19 (Memory and RAG Layer), in particular R19.1
  ("persist trades, market memory, psychology history, news history,
  symbol behavior, strategy outcomes, and execution statistics") and
  R19.4 ("cache hot read paths in Redis").
- Design § Components § Memory_RAG_Layer (R19) — Redis bullet.
- Design § Self-Healing — Failure_Detector subscribes to
  ``cache.redis.*``; the supervisor consumes the typed exceptions
  raised here.
"""

from __future__ import annotations

from types import TracebackType
from typing import Any, Final, Optional, Type

import structlog

# `redis.asyncio` is the canonical async client per `redis-py >= 4.2`.
# All wire-level exceptions live in `redis.exceptions`.
from redis import asyncio as aioredis
from redis import exceptions as redis_exc

from .codec import decode_payload, encode_payload
from .config import RedisCacheConfig, load_redis_cache_config
from .errors import (
    RedisCacheConnectError,
    RedisCacheError,
    RedisCacheTimeoutError,
)

_LOG: Final = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Key scheme ----------------------------------------------------------------
# ---------------------------------------------------------------------------

#: Per-symbol last-N trades list. Format:
#: ``<namespace>:trades:<symbol>``.
_KEY_TRADES: Final[str] = "trades"

#: Per-symbol last-N news list. Format:
#: ``<namespace>:news:<symbol>``.
_KEY_NEWS: Final[str] = "news"

#: Single-value current regime key. Format:
#: ``<namespace>:regime:current``.
_KEY_REGIME_CURRENT: Final[str] = "regime:current"

#: Single-value current Trader_Stability_Score key. Format:
#: ``<namespace>:psych:stability_score:current``.
_KEY_STABILITY_CURRENT: Final[str] = "psych:stability_score:current"


def _normalise_symbol(symbol: str) -> str:
    """Validate and normalise a symbol string before composing a key.

    Symbols must be non-empty and free of the reserved ``:`` separator
    used by the namespace scheme. We do not lowercase — the rest of
    Hedge keeps NSE symbols upper-case.
    """
    if not isinstance(symbol, str):
        raise TypeError(f"symbol must be str, got {type(symbol).__name__}")
    if not symbol:
        raise ValueError("symbol must be non-empty")
    if ":" in symbol:
        raise ValueError(f"symbol must not contain ':' separator: {symbol!r}")
    return symbol


# ---------------------------------------------------------------------------
# Public class --------------------------------------------------------------
# ---------------------------------------------------------------------------


class RedisHotCache:
    """Async bounded-LRU hot cache for Memory_RAG_Layer read paths.

    Construct with :meth:`from_env` for production deployments
    (``HEDGE_REDIS_URL`` resolution) or pass an explicit
    :class:`RedisCacheConfig` for tests.

    Lifecycle::

        async with await RedisHotCache.from_env() as cache:
            await cache.cache_trade("RELIANCE", trade_event)
            recent = await cache.recent_trades("RELIANCE")

    Or manage the underlying client manually::

        cache = RedisHotCache(config)
        await cache.start()
        try:
            ...
        finally:
            await cache.aclose()
    """

    def __init__(
        self,
        config: RedisCacheConfig,
        *,
        client: aioredis.Redis | None = None,
    ) -> None:
        """Construct the cache.

        Args:
            config: Resolved :class:`RedisCacheConfig`.
            client: Pre-built :class:`redis.asyncio.Redis` client. If
                ``None`` (the common case), the cache builds its own
                via :meth:`Redis.from_url` on :meth:`start`.
        """
        self._config: RedisCacheConfig = config
        self._owns_client: bool = client is None
        self._client: Optional[aioredis.Redis] = client
        self._closed: bool = False

    # ----- factories + lifecycle -------------------------------------------

    @classmethod
    async def from_env(cls) -> "RedisHotCache":
        """Build a cache from ``HEDGE_REDIS_URL`` and call :meth:`start`."""
        cache = cls(load_redis_cache_config())
        await cache.start()
        return cache

    @classmethod
    async def connect(cls, config: RedisCacheConfig) -> "RedisHotCache":
        """Build and start a cache from an explicit config (test helper)."""
        cache = cls(config)
        await cache.start()
        return cache

    async def __aenter__(self) -> "RedisHotCache":
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: Type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def start(self) -> None:
        """Create the underlying :class:`redis.asyncio.Redis` if owned.

        Raises:
            RedisCacheConnectError: the URL is malformed.
        """
        if self._client is not None:
            return
        try:
            self._client = aioredis.Redis.from_url(
                self._config.redis_url,
                socket_timeout=self._config.socket_timeout_s,
                socket_connect_timeout=self._config.connect_timeout_s,
                decode_responses=False,
                # health_check_interval surfaces broken connections to
                # the supervisor on the next operation rather than only
                # on the next bind cycle.
                health_check_interval=30,
            )
        except (ValueError, redis_exc.RedisError) as exc:
            raise RedisCacheConnectError(
                f"failed to construct Redis client for {self._config.redis_url!r}: {exc}",
                op="start",
            ) from exc
        self._owns_client = True

    async def aclose(self) -> None:
        """Close the underlying client (if owned). Idempotent."""
        if self._closed:
            return
        self._closed = True
        if self._client is not None and self._owns_client:
            try:
                await self._client.aclose()
            except redis_exc.RedisError as exc:  # pragma: no cover - logged
                _LOG.warning("redis_cache_close_failed", error=str(exc))
        self._client = None

    @property
    def config(self) -> RedisCacheConfig:
        """Read-only view of the resolved config."""
        return self._config

    # ----- key composition -------------------------------------------------

    def _ns(self, suffix: str) -> str:
        return f"{self._config.namespace}:{suffix}"

    def _trade_key(self, symbol: str) -> str:
        return self._ns(f"{_KEY_TRADES}:{_normalise_symbol(symbol)}")

    def _news_key(self, symbol: str) -> str:
        return self._ns(f"{_KEY_NEWS}:{_normalise_symbol(symbol)}")

    def _regime_key(self) -> str:
        return self._ns(_KEY_REGIME_CURRENT)

    def _stability_key(self) -> str:
        return self._ns(_KEY_STABILITY_CURRENT)

    # ----- bounded-LRU ring helpers ----------------------------------------

    async def _push_bounded(
        self,
        *,
        key: str,
        payload: bytes,
        max_len: int,
        op: str,
    ) -> None:
        """Atomic ``LPUSH`` + ``LTRIM`` to enforce a bounded LRU ring.

        Both commands are issued in a single MULTI/EXEC pipeline so a
        crashing client cannot leave the list temporarily over-bounded.
        """
        client = self._require_client(op=op)
        try:
            async with client.pipeline(transaction=True) as pipe:
                pipe.lpush(key, payload)
                pipe.ltrim(key, 0, max_len - 1)
                await pipe.execute()
        except redis_exc.TimeoutError as exc:
            raise RedisCacheTimeoutError(
                f"timeout on {op} for key={key!r}: {exc}",
                op=op,
                key=key,
            ) from exc
        except redis_exc.ConnectionError as exc:
            raise RedisCacheConnectError(
                f"connection error on {op} for key={key!r}: {exc}",
                op=op,
                key=key,
            ) from exc
        except redis_exc.RedisError as exc:
            raise RedisCacheError(
                f"redis error on {op} for key={key!r}: {exc}",
                op=op,
                key=key,
            ) from exc

    async def _read_ring(self, *, key: str, op: str) -> list[Any]:
        """Read the full ring at ``key`` newest-first. Decodes each entry."""
        client = self._require_client(op=op)
        try:
            raw_items: list[bytes] = await client.lrange(key, 0, -1)
        except redis_exc.TimeoutError as exc:
            raise RedisCacheTimeoutError(
                f"timeout on {op} for key={key!r}: {exc}",
                op=op,
                key=key,
            ) from exc
        except redis_exc.ConnectionError as exc:
            raise RedisCacheConnectError(
                f"connection error on {op} for key={key!r}: {exc}",
                op=op,
                key=key,
            ) from exc
        except redis_exc.RedisError as exc:
            raise RedisCacheError(
                f"redis error on {op} for key={key!r}: {exc}",
                op=op,
                key=key,
            ) from exc
        return [decode_payload(item, op=op, key=key) for item in raw_items]

    # ----- per-symbol trades ring (R19.1, R19.4) ---------------------------

    async def cache_trade(self, symbol: str, trade: Any) -> None:
        """Push a trade onto the per-symbol ``last N`` ring.

        Args:
            symbol: Symbol identifier (e.g. ``"RELIANCE"``).
            trade: Pydantic model or JSON-compatible mapping.

        Raises:
            RedisCacheConnectError, RedisCacheTimeoutError,
            RedisCacheCodecError: see module docstring.
        """
        key = self._trade_key(symbol)
        payload = encode_payload(trade, op="cache_trade", key=key)
        await self._push_bounded(
            key=key,
            payload=payload,
            max_len=self._config.trades_per_symbol,
            op="cache_trade",
        )

    async def recent_trades(self, symbol: str) -> list[Any]:
        """Return the ring of trades for ``symbol`` newest-first.

        Returns an empty list if the ring is empty or has expired.
        """
        return await self._read_ring(key=self._trade_key(symbol), op="recent_trades")

    # ----- per-symbol news ring (R19.1, R19.4) -----------------------------

    async def cache_news(self, symbol: str, news: Any) -> None:
        """Push a news item onto the per-symbol ``last N`` ring."""
        key = self._news_key(symbol)
        payload = encode_payload(news, op="cache_news", key=key)
        await self._push_bounded(
            key=key,
            payload=payload,
            max_len=self._config.news_per_symbol,
            op="cache_news",
        )

    async def recent_news(self, symbol: str) -> list[Any]:
        """Return the ring of news items for ``symbol`` newest-first."""
        return await self._read_ring(key=self._news_key(symbol), op="recent_news")

    # ----- current regime (R19.4) ------------------------------------------

    async def set_regime(self, regime: Any) -> None:
        """Replace the current regime cache entry (cache invalidation on write).

        ``SET ... EX <ttl>`` overwrites any previous value atomically
        and bounds staleness via the configured TTL.

        Args:
            regime: A :class:`RegimeChanged` model, the regime label
                string, or any JSON-compatible payload. The cache is
                opaque about the shape.
        """
        key = self._regime_key()
        payload = encode_payload(regime, op="set_regime", key=key)
        client = self._require_client(op="set_regime")
        try:
            await client.set(key, payload, ex=self._config.regime_ttl_s)
        except redis_exc.TimeoutError as exc:
            raise RedisCacheTimeoutError(
                f"timeout on set_regime for key={key!r}: {exc}",
                op="set_regime",
                key=key,
            ) from exc
        except redis_exc.ConnectionError as exc:
            raise RedisCacheConnectError(
                f"connection error on set_regime for key={key!r}: {exc}",
                op="set_regime",
                key=key,
            ) from exc
        except redis_exc.RedisError as exc:
            raise RedisCacheError(
                f"redis error on set_regime for key={key!r}: {exc}",
                op="set_regime",
                key=key,
            ) from exc

    async def get_regime(self) -> Any | None:
        """Return the current regime, or ``None`` if missing or expired."""
        return await self._get_simple(key=self._regime_key(), op="get_regime")

    # ----- current Trader_Stability_Score (R19.4, R16) ---------------------

    async def set_stability_score(self, score: Any) -> None:
        """Replace the current Trader_Stability_Score cache entry.

        Cache invalidation on write: subsequent ``get_stability_score``
        callers see the new value immediately. Bounded by
        ``stability_ttl_s`` so a stalled producer does not surface a
        stale score forever.

        Args:
            score: A :class:`PsychStability` model, a bare float, or
                any JSON-compatible payload.
        """
        key = self._stability_key()
        payload = encode_payload(score, op="set_stability_score", key=key)
        client = self._require_client(op="set_stability_score")
        try:
            await client.set(key, payload, ex=self._config.stability_ttl_s)
        except redis_exc.TimeoutError as exc:
            raise RedisCacheTimeoutError(
                f"timeout on set_stability_score for key={key!r}: {exc}",
                op="set_stability_score",
                key=key,
            ) from exc
        except redis_exc.ConnectionError as exc:
            raise RedisCacheConnectError(
                f"connection error on set_stability_score for key={key!r}: {exc}",
                op="set_stability_score",
                key=key,
            ) from exc
        except redis_exc.RedisError as exc:
            raise RedisCacheError(
                f"redis error on set_stability_score for key={key!r}: {exc}",
                op="set_stability_score",
                key=key,
            ) from exc

    async def get_stability_score(self) -> Any | None:
        """Return the current Trader_Stability_Score, or ``None``."""
        return await self._get_simple(
            key=self._stability_key(), op="get_stability_score"
        )

    # ----- helpers ---------------------------------------------------------

    async def _get_simple(self, *, key: str, op: str) -> Any | None:
        client = self._require_client(op=op)
        try:
            raw: bytes | None = await client.get(key)
        except redis_exc.TimeoutError as exc:
            raise RedisCacheTimeoutError(
                f"timeout on {op} for key={key!r}: {exc}",
                op=op,
                key=key,
            ) from exc
        except redis_exc.ConnectionError as exc:
            raise RedisCacheConnectError(
                f"connection error on {op} for key={key!r}: {exc}",
                op=op,
                key=key,
            ) from exc
        except redis_exc.RedisError as exc:
            raise RedisCacheError(
                f"redis error on {op} for key={key!r}: {exc}",
                op=op,
                key=key,
            ) from exc
        return decode_payload(raw, op=op, key=key)

    def _require_client(self, *, op: str) -> aioredis.Redis:
        if self._client is None or self._closed:
            raise RedisCacheConnectError(
                f"cache is not started (call await cache.start()); op={op!r}",
                op=op,
            )
        return self._client


__all__ = ["RedisHotCache"]
