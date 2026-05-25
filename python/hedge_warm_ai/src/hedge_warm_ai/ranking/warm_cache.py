"""``AiRank`` cache adaptors used by the AI_Trade_Ranking_Engine.

The Risk_Engine consumes the latest per-symbol ``AiRank`` through the
WarmCache last-known-value path (R5.13, R17.4, design § Latency Budget
Allocation, design § Components § Risk_Engine). The WarmCache itself is
a Rust crate that lands as task 44.x; until that ships, the ranking
engine writes the latest rank to a Redis cache key inside the existing
:mod:`hedge_memory_rag.redis_cache` namespace using
:class:`~hedge_memory_rag.redis_cache.RedisHotCache`.

This module is the seam between the engine and whichever backend is
live:

* :class:`AiRankCache` — the protocol every backend implements.
* :class:`RedisAiRankCache` — wraps the existing
  :class:`RedisHotCache` and writes through its
  underlying client. Heavy imports
  (:mod:`hedge_memory_rag`, :mod:`redis.asyncio`) are deferred to
  construction so the ranking subpackage can be imported in
  environments that have not installed the RAG package yet.
* :class:`InMemoryAiRankCache` — captures writes in memory for
  assertion in tests. Mirrors the
  :class:`hedge_warm_ai.ranking.publisher.InMemoryRankPublisher`
  pattern.

When the Rust WarmCache (task 44.x) lands, a thin
``WarmCacheAiRank`` wrapper will sit alongside the Redis adaptor and
the engine will be re-pointed at it without any change to the public
API surface — both adaptors implement the same :class:`AiRankCache`
protocol.

Key scheme:
    The interim Redis namespace for the rank cache is
    ``hedge.warm.rank.<symbol>`` (parameterised by symbol so the
    Risk_Engine can read the latest rank for a given symbol with a
    single ``GET``). The namespace is intentionally distinct from the
    Hot_Path Redis Streams keys (``hedge.hot.*``) and the Memory_RAG
    cache keys (``hedge:rag:cache:*``) so the three lanes stay
    separately observable.
"""

from __future__ import annotations

import json
from threading import RLock
from typing import TYPE_CHECKING, Any, Final, Mapping, Optional, Protocol

import structlog

from .errors import RankingCacheError
from .score import RankingFactors
from .state import AiRank

if TYPE_CHECKING:  # pragma: no cover - typing only
    from hedge_memory_rag.redis_cache import RedisHotCache

_LOG: Final = structlog.get_logger(__name__)

#: Default key namespace for the rank cache. Distinct from the
#: Memory_RAG cache namespace so the WarmCache lane stays separately
#: observable. The full key is ``<namespace>.<symbol>``.
DEFAULT_RANK_CACHE_NAMESPACE: Final[str] = "hedge.warm.rank"

#: Default TTL (seconds) for rank entries. Generous enough to cover a
#: stalled ranking engine's restart window; bounded so the Risk_Engine
#: cannot read indefinitely-stale ranks.
DEFAULT_RANK_CACHE_TTL_S: Final[int] = 300


# ---------------------------------------------------------------------------
# Protocol ------------------------------------------------------------------
# ---------------------------------------------------------------------------


class AiRankCache(Protocol):
    """Sink for the latest per-symbol :class:`AiRank`.

    Implementations MUST:

    * Be async-safe — multiple coroutines may race writes.
    * Persist a *cache-invalidation-on-write* semantic: a subsequent
      :meth:`get_rank` returns the most recent :meth:`set_rank`
      (within the staleness window the backend chooses).
    * Translate wire-level failures to :class:`RankingCacheError` so
      the engine can surface the degraded state to the supervisor.
    """

    async def set_rank(self, rank: AiRank) -> None: ...
    async def get_rank(self, symbol: str) -> Optional[AiRank]: ...


# ---------------------------------------------------------------------------
# In-memory adaptor ---------------------------------------------------------
# ---------------------------------------------------------------------------


class InMemoryAiRankCache:
    """In-memory rank cache for tests.

    Captures every :meth:`set_rank` call so assertions can confirm
    the engine writes the right rank at the right edge.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._per_symbol: dict[str, AiRank] = {}
        self._writes: list[AiRank] = []

    async def set_rank(self, rank: AiRank) -> None:
        with self._lock:
            self._per_symbol[rank.symbol] = rank
            self._writes.append(rank)

    async def get_rank(self, symbol: str) -> Optional[AiRank]:
        with self._lock:
            return self._per_symbol.get(symbol)

    @property
    def writes(self) -> list[AiRank]:
        with self._lock:
            return list(self._writes)

    def reset(self) -> None:
        with self._lock:
            self._per_symbol.clear()
            self._writes.clear()


# ---------------------------------------------------------------------------
# Redis adaptor (interim until the WarmCache crate / task 44.x lands) ------
# ---------------------------------------------------------------------------


def _normalise_symbol(symbol: str) -> str:
    """Validate a symbol string before composing the rank-cache key."""
    if not isinstance(symbol, str):
        raise TypeError(f"symbol must be str, got {type(symbol).__name__}")
    if not symbol:
        raise ValueError("symbol must be non-empty")
    if "." in symbol:
        # We use ``.`` as the namespace/symbol separator and the
        # WarmCache crate (task 44.x) does the same; reject ``.`` to
        # keep the key scheme unambiguous.
        raise ValueError(f"symbol must not contain '.' separator: {symbol!r}")
    return symbol


def _encode_ai_rank(rank: AiRank) -> bytes:
    """Encode an :class:`AiRank` as the cache wire payload.

    The shape mirrors the canonical ``ai_rank.schema.json`` plus the
    symbol so the Risk_Engine can confirm it is reading the right
    symbol's rank from the cache.
    """
    payload: dict[str, Any] = {
        "correlation_id": rank.correlation_id.hex(),
        "signal_id": rank.signal_id,
        "trade_confidence_score": float(rank.trade_confidence_score),
        "factors": {
            "orderflow": float(rank.factors.orderflow),
            "technical_strength": float(rank.factors.technical_strength),
            "news_sentiment": float(rank.factors.news_sentiment),
            "market_regime": float(rank.factors.market_regime),
            "trader_discipline": float(rank.factors.trader_discipline),
        },
        "symbol": rank.symbol,
        "shadow": bool(rank.shadow),
        "ts_ns": int(rank.ts_ns),
    }
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def _decode_ai_rank(raw: Any) -> Optional[AiRank]:
    """Decode an :class:`AiRank` from the cache wire payload, or ``None``."""
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        try:
            payload = json.loads(bytes(raw).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
    elif isinstance(raw, str):
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return None
    elif isinstance(raw, Mapping):
        payload = raw
    else:
        return None

    if not isinstance(payload, Mapping):
        return None

    try:
        cid_hex = payload["correlation_id"]
        if not isinstance(cid_hex, str):
            return None
        correlation_id = bytes.fromhex(cid_hex)
        factors_dict = payload.get("factors", {})
        factors = RankingFactors(
            orderflow=float(factors_dict.get("orderflow", 0.0)),
            technical_strength=float(factors_dict.get("technical_strength", 0.0)),
            news_sentiment=float(factors_dict.get("news_sentiment", 0.0)),
            market_regime=float(factors_dict.get("market_regime", 0.0)),
            trader_discipline=float(factors_dict.get("trader_discipline", 0.0)),
        )
        return AiRank(
            correlation_id=correlation_id,
            signal_id=str(payload["signal_id"]),
            trade_confidence_score=float(payload["trade_confidence_score"]),
            factors=factors,
            symbol=str(payload.get("symbol", "")),
            shadow=bool(payload.get("shadow", False)),
            ts_ns=int(payload.get("ts_ns", 0)),
        )
    except (KeyError, TypeError, ValueError):
        return None


class RedisAiRankCache:
    """``AiRank`` cache backed by :class:`RedisHotCache`'s underlying client.

    Stores one key per symbol under the configured namespace
    (``hedge.warm.rank.<symbol>`` by default). The TTL is enforced via
    Redis ``SET ... EX`` so a stalled ranking engine cannot leak stale
    ranks into the Risk_Engine forever.

    Note (task 44.x replacement plan):
        When the Rust ``hedge-warmcache`` crate lands, the engine will
        construct a new ``WarmCacheAiRank`` adaptor (writing directly
        to the WarmCache atomic slot) and pass it where
        :class:`RedisAiRankCache` is passed today. Both adaptors
        implement :class:`AiRankCache`, so no engine code changes —
        only the constructor call.
    """

    def __init__(
        self,
        hot_cache: "RedisHotCache",
        *,
        namespace: str = DEFAULT_RANK_CACHE_NAMESPACE,
        ttl_s: int = DEFAULT_RANK_CACHE_TTL_S,
    ) -> None:
        if not namespace:
            raise ValueError("namespace must be a non-empty string")
        if ttl_s <= 0:
            raise ValueError(f"ttl_s must be positive, got {ttl_s!r}")
        self._hot = hot_cache
        self._namespace = namespace
        self._ttl_s = ttl_s

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def ttl_s(self) -> int:
        return self._ttl_s

    def _key(self, symbol: str) -> str:
        return f"{self._namespace}.{_normalise_symbol(symbol)}"

    def _client(self) -> Any:
        client = getattr(self._hot, "_client", None)
        closed = getattr(self._hot, "_closed", False)
        if client is None or closed:
            raise RankingCacheError(
                "RedisHotCache is not started; cannot read/write rank cache"
            )
        return client

    async def set_rank(self, rank: AiRank) -> None:
        if not rank.symbol:
            # Portfolio-scoped ranks have no symbol to key on; the
            # Risk_Engine reads the rank cache by symbol so writing
            # one without a key would be useless. Skip silently —
            # the engine still publishes ``ai.rank.<cid>`` for the
            # journal.
            return
        client = self._client()
        key = self._key(rank.symbol)
        payload = _encode_ai_rank(rank)
        try:
            await client.set(key, payload, ex=self._ttl_s)
        except Exception as exc:
            _LOG.warning(
                "ai_rank_cache_set_failed",
                key=key,
                symbol=rank.symbol,
                trade_confidence_score=rank.trade_confidence_score,
                error=str(exc),
            )
            raise RankingCacheError(
                f"failed to write AiRank to RedisHotCache: {exc}"
            ) from exc

    async def get_rank(self, symbol: str) -> Optional[AiRank]:
        client = self._client()
        key = self._key(symbol)
        try:
            raw = await client.get(key)
        except Exception as exc:
            raise RankingCacheError(
                f"failed to read AiRank from RedisHotCache: {exc}"
            ) from exc
        return _decode_ai_rank(raw)


__all__ = [
    "AiRankCache",
    "DEFAULT_RANK_CACHE_NAMESPACE",
    "DEFAULT_RANK_CACHE_TTL_S",
    "InMemoryAiRankCache",
    "RedisAiRankCache",
]
