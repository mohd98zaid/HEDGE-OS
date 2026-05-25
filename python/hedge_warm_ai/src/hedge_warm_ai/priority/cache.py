"""Redis-backed bridge for current tier and allocation (R14.4).

Until the dedicated ``hedge-warmcache`` crate (task 44.x) lands, the
Hot_Path Rust components read the current priority tier and its
:class:`PriorityAllocation` from Redis. We piggy-back on
``hedge_memory_rag.redis_cache`` so we re-use the same connection
config (``HEDGE_REDIS_URL``), the same JSON codec, and the same typed
exception hierarchy that the Self_Healing_Supervisor already
recognises.

Keying scheme
-------------

Keys live under a ``hedge:warm:priority`` namespace (disjoint from the
Memory_RAG_Layer's ``hedge:rag:cache`` namespace) so the WarmCache
crate can take over the namespace cleanly when it lands::

    hedge:warm:priority:tier:<symbol>         -> "P1" | "P2" | "P3" | "P4"
    hedge:warm:priority:allocation:<symbol>   -> {"cpu_budget": .., ...}

Both keys are written **together** by :meth:`PriorityWarmCache.put`
inside a Redis ``MULTI/EXEC`` pipeline, so a Hot_Path consumer that
reads ``allocation`` after observing ``tier`` cannot see a
mismatched pair.

Migration to the dedicated WarmCache (task 44.x)
------------------------------------------------

When ``hedge-warmcache`` ships, the engine will write through that
crate's ``priority(symbol)`` slot instead of Redis directly. The
keying scheme above is named ``hedge:warm:priority:*`` (not
``hedge:rag:cache:priority:*``) so the WarmCache can adopt the same
namespace verbatim and Hot_Path readers do not need a key migration.
The exact contract — atomic ``(tier, allocation)`` pair updates
visible to all subscribers — is what task 44.1 requires (R14.4
WarmCache lookup), so this stop-gap is API-compatible.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Final, Optional

import structlog
from redis import asyncio as aioredis
from redis import exceptions as redis_exc

from hedge_memory_rag.redis_cache.config import (
    DEFAULT_CONNECT_TIMEOUT_S,
    DEFAULT_SOCKET_TIMEOUT_S,
    ENV_REDIS_URL,
    load_redis_cache_config,
)
from hedge_memory_rag.redis_cache.errors import (
    RedisCacheConnectError,
    RedisCacheError,
    RedisCacheTimeoutError,
)

from ..schemas.ai_priority_changed import PriorityTier
from .allocation import PriorityAllocation

_LOG: Final = structlog.get_logger(__name__)

#: Namespace prefix. Disjoint from ``hedge:rag:cache`` so the dedicated
#: WarmCache crate can take this over without disturbing the
#: Memory_RAG_Layer cache. See module docstring for the migration
#: rationale.
DEFAULT_WARM_PRIORITY_NAMESPACE: Final[str] = "hedge:warm:priority"


def _normalise_symbol(symbol: str) -> str:
    if not isinstance(symbol, str):
        raise TypeError(f"symbol must be str, got {type(symbol).__name__}")
    if not symbol:
        raise ValueError("symbol must be non-empty")
    if ":" in symbol:
        raise ValueError(f"symbol must not contain ':' separator: {symbol!r}")
    return symbol


@dataclass
class PriorityWarmCache:
    """Async Redis-backed snapshot of the current tier and allocation.

    Construct with :meth:`from_env` for production deployments
    (``HEDGE_REDIS_URL`` resolution) or pass an explicit Redis client
    for tests::

        cache = PriorityWarmCache.from_client(client)
        await cache.put("RELIANCE", "P1", DEFAULT_ALLOCATION_TABLE["P1"])
        tier = await cache.get_tier("RELIANCE")  # "P1"

    All public methods are async. The class is async-safe — multiple
    coroutines may share one instance against the shared
    ``redis.asyncio.Redis`` connection pool.
    """

    client: aioredis.Redis
    namespace: str = DEFAULT_WARM_PRIORITY_NAMESPACE
    _owns_client: bool = False

    # ----- factories -------------------------------------------------------

    @classmethod
    def from_client(
        cls,
        client: aioredis.Redis,
        *,
        namespace: str = DEFAULT_WARM_PRIORITY_NAMESPACE,
    ) -> "PriorityWarmCache":
        """Build a cache around an already-started ``redis.asyncio`` client.

        Use this in tests where the lifecycle of the client is owned
        by the test harness.
        """
        return cls(client=client, namespace=namespace, _owns_client=False)

    @classmethod
    async def from_env(
        cls,
        *,
        namespace: str = DEFAULT_WARM_PRIORITY_NAMESPACE,
    ) -> "PriorityWarmCache":
        """Build a cache from ``HEDGE_REDIS_URL`` and start the client.

        The Redis URL is resolved via
        :func:`hedge_memory_rag.redis_cache.config.load_redis_cache_config`
        so the cache reads the same env var (:data:`ENV_REDIS_URL`)
        as the Memory_RAG_Layer cache and the Hot_Path Rust crates.
        """
        config = load_redis_cache_config()
        try:
            client = aioredis.Redis.from_url(
                config.redis_url,
                socket_timeout=DEFAULT_SOCKET_TIMEOUT_S,
                socket_connect_timeout=DEFAULT_CONNECT_TIMEOUT_S,
                decode_responses=False,
                health_check_interval=30,
            )
        except (ValueError, redis_exc.RedisError) as exc:
            raise RedisCacheConnectError(
                f"failed to construct Redis client for {config.redis_url!r}: {exc}",
                op="warm_priority_start",
            ) from exc
        return cls(client=client, namespace=namespace, _owns_client=True)

    async def aclose(self) -> None:
        """Close the underlying client (if owned). Idempotent."""
        if self._owns_client:
            try:
                await self.client.aclose()
            except redis_exc.RedisError as exc:  # pragma: no cover - logged
                _LOG.warning("warm_priority_close_failed", error=str(exc))
            self._owns_client = False

    # ----- key composition -------------------------------------------------

    def _tier_key(self, symbol: str) -> str:
        return f"{self.namespace}:tier:{_normalise_symbol(symbol)}"

    def _allocation_key(self, symbol: str) -> str:
        return f"{self.namespace}:allocation:{_normalise_symbol(symbol)}"

    # ----- writes ----------------------------------------------------------

    async def put(
        self,
        symbol: str,
        tier: PriorityTier,
        allocation: PriorityAllocation,
    ) -> None:
        """Atomically replace the cached tier and allocation for ``symbol``.

        The two keys are written inside a single ``MULTI/EXEC``
        pipeline so a concurrent reader cannot observe a stale tier
        with a fresh allocation or vice versa.

        Raises:
            RedisCacheConnectError, RedisCacheTimeoutError,
            RedisCacheError: wire-level Redis failures, mapped to
                :mod:`hedge_memory_rag.redis_cache.errors` so the
                Self_Healing_Supervisor recognises them.
        """
        tier_key = self._tier_key(symbol)
        alloc_key = self._allocation_key(symbol)
        tier_bytes = tier.encode("utf-8")
        alloc_bytes = json.dumps(allocation.as_dict(), separators=(",", ":")).encode(
            "utf-8"
        )
        try:
            async with self.client.pipeline(transaction=True) as pipe:
                pipe.set(tier_key, tier_bytes)
                pipe.set(alloc_key, alloc_bytes)
                await pipe.execute()
        except redis_exc.TimeoutError as exc:
            raise RedisCacheTimeoutError(
                f"timeout on warm-priority put for symbol={symbol!r}: {exc}",
                op="warm_priority_put",
                key=tier_key,
            ) from exc
        except redis_exc.ConnectionError as exc:
            raise RedisCacheConnectError(
                f"connection error on warm-priority put for symbol={symbol!r}: {exc}",
                op="warm_priority_put",
                key=tier_key,
            ) from exc
        except redis_exc.RedisError as exc:
            raise RedisCacheError(
                f"redis error on warm-priority put for symbol={symbol!r}: {exc}",
                op="warm_priority_put",
                key=tier_key,
            ) from exc

    # ----- reads -----------------------------------------------------------

    async def get_tier(self, symbol: str) -> Optional[PriorityTier]:
        """Return the cached tier for ``symbol`` or ``None`` if absent.

        The decoded value is validated against the canonical
        ``P1 | P2 | P3 | P4`` set; an unexpected value indicates a
        scheme drift between writer and reader and raises
        :class:`RedisCacheError`.
        """
        key = self._tier_key(symbol)
        raw = await self._safe_get(key=key, op="warm_priority_get_tier")
        if raw is None:
            return None
        decoded = raw.decode("utf-8")
        if decoded not in ("P1", "P2", "P3", "P4"):
            raise RedisCacheError(
                f"unexpected tier value {decoded!r} at key={key!r}",
                op="warm_priority_get_tier",
                key=key,
            )
        # mypy: narrow to PriorityTier literal
        return decoded  # type: ignore[return-value]

    async def get_allocation(
        self, symbol: str
    ) -> Optional[PriorityAllocation]:
        """Return the cached allocation row for ``symbol`` or ``None``."""
        key = self._allocation_key(symbol)
        raw = await self._safe_get(key=key, op="warm_priority_get_allocation")
        if raw is None:
            return None
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RedisCacheError(
                f"undecodable allocation payload at key={key!r}: {exc}",
                op="warm_priority_get_allocation",
                key=key,
            ) from exc
        try:
            return PriorityAllocation(**payload)
        except (TypeError, ValueError) as exc:
            raise RedisCacheError(
                f"invalid allocation payload at key={key!r}: {exc}",
                op="warm_priority_get_allocation",
                key=key,
            ) from exc

    async def _safe_get(self, *, key: str, op: str) -> bytes | None:
        try:
            return await self.client.get(key)
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


__all__ = [
    "DEFAULT_WARM_PRIORITY_NAMESPACE",
    "ENV_REDIS_URL",
    "PriorityWarmCache",
]
