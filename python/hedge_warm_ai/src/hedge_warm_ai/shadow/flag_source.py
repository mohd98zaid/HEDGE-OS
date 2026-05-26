"""Shadow-flag source adaptors for AI_Shadow_Mode (R23.1, R24.3).

The AI_Governance_Engine writes per-component shadow flags into the
interim WarmCache surface at ``hedge.warm.shadow.<component>``
(task 28.1). The shadow service is the consumer: on every poll cycle
it refreshes its in-memory snapshot of which components are
currently shadowed.

This module is the seam between the service and whichever backend is
live:

* :class:`ShadowFlagSource` — protocol every backend implements.
* :class:`InMemoryShadowFlagSource` — captures writes in memory for
  assertion in tests; mirrors the
  :class:`hedge_warm_ai.governance.warm_cache.InMemoryGovernanceWarmCache`
  pattern.
* :class:`RedisShadowFlagSource` — wraps an
  ``async`` Redis client (``redis.asyncio.Redis``) and reads from
  ``hedge.warm.shadow.<component>`` for every governed component.

The Redis client is intentionally a parameter rather than fetched
from a global singleton: tests pass an in-memory fake; the
``hedge-shadow`` service-layer entry point passes the same
``aioredis.Redis`` instance the rest of the Warm_AI_Pipeline uses
(constructed via :func:`hedge_memory_rag.redis_cache.config.load_redis_cache_config`).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import RLock
from typing import TYPE_CHECKING, Any, Final, Iterable, Optional, Protocol

import structlog

from ..governance.state import DEFAULT_COMPONENTS, GovernedComponent
from .errors import ShadowFlagSourceError
from .subjects import SHADOW_FLAG_NAMESPACE, shadow_flag_key

if TYPE_CHECKING:  # pragma: no cover - typing only
    from redis import asyncio as aioredis

_LOG: Final = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Protocol -----------------------------------------------------------------
# ---------------------------------------------------------------------------


class ShadowFlagSource(Protocol):
    """Source of "is component X currently shadowed?" booleans.

    Implementations MUST:

    * Be async-safe — multiple poll coroutines may race reads.
    * Translate wire-level failures to
      :class:`ShadowFlagSourceError` so the service can surface the
      degraded state to the supervisor and fall back to its
      in-memory snapshot until the source recovers.
    """

    async def is_shadowed(self, component: str) -> bool: ...

    async def fetch_all(
        self, components: Iterable[str]
    ) -> frozenset[str]: ...


# ---------------------------------------------------------------------------
# In-memory adaptor --------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass
class InMemoryShadowFlagSource:
    """In-memory shadow-flag source for tests.

    Provides set/clear/is_shadowed methods so a unit test can drive
    the service through state transitions without standing up Redis.
    """

    _lock: RLock = field(default_factory=RLock, init=False)
    _shadow: set[str] = field(default_factory=set, init=False)

    async def is_shadowed(self, component: str) -> bool:
        with self._lock:
            return component in self._shadow

    async def fetch_all(
        self, components: Iterable[str]
    ) -> frozenset[str]:
        with self._lock:
            requested = list(components)
            return frozenset(c for c in requested if c in self._shadow)

    # ----- test helpers ---------------------------------------------------

    def set(self, component: str) -> None:
        with self._lock:
            self._shadow.add(component)

    def clear(self, component: str) -> None:
        with self._lock:
            self._shadow.discard(component)

    def reset(self) -> None:
        with self._lock:
            self._shadow.clear()


# ---------------------------------------------------------------------------
# Redis adaptor ------------------------------------------------------------
# ---------------------------------------------------------------------------


def _default_components_iter() -> tuple[str, ...]:
    return tuple(c.value for c in DEFAULT_COMPONENTS)


@dataclass
class RedisShadowFlagSource:
    """``ShadowFlagSource`` backed by ``redis.asyncio``.

    Reads ``hedge.warm.shadow.<component>`` for every governed
    component on each refresh cycle. The keys are written by the
    AI_Governance_Engine (task 28.1) with a TTL; absence of the key
    means the component is not shadowed.

    Wire-level Redis failures raise :class:`ShadowFlagSourceError`.

    Attributes:
        client: An ``async`` Redis client instance. The shadow
            service does not own the client lifecycle — the caller
            (typically the ``hedge-shadow`` service-layer binary)
            constructs and disposes it.
        namespace: Redis namespace prefix. Default
            :data:`SHADOW_FLAG_NAMESPACE`. Mutable so a test can
            point the source at a sandboxed namespace.
        components: Iterable of canonical component names polled on
            every :meth:`fetch_all` (when the caller does not pass
            its own component list). Defaults to the seven values of
            :data:`hedge_warm_ai.governance.state.DEFAULT_COMPONENTS`.
    """

    client: "aioredis.Redis"
    namespace: str = SHADOW_FLAG_NAMESPACE
    components: tuple[str, ...] = field(default_factory=_default_components_iter)

    async def is_shadowed(self, component: str) -> bool:
        key = shadow_flag_key(component, namespace=self.namespace)
        try:
            raw = await self.client.get(key)
        except Exception as exc:
            _LOG.warning(
                "shadow_flag_source_get_failed",
                key=key,
                component=component,
                error=str(exc),
            )
            raise ShadowFlagSourceError(
                f"failed to read shadow flag for {component!r}: {exc}"
            ) from exc
        return raw is not None

    async def fetch_all(
        self, components: Iterable[str] | None = None
    ) -> frozenset[str]:
        targets = tuple(components) if components is not None else self.components
        if not targets:
            return frozenset()
        keys = [shadow_flag_key(c, namespace=self.namespace) for c in targets]
        try:
            # MGET returns one value per requested key; ``None``
            # indicates the key is absent (component is not
            # shadowed). One round trip is cheaper than N gets and
            # bounds Redis load even when the governance engine
            # toggles many components at once.
            raw_values: list[Any] = await self.client.mget(keys)
        except Exception as exc:
            _LOG.warning(
                "shadow_flag_source_mget_failed",
                key_count=len(keys),
                error=str(exc),
            )
            raise ShadowFlagSourceError(
                f"failed to mget shadow flags: {exc}"
            ) from exc
        return frozenset(
            comp
            for comp, raw in zip(targets, raw_values)
            if raw is not None
        )

    @classmethod
    def for_governed_components(
        cls,
        client: "aioredis.Redis",
        *,
        namespace: str = SHADOW_FLAG_NAMESPACE,
    ) -> "RedisShadowFlagSource":
        """Construct a source polling every :class:`GovernedComponent`."""
        return cls(
            client=client,
            namespace=namespace,
            components=tuple(c.value for c in DEFAULT_COMPONENTS),
        )

    @staticmethod
    def derive_components(extra: Iterable[str] = ()) -> tuple[str, ...]:
        """Return the union of the canonical components and ``extra``.

        Useful when a deployment governs additional non-canonical
        components and wants the shadow service to track them too.
        Order: canonical components first (in
        :data:`DEFAULT_COMPONENTS` order), then the unique entries
        of ``extra``.
        """
        canon = tuple(c.value for c in DEFAULT_COMPONENTS)
        seen: set[str] = set(canon)
        result = list(canon)
        for raw in extra:
            if raw and raw not in seen:
                result.append(raw)
                seen.add(raw)
        return tuple(result)

    @staticmethod
    def is_known_component(name: str) -> bool:
        """Whether ``name`` is one of the canonical governed components."""
        try:
            GovernedComponent(name)
        except ValueError:
            return False
        return True


__all__ = [
    "InMemoryShadowFlagSource",
    "RedisShadowFlagSource",
    "ShadowFlagSource",
]


# Silence `Optional` re-export check — kept for backwards compat with the
# governance subpackage's protocol shape.
_ = Optional
