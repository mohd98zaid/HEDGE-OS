"""Exception hierarchy for :mod:`hedge_memory_rag.redis_cache`.

Why a dedicated hierarchy rather than re-exporting :mod:`redis.exceptions`?
The Self_Healing_Supervisor (F2 / R25.2 — task 41.1) subscribes to typed
errors so it can decide whether to fire a ``cache.redis.degraded`` event
and trigger reconnection. Surfacing a narrow, stable set of types
decouples the supervisor from the wire-level :mod:`redis-py` exception
classes, which evolve across minor releases.

This module **does not** publish degraded events itself — that is the
supervisor's job (task 33.2 / 41.1). It only converts wire-level
failures into typed exceptions the supervisor can match on.

Class layout::

    RedisCacheError                <- base, never raised directly
    ├── RedisCacheConnectError     <- TCP refused, DNS, reset, auth, BusyLoading
    ├── RedisCacheTimeoutError     <- socket timeout exceeded
    └── RedisCacheCodecError       <- JSON encode/decode failure (corrupt payload)
"""

from __future__ import annotations


class RedisCacheError(Exception):
    """Base class for every :mod:`hedge_memory_rag.redis_cache` failure.

    Holds the cache *operation* and *key* so structured log scrapers can
    filter without parsing the message.
    """

    def __init__(self, message: str, *, op: str, key: str | None = None) -> None:
        super().__init__(message)
        self.op = op
        self.key = key


class RedisCacheConnectError(RedisCacheError):
    """Raised when the Redis daemon refuses, resets, or rejects auth.

    The Self_Healing_Supervisor maps this to ``cache.redis.degraded``
    (R25.2) and triggers a reconnect.
    """


class RedisCacheTimeoutError(RedisCacheError):
    """Raised when a Redis operation exceeds the configured socket timeout."""


class RedisCacheCodecError(RedisCacheError):
    """Raised when a payload fails to encode or decode.

    A decode failure indicates a corrupt or wrong-shape value at rest;
    we surface it rather than silently returning ``None`` so callers can
    distinguish a missing key from an unreadable one.
    """


__all__ = [
    "RedisCacheError",
    "RedisCacheConnectError",
    "RedisCacheTimeoutError",
    "RedisCacheCodecError",
]
