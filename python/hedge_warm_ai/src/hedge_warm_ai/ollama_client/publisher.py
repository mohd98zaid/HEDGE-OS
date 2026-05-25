"""Degraded-event publisher abstractions for :mod:`hedge_warm_ai.ollama_client`.

The Ollama client emits an ``ai.ollama.degraded`` event on NATS each
time a request is rerouted from a primary model to its configured
fallback (R10.9). The actual NATS publish is decoupled behind a small
:class:`DegradedEventPublisher` protocol so:

1. Tests can substitute :class:`InMemoryDegradedPublisher` and assert on
   captured events without spinning up NATS.
2. Production wires :class:`NatsDegradedPublisher` around the same
   ``async def publish(subject, payload)`` callable that the rest of
   the Warm_AI_Pipeline already uses (mirrors the
   :class:`hedge_warm_ai.onnx_runtime.NatsAiLatencyEmitter` pattern).

The canonical NATS subject is ``ai.ollama.degraded`` (no parameter), as
declared in ``hedge-bus::subject::AI_OLLAMA_DEGRADED``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from threading import RLock
from typing import Awaitable, Callable, Final, Protocol

import structlog

from ..schemas import OllamaDegraded

_LOG: Final = structlog.get_logger(__name__)

#: Canonical NATS subject for the degraded event. Matches the constant in
#: ``crates/hedge-bus/src/subject.rs``.
DEGRADED_SUBJECT: Final[str] = "ai.ollama.degraded"


# ---------------------------------------------------------------------------
# Protocol ------------------------------------------------------------------
# ---------------------------------------------------------------------------


class DegradedEventPublisher(Protocol):
    """Sink for :class:`OllamaDegraded` payloads.

    The async signature lets implementations route the event over the
    network without requiring a synchronous fire-and-forget shim, while
    still allowing in-memory implementations to no-op.
    """

    async def publish_degraded(self, event: OllamaDegraded) -> None: ...


# ---------------------------------------------------------------------------
# Implementations -----------------------------------------------------------
# ---------------------------------------------------------------------------


class NoopDegradedPublisher:
    """Discards every event. Useful when the bus is not yet wired."""

    async def publish_degraded(self, event: OllamaDegraded) -> None:  # noqa: D401
        return


class InMemoryDegradedPublisher:
    """Captures published events in memory for assertion in tests.

    Thread-safe: the underlying list is guarded by an :class:`RLock`.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._events: list[OllamaDegraded] = []

    async def publish_degraded(self, event: OllamaDegraded) -> None:
        with self._lock:
            self._events.append(event)

    @property
    def events(self) -> list[OllamaDegraded]:
        """Return a snapshot of the captured events."""
        with self._lock:
            return list(self._events)

    def reset(self) -> None:
        """Clear the captured events (test helper)."""
        with self._lock:
            self._events.clear()


@dataclass
class NatsDegradedPublisher:
    """NATS-backed publisher.

    Takes an ``async def publish(subject, payload)`` callable that
    performs the network I/O. The wrapper:

    * Validates the payload against the canonical JSON Schema by
      round-tripping through :class:`OllamaDegraded` (the model carries
      ``additionalProperties: false`` and bound enums).
    * Encodes the payload as compact JSON.
    * Publishes on ``ai.ollama.degraded``.

    The callable is invoked with ``await``; failures are logged at
    ``warning`` level and otherwise swallowed so a degraded broker
    cannot turn a single failed inference into a cascading failure.
    """

    async_publish: Callable[[str, bytes], Awaitable[None]]
    subject: str = DEGRADED_SUBJECT

    async def publish_degraded(self, event: OllamaDegraded) -> None:
        # `model_dump(mode="json")` produces a JSON-compatible dict that
        # respects the schema's enum and integer-bound constraints.
        payload = json.dumps(event.model_dump(mode="json"), separators=(",", ":")).encode(
            "utf-8"
        )
        try:
            await self.async_publish(self.subject, payload)
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "ollama_degraded_publish_failed",
                subject=self.subject,
                model=event.model,
                fallback=event.fallback_model,
                reason=event.reason,
                error=str(exc),
            )


__all__ = [
    "DegradedEventPublisher",
    "DEGRADED_SUBJECT",
    "InMemoryDegradedPublisher",
    "NatsDegradedPublisher",
    "NoopDegradedPublisher",
]
