"""Edge-triggered publisher for ``ai.priority.changed.<symbol>`` (R14.3).

The :class:`SymbolPriorityEngine` is responsible for *deciding* when a
priority change happened. The actual NATS write is decoupled behind a
small :class:`PriorityChangedPublisher` protocol so:

1. Tests can substitute :class:`InMemoryPriorityChangedPublisher` and
   assert on captured events without spinning up NATS.
2. Production wires :class:`NatsPriorityChangedPublisher` around the
   same ``async def publish(subject, payload)`` callable that
   :class:`hedge_warm_ai.ollama_client.NatsDegradedPublisher` and
   :class:`hedge_warm_ai.onnx_runtime.NatsAiLatencyEmitter` already
   use.

The canonical NATS subject prefix is ``ai.priority.changed`` and the
full subject for a symbol is ``ai.priority.changed.<symbol>``,
matching ``hedge-bus::subject::AI_PRIORITY_CHANGED``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from threading import RLock
from typing import Awaitable, Callable, Final, Protocol

import structlog

from ..schemas import PriorityChanged

_LOG: Final = structlog.get_logger(__name__)

#: Canonical NATS subject prefix. Symbols are appended as
#: ``ai.priority.changed.<symbol>``. Matches the Rust constant
#: ``hedge_bus::subject::AI_PRIORITY_CHANGED``.
AI_PRIORITY_CHANGED_PREFIX: Final[str] = "ai.priority.changed"


def priority_subject(symbol: str) -> str:
    """Build the per-symbol ``ai.priority.changed.<sym>`` subject.

    Args:
        symbol: Symbol identifier; must be non-empty and free of
            ``.`` separators (NATS uses ``.`` as a topic delimiter).

    Raises:
        ValueError: ``symbol`` is empty or contains ``.``.
    """
    if not symbol:
        raise ValueError("symbol must be non-empty")
    if "." in symbol:
        raise ValueError(f"symbol must not contain '.' separator: {symbol!r}")
    return f"{AI_PRIORITY_CHANGED_PREFIX}.{symbol}"


# ---------------------------------------------------------------------------
# Protocol ------------------------------------------------------------------
# ---------------------------------------------------------------------------


class PriorityChangedPublisher(Protocol):
    """Sink for :class:`PriorityChanged` payloads.

    Implementations route the payload to the per-symbol
    ``ai.priority.changed.<sym>`` subject; the symbol is taken from
    ``event.symbol`` so callers do not need to thread it separately.
    """

    async def publish_changed(self, event: PriorityChanged) -> None: ...


# ---------------------------------------------------------------------------
# Implementations -----------------------------------------------------------
# ---------------------------------------------------------------------------


class NoopPriorityChangedPublisher:
    """Discards every event. Useful when the bus is not yet wired."""

    async def publish_changed(self, event: PriorityChanged) -> None:  # noqa: D401
        return


class InMemoryPriorityChangedPublisher:
    """Captures events in memory for assertion in tests.

    Thread-safe: the underlying list is guarded by an :class:`RLock`.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._events: list[PriorityChanged] = []

    async def publish_changed(self, event: PriorityChanged) -> None:
        with self._lock:
            self._events.append(event)

    @property
    def events(self) -> list[PriorityChanged]:
        """Return a snapshot of the captured events (newest last)."""
        with self._lock:
            return list(self._events)

    def reset(self) -> None:
        """Clear the captured events (test helper)."""
        with self._lock:
            self._events.clear()


@dataclass
class NatsPriorityChangedPublisher:
    """NATS-backed publisher.

    Takes an ``async def publish(subject, payload)`` callable that
    performs the network I/O. The wrapper:

    * Serialises the :class:`PriorityChanged` model to compact JSON
      (the model carries ``additionalProperties: false`` and the
      ``from``/``to`` enums, so the serialized form is byte-for-byte
      compatible with the Rust ``hedge_schemas`` mirror).
    * Publishes on ``ai.priority.changed.<event.symbol>``.
    * Logs and swallows publish failures at ``warning`` level so a
      degraded broker cannot turn a single tier change into a
      cascading failure.
    """

    async_publish: Callable[[str, bytes], Awaitable[None]]

    async def publish_changed(self, event: PriorityChanged) -> None:
        subject = priority_subject(event.symbol)
        payload = json.dumps(
            event.model_dump(mode="json", by_alias=True), separators=(",", ":")
        ).encode("utf-8")
        try:
            await self.async_publish(subject, payload)
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "priority_changed_publish_failed",
                subject=subject,
                symbol=event.symbol,
                **{"from": event.from_, "to": event.to},
                error=str(exc),
            )


__all__ = [
    "AI_PRIORITY_CHANGED_PREFIX",
    "InMemoryPriorityChangedPublisher",
    "NatsPriorityChangedPublisher",
    "NoopPriorityChangedPublisher",
    "PriorityChangedPublisher",
    "priority_subject",
]
