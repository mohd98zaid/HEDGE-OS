"""Edge-triggered ``ai.regime.changed`` publishers.

The Market_Regime_Engine emits one :class:`RegimeChanged` payload on
the canonical NATS subject (``ai.regime.changed`` per
:data:`hedge_warm_ai.regime.config.DEFAULT_REGIME_SUBJECT`) **only on
transitions** between adjacent classified regimes (R13.3, Property 8).
The classifier and edge-trigger logic live in :mod:`.engine`; this
module is the network sink behind a small protocol so:

1. Tests can substitute :class:`InMemoryRegimePublisher` and assert on
   captured events without spinning up NATS (matches the pattern used
   by :class:`hedge_warm_ai.ollama_client.publisher.InMemoryDegradedPublisher`).
2. Production wires :class:`NatsRegimePublisher` around the same
   ``async def publish(subject, payload)`` callable that the rest of
   the Warm_AI_Pipeline uses.

The canonical wire format is the byte-for-byte
``ai_regime_changed.schema.json`` payload, mirrored as
:class:`hedge_warm_ai.schemas.RegimeChanged` (R12, task 4.1). Bypassing
that schema is forbidden — the publisher takes a typed model so a
caller cannot accidentally publish a malformed payload.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from threading import RLock
from typing import Awaitable, Callable, Final, Protocol

import structlog

from ..schemas import RegimeChanged
from .config import DEFAULT_REGIME_SUBJECT
from .errors import RegimePublishError

_LOG: Final = structlog.get_logger(__name__)


class RegimePublisher(Protocol):
    """Sink for typed :class:`RegimeChanged` payloads.

    The async signature lets implementations route the event over the
    network without requiring a synchronous fire-and-forget shim, while
    in-memory implementations can no-op cheaply.

    Implementations MUST:

    * Serialise the model via ``model_dump(mode="json", by_alias=True)``
      so the ``from`` alias is honoured (Pydantic reserves the
      attribute name ``from_``).
    * Raise :class:`RegimePublishError` on any wire-level failure so
      the engine can decide whether to retry or surface the failure
      to the Self_Healing_Supervisor.
    """

    async def publish_regime_change(self, event: RegimeChanged) -> None: ...


# ---------------------------------------------------------------------------
# Implementations -----------------------------------------------------------
# ---------------------------------------------------------------------------


class NoopRegimePublisher:
    """Discards every event. Useful when the bus is not yet wired."""

    async def publish_regime_change(self, event: RegimeChanged) -> None:  # noqa: D401
        return


class InMemoryRegimePublisher:
    """Captures published events in memory for assertion in tests.

    Thread-safe: the underlying list is guarded by an :class:`RLock`.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._events: list[RegimeChanged] = []

    async def publish_regime_change(self, event: RegimeChanged) -> None:
        with self._lock:
            self._events.append(event)

    @property
    def events(self) -> list[RegimeChanged]:
        """Return a snapshot of the captured events."""
        with self._lock:
            return list(self._events)

    def reset(self) -> None:
        """Clear the captured events (test helper)."""
        with self._lock:
            self._events.clear()


@dataclass
class NatsRegimePublisher:
    """NATS-backed publisher.

    Takes an ``async def publish(subject, payload)`` callable that
    performs the network I/O. The wrapper:

    * Encodes the payload as compact JSON via
      ``model_dump_json(by_alias=True)`` so the schema's ``from`` /
      ``to`` enum aliasing is honoured.
    * Publishes on :data:`hedge_warm_ai.regime.config.DEFAULT_REGIME_SUBJECT`
      unless an override is provided.
    * Translates any wire-level failure into :class:`RegimePublishError`
      so the engine and the Self_Healing_Supervisor can react.
    """

    async_publish: Callable[[str, bytes], Awaitable[None]]
    subject: str = DEFAULT_REGIME_SUBJECT

    async def publish_regime_change(self, event: RegimeChanged) -> None:
        # ``model_dump_json`` is the canonical Pydantic v2 path that
        # honours the ``alias=`` declaration on the ``from_`` field.
        payload = event.model_dump_json(by_alias=True).encode("utf-8")
        try:
            await self.async_publish(self.subject, payload)
        except RegimePublishError:
            # already typed; preserve traceback
            raise
        except Exception as exc:
            _LOG.warning(
                "regime_publish_failed",
                subject=self.subject,
                from_=event.from_,
                to=event.to,
                error=str(exc),
            )
            raise RegimePublishError(
                f"failed to publish ai.regime.changed on {self.subject!r}: {exc}"
            ) from exc


# ---------------------------------------------------------------------------
# Helpers -------------------------------------------------------------------
# ---------------------------------------------------------------------------


def encode_event(event: RegimeChanged) -> bytes:
    """Encode a :class:`RegimeChanged` as the canonical JSON byte string.

    Exposed so callers wiring a custom transport (e.g. Redis Streams in
    a replay rig) can produce the byte-identical wire format without
    constructing a publisher.
    """
    # ``model_dump_json`` is preferable but ``json.dumps`` over
    # ``model_dump`` lets us dictate separators for size parity with
    # the rest of the Warm_AI_Pipeline emitters.
    return json.dumps(
        event.model_dump(mode="json", by_alias=True), separators=(",", ":")
    ).encode("utf-8")


__all__ = [
    "InMemoryRegimePublisher",
    "NatsRegimePublisher",
    "NoopRegimePublisher",
    "RegimePublisher",
    "encode_event",
]
