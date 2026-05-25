"""NATS publisher abstractions for the Trader_Psychology_Engine.

The engine emits two canonical JSON payloads:

* :class:`hedge_warm_ai.schemas.PsychStability` on
  :data:`STABILITY_SUBJECT` (``ai.psych.stability``) — produced on
  every behavioral event (R16.3).
* :class:`hedge_warm_ai.schemas.PsychIntervention` on
  :data:`INTERVENTION_SUBJECT` (``ai.psych.intervention``) — produced
  edge-triggered when the threshold ladder rung changes (R16.4–R16.7,
  Property 8).

The actual NATS publish is decoupled behind a small
:class:`PsychPublisher` protocol so:

1. Tests substitute :class:`InMemoryPsychPublisher` and assert on
   captured events without spinning up NATS.
2. Production wires :class:`NatsPsychPublisher` around the same
   ``async def publish(subject, payload)`` callable that the rest of
   the Warm_AI_Pipeline already uses (mirrors
   :class:`hedge_warm_ai.ollama_client.NatsDegradedPublisher` and
   :class:`hedge_warm_ai.onnx_runtime.NatsAiLatencyEmitter`).

The canonical subject names match the constants declared in
``crates/hedge-bus/src/subject.rs`` (:data:`AI_PSYCH_STABILITY` and
:data:`AI_PSYCH_INTERVENTION`).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from threading import RLock
from typing import Awaitable, Callable, Final, Protocol

import structlog

from ..schemas import PsychIntervention, PsychStability

_LOG: Final = structlog.get_logger(__name__)

#: Canonical NATS subject for ``ai.psych.stability`` (R16.3).
#: Mirrors ``hedge-bus::subject::AI_PSYCH_STABILITY``.
STABILITY_SUBJECT: Final[str] = "ai.psych.stability"

#: Canonical NATS subject for ``ai.psych.intervention`` (R16.4–R16.7).
#: Mirrors ``hedge-bus::subject::AI_PSYCH_INTERVENTION``.
INTERVENTION_SUBJECT: Final[str] = "ai.psych.intervention"


# ---------------------------------------------------------------------------
# Protocol ------------------------------------------------------------------
# ---------------------------------------------------------------------------


class PsychPublisher(Protocol):
    """Sink for psychology payloads.

    Two async methods, one per payload type, both fire-and-forget at
    the engine layer. Implementations should not raise on broker-side
    failures; surface them via structured logs instead so a single
    failed inference cannot cascade into engine-wide failures.
    """

    async def publish_stability(self, event: PsychStability) -> None: ...

    async def publish_intervention(self, event: PsychIntervention) -> None: ...


# ---------------------------------------------------------------------------
# Implementations -----------------------------------------------------------
# ---------------------------------------------------------------------------


class NoopPsychPublisher:
    """Discards every event. Useful when the bus is not yet wired."""

    async def publish_stability(self, event: PsychStability) -> None:  # noqa: D401
        return

    async def publish_intervention(
        self, event: PsychIntervention
    ) -> None:  # noqa: D401
        return


class InMemoryPsychPublisher:
    """Captures published events in memory for assertion in tests.

    Thread-safe: two underlying lists are guarded by an :class:`RLock`.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._stability: list[PsychStability] = []
        self._interventions: list[PsychIntervention] = []

    async def publish_stability(self, event: PsychStability) -> None:
        with self._lock:
            self._stability.append(event)

    async def publish_intervention(self, event: PsychIntervention) -> None:
        with self._lock:
            self._interventions.append(event)

    @property
    def stability_events(self) -> list[PsychStability]:
        """Return a snapshot of captured stability events."""
        with self._lock:
            return list(self._stability)

    @property
    def intervention_events(self) -> list[PsychIntervention]:
        """Return a snapshot of captured intervention events."""
        with self._lock:
            return list(self._interventions)

    def reset(self) -> None:
        """Clear the captured events (test helper)."""
        with self._lock:
            self._stability.clear()
            self._interventions.clear()


@dataclass
class NatsPsychPublisher:
    """NATS-backed publisher.

    Takes an ``async def publish(subject, payload)`` callable that
    performs the network I/O. The wrapper:

    * Validates the payload by round-tripping through
      :class:`PsychStability` / :class:`PsychIntervention` (the models
      carry ``additionalProperties: false`` and bound enums).
    * Encodes the payload as compact JSON.
    * Publishes on the canonical subjects.

    Failures are logged at ``warning`` level and otherwise swallowed so
    a degraded broker cannot turn a single emission into a cascading
    failure.
    """

    async_publish: Callable[[str, bytes], Awaitable[None]]
    stability_subject: str = STABILITY_SUBJECT
    intervention_subject: str = INTERVENTION_SUBJECT

    async def publish_stability(self, event: PsychStability) -> None:
        payload = self._encode(event)
        try:
            await self.async_publish(self.stability_subject, payload)
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "psych_stability_publish_failed",
                subject=self.stability_subject,
                score=event.score,
                error=str(exc),
            )

    async def publish_intervention(self, event: PsychIntervention) -> None:
        payload = self._encode(event)
        try:
            await self.async_publish(self.intervention_subject, payload)
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "psych_intervention_publish_failed",
                subject=self.intervention_subject,
                action=event.action,
                trigger_score=event.trigger_score,
                error=str(exc),
            )

    @staticmethod
    def _encode(event: PsychStability | PsychIntervention) -> bytes:
        # ``model_dump(mode="json")`` produces a JSON-compatible dict
        # that respects the schema's enum and integer-bound constraints.
        return json.dumps(
            event.model_dump(mode="json"), separators=(",", ":")
        ).encode("utf-8")


__all__ = [
    "INTERVENTION_SUBJECT",
    "InMemoryPsychPublisher",
    "NatsPsychPublisher",
    "NoopPsychPublisher",
    "PsychPublisher",
    "STABILITY_SUBJECT",
]
