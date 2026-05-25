"""NATS publisher abstractions for the AI_Governance_Engine (R24.4).

The engine emits one canonical JSON payload per per-component
governance level transition (Property 8):

* :class:`hedge_warm_ai.schemas.AiGovAction` on
  :data:`SUBJECT_AI_GOV_ACTION` (``ai.gov.action``).

The actual NATS publish is decoupled behind a small
:class:`AiGovActionPublisher` protocol so:

1. Tests substitute :class:`InMemoryAiGovActionPublisher` and assert
   on captured events without spinning up NATS.
2. Production wires :class:`NatsAiGovActionPublisher` around the same
   ``async def publish(subject, payload)`` callable that the rest of
   the Warm_AI_Pipeline already uses (mirrors
   :class:`hedge_warm_ai.psychology.publisher.NatsPsychPublisher` and
   :class:`hedge_warm_ai.regime.publisher.NatsRegimePublisher`).

The canonical subject name matches the constant declared in
``crates/hedge-bus/src/subject.rs`` (``AI_GOV_ACTION``).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from threading import RLock
from typing import Awaitable, Callable, Final, Protocol

import structlog

from ..schemas import AiGovAction
from .errors import GovernancePublishError
from .subjects import SUBJECT_AI_GOV_ACTION

_LOG: Final = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Protocol ------------------------------------------------------------------
# ---------------------------------------------------------------------------


class AiGovActionPublisher(Protocol):
    """Sink for typed :class:`AiGovAction` payloads.

    Implementations MUST:

    * Validate the payload by round-tripping through
      :class:`AiGovAction` (the model carries
      ``additionalProperties: false`` and the bound enums).
    * Encode the payload as compact JSON.
    * Raise :class:`GovernancePublishError` on any wire-level failure
      so the engine can decide whether to retry or surface the
      failure to the Self_Healing_Supervisor.
    """

    async def publish_action(self, event: AiGovAction) -> None: ...


# ---------------------------------------------------------------------------
# Implementations -----------------------------------------------------------
# ---------------------------------------------------------------------------


class NoopAiGovActionPublisher:
    """Discards every event. Useful when the bus is not yet wired."""

    async def publish_action(self, event: AiGovAction) -> None:  # noqa: D401
        return


class InMemoryAiGovActionPublisher:
    """Captures published events in memory for assertion in tests.

    Thread-safe: the underlying list is guarded by an :class:`RLock`.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._events: list[AiGovAction] = []

    async def publish_action(self, event: AiGovAction) -> None:
        with self._lock:
            self._events.append(event)

    @property
    def events(self) -> list[AiGovAction]:
        """Return a snapshot of captured events (newest last)."""
        with self._lock:
            return list(self._events)

    def reset(self) -> None:
        """Clear the captured events (test helper)."""
        with self._lock:
            self._events.clear()


@dataclass
class NatsAiGovActionPublisher:
    """NATS-backed publisher.

    Takes an ``async def publish(subject, payload)`` callable that
    performs the network I/O. The wrapper:

    * Serialises the :class:`AiGovAction` model to compact JSON via
      ``model_dump(mode="json")`` so the schema's enum and bounded
      number fields are honoured.
    * Publishes on :data:`SUBJECT_AI_GOV_ACTION`.
    * Translates wire-level failures into
      :class:`GovernancePublishError` so the engine and the
      Self_Healing_Supervisor can react.
    """

    async_publish: Callable[[str, bytes], Awaitable[None]]
    subject: str = SUBJECT_AI_GOV_ACTION

    async def publish_action(self, event: AiGovAction) -> None:
        payload = json.dumps(
            event.model_dump(mode="json"), separators=(",", ":")
        ).encode("utf-8")
        try:
            await self.async_publish(self.subject, payload)
        except GovernancePublishError:
            raise
        except Exception as exc:
            _LOG.warning(
                "ai_gov_action_publish_failed",
                subject=self.subject,
                component=event.component,
                action=event.action,
                metric=event.metric,
                value=event.value,
                threshold=event.threshold,
                error=str(exc),
            )
            raise GovernancePublishError(
                f"failed to publish ai.gov.action on {self.subject!r}: {exc}"
            ) from exc


__all__ = [
    "AiGovActionPublisher",
    "InMemoryAiGovActionPublisher",
    "NatsAiGovActionPublisher",
    "NoopAiGovActionPublisher",
]
