"""NATS publisher abstractions for the AI_Trade_Journal_Engine.

The engine emits one canonical JSON payload per closed trade:

* :class:`hedge_warm_ai.schemas.AiJournalEntry` on
  :data:`SUBJECT_AI_JOURNAL_ENTRY` (``ai.journal.entry``) — produced
  exactly once per ``exec.trade.closed`` (R18.1, R18.3, Property 10).

The actual NATS publish is decoupled behind a small
:class:`JournalEntryPublisher` protocol so:

1. Tests substitute :class:`InMemoryJournalEntryPublisher` and assert
   on captured events without spinning up NATS.
2. Production wires :class:`NatsJournalEntryPublisher` around the same
   ``async def publish(subject, payload)`` callable that the rest of
   the Warm_AI_Pipeline already uses (mirrors
   :class:`hedge_warm_ai.psychology.NatsPsychPublisher` and
   :class:`hedge_warm_ai.priority.NatsPriorityChangedPublisher`).

The canonical subject name matches the constant declared in
``crates/hedge-bus/src/subject.rs`` (:data:`AI_JOURNAL_ENTRY`).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from threading import RLock
from typing import Awaitable, Callable, Final, Protocol

import structlog

from ..schemas import AiJournalEntry
from .subjects import SUBJECT_AI_JOURNAL_ENTRY

_LOG: Final = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Protocol ------------------------------------------------------------------
# ---------------------------------------------------------------------------


class JournalEntryPublisher(Protocol):
    """Sink for :class:`AiJournalEntry` payloads.

    One async method, fire-and-forget at the engine layer.
    Implementations should not raise on broker-side failures; surface
    them via structured logs instead so a single failed publish
    cannot cascade into engine-wide failures.
    """

    async def publish_entry(self, event: AiJournalEntry) -> None: ...


# ---------------------------------------------------------------------------
# Implementations -----------------------------------------------------------
# ---------------------------------------------------------------------------


class NoopJournalEntryPublisher:
    """Discards every event. Useful when the bus is not yet wired."""

    async def publish_entry(self, event: AiJournalEntry) -> None:  # noqa: D401
        return


class InMemoryJournalEntryPublisher:
    """Captures published events in memory for assertion in tests.

    Thread-safe: the underlying list is guarded by an :class:`RLock`.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._entries: list[AiJournalEntry] = []

    async def publish_entry(self, event: AiJournalEntry) -> None:
        with self._lock:
            self._entries.append(event)

    @property
    def entries(self) -> list[AiJournalEntry]:
        """Return a snapshot of captured entries (newest last)."""
        with self._lock:
            return list(self._entries)

    def reset(self) -> None:
        """Clear the captured entries (test helper)."""
        with self._lock:
            self._entries.clear()


@dataclass
class NatsJournalEntryPublisher:
    """NATS-backed publisher.

    Takes an ``async def publish(subject, payload)`` callable that
    performs the network I/O. The wrapper:

    * Validates the payload by round-tripping through
      :class:`AiJournalEntry` (the model carries
      ``additionalProperties: false`` and the bound enums).
    * Encodes the payload as compact JSON.
    * Publishes on the canonical :data:`SUBJECT_AI_JOURNAL_ENTRY`.

    Failures are logged at ``warning`` level and otherwise swallowed
    so a degraded broker cannot turn a single emission into a
    cascading failure.
    """

    async_publish: Callable[[str, bytes], Awaitable[None]]
    subject: str = SUBJECT_AI_JOURNAL_ENTRY

    async def publish_entry(self, event: AiJournalEntry) -> None:
        payload = json.dumps(
            event.model_dump(mode="json"), separators=(",", ":")
        ).encode("utf-8")
        try:
            await self.async_publish(self.subject, payload)
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "journal_entry_publish_failed",
                subject=self.subject,
                trade_id=event.trade_id,
                correlation_id=event.correlation_id,
                error=str(exc),
            )


__all__ = [
    "InMemoryJournalEntryPublisher",
    "JournalEntryPublisher",
    "NatsJournalEntryPublisher",
    "NoopJournalEntryPublisher",
]
