"""Per-correlation-id publisher for ``ai.rank.<correlation_id>`` (R17.3).

The :class:`hedge_warm_ai.ranking.AiTradeRankingEngine` decides *what*
to emit. The network sink is decoupled behind a small
:class:`RankPublisher` protocol so:

1. Tests substitute :class:`InMemoryRankPublisher` and assert on
   captured events without spinning up NATS.
2. Production wires :class:`NatsRankPublisher` around the same
   ``async def publish(subject, payload)`` callable that the rest of
   the Warm_AI_Pipeline already uses (mirrors
   :class:`hedge_warm_ai.psychology.publisher.NatsPsychPublisher`).

The canonical NATS subject prefix is ``ai.rank`` and the full subject
for a correlation id is ``ai.rank.<hex>`` where ``<hex>`` is the
lower-case hex form of the 16-byte ``CorrelationId`` (matches the Rust
``hedge_bus::subject::AI_RANK`` + ``subjects::ai_rank`` helper).

The wire payload is the canonical JSON shape declared in
``ai_rank.schema.json`` and mirrored by
:class:`hedge_warm_ai.schemas.RankedSignal`. Bypassing that schema is
forbidden — the publisher takes a typed model so a caller cannot
accidentally publish a malformed payload.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from threading import RLock
from typing import Awaitable, Callable, Final, Protocol

import structlog

from ..schemas import RankedSignal
from .errors import RankingPublishError

_LOG: Final = structlog.get_logger(__name__)

#: Canonical NATS subject prefix. Correlation ids are appended as
#: ``ai.rank.<hex>``. Mirrors the Rust constant
#: ``hedge_bus::subject::AI_RANK``.
AI_RANK_PREFIX: Final[str] = "ai.rank"


def ai_rank_subject(correlation_id_hex: str) -> str:
    """Build the per-correlation-id ``ai.rank.<hex>`` subject.

    Args:
        correlation_id_hex: Lower-case hex form of the 16-byte
            correlation id. Must be non-empty and free of ``.``
            separators (NATS uses ``.`` as a topic delimiter).

    Raises:
        ValueError: ``correlation_id_hex`` is empty or contains ``.``.
    """
    if not correlation_id_hex:
        raise ValueError("correlation_id_hex must be non-empty")
    if "." in correlation_id_hex:
        raise ValueError(
            f"correlation_id_hex must not contain '.' separator: "
            f"{correlation_id_hex!r}"
        )
    return f"{AI_RANK_PREFIX}.{correlation_id_hex}"


# ---------------------------------------------------------------------------
# Protocol ------------------------------------------------------------------
# ---------------------------------------------------------------------------


class RankPublisher(Protocol):
    """Sink for typed :class:`RankedSignal` payloads.

    Implementations route the payload to the per-correlation-id
    ``ai.rank.<hex>`` subject; the hex is taken from
    ``event.correlation_id`` so callers do not need to thread it
    separately.

    Implementations MUST:

    * Validate the payload by round-tripping through
      :class:`RankedSignal` (the model carries
      ``additionalProperties: false`` and bounds every factor +
      score to ``[0.0, 1.0]``).
    * Encode the payload as compact JSON.
    * Raise :class:`RankingPublishError` on any wire-level failure
      so the engine can decide whether to retry or surface the
      failure to the Self_Healing_Supervisor.
    """

    async def publish_rank(self, event: RankedSignal) -> None: ...


# ---------------------------------------------------------------------------
# Implementations -----------------------------------------------------------
# ---------------------------------------------------------------------------


class NoopRankPublisher:
    """Discards every event. Useful when the bus is not yet wired."""

    async def publish_rank(self, event: RankedSignal) -> None:  # noqa: D401
        return


class InMemoryRankPublisher:
    """Captures published events in memory for assertion in tests.

    Thread-safe: the underlying list is guarded by an :class:`RLock`.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._events: list[RankedSignal] = []

    async def publish_rank(self, event: RankedSignal) -> None:
        with self._lock:
            self._events.append(event)

    @property
    def events(self) -> list[RankedSignal]:
        """Return a snapshot of the captured events (newest last)."""
        with self._lock:
            return list(self._events)

    def reset(self) -> None:
        """Clear the captured events (test helper)."""
        with self._lock:
            self._events.clear()


@dataclass
class NatsRankPublisher:
    """NATS-backed publisher.

    Takes an ``async def publish(subject, payload)`` callable that
    performs the network I/O. The wrapper:

    * Serialises the :class:`RankedSignal` model to compact JSON via
      ``model_dump(mode="json")`` so the schema's enum and bounded
      number fields are honoured.
    * Publishes on ``ai.rank.<correlation_id>`` (per-correlation-id
      pattern matching the Rust ``subjects::ai_rank`` helper).
    * Translates wire-level failures into :class:`RankingPublishError`
      so the engine and the Self_Healing_Supervisor can react.
    """

    async_publish: Callable[[str, bytes], Awaitable[None]]

    async def publish_rank(self, event: RankedSignal) -> None:
        subject = ai_rank_subject(event.correlation_id)
        payload = json.dumps(
            event.model_dump(mode="json"), separators=(",", ":")
        ).encode("utf-8")
        try:
            await self.async_publish(subject, payload)
        except RankingPublishError:
            # already typed; preserve traceback
            raise
        except Exception as exc:
            _LOG.warning(
                "ai_rank_publish_failed",
                subject=subject,
                signal_id=event.signal_id,
                trade_confidence_score=event.trade_confidence_score,
                error=str(exc),
            )
            raise RankingPublishError(
                f"failed to publish ai.rank on {subject!r}: {exc}"
            ) from exc


__all__ = [
    "AI_RANK_PREFIX",
    "InMemoryRankPublisher",
    "NatsRankPublisher",
    "NoopRankPublisher",
    "RankPublisher",
    "ai_rank_subject",
]
