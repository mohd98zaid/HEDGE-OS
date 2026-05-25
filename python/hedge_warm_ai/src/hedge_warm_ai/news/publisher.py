"""Edge-triggered publisher for ``ai.news.impact.<symbol>`` (R12.4).

The :class:`NewsIntelligenceEngine` decides *what* to emit. The
network sink is decoupled behind a small :class:`NewsPublisher`
protocol so:

1. Tests substitute :class:`InMemoryNewsPublisher` and assert on
   captured events without spinning up NATS.
2. Production wires :class:`NatsNewsPublisher` around the same
   ``async def publish(subject, payload)`` callable that
   :class:`hedge_warm_ai.priority.publisher.NatsPriorityChangedPublisher`
   and :class:`hedge_warm_ai.psychology.publisher.NatsPsychPublisher`
   already use.

The canonical NATS subject prefix is ``ai.news.impact`` and the
full subject for a symbol is ``ai.news.impact.<symbol>``,
matching ``hedge-bus::subject::AI_NEWS_IMPACT``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from threading import RLock
from typing import Awaitable, Callable, Final, Protocol

import structlog

from ..schemas import NewsImpact
from .errors import NewsPublishError

_LOG: Final = structlog.get_logger(__name__)

#: Canonical NATS subject prefix. Symbols are appended as
#: ``ai.news.impact.<symbol>``. Mirrors the Rust constant
#: ``hedge_bus::subject::AI_NEWS_IMPACT``.
AI_NEWS_IMPACT_PREFIX: Final[str] = "ai.news.impact"


def news_impact_subject(symbol: str) -> str:
    """Build the per-symbol ``ai.news.impact.<sym>`` subject.

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
    return f"{AI_NEWS_IMPACT_PREFIX}.{symbol}"


# ---------------------------------------------------------------------------
# Protocol ------------------------------------------------------------------
# ---------------------------------------------------------------------------


class NewsPublisher(Protocol):
    """Sink for typed :class:`NewsImpact` payloads.

    Implementations route the payload to the per-symbol
    ``ai.news.impact.<sym>`` subject; the symbol is taken from
    ``event.symbol`` so callers do not need to thread it separately.

    Implementations MUST:

    * Validate the payload by round-tripping through
      :class:`NewsImpact` (the model carries
      ``additionalProperties: false`` and bounds ``sentiment``,
      ``impact_magnitude`` to the canonical ranges).
    * Encode the payload as compact JSON.
    * Raise :class:`NewsPublishError` on any wire-level failure so
      the engine can decide whether to retry or surface the failure
      to the Self_Healing_Supervisor.
    """

    async def publish_news_impact(self, event: NewsImpact) -> None: ...


# ---------------------------------------------------------------------------
# Implementations -----------------------------------------------------------
# ---------------------------------------------------------------------------


class NoopNewsPublisher:
    """Discards every event. Useful when the bus is not yet wired."""

    async def publish_news_impact(self, event: NewsImpact) -> None:  # noqa: D401
        return


class InMemoryNewsPublisher:
    """Captures events in memory for assertion in tests.

    Thread-safe: the underlying list is guarded by an :class:`RLock`.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._events: list[NewsImpact] = []

    async def publish_news_impact(self, event: NewsImpact) -> None:
        with self._lock:
            self._events.append(event)

    @property
    def events(self) -> list[NewsImpact]:
        """Return a snapshot of the captured events (newest last)."""
        with self._lock:
            return list(self._events)

    def reset(self) -> None:
        """Clear the captured events (test helper)."""
        with self._lock:
            self._events.clear()


@dataclass
class NatsNewsPublisher:
    """NATS-backed publisher.

    Takes an ``async def publish(subject, payload)`` callable that
    performs the network I/O. The wrapper:

    * Serialises the :class:`NewsImpact` model to compact JSON.
    * Publishes on ``ai.news.impact.<event.symbol>``.
    * Translates wire-level failures into :class:`NewsPublishError`
      so the engine and the Self_Healing_Supervisor can react.
    """

    async_publish: Callable[[str, bytes], Awaitable[None]]

    async def publish_news_impact(self, event: NewsImpact) -> None:
        subject = news_impact_subject(event.symbol)
        payload = json.dumps(
            event.model_dump(mode="json"), separators=(",", ":")
        ).encode("utf-8")
        try:
            await self.async_publish(subject, payload)
        except NewsPublishError:
            raise
        except Exception as exc:
            _LOG.warning(
                "news_impact_publish_failed",
                subject=subject,
                symbol=event.symbol,
                sentiment=event.sentiment,
                impact_magnitude=event.impact_magnitude,
                error=str(exc),
            )
            raise NewsPublishError(
                f"failed to publish ai.news.impact on {subject!r}: {exc}"
            ) from exc


__all__ = [
    "AI_NEWS_IMPACT_PREFIX",
    "InMemoryNewsPublisher",
    "NatsNewsPublisher",
    "NewsPublisher",
    "NoopNewsPublisher",
    "news_impact_subject",
]
