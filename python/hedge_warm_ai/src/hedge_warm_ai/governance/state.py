"""Value types consumed and produced by the AI_Governance_Engine.

The engine tracks four metrics per Warm_AI_Pipeline component:

* ``drift`` — KS-test or population-stability-index over a rolling
  window of the component's output distribution.
* ``confidence_stability`` — variance of consecutive confidence
  outputs over a rolling window (low variance = stable, high
  variance = unstable).
* ``hallucination_indicators`` — heuristic count of malformed or
  inconsistent outputs (e.g. for the news engine, headlines whose
  Ollama slow-path output references a symbol absent from the
  tracked universe; for ranking, ``AiRank`` payloads whose factor
  breakdown does not sum to the configured weight scheme).
* ``prediction_quality`` — accuracy of the component's outputs
  against subsequent market outcomes (e.g. did a high
  ``Trade_Confidence_Score`` correlate with a profitable trade in
  the next 30 minutes).

The on-the-wire ``ai.gov.action`` schema bounds the metric enum to
``drift | accuracy | latency | error_rate``. Two of the four
engine-tracked metrics map to the ``error_rate`` wire value
(``confidence_stability`` and ``hallucination_indicators``); the
:class:`MetricKind` enum below carries the precise engine-internal
name so a row read from TimescaleDB or a structured log line can
distinguish them. The engine's :class:`GovernanceMetricSample` row
carries both fields verbatim.

The dataclasses are immutable + ``slots=True`` so accidental mutation
is impossible and the engine's allocation profile is bounded.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Final, Iterable, Optional


# ---------------------------------------------------------------------------
# Component enum -----------------------------------------------------------
# ---------------------------------------------------------------------------


class GovernedComponent(str, Enum):
    """Stable string identifier for each governed Warm_AI_Pipeline component.

    The seven values match the Warm_AI_Pipeline component list in the
    spec brief, in the same order they appear in design § Components.
    The string value is what is echoed verbatim into the
    ``component`` field of the ``ai.gov.action`` payload.
    """

    NEWS = "news"
    REGIME = "regime"
    PRIORITY = "priority"
    PREV_DAY = "prev_day"
    PSYCHOLOGY = "psychology"
    RANKING = "ranking"
    JOURNAL = "journal"


#: Stable iteration order of the canonical components, used by the
#: engine to enumerate them at startup.
DEFAULT_COMPONENTS: Final[tuple[GovernedComponent, ...]] = (
    GovernedComponent.NEWS,
    GovernedComponent.REGIME,
    GovernedComponent.PRIORITY,
    GovernedComponent.PREV_DAY,
    GovernedComponent.PSYCHOLOGY,
    GovernedComponent.RANKING,
    GovernedComponent.JOURNAL,
)


# ---------------------------------------------------------------------------
# Metric enums --------------------------------------------------------------
# ---------------------------------------------------------------------------


class MetricKind(str, Enum):
    """Engine-internal metric name (richer than the wire enum).

    The four values cover the four metrics the engine tracks per
    component. Each kind maps to exactly one canonical
    ``ai.gov.action.metric`` enum value via :func:`wire_metric_for`.
    """

    DRIFT = "drift"
    CONFIDENCE_STABILITY = "confidence_stability"
    HALLUCINATION_INDICATORS = "hallucination_indicators"
    PREDICTION_QUALITY = "prediction_quality"


def wire_metric_for(kind: MetricKind) -> str:
    """Project a :class:`MetricKind` onto the canonical wire enum.

    ``ai.gov.action.metric`` is bounded to
    ``drift | accuracy | latency | error_rate``. The mapping is:

    * ``DRIFT``                   → ``drift``
    * ``CONFIDENCE_STABILITY``    → ``error_rate`` (variance is treated
                                    as a normalised error-rate signal)
    * ``HALLUCINATION_INDICATORS``→ ``error_rate``
    * ``PREDICTION_QUALITY``      → ``accuracy``
    """
    if kind == MetricKind.DRIFT:
        return "drift"
    if kind == MetricKind.PREDICTION_QUALITY:
        return "accuracy"
    return "error_rate"


# ---------------------------------------------------------------------------
# Component output observation --------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ComponentOutput:
    """One normalised observation of a component's emission.

    The engine takes these directly; service-layer adapters
    (subscribed to ``ai.rank.*``, ``ai.regime.changed``,
    ``ai.psych.stability``, ``ai.priority.changed.*``,
    ``ai.news.impact.*``, ``mem.prev_day.<sym>``,
    ``ai.journal.entry``) translate each wire payload into a
    :class:`ComponentOutput` and feed it to
    :meth:`hedge_warm_ai.governance.AiGovernanceEngine.observe`.

    Attributes:
        component: The :class:`GovernedComponent` that emitted the
            output.
        confidence: The output's confidence in ``[0.0, 1.0]`` when the
            component supplies one (ranking, regime, psychology),
            else ``None``. Used by the
            :class:`MetricKind.CONFIDENCE_STABILITY` metric.
        feature_vector: Optional numerical vector summarising the
            output's distributional shape; the engine feeds this into
            the population-stability-index drift estimator. ``()``
            for components that do not have a stable feature vector
            (e.g. news engine emissions are not vector-shaped).
        hallucination_flag: ``True`` when the service-layer adapter
            detected a hallucination heuristic (e.g. news engine
            output referenced an unknown symbol; ranking factor
            breakdown did not sum to the configured weight scheme).
        correlation_id: Stable correlation id (lower-case hex form)
            of the output. The engine retains this so prediction-
            quality outcomes derived from a closed trade can be
            attributed back to the originating component output.
        ts_ns: Producer-side wall-clock ns timestamp.
    """

    component: GovernedComponent
    confidence: Optional[float] = None
    feature_vector: tuple[float, ...] = ()
    hallucination_flag: bool = False
    correlation_id: str = ""
    ts_ns: int = 0


# ---------------------------------------------------------------------------
# Trade outcome -------------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TradeOutcome:
    """One realised market outcome derived from ``exec.trade.closed``.

    The engine correlates outcomes with component outputs via
    ``correlation_id`` so a high-confidence ranking that lost money
    decays the ``ranking`` component's ``prediction_quality`` metric.

    Attributes:
        correlation_id: Same correlation id the originating component
            output carried. The engine looks the output up in its
            per-component pending-correlation table to decide which
            outputs are scored by this outcome.
        pnl_inr: Realised P&L in INR. The engine converts this to a
            binary "profitable" (``pnl_inr > 0``) signal for the
            accuracy calculation.
        ts_ns: Wall-clock ns of the trade closure.
    """

    correlation_id: str
    pnl_inr: float
    ts_ns: int


# ---------------------------------------------------------------------------
# Rolling window -----------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class RollingWindow:
    """Bounded time-ordered ring of recent floats.

    Used by every metric estimator. ``capacity`` is per-metric
    (configured via :class:`hedge_warm_ai.governance.config.GovernanceConfig`).
    The window is intentionally a :class:`collections.deque` so
    appends and the oldest-eviction are O(1).
    """

    capacity: int
    values: deque[float] = field(default_factory=deque)

    def append(self, value: float) -> None:
        if not isinstance(value, (int, float)) or value != value:
            # NaN: skip silently — the metric estimator returns the
            # current value unchanged when the window contains only
            # finite numbers.
            return
        self.values.append(float(value))
        while len(self.values) > self.capacity:
            self.values.popleft()

    def extend(self, values: Iterable[float]) -> None:
        for v in values:
            self.append(v)

    def is_full(self) -> bool:
        return len(self.values) >= self.capacity

    def is_empty(self) -> bool:
        return not self.values

    def __len__(self) -> int:
        return len(self.values)

    def snapshot(self) -> tuple[float, ...]:
        return tuple(self.values)


__all__ = [
    "ComponentOutput",
    "DEFAULT_COMPONENTS",
    "GovernedComponent",
    "MetricKind",
    "RollingWindow",
    "TradeOutcome",
    "wire_metric_for",
]
