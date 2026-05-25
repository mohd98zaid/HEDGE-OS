"""Value types consumed and produced by the Market_Regime_Engine.

* :class:`RegimeObservation` is the per-evaluation bundle of market
  signals fed into the classifier. The fields are the lowest common
  denominator across the seven design regimes (R13.1) and are
  populated by the Hot_Path's Feature_Extraction_Engine and
  Orderflow_Engine via the NATS subjects the engine subscribes to.
  Bounds are encoded at the type-level so a malformed upstream event
  raises immediately rather than silently misclassifying.
* :class:`MarketStabilityFactor` is the value the engine writes to
  the WarmCache for the Risk_Engine's
  ``Adaptive_Risk = BaseRisk × MarketStability × …`` formula
  (R5.13). It is a frozen dataclass with a clamped scalar in
  ``[0.0, 1.0]`` and a ``derived_from`` regime label so consumers can
  log-trace why the factor changed.

Both types are deliberately framework-light: they do not depend on
:mod:`pydantic` so they can be constructed cheaply on the engine's
hot loop without revalidation. Validation happens once at construction
time in ``__post_init__``.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from ..schemas.ai_regime_changed import Regime
from .errors import RegimeClassificationError


def _check_unit_interval(name: str, value: float) -> None:
    if math.isnan(value) or math.isinf(value):
        raise RegimeClassificationError(
            f"{name} must be a finite number in [0.0, 1.0]; got {value!r}"
        )
    if not (0.0 <= value <= 1.0):
        raise RegimeClassificationError(
            f"{name} must be in [0.0, 1.0]; got {value!r}"
        )


def _check_signed_unit(name: str, value: float) -> None:
    if math.isnan(value) or math.isinf(value):
        raise RegimeClassificationError(
            f"{name} must be a finite number in [-1.0, 1.0]; got {value!r}"
        )
    if not (-1.0 <= value <= 1.0):
        raise RegimeClassificationError(
            f"{name} must be in [-1.0, 1.0]; got {value!r}"
        )


@dataclass(frozen=True, slots=True)
class RegimeObservation:
    """One evaluation-interval observation of market state.

    The classifier consumes these directly. Producers populate the
    fields from the canonical Hot_Path NATS subjects:

    * ``volatility``         ← realised volatility on the index proxy
                               (Feature_Extraction_Engine output, R3.1).
    * ``trend_strength``     ← signed EMA-slope-derived trend score on
                               the index proxy (R3.1, R3.2). Range
                               ``[-1.0, 1.0]``; sign indicates direction.
    * ``breadth``            ← sector breadth from
                               ``md.breadth.sector`` (R1.7). ``[-1, 1]``.
    * ``volatility_breadth`` ← share of names making fresh highs /
                               lows from ``md.breadth.volatility``.
                               ``[0.0, 1.0]``.
    * ``news_pressure``      ← aggregate news-impact magnitude over a
                               short window (News_Intelligence_Engine,
                               R12.4). ``[0.0, 1.0]``.
    * ``liquidity_score``    ← rolling liquidity health derived from
                               Orderflow_Engine ``liquidity_pressure``
                               (R2.5) re-mapped to ``[0.0, 1.0]`` —
                               ``1.0`` = healthy, ``0.0`` = liquidity
                               crisis.
    * ``participation``      ← active-symbols / tracked-symbols ratio
                               (Symbol_Priority_Engine, R14). ``[0, 1]``.
    * ``drawdown``           ← intraday drawdown of the index proxy
                               from session high; ``[0.0, 1.0]``
                               where ``1.0`` is a 100% drawdown.
    * ``ts_ns``              ← producer's wall-clock ns. Forwarded to
                               the emitted ``ai.regime.changed`` event
                               so consumers see the source-side
                               timestamp rather than the engine's.

    All bounded fields are validated in ``__post_init__``; out-of-
    range values raise :class:`RegimeClassificationError` so the
    engine refuses to classify on bad input rather than emitting a
    spurious regime change.
    """

    volatility: float
    trend_strength: float
    breadth: float
    volatility_breadth: float
    news_pressure: float
    liquidity_score: float
    participation: float
    drawdown: float
    ts_ns: int

    def __post_init__(self) -> None:
        _check_unit_interval("volatility", self.volatility)
        _check_signed_unit("trend_strength", self.trend_strength)
        _check_signed_unit("breadth", self.breadth)
        _check_unit_interval("volatility_breadth", self.volatility_breadth)
        _check_unit_interval("news_pressure", self.news_pressure)
        _check_unit_interval("liquidity_score", self.liquidity_score)
        _check_unit_interval("participation", self.participation)
        _check_unit_interval("drawdown", self.drawdown)
        if self.ts_ns < 0:
            raise RegimeClassificationError(
                f"ts_ns must be a non-negative wall-clock ns; got {self.ts_ns!r}"
            )


@dataclass(frozen=True, slots=True)
class MarketStabilityFactor:
    """The ``MarketStability`` scalar surfaced to the Risk_Engine.

    The Risk_Engine multiplies this directly into ``Adaptive_Risk``
    (R5.13). A value of ``1.0`` means "no stability dampening"; a
    value of ``0.0`` means "block all sizing" (the Risk_Engine treats
    the resulting ``Adaptive_Risk == 0`` as a reject reason).

    Attributes:
        value: Stability scalar in ``[0.0, 1.0]``. Producers MUST
            clamp before construction; this dataclass re-validates.
        derived_from: The regime label that produced this value.
            Stored so log-tracers can correlate a stability dip with
            the regime change that caused it.
        ts_ns: Producer's wall-clock ns. Carries through to consumers
            for staleness checks.
    """

    value: float
    derived_from: Regime
    ts_ns: int

    def __post_init__(self) -> None:
        _check_unit_interval("value", self.value)
        if self.ts_ns < 0:
            raise RegimeClassificationError(
                f"ts_ns must be a non-negative wall-clock ns; got {self.ts_ns!r}"
            )


__all__ = [
    "MarketStabilityFactor",
    "RegimeObservation",
]
