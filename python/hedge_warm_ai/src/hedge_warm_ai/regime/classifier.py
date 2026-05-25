"""Regime classifiers used by the Market_Regime_Engine.

The engine exposes a :class:`RegimeClassifier` protocol so the
classification policy can be swapped without touching the engine's
state machine. Two implementations live here:

* :class:`RuleBasedRegimeClassifier` — the default. Walks the seven
  design regimes (R13.1) in priority order and applies the
  cut-points from :class:`~.config.RegimeThresholds`. Pure Python,
  no model artefacts, sub-microsecond per call.
* :class:`OnnxRegimeClassifier` — optional reserved seat for a
  classical-ML / Tiny LSTM scorer that imports the ONNX wrappers
  from :mod:`hedge_warm_ai.onnx_runtime` lazily. Constructed only
  when :attr:`RegimeConfig.use_onnx_classifier` is true.

Both classifiers consume :class:`~.signals.RegimeObservation` and
return a :class:`Regime` literal. They are pure functions of
``(observation, config)`` — there is **no** internal state — which
preserves the determinism property the design requires of regime
classification (R13.2 evaluation interval cleanly maps to "one call
per interval").

The classification priority order baked into the rule-based classifier
is fixed:

    1. ``LiquidityCrisis`` — overrides everything else; if the book is
       broken, no other label is meaningful.
    2. ``Panic``           — drawdown + negative breadth.
    3. ``HighVolatility``  — elevated realised vol with broad
                             participation in highs/lows.
    4. ``NewsDriven``      — news-impact dominates.
    5. ``LowParticipation``— participation ratio below floor.
    6. ``Trending``        — strong directional trend.
    7. ``Sideways``        — fallback.

The order is part of the contract; tests in 22.2 will assert it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final, Optional, Protocol

import structlog

from ..schemas.ai_regime_changed import Regime
from .config import RegimeConfig
from .errors import RegimeClassificationError
from .signals import RegimeObservation

_LOG: Final = structlog.get_logger(__name__)


class RegimeClassifier(Protocol):
    """Protocol implemented by every regime classifier."""

    def classify(self, observation: RegimeObservation) -> Regime: ...


@dataclass(slots=True)
class RuleBasedRegimeClassifier:
    """Default rule-based classifier driven by :class:`RegimeConfig`.

    Construction is cheap; the same instance is used across the engine's
    evaluation loop. The ``classify`` method is pure and re-entrant.
    """

    config: RegimeConfig

    def classify(self, observation: RegimeObservation) -> Regime:  # noqa: D401
        """Bucket *observation* into one of the seven design regimes.

        Args:
            observation: The bundle from the Hot_Path. Validated at
                construction; this method does not re-validate.

        Returns:
            A :class:`Regime` literal.
        """
        thr = self.config.thresholds

        # 1. LiquidityCrisis — broken book overrides everything.
        if observation.liquidity_score < thr.liquidity_crisis_liquidity_score:
            return "LiquidityCrisis"

        # 2. Panic — sharp drawdown with negative breadth.
        if (
            observation.drawdown >= thr.panic_drawdown
            and observation.breadth <= thr.panic_breadth
        ):
            return "Panic"

        # 3. HighVolatility — elevated realised vol *and* broad
        # participation in highs/lows. Volatility alone is not enough;
        # otherwise a single fast-moving symbol could flip the regime.
        if (
            observation.volatility >= thr.high_volatility_volatility
            and observation.volatility_breadth >= thr.high_volatility_breadth
        ):
            return "HighVolatility"

        # 4. NewsDriven — news-impact pressure dominates.
        if observation.news_pressure >= thr.news_driven_pressure:
            return "NewsDriven"

        # 5. LowParticipation — too few names trading.
        if observation.participation < thr.low_participation_max:
            return "LowParticipation"

        # 6. Trending — strong directional move.
        if abs(observation.trend_strength) >= thr.trending_trend_strength:
            return "Trending"

        # 7. Fallback.
        return "Sideways"


# ---------------------------------------------------------------------------
# Optional ONNX-backed classifier (task 22.x follow-up) ---------------------
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class OnnxRegimeClassifier:
    """Reserved seat for a classical-ML / Tiny LSTM regime scorer.

    The constructor accepts a lazily-loaded
    :class:`hedge_warm_ai.onnx_runtime.OnnxRuntime` and a model name —
    no inference happens at construction time. This lets a deployment
    that does not yet have the artefact gracefully fall back to the
    rule-based classifier:

        if config.use_onnx_classifier and runtime is not None:
            classifier = OnnxRegimeClassifier(runtime, config, fallback=...)
        else:
            classifier = RuleBasedRegimeClassifier(config)

    Today the implementation always raises :class:`NotImplementedError`
    because the trained artefact is delivered by a follow-up task. The
    type lives here so the engine's wiring is stable across the two
    futures.

    Args:
        runtime: Shared ONNX runtime instance.
        config: The same :class:`RegimeConfig`; thresholds may still
            apply as a calibration layer over the model output.
        model_name: Cache key for the runtime (e.g. ``regime_lstm``).
        fallback: Required fallback classifier used until the model
            artefact ships. The protocol forces the caller to wire
            one rather than silently no-op.
    """

    runtime: object  # typed as ``Any`` to avoid an unconditional ONNX import
    config: RegimeConfig
    model_name: str
    fallback: RegimeClassifier

    def classify(self, observation: RegimeObservation) -> Regime:  # noqa: D401
        # Until the trained artefact is published, route to the
        # fallback classifier so the engine remains operational.
        try:
            return self.fallback.classify(observation)
        except RegimeClassificationError:
            raise
        except Exception as exc:  # pragma: no cover - defensive
            _LOG.warning(
                "regime_onnx_classifier_fallback_failed",
                model=self.model_name,
                error=str(exc),
            )
            raise


def build_classifier(
    config: RegimeConfig,
    *,
    onnx_runtime: Optional[object] = None,
    model_name: str = "regime_lstm",
) -> RegimeClassifier:
    """Factory selecting between the rule-based and ONNX classifiers.

    The selection honours :attr:`RegimeConfig.use_onnx_classifier` but
    requires a runtime to be supplied; absent runtime always falls back
    to :class:`RuleBasedRegimeClassifier`.
    """
    rule_based = RuleBasedRegimeClassifier(config=config)
    if config.use_onnx_classifier and onnx_runtime is not None:
        return OnnxRegimeClassifier(
            runtime=onnx_runtime,
            config=config,
            model_name=model_name,
            fallback=rule_based,
        )
    return rule_based


__all__ = [
    "OnnxRegimeClassifier",
    "RegimeClassifier",
    "RuleBasedRegimeClassifier",
    "build_classifier",
]
