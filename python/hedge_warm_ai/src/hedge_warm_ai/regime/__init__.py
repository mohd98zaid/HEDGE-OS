"""Market_Regime_Engine subpackage (task 22.1 of PROJECT HEDGE).

This package implements R13 of the requirements doc:

* Classify the current regime at every evaluation interval into one of
  ``Trending``, ``Sideways``, ``Panic``, ``HighVolatility``,
  ``NewsDriven``, ``LiquidityCrisis``, ``LowParticipation`` (R13.1).
* Emit ``ai.regime.changed`` to NATS **only on transitions**, with
  ``from`` and ``to`` (R13.3, Property 8 — edge-triggered emission).
* Update the ``MarketStability`` factor exposed via WarmCache for
  Risk_Engine consumption (R13.5, R5.13).

Module layout:

* :mod:`.signals`      — :class:`RegimeObservation` / :class:`MarketStabilityFactor`.
* :mod:`.config`       — :class:`RegimeConfig` thresholds + interval +
                         NATS subject. Loadable from YAML; bridges to
                         :class:`hedge_warm_ai.config.HedgeConfig`.
* :mod:`.classifier`   — :class:`RuleBasedRegimeClassifier` (default)
                         + :class:`OnnxRegimeClassifier` seat.
* :mod:`.publisher`    — :class:`RegimePublisher` protocol +
                         in-memory and NATS-backed implementations.
* :mod:`.warm_cache`   — :class:`MarketStabilityCache` protocol +
                         :class:`RedisMarketStabilityCache` (interim,
                         until the Rust WarmCache / task 44.x lands).
* :mod:`.engine`       — :class:`MarketRegimeEngine` orchestrator.
* :mod:`.errors`       — typed exception hierarchy.
* :mod:`.service`      — ``hedge-regime`` console-script entry point.

Heavy dependencies (:mod:`hedge_memory_rag`, :mod:`redis.asyncio`)
are imported lazily inside the adaptor modules so importing this
package does not pay the cost of the RAG layer in environments that
only need the classifier types.
"""

from __future__ import annotations

from .classifier import (
    OnnxRegimeClassifier,
    RegimeClassifier,
    RuleBasedRegimeClassifier,
    build_classifier,
)
from .config import (
    DEFAULT_EVALUATION_INTERVAL_S,
    DEFAULT_REGIME_SUBJECT,
    DEFAULT_SEED_REGIME,
    RegimeConfig,
    RegimeThresholds,
    StabilityFactorMap,
)
from .engine import MarketRegimeEngine, RegimeEvaluation
from .errors import (
    MarketStabilityCacheError,
    RegimeClassificationError,
    RegimeConfigError,
    RegimeEngineError,
    RegimePublishError,
)
from .publisher import (
    InMemoryRegimePublisher,
    NatsRegimePublisher,
    NoopRegimePublisher,
    RegimePublisher,
    encode_event,
)
from .signals import MarketStabilityFactor, RegimeObservation
from .warm_cache import (
    InMemoryMarketStabilityCache,
    MarketStabilityCache,
    RedisMarketStabilityCache,
    derive_stability_factor,
)

__all__ = [
    # classifier
    "OnnxRegimeClassifier",
    "RegimeClassifier",
    "RuleBasedRegimeClassifier",
    "build_classifier",
    # config
    "DEFAULT_EVALUATION_INTERVAL_S",
    "DEFAULT_REGIME_SUBJECT",
    "DEFAULT_SEED_REGIME",
    "RegimeConfig",
    "RegimeThresholds",
    "StabilityFactorMap",
    # engine
    "MarketRegimeEngine",
    "RegimeEvaluation",
    # errors
    "MarketStabilityCacheError",
    "RegimeClassificationError",
    "RegimeConfigError",
    "RegimeEngineError",
    "RegimePublishError",
    # publisher
    "InMemoryRegimePublisher",
    "NatsRegimePublisher",
    "NoopRegimePublisher",
    "RegimePublisher",
    "encode_event",
    # signals
    "MarketStabilityFactor",
    "RegimeObservation",
    # warm cache
    "InMemoryMarketStabilityCache",
    "MarketStabilityCache",
    "RedisMarketStabilityCache",
    "derive_stability_factor",
]
