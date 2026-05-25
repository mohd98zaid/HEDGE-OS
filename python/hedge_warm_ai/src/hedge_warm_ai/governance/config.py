"""Configuration surface for the AI_Governance_Engine (R23, R24, task 28.1).

The engine takes its tunables from the canonical
:class:`hedge_warm_ai.config.HedgeConfig`. The Rust ``HedgeConfig``
schema (`crates/hedge-config/schema.json`, mirrored at
`hedge_warm_ai.json_schemas.hedge_config.schema.json`) currently
exposes only ``ai.governance.drift_warn`` and
``ai.governance.drift_critical`` (R32 / task 6.1). The remaining
governance-engine knobs (per-metric thresholds for
``confidence_stability``, ``hallucination_indicators``, and
``prediction_quality``; rolling-window sizes; per-level weight
multipliers) sit in this module today and will fold into the Rust
schema when the canonical config grows.

This split mirrors the pattern used by the other Warm_AI_Pipeline
engines (regime, ranking, prev_day) where engine-specific tunables
live alongside the engine until the Rust schema catches up.

Two cross-field invariants are enforced at construction time:

* For every :class:`MetricKind`, ``degradation_threshold <
  critical_threshold`` (matches the Rust crate's existing
  ``drift_warn < drift_critical`` invariant).
* Every rolling window size is positive.

Failures raise :class:`GovernanceConfigError` so the engine fails
closed at startup.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final, Mapping

from ..config import HedgeConfig
from .errors import GovernanceConfigError
from .ladder import DEFAULT_WEIGHT_BY_LEVEL, GovernanceLevel
from .state import DEFAULT_COMPONENTS, GovernedComponent, MetricKind

# ---------------------------------------------------------------------------
# Defaults ------------------------------------------------------------------
# ---------------------------------------------------------------------------

#: Default rolling-window size for the drift, confidence-stability,
#: and hallucination metrics. 32 samples is enough to detect a
#: shifted distribution while remaining cheap to compute on every
#: observation.
DEFAULT_DRIFT_WINDOW: Final[int] = 32
DEFAULT_STABILITY_WINDOW: Final[int] = 32
DEFAULT_HALLUCINATION_WINDOW: Final[int] = 64

#: Default rolling-window size for the prediction-quality metric.
#: Larger because realised outcomes arrive less frequently than the
#: per-output observation rate.
DEFAULT_PREDICTION_WINDOW: Final[int] = 32

#: Reference-window size for the drift estimator. The first
#: :data:`DEFAULT_DRIFT_REFERENCE_WINDOW` non-empty samples after
#: engine startup are captured as the reference distribution; the
#: live :class:`RollingWindow` is then compared against it on every
#: observation.
DEFAULT_DRIFT_REFERENCE_WINDOW: Final[int] = 64

#: Default per-metric thresholds. These mirror the design's existing
#: ``ai.governance.drift_warn = 0.20`` and ``drift_critical = 0.35``
#: defaults from :class:`hedge_warm_ai.config.GovernanceConfig`. The
#: same numeric pair is reused for the other three metrics — operators
#: tune the live deployment via the YAML config rather than this
#: module's defaults.
DEFAULT_DEGRADATION_THRESHOLD: Final[float] = 0.20
DEFAULT_CRITICAL_THRESHOLD: Final[float] = 0.35


# ---------------------------------------------------------------------------
# Per-metric thresholds ----------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class GovernanceMetricThresholds:
    """Threshold pair for one :class:`MetricKind` (R24.2, R24.3).

    Invariant: ``0.0 <= degradation < critical <= 1.0``. The engine's
    :class:`hedge_warm_ai.governance.ladder.GovernanceLadder` re-checks
    the invariant; the same numbers are re-validated here so
    misconfigured deployments fail closed at startup.
    """

    degradation: float
    critical: float

    def __post_init__(self) -> None:
        if not (0.0 <= self.degradation < self.critical <= 1.0):
            raise GovernanceConfigError(
                "GovernanceMetricThresholds requires "
                "0.0 <= degradation < critical <= 1.0; got "
                f"degradation={self.degradation}, critical={self.critical}"
            )


# ---------------------------------------------------------------------------
# Top-level GovernanceConfig ------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class GovernanceConfig:
    """Bundle of every tunable surfaced by the AI_Governance_Engine.

    Attributes:
        components: Stable iteration order of the components the
            engine governs. Defaults to the seven canonical
            Warm_AI_Pipeline components.
        thresholds: Per-:class:`MetricKind` threshold pair. Defaults
            to the same ``(0.20, 0.35)`` pair the Rust config uses
            for ``drift``.
        drift_window: Rolling-window size for the drift estimator.
        drift_reference_window: Number of samples retained as the
            stable reference distribution against which drift is
            measured.
        stability_window: Rolling-window size for confidence
            stability.
        hallucination_window: Rolling-window size for the
            hallucination-indicator rate.
        prediction_window: Rolling-window size for prediction
            quality.
        weights: Per-:class:`GovernanceLevel` numeric multiplier
            applied to a component's contribution in
            ``Trade_Confidence_Score`` / ``Adaptive_Risk``. Defaults
            from :data:`hedge_warm_ai.governance.ladder.DEFAULT_WEIGHT_BY_LEVEL`.
        prediction_pnl_threshold_inr: A trade outcome is considered
            "matched the component's intent" when ``pnl_inr`` is on
            the same sign as the component's directional bias. The
            engine treats |pnl_inr| ≤ this threshold as a tie that
            does not penalise the component. Default ``0.0`` (any
            positive P&L counts as a hit).
    """

    components: tuple[GovernedComponent, ...] = DEFAULT_COMPONENTS
    thresholds: Mapping[MetricKind, GovernanceMetricThresholds] = field(
        default_factory=lambda: {
            kind: GovernanceMetricThresholds(
                degradation=DEFAULT_DEGRADATION_THRESHOLD,
                critical=DEFAULT_CRITICAL_THRESHOLD,
            )
            for kind in MetricKind
        }
    )
    drift_window: int = DEFAULT_DRIFT_WINDOW
    drift_reference_window: int = DEFAULT_DRIFT_REFERENCE_WINDOW
    stability_window: int = DEFAULT_STABILITY_WINDOW
    hallucination_window: int = DEFAULT_HALLUCINATION_WINDOW
    prediction_window: int = DEFAULT_PREDICTION_WINDOW
    weights: Mapping[GovernanceLevel, float] = field(
        default_factory=lambda: dict(DEFAULT_WEIGHT_BY_LEVEL)
    )
    prediction_pnl_threshold_inr: float = 0.0

    def __post_init__(self) -> None:
        # Component set is non-empty.
        if not self.components:
            raise GovernanceConfigError(
                "GovernanceConfig.components must be non-empty"
            )
        # Window sizes are positive.
        for name, n in (
            ("drift_window", self.drift_window),
            ("drift_reference_window", self.drift_reference_window),
            ("stability_window", self.stability_window),
            ("hallucination_window", self.hallucination_window),
            ("prediction_window", self.prediction_window),
        ):
            if n <= 0:
                raise GovernanceConfigError(
                    f"GovernanceConfig.{name} must be > 0; got {n!r}"
                )
        # Every metric kind has a threshold pair.
        missing = [kind for kind in MetricKind if kind not in self.thresholds]
        if missing:
            raise GovernanceConfigError(
                "GovernanceConfig.thresholds is missing entries for "
                + ", ".join(k.value for k in missing)
            )
        # Every governance level has a weight (NONE/DEGRADED/CRITICAL).
        missing_levels = [
            lvl for lvl in (GovernanceLevel.NONE, GovernanceLevel.DEGRADED, GovernanceLevel.CRITICAL)
            if lvl not in self.weights
        ]
        if missing_levels:
            raise GovernanceConfigError(
                "GovernanceConfig.weights is missing entries for "
                + ", ".join(lvl.value for lvl in missing_levels)
            )
        # NONE weight must be 1.0 (full influence) — anything else
        # would silently shrink healthy components' influence and
        # break Property 4 (formula equivalence) for downstream
        # consumers.
        if self.weights[GovernanceLevel.NONE] != 1.0:
            raise GovernanceConfigError(
                "GovernanceConfig.weights[NONE] must be 1.0; got "
                f"{self.weights[GovernanceLevel.NONE]!r}"
            )

    # ----- factories ------------------------------------------------------

    @classmethod
    def from_hedge_config(
        cls,
        cfg: HedgeConfig | None = None,
        **overrides: object,
    ) -> "GovernanceConfig":
        """Build a :class:`GovernanceConfig` from a live :class:`HedgeConfig`.

        The Rust schema currently exposes only the ``drift_warn`` /
        ``drift_critical`` pair; the engine bridges these into the
        :class:`MetricKind.DRIFT` thresholds and falls back to the
        documented defaults for the remaining three metrics.

        ``overrides`` are forwarded to the dataclass constructor so
        callers can tune per-metric thresholds and window sizes
        without re-declaring the full mapping.
        """
        if cfg is None:
            return cls(**overrides)  # type: ignore[arg-type]

        thresholds: dict[MetricKind, GovernanceMetricThresholds] = {
            MetricKind.DRIFT: GovernanceMetricThresholds(
                degradation=float(cfg.ai.governance.drift_warn),
                critical=float(cfg.ai.governance.drift_critical),
            ),
            MetricKind.CONFIDENCE_STABILITY: GovernanceMetricThresholds(
                degradation=DEFAULT_DEGRADATION_THRESHOLD,
                critical=DEFAULT_CRITICAL_THRESHOLD,
            ),
            MetricKind.HALLUCINATION_INDICATORS: GovernanceMetricThresholds(
                degradation=DEFAULT_DEGRADATION_THRESHOLD,
                critical=DEFAULT_CRITICAL_THRESHOLD,
            ),
            MetricKind.PREDICTION_QUALITY: GovernanceMetricThresholds(
                degradation=DEFAULT_DEGRADATION_THRESHOLD,
                critical=DEFAULT_CRITICAL_THRESHOLD,
            ),
        }
        # ``overrides`` may carry a custom thresholds mapping; only
        # use the bridged defaults when the caller didn't supply one.
        kwargs: dict[str, object] = dict(overrides)
        kwargs.setdefault("thresholds", thresholds)
        return cls(**kwargs)  # type: ignore[arg-type]


__all__ = [
    "DEFAULT_CRITICAL_THRESHOLD",
    "DEFAULT_DEGRADATION_THRESHOLD",
    "DEFAULT_DRIFT_REFERENCE_WINDOW",
    "DEFAULT_DRIFT_WINDOW",
    "DEFAULT_HALLUCINATION_WINDOW",
    "DEFAULT_PREDICTION_WINDOW",
    "DEFAULT_STABILITY_WINDOW",
    "GovernanceConfig",
    "GovernanceMetricThresholds",
]
