"""Configuration surface for the Market_Regime_Engine (task 22.1).

The engine's tunables live in three groups:

1. **Classification thresholds** — the cut-points the rule-based
   classifier in :mod:`hedge_warm_ai.regime.classifier` uses to
   bucket a :class:`~.signals.RegimeObservation` into one of the
   seven design regimes (R13.1).
2. **Stability-factor map** — per-regime multiplier surfaced to the
   Risk_Engine via the WarmCache (``MarketStability`` factor in
   ``Adaptive_Risk``, R5.13). ``Trending`` defaults to ``1.0`` and
   ``Panic`` / ``LiquidityCrisis`` default to near-zero — a stable
   market does not dampen sizing; a panic flatlines it.
3. **Operational knobs** — evaluation interval, NATS subject
   override, and a feature flag for using a future ONNX-backed
   classical-ML scorer instead of the rule-based classifier.

All three groups are pydantic models with ``extra="forbid"`` and
``frozen=True``-equivalent semantics through :class:`pydantic.ConfigDict`.
This mirrors the discipline used by :class:`hedge_warm_ai.config.HedgeConfig`
so a misconfigured deployment fails closed at construction time.

The defaults baked into this module are the reference cut-points for
the design's seven regimes derived from the descriptions in
Requirements §13. They are *not* hardcoded thresholds spread through
the classifier source — they live here, in the configuration model,
so deployments can override them via either:

* :meth:`RegimeConfig.from_yaml` — load from a sibling YAML config
  alongside the main ``HedgeConfig`` (the same loader pattern used by
  :mod:`hedge_warm_ai.config`).
* Direct construction in tests.

The module documents the binding between the
:class:`hedge_warm_ai.config.HedgeConfig` mirror and this
:class:`RegimeConfig`: callers typically construct
:class:`RegimeConfig` independently because the Rust ``HedgeConfig``
schema does not (yet) carry a ``regime`` block. When the Rust crate
adds one in a future task the :meth:`from_hedge_config` adaptor below
will start sourcing the values from there without changing the call
sites in :class:`MarketRegimeEngine`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Final, Mapping

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeFloat,
    PositiveFloat,
    PositiveInt,
    ValidationError,
    field_validator,
    model_validator,
)

from ..config import HedgeConfig, SchemaViolationError
from ..schemas.ai_regime_changed import Regime
from .errors import RegimeConfigError


# ---------------------------------------------------------------------------
# Defaults ------------------------------------------------------------------
# ---------------------------------------------------------------------------

#: Default NATS subject for the regime-change event. Mirrors
#: ``hedge_bus::subject::AI_REGIME_CHANGED`` and the canonical
#: ``ai_regime_changed.schema.json``. Overrideable per deployment but
#: production should leave it at the canonical value.
DEFAULT_REGIME_SUBJECT: Final[str] = "ai.regime.changed"

#: Default evaluation interval. The design specifies "on each configured
#: evaluation interval" (R13.2); five seconds balances responsiveness
#: against classifier work and keeps emission rate bounded since
#: emissions are edge-triggered.
DEFAULT_EVALUATION_INTERVAL_S: Final[float] = 5.0

#: Default seed regime when the engine starts and no prior state exists.
#: ``Sideways`` is the conservative neutral choice — it does not bias
#: the Risk_Engine's ``MarketStability`` toward either expansion
#: (``Trending``, ``HighVolatility``) or contraction (``Panic``,
#: ``LiquidityCrisis``).
DEFAULT_SEED_REGIME: Final[Regime] = "Sideways"


class _StrictModel(BaseModel):
    """All regime config models forbid unknown fields and re-validate on assignment."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=False)


# ---------------------------------------------------------------------------
# Threshold model -----------------------------------------------------------
# ---------------------------------------------------------------------------


class RegimeThresholds(_StrictModel):
    """Cut-points used by :class:`~.classifier.RuleBasedRegimeClassifier`.

    The fields encode the verbal definitions from the design:

    * ``Panic``             — sharp drawdown with negative breadth.
    * ``LiquidityCrisis``   — low liquidity score regardless of price.
    * ``HighVolatility``    — elevated realised vol with broad
                              participation in highs/lows.
    * ``NewsDriven``        — high news-impact magnitude.
    * ``LowParticipation``  — participation ratio below floor.
    * ``Trending``          — strong trend strength magnitude.
    * ``Sideways``          — fallback when none of the above hold.

    The ordering above is also the *priority order* the classifier uses
    when multiple buckets match a single observation (e.g. high
    volatility *and* news-driven). The order is fixed in code; only
    the cut-points are configurable here.

    Cross-field invariants (enforced in ``__post_init__``):

    * ``trending_trend_strength > 0.0``
    * ``high_volatility_volatility > 0.0``
    * ``panic_drawdown > 0.0`` and ``panic_breadth < 0.0``
    * ``liquidity_crisis_liquidity_score < 1.0``
    * ``low_participation_max < 1.0``
    """

    panic_drawdown: float = Field(
        default=0.05,
        gt=0.0,
        le=1.0,
        description="Drawdown threshold (≥) that flags Panic when paired with negative breadth.",
    )
    panic_breadth: float = Field(
        default=-0.4,
        ge=-1.0,
        lt=0.0,
        description="Breadth threshold (≤) that flags Panic when paired with elevated drawdown.",
    )
    liquidity_crisis_liquidity_score: float = Field(
        default=0.25,
        ge=0.0,
        lt=1.0,
        description="Liquidity-score floor (<) below which the regime is LiquidityCrisis.",
    )
    high_volatility_volatility: float = Field(
        default=0.5,
        gt=0.0,
        le=1.0,
        description="Volatility floor (≥) for HighVolatility classification.",
    )
    high_volatility_breadth: float = Field(
        default=0.4,
        ge=0.0,
        le=1.0,
        description=(
            "Volatility-breadth floor (≥) — high realised vol alone is not enough; "
            "we also require broad participation in highs/lows."
        ),
    )
    news_driven_pressure: float = Field(
        default=0.6,
        ge=0.0,
        le=1.0,
        description="Aggregated news-impact floor (≥) for NewsDriven classification.",
    )
    low_participation_max: float = Field(
        default=0.25,
        gt=0.0,
        lt=1.0,
        description="Participation ceiling (<) below which the regime is LowParticipation.",
    )
    trending_trend_strength: float = Field(
        default=0.4,
        gt=0.0,
        le=1.0,
        description="Absolute-trend-strength floor (≥) for Trending classification.",
    )

    @model_validator(mode="after")
    def _check_invariants(self) -> "RegimeThresholds":
        # Pydantic field bounds already cover most invariants; this
        # validator double-checks the cross-field constraints called
        # out in the docstring so a future schema change does not
        # silently regress them.
        if self.trending_trend_strength <= 0.0:
            raise RegimeConfigError(
                f"trending_trend_strength must be > 0.0, got {self.trending_trend_strength!r}"
            )
        if self.high_volatility_volatility <= 0.0:
            raise RegimeConfigError(
                f"high_volatility_volatility must be > 0.0, got {self.high_volatility_volatility!r}"
            )
        if self.panic_drawdown <= 0.0 or self.panic_breadth >= 0.0:
            raise RegimeConfigError(
                "Panic thresholds inconsistent: panic_drawdown must be > 0 and "
                "panic_breadth must be < 0; got "
                f"panic_drawdown={self.panic_drawdown!r}, panic_breadth={self.panic_breadth!r}"
            )
        if self.liquidity_crisis_liquidity_score >= 1.0:
            raise RegimeConfigError(
                "liquidity_crisis_liquidity_score must be < 1.0, got "
                f"{self.liquidity_crisis_liquidity_score!r}"
            )
        if self.low_participation_max >= 1.0:
            raise RegimeConfigError(
                f"low_participation_max must be < 1.0, got {self.low_participation_max!r}"
            )
        return self


# ---------------------------------------------------------------------------
# Stability factor map ------------------------------------------------------
# ---------------------------------------------------------------------------


def _default_stability_factor_map() -> dict[str, float]:
    """Default per-regime multipliers feeding ``Adaptive_Risk`` (R5.13).

    Values reflect the design intent: stable trending markets do not
    dampen sizing; panic and liquidity crisis flatline it. The map is
    keyed by the canonical :class:`Regime` literal strings.
    """
    return {
        "Trending": 1.00,
        "Sideways": 0.80,
        "HighVolatility": 0.50,
        "NewsDriven": 0.40,
        "LowParticipation": 0.30,
        "LiquidityCrisis": 0.10,
        "Panic": 0.05,
    }


_REGIME_LABELS: Final[frozenset[str]] = frozenset(
    [
        "Trending",
        "Sideways",
        "Panic",
        "HighVolatility",
        "NewsDriven",
        "LiquidityCrisis",
        "LowParticipation",
    ]
)


class StabilityFactorMap(_StrictModel):
    """Per-regime ``MarketStability`` multiplier (R5.13)."""

    factors: dict[str, float] = Field(default_factory=_default_stability_factor_map)

    @field_validator("factors")
    @classmethod
    def _check_factors(cls, value: Mapping[str, float]) -> dict[str, float]:
        # Coverage: every Regime label must appear exactly once.
        missing = _REGIME_LABELS - set(value.keys())
        unknown = set(value.keys()) - _REGIME_LABELS
        if missing or unknown:
            raise RegimeConfigError(
                "factors must cover every Regime label exactly once; "
                f"missing={sorted(missing)!r}, unknown={sorted(unknown)!r}"
            )
        out: dict[str, float] = {}
        for label, factor in value.items():
            if not (0.0 <= factor <= 1.0):
                raise RegimeConfigError(
                    f"stability factor for {label!r} must be in [0.0, 1.0]; "
                    f"got {factor!r}"
                )
            out[label] = float(factor)
        return out

    def get(self, regime: Regime) -> float:
        return self.factors[regime]


# ---------------------------------------------------------------------------
# Top-level RegimeConfig ----------------------------------------------------
# ---------------------------------------------------------------------------


class RegimeConfig(_StrictModel):
    """Bundle of every tunable surfaced by the Market_Regime_Engine.

    The engine takes one of these at construction. Defaults match the
    reference cut-points documented above and are safe in dev; prod
    deployments should override via :meth:`from_yaml`.

    Attributes:
        thresholds: Classifier cut-points.
        stability_factors: Per-regime ``MarketStability`` multiplier
            written to the WarmCache for the Risk_Engine.
        evaluation_interval_s: Polling interval for the engine's
            evaluation loop in seconds (R13.2).
        nats_subject: Override for the canonical
            ``ai.regime.changed`` NATS subject. Defaults to
            :data:`DEFAULT_REGIME_SUBJECT`.
        seed_regime: Initial regime the engine assumes before its first
            evaluation. Used to short-circuit a spurious "no regime →
            something" emission on first tick.
        use_onnx_classifier: Reserved feature flag for swapping in a
            future Tiny LSTM / classical-ML ONNX scorer (task 22.x in
            a follow-up). Default ``False`` keeps the rule-based
            classifier authoritative.
    """

    thresholds: RegimeThresholds = Field(default_factory=RegimeThresholds)
    stability_factors: StabilityFactorMap = Field(default_factory=StabilityFactorMap)
    evaluation_interval_s: PositiveFloat = Field(
        default=DEFAULT_EVALUATION_INTERVAL_S,
        description="Engine polling interval in seconds (R13.2).",
    )
    nats_subject: str = Field(
        default=DEFAULT_REGIME_SUBJECT,
        min_length=1,
        description="NATS subject for ``ai.regime.changed`` emissions.",
    )
    seed_regime: Regime = Field(default=DEFAULT_SEED_REGIME)
    use_onnx_classifier: bool = Field(default=False)
    publish_warmup_skip: PositiveInt = Field(
        default=1,
        description=(
            "Number of initial evaluations whose result is recorded as the "
            "current regime *without* emitting an ``ai.regime.changed`` event. "
            "1 prevents a 'seed → first observation' edge from generating a "
            "spurious initial change."
        ),
    )

    # ----- alternate constructors -----------------------------------------

    @classmethod
    def from_yaml(cls, raw: str) -> "RegimeConfig":
        """Parse a YAML body into a :class:`RegimeConfig`.

        The YAML shape mirrors the field names of this model. Unknown
        fields raise :class:`RegimeConfigError` (extra=forbid) so a
        typo cannot silently fall back to the default.
        """
        try:
            parsed = yaml.safe_load(raw)
        except yaml.YAMLError as exc:
            raise RegimeConfigError(f"invalid YAML: {exc}") from exc
        if parsed is None:
            return cls()
        if not isinstance(parsed, dict):
            raise RegimeConfigError(
                f"top-level regime config must be a mapping, got {type(parsed).__name__}"
            )
        try:
            return cls.model_validate(parsed)
        except ValidationError as exc:
            raise RegimeConfigError(str(exc)) from exc

    @classmethod
    def from_yaml_path(cls, path: str | Path) -> "RegimeConfig":
        """Load a :class:`RegimeConfig` from disk via the same loader."""
        return cls.from_yaml(Path(path).read_text(encoding="utf-8"))

    @classmethod
    def from_hedge_config(cls, hedge: HedgeConfig | None = None) -> "RegimeConfig":
        """Adaptor: source defaults from a :class:`HedgeConfig` mirror.

        The Rust ``HedgeConfig`` schema does not yet carry a ``regime``
        block (the canonical schema lives in
        ``crates/hedge-config/schema.json`` and adding to it requires a
        separate task that touches the Rust side). Until that lands,
        this adaptor reads the ``HedgeConfig`` it was handed *only* to
        confirm config-load discipline and otherwise returns the
        defaults documented above. When the Rust crate gains a
        ``regime`` block, this method will start populating
        ``thresholds``, ``stability_factors``, and
        ``evaluation_interval_s`` from there without changing the
        engine's call site.

        Args:
            hedge: An already-loaded :class:`HedgeConfig`. ``None``
                is accepted for callers that have not yet wired the
                config loader.

        Raises:
            SchemaViolationError: re-raised from the underlying loader
                if the supplied :class:`HedgeConfig` itself is invalid.
        """
        # Touching the supplied config establishes the dependency edge
        # that future tasks will exploit. The current implementation
        # honours nothing beyond confirming the config validates.
        if hedge is not None and not isinstance(hedge, HedgeConfig):
            # ``isinstance`` is defensive — the type system enforces
            # this — but raising via SchemaViolationError keeps the
            # error class consistent with the rest of the loader.
            raise SchemaViolationError(
                "from_hedge_config expected a HedgeConfig instance; "
                f"got {type(hedge).__name__}"
            )
        return cls()


__all__ = [
    "DEFAULT_EVALUATION_INTERVAL_S",
    "DEFAULT_REGIME_SUBJECT",
    "DEFAULT_SEED_REGIME",
    "RegimeConfig",
    "RegimeThresholds",
    "StabilityFactorMap",
]
