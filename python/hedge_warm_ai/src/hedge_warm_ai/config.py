"""Pydantic mirror of the Rust ``hedge-config`` crate (task 6.1).

This module is the Python source of truth for the same configuration
surface the Hot_Path Rust binaries load from ``/etc/hedge/config.yaml``.
The bundled JSON Schema (``json_schemas/hedge_config.schema.json``) is a
byte-for-byte copy of the Rust crate's ``schema.json`` so both runtimes
fail closed on the same inputs.

References:
- Design § Data Models — Configuration Surface and Defaults (R32, R5, R20, R26)
- Design § Error Handling — Configuration: fail closed at startup, emit
  ``cfg.error``, exit non-zero on any schema or invariant violation.
"""

from __future__ import annotations

import json
import sys
from datetime import time
from enum import Enum
from importlib import resources
from pathlib import Path
from typing import Any, Final

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveInt,
    ValidationError,
    field_validator,
    model_validator,
)

try:
    from jsonschema import Draft202012Validator
except ImportError as exc:  # pragma: no cover - import guard
    raise RuntimeError(
        "hedge_warm_ai.config requires `jsonschema` (jsonschema-rs Python "
        "bindings or upstream `jsonschema`)."
    ) from exc


# ---------------------------------------------------------------------------
# Errors --------------------------------------------------------------------
# ---------------------------------------------------------------------------


class ConfigError(Exception):
    """Base class for every config-load failure."""


class SchemaViolationError(ConfigError):
    """Parsed YAML failed JSON Schema validation."""


class InvariantViolationError(ConfigError):
    """Parsed config passed schema but failed a cross-field invariant."""


# ---------------------------------------------------------------------------
# Enums ---------------------------------------------------------------------
# ---------------------------------------------------------------------------


class PostTargetPolicy(str, Enum):
    """Behaviour after ``daily_profit_target_max_inr`` is hit (R32.3)."""

    REDUCE_SIZE_TO_ZERO = "reduce_size_to_zero"
    STOP_FOR_SESSION = "stop_for_session"
    HALVE_SIZE = "halve_size"
    CONTINUE = "continue"


class BrokerId(str, Enum):
    """Broker identifier mirrored from ``hedge_core::BrokerId``."""

    ZERODHA = "zerodha"
    DHAN = "dhan"
    SHOONYA = "shoonya"
    ANGEL_ONE = "angel_one"
    SIMULATED = "simulated"


class OllamaRole(str, Enum):
    """Role assigned to an Ollama model in the routing table (R10)."""

    PRIMARY = "primary"
    FAST = "fast"
    DEEP = "deep"
    LIGHTWEIGHT = "lightweight"


# ---------------------------------------------------------------------------
# Config models -------------------------------------------------------------
# ---------------------------------------------------------------------------


class _StrictModel(BaseModel):
    """All config models forbid unknown fields and re-validate on assignment."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=False)


class CapitalConfig(_StrictModel):
    """Capital base and daily-profit-target band (R32.1–R32.3)."""

    base_inr: PositiveInt = 20_000
    daily_profit_target_min_inr: NonNegativeInt = 300
    daily_profit_target_max_inr: NonNegativeInt = 1_000
    post_target_policy: PostTargetPolicy = PostTargetPolicy.REDUCE_SIZE_TO_ZERO


class RiskConfig(_StrictModel):
    """Risk_Engine limits and gates (R5, R31, R32.4)."""

    max_daily_loss_inr: NonNegativeInt = 600
    max_position_per_symbol: NonNegativeInt = 200
    max_position_portfolio: NonNegativeInt = 500
    max_leverage_per_symbol: NonNegativeFloat = 5.0
    max_leverage_account: NonNegativeFloat = 5.0
    max_drawdown_inr: NonNegativeInt = 1_000
    max_trades_per_minute: NonNegativeInt = 4
    max_trades_per_hour: NonNegativeInt = 30
    max_trades_per_session: NonNegativeInt = 60
    max_exposure_per_symbol_inr: NonNegativeInt = 20_000
    max_exposure_per_sector_inr: NonNegativeInt = 30_000
    slippage_threshold_bps: NonNegativeInt = 25
    slippage_cooldown_ms: NonNegativeInt = 60_000
    volatility_block_threshold: NonNegativeFloat = 0.06
    broker_latency_block_ms: NonNegativeInt = 250
    base_risk_per_trade_inr: NonNegativeInt = 100


def _parse_ist(value: Any) -> time:
    if isinstance(value, time):
        return value
    if isinstance(value, str):
        try:
            return time.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"invalid HH:MM:SS time {value!r}: {exc}") from exc
    raise TypeError(f"expected HH:MM:SS string, got {type(value).__name__}")


class SessionConfig(_StrictModel):
    """Regular trading session window in IST (R26.1)."""

    start_ist: time = Field(default_factory=lambda: time(9, 15, 0))
    end_ist: time = Field(default_factory=lambda: time(15, 30, 0))

    @field_validator("start_ist", "end_ist", mode="before")
    @classmethod
    def _coerce_time(cls, v: Any) -> time:  # noqa: D401
        return _parse_ist(v)


class WarModeConfig(_StrictModel):
    """Market_Open_War_Mode parameters (R26.2, R26.3)."""

    start_ist: time = Field(default_factory=lambda: time(9, 15, 0))
    end_ist: time = Field(default_factory=lambda: time(9, 45, 0))
    min_confidence: float = Field(default=0.6, ge=0.0, le=1.0)
    scan_multiplier: NonNegativeFloat = 2.0

    @field_validator("start_ist", "end_ist", mode="before")
    @classmethod
    def _coerce_time(cls, v: Any) -> time:  # noqa: D401
        return _parse_ist(v)


class UiConfig(_StrictModel):
    """UI-side thresholds."""

    high_vol_threshold: NonNegativeFloat = 0.05


class GovernanceConfig(_StrictModel):
    """Drift bands governing AI suspend/critical actions (R23)."""

    drift_warn: float = Field(default=0.20, ge=0.0, le=1.0)
    drift_critical: float = Field(default=0.35, ge=0.0, le=1.0)


class RankingFactorsConfig(_StrictModel):
    """Per-factor weights used by the AI ranking engine (R17.1).

    Surface only — formula constants are fixed in R17.1.
    """

    orderflow: float = Field(default=0.30, ge=0.0, le=1.0)
    technical_strength: float = Field(default=0.25, ge=0.0, le=1.0)
    news_sentiment: float = Field(default=0.20, ge=0.0, le=1.0)
    market_regime: float = Field(default=0.15, ge=0.0, le=1.0)
    trader_discipline: float = Field(default=0.10, ge=0.0, le=1.0)


class AiConfig(_StrictModel):
    """AI governance, ranking, and shadow-mode surface (R10, R17, R23)."""

    shadow_components: list[str] = Field(default_factory=list)
    governance: GovernanceConfig = Field(default_factory=GovernanceConfig)
    rank_p95_budget_ms: NonNegativeInt = 5
    ranking_factors: RankingFactorsConfig = Field(default_factory=RankingFactorsConfig)


class PsychologyThresholds(_StrictModel):
    """Threshold ladder for the Trader_Stability_Score (R16).

    Invariant: ``critical < suppression < cooldown < warning``.
    """

    warning: float = Field(default=0.6, ge=0.0, le=1.0)
    cooldown: float = Field(default=0.5, ge=0.0, le=1.0)
    suppression: float = Field(default=0.4, ge=0.0, le=1.0)
    critical: float = Field(default=0.3, ge=0.0, le=1.0)

    @model_validator(mode="after")
    def _check_ordering(self) -> "PsychologyThresholds":
        if not (self.critical < self.suppression < self.cooldown < self.warning):
            raise InvariantViolationError(
                "trader_psychology.thresholds must satisfy "
                f"critical < suppression < cooldown < warning; got "
                f"critical={self.critical}, suppression={self.suppression}, "
                f"cooldown={self.cooldown}, warning={self.warning}"
            )
        return self


class TraderPsychologyConfig(_StrictModel):
    """Trader_Psychology config (R16)."""

    thresholds: PsychologyThresholds = Field(default_factory=PsychologyThresholds)


class BrokerConfig(_StrictModel):
    """Broker primary/backup selection and failover thresholds (R6.5, R7.1)."""

    primary: BrokerId = BrokerId.ZERODHA
    backup: BrokerId = BrokerId.DHAN
    failover_error_rate: float = Field(default=0.20, ge=0.0, le=1.0)
    failover_latency_ms: NonNegativeInt = 250


class OllamaModelConfig(_StrictModel):
    """One row of the Ollama model registry."""

    name: str = Field(min_length=1)
    role: OllamaRole
    quant: str = Field(min_length=1)


def _default_ollama_models() -> list[OllamaModelConfig]:
    return [
        OllamaModelConfig(name="qwen2.5:14b", role=OllamaRole.PRIMARY, quant="q4_k_m"),
        OllamaModelConfig(name="mistral:7b", role=OllamaRole.FAST, quant="q4_k_m"),
        OllamaModelConfig(name="deepseek-r1", role=OllamaRole.DEEP, quant="q4_k_m"),
        OllamaModelConfig(name="phi", role=OllamaRole.LIGHTWEIGHT, quant="q4_k_m"),
    ]


class OllamaConfig(_StrictModel):
    """Ollama_Infrastructure model registry (R10)."""

    models: list[OllamaModelConfig] = Field(default_factory=_default_ollama_models, min_length=1)


class RetentionConfig(_StrictModel):
    """Retention windows in days for observability stores."""

    metrics_days: NonNegativeInt = 30
    logs_days: NonNegativeInt = 14
    traces_days: NonNegativeInt = 7


class DegradedModeConfig(_StrictModel):
    """Degraded-mode policy when observability backends misbehave (R28.6)."""

    drop_low_severity_logs_at_loki_unavailable: bool = True
    sample_traces_at_jaeger_overload: float = Field(default=0.1, ge=0.0, le=1.0)


class ObservabilityConfig(_StrictModel):
    """Observability config (R27, R28)."""

    retention: RetentionConfig = Field(default_factory=RetentionConfig)
    degraded_mode: DegradedModeConfig = Field(default_factory=DegradedModeConfig)


class HedgeConfig(_StrictModel):
    """Root configuration mirroring the Rust crate's ``HedgeConfig``."""

    capital: CapitalConfig = Field(default_factory=CapitalConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    session: SessionConfig = Field(default_factory=SessionConfig)
    war_mode: WarModeConfig = Field(default_factory=WarModeConfig)
    ui: UiConfig = Field(default_factory=UiConfig)
    ai: AiConfig = Field(default_factory=AiConfig)
    trader_psychology: TraderPsychologyConfig = Field(default_factory=TraderPsychologyConfig)
    brokers: BrokerConfig = Field(default_factory=BrokerConfig)
    ollama: OllamaConfig = Field(default_factory=OllamaConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)

    @model_validator(mode="after")
    def _check_invariants(self) -> "HedgeConfig":
        if self.capital.daily_profit_target_min_inr >= self.capital.daily_profit_target_max_inr:
            raise InvariantViolationError(
                "capital.daily_profit_target_min_inr "
                f"({self.capital.daily_profit_target_min_inr}) must be < "
                f"capital.daily_profit_target_max_inr "
                f"({self.capital.daily_profit_target_max_inr})"
            )
        if self.session.start_ist >= self.session.end_ist:
            raise InvariantViolationError(
                f"session.start_ist ({self.session.start_ist}) must be < "
                f"session.end_ist ({self.session.end_ist})"
            )
        if self.war_mode.start_ist >= self.war_mode.end_ist:
            raise InvariantViolationError(
                f"war_mode.start_ist ({self.war_mode.start_ist}) must be < "
                f"war_mode.end_ist ({self.war_mode.end_ist})"
            )
        if self.ai.governance.drift_warn >= self.ai.governance.drift_critical:
            raise InvariantViolationError(
                f"ai.governance.drift_warn ({self.ai.governance.drift_warn}) must be < "
                f"drift_critical ({self.ai.governance.drift_critical})"
            )
        if self.brokers.primary == self.brokers.backup:
            raise InvariantViolationError(
                "brokers.primary and brokers.backup must differ; "
                f"both are {self.brokers.primary.value!r}"
            )
        return self


# ---------------------------------------------------------------------------
# Schema bundling -----------------------------------------------------------
# ---------------------------------------------------------------------------

_SCHEMA_FILE: Final[str] = "hedge_config.schema.json"


def schema_dict() -> dict[str, Any]:
    """Return the bundled JSON Schema as a Python dict."""
    raw = resources.files("hedge_warm_ai.json_schemas").joinpath(_SCHEMA_FILE).read_text(
        encoding="utf-8"
    )
    return json.loads(raw)


def _validate_schema(payload: dict[str, Any]) -> None:
    """Validate ``payload`` against the bundled schema or raise ``SchemaViolationError``."""
    validator = Draft202012Validator(schema_dict())
    errors = sorted(validator.iter_errors(payload), key=lambda e: list(e.absolute_path))
    if errors:
        joined = "; ".join(
            f"{'.'.join(map(str, e.absolute_path)) or '<root>'}: {e.message}" for e in errors
        )
        raise SchemaViolationError(joined)


# ---------------------------------------------------------------------------
# Loaders -------------------------------------------------------------------
# ---------------------------------------------------------------------------


def load_default() -> HedgeConfig:
    """Return the default :class:`HedgeConfig` matching the Rust defaults."""
    return HedgeConfig()


def load_from_str(raw: str) -> HedgeConfig:
    """Parse a YAML body, validate against the schema, and return a typed config."""
    parsed = yaml.safe_load(raw)
    if not isinstance(parsed, dict):
        raise SchemaViolationError(f"top-level config must be a mapping, got {type(parsed).__name__}")
    _validate_schema(parsed)
    try:
        return HedgeConfig.model_validate(parsed)
    except InvariantViolationError:
        raise
    except ValidationError as exc:
        # Pydantic-level errors that JSON Schema didn't catch (e.g. enum case).
        raise SchemaViolationError(str(exc)) from exc


def load_from_path(path: str | Path) -> HedgeConfig:
    """Load and validate a :class:`HedgeConfig` from disk."""
    return load_from_str(Path(path).read_text(encoding="utf-8"))


def load_or_default(path: str | Path | None) -> HedgeConfig:
    """Convenience: load from ``path`` if given, else return defaults."""
    if path is None:
        return load_default()
    return load_from_path(path)


def fail_closed(err: ConfigError) -> "Any":
    """Print a ``cfg.error`` line to stderr and exit the process with code 2.

    Mirrors the Rust ``hedge_config::loader::fail_closed`` helper. Returning
    ``Any`` documents that the function does not return (``NoReturn``); we
    use a ``sys.exit`` rather than ``raise SystemExit`` to make the exit
    explicit.
    """
    print(f"cfg.error: {err}", file=sys.stderr)
    sys.exit(2)


__all__ = [
    "AiConfig",
    "BrokerConfig",
    "BrokerId",
    "CapitalConfig",
    "ConfigError",
    "DegradedModeConfig",
    "GovernanceConfig",
    "HedgeConfig",
    "InvariantViolationError",
    "ObservabilityConfig",
    "OllamaConfig",
    "OllamaModelConfig",
    "OllamaRole",
    "PostTargetPolicy",
    "PsychologyThresholds",
    "RankingFactorsConfig",
    "RetentionConfig",
    "RiskConfig",
    "SchemaViolationError",
    "SessionConfig",
    "TraderPsychologyConfig",
    "UiConfig",
    "WarModeConfig",
    "fail_closed",
    "load_default",
    "load_from_path",
    "load_from_str",
    "load_or_default",
    "schema_dict",
]
