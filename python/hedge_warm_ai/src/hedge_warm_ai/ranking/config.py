"""Configuration surface for the AI_Trade_Ranking_Engine (task 26.1).

The engine's tunables live in three groups:

1. **Cache namespace** — the Redis key prefix the interim WarmCache
   adaptor writes the latest per-symbol :class:`AiRank` to. Distinct
   from the Memory_RAG cache namespace so the two lanes stay
   separately observable. Default ``hedge.warm.rank``; full key is
   ``<namespace>.<symbol>``.
2. **Factor staleness window** — maximum permitted age (seconds) of a
   cached factor entry before the
   :class:`hedge_warm_ai.ranking.factors.RankingFactorProvider`
   substitutes the configured default. Bounds the ranking engine's
   exposure to a stalled producer (regime engine, news engine,
   psychology engine, Hot_Path-to-Warm_AI orderflow bridge).
3. **Ranking timeout** — wall-clock budget for one
   :meth:`AiTradeRankingEngine.rank` call. The ranking decision must
   complete within this budget; if a factor lookup or publish blows
   past it, the engine logs the breach via the existing
   :class:`hedge_warm_ai.onnx_runtime.LatencyTracer` so the
   ``obs.budget.breach.<stage>`` event fires (R17.5).

All three groups are pydantic models with ``extra="forbid"`` and
``validate_assignment=True``. This mirrors the discipline used by
:class:`hedge_warm_ai.config.HedgeConfig` so a misconfigured deployment
fails closed at construction time.

Nothing in this module is hardcoded; all defaults are exposed as named
constants and overridable via either:

* :meth:`RankingConfig.from_yaml` — load from a sibling YAML config
  alongside the main ``HedgeConfig`` (the same loader pattern used by
  :mod:`hedge_warm_ai.config`).
* :meth:`RankingConfig.from_hedge_config` — adapter that sources
  defaults from the canonical :class:`hedge_warm_ai.config.HedgeConfig`
  mirror (currently a passthrough until the Rust ``HedgeConfig``
  schema gains a ``ranking`` block in a follow-up task).
* Direct construction in tests.
"""

from __future__ import annotations

from pathlib import Path
from typing import Final

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeFloat,
    PositiveFloat,
    PositiveInt,
    ValidationError,
)

from ..config import HedgeConfig, SchemaViolationError
from .errors import RankingConfigError
from .warm_cache import (
    DEFAULT_RANK_CACHE_NAMESPACE,
    DEFAULT_RANK_CACHE_TTL_S,
)


# ---------------------------------------------------------------------------
# Defaults ------------------------------------------------------------------
# ---------------------------------------------------------------------------

#: Default ranking timeout in milliseconds. Aligned with R17.5
#: ("THE AI_Trade_Ranking_Engine SHALL produce a ranking decision
#: within 5 milliseconds at the 95th percentile") — we use the same
#: 5 ms as the per-call wall-clock budget. The latency tracer fires
#: ``obs.budget.breach.<stage>`` when this is exceeded.
DEFAULT_RANKING_TIMEOUT_MS: Final[float] = 5.0

#: Default factor staleness window in seconds. Five seconds is enough
#: to absorb a one-second sibling-engine restart but tight enough that
#: a stalled producer cannot feed stale factors into a fresh signal.
DEFAULT_FACTOR_STALENESS_WINDOW_S: Final[float] = 5.0


class _StrictModel(BaseModel):
    """All ranking config models forbid unknown fields and re-validate on assignment."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=False)


# ---------------------------------------------------------------------------
# Top-level RankingConfig ---------------------------------------------------
# ---------------------------------------------------------------------------


class RankingConfig(_StrictModel):
    """Bundle of every tunable surfaced by the AI_Trade_Ranking_Engine.

    The engine takes one of these at construction. Defaults match the
    reference values documented in the module docstring and are safe
    in dev; production deployments should override via
    :meth:`RankingConfig.from_yaml`.

    Attributes:
        rank_cache_namespace: Redis key namespace for the interim
            WarmCache adaptor. Default ``hedge.warm.rank``.
        rank_cache_ttl_s: TTL (seconds) for entries written to the
            interim WarmCache. Bounded so a stalled engine cannot
            leak stale ranks into the Risk_Engine forever.
        factor_staleness_window_s: Maximum permitted age of a cached
            factor entry before the provider substitutes the
            configured default. ``None`` disables the check (the
            cache's own TTL is then the only bound).
        ranking_timeout_ms: Wall-clock budget for one
            :meth:`AiTradeRankingEngine.rank` call. Defaults to
            :data:`DEFAULT_RANKING_TIMEOUT_MS` (5.0 ms, matching
            R17.5).
    """

    rank_cache_namespace: str = Field(
        default=DEFAULT_RANK_CACHE_NAMESPACE,
        min_length=1,
        description="Redis key namespace for the interim WarmCache adaptor.",
    )
    rank_cache_ttl_s: PositiveInt = Field(
        default=DEFAULT_RANK_CACHE_TTL_S,
        description="TTL (seconds) for entries written to the interim WarmCache.",
    )
    factor_staleness_window_s: NonNegativeFloat | None = Field(
        default=DEFAULT_FACTOR_STALENESS_WINDOW_S,
        description=(
            "Maximum permitted age (seconds) of a cached factor entry before "
            "the provider substitutes the configured default. ``None`` disables."
        ),
    )
    ranking_timeout_ms: PositiveFloat = Field(
        default=DEFAULT_RANKING_TIMEOUT_MS,
        description=(
            "Wall-clock budget for one rank call (ms). Aligned with R17.5 "
            "(p95 ≤ 5 ms)."
        ),
    )

    # ----- alternate constructors -----------------------------------------

    @classmethod
    def from_yaml(cls, raw: str) -> "RankingConfig":
        """Parse a YAML body into a :class:`RankingConfig`.

        The YAML shape mirrors the field names of this model. Unknown
        fields raise :class:`RankingConfigError` (extra=forbid) so a
        typo cannot silently fall back to the default.
        """
        try:
            parsed = yaml.safe_load(raw)
        except yaml.YAMLError as exc:
            raise RankingConfigError(f"invalid YAML: {exc}") from exc
        if parsed is None:
            return cls()
        if not isinstance(parsed, dict):
            raise RankingConfigError(
                f"top-level ranking config must be a mapping, got "
                f"{type(parsed).__name__}"
            )
        try:
            return cls.model_validate(parsed)
        except ValidationError as exc:
            raise RankingConfigError(str(exc)) from exc

    @classmethod
    def from_yaml_path(cls, path: str | Path) -> "RankingConfig":
        """Load a :class:`RankingConfig` from disk via the same loader."""
        return cls.from_yaml(Path(path).read_text(encoding="utf-8"))

    @classmethod
    def from_hedge_config(cls, hedge: HedgeConfig | None = None) -> "RankingConfig":
        """Adaptor: source defaults from a :class:`HedgeConfig` mirror.

        The Rust ``HedgeConfig`` schema does not yet carry a
        ``ranking`` block (the canonical schema lives in
        ``crates/hedge-config/schema.json`` and adding to it requires
        a separate task that touches the Rust side). Until that
        lands, this adaptor reads the supplied :class:`HedgeConfig`
        only to confirm config-load discipline and otherwise returns
        the defaults documented above. When the Rust crate gains a
        ``ranking`` block, this method will start populating
        ``rank_cache_namespace``, ``factor_staleness_window_s``, and
        ``ranking_timeout_ms`` from there without changing the
        engine's call site.

        The :attr:`hedge_warm_ai.config.HedgeConfig.ai.rank_p95_budget_ms`
        field is **read** when ``hedge`` is supplied so the ranking
        timeout matches the active Hot_Path budget exactly.

        Args:
            hedge: An already-loaded :class:`HedgeConfig`. ``None``
                returns defaults.

        Raises:
            SchemaViolationError: re-raised from the underlying loader
                if the supplied :class:`HedgeConfig` itself is invalid.
        """
        if hedge is None:
            return cls()
        if not isinstance(hedge, HedgeConfig):
            raise SchemaViolationError(
                "from_hedge_config expected a HedgeConfig instance; "
                f"got {type(hedge).__name__}"
            )
        # Bridge ``ai.rank_p95_budget_ms`` into the ranking timeout
        # so the surfaces stay coherent. ``rank_p95_budget_ms`` is an
        # ``NonNegativeInt`` — coerce to float to satisfy the
        # ``PositiveFloat`` field. Zero would fail the ``positive``
        # invariant, so fall back to the default in that edge case.
        budget = float(hedge.ai.rank_p95_budget_ms)
        if budget <= 0.0:
            budget = DEFAULT_RANKING_TIMEOUT_MS
        return cls(ranking_timeout_ms=budget)


__all__ = [
    "DEFAULT_FACTOR_STALENESS_WINDOW_S",
    "DEFAULT_RANK_CACHE_NAMESPACE",
    "DEFAULT_RANK_CACHE_TTL_S",
    "DEFAULT_RANKING_TIMEOUT_MS",
    "RankingConfig",
]
