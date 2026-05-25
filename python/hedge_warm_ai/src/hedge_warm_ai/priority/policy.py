"""Priority-assignment policy used by :class:`SymbolPriorityEngine`.

The engine's responsibility (R14.1) is to assign each tracked symbol
**exactly** one tier. *How* the tier is chosen from the available
inputs is a strategy decision; the engine takes a
:class:`PriorityPolicy` so the policy can evolve without disturbing
the totality invariant or the edge-emission semantics.

Inputs (per the task brief)
---------------------------

* **Trader intent** — ``trader.intent.priority`` (R20.8). Wins per the
  Authority_Hierarchy (R21): a trader-issued tier overrides AI
  recommendations until the next trader intent flips it.
* **Regime** — ``ai.regime.changed``. The current regime adjusts the
  baseline tier (e.g. ``Panic`` and ``LiquidityCrisis`` push every
  symbol toward higher attention; ``LowParticipation`` pulls them
  down).
* **News** — ``ai.news.impact.<sym>``. High-impact, high-magnitude
  news on a symbol pushes that symbol's baseline upward.

The :class:`DefaultPriorityPolicy` below is intentionally simple and
deterministic so the property-based test in task 23.2 can fuzz it
without a model under it. The engine itself does not depend on the
policy's internal logic — it only relies on the policy returning a
single :class:`PriorityTier` per ``(symbol, inputs)`` pair.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final, Protocol

from ..schemas.ai_news_impact import NewsImpact
from ..schemas.ai_priority_changed import PriorityTier
from ..schemas.ai_regime_changed import Regime
from ..schemas.trader_intent_priority import TraderIntentPriority

# ---------------------------------------------------------------------------
# Inputs --------------------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PriorityInputs:
    """Snapshot of the inputs the policy consumes for one symbol.

    Field semantics
    ---------------

    * ``trader_intent`` — most-recent :class:`TraderIntentPriority` for
      the symbol (or ``None`` if the trader has never set a tier).
    * ``regime`` — most-recent :class:`RegimeChanged.to`. Regime is
      global (not per-symbol) but it is part of every symbol's input
      tuple because every symbol's tier depends on it.
    * ``news`` — most-recent :class:`NewsImpact` for this symbol (or
      ``None``).
    * ``baseline`` — fall-through tier used when no other input
      applies. Defaults to ``"P3"``.
    """

    trader_intent: TraderIntentPriority | None = None
    regime: Regime | None = None
    news: NewsImpact | None = None
    baseline: PriorityTier = "P3"


# ---------------------------------------------------------------------------
# Policy protocol -----------------------------------------------------------
# ---------------------------------------------------------------------------


class PriorityPolicy(Protocol):
    """Strategy interface for ``inputs → PriorityTier``.

    Implementations must be **deterministic**: given the same
    :class:`PriorityInputs` they must return the same
    :class:`PriorityTier`. Determinism keeps the edge-emission
    semantics testable — every observed tier change corresponds to a
    distinct input tuple.
    """

    def assign(self, *, symbol: str, inputs: PriorityInputs) -> PriorityTier: ...


# ---------------------------------------------------------------------------
# Default policy ------------------------------------------------------------
# ---------------------------------------------------------------------------

#: Tiers ordered from most-resource (P1) to least-resource (P4).
_TIER_ORDER: Final[tuple[PriorityTier, ...]] = ("P1", "P2", "P3", "P4")
_TIER_INDEX: Final[dict[PriorityTier, int]] = {t: i for i, t in enumerate(_TIER_ORDER)}


def _bump_up(tier: PriorityTier, steps: int = 1) -> PriorityTier:
    """Move ``tier`` toward P1 by ``steps`` (clamped at P1)."""
    new_index = max(0, _TIER_INDEX[tier] - max(0, steps))
    return _TIER_ORDER[new_index]


def _bump_down(tier: PriorityTier, steps: int = 1) -> PriorityTier:
    """Move ``tier`` toward P4 by ``steps`` (clamped at P4)."""
    new_index = min(len(_TIER_ORDER) - 1, _TIER_INDEX[tier] + max(0, steps))
    return _TIER_ORDER[new_index]


@dataclass(frozen=True, slots=True)
class DefaultPriorityPolicy:
    """Reference :class:`PriorityPolicy`.

    Decision order (highest-precedence first):

    1. **Trader intent** wins outright (R21 Authority_Hierarchy).
       Returns ``inputs.trader_intent.to``.
    2. Otherwise start from ``inputs.baseline``.
    3. **Regime adjustment** (per :data:`regime_bumps`).
    4. **News adjustment**: if ``inputs.news`` exists and exceeds
       configured thresholds, bump up by one tier; combined
       sentiment-driven boost is capped at one tier per news event.

    The thresholds and regime bumps are dataclass fields so callers
    can override the policy without subclassing.
    """

    #: Map ``regime → tier-bump steps``. Positive bumps move toward
    #: P1 (more resources); negative bumps move toward P4. Regimes
    #: not present in the map produce no bump.
    regime_bumps: dict[Regime, int] = field(
        default_factory=lambda: {
            "Panic": 2,
            "HighVolatility": 1,
            "NewsDriven": 1,
            "LiquidityCrisis": 2,
            "Trending": 0,
            "Sideways": 0,
            "LowParticipation": -1,
        }
    )
    #: Minimum ``impact_magnitude`` that promotes a symbol by one tier.
    news_impact_threshold: float = 0.5
    #: Minimum absolute ``sentiment`` that promotes a symbol by one
    #: tier (orthogonal to ``news_impact_threshold``; either suffices).
    news_sentiment_threshold: float = 0.7

    def assign(self, *, symbol: str, inputs: PriorityInputs) -> PriorityTier:
        # 1) Trader override wins (Authority_Hierarchy).
        if inputs.trader_intent is not None and inputs.trader_intent.symbol == symbol:
            return inputs.trader_intent.to

        # 2) Start at the baseline.
        tier: PriorityTier = inputs.baseline

        # 3) Regime adjustment.
        if inputs.regime is not None:
            tier = self._apply_steps(tier, self.regime_bumps.get(inputs.regime, 0))

        # 4) News adjustment (capped at one step regardless of which
        #    threshold tripped, to avoid double-counting overlapping
        #    sentiment + magnitude conditions).
        if inputs.news is not None and inputs.news.symbol == symbol:
            if (
                inputs.news.impact_magnitude >= self.news_impact_threshold
                or abs(inputs.news.sentiment) >= self.news_sentiment_threshold
            ):
                tier = _bump_up(tier, 1)

        return tier

    @staticmethod
    def _apply_steps(tier: PriorityTier, steps: int) -> PriorityTier:
        if steps > 0:
            return _bump_up(tier, steps)
        if steps < 0:
            return _bump_down(tier, -steps)
        return tier


__all__ = [
    "DefaultPriorityPolicy",
    "PriorityInputs",
    "PriorityPolicy",
]
