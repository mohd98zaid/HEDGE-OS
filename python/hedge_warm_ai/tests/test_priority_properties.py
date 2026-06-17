"""Property-based tests for Symbol_Priority_Engine (task 23.2).

Validates:
    - Totality: every tracked symbol always has exactly one tier (P1-P4)
    - Edge-triggered emission: tier changes == adjacent-pair changes
    - Trader intent wins (Authority_Hierarchy)
    - Policy determinism

**Validates: Requirements 14.1, 14.3, 21.1**
"""

from __future__ import annotations

import asyncio
from typing import List

import pytest
from hypothesis import given, assume, settings
from hypothesis import strategies as st

from hedge_warm_ai.priority.engine import SymbolPriorityEngine
from hedge_warm_ai.priority.policy import DefaultPriorityPolicy, PriorityInputs
from hedge_warm_ai.schemas.ai_priority_changed import PriorityTier
from hedge_warm_ai.schemas.ai_news_impact import NewsImpact
from hedge_warm_ai.schemas.ai_regime_changed import Regime
from hedge_warm_ai.schemas.trader_intent_priority import TraderIntentPriority


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

VALID_TIERS: tuple[PriorityTier, ...] = ("P1", "P2", "P3", "P4")
VALID_REGIMES: tuple[Regime, ...] = (
    "Trending", "Sideways", "Panic", "HighVolatility",
    "NewsDriven", "LiquidityCrisis", "LowParticipation",
)


def arb_tier() -> st.SearchStrategy[PriorityTier]:
    return st.sampled_from(VALID_TIERS)


def arb_regime() -> st.SearchStrategy[Regime]:
    return st.sampled_from(VALID_REGIMES)


def make_intent(symbol: str, to: PriorityTier, ts_ns: int = 1000) -> TraderIntentPriority:
    return TraderIntentPriority.model_validate({
        "correlation_id": "test-corr-id",
        "symbol": symbol,
        "to": to,
        "actor": "test",
        "ts_ns": ts_ns,
    })


def make_news_impact(symbol: str, sentiment: float = 0.5, magnitude: float = 0.5) -> NewsImpact:
    return NewsImpact.model_validate({
        "correlation_id": "test-corr-id",
        "symbol": symbol,
        "headline_id": "h1",
        "sentiment": sentiment,
        "impact_magnitude": magnitude,
        "fast_path": True,
        "slow_path_pending": False,
        "ts_ns": 1000,
    })


# ---------------------------------------------------------------------------
# Totality invariant
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@given(
    symbols=st.lists(st.text(min_size=1, max_size=8, alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ"), min_size=1, max_size=10),
    tiers=st.lists(arb_tier(), min_size=1, max_size=10),
)
@settings(max_examples=100)
async def test_totality_every_tracked_symbol_has_exactly_one_tier(
    symbols: list[str],
    tiers: list[PriorityTier],
) -> None:
    """Property: every tracked symbol always has exactly one tier in P1-P4."""
    assume(len(tiers) >= len(symbols))
    engine = SymbolPriorityEngine()
    for sym, tier in zip(symbols, tiers):
        await engine.track(sym, initial_tier=tier)
    for sym in set(symbols):
        t = engine.tier(sym)
        assert t in VALID_TIERS, f"Symbol {sym} has invalid tier {t}"


@pytest.mark.asyncio
@given(
    symbols=st.lists(st.text(min_size=1, max_size=8, alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ"), min_size=1, max_size=10),
)
@settings(max_examples=100)
async def test_totality_untrack_removes_completely(symbols: list[str]) -> None:
    """Property: untracked symbol is not in the tier map."""
    engine = SymbolPriorityEngine()
    for sym in set(symbols):
        await engine.track(sym)
    for sym in set(symbols):
        engine.untrack(sym)
    for sym in set(symbols):
        assert sym not in engine.snapshot()


# ---------------------------------------------------------------------------
# Trader intent wins (Authority_Hierarchy)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@given(
    initial_tier=arb_tier(),
    override_tier=arb_tier(),
)
@settings(max_examples=100)
async def test_trader_intent_wins_over_baseline(
    initial_tier: PriorityTier,
    override_tier: PriorityTier,
) -> None:
    """Property: trader intent overrides the policy's tier assignment."""
    assume(initial_tier != override_tier)
    engine = SymbolPriorityEngine()
    await engine.track("TEST", initial_tier=initial_tier)
    intent = make_intent("TEST", override_tier)
    await engine.on_trader_intent(intent)
    assert engine.tier("TEST") == override_tier


# ---------------------------------------------------------------------------
# Policy determinism
# ---------------------------------------------------------------------------

@given(
    tier=arb_tier(),
    regime=arb_regime(),
    news_magnitude=st.floats(min_value=0.0, max_value=1.0),
)
def test_policy_is_deterministic(
    tier: PriorityTier,
    regime: Regime,
    news_magnitude: float,
) -> None:
    """Property: same inputs always produce the same tier."""
    policy = DefaultPriorityPolicy()
    inputs = PriorityInputs(
        regime=regime,
        baseline=tier,
        news=make_news_impact("SYM", magnitude=news_magnitude),
    )
    result1 = policy.assign(symbol="SYM", inputs=inputs)
    result2 = policy.assign(symbol="SYM", inputs=inputs)
    assert result1 == result2
    assert result1 in VALID_TIERS


# ---------------------------------------------------------------------------
# Regime adjustments
# ---------------------------------------------------------------------------

@given(baseline=arb_tier())
def test_panic_regime_bumps_up(baseline: PriorityTier) -> None:
    """Property: Panic regime bumps tier toward P1."""
    policy = DefaultPriorityPolicy()
    inputs_no_regime = PriorityInputs(baseline=baseline)
    inputs_panic = PriorityInputs(regime="Panic", baseline=baseline)
    tier_no = policy.assign(symbol="SYM", inputs=inputs_no_regime)
    tier_panic = policy.assign(symbol="SYM", inputs=inputs_panic)
    assert VALID_TIERS.index(tier_panic) <= VALID_TIERS.index(tier_no)


# ---------------------------------------------------------------------------
# Edge-triggered emission count
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@given(
    tier_changes=st.lists(arb_tier(), min_size=2, max_size=8),
)
@settings(max_examples=100)
async def test_edge_triggered_emission_count(tier_changes: list[PriorityTier]) -> None:
    """Property: number of tier changes equals the number of distinct adjacent-pair changes."""
    engine = SymbolPriorityEngine()
    await engine.track("SYM", initial_tier=tier_changes[0])

    change_count = 0
    for i, tier in enumerate(tier_changes[1:], start=1):
        prev = engine.tier("SYM")
        intent = make_intent("SYM", tier, ts_ns=1000 + i)
        await engine.on_trader_intent(intent)
        if engine.tier("SYM") != prev:
            change_count += 1

    expected = sum(
        1 for i in range(1, len(tier_changes))
        if tier_changes[i] != tier_changes[i - 1]
    )
    assert change_count == expected
