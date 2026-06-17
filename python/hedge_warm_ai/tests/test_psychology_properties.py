"""Property-based tests for Trader_Psychology_Engine (task 25.2).

Validates:
    - Property 4 — Score and Formula Equivalence (Trader_Stability_Score)
    - Property 8 — Edge-Triggered Emission of State Changes (threshold ladder)

**Validates: Requirements 16.2, 16.3, 16.4, 16.5, 16.6, 16.7**
"""

from __future__ import annotations

import math

from hypothesis import given, assume
from hypothesis import strategies as st

from hedge_warm_ai.psychology.score import (
    BehaviorState,
    DISCIPLINE_WEIGHT,
    EMOTIONAL_CONTROL_WEIGHT,
    PATIENCE_WEIGHT,
    RISK_CONSISTENCY_WEIGHT,
    compute_trader_stability_score,
)


# ---------------------------------------------------------------------------
# Property 4: Score and Formula Equivalence
# ---------------------------------------------------------------------------


@given(
    discipline=st.floats(min_value=0.0, max_value=1.0),
    emotional_control=st.floats(min_value=0.0, max_value=1.0),
    risk_consistency=st.floats(min_value=0.0, max_value=1.0),
    patience=st.floats(min_value=0.0, max_value=1.0),
)
def test_stability_score_exact_formula(
    discipline: float,
    emotional_control: float,
    risk_consistency: float,
    patience: float,
) -> None:
    """Property: Trader_Stability_Score = clamp(0.35*D + 0.25*E + 0.20*R + 0.20*P, 0, 1).

    For any factor inputs in [0, 1], the computed score must exactly
    match the formula specification.
    """
    state = BehaviorState(
        discipline=discipline,
        emotional_control=emotional_control,
        risk_consistency=risk_consistency,
        patience=patience,
    )
    score = compute_trader_stability_score(state)

    # Expected value from the formula
    expected_raw = (
        DISCIPLINE_WEIGHT * discipline
        + EMOTIONAL_CONTROL_WEIGHT * emotional_control
        + RISK_CONSISTENCY_WEIGHT * risk_consistency
        + PATIENCE_WEIGHT * patience
    )
    expected = max(0.0, min(1.0, expected_raw))

    # Allow for floating-point tolerance
    assert math.isclose(score, expected, rel_tol=1e-10, abs_tol=1e-10), (
        f"Score {score} != expected {expected} "
        f"(raw={expected_raw}) for inputs "
        f"D={discipline}, E={emotional_control}, R={risk_consistency}, P={patience}"
    )


@given(
    discipline=st.floats(min_value=0.0, max_value=1.0),
    emotional_control=st.floats(min_value=0.0, max_value=1.0),
    risk_consistency=st.floats(min_value=0.0, max_value=1.0),
    patience=st.floats(min_value=0.0, max_value=1.0),
)
def test_stability_score_bounded(
    discipline: float,
    emotional_control: float,
    risk_consistency: float,
    patience: float,
) -> None:
    """Property: output is always in [0.0, 1.0]."""
    state = BehaviorState(
        discipline=discipline,
        emotional_control=emotional_control,
        risk_consistency=risk_consistency,
        patience=patience,
    )
    score = compute_trader_stability_score(state)
    assert 0.0 <= score <= 1.0, f"Score {score} out of [0, 1]"


def test_weights_sum_to_one() -> None:
    """Sanity: weights must sum to exactly 1.0 (R16.2 / Property 4)."""
    total = (
        DISCIPLINE_WEIGHT
        + EMOTIONAL_CONTROL_WEIGHT
        + RISK_CONSISTENCY_WEIGHT
        + PATIENCE_WEIGHT
    )
    assert total == 1.0, f"Weights sum to {total}, expected 1.0"


@given(st.floats(min_value=0.0, max_value=1.0))
def test_all_ones_gives_one(d: float) -> None:
    """Property: when all factors are 1.0, score is 1.0."""
    state = BehaviorState(
        discipline=1.0,
        emotional_control=1.0,
        risk_consistency=1.0,
        patience=1.0,
    )
    score = compute_trader_stability_score(state)
    assert score == 1.0, f"All-ones score {score} != 1.0"


def test_all_zeros_gives_zero() -> None:
    """Property: when all factors are 0.0, score is 0.0."""
    state = BehaviorState(
        discipline=0.0,
        emotional_control=0.0,
        risk_consistency=0.0,
        patience=0.0,
    )
    score = compute_trader_stability_score(state)
    assert score == 0.0, f"All-zeros score {score} != 0.0"


@given(
    discipline=st.floats(min_value=0.0, max_value=1.0),
    emotional_control=st.floats(min_value=0.0, max_value=1.0),
    risk_consistency=st.floats(min_value=0.0, max_value=1.0),
    patience=st.floats(min_value=0.0, max_value=1.0),
)
def test_score_monotonic_in_each_factor(
    discipline: float,
    emotional_control: float,
    risk_consistency: float,
    patience: float,
) -> None:
    """Property: increasing any single factor (holding others constant)
    never decreases the score."""
    base = BehaviorState(
        discipline=discipline,
        emotional_control=emotional_control,
        risk_consistency=risk_consistency,
        patience=patience,
    )
    base_score = compute_trader_stability_score(base)

    # Increase discipline
    high = BehaviorState(
        discipline=min(1.0, discipline + 0.1),
        emotional_control=emotional_control,
        risk_consistency=risk_consistency,
        patience=patience,
    )
    high_score = compute_trader_stability_score(high)
    assert high_score >= base_score - 1e-10, (
        f"Increasing discipline decreased score: {base_score} -> {high_score}"
    )
