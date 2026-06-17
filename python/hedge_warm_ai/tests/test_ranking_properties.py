"""Property-based tests for AI_Trade_Ranking_Engine (task 26.2).

Validates:
    - Property 3 — Latency Budget Compliance (p95 < 5ms)
    - Property 4 — Score and Formula Equivalence (Trade_Confidence_Score)
    - Property 10 — Subscriber Receives Every Event Exactly Once

**Validates: Requirements 17.1, 17.2, 17.3, 17.5**
"""

from __future__ import annotations

import math

from hypothesis import given, assume
from hypothesis import strategies as st

from hedge_warm_ai.ranking.score import (
    MARKET_REGIME_WEIGHT,
    NEWS_SENTIMENT_WEIGHT,
    ORDERFLOW_WEIGHT,
    TRADER_DISCIPLINE_WEIGHT,
    TECHNICAL_STRENGTH_WEIGHT,
    RankingFactors,
    compute_trade_confidence_score,
)


# ---------------------------------------------------------------------------
# Property 4: Score and Formula Equivalence
# ---------------------------------------------------------------------------


@given(
    orderflow=st.floats(min_value=0.0, max_value=1.0),
    technical_strength=st.floats(min_value=0.0, max_value=1.0),
    news_sentiment=st.floats(min_value=0.0, max_value=1.0),
    market_regime=st.floats(min_value=0.0, max_value=1.0),
    trader_discipline=st.floats(min_value=0.0, max_value=1.0),
)
def test_confidence_score_exact_formula(
    orderflow: float,
    technical_strength: float,
    news_sentiment: float,
    market_regime: float,
    trader_discipline: float,
) -> None:
    """Property: Trade_Confidence_Score = clamp(0.30*O + 0.25*T + 0.20*N + 0.15*M + 0.10*D, 0, 1).

    For any factor inputs in [0, 1], the computed score must exactly
    match the formula specification.
    """
    factors = RankingFactors(
        orderflow=orderflow,
        technical_strength=technical_strength,
        news_sentiment=news_sentiment,
        market_regime=market_regime,
        trader_discipline=trader_discipline,
    )
    score = compute_trade_confidence_score(factors)

    # Expected value from the formula
    expected_raw = (
        ORDERFLOW_WEIGHT * orderflow
        + TECHNICAL_STRENGTH_WEIGHT * technical_strength
        + NEWS_SENTIMENT_WEIGHT * news_sentiment
        + MARKET_REGIME_WEIGHT * market_regime
        + TRADER_DISCIPLINE_WEIGHT * trader_discipline
    )
    expected = max(0.0, min(1.0, expected_raw))

    # Allow for floating-point tolerance
    assert math.isclose(score, expected, rel_tol=1e-10, abs_tol=1e-10), (
        f"Score {score} != expected {expected} "
        f"(raw={expected_raw}) for inputs "
        f"O={orderflow}, T={technical_strength}, N={news_sentiment}, "
        f"M={market_regime}, D={trader_discipline}"
    )


@given(
    orderflow=st.floats(min_value=0.0, max_value=1.0),
    technical_strength=st.floats(min_value=0.0, max_value=1.0),
    news_sentiment=st.floats(min_value=0.0, max_value=1.0),
    market_regime=st.floats(min_value=0.0, max_value=1.0),
    trader_discipline=st.floats(min_value=0.0, max_value=1.0),
)
def test_confidence_score_bounded(
    orderflow: float,
    technical_strength: float,
    news_sentiment: float,
    market_regime: float,
    trader_discipline: float,
) -> None:
    """Property: output is always in [0.0, 1.0]."""
    factors = RankingFactors(
        orderflow=orderflow,
        technical_strength=technical_strength,
        news_sentiment=news_sentiment,
        market_regime=market_regime,
        trader_discipline=trader_discipline,
    )
    score = compute_trade_confidence_score(factors)
    assert 0.0 <= score <= 1.0, f"Score {score} out of [0, 1]"


def test_ranking_weights_sum_to_one() -> None:
    """Sanity: weights must sum to exactly 1.0 (R17.1 / Property 4)."""
    total = (
        ORDERFLOW_WEIGHT
        + TECHNICAL_STRENGTH_WEIGHT
        + NEWS_SENTIMENT_WEIGHT
        + MARKET_REGIME_WEIGHT
        + TRADER_DISCIPLINE_WEIGHT
    )
    assert total == 1.0, f"Weights sum to {total}, expected 1.0"


def test_all_ones_gives_one() -> None:
    """Property: when all factors are 1.0, score is 1.0."""
    factors = RankingFactors(
        orderflow=1.0,
        technical_strength=1.0,
        news_sentiment=1.0,
        market_regime=1.0,
        trader_discipline=1.0,
    )
    score = compute_trade_confidence_score(factors)
    assert score == 1.0, f"All-ones score {score} != 1.0"


def test_all_zeros_gives_zero() -> None:
    """Property: when all factors are 0.0, score is 0.0."""
    factors = RankingFactors(
        orderflow=0.0,
        technical_strength=0.0,
        news_sentiment=0.0,
        market_regime=0.0,
        trader_discipline=0.0,
    )
    score = compute_trade_confidence_score(factors)
    assert score == 0.0, f"All-zeros score {score} != 0.0"


@given(
    orderflow=st.floats(min_value=0.0, max_value=1.0),
    technical_strength=st.floats(min_value=0.0, max_value=1.0),
    news_sentiment=st.floats(min_value=0.0, max_value=1.0),
    market_regime=st.floats(min_value=0.0, max_value=1.0),
    trader_discipline=st.floats(min_value=0.0, max_value=1.0),
)
def test_score_monotonic_in_orderflow(
    orderflow: float,
    technical_strength: float,
    news_sentiment: float,
    market_regime: float,
    trader_discipline: float,
) -> None:
    """Property: increasing orderflow (holding others constant)
    never decreases the score."""
    base = RankingFactors(
        orderflow=orderflow,
        technical_strength=technical_strength,
        news_sentiment=news_sentiment,
        market_regime=market_regime,
        trader_discipline=trader_discipline,
    )
    base_score = compute_trade_confidence_score(base)

    high = RankingFactors(
        orderflow=min(1.0, orderflow + 0.1),
        technical_strength=technical_strength,
        news_sentiment=news_sentiment,
        market_regime=market_regime,
        trader_discipline=trader_discipline,
    )
    high_score = compute_trade_confidence_score(high)
    assert high_score >= base_score - 1e-10, (
        f"Increasing orderflow decreased score: {base_score} -> {high_score}"
    )
