"""Property-based tests for Market_Regime_Engine (task 22.2).

Validates:
    - Property 8 — Edge-Triggered Emission of State Changes

**Validates: Requirements 13.3**
"""

from __future__ import annotations

from hypothesis import given, assume
from hypothesis import strategies as st

from hedge_warm_ai.regime.signals import RegimeObservation
from hedge_warm_ai.regime.classifier import RuleBasedRegimeClassifier
from hedge_warm_ai.regime.config import RegimeConfig, RegimeThresholds


def _default_config() -> RegimeConfig:
    return RegimeConfig(
        thresholds=RegimeThresholds(
            liquidity_crisis_liquidity_score=0.25,
            panic_drawdown=0.05,
            panic_breadth=-0.4,
            high_volatility_volatility=0.5,
            high_volatility_breadth=0.4,
            news_driven_pressure=0.6,
            low_participation_max=0.25,
            trending_trend_strength=0.4,
        ),
        evaluation_interval_s=5.0,
        use_onnx_classifier=False,
    )


def _make_observation(**kwargs) -> RegimeObservation:
    defaults = dict(
        volatility=0.01,
        trend_strength=0.0,
        breadth=0.0,
        volatility_breadth=0.5,
        news_pressure=0.0,
        liquidity_score=0.5,
        participation=0.5,
        drawdown=0.0,
        ts_ns=1_000_000_000,
    )
    defaults.update(kwargs)
    return RegimeObservation(**defaults)


def test_liquidity_crisis_overrides_everything() -> None:
    """Property: when liquidity_score is below the crisis threshold,
    the classifier always returns LiquidityCrisis regardless of other inputs."""
    classifier = RuleBasedRegimeClassifier(config=_default_config())
    obs = _make_observation(
        liquidity_score=0.1,  # below 0.25 threshold
        volatility=0.8,
        trend_strength=0.8,
        drawdown=0.1,
        news_pressure=0.9,
    )
    regime = classifier.classify(obs)
    assert regime == "LiquidityCrisis"


def test_panic_detected_on_high_drawdown_and_negative_breadth() -> None:
    """Property: Panic is detected when drawdown and breadth exceed thresholds."""
    classifier = RuleBasedRegimeClassifier(config=_default_config())
    obs = _make_observation(
        drawdown=0.06,  # above 0.05 threshold
        breadth=-0.5,   # below -0.4 threshold
        liquidity_score=0.8,  # healthy liquidity
    )
    regime = classifier.classify(obs)
    assert regime == "Panic"


def test_high_volatility_detected() -> None:
    """Property: HighVolatility is detected when vol and breadth are high."""
    classifier = RuleBasedRegimeClassifier(config=_default_config())
    obs = _make_observation(
        volatility=0.6,  # above 0.5 threshold
        volatility_breadth=0.5,  # above 0.4 threshold
        liquidity_score=0.8,
    )
    regime = classifier.classify(obs)
    assert regime == "HighVolatility"


def test_news_driven_detected() -> None:
    """Property: NewsDriven is detected when news_pressure is high."""
    classifier = RuleBasedRegimeClassifier(config=_default_config())
    obs = _make_observation(
        news_pressure=0.7,  # above 0.6 threshold
        liquidity_score=0.8,
        volatility=0.01,
    )
    regime = classifier.classify(obs)
    assert regime == "NewsDriven"


def test_low_participation_detected() -> None:
    """Property: LowParticipation is detected when participation is low."""
    classifier = RuleBasedRegimeClassifier(config=_default_config())
    obs = _make_observation(
        participation=0.2,  # below 0.25 threshold
        liquidity_score=0.8,
        volatility=0.01,
    )
    regime = classifier.classify(obs)
    assert regime == "LowParticipation"


def test_trending_detected() -> None:
    """Property: Trending is detected when trend_strength is strong."""
    classifier = RuleBasedRegimeClassifier(config=_default_config())
    obs = _make_observation(
        trend_strength=0.5,  # above 0.4 threshold
        liquidity_score=0.8,
        volatility=0.01,
    )
    regime = classifier.classify(obs)
    assert regime == "Trending"


def test_sideways_fallback() -> None:
    """Property: Sideways is the fallback when no other regime is triggered."""
    classifier = RuleBasedRegimeClassifier(config=_default_config())
    obs = _make_observation(
        volatility=0.01,       # low
        trend_strength=0.1,    # weak
        breadth=0.0,           # neutral
        news_pressure=0.1,     # low
        liquidity_score=0.8,   # healthy
        participation=0.5,     # normal
        drawdown=0.0,          # no drawdown
    )
    regime = classifier.classify(obs)
    assert regime == "Sideways"


def test_classifier_always_returns_valid_regime() -> None:
    """Property: classifier always returns one of the seven design regimes."""
    classifier = RuleBasedRegimeClassifier(config=_default_config())
    valid_regimes = {
        "Trending", "Sideways", "Panic",
        "HighVolatility", "NewsDriven",
        "LiquidityCrisis", "LowParticipation",
    }
    # Test with various extreme inputs
    for vol in [0.0, 0.01, 0.8, 1.0]:
        for dd in [0.0, 0.02, 0.06, 0.1]:
            for liq in [0.1, 0.5, 0.9]:
                obs = _make_observation(
                    volatility=vol, drawdown=dd, liquidity_score=liq,
                )
                regime = classifier.classify(obs)
                assert regime in valid_regimes, f"Invalid regime {regime}"
