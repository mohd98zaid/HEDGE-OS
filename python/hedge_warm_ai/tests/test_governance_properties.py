"""Property-based tests for AI_Governance_Engine (task 28.2).

Validates:
    - Payload confidence extraction bounds
    - Governance observer forwarding

**Validates: Requirements 23.1, 23.2, 23.3**
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from hedge_warm_ai.shadow.governance_observer import (
    InMemoryGovernanceObserver,
    _payload_confidence,
)
from hedge_warm_ai.shadow.state import ShadowedOutput, ShadowKind


# ---------------------------------------------------------------------------
# _payload_confidence properties
# ---------------------------------------------------------------------------

@given(score=st.floats(min_value=-1.0, max_value=2.0))
def test_payload_confidence_bounded_for_rank(score: float) -> None:
    """Property: confidence for AI_RANK is clamped to [0, 1]."""
    result = _payload_confidence("ai_rank", {"trade_confidence_score": score})
    assert result is not None
    assert 0.0 <= result <= 1.0


@given(score=st.floats(min_value=-1.0, max_value=2.0))
def test_payload_confidence_bounded_for_psych(score: float) -> None:
    """Property: confidence for AI_PSYCH_STABILITY is clamped to [0, 1]."""
    result = _payload_confidence("ai_psych_stability", {"score": score})
    assert result is not None
    assert 0.0 <= result <= 1.0


@given(magnitude=st.floats(min_value=-1.0, max_value=2.0))
def test_payload_confidence_bounded_for_news(magnitude: float) -> None:
    """Property: confidence for AI_NEWS_IMPACT is clamped to [0, 1]."""
    result = _payload_confidence("ai_news_impact", {"impact_magnitude": magnitude})
    assert result is not None
    assert 0.0 <= result <= 1.0


@given(kind=st.sampled_from(["ai_regime_changed", "ai_priority_changed", "other"]))
def test_payload_confidence_none_for_non_confidence_kinds(kind: str) -> None:
    """Property: non-confidence kinds return None."""
    result = _payload_confidence(kind, {"data": 1.0})
    assert result is None


def test_payload_confidence_none_for_non_numeric_payload() -> None:
    """Property: non-numeric payload values return None."""
    result = _payload_confidence("ai_rank", {"trade_confidence_score": "not_a_number"})
    assert result is None


def test_payload_confidence_zero_for_missing_key() -> None:
    """Property: missing key returns 0.0 (default)."""
    result = _payload_confidence("ai_rank", {})
    assert result == 0.0


# ---------------------------------------------------------------------------
# GovernanceObserver forwarding
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@given(
    num_outputs=st.integers(min_value=1, max_value=10),
)
@settings(max_examples=50)
async def test_governance_observer_captures_all_forwarded(num_outputs: int) -> None:
    """Property: InMemoryGovernanceObserver captures every forwarded output."""
    observer = InMemoryGovernanceObserver()
    for i in range(num_outputs):
        output = ShadowedOutput(
            kind=ShadowKind.AI_RANK,
            component="ranking",
            payload={"shadow": True, "score": float(i)},
            ts_ns=1000 + i,
        )
        await observer.forward(output)
    assert len(observer.forwarded) == num_outputs
