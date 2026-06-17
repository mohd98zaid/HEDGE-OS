"""Property-based tests for AI_Shadow_Mode (task 29.2).

Validates:
    - ShadowFilter: shadow=True payloads are blocked, shadow=False pass
    - ShadowSnapshot: is_shadowed correctness
    - ShadowedOutput: shadow flag enforcement
    - GovernanceObserver forwarding

**Validates: Requirements 23.1, 23.2, 23.3**
"""

from __future__ import annotations

import asyncio
from typing import Mapping

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from hedge_warm_ai.shadow.filter import ShadowFilter, is_payload_shadowed, passes_ui_filter
from hedge_warm_ai.shadow.state import ShadowSnapshot, ShadowedOutput, ShadowKind, EMPTY_SHADOW_SNAPSHOT
from hedge_warm_ai.shadow.engine import ShadowModeService, _is_marked_shadow
from hedge_warm_ai.shadow.config import ShadowModeConfig
from hedge_warm_ai.shadow.governance_observer import InMemoryGovernanceObserver


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

def arb_shadow_payload() -> st.SearchStrategy[dict]:
    return st.fixed_dictionaries({
        "shadow": st.booleans(),
        "value": st.floats(min_value=0.0, max_value=1.0),
    })


# ---------------------------------------------------------------------------
# is_payload_shadowed
# ---------------------------------------------------------------------------

@given(payload=arb_shadow_payload())
def test_is_payload_shadowed_matches_field(payload: dict) -> None:
    """Property: is_payload_shadowed returns the 'shadow' field value."""
    result = is_payload_shadowed(payload)
    assert result == bool(payload.get("shadow", False))


def test_non_mapping_payload_not_shadowed() -> None:
    """Property: non-mapping payloads are never shadowed."""
    assert not is_payload_shadowed("string")
    assert not is_payload_shadowed(42)
    assert not is_payload_shadowed(None)
    assert not is_payload_shadowed([1, 2, 3])


# ---------------------------------------------------------------------------
# passes_ui_filter (inverse)
# ---------------------------------------------------------------------------

@given(payload=arb_shadow_payload())
def test_passes_ui_filter_inverse_of_shadowed(payload: dict) -> None:
    """Property: passes_ui_filter is the logical inverse of is_payload_shadowed."""
    assert passes_ui_filter(payload) == (not is_payload_shadowed(payload))


# ---------------------------------------------------------------------------
# ShadowFilter callable
# ---------------------------------------------------------------------------

def test_shadow_filter_blocks_shadow_true() -> None:
    """Property: ShadowFilter returns False for shadow=True payloads."""
    f = ShadowFilter(log_dropped=False)
    assert f({"shadow": True, "data": 1}) is False


def test_shadow_filter_passes_shadow_false() -> None:
    """Property: ShadowFilter returns True for shadow=False payloads."""
    f = ShadowFilter(log_dropped=False)
    assert f({"shadow": False, "data": 1}) is True


def test_shadow_filter_passes_no_shadow_field() -> None:
    """Property: ShadowFilter returns True when shadow field is absent."""
    f = ShadowFilter(log_dropped=False)
    assert f({"data": 1}) is True


# ---------------------------------------------------------------------------
# ShadowSnapshot
# ---------------------------------------------------------------------------

@given(components=st.frozensets(st.text(min_size=1, max_size=10), max_size=20))
def test_shadow_snapshot_is_shadowed(components: frozenset[str]) -> None:
    """Property: is_shadowed is True iff component is in the set."""
    snap = ShadowSnapshot(components=components, refreshed_at_ns=1000)
    for comp in components:
        assert snap.is_shadowed(comp) is True
    # Test a component not in the set
    assert snap.is_shadowed("NOT_IN_SET") is False


def test_empty_snapshot_has_no_shadowed() -> None:
    """Property: empty snapshot shadows nothing."""
    assert not EMPTY_SHADOW_SNAPSHOT.is_shadowed("anything")
    assert len(EMPTY_SHADOW_SNAPSHOT) == 0


# ---------------------------------------------------------------------------
# ShadowedOutput shadow flag enforcement
# ---------------------------------------------------------------------------

@given(
    shadow=st.booleans(),
    component=st.text(min_size=1, max_size=20),
)
def test_is_marked_shadow_checks_flag(shadow: bool, component: str) -> None:
    """Property: _is_marked_shadow reads the payload's shadow field."""
    output = ShadowedOutput(
        kind=ShadowKind.AI_RANK,
        component=component,
        payload={"shadow": shadow, "data": 1},
        ts_ns=1000,
    )
    assert _is_marked_shadow(output) == shadow


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
