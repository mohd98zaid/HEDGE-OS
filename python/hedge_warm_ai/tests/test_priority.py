"""Unit tests for :mod:`hedge_warm_ai.priority` (task 23.1).

These cover the core behaviours required by R14:

* Totality (R14.1): every tracked symbol carries exactly one tier at
  all times.
* Allocation table (R14.2): every tier has a row, the table is
  read-only, and overrides merge correctly.
* Edge-triggered emission (R14.3): exactly one
  ``ai.priority.changed.<sym>`` event is emitted per adjacent-pair
  change in the per-symbol observation stream; the payload carries
  ``from`` and ``to``.
* Authority_Hierarchy (R21): trader intents win over AI inputs.

Property-based tests of the same invariants live in task 23.2 and
are *not* in scope for this task.
"""

from __future__ import annotations

import json

import pytest

from hedge_warm_ai.priority import (
    DEFAULT_ALLOCATION_TABLE,
    DefaultPriorityPolicy,
    InMemoryPriorityChangedPublisher,
    NatsPriorityChangedPublisher,
    PRIORITY_TIERS,
    PriorityAllocation,
    PriorityAllocationTable,
    PriorityInputs,
    SymbolPriorityEngine,
    UnknownSymbolError,
    load_priority_allocation_table,
    priority_subject,
)
from hedge_warm_ai.schemas import (
    NewsImpact,
    PriorityChanged,
    RegimeChanged,
    TraderIntentPriority,
)


# --------------------------------------------------------------------------- #
# Allocation table (R14.2).
# --------------------------------------------------------------------------- #


def test_default_allocation_table_covers_every_tier() -> None:
    for tier in PRIORITY_TIERS:
        row = DEFAULT_ALLOCATION_TABLE[tier]
        assert isinstance(row, PriorityAllocation)
        # Both budgets must lie in [0.0, 1.0].
        assert 0.0 <= row.cpu_budget <= 1.0
        assert 0.0 <= row.ai_inference_budget <= 1.0
        assert row.scan_hz >= 0.0
        assert row.alert_hz >= 0.0


def test_default_allocation_is_monotone_in_tier() -> None:
    """P1 must allocate at least as much of every resource as P4."""
    p1 = DEFAULT_ALLOCATION_TABLE["P1"]
    p4 = DEFAULT_ALLOCATION_TABLE["P4"]
    assert p1.cpu_budget >= p4.cpu_budget
    assert p1.ai_inference_budget >= p4.ai_inference_budget
    assert p1.scan_hz >= p4.scan_hz
    assert p1.alert_hz >= p4.alert_hz


def test_priority_allocation_rejects_out_of_range() -> None:
    with pytest.raises(ValueError):
        PriorityAllocation(
            cpu_budget=1.5, ai_inference_budget=0.5, scan_hz=1.0, alert_hz=1.0
        )
    with pytest.raises(ValueError):
        PriorityAllocation(
            cpu_budget=0.5, ai_inference_budget=-0.1, scan_hz=1.0, alert_hz=1.0
        )
    with pytest.raises(ValueError):
        PriorityAllocation(
            cpu_budget=0.5, ai_inference_budget=0.5, scan_hz=-1.0, alert_hz=1.0
        )


def test_priority_allocation_table_requires_every_tier() -> None:
    with pytest.raises(ValueError):
        PriorityAllocationTable(
            rows={
                "P1": PriorityAllocation(
                    cpu_budget=1.0,
                    ai_inference_budget=1.0,
                    scan_hz=10.0,
                    alert_hz=10.0,
                )
            }
        )


def test_priority_allocation_table_is_read_only() -> None:
    """Mutating the source mapping must not affect the table."""
    src = dict(DEFAULT_ALLOCATION_TABLE.rows)
    table = PriorityAllocationTable(rows=src)
    src["P1"] = PriorityAllocation(  # type: ignore[index]
        cpu_budget=0.0, ai_inference_budget=0.0, scan_hz=0.0, alert_hz=0.0
    )
    # The table snapshotted at construction.
    assert table["P1"].cpu_budget == DEFAULT_ALLOCATION_TABLE["P1"].cpu_budget
    # And the table itself is read-only.
    with pytest.raises(TypeError):
        table.rows["P1"] = src["P1"]  # type: ignore[index]


def test_load_priority_allocation_table_merges_overrides() -> None:
    custom = PriorityAllocation(
        cpu_budget=0.99, ai_inference_budget=0.99, scan_hz=20.0, alert_hz=20.0
    )
    table = load_priority_allocation_table(overrides={"P1": custom})
    assert table["P1"] is custom
    # Other tiers fall through to the defaults.
    assert table["P2"] == DEFAULT_ALLOCATION_TABLE["P2"]


# --------------------------------------------------------------------------- #
# Subject helper.
# --------------------------------------------------------------------------- #


def test_priority_subject_for_symbol() -> None:
    assert priority_subject("RELIANCE") == "ai.priority.changed.RELIANCE"


def test_priority_subject_rejects_invalid_symbols() -> None:
    with pytest.raises(ValueError):
        priority_subject("")
    with pytest.raises(ValueError):
        priority_subject("BAD.SYMBOL")


# --------------------------------------------------------------------------- #
# Engine — totality (R14.1).
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_track_assigns_initial_tier_without_emitting() -> None:
    """Track must satisfy totality immediately and must NOT emit on init."""
    pub = InMemoryPriorityChangedPublisher()
    eng = SymbolPriorityEngine(publisher=pub)

    tier = await eng.track("RELIANCE", initial_tier="P2")

    assert tier == "P2"
    assert eng.tier("RELIANCE") == "P2"
    assert eng.tracked_symbols() == ("RELIANCE",)
    # Totality is satisfied — symbol is assigned exactly one tier.
    assert eng.snapshot() == {"RELIANCE": "P2"}
    # Edge-triggered: no event is emitted on the *initial* assignment
    # because there is no prior tier to transition from.
    assert pub.events == []


@pytest.mark.asyncio
async def test_track_is_idempotent() -> None:
    eng = SymbolPriorityEngine()
    await eng.track("RELIANCE", initial_tier="P2")
    again = await eng.track("RELIANCE", initial_tier="P1")  # ignored
    assert again == "P2"
    assert eng.tier("RELIANCE") == "P2"


@pytest.mark.asyncio
async def test_untrack_removes_symbol_atomically() -> None:
    eng = SymbolPriorityEngine()
    await eng.track("RELIANCE", initial_tier="P2")
    eng.untrack("RELIANCE")
    assert "RELIANCE" not in eng.tracked_symbols()
    with pytest.raises(UnknownSymbolError):
        eng.tier("RELIANCE")


def test_tier_for_unknown_symbol_raises() -> None:
    eng = SymbolPriorityEngine()
    with pytest.raises(UnknownSymbolError):
        eng.tier("NEVER_SEEN")


# --------------------------------------------------------------------------- #
# Engine — edge-triggered emission (R14.3, Property 8).
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_trader_intent_overrides_baseline_and_emits_once() -> None:
    pub = InMemoryPriorityChangedPublisher()
    eng = SymbolPriorityEngine(publisher=pub, clock_ns=lambda: 100)

    await eng.track("RELIANCE", initial_tier="P3")  # no event
    intent = TraderIntentPriority(
        correlation_id="c1",
        symbol="RELIANCE",
        to="P1",
        actor="trader",
        ts_ns=42,
    )
    await eng.on_trader_intent(intent)

    assert eng.tier("RELIANCE") == "P1"
    assert len(pub.events) == 1
    evt = pub.events[0]
    assert isinstance(evt, PriorityChanged)
    assert evt.symbol == "RELIANCE"
    assert evt.from_ == "P3"
    assert evt.to == "P1"
    assert evt.ts_ns == 100


@pytest.mark.asyncio
async def test_trader_intent_for_untracked_symbol_starts_tracking_silently() -> None:
    """A trader intent for a brand-new symbol enrolls it without emitting."""
    pub = InMemoryPriorityChangedPublisher()
    eng = SymbolPriorityEngine(publisher=pub)
    intent = TraderIntentPriority(
        correlation_id="c1",
        symbol="HDFC",
        to="P2",
        actor="trader",
        ts_ns=1,
    )
    await eng.on_trader_intent(intent)

    assert eng.tier("HDFC") == "P2"
    assert pub.events == []


@pytest.mark.asyncio
async def test_repeated_identical_intents_emit_at_most_one_event() -> None:
    """Edge-triggered: only adjacent-pair *changes* produce events."""
    pub = InMemoryPriorityChangedPublisher()
    eng = SymbolPriorityEngine(publisher=pub)
    await eng.track("RELIANCE", initial_tier="P3")

    for _ in range(5):
        await eng.on_trader_intent(
            TraderIntentPriority(
                correlation_id="c",
                symbol="RELIANCE",
                to="P1",
                actor="trader",
                ts_ns=1,
            )
        )

    # First intent flips P3 → P1 (one event); the next four are no-ops.
    assert [e.to for e in pub.events] == ["P1"]


@pytest.mark.asyncio
async def test_emitted_count_equals_distinct_adjacent_changes() -> None:
    """Property 8 unit-test: count_of_emits == count_of_adjacent_changes."""
    pub = InMemoryPriorityChangedPublisher()
    eng = SymbolPriorityEngine(publisher=pub)
    await eng.track("RELIANCE", initial_tier="P3")

    sequence = ["P3", "P3", "P1", "P1", "P2", "P3", "P3", "P4"]
    expected_changes = sum(
        1 for prior, curr in zip(sequence, sequence[1:]) if prior != curr
    )

    for tier in sequence[1:]:  # The first element is the initial tier.
        await eng.on_trader_intent(
            TraderIntentPriority(
                correlation_id="c",
                symbol="RELIANCE",
                to=tier,  # type: ignore[arg-type]
                actor="trader",
                ts_ns=1,
            )
        )

    assert len(pub.events) == expected_changes
    # And every event carries from/to.
    for evt in pub.events:
        assert evt.from_ != evt.to


@pytest.mark.asyncio
async def test_regime_change_recomputes_every_tracked_symbol() -> None:
    pub = InMemoryPriorityChangedPublisher()
    eng = SymbolPriorityEngine(publisher=pub)
    # Both start at the baseline P3.
    await eng.track("RELIANCE")
    await eng.track("INFY")

    # Panic bumps every symbol up two tiers (P3 → P1 by default).
    await eng.on_regime_change(
        RegimeChanged.model_validate({"from": "Sideways", "to": "Panic", "ts_ns": 1})
    )

    assert eng.tier("RELIANCE") == "P1"
    assert eng.tier("INFY") == "P1"
    # One event per symbol.
    symbols = sorted(e.symbol for e in pub.events)
    assert symbols == ["INFY", "RELIANCE"]


@pytest.mark.asyncio
async def test_news_impact_does_not_affect_untracked_symbols() -> None:
    pub = InMemoryPriorityChangedPublisher()
    eng = SymbolPriorityEngine(publisher=pub)

    await eng.on_news_impact(
        NewsImpact(
            correlation_id="c",
            symbol="UNKNOWN",
            headline_id="h",
            sentiment=0.9,
            impact_magnitude=0.9,
            fast_path=True,
            slow_path_pending=False,
            ts_ns=1,
        )
    )
    assert eng.tracked_symbols() == ()
    assert pub.events == []


@pytest.mark.asyncio
async def test_high_impact_news_promotes_symbol_one_tier() -> None:
    pub = InMemoryPriorityChangedPublisher()
    eng = SymbolPriorityEngine(publisher=pub)
    await eng.track("RELIANCE", initial_tier="P3")

    await eng.on_news_impact(
        NewsImpact(
            correlation_id="c",
            symbol="RELIANCE",
            headline_id="h1",
            sentiment=0.1,
            impact_magnitude=0.9,
            fast_path=True,
            slow_path_pending=False,
            ts_ns=1,
        )
    )

    assert eng.tier("RELIANCE") == "P2"
    assert len(pub.events) == 1


# --------------------------------------------------------------------------- #
# Authority_Hierarchy (R21).
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_trader_intent_overrides_subsequent_news_or_regime_inputs() -> None:
    """Trader-pinned tier survives noisy AI inputs."""
    pub = InMemoryPriorityChangedPublisher()
    eng = SymbolPriorityEngine(publisher=pub)
    await eng.track("RELIANCE", initial_tier="P3")

    # Trader pins to P4.
    await eng.on_trader_intent(
        TraderIntentPriority(
            correlation_id="c1",
            symbol="RELIANCE",
            to="P4",
            actor="trader",
            ts_ns=1,
        )
    )
    assert eng.tier("RELIANCE") == "P4"

    # Panic regime + high-impact news would normally bump to P1 — but
    # the trader override wins.
    await eng.on_regime_change(
        RegimeChanged.model_validate({"from": "Sideways", "to": "Panic", "ts_ns": 1})
    )
    await eng.on_news_impact(
        NewsImpact(
            correlation_id="c2",
            symbol="RELIANCE",
            headline_id="h1",
            sentiment=0.9,
            impact_magnitude=0.9,
            fast_path=True,
            slow_path_pending=False,
            ts_ns=2,
        )
    )

    assert eng.tier("RELIANCE") == "P4"


# --------------------------------------------------------------------------- #
# Default policy direct unit checks.
# --------------------------------------------------------------------------- #


def test_default_policy_returns_baseline_when_no_inputs() -> None:
    policy = DefaultPriorityPolicy()
    inputs = PriorityInputs()
    assert policy.assign(symbol="X", inputs=inputs) == "P3"


def test_default_policy_panic_regime_bumps_two_tiers() -> None:
    policy = DefaultPriorityPolicy()
    inputs = PriorityInputs(regime="Panic")
    # Baseline P3 -> bump up two -> P1.
    assert policy.assign(symbol="X", inputs=inputs) == "P1"


def test_default_policy_low_participation_bumps_down() -> None:
    policy = DefaultPriorityPolicy()
    inputs = PriorityInputs(regime="LowParticipation")
    # Baseline P3 -> bump down one -> P4.
    assert policy.assign(symbol="X", inputs=inputs) == "P4"


# --------------------------------------------------------------------------- #
# NATS publisher (subject + payload shape).
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_nats_publisher_uses_per_symbol_subject_and_compact_json() -> None:
    captured: list[tuple[str, bytes]] = []

    async def fake_publish(subject: str, payload: bytes) -> None:
        captured.append((subject, payload))

    pub = NatsPriorityChangedPublisher(async_publish=fake_publish)
    evt = PriorityChanged.model_validate(
        {"symbol": "RELIANCE", "from": "P3", "to": "P1", "ts_ns": 42}
    )
    await pub.publish_changed(evt)

    assert len(captured) == 1
    subject, payload = captured[0]
    assert subject == "ai.priority.changed.RELIANCE"
    decoded = json.loads(payload.decode("utf-8"))
    assert decoded == {"symbol": "RELIANCE", "from": "P3", "to": "P1", "ts_ns": 42}
    # Compact: no spaces.
    assert b" " not in payload
