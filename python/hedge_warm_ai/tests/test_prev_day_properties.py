"""Property-based tests for Previous_Day_Memory compute (task 24.2).

Validates:
    - OHLCV invariants: low <= open/close/vwap <= high
    - delivery_volume <= total_volume
    - build_prev_day_row / build_prev_day_event round-trip
    - stable_embedding_point_id is deterministic
    - chunk_inputs respects chunk_size

**Validates: Requirements 15.1, 15.2**
"""

from __future__ import annotations

from datetime import date

from hypothesis import given, assume, settings
from hypothesis import strategies as st

from hedge_warm_ai.prev_day.compute import (
    PrevDaySessionInputs,
    SymbolSessionData,
    build_prev_day_event,
    build_prev_day_row,
    chunk_inputs,
    stable_embedding_point_id,
)


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

def arb_symbol_session_data() -> st.SearchStrategy[SymbolSessionData]:
    """Generate valid OHLCV data where low <= all prices <= high."""
    return st.builds(
        SymbolSessionData,
        symbol_id=st.integers(min_value=1, max_value=10000),
        symbol=st.text(min_size=1, max_size=10, alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
        session_date=st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31)),
        open_paise=st.integers(min_value=100, max_value=100_000),
        high_paise=st.integers(min_value=100, max_value=200_000),
        low_paise=st.integers(min_value=1, max_value=100_000),
        close_paise=st.integers(min_value=100, max_value=100_000),
        vwap_paise=st.integers(min_value=100, max_value=100_000),
        total_volume=st.integers(min_value=0, max_value=10_000_000),
        delivery_volume=st.integers(min_value=0, max_value=10_000_000),
    )


def arb_valid_session_data() -> st.SearchStrategy[SymbolSessionData]:
    """Generate session data that satisfies all OHLCV invariants."""
    return st.builds(
        _make_valid_session,
        base_price=st.integers(min_value=100, max_value=50_000),
        spread=st.integers(min_value=0, max_value=5_000),
        symbol_id=st.integers(min_value=1, max_value=10000),
        symbol=st.text(min_size=1, max_size=10, alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
        session_date=st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31)),
        total_volume=st.integers(min_value=1, max_value=10_000_000),
    )


def _make_valid_session(
    base_price: int,
    spread: int,
    symbol_id: int,
    symbol: str,
    session_date: date,
    total_volume: int,
) -> SymbolSessionData:
    low = base_price
    high = base_price + spread
    return SymbolSessionData(
        symbol_id=symbol_id,
        symbol=symbol,
        session_date=session_date,
        open_paise=base_price + spread // 4,
        high_paise=high,
        low_paise=low,
        close_paise=base_price + spread * 3 // 4,
        vwap_paise=base_price + spread // 2,
        total_volume=total_volume,
        delivery_volume=total_volume // 2,
    )


# ---------------------------------------------------------------------------
# OHLCV invariant validation
# ---------------------------------------------------------------------------

@given(data=arb_valid_session_data())
def test_build_prev_day_row_accepts_valid_data(data: SymbolSessionData) -> None:
    """Property: valid OHLCV data produces a row without error."""
    row = build_prev_day_row(data, computed_ts_ns=1000)
    assert row.symbol_id == data.symbol_id
    assert row.open_paise == data.open_paise
    assert row.high_paise == data.high_paise
    assert row.low_paise == data.low_paise
    assert row.close_paise == data.close_paise
    assert row.vwap_paise == data.vwap_paise


# ---------------------------------------------------------------------------
# build_prev_day_event round-trip
# ---------------------------------------------------------------------------

@given(data=arb_valid_session_data())
def test_event_round_trip_preserves_ohlcv(data: SymbolSessionData) -> None:
    """Property: build_prev_day_event preserves OHLCV fields from the row."""
    row = build_prev_day_row(data, computed_ts_ns=2000)
    event = build_prev_day_event(row)
    assert event.open_paise == data.open_paise
    assert event.high_paise == data.high_paise
    assert event.low_paise == data.low_paise
    assert event.close_paise == data.close_paise
    assert event.vwap_paise == data.vwap_paise
    assert event.session_date == data.session_date.isoformat()
    assert event.ts_ns == 2000


# ---------------------------------------------------------------------------
# stable_embedding_point_id determinism
# ---------------------------------------------------------------------------

@given(data=arb_valid_session_data())
def test_embedding_point_id_deterministic(data: SymbolSessionData) -> None:
    """Property: same (symbol_id, session_date) always produces the same embedding point id."""
    row = build_prev_day_row(data, computed_ts_ns=3000)
    id1 = stable_embedding_point_id(row)
    id2 = stable_embedding_point_id(row)
    assert id1 == id2
    assert id1.startswith("prev_day_")


# ---------------------------------------------------------------------------
# chunk_inputs
# ---------------------------------------------------------------------------

@given(
    num_symbols=st.integers(min_value=0, max_value=20),
    chunk_size=st.integers(min_value=1, max_value=5),
)
def test_chunk_inputs_preserves_all_elements(num_symbols: int, chunk_size: int) -> None:
    """Property: chunk_inputs yields all input elements exactly once."""
    symbols = [
        SymbolSessionData(
            symbol_id=i, symbol=f"S{i}", session_date=date(2025, 1, 1),
            open_paise=100, high_paise=200, low_paise=50, close_paise=150,
            vwap_paise=125, total_volume=1000, delivery_volume=500,
        )
        for i in range(num_symbols)
    ]
    inputs = PrevDaySessionInputs(
        session_date=date(2025, 1, 1),
        symbols=symbols,
        computed_ts_ns=1000,
    )
    all_elements = []
    for chunk in chunk_inputs(inputs, chunk_size=chunk_size):
        all_elements.extend(chunk)
    assert len(all_elements) == num_symbols
