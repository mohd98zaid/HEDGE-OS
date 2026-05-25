"""Typed pydantic records for every Timescale hypertable (task 32.1).

Each model mirrors a canonical FlatBuffers / JSON schema from
``crates/hedge-schemas`` so the persisted rows can be lifted back into
the same shapes the rest of the system already speaks.

The mapping (R19.1):

* ``TickSample``           ← ``Tick_v1`` (FlatBuffers, ``tick.fbs``).
* ``OrderRecord``          ← ``OrderState_v1`` (FlatBuffers, ``order.fbs``).
* ``FillRecord``           — derived projection of ``OrderState_v1`` for the
                             fills hypertable (one row per fill event).
* ``AiScore``              ← ``ai.rank.<cid>`` (JSON, ``ai_rank.schema.json``).
* ``RegimeTransition``     ← ``ai.regime.changed``.
* ``PsychologyTimelinePoint`` ← ``ai.psych.stability``.
* ``BrokerMetric``         — Memory_RAG-local broker latency / error sample.
* ``JournalEntry``         ← ``ai.journal.entry`` (R18.2).
* ``PreviousDayMemoryRow`` ← ``mem.prev_day.<sym>`` (R15) — full
                             persisted row including the structural
                             behaviour markers (failed breakouts, gap
                             reactions, delivery volume, trend continuation,
                             institutional behavior, news reactions).

All models forbid extra fields and freeze on instantiation so the writers
can rely on structural equality during the round-trip property test
(task 32.2).
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Annotated, Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field

# --- Enumerations ----------------------------------------------------------

# Mirrored from `Tick_v1.exchange` (`tick.fbs`).
ExchangeCode = Literal["NSE", "BSE"]

# Mirrored from `OrderIntent_v1.side`.
OrderSide = Literal["Buy", "Sell"]

# Mirrored from `OrderIntent_v1.order_type`.
OrderType = Literal["Market", "Limit"]

# Mirrored from `OrderState_v1.state`.
OrderStateName = Literal[
    "New",
    "Submitted",
    "PartiallyFilled",
    "Filled",
    "Cancelled",
    "Rejected",
]

# Mirrored from `ai.regime.changed.{from,to}`.
RegimeName = Literal[
    "Trending",
    "Sideways",
    "Panic",
    "HighVolatility",
    "NewsDriven",
    "LiquidityCrisis",
    "LowParticipation",
]


# --- Helpers ---------------------------------------------------------------


def _ns_to_utc(ts_ns: int) -> datetime:
    """Convert a uint64 nanosecond timestamp to a UTC :class:`datetime`."""
    return datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=timezone.utc)


def _utc_to_ns(ts: datetime) -> int:
    """Inverse of :func:`_ns_to_utc` — accepts naive datetimes as UTC."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return int(ts.timestamp() * 1_000_000_000)


# --- Base ------------------------------------------------------------------


class _Record(BaseModel):
    """All hypertable records forbid extras and are frozen post-init."""

    model_config = ConfigDict(extra="forbid", frozen=True, validate_assignment=False)


# --- Tick samples ----------------------------------------------------------


class TickSample(_Record):
    """One sampled tick row (R19.1, R19.3).

    Sampled means the Hot_Path Market_Data_Engine can downsample its
    ``md.tick.<sym>`` stream before publishing to the Memory_RAG_Layer.
    Field semantics mirror ``Tick_v1`` byte-for-byte.
    """

    ts: datetime
    symbol_id: Annotated[int, Field(ge=0, le=2**32 - 1)]
    exchange: ExchangeCode
    ltp_paise: int
    bid_paise: int
    ask_paise: int
    ltq: Annotated[int, Field(ge=0)]
    total_buy_qty: Annotated[int, Field(ge=0)]
    total_sell_qty: Annotated[int, Field(ge=0)]
    correlation_id: Annotated[bytes, Field(min_length=16, max_length=16)]


# --- Orders ----------------------------------------------------------------


class OrderRecord(_Record):
    """One row per ``OrderState_v1`` lifecycle update (R6, R19.1)."""

    ts: datetime
    correlation_id: Annotated[bytes, Field(min_length=16, max_length=16)]
    broker_order_id: Annotated[str, Field(min_length=1, max_length=128)]
    state: OrderStateName
    symbol_id: Annotated[int, Field(ge=0, le=2**32 - 1)]
    side: OrderSide
    order_type: OrderType
    quantity: Annotated[int, Field(ge=0)]
    limit_paise: int | None = None
    filled_qty: Annotated[int, Field(ge=0)]
    avg_fill_paise: int


class FillRecord(_Record):
    """One row per partial / final fill (projection of ``OrderState_v1``)."""

    ts: datetime
    correlation_id: Annotated[bytes, Field(min_length=16, max_length=16)]
    broker_order_id: Annotated[str, Field(min_length=1, max_length=128)]
    symbol_id: Annotated[int, Field(ge=0, le=2**32 - 1)]
    side: OrderSide
    fill_qty: Annotated[int, Field(ge=0)]
    fill_paise: int
    cumulative_qty: Annotated[int, Field(ge=0)]
    avg_fill_paise: int


# --- AI scores -------------------------------------------------------------


class AiScore(_Record):
    """One row per ``ai.rank.<cid>`` emission (R17, R19.1).

    Component factors are stored as separate columns so time-window
    queries can aggregate by factor without parsing JSONB.
    """

    ts: datetime
    correlation_id: Annotated[str, Field(min_length=1, max_length=64)]
    signal_id: Annotated[str, Field(min_length=1, max_length=64)]
    trade_confidence_score: Annotated[float, Field(ge=0.0, le=1.0)]
    factor_orderflow: Annotated[float, Field(ge=0.0, le=1.0)]
    factor_technical_strength: Annotated[float, Field(ge=0.0, le=1.0)]
    factor_news_sentiment: Annotated[float, Field(ge=0.0, le=1.0)]
    factor_market_regime: Annotated[float, Field(ge=0.0, le=1.0)]
    factor_trader_discipline: Annotated[float, Field(ge=0.0, le=1.0)]
    shadow: bool


# --- Regime ----------------------------------------------------------------


class RegimeTransition(_Record):
    """Edge-triggered regime change (R22, R19.1)."""

    ts: datetime
    from_regime: RegimeName
    to_regime: RegimeName


# --- Psychology ------------------------------------------------------------


class PsychologyTimelinePoint(_Record):
    """One sampled point of the Trader_Stability_Score timeline (R16)."""

    ts: datetime
    score: Annotated[float, Field(ge=0.0, le=1.0)]
    discipline: Annotated[float, Field(ge=0.0, le=1.0)]
    emotional_control: Annotated[float, Field(ge=0.0, le=1.0)]
    risk_consistency: Annotated[float, Field(ge=0.0, le=1.0)]
    patience: Annotated[float, Field(ge=0.0, le=1.0)]
    behaviors: Annotated[list[str], Field(default_factory=list, max_length=32)]


# --- Broker metrics --------------------------------------------------------


class BrokerMetric(_Record):
    """One sample of broker connection health (R6.5, R19.1)."""

    ts: datetime
    broker: Annotated[str, Field(min_length=1, max_length=32)]
    latency_ms: Annotated[float, Field(ge=0.0)]
    error_rate: Annotated[float, Field(ge=0.0, le=1.0)]
    connected: bool
    last_error: Annotated[str, Field(max_length=256)] | None = None


# --- Journal entries -------------------------------------------------------


class JournalEntry(_Record):
    """Persisted ``ai.journal.entry`` (R18.2, R19.1)."""

    ts: datetime
    correlation_id: Annotated[str, Field(min_length=1, max_length=64)]
    trade_id: Annotated[str, Field(min_length=1, max_length=64)]
    symbol: Annotated[str, Field(min_length=1, max_length=32)]
    side: OrderSide
    quantity: Annotated[int, Field(ge=0)]
    entry_paise: int
    exit_paise: int
    pnl_inr: float
    narrative: Annotated[str, Field(min_length=1, max_length=8192)]


# --- Governance metrics ----------------------------------------------------


# Mirror of the canonical ``ai.gov.action.metric`` enum, so a row read
# back from the hypertable round-trips into a wire-valid payload.
GovernanceMetric = Literal["drift", "accuracy", "latency", "error_rate"]

# Engine-internal metric name; richer than the canonical wire enum so
# the row can distinguish ``confidence_stability`` and
# ``hallucination_indicators`` (both map to the wire ``error_rate``).
GovernanceMetricKind = Literal[
    "drift",
    "confidence_stability",
    "hallucination_indicators",
    "prediction_quality",
]

# Per-component governance level enum (matches
# ``GovernanceLevel`` in :mod:`hedge_warm_ai.governance.ladder`).
GovernanceLevel = Literal["none", "degraded", "critical"]

# Subset of the canonical ``ai.gov.action.action`` enum the engine
# actually emits. ``freeze`` is reserved for future use; not used by
# the engine today.
GovernanceAction = Literal["reduce_influence", "shadow_mode", "rollback"]


class GovernanceMetricSample(_Record):
    """Persisted row for the ``governance_metrics`` hypertable (R23.3, R24.1).

    The row carries every field needed to:

    * reconstruct the canonical ``ai.gov.action`` payload for a given
      edge emission (``component``, ``metric``, ``value``,
      ``threshold``, ``action``);
    * track the per-component governance level over time
      (``level``);
    * correlate prediction-quality outcomes with the upstream
      component output (``correlation_id``); and
    * disambiguate ``confidence_stability`` from
      ``hallucination_indicators`` rows (``metric_kind``) — both map
      to the canonical wire enum value ``error_rate``.

    ``action`` is ``None`` when the row records a non-edge metric
    snapshot (continued tracking with the level unchanged).
    ``correlation_id`` is ``None`` for plain metric snapshots and
    populated when the row correlates a component output with a
    realised market outcome.
    """

    ts: datetime
    component: Annotated[str, Field(min_length=1, max_length=64)]
    metric: GovernanceMetric
    metric_kind: GovernanceMetricKind
    value: float
    threshold: float
    level: GovernanceLevel
    action: GovernanceAction | None = None
    correlation_id: Annotated[str, Field(min_length=1, max_length=64)] | None = None
    sample_count: Annotated[int, Field(ge=0)] = 0


# --- Previous-day memory ---------------------------------------------------


# Mirrors ``KeyLevelKind`` in ``hedge_warm_ai.schemas.mem_prev_day``.
PrevDayKeyLevelKind = Literal[
    "support",
    "resistance",
    "swing_high",
    "swing_low",
    "vwap",
    "open",
    "close",
]


class PrevDayKeyLevel(_Record):
    """One persisted previous-day key price level (mirrors ``KeyLevel``)."""

    kind: PrevDayKeyLevelKind
    price_paise: int


class PreviousDayMemoryRow(_Record):
    """One row of the ``prev_day_memory`` hypertable (R15.1, R15.3).

    The OHLCV / VWAP fix points and the canonical ``key_levels`` array
    are mirrored 1:1 from ``hedge_warm_ai.schemas.PreviousDayMemory`` so
    a row read from Timescale round-trips into the wire schema for the
    ``mem.prev_day.<sym>`` event verbatim (Property 5).

    The structural behaviour markers are stored as JSON-able mappings /
    sequences. They evolve more often than the fix points, so keeping
    them in JSONB columns avoids further migrations:

    * ``failed_breakouts``       — list of {price_paise, ts_ns, side, ...}
    * ``gap_reactions``          — {gap_paise, fill_ratio, retraced, ...}
    * ``trend_continuation``     — {direction, strength, ...}
    * ``institutional_behavior`` — {delivery_pct, large_order_ratio, ...}
    * ``news_reactions``         — list of {headline_id, magnitude, ts_ns, ...}

    ``embedding_point_id`` is the Qdrant ``market_memory`` point id of
    the embedding-friendly summary attached to this row; ``None`` when
    the embedder has not yet run (e.g. mid-compute).
    """

    ts: datetime
    session_date: date
    symbol_id: Annotated[int, Field(ge=0, le=2**32 - 1)]
    symbol: Annotated[str, Field(min_length=1, max_length=32)]
    open_paise: int
    high_paise: int
    low_paise: int
    close_paise: int
    vwap_paise: int
    total_volume: Annotated[int, Field(ge=0)]
    delivery_volume: Annotated[int, Field(ge=0)]
    key_levels: Annotated[list[PrevDayKeyLevel], Field(default_factory=list, max_length=16)]
    failed_breakouts: list[dict[str, Any]] = Field(default_factory=list)
    gap_reactions: dict[str, Any] = Field(default_factory=dict)
    trend_continuation: dict[str, Any] = Field(default_factory=dict)
    institutional_behavior: dict[str, Any] = Field(default_factory=dict)
    news_reactions: list[dict[str, Any]] = Field(default_factory=list)
    embedding_point_id: Annotated[str, Field(max_length=128)] | None = None
    computed_ts_ns: Annotated[int, Field(ge=0)]


# --- Constants -------------------------------------------------------------

#: Stable canonical names every hypertable in this layer is registered
#: under. Used by the migration runner as the source of truth.
HYPERTABLE_NAMES: Final[tuple[str, ...]] = (
    "tick_samples",
    "orders",
    "fills",
    "ai_scores",
    "regime_history",
    "psychology_timeline",
    "broker_metrics",
    "journal_entries",
    "prev_day_memory",
    "governance_metrics",
)


__all__ = [
    "AiScore",
    "BrokerMetric",
    "ExchangeCode",
    "FillRecord",
    "GovernanceAction",
    "GovernanceLevel",
    "GovernanceMetric",
    "GovernanceMetricKind",
    "GovernanceMetricSample",
    "HYPERTABLE_NAMES",
    "JournalEntry",
    "OrderRecord",
    "OrderSide",
    "OrderStateName",
    "OrderType",
    "PrevDayKeyLevel",
    "PrevDayKeyLevelKind",
    "PreviousDayMemoryRow",
    "PsychologyTimelinePoint",
    "RegimeName",
    "RegimeTransition",
    "TickSample",
]
