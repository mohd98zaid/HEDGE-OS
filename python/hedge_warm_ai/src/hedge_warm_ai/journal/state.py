"""Wire model for the ``exec.trade.closed`` payload consumed by the engine.

The Hot_Path Execution_Engine emits ``exec.trade.closed`` once a
round-trip trade collapses to zero net quantity. The canonical
FlatBuffers schema for this event has not been declared yet (the
schema list in ``hedge-schemas`` is incremental — task 4.1 declared
the lifecycle/fill schemas; the closed-trade aggregate is a
Warm_AI_Pipeline-side projection of those fills).

This module declares the *Warm-side* canonical pydantic shape used by
the AI_Trade_Journal_Engine to consume ``exec.trade.closed`` payloads.
The fields enumerate every input the design's AI_Trade_Journal_Engine
needs:

* outcome ............. ``pnl_inr``, ``pnl_paise``, ``side``, ``quantity``,
                        ``entry_paise`` / ``exit_paise``,
                        ``opened_ts_ns`` / ``closed_ts_ns``.
* contributing strategy
  + signal ............ ``strategy_id`` / ``signal_id``.
* trader emotional
  state at entry/exit . ``emotional_at_entry`` / ``emotional_at_exit``
                        (a :class:`EmotionalSnapshot` carrying the
                        live ``Trader_Stability_Score`` and component
                        factors at each side of the trade).
* prevailing regime ... ``regime`` (one of the canonical
                        :class:`RegimeName` values).
* identified missed
  opportunities ....... a free-form list of textual notes the
                        Execution_Engine attaches when its post-trade
                        evaluator detects a missed exit (e.g.
                        "exited 1.2% before VWAP retest").
* execution-quality
  metrics ............. ``slippage_bps``, ``latency_ms``,
                        ``fill_attempts``, ``commission_paise``.

The model is :class:`pydantic.BaseModel`-based with
``ConfigDict(extra="forbid")`` so producer-side mistakes (extra
fields) raise immediately rather than silently corrupting the journal.

This is *not* a canonical wire schema yet — when the Hot_Path schema
catches up, this model will be regenerated from the canonical JSON
schema and the file in this module will be deleted in favour of a
``hedge_warm_ai.schemas.exec_trade_closed`` mirror. Until then, the
in-process layout is the single source of truth.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

# Imported for the canonical regime names — same Literal used by every
# Warm_AI_Pipeline schema that talks about regimes.
from ..schemas.ai_regime_changed import Regime as RegimeName  # noqa: F401

# --- Enumerations ----------------------------------------------------------

# Mirrors ``OrderIntent_v1.side`` and the persisted JournalEntry side.
OrderSide = Literal["Buy", "Sell"]


# --- Snapshots -------------------------------------------------------------


class EmotionalSnapshot(BaseModel):
    """Trader emotional state captured at one timestamp.

    Mirrors the four-factor :class:`StabilityComponents` model from
    :mod:`hedge_warm_ai.schemas.ai_psych_stability` (R16.2). Every
    factor is in ``[0, 1]``; the live ``Trader_Stability_Score`` is
    carried separately so the journal narrative can reference both
    the aggregate and the component breakdown.
    """

    model_config = ConfigDict(extra="forbid")

    score: Annotated[float, Field(ge=0.0, le=1.0)]
    discipline: Annotated[float, Field(ge=0.0, le=1.0)]
    emotional_control: Annotated[float, Field(ge=0.0, le=1.0)]
    risk_consistency: Annotated[float, Field(ge=0.0, le=1.0)]
    patience: Annotated[float, Field(ge=0.0, le=1.0)]


class ExecutionQuality(BaseModel):
    """Execution-quality summary for one closed trade.

    All fields are optional because the Execution_Engine attaches
    whatever it has measured; the journal narrative gracefully
    degrades when fields are missing.
    """

    model_config = ConfigDict(extra="forbid")

    slippage_bps: float | None = None
    latency_ms: float | None = None
    fill_attempts: Annotated[int, Field(ge=0)] | None = None
    commission_paise: int | None = None


# --- Closed-trade event ----------------------------------------------------


class TradeClosedEvent(BaseModel):
    """Payload of one ``exec.trade.closed`` message.

    The model is the **Warm-side** canonical representation; the
    Execution_Engine's wire bytes are projected into it by the bus
    adapter (see :class:`JournalBusSubscriber`). Every field maps
    onto the design's R18.1 list of journal-entry inputs.
    """

    model_config = ConfigDict(extra="forbid")

    correlation_id: Annotated[str, Field(min_length=1, max_length=64)]
    trade_id: Annotated[str, Field(min_length=1, max_length=64)]
    symbol: Annotated[str, Field(min_length=1, max_length=32)]
    side: OrderSide
    quantity: Annotated[int, Field(ge=0)]
    entry_paise: int
    exit_paise: int
    pnl_paise: int
    pnl_inr: float
    opened_ts_ns: Annotated[int, Field(ge=0)]
    closed_ts_ns: Annotated[int, Field(ge=0)]

    strategy_id: Annotated[str, Field(min_length=1, max_length=64)]
    signal_id: Annotated[str, Field(min_length=1, max_length=64)]

    regime: RegimeName

    emotional_at_entry: EmotionalSnapshot
    emotional_at_exit: EmotionalSnapshot

    missed_opportunities: list[
        Annotated[str, Field(min_length=1, max_length=256)]
    ] = Field(default_factory=list, max_length=16)

    execution_quality: ExecutionQuality = Field(default_factory=ExecutionQuality)


__all__ = [
    "EmotionalSnapshot",
    "ExecutionQuality",
    "OrderSide",
    "RegimeName",
    "TradeClosedEvent",
]
