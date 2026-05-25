"""Trader-action and behavior-event domain types.

The Trader_Psychology_Engine subscribes to a curated subset of Hot_Path
events (``exec.order.*``, ``exec.fill.*``, ``risk.decision.*``,
``pos.update.<sym>``) and the trader-issued ``trader.intent.*`` family.
Those wire payloads are FlatBuffers (Hot_Path) or canonical JSON
(Warm_AI_Pipeline).

To keep the detectors strictly typed and easy to test, we normalise the
on-the-wire payloads into a small set of value-typed dataclasses:

* :class:`TraderAction` is the canonical input the detectors operate on.
  Service code (``hedge-psych`` entry point, task 25.1 wiring) is
  responsible for translating each subscribed wire event into a
  :class:`TraderAction` and feeding it to
  :class:`hedge_warm_ai.psychology.TraderPsychologyEngine.observe`.
* :class:`BehaviorEvent` is the output of a single detector firing. The
  engine aggregates the events emitted by all detectors for a given
  observation into one ``ai.psych.stability`` payload (R16.3).

The dataclasses are immutable and ``slots=True`` so accidental mutation
is impossible and the engine's allocation profile is bounded.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Final, Optional

# ---------------------------------------------------------------------------
# Enumerations --------------------------------------------------------------
# ---------------------------------------------------------------------------


class Side(str, Enum):
    """Trade side (mirrors ``hedge_core::Side`` and the schema enum)."""

    BUY = "Buy"
    SELL = "Sell"


class OrderState(str, Enum):
    """Per-order-state name (mirrors ``OrderState_v1.state``)."""

    NEW = "New"
    SUBMITTED = "Submitted"
    PARTIALLY_FILLED = "PartiallyFilled"
    FILLED = "Filled"
    CANCELLED = "Cancelled"
    REJECTED = "Rejected"


class TraderActionKind(str, Enum):
    """Kind of normalised trader action consumed by the detectors.

    Maps to upstream subjects:

    * :data:`ORDER_SUBMITTED`, :data:`ORDER_FILLED`,
      :data:`ORDER_CANCELLED`, :data:`ORDER_REJECTED` — derived from
      ``exec.order.<state>`` lifecycle transitions and (for ``filled``)
      ``exec.fill.<sym>``.
    * :data:`RISK_REJECTED` — derived from ``risk.decision.rejected``.
    * :data:`POSITION_UPDATE` — derived from ``pos.update.<sym>``.
    * :data:`STOP_LOSS_REMOVED` — derived from a trader-side modify
      action that clears the stop. The service layer detects this by
      diffing successive ``trader.intent.order.modify`` payloads against
      the original ``OrderIntent_v1`` it consumed earlier; the
      psychology engine just consumes the normalised verdict.
    """

    ORDER_SUBMITTED = "order_submitted"
    ORDER_FILLED = "order_filled"
    ORDER_CANCELLED = "order_cancelled"
    ORDER_REJECTED = "order_rejected"
    RISK_REJECTED = "risk_rejected"
    POSITION_UPDATE = "position_update"
    STOP_LOSS_REMOVED = "stop_loss_removed"


class BehaviorPattern(str, Enum):
    """Stable-string identifier for each detected behavior (R16.1).

    The string values are echoed verbatim into the ``behaviors`` array
    of the ``ai.psych.stability`` payload (canonical JSON schema in
    ``ai_psych_stability.schema.json``: ``items.maxLength: 64``).
    """

    REVENGE_TRADING = "revenge_trading"
    FOMO_ENTRY = "fomo_entry"
    OVERCONFIDENCE = "overconfidence"
    TILT = "tilt"
    IMPULSIVE_TRADING = "impulsive_trading"
    RAPID_RE_ENTRY = "rapid_re_entry"
    STOP_LOSS_REMOVED = "stop_loss_removed"
    DISCIPLINE_DEVIATION = "discipline_deviation"


# ---------------------------------------------------------------------------
# Dataclasses ---------------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TraderAction:
    """A single normalised trader action observed by the engine.

    Every detector takes the *current* action, the engine's
    :class:`TraderActionLog`, and the live :class:`BehaviorState` as
    input. Detectors are pure async functions that return either a
    :class:`BehaviorEvent` (when fired) or ``None`` (otherwise).

    Attributes:
        ts_ns: Monotonic-ns timestamp; matches ``ts_ns`` on the wire
            payload that produced this action.
        kind: The :class:`TraderActionKind` enum.
        symbol_id: Per-symbol id (``hedge_core::SymbolId``); ``0`` when
            the action is portfolio-scoped (e.g. risk-rejection of a
            cross-symbol intent).
        side: Trade side (when applicable; ``None`` for cancellations
            and pure position updates).
        quantity: Absolute order quantity. ``0`` for non-order actions.
        price_paise: Limit price in paise; ``None`` for market orders
            and non-order actions.
        pnl_inr: Realised P&L tied to this action in INR (negative on
            losses). Used by the revenge-trading and tilt detectors.
            ``0.0`` when not applicable.
        confidence: Optional confidence reported by upstream signal
            ranking (``ai.rank.<cid>``) for this action's correlation
            id. Used by the overconfidence detector.
        correlation_id: 16-byte ``CorrelationId`` from the originating
            wire payload; carried through for trace correlation.
    """

    ts_ns: int
    kind: TraderActionKind
    symbol_id: int = 0
    side: Optional[Side] = None
    quantity: int = 0
    price_paise: Optional[int] = None
    pnl_inr: float = 0.0
    confidence: Optional[float] = None
    correlation_id: bytes = b""


@dataclass(frozen=True, slots=True)
class BehaviorEvent:
    """One detector firing.

    The engine collapses all events emitted for a single observation
    into the ``behaviors`` array of one ``ai.psych.stability`` payload.
    The :attr:`severity` is in [0.0, 1.0] and is used by the engine to
    decay the relevant component factor (e.g. a revenge-trading event
    decays ``emotional_control``).
    """

    pattern: BehaviorPattern
    severity: float = 1.0
    detail: str = ""

    def __post_init__(self) -> None:
        if not (0.0 <= self.severity <= 1.0):
            raise ValueError(
                f"severity must be in [0.0, 1.0], got {self.severity}"
            )


# ---------------------------------------------------------------------------
# Action log ----------------------------------------------------------------
# ---------------------------------------------------------------------------


#: Maximum number of recent trader actions retained in the log.
#:
#: 256 actions is more than enough to cover an hour of single-trader
#: activity at the configured ``max_trades_per_hour`` ceiling
#: (``RiskConfig.max_trades_per_hour = 30``) and well within the
#: Warm_AI_Pipeline's per-process memory budget.
DEFAULT_LOG_CAPACITY: Final[int] = 256


@dataclass(slots=True)
class TraderActionLog:
    """Bounded, time-ordered ring of recent :class:`TraderAction` items.

    The log is **per-trader** (this engine is single-tenant per process
    in the configured deployment topology — design § Deployment
    Topology) and is consulted by every detector. It is intentionally a
    plain Python list rather than ``collections.deque`` so detectors can
    cheaply slice the most recent N actions without re-shaping a deque.
    """

    capacity: int = DEFAULT_LOG_CAPACITY
    actions: list[TraderAction] = field(default_factory=list)

    def append(self, action: TraderAction) -> None:
        self.actions.append(action)
        # Evict the oldest entries beyond the configured capacity.
        if len(self.actions) > self.capacity:
            del self.actions[0 : len(self.actions) - self.capacity]

    def recent(self, since_ns: int) -> list[TraderAction]:
        """Return actions whose ``ts_ns`` is ``>= since_ns``.

        The log is time-ordered by construction (``append`` is the only
        mutator and callers feed actions in monotonic order); we
        therefore scan from the tail backward to find the first entry
        older than the cutoff and slice from there.
        """
        # Walk backward — typical detector windows are seconds, so the
        # answer is usually in the last few entries.
        for idx in range(len(self.actions) - 1, -1, -1):
            if self.actions[idx].ts_ns < since_ns:
                return self.actions[idx + 1 :]
        return list(self.actions)

    def last(self) -> Optional[TraderAction]:
        return self.actions[-1] if self.actions else None

    def __len__(self) -> int:  # pragma: no cover - trivial
        return len(self.actions)


__all__ = [
    "BehaviorEvent",
    "BehaviorPattern",
    "DEFAULT_LOG_CAPACITY",
    "OrderState",
    "Side",
    "TraderAction",
    "TraderActionKind",
    "TraderActionLog",
]
