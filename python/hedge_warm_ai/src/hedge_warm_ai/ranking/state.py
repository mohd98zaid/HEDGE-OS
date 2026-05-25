"""Value types consumed and produced by the AI_Trade_Ranking_Engine.

The Hot_Path Signal_Engine emits :class:`hedge_v1.Signal_v1` payloads
on the canonical NATS subject ``sig.emitted`` (task 16.1, Rust side).
The wire payload is a FlatBuffers blob, but the Warm_AI_Pipeline
ranking engine consumes a *normalised* Python value type so the
detectors and tests do not need to know FlatBuffers.

Three frozen dataclasses live in this module:

* :class:`SignalEvent` — the canonical normalised input to the engine.
  Service code (the ``hedge-rank`` console-script entry point) is
  responsible for translating each ``sig.emitted`` FlatBuffers payload
  into a :class:`SignalEvent` and feeding it to
  :meth:`hedge_warm_ai.ranking.AiTradeRankingEngine.rank`.
* :class:`AiRank` — the canonical normalised output. The engine
  produces one :class:`AiRank` per :class:`SignalEvent`, builds the
  matching :class:`hedge_warm_ai.schemas.RankedSignal` Pydantic mirror
  for the wire payload, and writes the same :class:`AiRank` to the
  WarmCache for the Risk_Engine.
* :class:`RankingSample` — the diagnostic bundle returned to callers
  of :meth:`AiTradeRankingEngine.rank` (mirrors
  :class:`hedge_warm_ai.psychology.PsychologySample`).

The dataclasses are immutable + ``slots=True`` so accidental mutation
is impossible and the engine's allocation profile is bounded.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from .score import RankingFactors


class Side(str, Enum):
    """Trade side mirroring ``hedge_core::Side`` and the schema enum."""

    BUY = "Buy"
    SELL = "Sell"


@dataclass(frozen=True, slots=True)
class SignalEvent:
    """Normalised view of a Hot_Path ``sig.emitted`` payload.

    The engine accepts these directly; service code populates them
    from the decoded ``Signal_v1`` FlatBuffers payload.

    Attributes:
        signal_id: Stable string identifier for the signal. Bounded
            to ``[1, 64]`` characters by the
            ``ai_rank.schema.json`` ``signal_id`` field; the engine
            re-validates via :class:`RankedSignal` at emission time.
        correlation_id: 16-byte ``CorrelationId`` from the originating
            Signal_Engine emission. Carried verbatim into the
            ``ai.rank.<correlation_id>`` subject and the
            :attr:`RankedSignal.correlation_id` payload field
            (lower-case hex form).
        symbol: Per-symbol identifier (string label, e.g.
            ``"RELIANCE"``). Used to look up per-symbol factors
            (orderflow, technical strength, news sentiment, market
            regime). Empty string indicates a portfolio-scoped
            signal — uncommon in practice but tolerated.
        symbol_id: Numeric :class:`hedge_core::SymbolId` that was on
            the Hot_Path wire. Carried for trace correlation; not
            used by the ranker itself.
        side: Trade side. Carried for trace correlation; the
            score formula is direction-agnostic.
        base_probability: Signal_Engine-supplied raw alpha in
            ``[0.0, 1.0]``. Not used by the score; carried so the
            engine can log it for governance comparison.
        confidence: Signal_Engine-supplied confidence in
            ``[0.0, 1.0]``. The Risk_Engine uses this as the fallback
            ``SignalConfidence`` factor when the WarmCache rank entry
            is stale (design § Components § AI_Trade_Ranking_Engine).
        ts_ns: Signal_Engine-supplied wall-clock ns timestamp. Carried
            into :class:`AiRank.ts_ns` so subscribers see the
            source-side time, not the ranking engine's clock.
        shadow: ``True`` when the originating Signal_Engine had its
            ``shadow`` flag set (AI_Shadow_Mode, R24.3). The flag is
            forwarded into the ``ai.rank.<cid>`` payload so the UI
            and Risk_Engine know whether to act on the rank.
    """

    signal_id: str
    correlation_id: bytes
    symbol: str = ""
    symbol_id: int = 0
    side: Optional[Side] = None
    base_probability: float = 0.0
    confidence: float = 0.0
    ts_ns: int = 0
    shadow: bool = False


@dataclass(frozen=True, slots=True)
class AiRank:
    """Normalised view of one ``ai.rank.<correlation_id>`` emission.

    The engine produces one :class:`AiRank` per :class:`SignalEvent`,
    converts it to a :class:`hedge_warm_ai.schemas.RankedSignal` for
    the wire payload, and stores the same :class:`AiRank` in the
    WarmCache (interim Redis cache key namespace) so the
    :class:`Risk_Engine` can read the latest per-symbol rank without
    subscribing to the ``ai.rank.*`` fan-out.

    Attributes:
        correlation_id: 16-byte ``CorrelationId`` (matches
            :attr:`SignalEvent.correlation_id`).
        signal_id: Stable signal identifier echoed from the input.
        trade_confidence_score: ``Trade_Confidence_Score`` clamped to
            ``[0.0, 1.0]`` (R17.1, R17.2).
        factors: The exact :class:`RankingFactors` used to compute the
            score (so subscribers can trace which factor moved the
            score).
        symbol: Symbol identifier from the input. Used to compose the
            interim WarmCache key
            (``hedge:rag:cache:rank:<symbol>``) so the Risk_Engine
            can look up "the latest rank for symbol X" by symbol id.
        shadow: Forwarded from :attr:`SignalEvent.shadow` so
            shadow-mode rank emissions stay distinguishable on the
            wire (R24.3).
        ts_ns: Producer-side timestamp from
            :attr:`SignalEvent.ts_ns`. Carried into the
            :class:`RankedSignal` payload's ``ts_ns`` field.
    """

    correlation_id: bytes
    signal_id: str
    trade_confidence_score: float
    factors: RankingFactors
    symbol: str
    shadow: bool
    ts_ns: int


@dataclass(frozen=True, slots=True)
class RankingSample:
    """Outcome of one :meth:`AiTradeRankingEngine.rank` call.

    Returned to callers (and to the test suite) for assertion. The
    diagnostic bundle includes both the canonical
    :class:`AiRank` and a flag describing whether the WarmCache write
    succeeded — degraded WarmCache writes are non-fatal at the engine
    layer (the Risk_Engine has its own fallback path).
    """

    rank: AiRank
    cache_write_succeeded: bool
    publish_subject: str


__all__ = [
    "AiRank",
    "RankingSample",
    "Side",
    "SignalEvent",
]
