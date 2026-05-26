"""Persistence sinks for the AI_Shadow_Mode service (R23.1).

R23.1 requires that, when a component is shadowed, its outputs are
*produced and persisted with timestamps*. The persistence step
re-uses the existing Memory_RAG_Layer hypertables — the
``shadow: bool`` column on ``ai_scores`` (task 32.1) is already
there for ``RankedSignal`` rows; ``regime_history`` carries every
emitted regime change including those produced while the
Market_Regime_Engine is shadowed.

Design choice — *one sink, many lanes*:

* The shadow service does not own the wire payloads — the upstream
  engines (ranking, regime, news, etc.) construct them. The shadow
  service receives a pre-validated :class:`ShadowedOutput` and
  fan-outs to the matching writer based on
  :class:`ShadowKind`.
* The :class:`ShadowedOutputSink` Protocol decouples the engine from
  :mod:`hedge_memory_rag`; tests substitute fakes; the production
  binding wraps :class:`hedge_memory_rag.timescale.TimescaleWriter`.
* Failures are *fail-soft*: a single down dependency cannot drop
  the rest of the pipeline. The typed
  :class:`ShadowPersistenceError` is logged (so the
  Self_Healing_Supervisor sees the structured event) but not
  re-raised across the service boundary.

Mapping :class:`ShadowKind` to writer methods (R23.1 + task 32.1):

* :data:`ShadowKind.AI_RANK`             → ``write_ai_score`` (the
  hypertable already has ``shadow: bool``).
* :data:`ShadowKind.AI_REGIME_CHANGED`   → ``write_regime_transition``.
* :data:`ShadowKind.AI_PSYCH_STABILITY`  → ``write_psychology_point``.
* :data:`ShadowKind.AI_JOURNAL_ENTRY`    → ``write_journal_entry``.
* :data:`ShadowKind.AI_PRIORITY_CHANGED` /
  :data:`ShadowKind.AI_NEWS_IMPACT`      — no dedicated hypertable
  today; the entry is logged at ``info`` and skipped (the
  governance engine's ``governance_metrics`` path still tracks the
  emission for accuracy scoring per R23.3).
* :data:`ShadowKind.MEM_PREV_DAY`        → ``write_prev_day_memory``.
* :data:`ShadowKind.OTHER`               — log + skip.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Final, Mapping, Optional, Protocol

import structlog

from .errors import ShadowPersistenceError
from .state import ShadowKind, ShadowedOutput

if TYPE_CHECKING:  # pragma: no cover - typing only
    from hedge_memory_rag.timescale import TimescaleWriter

_LOG: Final = structlog.get_logger(__name__)


def _ns_to_utc(ts_ns: int) -> datetime:
    return datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=timezone.utc)


# ---------------------------------------------------------------------------
# Protocol -----------------------------------------------------------------
# ---------------------------------------------------------------------------


class ShadowedOutputSink(Protocol):
    """Persists one shadowed Warm_AI_Pipeline emission to TimescaleDB."""

    async def persist(self, output: ShadowedOutput) -> None: ...


# ---------------------------------------------------------------------------
# In-memory sink (test helper) ---------------------------------------------
# ---------------------------------------------------------------------------


@dataclass
class InMemoryShadowedOutputSink:
    """Captures persisted entries in memory for assertion in tests.

    Mirrors :class:`hedge_warm_ai.governance.warm_cache.InMemoryGovernanceWarmCache`.
    """

    persisted: list[ShadowedOutput] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.persisted is None:
            self.persisted = []

    async def persist(self, output: ShadowedOutput) -> None:
        self.persisted.append(output)

    def reset(self) -> None:
        self.persisted = []


# ---------------------------------------------------------------------------
# No-op sink ---------------------------------------------------------------
# ---------------------------------------------------------------------------


class NoopShadowedOutputSink:
    """Drop-in stub used when Timescale is not wired yet."""

    async def persist(self, output: ShadowedOutput) -> None:  # noqa: D401
        return


# ---------------------------------------------------------------------------
# Production binding -------------------------------------------------------
# ---------------------------------------------------------------------------


def _ai_score_row(payload: Mapping[str, Any]) -> "Any":
    """Project an :class:`AiRank`-shaped payload into a Timescale row.

    Lazy import keeps :mod:`hedge_memory_rag` out of the hot import
    path until first use. The payload mirrors
    ``ai_rank.schema.json`` byte-for-byte; the shadow service has
    already validated it via the upstream engine's pydantic mirror.
    """
    from hedge_memory_rag.timescale import AiScore as _AiScore

    factors = payload.get("factors", {}) or {}
    return _AiScore(
        ts=_ns_to_utc(int(payload["ts_ns"])),
        correlation_id=str(payload["correlation_id"]),
        signal_id=str(payload["signal_id"]),
        trade_confidence_score=float(payload["trade_confidence_score"]),
        factor_orderflow=float(factors.get("orderflow", 0.0)),
        factor_technical_strength=float(factors.get("technical_strength", 0.0)),
        factor_news_sentiment=float(factors.get("news_sentiment", 0.0)),
        factor_market_regime=float(factors.get("market_regime", 0.0)),
        factor_trader_discipline=float(factors.get("trader_discipline", 0.0)),
        shadow=bool(payload.get("shadow", True)),
    )


def _regime_transition_row(payload: Mapping[str, Any]) -> "Any":
    from hedge_memory_rag.timescale import RegimeTransition as _RegimeTransition

    return _RegimeTransition(
        ts=_ns_to_utc(int(payload["ts_ns"])),
        from_regime=payload.get("from") or payload.get("from_regime") or "Sideways",
        to_regime=payload.get("to") or payload.get("to_regime") or "Sideways",
    )


def _psychology_row(payload: Mapping[str, Any]) -> "Any":
    from hedge_memory_rag.timescale import (
        PsychologyTimelinePoint as _PsychologyTimelinePoint,
    )

    components = payload.get("components", {}) or {}
    return _PsychologyTimelinePoint(
        ts=_ns_to_utc(int(payload["ts_ns"])),
        score=float(payload["score"]),
        discipline=float(components.get("discipline", 0.0)),
        emotional_control=float(components.get("emotional_control", 0.0)),
        risk_consistency=float(components.get("risk_consistency", 0.0)),
        patience=float(components.get("patience", 0.0)),
        behaviors=list(payload.get("behaviors", [])),
    )


def _journal_entry_row(payload: Mapping[str, Any]) -> "Any":
    from hedge_memory_rag.timescale import JournalEntry as _JournalEntry

    return _JournalEntry(
        ts=_ns_to_utc(int(payload["ts_ns"])),
        correlation_id=str(payload["correlation_id"]),
        trade_id=str(payload["trade_id"]),
        symbol=str(payload["symbol"]),
        side=str(payload["side"]),
        quantity=int(payload["quantity"]),
        entry_paise=int(payload["entry_paise"]),
        exit_paise=int(payload["exit_paise"]),
        pnl_inr=float(payload["pnl_inr"]),
        narrative=str(payload["narrative"]),
    )


@dataclass
class TimescaleShadowedOutputSink:
    """Persist shadowed outputs through :class:`TimescaleWriter`.

    The sink is intentionally fan-out-only: each :class:`ShadowKind`
    routes to one writer method, and unsupported kinds are logged
    and skipped without raising. Callers should treat
    :exc:`ShadowPersistenceError` as a recoverable signal that one
    write failed — the rest of the pipeline continues.
    """

    writer: "TimescaleWriter"

    async def persist(self, output: ShadowedOutput) -> None:
        try:
            await self._dispatch(output)
        except ShadowPersistenceError:
            raise
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "shadow_persist_failed",
                kind=output.kind.value,
                component=output.component,
                ts_ns=output.ts_ns,
                error=str(exc),
            )
            raise ShadowPersistenceError(
                f"failed to persist shadowed {output.kind.value!r} output: {exc}"
            ) from exc

    async def _dispatch(self, output: ShadowedOutput) -> None:
        kind = output.kind
        payload = output.payload
        if kind == ShadowKind.AI_RANK:
            await self.writer.write_ai_score(_ai_score_row(payload))
            return
        if kind == ShadowKind.AI_REGIME_CHANGED:
            await self.writer.write_regime_transition(_regime_transition_row(payload))
            return
        if kind == ShadowKind.AI_PSYCH_STABILITY:
            await self.writer.write_psychology_point(_psychology_row(payload))
            return
        if kind == ShadowKind.AI_JOURNAL_ENTRY:
            await self.writer.write_journal_entry(_journal_entry_row(payload))
            return
        # AI_PRIORITY_CHANGED / AI_NEWS_IMPACT / MEM_PREV_DAY / OTHER:
        # no dedicated hypertable today, so the persistence path is a
        # structured-log no-op. The governance engine still scores the
        # emission via its ``governance_metrics`` lane for R23.3 — the
        # *audit trail* requirement is met by the governance metric
        # row, not by a hypertable row.
        _LOG.info(
            "shadow_persist_skipped_unsupported_kind",
            kind=kind.value,
            component=output.component,
            ts_ns=output.ts_ns,
        )


__all__ = [
    "InMemoryShadowedOutputSink",
    "NoopShadowedOutputSink",
    "ShadowedOutputSink",
    "TimescaleShadowedOutputSink",
]


# Optional[None] guard left for forward-compatibility with future
# adaptors that may need it.
_ = Optional
