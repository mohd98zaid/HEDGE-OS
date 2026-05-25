"""Composable behavioral detectors (R16.1).

Each detector is a small ``async`` function with a uniform signature::

    async def detect_<name>(
        action: TraderAction,
        log: TraderActionLog,
    ) -> Optional[BehaviorEvent]: ...

This shape lets the engine compose detectors without bespoke wiring per
pattern and lets the test suite verify each detector in isolation.

Detectors are pure: they consult the action being processed, the
recently-logged actions, and nothing else. They never publish events
themselves — the engine collapses their outputs into one
``ai.psych.stability`` payload per observation (Property 8 — Edge-
Triggered Emission of State Changes for the *intervention* event; the
stability event itself is per-observation per R16.3).

The thresholds embedded in the detectors are per-detector behavioural
heuristics (e.g. *rapid re-entry* fires when two opposing-side actions
on the same symbol arrive within 5 seconds). They are intentionally
not part of the user-visible config surface — that surface is reserved
for the threshold ladder thresholds (R16.4–R16.7), which decide what
*action* is taken once the score has been computed. Future work may
externalise the per-detector windows behind a separate config block.
"""

from __future__ import annotations

from typing import Awaitable, Callable, Final, Optional, Sequence

from .state import (
    BehaviorEvent,
    BehaviorPattern,
    TraderAction,
    TraderActionKind,
    TraderActionLog,
)

# ---------------------------------------------------------------------------
# Detector type alias -------------------------------------------------------
# ---------------------------------------------------------------------------

#: Uniform signature for every detector. Returning ``None`` means the
#: detector did not fire for *action*.
Detector = Callable[
    [TraderAction, TraderActionLog],
    Awaitable[Optional[BehaviorEvent]],
]


# ---------------------------------------------------------------------------
# Heuristic constants -------------------------------------------------------
# ---------------------------------------------------------------------------

# These windows are deliberately small and conservative. The intent is
# to flag *patterns* that warrant a stability-score recompute, not to
# misdiagnose isolated trades. The ladder thresholds (R16.4–R16.7)
# decide the action; here we only decide whether a behavior fired.

# Re-entry on the same symbol within 5s of the previous fill or
# cancellation is flagged as ``RAPID_RE_ENTRY``.
_RAPID_RE_ENTRY_WINDOW_NS: Final[int] = 5 * 1_000_000_000

# A new entry placed within 10s of a *losing* exit on the same symbol
# is flagged as ``REVENGE_TRADING``.
_REVENGE_WINDOW_NS: Final[int] = 10 * 1_000_000_000

# The entry must be at least this big (in INR equivalent of ``quantity ×
# price_paise``) to count as revenge — single-share dust trades aren't
# revenge. We use INR rupees, not paise: 100 INR ~ 1% of base capital
# (R32.1: ``capital.base_inr = 20000``).
_REVENGE_MIN_NOTIONAL_INR: Final[float] = 100.0

# Three or more risk-rejected intents in 60s is ``IMPULSIVE_TRADING``.
_IMPULSIVE_WINDOW_NS: Final[int] = 60 * 1_000_000_000
_IMPULSIVE_REJECT_COUNT: Final[int] = 3

# Two or more *losing* fills in 30s on any symbol is ``TILT``.
_TILT_WINDOW_NS: Final[int] = 30 * 1_000_000_000
_TILT_LOSS_COUNT: Final[int] = 2

# Confidence below 0.40 on an entry that the trader still placed is
# ``DISCIPLINE_DEVIATION``. The threshold mirrors the
# ``trader_psychology.thresholds.suppression`` rung (default 0.4).
_DISCIPLINE_MIN_CONFIDENCE: Final[float] = 0.40

# Confidence above 0.85 combined with notional > 2× the configured
# base risk is ``OVERCONFIDENCE``. We use a static 200 INR ceiling
# corresponding to ``base_risk_per_trade_inr (= 100) × 2`` from the
# default config; the engine wires the live ceiling in once configured.
_OVERCONFIDENCE_MIN_CONFIDENCE: Final[float] = 0.85
_OVERCONFIDENCE_NOTIONAL_INR: Final[float] = 200.0

# An entry within 30s of a winning exit on the same symbol with
# confidence > 0.80 is ``FOMO_ENTRY``.
_FOMO_WINDOW_NS: Final[int] = 30 * 1_000_000_000
_FOMO_MIN_CONFIDENCE: Final[float] = 0.80


# ---------------------------------------------------------------------------
# Helpers -------------------------------------------------------------------
# ---------------------------------------------------------------------------


def _notional_inr(action: TraderAction) -> float:
    """Estimated INR notional of an action; returns 0 when undefined."""
    if action.quantity <= 0 or action.price_paise is None:
        return 0.0
    return (action.quantity * action.price_paise) / 100.0


def _is_entry(action: TraderAction) -> bool:
    return action.kind in (
        TraderActionKind.ORDER_SUBMITTED,
        TraderActionKind.ORDER_FILLED,
    )


def _is_losing_fill(action: TraderAction) -> bool:
    return action.kind == TraderActionKind.ORDER_FILLED and action.pnl_inr < 0.0


def _is_winning_fill(action: TraderAction) -> bool:
    return action.kind == TraderActionKind.ORDER_FILLED and action.pnl_inr > 0.0


# ---------------------------------------------------------------------------
# Detectors -----------------------------------------------------------------
# ---------------------------------------------------------------------------


async def detect_revenge_trading(
    action: TraderAction,
    log: TraderActionLog,
) -> Optional[BehaviorEvent]:
    """Fire when a sizeable entry follows a recent losing fill (same symbol).

    Heuristic: the action is an entry, *and* one of the last actions
    on the same symbol within :data:`_REVENGE_WINDOW_NS` was a losing
    fill, *and* the new entry's notional is >= the configured floor.
    """
    if not _is_entry(action):
        return None
    if _notional_inr(action) < _REVENGE_MIN_NOTIONAL_INR:
        return None

    cutoff = action.ts_ns - _REVENGE_WINDOW_NS
    for prior in log.recent(cutoff):
        if prior is action:
            continue
        if prior.symbol_id != action.symbol_id:
            continue
        if _is_losing_fill(prior):
            return BehaviorEvent(
                pattern=BehaviorPattern.REVENGE_TRADING,
                severity=min(1.0, abs(prior.pnl_inr) / max(1.0, _REVENGE_MIN_NOTIONAL_INR)),
                detail=f"recent_loss_pnl_inr={prior.pnl_inr:.2f}",
            )
    return None


async def detect_fomo_entry(
    action: TraderAction,
    log: TraderActionLog,
) -> Optional[BehaviorEvent]:
    """Fire when a high-confidence entry follows a recent winning fill on the same symbol."""
    if not _is_entry(action):
        return None
    if action.confidence is None or action.confidence < _FOMO_MIN_CONFIDENCE:
        return None

    cutoff = action.ts_ns - _FOMO_WINDOW_NS
    for prior in log.recent(cutoff):
        if prior is action:
            continue
        if prior.symbol_id != action.symbol_id:
            continue
        if _is_winning_fill(prior):
            return BehaviorEvent(
                pattern=BehaviorPattern.FOMO_ENTRY,
                severity=action.confidence,
                detail=f"chasing_recent_winner pnl_inr={prior.pnl_inr:.2f}",
            )
    return None


async def detect_overconfidence(
    action: TraderAction,
    log: TraderActionLog,
) -> Optional[BehaviorEvent]:
    """Fire when a high-confidence entry exceeds the over-sized notional ceiling."""
    if not _is_entry(action):
        return None
    if action.confidence is None or action.confidence < _OVERCONFIDENCE_MIN_CONFIDENCE:
        return None
    notional = _notional_inr(action)
    if notional < _OVERCONFIDENCE_NOTIONAL_INR:
        return None
    return BehaviorEvent(
        pattern=BehaviorPattern.OVERCONFIDENCE,
        severity=min(1.0, notional / (_OVERCONFIDENCE_NOTIONAL_INR * 2)),
        detail=f"confidence={action.confidence:.2f} notional_inr={notional:.2f}",
    )


async def detect_tilt(
    action: TraderAction,
    log: TraderActionLog,
) -> Optional[BehaviorEvent]:
    """Fire when at least :data:`_TILT_LOSS_COUNT` losing fills land in the last :data:`_TILT_WINDOW_NS`."""
    if action.kind != TraderActionKind.ORDER_FILLED:
        return None
    if action.pnl_inr >= 0.0:
        return None

    cutoff = action.ts_ns - _TILT_WINDOW_NS
    losing = sum(1 for p in log.recent(cutoff) if _is_losing_fill(p))
    # The current action is a losing fill that is already in the log
    # (the engine appends before running detectors). If the engine
    # ordering changes in the future, ``+1`` would double-count; we
    # therefore explicitly check identity below.
    if losing < _TILT_LOSS_COUNT:
        return None
    return BehaviorEvent(
        pattern=BehaviorPattern.TILT,
        severity=min(1.0, losing / (_TILT_LOSS_COUNT * 2)),
        detail=f"losing_fills_in_window={losing}",
    )


async def detect_impulsive_trading(
    action: TraderAction,
    log: TraderActionLog,
) -> Optional[BehaviorEvent]:
    """Fire when at least :data:`_IMPULSIVE_REJECT_COUNT` risk rejections land in the last :data:`_IMPULSIVE_WINDOW_NS`."""
    if action.kind != TraderActionKind.RISK_REJECTED:
        return None

    cutoff = action.ts_ns - _IMPULSIVE_WINDOW_NS
    rejects = sum(
        1 for p in log.recent(cutoff) if p.kind == TraderActionKind.RISK_REJECTED
    )
    if rejects < _IMPULSIVE_REJECT_COUNT:
        return None
    return BehaviorEvent(
        pattern=BehaviorPattern.IMPULSIVE_TRADING,
        severity=min(1.0, rejects / (_IMPULSIVE_REJECT_COUNT * 2)),
        detail=f"risk_rejections_in_window={rejects}",
    )


async def detect_rapid_re_entry(
    action: TraderAction,
    log: TraderActionLog,
) -> Optional[BehaviorEvent]:
    """Fire when an entry follows a closing action on the same symbol within :data:`_RAPID_RE_ENTRY_WINDOW_NS`."""
    if not _is_entry(action):
        return None

    cutoff = action.ts_ns - _RAPID_RE_ENTRY_WINDOW_NS
    for prior in log.recent(cutoff):
        if prior is action:
            continue
        if prior.symbol_id != action.symbol_id:
            continue
        if prior.kind in (
            TraderActionKind.ORDER_FILLED,
            TraderActionKind.ORDER_CANCELLED,
        ):
            elapsed_ns = max(1, action.ts_ns - prior.ts_ns)
            severity = max(0.0, 1.0 - (elapsed_ns / _RAPID_RE_ENTRY_WINDOW_NS))
            return BehaviorEvent(
                pattern=BehaviorPattern.RAPID_RE_ENTRY,
                severity=severity,
                detail=f"elapsed_ns={elapsed_ns}",
            )
    return None


async def detect_stop_loss_removal(
    action: TraderAction,
    log: TraderActionLog,
) -> Optional[BehaviorEvent]:
    """Fire whenever the upstream service flags a stop-loss removal.

    Stop-loss removal cannot be inferred from execution events alone;
    it is detected at the trader-intent layer (see :class:`TraderActionKind`
    docstring) and surfaces here as a typed action.
    """
    if action.kind != TraderActionKind.STOP_LOSS_REMOVED:
        return None
    return BehaviorEvent(
        pattern=BehaviorPattern.STOP_LOSS_REMOVED,
        severity=1.0,
        detail="stop_removed_pre_close",
    )


async def detect_discipline_deviation(
    action: TraderAction,
    log: TraderActionLog,
) -> Optional[BehaviorEvent]:
    """Fire when the trader places an entry below the discipline-confidence floor."""
    if not _is_entry(action):
        return None
    if action.confidence is None or action.confidence >= _DISCIPLINE_MIN_CONFIDENCE:
        return None
    return BehaviorEvent(
        pattern=BehaviorPattern.DISCIPLINE_DEVIATION,
        severity=1.0 - action.confidence,
        detail=f"low_confidence={action.confidence:.2f}",
    )


# ---------------------------------------------------------------------------
# Registry ------------------------------------------------------------------
# ---------------------------------------------------------------------------

#: Default detector tuple, in the order the engine runs them.
#:
#: Stop-loss removal is run first because it is unambiguous — a single
#: trader-intent diff produces it and there is no log scanning cost.
DETECTORS: Final[tuple[Detector, ...]] = (
    detect_stop_loss_removal,
    detect_revenge_trading,
    detect_fomo_entry,
    detect_overconfidence,
    detect_tilt,
    detect_impulsive_trading,
    detect_rapid_re_entry,
    detect_discipline_deviation,
)


async def run_detectors(
    action: TraderAction,
    log: TraderActionLog,
    detectors: Sequence[Detector] = DETECTORS,
) -> list[BehaviorEvent]:
    """Run *detectors* against (action, log) and return all fired events.

    Detectors run sequentially. They are async by interface but are
    expected to be cheap; running them sequentially keeps the order
    deterministic so the resulting ``behaviors`` array is stable for
    the test suite.
    """
    fired: list[BehaviorEvent] = []
    for detector in detectors:
        evt = await detector(action, log)
        if evt is not None:
            fired.append(evt)
    return fired


__all__ = [
    "DETECTORS",
    "Detector",
    "detect_discipline_deviation",
    "detect_fomo_entry",
    "detect_impulsive_trading",
    "detect_overconfidence",
    "detect_rapid_re_entry",
    "detect_revenge_trading",
    "detect_stop_loss_removal",
    "detect_tilt",
    "run_detectors",
]
