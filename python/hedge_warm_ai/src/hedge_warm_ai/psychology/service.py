"""Console-script entry point for the ``hedge-psych`` microservice (task C.10).

The Trader_Psychology_Engine watches the trader's order/fill/risk activity
and publishes:

* ``ai.psych.stability`` — the live Trader_Stability_Score + four-factor
  breakdown, emitted on every observed action AND on a ≥0.2 Hz heartbeat so
  the cockpit gauge always has a fresh value (R16.3, C.10 acceptance).
* ``ai.psych.intervention`` — edge-triggered when the stability score
  crosses a ladder rung (R16.4–R16.7).

Both are wrapped in the cockpit ``{kind, data}`` discriminated-union shape
(``PsychEvent``) the ``/psych`` reducer expects — the same shape the
demo-synth emits — by a thin cockpit publisher defined here, rather than the
engine's bare-payload :class:`NatsPsychPublisher`.

Inputs (best-effort; the service runs even if none arrive):

* ``exec.order.*``  → submitted / filled / cancelled / rejected actions.
* ``exec.fill.>``   → filled actions with realised P&L when present.
* ``risk.decision.rejected`` → risk-rejected actions.
* ``pos.update.>``  → position updates carrying realised P&L.

When no Hot_Path activity is flowing (e.g. outside trading hours), the
heartbeat still publishes the current score so the gauge never goes stale.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from typing import Optional, Sequence

import structlog

from ..config import PsychologyThresholds
from ..schemas import PsychIntervention, PsychStability
from ..service_runtime import NatsService, configure_logging
from .engine import TraderPsychologyEngine
from .ladder import ladder_from_thresholds
from .publisher import PsychPublisher
from .state import Side, TraderAction, TraderActionKind

_LOG = structlog.get_logger(__name__)

#: Heartbeat period. 4 s ≥ the 0.2 Hz floor in the C.10 acceptance criterion.
HEARTBEAT_PERIOD_S: float = 4.0


class CockpitPsychPublisher(PsychPublisher):
    """Publish psych events in the cockpit ``{kind, data}`` envelope.

    The cockpit ``/psych`` reducer expects a ``PsychEvent`` discriminated
    union (``{kind:"stability"|"intervention", data:{...}}``), matching the
    demo-synth output — NOT the bare ``PsychStability`` / ``PsychIntervention``
    payload the engine's :class:`NatsPsychPublisher` emits. This wrapper
    adapts the engine output to the cockpit shape.
    """

    def __init__(self, svc: NatsService) -> None:
        self._svc = svc

    async def publish_stability(self, event: PsychStability) -> None:
        payload = {"kind": "stability", "data": event.model_dump(mode="json")}
        await self._svc.publish(
            "ai.psych.stability",
            json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        )

    async def publish_intervention(self, event: PsychIntervention) -> None:
        payload = {"kind": "intervention", "data": event.model_dump(mode="json")}
        await self._svc.publish(
            "ai.psych.intervention",
            json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        )


def _action_from_exec_order(data: bytes) -> Optional[TraderAction]:
    """Map an ``exec.order.*`` cockpit ExecEvent into a TraderAction."""
    try:
        v = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(v, dict):
        return None
    inner = v.get("data", v)
    state = str(inner.get("state", "")).lower()
    kind_map = {
        "submitted": TraderActionKind.ORDER_SUBMITTED,
        "filled": TraderActionKind.ORDER_FILLED,
        "cancelled": TraderActionKind.ORDER_CANCELLED,
        "rejected": TraderActionKind.ORDER_REJECTED,
    }
    kind = kind_map.get(state)
    if kind is None:
        return None
    return _build_action(kind, inner)


def _action_from_fill(data: bytes) -> Optional[TraderAction]:
    try:
        v = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(v, dict):
        return None
    inner = v.get("data", v)
    return _build_action(TraderActionKind.ORDER_FILLED, inner)


def _action_from_risk_rejected(data: bytes) -> Optional[TraderAction]:
    try:
        v = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(v, dict):
        return None
    inner = v.get("data", v)
    return _build_action(TraderActionKind.RISK_REJECTED, inner)


def _action_from_pos_update(data: bytes) -> Optional[TraderAction]:
    try:
        v = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(v, dict):
        return None
    inner = v.get("data", v)
    return _build_action(TraderActionKind.POSITION_UPDATE, inner)


def _build_action(kind: TraderActionKind, inner: dict) -> TraderAction:
    side_raw = str(inner.get("side", "")).lower()
    side = Side.BUY if side_raw == "buy" else (Side.SELL if side_raw == "sell" else None)
    # Pull realised P&L from whichever field the producer used.
    pnl = (
        inner.get("realised_pnl_inr")
        or inner.get("realized_pnl_inr")
        or inner.get("pnl_inr")
        or 0.0
    )
    qty = inner.get("filled_qty") or inner.get("quantity") or 0
    return TraderAction(
        ts_ns=int(inner.get("ts_ns", 0) or time.time_ns()),
        kind=kind,
        side=side,
        quantity=int(qty) if isinstance(qty, (int, float)) else 0,
        pnl_inr=float(pnl) if isinstance(pnl, (int, float)) else 0.0,
        confidence=inner.get("confidence"),
    )


async def _run() -> int:
    configure_logging()
    parser = argparse.ArgumentParser(prog="hedge-psych")
    parser.add_argument("--check", action="store_true", help="Validate config and exit.")
    args = parser.parse_args()

    thresholds = PsychologyThresholds()
    if args.check:
        print(
            json.dumps(
                {
                    "thresholds": {
                        "warning": thresholds.warning,
                        "cooldown": thresholds.cooldown,
                        "suppression": thresholds.suppression,
                        "critical": thresholds.critical,
                    },
                    "heartbeat_period_s": HEARTBEAT_PERIOD_S,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    svc = await NatsService.connect("hedge-psych")
    publisher = CockpitPsychPublisher(svc)
    engine = TraderPsychologyEngine(
        ladder=ladder_from_thresholds(thresholds),
        publisher=publisher,
    )
    _LOG.info("hedge_psych_starting", heartbeat_s=HEARTBEAT_PERIOD_S)

    # Serialise engine access — observe() is documented single-task only.
    lock = asyncio.Lock()

    async def observe(action: Optional[TraderAction]) -> None:
        if action is None:
            return
        async with lock:
            await engine.observe(action)

    async def on_exec_order(_s: str, data: bytes) -> None:
        await observe(_action_from_exec_order(data))

    async def on_fill(_s: str, data: bytes) -> None:
        await observe(_action_from_fill(data))

    async def on_risk_rejected(_s: str, data: bytes) -> None:
        await observe(_action_from_risk_rejected(data))

    async def on_pos_update(_s: str, data: bytes) -> None:
        await observe(_action_from_pos_update(data))

    await svc.subscribe("exec.order.>", on_exec_order)
    await svc.subscribe("exec.fill.>", on_fill)
    await svc.subscribe("risk.decision.rejected", on_risk_rejected)
    await svc.subscribe("pos.update.>", on_pos_update)

    async def heartbeat(stop: asyncio.Event) -> None:
        """Publish the current stability score on a ≥0.2 Hz cadence.

        Uses the engine's score directly (no synthetic action) so the gauge
        stays fresh even when no Hot_Path activity is flowing. We build a
        :class:`PsychStability` from the engine's current state and publish
        it; the ladder transition is consulted via ``recompute`` so a
        crossing still emits an intervention.
        """
        from .score import compute_trader_stability_score
        from ..schemas.ai_psych_stability import StabilityComponents

        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=HEARTBEAT_PERIOD_S)
            except asyncio.TimeoutError:
                pass
            if stop.is_set():
                return
            async with lock:
                state = engine.state.clipped()
                score = compute_trader_stability_score(state)
                stability = PsychStability(
                    score=score,
                    components=StabilityComponents(
                        discipline=state.discipline,
                        emotional_control=state.emotional_control,
                        risk_consistency=state.risk_consistency,
                        patience=state.patience,
                    ),
                    behaviors=[],
                    ts_ns=time.time_ns(),
                )
                await publisher.publish_stability(stability)
                await engine.recompute()

    await svc.run_until(heartbeat)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for the ``hedge-psych`` console script."""
    try:
        return asyncio.run(_run())
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
