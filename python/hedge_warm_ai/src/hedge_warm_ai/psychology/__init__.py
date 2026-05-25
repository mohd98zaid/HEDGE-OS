"""Trader_Psychology_Engine — task 25.1 of the PROJECT HEDGE spec.

This sub-package implements the design's
*Trader_Psychology_Engine* (design § Components — Trader_Psychology_Engine)
and the requirements 16.1–16.7 from ``requirements.md``.

It does four things:

1. **Behavior detection.** A composable set of async detectors flag
   trader actions exhibiting revenge trading, FOMO entries,
   overconfidence, tilt, impulsive trading, rapid re-entry, stop-loss
   removal, or discipline deviation (R16.1).
2. **Stability scoring.** :func:`compute_trader_stability_score`
   implements the exact closed-form formula
   ``clamp(0.35×D + 0.25×E + 0.20×R + 0.20×P, 0.0, 1.0)`` (R16.2,
   Property 4 — Score and Formula Equivalence). The four weights are
   exposed as named module constants so the formula's audit-trail
   matches the design verbatim.
3. **Edge-triggered intervention.** A configurable threshold ladder
   (warning → cooldown → size_reduction → kill_switch, R16.4–R16.7)
   tracks the current intervention level. An
   :class:`PsychIntervention` event is emitted **only when the level
   changes** (Property 8 — Edge-Triggered Emission of State Changes).
4. **Persistence and downstream wiring.** Each behavioral event emits
   one ``ai.psych.stability`` payload to NATS and one
   :class:`PsychologyTimelinePoint` row to TimescaleDB via the
   ``hedge_memory_rag.timescale.TimescaleWriter.write_psychology_point``
   API. The Risk_Engine consumes the published interventions per the
   contract documented in design § Authority Hierarchy and Decision
   Flow.

The engine is **strictly off the Hot_Path**. It writes only to
``ai.psych.*`` subjects (NATS ACL: ``warm_ai`` account, R21.3) and never
attempts to publish on ``risk.*`` or ``exec.*``.
"""

from __future__ import annotations

from .ladder import (
    DEFAULT_LADDER_KEYS,
    InterventionAction,
    LadderTransition,
    ThresholdLadder,
    ladder_from_thresholds,
)
from .publisher import (
    InMemoryPsychPublisher,
    NatsPsychPublisher,
    NoopPsychPublisher,
    PsychPublisher,
    STABILITY_SUBJECT,
    INTERVENTION_SUBJECT,
)
from .score import (
    DISCIPLINE_WEIGHT,
    EMOTIONAL_CONTROL_WEIGHT,
    PATIENCE_WEIGHT,
    RISK_CONSISTENCY_WEIGHT,
    BehaviorState,
    compute_trader_stability_score,
)
from .state import (
    BehaviorEvent,
    BehaviorPattern,
    OrderState,
    Side,
    TraderAction,
    TraderActionKind,
    TraderActionLog,
)
from .detectors import (
    DETECTORS,
    Detector,
    detect_discipline_deviation,
    detect_fomo_entry,
    detect_impulsive_trading,
    detect_overconfidence,
    detect_rapid_re_entry,
    detect_revenge_trading,
    detect_stop_loss_removal,
    detect_tilt,
    run_detectors,
)
from .engine import (
    DEFAULT_RISK_CONSISTENCY_DECAY,
    PsychologySample,
    TraderPsychologyEngine,
)


__all__ = [
    # score
    "BehaviorState",
    "DISCIPLINE_WEIGHT",
    "EMOTIONAL_CONTROL_WEIGHT",
    "PATIENCE_WEIGHT",
    "RISK_CONSISTENCY_WEIGHT",
    "compute_trader_stability_score",
    # state
    "BehaviorEvent",
    "BehaviorPattern",
    "OrderState",
    "Side",
    "TraderAction",
    "TraderActionKind",
    "TraderActionLog",
    # detectors
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
    # ladder
    "DEFAULT_LADDER_KEYS",
    "InterventionAction",
    "LadderTransition",
    "ThresholdLadder",
    "ladder_from_thresholds",
    # publisher
    "INTERVENTION_SUBJECT",
    "InMemoryPsychPublisher",
    "NatsPsychPublisher",
    "NoopPsychPublisher",
    "PsychPublisher",
    "STABILITY_SUBJECT",
    # engine
    "DEFAULT_RISK_CONSISTENCY_DECAY",
    "PsychologySample",
    "TraderPsychologyEngine",
]
