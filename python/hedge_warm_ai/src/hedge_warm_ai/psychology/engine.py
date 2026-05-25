"""Trader_Psychology_Engine — orchestrator for detection, scoring, ladder,
emission, and persistence (R16, design § Components — Trader_Psychology_Engine).

The engine is the only object the rest of the Warm_AI_Pipeline imports
to consume the psychology subpackage. Wire-up:

* ``TraderPsychologyEngine.observe(action)`` is called once per
  normalised :class:`TraderAction`. The service-layer (``hedge-psych``
  console script — see ``[project.scripts]`` in ``pyproject.toml``)
  is responsible for translating subscribed Hot_Path events
  (``exec.order.*``, ``exec.fill.*``, ``risk.decision.*``,
  ``pos.update.<sym>``) into :class:`TraderAction`.

  ``observe()``:

  1. appends the action to the engine's bounded
     :class:`hedge_warm_ai.psychology.state.TraderActionLog`,
  2. runs every detector in :data:`DETECTORS` against the action,
  3. updates the live :class:`BehaviorState` based on the fired events
     (each detector is mapped to one or two component factors so the
     scoring formula remains the verbatim R16.2 expression),
  4. recomputes :func:`compute_trader_stability_score`,
  5. emits ``ai.psych.stability`` to NATS via the
     :class:`PsychPublisher` (R16.3),
  6. asks the :class:`ThresholdLadder` for an edge-triggered transition
     and, if any, emits ``ai.psych.intervention`` (R16.4–R16.7,
     Property 8 — Edge-Triggered Emission of State Changes), and
  7. writes one :class:`PsychologyTimelinePoint` to TimescaleDB via
     :class:`PsychologyTimelineSink` (design § Components —
     Trader_Psychology_Engine: Persistence).

The Risk_Engine consumes the published interventions per the
authoritative Authority Hierarchy (design § Authority Hierarchy and
Decision Flow):

  * ``cooldown`` blocks new entries.
  * ``size_reduction`` reduces position sizing per the configured
    factor.
  * ``kill_switch`` activates the Kill_Switch.

This module does **not** itself talk to the Risk_Engine — it talks
exclusively to the bus on the ``ai.psych.*`` subjects. The Risk_Engine
subscribes (R5 / task 14.1) and the wiring is enforced at the NATS ACL
layer (task 7.1).
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Final, Mapping, Optional, Protocol, Sequence

import structlog

from ..schemas import PsychIntervention, PsychStability
from ..schemas.ai_psych_stability import StabilityComponents
from .detectors import DETECTORS, Detector, run_detectors
from .ladder import (
    LadderTransition,
    PUBLISHABLE_ACTIONS,
    ThresholdLadder,
)
from .publisher import NoopPsychPublisher, PsychPublisher
from .score import (
    BehaviorState,
    compute_trader_stability_score,
)
from .state import (
    BehaviorEvent,
    BehaviorPattern,
    DEFAULT_LOG_CAPACITY,
    TraderAction,
    TraderActionLog,
)

_LOG: Final = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Component impact map ------------------------------------------------------
# ---------------------------------------------------------------------------

#: Default decay applied to each component factor when its associated
#: behavior pattern fires. Multiplied by the event's :attr:`severity`.
#:
#: The mapping is deliberately conservative — a single fired detector
#: should not pull the score across multiple ladder rungs in one shot.
#: A value of ``0.20`` means a max-severity event drops the targeted
#: factor by 0.20 (20%). The factor is clipped to [0.0, 1.0] after each
#: update by :meth:`BehaviorState.clipped`.
_DEFAULT_COMPONENT_DECAYS: Final[Mapping[BehaviorPattern, Mapping[str, float]]] = {
    BehaviorPattern.REVENGE_TRADING: {
        "emotional_control": 0.30,
        "discipline": 0.10,
    },
    BehaviorPattern.FOMO_ENTRY: {
        "patience": 0.25,
        "discipline": 0.10,
    },
    BehaviorPattern.OVERCONFIDENCE: {
        "risk_consistency": 0.25,
        "discipline": 0.10,
    },
    BehaviorPattern.TILT: {
        "emotional_control": 0.30,
        "patience": 0.20,
    },
    BehaviorPattern.IMPULSIVE_TRADING: {
        "patience": 0.25,
        "discipline": 0.20,
    },
    BehaviorPattern.RAPID_RE_ENTRY: {
        "patience": 0.20,
    },
    BehaviorPattern.STOP_LOSS_REMOVED: {
        "discipline": 0.40,
        "risk_consistency": 0.30,
    },
    BehaviorPattern.DISCIPLINE_DEVIATION: {
        "discipline": 0.30,
    },
}

#: Per-observation passive recovery applied to every factor when no
#: detectors fire. Keeps the engine from getting "stuck" in a low
#: state once the trader's behavior normalises. Mirrors the design's
#: implicit assumption that the score returns to 1.0 in the absence
#: of negative signals.
DEFAULT_RISK_CONSISTENCY_DECAY: Final[float] = 0.05


# ---------------------------------------------------------------------------
# Persistence sink ----------------------------------------------------------
# ---------------------------------------------------------------------------


class PsychologyTimelineSink(Protocol):
    """Protocol for the persistence sink the engine writes to.

    The production binding is
    :class:`hedge_memory_rag.timescale.TimescaleWriter` — the engine
    builds a :class:`hedge_memory_rag.timescale.PsychologyTimelinePoint`
    record on every emission and calls
    :meth:`TimescaleWriter.write_psychology_point`. We accept any
    callable matching the structural signature so the unit tests can
    record points in memory without a live database.
    """

    async def write_psychology_point(self, point: object) -> int: ...


class _NoopTimelineSink:
    """Discard every point. Default when the sink is not yet wired."""

    async def write_psychology_point(self, point: object) -> int:
        return 0


# ---------------------------------------------------------------------------
# Sample --------------------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PsychologySample:
    """One produced psychology snapshot.

    Returned from :meth:`TraderPsychologyEngine.observe` and consumed by
    the test suite. Contains the canonical wire payload (so the test
    can assert on the JSON shape) plus the in-process :class:`BehaviorState`
    snapshot for white-box assertions.
    """

    stability: PsychStability
    state: BehaviorState
    fired: tuple[BehaviorEvent, ...]
    transition: Optional[LadderTransition]
    intervention: Optional[PsychIntervention]


# ---------------------------------------------------------------------------
# Engine --------------------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass
class TraderPsychologyEngine:
    """The Trader_Psychology_Engine.

    Construction:

    * ``ladder`` — :class:`ThresholdLadder` built from the live
      :class:`hedge_warm_ai.config.PsychologyThresholds` via
      :func:`hedge_warm_ai.psychology.ladder.ladder_from_thresholds`.
      The ladder's invariant
      ``critical < suppression < cooldown < warning`` is validated by
      both the config loader and the ladder constructor.
    * ``publisher`` — concrete :class:`PsychPublisher` (NATS-backed in
      production, in-memory in tests).
    * ``timeline_sink`` — :class:`PsychologyTimelineSink` (production:
      :class:`hedge_memory_rag.timescale.TimescaleWriter`).
    * ``state`` — initial :class:`BehaviorState`. Defaults to all-1.0
      so a fresh engine starts with the maximum stability score.
    * ``log_capacity`` — bounded log size; default
      :data:`hedge_warm_ai.psychology.state.DEFAULT_LOG_CAPACITY`.
    * ``recovery_per_observation`` — passive factor recovery applied
      when no detectors fire (default
      :data:`DEFAULT_RISK_CONSISTENCY_DECAY` = 0.05).

    Threading: the engine is intended to be driven by a single async
    task that receives Hot_Path → trader-action mappings serially. It
    is **not** safe to call :meth:`observe` concurrently from multiple
    tasks — there is no internal lock because the design's deployment
    topology runs one psychology engine per process per trader.
    """

    ladder: ThresholdLadder
    publisher: PsychPublisher = field(default_factory=NoopPsychPublisher)
    timeline_sink: PsychologyTimelineSink = field(default_factory=_NoopTimelineSink)
    state: BehaviorState = field(default_factory=BehaviorState)
    log_capacity: int = DEFAULT_LOG_CAPACITY
    recovery_per_observation: float = DEFAULT_RISK_CONSISTENCY_DECAY
    detectors: Sequence[Detector] = DETECTORS
    component_decays: Mapping[BehaviorPattern, Mapping[str, float]] = field(
        default_factory=lambda: _DEFAULT_COMPONENT_DECAYS
    )
    clock_ns: Callable[[], int] = field(default=time.time_ns)
    log: TraderActionLog = field(init=False)

    def __post_init__(self) -> None:
        self.log = TraderActionLog(capacity=self.log_capacity)

    # -- public API ---------------------------------------------------------

    async def observe(self, action: TraderAction) -> PsychologySample:
        """Process one normalised trader action.

        Returns a :class:`PsychologySample` describing what happened
        for the action: the fired detectors, the new component state,
        the recomputed score, and any intervention transition.
        """
        # 1. Append the action to the bounded log so detectors see it.
        self.log.append(action)

        # 2. Run every detector in order; collect fired events.
        fired = await run_detectors(action, self.log, self.detectors)

        # 3. Update component factors based on the fired events.
        self._update_state(fired)

        # 4. Recompute Trader_Stability_Score (R16.2 / Property 4).
        score = compute_trader_stability_score(self.state)

        # 5. Build and publish the ai.psych.stability payload (R16.3).
        stability = self._build_stability_event(action, score, fired)
        await self.publisher.publish_stability(stability)

        # 6. Persist the timeline point.
        await self._persist_timeline_point(stability)

        # 7. Edge-triggered intervention (R16.4–R16.7, Property 8).
        transition = self.ladder.transition(score)
        intervention: Optional[PsychIntervention] = None
        if transition is not None and transition.current in PUBLISHABLE_ACTIONS:
            intervention = PsychIntervention(
                action=transition.current.value,  # type: ignore[arg-type]
                trigger_score=score,
                ts_ns=stability.ts_ns,
            )
            await self.publisher.publish_intervention(intervention)

        return PsychologySample(
            stability=stability,
            state=self.state.clipped(),
            fired=tuple(fired),
            transition=transition,
            intervention=intervention,
        )

    async def recompute(self) -> float:
        """Force a stability recompute *without* a new action.

        Used by the service layer when an exogenous signal (e.g.
        configured cooldown elapsed) should re-evaluate the ladder
        rung. Emits ``ai.psych.intervention`` if the rung changes
        (Property 8 again).
        """
        score = compute_trader_stability_score(self.state)
        transition = self.ladder.transition(score)
        if transition is not None and transition.current in PUBLISHABLE_ACTIONS:
            event = PsychIntervention(
                action=transition.current.value,  # type: ignore[arg-type]
                trigger_score=score,
                ts_ns=self.clock_ns(),
            )
            await self.publisher.publish_intervention(event)
        return score

    # -- internals ----------------------------------------------------------

    def _update_state(self, fired: Sequence[BehaviorEvent]) -> None:
        """Apply per-event factor decay, then clip every factor to [0,1]."""
        if not fired:
            # Passive recovery toward 1.0.
            self.state = BehaviorState(
                discipline=min(1.0, self.state.discipline + self.recovery_per_observation),
                emotional_control=min(
                    1.0, self.state.emotional_control + self.recovery_per_observation
                ),
                risk_consistency=min(
                    1.0, self.state.risk_consistency + self.recovery_per_observation
                ),
                patience=min(1.0, self.state.patience + self.recovery_per_observation),
            )
            return

        # Sum decays per component across all fired events.
        decay_totals: dict[str, float] = {
            "discipline": 0.0,
            "emotional_control": 0.0,
            "risk_consistency": 0.0,
            "patience": 0.0,
        }
        for evt in fired:
            decays = self.component_decays.get(evt.pattern, {})
            for component, base in decays.items():
                decay_totals[component] += base * evt.severity

        new_state = BehaviorState(
            discipline=self.state.discipline - decay_totals["discipline"],
            emotional_control=self.state.emotional_control - decay_totals["emotional_control"],
            risk_consistency=self.state.risk_consistency - decay_totals["risk_consistency"],
            patience=self.state.patience - decay_totals["patience"],
        )
        # Clip every factor back to [0,1] so future scores remain
        # well-formed even after a long burst of negative events.
        self.state = new_state.clipped()

    def _build_stability_event(
        self,
        action: TraderAction,
        score: float,
        fired: Sequence[BehaviorEvent],
    ) -> PsychStability:
        """Produce the canonical ``ai.psych.stability`` payload."""
        # Names of fired patterns, deduplicated and capped at the
        # schema's ``items.maxItems: 32`` limit.
        seen: dict[str, None] = {}
        for evt in fired:
            seen[evt.pattern.value] = None
        names = list(seen.keys())[:32]

        components = StabilityComponents(
            discipline=self.state.discipline,
            emotional_control=self.state.emotional_control,
            risk_consistency=self.state.risk_consistency,
            patience=self.state.patience,
        )
        # Use the action's ts_ns when present, else the engine clock.
        ts_ns = action.ts_ns if action.ts_ns > 0 else self.clock_ns()
        return PsychStability(
            score=score,
            components=components,
            behaviors=names,
            ts_ns=ts_ns,
        )

    async def _persist_timeline_point(self, stability: PsychStability) -> None:
        """Write one row to ``psychology_timeline`` via the configured sink."""
        # Lazy-import :class:`PsychologyTimelinePoint` so the engine
        # remains importable in environments where ``hedge_memory_rag``
        # is not installed (e.g. unit tests of detectors only).
        try:
            from hedge_memory_rag.timescale import PsychologyTimelinePoint  # type: ignore
        except ImportError:
            # Memory_RAG_Layer not present; default sink is a noop in
            # this case anyway. Fall back to a structural object.
            await self.timeline_sink.write_psychology_point(stability)
            return

        ts = datetime.fromtimestamp(stability.ts_ns / 1_000_000_000, tz=timezone.utc)
        point = PsychologyTimelinePoint(
            ts=ts,
            score=stability.score,
            discipline=stability.components.discipline,
            emotional_control=stability.components.emotional_control,
            risk_consistency=stability.components.risk_consistency,
            patience=stability.components.patience,
            behaviors=list(stability.behaviors),
        )
        try:
            await self.timeline_sink.write_psychology_point(point)
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "psychology_timeline_persist_failed",
                score=stability.score,
                error=str(exc),
            )


__all__ = [
    "DEFAULT_RISK_CONSISTENCY_DECAY",
    "PsychologySample",
    "PsychologyTimelineSink",
    "TraderPsychologyEngine",
]
