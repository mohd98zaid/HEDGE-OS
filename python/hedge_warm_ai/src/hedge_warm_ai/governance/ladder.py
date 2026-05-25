"""Per-component governance threshold ladder (R24.2, R24.3, Property 8).

The :class:`GovernanceLadder` maps a component's max-across-metrics
value to a discrete :class:`GovernanceLevel`:

* ``value < degradation_threshold`` → :data:`GovernanceLevel.NONE`
  (component is healthy, full influence weight)
* ``degradation_threshold ≤ value < critical_threshold``
  → :data:`GovernanceLevel.DEGRADED` (reduce influence weight in
  ``Trade_Confidence_Score`` and ``Adaptive_Risk`` per the configured
  policy — R24.2)
* ``value ≥ critical_threshold`` → :data:`GovernanceLevel.CRITICAL`
  (move component into AI_Shadow_Mode — R24.3)

Edge-triggered emission (Property 8 — Edge-Triggered Emission of State
Changes) is the main correctness invariant of this module: the ladder
emits a :class:`LadderTransition` **only** when the level changes,
mirrors the pattern :class:`hedge_warm_ai.psychology.ladder.ThresholdLadder`
established for the trader-stability ladder.

The thresholds come from
:class:`hedge_warm_ai.governance.config.GovernanceMetricThresholds`
which already enforces ``degradation < critical`` at load time;
nothing here is hardcoded. The ladder accepts raw floats so it can
also be constructed in tests without the full config.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Final, Optional

from .errors import GovernanceConfigError


class GovernanceLevel(str, Enum):
    """Discrete per-component governance level (R24.2, R24.3).

    Three values, ordered from least to most severe. The string
    values are persisted to TimescaleDB and projected into the
    canonical ``ai.gov.action.action`` enum on emission via
    :func:`action_for`.

    * ``NONE``     — component is healthy. No influence reduction,
                     no shadow mode. Emission is suppressed when the
                     ladder transitions *down* into ``NONE`` because
                     the canonical wire enum does not carry a "back to
                     normal" value; the engine instead emits
                     ``rollback`` so consumers can drop the previously
                     applied weight reduction or shadow flag.
    * ``DEGRADED`` — component crossed ``degradation_threshold``.
                     Emission carries ``action = "reduce_influence"``
                     (R24.2).
    * ``CRITICAL`` — component crossed ``critical_threshold``.
                     Emission carries ``action = "shadow_mode"`` and
                     the engine writes a flag to the
                     ``hedge.warm.shadow.<component>`` Redis key for
                     the AI_Shadow_Mode service (R24.3).
    """

    NONE = "none"
    DEGRADED = "degraded"
    CRITICAL = "critical"


def action_for(level: GovernanceLevel, *, previous: GovernanceLevel) -> Optional[str]:
    """Return the canonical ``ai.gov.action.action`` string for a transition.

    Mapping (Property 8 — only fires on level change):

    * any → ``DEGRADED`` → ``"reduce_influence"``
    * any → ``CRITICAL`` → ``"shadow_mode"``
    * any → ``NONE``     → ``"rollback"`` when ``previous`` was not
                            ``NONE`` (engine signal that the
                            previously-applied weight reduction or
                            shadow flag should be dropped); otherwise
                            ``None`` (suppress emission — there is no
                            edge to announce).

    The wire enum permits ``rollback``; it is the only reasonable
    choice for "back to normal" since the design's bounded action set
    does not contain a separate "clear" action.
    """
    if level == GovernanceLevel.DEGRADED:
        return "reduce_influence"
    if level == GovernanceLevel.CRITICAL:
        return "shadow_mode"
    # level == NONE
    if previous == GovernanceLevel.NONE:
        return None
    return "rollback"


@dataclass(frozen=True, slots=True)
class LadderTransition:
    """Edge-triggered transition from one governance level to another.

    Returned by :meth:`GovernanceLadder.transition` only when the
    level changes (Property 8). The engine uses this to decide
    whether to emit ``ai.gov.action`` and write a new shadow-flag /
    governance-weight Redis row.
    """

    previous: GovernanceLevel
    current: GovernanceLevel
    value: float
    threshold: float
    action: Optional[str]


@dataclass(slots=True)
class GovernanceLadder:
    """Stateful per-component governance level mapper.

    Attributes:
        degradation_threshold: Value at and above which the component
            transitions to :data:`GovernanceLevel.DEGRADED`.
        critical_threshold: Value at and above which the component
            transitions to :data:`GovernanceLevel.CRITICAL`. Must be
            strictly greater than ``degradation_threshold``.
        current: The current governance level. Engine startup sets
            this to :data:`GovernanceLevel.NONE`.

    Invariant (validated on construction):
        ``0.0 <= degradation_threshold < critical_threshold <= 1.0``.
        The same invariant is enforced at config-load time by
        :class:`hedge_warm_ai.governance.config.GovernanceMetricThresholds`,
        but we re-check here because the ladder accepts raw floats so
        it can also be constructed from non-config sources (e.g. a
        unit test).
    """

    degradation_threshold: float
    critical_threshold: float
    current: GovernanceLevel = GovernanceLevel.NONE

    def __post_init__(self) -> None:
        if not (0.0 <= self.degradation_threshold < self.critical_threshold <= 1.0):
            raise GovernanceConfigError(
                "GovernanceLadder requires "
                "0.0 <= degradation_threshold < critical_threshold <= 1.0; got "
                f"degradation_threshold={self.degradation_threshold}, "
                f"critical_threshold={self.critical_threshold}"
            )

    # -- lookup -------------------------------------------------------------

    def level_for(self, value: float) -> GovernanceLevel:
        """Return the :class:`GovernanceLevel` corresponding to *value*.

        The mapping is closed at the lower bound of each band
        (``threshold <= value < next_threshold``); a value *exactly
        equal* to a threshold is treated as the *more severe* side.
        """
        if value >= self.critical_threshold:
            return GovernanceLevel.CRITICAL
        if value >= self.degradation_threshold:
            return GovernanceLevel.DEGRADED
        return GovernanceLevel.NONE

    # -- edge-triggered transition -----------------------------------------

    def transition(self, value: float) -> Optional[LadderTransition]:
        """Update :attr:`current` and return a transition if the band changed.

        Property 8 — Edge-Triggered Emission of State Changes — is
        enforced here: a :class:`LadderTransition` is returned **only**
        when the new level differs from the previous one. The engine
        therefore emits ``ai.gov.action`` exactly once per crossing.
        """
        new_level = self.level_for(value)
        if new_level == self.current:
            return None
        previous = self.current
        self.current = new_level
        # Pick the threshold that was actually crossed for the wire
        # payload's ``threshold`` field. When transitioning *down* to
        # NONE we report the ``degradation_threshold`` (the boundary
        # the value just fell back below).
        if new_level == GovernanceLevel.CRITICAL:
            threshold = self.critical_threshold
        elif new_level == GovernanceLevel.DEGRADED:
            threshold = self.degradation_threshold
        else:
            threshold = self.degradation_threshold
        return LadderTransition(
            previous=previous,
            current=new_level,
            value=float(value),
            threshold=float(threshold),
            action=action_for(new_level, previous=previous),
        )

    def reset(self, level: GovernanceLevel = GovernanceLevel.NONE) -> None:
        """Force the ladder back to *level* without emitting a transition.

        Used on engine startup and on shutdown to avoid spurious
        transitions from the implicit :data:`GovernanceLevel.NONE`
        state when the engine attaches to an already-running session.
        """
        self.current = level


#: Per-level numeric weight multiplier applied to a component's
#: contribution in ``Trade_Confidence_Score`` and ``Adaptive_Risk``.
#:
#: * ``NONE``     → 1.0 (full influence)
#: * ``DEGRADED`` → 0.5 (halved influence per design § Components §
#:                  AI_Governance_Engine — "reduce influence weight")
#: * ``CRITICAL`` → 0.0 (shadowed; the AI_Shadow_Mode service is the
#:                  authoritative consumer, but consumers reading the
#:                  multiplier should default to "ignore this component")
#:
#: The exact numerical values are configurable per
#: :class:`hedge_warm_ai.governance.config.GovernanceConfig.weights`.
#: This module exposes the defaults as a module-level constant so the
#: engine and the property tests share one audit trail.
DEFAULT_WEIGHT_BY_LEVEL: Final[dict[GovernanceLevel, float]] = {
    GovernanceLevel.NONE: 1.0,
    GovernanceLevel.DEGRADED: 0.5,
    GovernanceLevel.CRITICAL: 0.0,
}


__all__ = [
    "DEFAULT_WEIGHT_BY_LEVEL",
    "GovernanceLadder",
    "GovernanceLevel",
    "LadderTransition",
    "action_for",
]
