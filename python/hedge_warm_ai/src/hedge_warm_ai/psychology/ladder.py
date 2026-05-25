"""Threshold ladder for stability-score → intervention mapping (R16.4–R16.7).

The ladder maps the live ``Trader_Stability_Score`` to a discrete
:class:`InterventionAction`:

* score >= warning_threshold      → no action
* cooldown   <= score < warning   → :data:`InterventionAction.WARNING`
* suppression <= score < cooldown → :data:`InterventionAction.COOLDOWN`
* critical    <= score < suppression → :data:`InterventionAction.SIZE_REDUCTION`
* score < critical                → :data:`InterventionAction.KILL_SWITCH`

The thresholds come from the existing config loader
(:class:`hedge_warm_ai.config.PsychologyThresholds` — task 6.1) which
already enforces the invariant ``critical < suppression < cooldown <
warning`` at load time. Nothing in this module is hardcoded; the rung
scores are passed in.

Edge-triggered emission (R16, Property 8 — Edge-Triggered Emission of
State Changes):

* The engine consults :meth:`ThresholdLadder.transition` on every
  recompute. ``transition()`` updates the internal "current action"
  and returns a :class:`LadderTransition` only when the action level
  *changed* (i.e. the score crossed a rung).
* Re-emission only fires on *transitions*, not on every recompute,
  so a long stretch of low scores produces exactly one
  ``ai.psych.intervention`` per crossing.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Final, Optional, Tuple

# ---------------------------------------------------------------------------
# Types ---------------------------------------------------------------------
# ---------------------------------------------------------------------------


class InterventionAction(str, Enum):
    """Discrete intervention level (R16.4–R16.7).

    The string values match the canonical
    ``ai_psych_intervention.schema.json`` ``action`` enum
    (``warning|cooldown|size_reduction|kill_switch``).
    """

    NONE = "none"
    WARNING = "warning"
    COOLDOWN = "cooldown"
    SIZE_REDUCTION = "size_reduction"
    KILL_SWITCH = "kill_switch"


#: Action levels that produce a wire ``ai.psych.intervention`` event.
#: :data:`InterventionAction.NONE` is the "above warning" level and is
#: never published — when the score recovers, we publish the new
#: lower-severity rung (or stop publishing entirely if the recovery
#: takes the score back above ``warning``).
PUBLISHABLE_ACTIONS: Final[frozenset[InterventionAction]] = frozenset(
    {
        InterventionAction.WARNING,
        InterventionAction.COOLDOWN,
        InterventionAction.SIZE_REDUCTION,
        InterventionAction.KILL_SWITCH,
    }
)


#: Stable, descending order of the ladder rungs by severity. Matches
#: the design's pseudo-code field order ``warning, cooldown,
#: suppression, critical``.
DEFAULT_LADDER_KEYS: Final[Tuple[str, ...]] = (
    "warning",
    "cooldown",
    "suppression",
    "critical",
)


@dataclass(frozen=True, slots=True)
class LadderTransition:
    """Edge-triggered transition from one action level to another."""

    previous: InterventionAction
    current: InterventionAction
    score: float


# ---------------------------------------------------------------------------
# Ladder --------------------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ThresholdLadder:
    """Stateful score-to-action mapper.

    The four threshold values (``warning, cooldown, suppression,
    critical``) are stored verbatim. The current action is updated by
    :meth:`transition` and exposed via :attr:`current_action`.

    Invariant (validated on construction): the four thresholds must
    satisfy ``critical < suppression < cooldown < warning``. The same
    invariant is enforced at config-load time by
    :class:`hedge_warm_ai.config.PsychologyThresholds`, but we re-check
    here because the ladder accepts raw floats so it can also be
    constructed from non-config sources (e.g. a unit test).
    """

    warning: float
    cooldown: float
    suppression: float
    critical: float
    current_action: InterventionAction = InterventionAction.NONE

    def __post_init__(self) -> None:
        if not (self.critical < self.suppression < self.cooldown < self.warning):
            raise ValueError(
                "ThresholdLadder requires "
                "critical < suppression < cooldown < warning; got "
                f"critical={self.critical}, suppression={self.suppression}, "
                f"cooldown={self.cooldown}, warning={self.warning}"
            )

    # -- lookup -------------------------------------------------------------

    def action_for(self, score: float) -> InterventionAction:
        """Return the :class:`InterventionAction` corresponding to *score*.

        The mapping is closed at the lower bound of each rung
        (``rung_low <= score < rung_high``); a score *exactly equal* to
        a threshold is treated as the *less severe* side.
        """
        if score >= self.warning:
            return InterventionAction.NONE
        if score >= self.cooldown:
            return InterventionAction.WARNING
        if score >= self.suppression:
            return InterventionAction.COOLDOWN
        if score >= self.critical:
            return InterventionAction.SIZE_REDUCTION
        return InterventionAction.KILL_SWITCH

    # -- edge-triggered transition -----------------------------------------

    def transition(self, score: float) -> Optional[LadderTransition]:
        """Update :attr:`current_action` and return a transition if the rung changed.

        Property 8 — Edge-Triggered Emission of State Changes — is
        enforced here: a :class:`LadderTransition` is returned **only**
        when the new action differs from the previous one. The engine
        can therefore emit ``ai.psych.intervention`` exactly once per
        crossing.
        """
        new_action = self.action_for(score)
        if new_action == self.current_action:
            return None
        previous = self.current_action
        self.current_action = new_action
        return LadderTransition(
            previous=previous, current=new_action, score=float(score)
        )

    def reset(self, action: InterventionAction = InterventionAction.NONE) -> None:
        """Force the ladder back to *action* without emitting a transition.

        Used on engine startup and on shutdown to avoid spurious
        transitions from the implicit ``NONE`` state when the engine
        attaches to an already-running session.
        """
        self.current_action = action


# ---------------------------------------------------------------------------
# Constructors --------------------------------------------------------------
# ---------------------------------------------------------------------------


def ladder_from_thresholds(thresholds: object) -> ThresholdLadder:
    """Build a :class:`ThresholdLadder` from
    :class:`hedge_warm_ai.config.PsychologyThresholds` (or any object
    that exposes the same four ``float`` attributes).

    We accept a duck-typed argument rather than importing the config
    type directly to keep the psychology subpackage self-contained.
    """
    return ThresholdLadder(
        warning=float(thresholds.warning),  # type: ignore[attr-defined]
        cooldown=float(thresholds.cooldown),  # type: ignore[attr-defined]
        suppression=float(thresholds.suppression),  # type: ignore[attr-defined]
        critical=float(thresholds.critical),  # type: ignore[attr-defined]
    )


__all__ = [
    "DEFAULT_LADDER_KEYS",
    "InterventionAction",
    "LadderTransition",
    "PUBLISHABLE_ACTIONS",
    "ThresholdLadder",
    "ladder_from_thresholds",
]
