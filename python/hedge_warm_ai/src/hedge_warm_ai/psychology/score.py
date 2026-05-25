"""Trader_Stability_Score formula and ``BehaviorState`` factor model.

This module is the source of truth for the ``Trader_Stability_Score``
closed-form expression. It is invoked by
:class:`hedge_warm_ai.psychology.TraderPsychologyEngine` on every
behavioral event and again whenever a recompute is requested.

Formula (R16.2, Property 4 — Score and Formula Equivalence)::

    Trader_Stability_Score =
        clamp(0.35×Discipline + 0.25×EmotionalControl
              + 0.20×RiskConsistency + 0.20×Patience,
              0.0, 1.0)

The four weights sum to 1.0 by design so each factor is itself in
[0.0, 1.0] and the unclamped raw value is therefore always already in
[0.0, 1.0]; the explicit clamp is **kept** because:

1. The acceptance criterion R16.2 calls it out *verbatim*.
2. Float-arithmetic round-off can produce values like
   ``1.0000000000000002`` for ``D=E=R=P=1.0`` — the clamp keeps the
   wire payload schema-valid (``ai_psych_stability.schema.json`` has
   ``maximum: 1.0``).
3. We accept inputs *outside* [0.0, 1.0] without raising (matches
   Property 4 which says the *outputs* are bound, not the inputs); the
   clamp is the bound contract.

Property 4 is verified by task 25.2 against this exact function (the
test imports :func:`compute_trader_stability_score` directly), so the
formula's audit-trail is unambiguous: the design specifies it, this
module implements it as named constants, and the property test asserts
equivalence over the full input space.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

# ---------------------------------------------------------------------------
# Formula constants ---------------------------------------------------------
# ---------------------------------------------------------------------------

#: Weight of the ``Discipline`` factor in the
#: :func:`compute_trader_stability_score` formula (R16.2,
#: Property 4 — Score and Formula Equivalence).
DISCIPLINE_WEIGHT: Final[float] = 0.35

#: Weight of the ``EmotionalControl`` factor.
EMOTIONAL_CONTROL_WEIGHT: Final[float] = 0.25

#: Weight of the ``RiskConsistency`` factor.
RISK_CONSISTENCY_WEIGHT: Final[float] = 0.20

#: Weight of the ``Patience`` factor.
PATIENCE_WEIGHT: Final[float] = 0.20


# Sanity check: the four weights must sum to exactly 1.0 (the design
# specifies them as 0.35 + 0.25 + 0.20 + 0.20). This is asserted at
# module import time so a future edit that breaks the formula fails
# immediately rather than at score-emission time.
_WEIGHT_SUM: Final[float] = (
    DISCIPLINE_WEIGHT
    + EMOTIONAL_CONTROL_WEIGHT
    + RISK_CONSISTENCY_WEIGHT
    + PATIENCE_WEIGHT
)
assert _WEIGHT_SUM == 1.0, (
    f"Trader_Stability_Score weights must sum to 1.0 "
    f"(R16.2 / Property 4); got {_WEIGHT_SUM!r}"
)


# ---------------------------------------------------------------------------
# Behavior state ------------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class BehaviorState:
    """Live per-trader factor state consumed by the formula.

    The four fields are kept clipped to [0.0, 1.0] by the engine — the
    formula's clamp will keep the *output* in range even if a caller
    feeds out-of-range inputs, but the raw score is more meaningful
    when the inputs are normalised. The fields are mutable; the engine
    decays them in response to detector firings and gradually heals
    them when no detectors fire.
    """

    discipline: float = 1.0
    emotional_control: float = 1.0
    risk_consistency: float = 1.0
    patience: float = 1.0

    def clipped(self) -> "BehaviorState":
        """Return a copy with every factor clamped to [0.0, 1.0]."""
        return BehaviorState(
            discipline=_clamp_unit(self.discipline),
            emotional_control=_clamp_unit(self.emotional_control),
            risk_consistency=_clamp_unit(self.risk_consistency),
            patience=_clamp_unit(self.patience),
        )


# ---------------------------------------------------------------------------
# Formula -------------------------------------------------------------------
# ---------------------------------------------------------------------------


def _clamp_unit(value: float) -> float:
    """Clamp *value* to ``[0.0, 1.0]`` (NaN-safe)."""
    if value != value:  # NaN check: NaN != NaN
        return 0.0
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return value


def compute_trader_stability_score(s: BehaviorState) -> float:
    """Return ``Trader_Stability_Score`` exactly as specified in R16.2.

    The implementation mirrors the design pseudo-code byte-for-byte::

        raw = (
            0.35 * s.discipline
            + 0.25 * s.emotional_control
            + 0.20 * s.risk_consistency
            + 0.20 * s.patience
        )
        return clamp(raw, 0.0, 1.0)

    The named module-level weights (:data:`DISCIPLINE_WEIGHT` etc.) are
    used so the formula's audit-trail is unambiguous and a property
    test (task 25.2) can re-import the same constants to verify
    equivalence over the full input space (Property 4).

    Args:
        s: Live :class:`BehaviorState` — the four component factors.

    Returns:
        The clamped score in ``[0.0, 1.0]``.
    """
    raw = (
        DISCIPLINE_WEIGHT * s.discipline
        + EMOTIONAL_CONTROL_WEIGHT * s.emotional_control
        + RISK_CONSISTENCY_WEIGHT * s.risk_consistency
        + PATIENCE_WEIGHT * s.patience
    )
    return _clamp_unit(raw)


__all__ = [
    "BehaviorState",
    "DISCIPLINE_WEIGHT",
    "EMOTIONAL_CONTROL_WEIGHT",
    "PATIENCE_WEIGHT",
    "RISK_CONSISTENCY_WEIGHT",
    "compute_trader_stability_score",
]
