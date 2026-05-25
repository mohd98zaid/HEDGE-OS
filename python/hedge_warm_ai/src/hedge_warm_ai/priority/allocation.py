"""Per-tier resource allocation table for the Symbol_Priority_Engine (R14.2).

This module defines the read-only mapping

    tier → (cpu budget, AI inference budget, scan frequency Hz, alert frequency Hz)

that drives Hot_Path resource allocation for every tracked symbol.

Design intent
-------------

* The table is **read-only** at runtime: it is constructed once from
  the config layer (or from ``DEFAULT_ALLOCATION_TABLE``), wrapped in
  a frozen dataclass, and shared by reference. There is no ``set``
  surface; an operator changes resources by reloading config
  (R32 — config reload via SIGHUP for non-Hot_Path).
* Values are ratios of a CPU/inference quota (``0.0 .. 1.0``) plus
  absolute frequencies (``Hz``); the consuming Hot_Path crate
  translates them into concrete CPU shares and scan timer periods.
* The defaults below are deliberately monotonic in tier (P1 > P2 >
  P3 > P4) so the totality property never produces a non-sensical
  allocation if a symbol slips one tier.

The values currently come from
:func:`load_priority_allocation_table`'s ``defaults`` argument because
``hedge_config`` does not yet carry a ``priority_allocation`` block;
when task 32.x adds one, swap the default for a config-loader call.
The shape of the table is already what that block will surface.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Final, Mapping

from ..schemas.ai_priority_changed import PriorityTier

#: Canonical tuple of tiers, P1 → P4. Treat as immutable at runtime.
#: Defined here (the leaf module) to avoid a circular import between
#: :mod:`hedge_warm_ai.priority.engine` and :mod:`.allocation`. The
#: engine re-exports the same tuple for convenience.
PRIORITY_TIERS: Final[tuple[PriorityTier, ...]] = ("P1", "P2", "P3", "P4")


# ---------------------------------------------------------------------------
# Allocation row -----------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PriorityAllocation:
    """Resource allocation row for a single priority tier.

    Attributes:
        cpu_budget: Fraction of the Hot_Path CPU quota assigned to a
            symbol in this tier, ``0.0 .. 1.0``. The Hot_Path crate
            translates this into ``sched_setaffinity`` shares.
        ai_inference_budget: Fraction of the Warm_AI_Pipeline
            inference budget assigned to a symbol in this tier,
            ``0.0 .. 1.0``. Drives the rate at which the AI ranking
            and news engines pick the symbol off their backlog.
        scan_hz: Frequency at which the Signal_Engine scans the
            symbol's feature snapshot (Hz). Higher tiers scan more
            often; lower tiers scan less often.
        alert_hz: Frequency at which the Human_Control_UI surfaces
            updates for the symbol (Hz).

    Invariants:
        * Both budgets are constrained to ``[0.0, 1.0]``.
        * Frequencies are non-negative.
    """

    cpu_budget: float
    ai_inference_budget: float
    scan_hz: float
    alert_hz: float

    def __post_init__(self) -> None:
        for name, value in (
            ("cpu_budget", self.cpu_budget),
            ("ai_inference_budget", self.ai_inference_budget),
        ):
            if not (0.0 <= value <= 1.0):
                raise ValueError(
                    f"{name} must lie in [0.0, 1.0]; got {value!r}"
                )
        for name, value in (
            ("scan_hz", self.scan_hz),
            ("alert_hz", self.alert_hz),
        ):
            if value < 0.0:
                raise ValueError(f"{name} must be non-negative; got {value!r}")

    def as_dict(self) -> dict[str, float]:
        """Return a JSON-friendly dict (used by :class:`PriorityWarmCache`)."""
        return {
            "cpu_budget": float(self.cpu_budget),
            "ai_inference_budget": float(self.ai_inference_budget),
            "scan_hz": float(self.scan_hz),
            "alert_hz": float(self.alert_hz),
        }


# ---------------------------------------------------------------------------
# Allocation table ---------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PriorityAllocationTable:
    """Read-only mapping ``tier → PriorityAllocation`` (R14.2).

    The table covers every member of :data:`PriorityTier`
    (``P1 | P2 | P3 | P4``) — totality is enforced at construction
    time so callers can use ``table.get(tier)`` without a default.
    """

    rows: Mapping[PriorityTier, PriorityAllocation]

    def __post_init__(self) -> None:
        missing = set(PRIORITY_TIERS) - set(self.rows.keys())
        if missing:
            raise ValueError(
                "PriorityAllocationTable must cover every tier; "
                f"missing {sorted(missing)}"
            )
        unknown = set(self.rows.keys()) - set(PRIORITY_TIERS)
        if unknown:
            raise ValueError(
                "PriorityAllocationTable contains unknown tier(s): "
                f"{sorted(unknown)}"
            )
        # Ensure the mapping itself is read-only.
        object.__setattr__(self, "rows", MappingProxyType(dict(self.rows)))

    def get(self, tier: PriorityTier) -> PriorityAllocation:
        """Return the :class:`PriorityAllocation` for ``tier``.

        Raises:
            KeyError: never (``__post_init__`` enforces totality).
        """
        return self.rows[tier]

    def __getitem__(self, tier: PriorityTier) -> PriorityAllocation:
        return self.rows[tier]

    def items(self):  # noqa: ANN201 - thin proxy
        return self.rows.items()


# ---------------------------------------------------------------------------
# Defaults ------------------------------------------------------------------
# ---------------------------------------------------------------------------

#: Default allocation rows. Kept monotone in tier so a symbol slipping
#: one rank only loses, never gains, resources. Values are conservative
#: starting points sized for the ₹20,000 capital base (R32.4); they are
#: overridable via :func:`load_priority_allocation_table` once the
#: ``hedge_config`` ``priority_allocation`` block lands.
_DEFAULT_ROWS: Final[Mapping[PriorityTier, PriorityAllocation]] = MappingProxyType(
    {
        "P1": PriorityAllocation(
            cpu_budget=0.50, ai_inference_budget=0.50, scan_hz=10.0, alert_hz=10.0
        ),
        "P2": PriorityAllocation(
            cpu_budget=0.25, ai_inference_budget=0.25, scan_hz=5.0, alert_hz=5.0
        ),
        "P3": PriorityAllocation(
            cpu_budget=0.15, ai_inference_budget=0.15, scan_hz=2.0, alert_hz=2.0
        ),
        "P4": PriorityAllocation(
            cpu_budget=0.10, ai_inference_budget=0.10, scan_hz=1.0, alert_hz=1.0
        ),
    }
)

#: Default :class:`PriorityAllocationTable` constructed from
#: :data:`_DEFAULT_ROWS`. Production code resolves the table via
#: :func:`load_priority_allocation_table`; this constant is exported
#: for tests and as the fallback used until task 32.x extends
#: ``hedge_config`` with a dedicated block.
DEFAULT_ALLOCATION_TABLE: Final[PriorityAllocationTable] = PriorityAllocationTable(
    rows=_DEFAULT_ROWS
)


def load_priority_allocation_table(
    *,
    overrides: Mapping[PriorityTier, PriorityAllocation] | None = None,
) -> PriorityAllocationTable:
    """Resolve a :class:`PriorityAllocationTable` from config.

    The ``hedge_config`` schema does not yet carry a
    ``priority_allocation`` block (a follow-up task will add one). For
    now this loader returns :data:`DEFAULT_ALLOCATION_TABLE` merged
    with optional ``overrides`` so callers and tests can adjust a
    single tier without redefining the whole table.

    Args:
        overrides: Optional partial mapping. Each present tier
            replaces the default row; other tiers fall through.

    Returns:
        A frozen :class:`PriorityAllocationTable` covering every tier.
    """
    if not overrides:
        return DEFAULT_ALLOCATION_TABLE
    merged: dict[PriorityTier, PriorityAllocation] = dict(_DEFAULT_ROWS)
    for tier, row in overrides.items():
        merged[tier] = row
    return PriorityAllocationTable(rows=merged)


__all__ = [
    "DEFAULT_ALLOCATION_TABLE",
    "PRIORITY_TIERS",
    "PriorityAllocation",
    "PriorityAllocationTable",
    "load_priority_allocation_table",
]
