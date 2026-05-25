"""Symbol_Priority_Engine (R14, task 23.1).

This subpackage implements the Warm_AI_Pipeline component that assigns
each tracked symbol to exactly one priority tier (``P1 | P2 | P3 | P4``)
and edge-emits ``ai.priority.changed.<sym>`` whenever trader, regime,
or news inputs flip a symbol's tier.

Public surface
--------------

* :class:`PriorityAllocation`, :class:`PriorityAllocationTable`,
  :func:`load_priority_allocation_table` — the read-only mapping
  ``tier → (CPU budget, AI inference budget, scan Hz, alert Hz)``
  (R14.2).
* :class:`PriorityPolicy`, :class:`DefaultPriorityPolicy` — the
  pluggable strategy that combines trader / regime / news inputs into
  a final tier per symbol; trader intents win per R21
  (Authority_Hierarchy).
* :class:`SymbolPriorityEngine` — the core engine (R14.1, R14.3). It
  enforces the totality invariant (every tracked symbol carries a
  tier at all times) and emits exactly one
  :class:`hedge_warm_ai.schemas.PriorityChanged` per adjacent-pair
  tier change (Property 8).
* :class:`PriorityChangedPublisher`,
  :class:`InMemoryPriorityChangedPublisher`,
  :class:`NatsPriorityChangedPublisher`,
  :class:`NoopPriorityChangedPublisher` — sinks for the
  edge-triggered events.
* :class:`PriorityWarmCache` — bridge that writes the current tier
  and allocation into Redis under a dedicated ``hedge:warm:priority``
  namespace until the dedicated ``hedge-warmcache`` crate (task 44.x)
  lands. Hot_Path Rust consumers read this surface today and will be
  redirected to the WarmCache once it exists; see :doc:`README.md`.

References
----------
- Requirements §14 — Symbol Priority Allocation (R14.1–R14.4).
- Requirements §21 — Authority_Hierarchy (R21).
- Design § Components § Symbol_Priority_Engine.
- Design § Correctness Properties § Property 8 — Edge-Triggered
  Emission of State Changes.
"""

from __future__ import annotations

from .allocation import (
    DEFAULT_ALLOCATION_TABLE,
    PriorityAllocation,
    PriorityAllocationTable,
    load_priority_allocation_table,
)
from .cache import PriorityWarmCache
from .engine import (
    PRIORITY_TIERS,
    SymbolPriorityEngine,
    UnknownSymbolError,
)
from .policy import (
    DefaultPriorityPolicy,
    PriorityInputs,
    PriorityPolicy,
)
from .publisher import (
    AI_PRIORITY_CHANGED_PREFIX,
    InMemoryPriorityChangedPublisher,
    NatsPriorityChangedPublisher,
    NoopPriorityChangedPublisher,
    PriorityChangedPublisher,
    priority_subject,
)

__all__ = [
    # Allocation
    "DEFAULT_ALLOCATION_TABLE",
    "PriorityAllocation",
    "PriorityAllocationTable",
    "load_priority_allocation_table",
    # Engine
    "PRIORITY_TIERS",
    "SymbolPriorityEngine",
    "UnknownSymbolError",
    # Policy
    "DefaultPriorityPolicy",
    "PriorityInputs",
    "PriorityPolicy",
    # Publisher
    "AI_PRIORITY_CHANGED_PREFIX",
    "InMemoryPriorityChangedPublisher",
    "NatsPriorityChangedPublisher",
    "NoopPriorityChangedPublisher",
    "PriorityChangedPublisher",
    "priority_subject",
    # Cache
    "PriorityWarmCache",
]
