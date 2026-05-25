"""Canonical NATS subjects + Redis namespaces for the AI_Governance_Engine.

The governance engine talks to three lanes:

* **NATS** — emits ``ai.gov.action`` whenever a per-component
  governance level transitions. Mirrors
  ``crates/hedge-bus/src/subject.rs::AI_GOV_ACTION``.
* **Redis (interim WarmCache)** — writes a per-component
  ``governance_weight`` multiplier the Risk_Engine and the
  AI_Trade_Ranking_Engine read through the WarmCache last-known-value
  path (R24.2). The dedicated ``hedge-warmcache`` crate (task 44.x)
  will adopt this namespace verbatim once it lands.
* **Redis (interim shadow flag)** — writes a per-component shadow
  flag the AI_Shadow_Mode service (task 29.1) consumes to halt the
  component's influence on the displayed ranking (R24.3, R23.2).

Subject + namespace strings live here so the engine never embeds raw
literals in business code. The Hot_Path Rust side declares matching
constants in ``hedge_bus::subject``::

    pub const AI_GOV_ACTION:    &str = "ai.gov.action";
    pub const EXEC_TRADE_CLOSED: &str = "exec.trade.closed";
    pub const POS_UPDATE:       &str = "pos.update";

References:
- ``crates/hedge-bus/src/subject.rs`` — Rust source of truth.
- Design § Components § AI_Governance_Engine (R23, R24).
"""

from __future__ import annotations

from typing import Final

# ---------------------------------------------------------------------------
# NATS subjects -------------------------------------------------------------
# ---------------------------------------------------------------------------

#: Edge-triggered "governance action" announcement. One event per
#: per-component level transition (Property 8). Producers: the
#: AI_Governance_Engine. Consumers: UI cockpit, Risk_Engine,
#: AI_Shadow_Mode service.
#: Mirrors ``hedge_bus::subject::AI_GOV_ACTION``.
SUBJECT_AI_GOV_ACTION: Final[str] = "ai.gov.action"

#: Hot_Path → Warm_AI_Pipeline trigger consumed by the engine to drive
#: the ``prediction_quality`` metric (R23.3, R24.1). Mirrors
#: ``hedge_bus::subject::EXEC_TRADE_CLOSED``.
SUBJECT_EXEC_TRADE_CLOSED: Final[str] = "exec.trade.closed"

#: Hot_Path → Warm_AI_Pipeline trigger consumed by the engine to derive
#: realised market outcomes from per-symbol position updates (R23.3).
#: The full subject is per-symbol: ``pos.update.<symbol_id>``. The
#: prefix is what subscribers register under when they want every
#: symbol's updates routed back to one handler. Mirrors
#: ``hedge_bus::subject::POS_UPDATE``.
SUBJECT_POS_UPDATE_PREFIX: Final[str] = "pos.update"


def pos_update_subject_pattern() -> str:
    """Return the NATS wildcard pattern matching every per-symbol update."""
    return f"{SUBJECT_POS_UPDATE_PREFIX}.*"


# ---------------------------------------------------------------------------
# Redis namespaces (interim until the Rust WarmCache crate ships) ----------
# ---------------------------------------------------------------------------

#: Per-component ``governance_weight`` multiplier namespace. Full key:
#: ``hedge.warm.governance.<component>``. Reading produces a JSON
#: payload with ``{"weight": float, "level": str, "ts_ns": int}`` so a
#: consumer can choose between strict equality on level or numerical
#: weight blending. The Risk_Engine and AI_Trade_Ranking_Engine read
#: this surface through the WarmCache last-known-value path.
DEFAULT_GOVERNANCE_WEIGHT_NAMESPACE: Final[str] = "hedge.warm.governance"

#: Per-component shadow flag namespace. Full key:
#: ``hedge.warm.shadow.<component>``. Presence of the key (TTL > 0)
#: indicates the component is currently in AI_Shadow_Mode (R24.3).
#: Absence of the key indicates the component is influencing the
#: displayed ranking. Task 29.1 (AI_Shadow_Mode service) consumes this
#: surface.
DEFAULT_SHADOW_FLAG_NAMESPACE: Final[str] = "hedge.warm.shadow"


def governance_weight_key(
    component: str,
    *,
    namespace: str = DEFAULT_GOVERNANCE_WEIGHT_NAMESPACE,
) -> str:
    """Compose the per-component governance-weight Redis key."""
    if not isinstance(component, str) or not component:
        raise ValueError(
            f"component must be a non-empty string; got {component!r}"
        )
    if "." in component:
        raise ValueError(
            f"component must not contain '.' separator: {component!r}"
        )
    return f"{namespace}.{component}"


def shadow_flag_key(
    component: str,
    *,
    namespace: str = DEFAULT_SHADOW_FLAG_NAMESPACE,
) -> str:
    """Compose the per-component shadow-flag Redis key."""
    if not isinstance(component, str) or not component:
        raise ValueError(
            f"component must be a non-empty string; got {component!r}"
        )
    if "." in component:
        raise ValueError(
            f"component must not contain '.' separator: {component!r}"
        )
    return f"{namespace}.{component}"


__all__ = [
    "DEFAULT_GOVERNANCE_WEIGHT_NAMESPACE",
    "DEFAULT_SHADOW_FLAG_NAMESPACE",
    "SUBJECT_AI_GOV_ACTION",
    "SUBJECT_EXEC_TRADE_CLOSED",
    "SUBJECT_POS_UPDATE_PREFIX",
    "governance_weight_key",
    "pos_update_subject_pattern",
    "shadow_flag_key",
]
