"""Adapter forwarding shadowed outputs to the AI_Governance_Engine (R23.3).

R23.3 requires the AI_Governance_Engine to compare shadowed AI
outputs against actual subsequent market outcomes and produce
accuracy metrics per shadowed component. Concretely: shadowed
outputs must continue to reach the governance engine's
:meth:`AiGovernanceEngine.observe` API even after the UI gateway
filter has dropped them from the trader-facing ranking display.

This module is the explicit seam between the shadow service and the
governance engine:

* :class:`GovernanceObserver` Protocol — abstract sink the shadow
  service calls per emission.
* :class:`AiGovernanceEngineObserver` — production binding wrapping
  :meth:`AiGovernanceEngine.observe`. Lazy import keeps the
  governance subpackage's heavy dependencies out of the shadow
  subpackage's import path.
* :class:`InMemoryGovernanceObserver` — captures forwarded
  observations in memory for assertion in tests.
* :class:`NoopGovernanceObserver` — drop-in stub when the
  governance engine is not wired yet.

Documentation invariant
=======================

**Shadowed outputs are NOT filtered out of the governance metric
path; only the UI ranked-signal channel is filtered.** The shadow
service's :meth:`ShadowModeService.persist_output` calls (1) the
:class:`ShadowedOutputSink` (Timescale) and (2) the
:class:`GovernanceObserver` for every shadowed emission, regardless
of whether the UI gateway later drops the payload. Test fixtures
(task 29.2) assert this property by feeding a shadowed
:class:`RankedSignal` into the service and verifying:

* the persistence sink received the row, AND
* the governance observer received the matching
  :class:`ComponentOutput`, AND
* the UI filter returns ``False`` for the same payload.

Without this invariant the engine could not compute accuracy
metrics on shadowed components — defeating the entire purpose of
shadow mode.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import RLock
from typing import TYPE_CHECKING, Any, Final, Mapping, Optional, Protocol

import structlog

from ..governance.state import ComponentOutput, GovernedComponent
from .state import ShadowKind, ShadowedOutput

if TYPE_CHECKING:  # pragma: no cover - typing only
    from ..governance.engine import AiGovernanceEngine, GovernanceSample

_LOG: Final = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Protocol -----------------------------------------------------------------
# ---------------------------------------------------------------------------


class GovernanceObserver(Protocol):
    """Sink forwarding shadowed outputs to the AI_Governance_Engine."""

    async def forward(self, output: ShadowedOutput) -> None: ...


# ---------------------------------------------------------------------------
# In-memory observer (test helper) -----------------------------------------
# ---------------------------------------------------------------------------


@dataclass
class InMemoryGovernanceObserver:
    """Captures every forwarded :class:`ShadowedOutput` for assertion."""

    _lock: RLock = field(default_factory=RLock, init=False)
    _forwarded: list[ShadowedOutput] = field(default_factory=list, init=False)

    async def forward(self, output: ShadowedOutput) -> None:
        with self._lock:
            self._forwarded.append(output)

    @property
    def forwarded(self) -> list[ShadowedOutput]:
        with self._lock:
            return list(self._forwarded)

    def reset(self) -> None:
        with self._lock:
            self._forwarded.clear()


# ---------------------------------------------------------------------------
# No-op observer -----------------------------------------------------------
# ---------------------------------------------------------------------------


class NoopGovernanceObserver:
    """Drop-in stub used when the governance engine is not wired yet."""

    async def forward(self, output: ShadowedOutput) -> None:  # noqa: D401
        return


# ---------------------------------------------------------------------------
# Production binding -------------------------------------------------------
# ---------------------------------------------------------------------------


def _payload_confidence(kind: ShadowKind, payload: Mapping[str, Any]) -> Optional[float]:
    """Project a shadowed payload's confidence-like signal to ``[0.0, 1.0]``.

    Used by the governance engine's
    :class:`MetricKind.CONFIDENCE_STABILITY` window. Components that
    do not emit a confidence value (regime, journal, prev_day) yield
    ``None`` — the governance engine handles a missing confidence
    gracefully.
    """
    if kind == ShadowKind.AI_RANK:
        try:
            value = float(payload.get("trade_confidence_score", 0.0))
        except (TypeError, ValueError):
            return None
        return max(0.0, min(1.0, value))
    if kind == ShadowKind.AI_PSYCH_STABILITY:
        try:
            value = float(payload.get("score", 0.0))
        except (TypeError, ValueError):
            return None
        return max(0.0, min(1.0, value))
    if kind == ShadowKind.AI_NEWS_IMPACT:
        try:
            value = float(payload.get("impact_magnitude", 0.0))
        except (TypeError, ValueError):
            return None
        return max(0.0, min(1.0, value))
    return None


def _payload_correlation_id(
    payload: Mapping[str, Any], fallback: str | None
) -> str:
    raw = payload.get("correlation_id") or fallback or ""
    if isinstance(raw, str):
        return raw
    return str(raw)


def _shadowed_output_to_component_output(
    output: ShadowedOutput,
) -> Optional[ComponentOutput]:
    """Translate a :class:`ShadowedOutput` into a governance observation.

    Returns ``None`` when ``output.component`` is not one of the
    canonical :class:`GovernedComponent` values — those components
    are not tracked by the governance engine's metric estimators
    today, so forwarding would raise.
    """
    try:
        component = GovernedComponent(output.component)
    except ValueError:
        return None
    confidence = _payload_confidence(output.kind, output.payload)
    correlation_id = _payload_correlation_id(output.payload, output.correlation_id)
    return ComponentOutput(
        component=component,
        confidence=confidence,
        feature_vector=(),
        hallucination_flag=False,
        correlation_id=correlation_id,
        ts_ns=int(output.ts_ns),
    )


@dataclass
class AiGovernanceEngineObserver:
    """Forwards shadowed outputs into :meth:`AiGovernanceEngine.observe`.

    The engine receives the same :class:`ComponentOutput` shape it
    receives from non-shadowed emissions — the governance metric
    path treats shadowed and non-shadowed outputs identically, which
    is the exact behaviour R23.3 mandates (the engine produces
    *accuracy metrics per shadowed component*).
    """

    engine: "AiGovernanceEngine"

    async def forward(self, output: ShadowedOutput) -> None:
        observation = _shadowed_output_to_component_output(output)
        if observation is None:
            _LOG.info(
                "shadow_governance_forward_skipped_unknown_component",
                component=output.component,
                kind=output.kind.value,
            )
            return
        try:
            await self.engine.observe(observation)
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "shadow_governance_forward_failed",
                component=output.component,
                kind=output.kind.value,
                error=str(exc),
            )


__all__ = [
    "AiGovernanceEngineObserver",
    "GovernanceObserver",
    "InMemoryGovernanceObserver",
    "NoopGovernanceObserver",
]


# Keep a forward reference to ``GovernanceSample`` accessible for
# downstream tools that introspect the module's public API.
_ = "GovernanceSample"
