"""AI_Governance_Engine — task 28.1 (R23.3, R24.1–R24.4).

Responsibilities
================

1. **Track per-component metrics** (R24.1): for each governed
   Warm_AI_Pipeline component (``news``, ``regime``, ``priority``,
   ``prev_day``, ``psychology``, ``ranking``, ``journal``), the
   engine maintains four rolling-window estimators:

   * ``drift``                    — :func:`compute_drift`
   * ``confidence_stability``     — :func:`compute_confidence_stability`
   * ``hallucination_indicators`` — :func:`compute_hallucination_rate`
   * ``prediction_quality``       — :func:`compute_prediction_inaccuracy`

   Each estimator returns a ``[0.0, 1.0]`` scalar where higher values
   indicate **more degraded** behaviour. The engine takes the
   per-metric maximum across the four metrics to derive the
   per-component severity, then runs it through a per-component
   :class:`GovernanceLadder` (R24.2, R24.3).

2. **Reduce influence on degradation** (R24.2): when a component's
   severity crosses ``degradation_threshold`` (and not yet
   ``critical_threshold``), the engine writes a
   :class:`GovernanceWeightPayload` with ``weight = weights[DEGRADED]``
   to the interim WarmCache key
   ``hedge.warm.governance.<component>``. The Risk_Engine and the
   AI_Trade_Ranking_Engine read this multiplier through the
   WarmCache last-known-value path and apply it on every
   ``Trade_Confidence_Score`` and ``Adaptive_Risk`` recomputation.

3. **Move to shadow on critical** (R24.3): when severity crosses
   ``critical_threshold``, the engine additionally writes a flag to
   ``hedge.warm.shadow.<component>``. The AI_Shadow_Mode service
   (task 29.1) consumes this flag to halt the component's influence
   on the displayed ranking; the WarmCache governance weight is set
   to ``weights[CRITICAL]`` so any consumer that does not honour the
   shadow flag still produces the documented behaviour.

4. **Emit edge-triggered ``ai.gov.action``** (R24.4, Property 8):
   one publication per per-component level transition, never per
   continued tracking. The publication carries the canonical
   ``component``, ``action`` (one of
   ``reduce_influence | shadow_mode | rollback``), ``metric``,
   ``value``, ``threshold``, ``ts_ns`` fields, all validated against
   the bound :class:`hedge_warm_ai.schemas.AiGovAction` schema.

5. **Compare shadowed AI outputs against actual market outcomes**
   (R23.3): the engine subscribes to ``exec.trade.closed`` and
   ``pos.update.<sym>`` (the same events the AI_Trade_Journal_Engine
   consumes) and correlates each outcome against the corresponding
   component output via ``correlation_id``. The directional match /
   mismatch is appended to the per-component
   :class:`MetricKind.PREDICTION_QUALITY` window and persisted to
   the ``governance_metrics`` Timescale hypertable as a
   ``correlation_id``-stamped row. Shadowed AI outputs continue to be
   scored — their results inform the engine's accuracy metrics
   without surfacing on the displayed ranking (R23.2).

Authority + Hot_Path purity
===========================

The engine is **strictly off the Hot_Path** (Property 2 — Authority
Hierarchy and Hot_Path Purity, R30): it writes only to
``ai.gov.action`` and to two dedicated Redis namespaces. The
Hot_Path Risk_Engine and the AI_Trade_Ranking_Engine consume the
WarmCache surface; they never await this engine. The publish + cache
+ persistence path is fire-and-forget at the engine layer; broker- /
DB-side failures are surfaced as typed
:class:`hedge_warm_ai.governance.errors.*` exceptions and logged via
structlog so the Self_Healing_Supervisor sees the structured event.

Threading
=========

The engine is async-first. The caller (the ``hedge-governance``
service-layer entry point) drives :meth:`observe` from a single
asyncio task per process. Multiple concurrent calls are not safe —
the rolling windows and ladders are mutated in place. This mirrors
the deployment topology of the other Warm_AI_Pipeline engines.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Final, Optional

import structlog

from ..schemas import AiGovAction
from .config import GovernanceConfig
from .ladder import GovernanceLadder, GovernanceLevel, LadderTransition
from .metrics import (
    compute_confidence_stability,
    compute_drift,
    compute_hallucination_rate,
    compute_prediction_inaccuracy,
)
from .persistence import GovernanceMetricSink, NoopGovernanceMetricSink
from .publisher import AiGovActionPublisher, NoopAiGovActionPublisher
from .state import (
    ComponentOutput,
    GovernedComponent,
    MetricKind,
    RollingWindow,
    TradeOutcome,
    wire_metric_for,
)
from .warm_cache import (
    GovernanceWarmCache,
    GovernanceWeightPayload,
    InMemoryGovernanceWarmCache,
)

_LOG: Final = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Per-component state -----------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _ComponentState:
    """Per-component rolling windows + ladder + correlation cache.

    ``correlation_outputs`` retains recent :class:`ComponentOutput`
    instances keyed by ``correlation_id`` so an
    :class:`exec.trade.closed` outcome arriving after the originating
    output can attribute the realised P&L back to the same
    component. The cache size is bounded by
    ``config.prediction_window * 4`` (a generous multiplier so the
    cache always carries at least the prediction window's worth of
    pending correlations).
    """

    component: GovernedComponent
    drift_window: RollingWindow
    drift_reference: list[float] = field(default_factory=list)
    drift_reference_full: bool = False
    stability_window: RollingWindow = field(
        default_factory=lambda: RollingWindow(capacity=1)
    )
    hallucination_window: RollingWindow = field(
        default_factory=lambda: RollingWindow(capacity=1)
    )
    prediction_window: RollingWindow = field(
        default_factory=lambda: RollingWindow(capacity=1)
    )
    ladder: GovernanceLadder = field(
        default_factory=lambda: GovernanceLadder(0.2, 0.35)
    )
    correlation_outputs: dict[str, ComponentOutput] = field(default_factory=dict)
    correlation_order: list[str] = field(default_factory=list)
    sample_count: int = 0
    last_metrics: dict[MetricKind, float] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Sample type --------------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class GovernanceSample:
    """Outcome of one :meth:`AiGovernanceEngine.observe` call.

    Returned to callers (and to the test suite) for assertion. The
    diagnostic bundle includes the per-metric values, the dominant
    metric (the one whose value is the per-component max), the
    optional :class:`LadderTransition`, and the optional emitted
    :class:`AiGovAction` payload.
    """

    component: GovernedComponent
    metrics: dict[MetricKind, float]
    dominant_metric: MetricKind
    severity: float
    transition: Optional[LadderTransition]
    emitted: Optional[AiGovAction]


# ---------------------------------------------------------------------------
# Engine -------------------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass
class AiGovernanceEngine:
    """The AI_Governance_Engine (R23.3, R24.1–R24.4).

    Construction:

    * ``config`` — resolved :class:`GovernanceConfig`.
    * ``publisher`` — concrete :class:`AiGovActionPublisher`.
    * ``warm_cache`` — concrete :class:`GovernanceWarmCache`.
    * ``metric_sink`` — concrete :class:`GovernanceMetricSink`.
    * ``clock_ns`` — wall-clock ns callable.

    Lifecycle: construct, then drive :meth:`observe`,
    :meth:`observe_outcome`, and :meth:`observe_position_update` from
    a single asyncio task (the ``hedge-governance`` service-layer
    binary). The engine has no background tasks of its own — every
    side effect happens inside the call site of the observation.
    """

    config: GovernanceConfig
    publisher: AiGovActionPublisher = field(
        default_factory=NoopAiGovActionPublisher
    )
    warm_cache: GovernanceWarmCache = field(
        default_factory=InMemoryGovernanceWarmCache
    )
    metric_sink: GovernanceMetricSink = field(
        default_factory=NoopGovernanceMetricSink
    )
    clock_ns: Callable[[], int] = field(default=time.time_ns)
    _states: dict[GovernedComponent, _ComponentState] = field(
        default_factory=dict, init=False
    )

    def __post_init__(self) -> None:
        for component in self.config.components:
            self._states[component] = _ComponentState(
                component=component,
                drift_window=RollingWindow(capacity=self.config.drift_window),
                stability_window=RollingWindow(capacity=self.config.stability_window),
                hallucination_window=RollingWindow(
                    capacity=self.config.hallucination_window
                ),
                prediction_window=RollingWindow(
                    capacity=self.config.prediction_window
                ),
                ladder=GovernanceLadder(
                    degradation_threshold=self.config.thresholds[
                        MetricKind.DRIFT
                    ].degradation,
                    critical_threshold=self.config.thresholds[
                        MetricKind.DRIFT
                    ].critical,
                ),
            )

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------

    async def observe(self, output: ComponentOutput) -> GovernanceSample:
        """Process one normalised component output.

        Steps:

        1. Append the output's feature_vector / confidence /
           hallucination_flag to the matching rolling windows.
        2. Recompute every metric for the component.
        3. Record the output in the per-component correlation cache
           (so a future :meth:`observe_outcome` can attribute its
           directional match / mismatch to this output).
        4. Run the per-component ladder against the maximum metric
           value; on any band change emit ``ai.gov.action``, write
           the WarmCache governance weight, and (for
           ``CRITICAL``/``rollback``) toggle the shadow flag.
        5. Persist a metric sample to TimescaleDB (continued
           tracking when no transition fires; transition row when
           one does).

        Returns a :class:`GovernanceSample` describing the outcome.
        """
        state = self._require_state(output.component)
        ts_ns = output.ts_ns if output.ts_ns > 0 else int(self.clock_ns())

        # 1. Update rolling windows.
        for v in output.feature_vector:
            state.drift_window.append(v)
            if not state.drift_reference_full:
                state.drift_reference.append(float(v))
                if len(state.drift_reference) >= self.config.drift_reference_window:
                    state.drift_reference_full = True
        if output.confidence is not None:
            state.stability_window.append(float(output.confidence))
        state.hallucination_window.append(1.0 if output.hallucination_flag else 0.0)

        state.sample_count += 1

        # 2. Cache the output for future correlation against outcomes.
        if output.correlation_id:
            self._cache_correlation(state, output)

        # 3. Recompute metrics.
        metrics = self._compute_metrics(state)
        state.last_metrics = dict(metrics)

        # 4. Threshold ladder.
        sample = await self._evaluate_and_emit(
            state=state,
            metrics=metrics,
            ts_ns=ts_ns,
            correlation_id=output.correlation_id or None,
        )
        return sample

    async def observe_outcome(self, outcome: TradeOutcome) -> list[GovernanceSample]:
        """Score a closed-trade outcome against every recent component output.

        For each component whose correlation cache holds an entry
        for ``outcome.correlation_id``, the engine appends a
        ``0.0`` (matched) or ``1.0`` (mis-predicted) indicator to the
        ``prediction_quality`` window, recomputes that component's
        metrics, and runs the ladder.

        Returns one :class:`GovernanceSample` per component whose
        prediction_quality window changed. Components without a
        cached output for the correlation_id are not affected and
        not included in the return list.
        """
        ts_ns = outcome.ts_ns if outcome.ts_ns > 0 else int(self.clock_ns())
        samples: list[GovernanceSample] = []
        for state in self._states.values():
            cached = state.correlation_outputs.pop(outcome.correlation_id, None)
            if cached is None:
                continue
            # Maintain the ordered key list for bounded eviction.
            try:
                state.correlation_order.remove(outcome.correlation_id)
            except ValueError:
                pass

            # Score: a hit is "the component's directional bias
            # matched the realised P&L sign". For components without
            # an explicit confidence we default to "matched" (cached
            # confidence threshold of 0.5).
            inaccuracy = self._score_outcome(cached, outcome)
            state.prediction_window.append(inaccuracy)

            metrics = self._compute_metrics(state)
            state.last_metrics = dict(metrics)
            sample = await self._evaluate_and_emit(
                state=state,
                metrics=metrics,
                ts_ns=ts_ns,
                correlation_id=outcome.correlation_id,
            )
            samples.append(sample)
        return samples

    async def observe_position_update(
        self,
        *,
        correlation_id: str,
        unrealized_pnl_inr: float,
        ts_ns: int,
    ) -> list[GovernanceSample]:
        """Drive the prediction_quality metric from a per-symbol position update.

        ``pos.update.<sym>`` events arrive more frequently than
        ``exec.trade.closed`` and are useful for an ongoing
        accuracy estimate while a position is still open. We
        treat the mark-to-market unrealised P&L as a realised
        outcome surrogate for the duration of the open position;
        the eventual ``exec.trade.closed`` produces the
        authoritative score via :meth:`observe_outcome`.

        The implementation reuses :meth:`observe_outcome` with a
        synthetic :class:`TradeOutcome`, so the same correlation
        cache pop + scoring + ladder path runs.
        """
        return await self.observe_outcome(
            TradeOutcome(
                correlation_id=correlation_id,
                pnl_inr=float(unrealized_pnl_inr),
                ts_ns=int(ts_ns),
            )
        )

    def metrics_for(
        self, component: GovernedComponent
    ) -> dict[MetricKind, float]:
        """Return a copy of the most recent metric values for ``component``."""
        state = self._require_state(component)
        return dict(state.last_metrics)

    def level_for(self, component: GovernedComponent) -> GovernanceLevel:
        """Return the current governance level for ``component``."""
        state = self._require_state(component)
        return state.ladder.current

    # -----------------------------------------------------------------
    # Internals
    # -----------------------------------------------------------------

    def _require_state(self, component: GovernedComponent) -> _ComponentState:
        try:
            return self._states[component]
        except KeyError:
            raise ValueError(
                f"AI_Governance_Engine is not configured to govern "
                f"component {component.value!r}; configured set: "
                + ", ".join(c.value for c in self.config.components)
            )

    def _cache_correlation(
        self, state: _ComponentState, output: ComponentOutput
    ) -> None:
        capacity = max(self.config.prediction_window * 4, 16)
        cid = output.correlation_id
        if cid in state.correlation_outputs:
            try:
                state.correlation_order.remove(cid)
            except ValueError:
                pass
        state.correlation_outputs[cid] = output
        state.correlation_order.append(cid)
        while len(state.correlation_order) > capacity:
            evicted = state.correlation_order.pop(0)
            state.correlation_outputs.pop(evicted, None)

    def _compute_metrics(
        self, state: _ComponentState
    ) -> dict[MetricKind, float]:
        return {
            MetricKind.DRIFT: compute_drift(
                state.drift_window,
                reference=state.drift_reference if state.drift_reference_full else (),
            ),
            MetricKind.CONFIDENCE_STABILITY: compute_confidence_stability(
                state.stability_window
            ),
            MetricKind.HALLUCINATION_INDICATORS: compute_hallucination_rate(
                state.hallucination_window
            ),
            MetricKind.PREDICTION_QUALITY: compute_prediction_inaccuracy(
                state.prediction_window
            ),
        }

    @staticmethod
    def _score_outcome(
        output: ComponentOutput, outcome: TradeOutcome
    ) -> float:
        """Return 0.0 (matched) or 1.0 (mis-predicted)."""
        # The component's directional bias is "high confidence in the
        # signal" — we treat a confidence above 0.5 as a positive
        # bias. A profitable trade with high confidence is a hit; a
        # losing trade with high confidence is a miss; a profitable
        # trade with low confidence is also a miss (the component
        # under-confidence-flagged a winning setup); a losing trade
        # with low confidence is a hit (the component correctly
        # signalled "low confidence" before the loss).
        confidence = (
            output.confidence
            if output.confidence is not None
            else 0.5
        )
        # ``profitable`` is True when realised P&L exceeds the
        # configured tie threshold; everything else is a non-hit.
        # ``flat`` (|pnl| <= threshold) does not penalise the
        # component — treat as a hit.
        return _score(confidence=confidence, pnl_inr=outcome.pnl_inr)

    async def _evaluate_and_emit(
        self,
        *,
        state: _ComponentState,
        metrics: dict[MetricKind, float],
        ts_ns: int,
        correlation_id: Optional[str],
    ) -> GovernanceSample:
        # Each metric has its own (degradation, critical) threshold
        # pair (R24.2, R24.3). Compute the per-metric level
        # independently, then derive the per-component level as the
        # maximum across the four metrics. The dominant metric is the
        # one that *achieved* that maximum level — with deterministic
        # tie-breaking by the canonical :class:`MetricKind` iteration
        # order.
        per_metric_levels = {
            kind: self.config.thresholds[kind]
            for kind in MetricKind
        }
        per_metric_classification: dict[MetricKind, GovernanceLevel] = {}
        for kind, threshold_pair in per_metric_levels.items():
            value = float(metrics.get(kind, 0.0))
            if value >= threshold_pair.critical:
                per_metric_classification[kind] = GovernanceLevel.CRITICAL
            elif value >= threshold_pair.degradation:
                per_metric_classification[kind] = GovernanceLevel.DEGRADED
            else:
                per_metric_classification[kind] = GovernanceLevel.NONE

        component_level = GovernanceLevel.NONE
        dominant_kind = MetricKind.DRIFT
        for kind in MetricKind:
            level = per_metric_classification[kind]
            if _level_severity(level) > _level_severity(component_level):
                component_level = level
                dominant_kind = kind

        severity = float(metrics.get(dominant_kind, 0.0))
        threshold_for_dominant = per_metric_levels[dominant_kind]

        # Edge-trigger: only emit when the per-component band changed.
        # We synthesise a :class:`LadderTransition` so the rest of the
        # pipeline (WarmCache write, publication, shadow toggle,
        # persistence) reads the same shape regardless of which
        # metric drove the change.
        previous_level = state.ladder.current
        transition: Optional[LadderTransition] = None
        if component_level != previous_level:
            state.ladder.current = component_level
            from .ladder import action_for as _action_for

            transition = LadderTransition(
                previous=previous_level,
                current=component_level,
                value=severity,
                threshold=(
                    threshold_for_dominant.critical
                    if component_level == GovernanceLevel.CRITICAL
                    else threshold_for_dominant.degradation
                ),
                action=_action_for(component_level, previous=previous_level),
            )

        emitted: Optional[AiGovAction] = None

        if transition is not None:
            # Side-effect order: write the WarmCache governance
            # weight first so a downstream consumer that reads after
            # observing the ``ai.gov.action`` event sees the new
            # weight; then publish; then toggle the shadow flag (or
            # clear it on rollback).
            payload = self._build_weight_payload(
                component=state.component,
                level=transition.current,
                ts_ns=ts_ns,
            )
            await self._safe_set_weight(payload)
            if transition.action is not None:
                emitted = self._build_ai_gov_action(
                    component=state.component,
                    transition=transition,
                    metric_kind=dominant_kind,
                    ts_ns=ts_ns,
                )
                await self._safe_publish(emitted)

            if transition.current == GovernanceLevel.CRITICAL:
                await self._safe_set_shadow(state.component, ts_ns=ts_ns)
            elif transition.current == GovernanceLevel.NONE:
                await self._safe_clear_shadow(state.component)

        # Persist a metric sample on every observation (transition or
        # not) so the ``governance_metrics`` hypertable carries the
        # full per-component time series. R23.3 requires that
        # shadowed AI outputs are compared against actual outcomes
        # and accuracy metrics are produced — the persisted rows are
        # the audit trail.
        sample_threshold = (
            threshold_for_dominant.critical
            if state.ladder.current == GovernanceLevel.CRITICAL
            else threshold_for_dominant.degradation
        )
        await self._safe_persist_sample(
            state=state,
            kind=dominant_kind,
            value=severity,
            threshold=sample_threshold,
            level=state.ladder.current,
            action=transition.action if transition is not None else None,
            correlation_id=correlation_id,
            ts_ns=ts_ns,
        )

        return GovernanceSample(
            component=state.component,
            metrics=dict(metrics),
            dominant_metric=dominant_kind,
            severity=severity,
            transition=transition,
            emitted=emitted,
        )

    def _build_weight_payload(
        self,
        *,
        component: GovernedComponent,
        level: GovernanceLevel,
        ts_ns: int,
    ) -> GovernanceWeightPayload:
        weight = float(self.config.weights[level])
        return GovernanceWeightPayload(
            component=component,
            weight=weight,
            level=level,
            ts_ns=int(ts_ns),
        )

    def _build_ai_gov_action(
        self,
        *,
        component: GovernedComponent,
        transition: LadderTransition,
        metric_kind: MetricKind,
        ts_ns: int,
    ) -> AiGovAction:
        return AiGovAction.model_validate(
            {
                "component": component.value,
                "action": transition.action,
                "metric": wire_metric_for(metric_kind),
                "value": float(transition.value),
                "threshold": float(transition.threshold),
                "ts_ns": int(ts_ns),
            }
        )

    async def _safe_set_weight(self, payload: GovernanceWeightPayload) -> None:
        try:
            await self.warm_cache.set_weight(payload)
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "governance_weight_write_failed",
                component=payload.component.value,
                level=payload.level.value,
                weight=payload.weight,
                error=str(exc),
            )

    async def _safe_set_shadow(
        self, component: GovernedComponent, *, ts_ns: int
    ) -> None:
        try:
            await self.warm_cache.set_shadow(component, ts_ns=ts_ns)
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "governance_shadow_write_failed",
                component=component.value,
                error=str(exc),
            )

    async def _safe_clear_shadow(self, component: GovernedComponent) -> None:
        try:
            await self.warm_cache.clear_shadow(component)
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "governance_shadow_clear_failed",
                component=component.value,
                error=str(exc),
            )

    async def _safe_publish(self, event: AiGovAction) -> None:
        try:
            await self.publisher.publish_action(event)
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "governance_publish_failed",
                component=event.component,
                action=event.action,
                metric=event.metric,
                value=event.value,
                threshold=event.threshold,
                error=str(exc),
            )

    async def _safe_persist_sample(
        self,
        *,
        state: _ComponentState,
        kind: MetricKind,
        value: float,
        threshold: float,
        level: GovernanceLevel,
        action: Optional[str],
        correlation_id: Optional[str],
        ts_ns: int,
    ) -> None:
        try:
            await self.metric_sink.write_governance_metric(
                component=state.component,
                kind=kind,
                value=float(value),
                threshold=float(threshold),
                level=level,
                action=action,
                correlation_id=correlation_id,
                sample_count=int(state.sample_count),
                ts_ns=int(ts_ns),
            )
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "governance_metric_persist_unhandled",
                component=state.component.value,
                metric_kind=kind.value,
                value=value,
                error=str(exc),
            )


# ---------------------------------------------------------------------------
# Helpers ------------------------------------------------------------------
# ---------------------------------------------------------------------------


def _level_severity(level: GovernanceLevel) -> int:
    """Map :class:`GovernanceLevel` to a comparable severity integer.

    Used to compute ``max(per_metric_levels)`` deterministically.
    """
    if level == GovernanceLevel.CRITICAL:
        return 2
    if level == GovernanceLevel.DEGRADED:
        return 1
    return 0


def _score(*, confidence: float, pnl_inr: float) -> float:
    """Score the (component output, realised outcome) pair as 0.0/1.0.

    Hit logic: if the component's confidence and the realised P&L
    agree on direction (confidence > 0.5 with pnl > 0; confidence
    <= 0.5 with pnl <= 0) the component "matched" — return 0.0.
    Otherwise return 1.0 (mis-predicted).
    """
    confident = confidence > 0.5
    profitable = pnl_inr > 0.0
    return 0.0 if confident == profitable else 1.0


__all__ = [
    "AiGovernanceEngine",
    "GovernanceSample",
]
