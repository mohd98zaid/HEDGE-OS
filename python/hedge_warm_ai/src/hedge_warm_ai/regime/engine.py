"""Market_Regime_Engine — task 22.1 (R13.1–R13.5).

The engine ties together the four collaborators introduced in this
subpackage:

* :class:`~.classifier.RegimeClassifier` — decides what regime the
  current observation belongs to (R13.1).
* :class:`~.publisher.RegimePublisher` — emits ``ai.regime.changed`` on
  the canonical NATS subject (R13.3).
* :class:`~.warm_cache.MarketStabilityCache` — writes the
  ``MarketStability`` factor for the Risk_Engine (R13.5, R5.13).
* :class:`~.config.RegimeConfig` — supplies thresholds, evaluation
  interval (R13.2), and the seed regime.

The engine is **edge-triggered**: it maintains the current regime
internally and emits an ``ai.regime.changed`` event only when the new
classification differs from the previous one. The emitted payload
carries ``from`` and ``to`` so subscribers (Signal_Engine for strategy
gating, R13.4; Risk_Engine for Adaptive_Risk, R13.5) see both states
without needing local state of their own.

The engine is async-first. The two public entry points are:

* :meth:`MarketRegimeEngine.evaluate` — feed one
  :class:`~.signals.RegimeObservation`; the engine classifies, edge-
  triggers, publishes (if changed), updates the WarmCache, and returns
  a :class:`RegimeEvaluation` describing what happened. Idempotent on
  repeated identical observations: no duplicate emissions.
* :meth:`MarketRegimeEngine.run` — drive a periodic evaluation loop on
  the configured interval, pulling observations from a producer
  callable. Designed to be ``await``-ed under :func:`asyncio.run` in
  the ``hedge-regime`` service entry point.

Determinism: every transition decision is a pure function of
``(prior_regime, observation)`` once the classifier and config are
fixed. Tests in 22.2 enumerate adjacent-pair changes and assert that
the count of emissions equals the count of adjacent-pair regime
changes (Property 8).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable, Final, Optional

import structlog

from ..schemas import RegimeChanged
from ..schemas.ai_regime_changed import Regime
from .classifier import RegimeClassifier, RuleBasedRegimeClassifier
from .config import RegimeConfig
from .errors import (
    MarketStabilityCacheError,
    RegimeClassificationError,
    RegimePublishError,
)
from .publisher import NoopRegimePublisher, RegimePublisher
from .signals import MarketStabilityFactor, RegimeObservation
from .warm_cache import (
    InMemoryMarketStabilityCache,
    MarketStabilityCache,
    derive_stability_factor,
)

_LOG: Final = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Result types --------------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RegimeEvaluation:
    """Outcome of a single :meth:`MarketRegimeEngine.evaluate` call.

    Attributes:
        prior:        The regime in force *before* this evaluation.
                      ``None`` only on the very first evaluation when
                      ``publish_warmup_skip`` masks the initial
                      transition (the engine still records the seed
                      regime internally, but the public ``prior``
                      field surfaces the warm-up state).
        current:      The regime classified for this observation.
        changed:      ``True`` iff ``prior is not None and prior != current``.
        emitted:      ``True`` iff ``changed`` and the warm-up window
                      had passed; this is the bit that the Property 8
                      test in 22.2 counts.
        stability:    The :class:`MarketStabilityFactor` written to the
                      WarmCache for this evaluation. Always populated
                      so the Risk_Engine sees a fresh value even when
                      the regime did not change (last-known-value
                      semantics).
        published_at_ns: Wall-clock ns at which the engine called the
                      publisher. ``None`` when ``emitted`` is ``False``.
    """

    prior: Optional[Regime]
    current: Regime
    changed: bool
    emitted: bool
    stability: MarketStabilityFactor
    published_at_ns: Optional[int] = None


# ---------------------------------------------------------------------------
# Engine --------------------------------------------------------------------
# ---------------------------------------------------------------------------


class MarketRegimeEngine:
    """Orchestrator for the Market_Regime_Engine subsystem (R13).

    Lifecycle::

        engine = MarketRegimeEngine(
            config=RegimeConfig.from_yaml_path(...),
            publisher=NatsRegimePublisher(async_publish=nc.publish),
            stability_cache=RedisMarketStabilityCache(redis_hot_cache),
        )
        # one-shot evaluation:
        await engine.evaluate(observation)

        # or run the periodic loop:
        await engine.run(observation_provider=poll_market)

    The class is **not** a singleton — multiple engines can run side-
    by-side (e.g. one per index) provided each owns its own
    ``stability_cache`` to avoid clobbering writes.
    """

    def __init__(
        self,
        *,
        config: RegimeConfig,
        publisher: Optional[RegimePublisher] = None,
        stability_cache: Optional[MarketStabilityCache] = None,
        classifier: Optional[RegimeClassifier] = None,
        clock_ns: Optional[Callable[[], int]] = None,
    ) -> None:
        """Construct the engine.

        Args:
            config: Resolved :class:`RegimeConfig`.
            publisher: Edge-triggered publisher. Defaults to
                :class:`NoopRegimePublisher` so unit tests that focus
                on classification do not need to wire NATS.
            stability_cache: ``MarketStability`` cache. Defaults to
                :class:`InMemoryMarketStabilityCache` for the same
                reason.
            classifier: Optional override. Defaults to
                :class:`RuleBasedRegimeClassifier(config)`.
            clock_ns: Callable returning a wall-clock ns timestamp.
                Defaults to :mod:`time.time_ns`. Override in tests for
                determinism.
        """
        # Imported locally to avoid an unconditional :mod:`time` import
        # at module-import time when callers always supply a clock.
        import time as _time

        self._config = config
        self._publisher: RegimePublisher = publisher or NoopRegimePublisher()
        self._cache: MarketStabilityCache = (
            stability_cache or InMemoryMarketStabilityCache()
        )
        self._classifier: RegimeClassifier = classifier or RuleBasedRegimeClassifier(
            config=config
        )
        self._clock_ns: Callable[[], int] = clock_ns or _time.time_ns

        # Edge-trigger state — protected by an asyncio lock because
        # ``evaluate`` may be invoked concurrently by the periodic loop
        # and external callers (e.g. an integration test).
        self._lock = asyncio.Lock()
        self._current_regime: Optional[Regime] = None
        self._warmup_remaining: int = max(1, int(config.publish_warmup_skip))
        self._evaluations: int = 0

    # ----- introspection ---------------------------------------------------

    @property
    def config(self) -> RegimeConfig:
        return self._config

    @property
    def current_regime(self) -> Optional[Regime]:
        """Last classified regime, or ``None`` before the first ``evaluate``."""
        return self._current_regime

    @property
    def evaluation_count(self) -> int:
        """Total number of ``evaluate`` calls completed (test helper)."""
        return self._evaluations

    # ----- one-shot evaluation --------------------------------------------

    async def evaluate(self, observation: RegimeObservation) -> RegimeEvaluation:
        """Classify *observation*, edge-trigger emission, and update cache.

        Args:
            observation: The bundle of market signals at the current
                evaluation interval. Validated at construction; this
                method does not re-validate.

        Returns:
            A :class:`RegimeEvaluation` describing the outcome.

        Raises:
            RegimeClassificationError: classifier rejected the input.
            RegimePublishError: NATS publish failed.
            MarketStabilityCacheError: WarmCache write failed.
        """
        async with self._lock:
            current = self._classifier.classify(observation)
            prior = self._current_regime

            changed = prior is not None and prior != current
            emit = changed and self._warmup_remaining == 0

            published_at_ns: Optional[int] = None
            if emit:
                # Build the canonical payload. ``from`` carries the
                # prior regime; ``to`` carries the new one. ``ts_ns``
                # is the producer-side timestamp from the observation
                # so subscribers see the source-side time, not the
                # engine's. This matches the design's traceability
                # rule: every event carries the originating ts.
                event = RegimeChanged.model_validate(
                    {"from": prior, "to": current, "ts_ns": int(observation.ts_ns)}
                )
                try:
                    await self._publisher.publish_regime_change(event)
                except RegimePublishError:
                    # leave _current_regime unchanged so the next
                    # successful publish retries the *same* edge.
                    raise
                published_at_ns = self._clock_ns()
                _LOG.info(
                    "regime_changed_emitted",
                    from_=prior,
                    to=current,
                    ts_ns=int(observation.ts_ns),
                )

            # Update the WarmCache regardless of edge: the Risk_Engine
            # reads last-known-value semantics, so we keep the cache
            # fresh even when the regime did not change. This is what
            # gives the Risk_Engine a sub-50µs cache lookup at risk-
            # check time (design § Latency Budget Allocation).
            stability_value = self._config.stability_factors.get(current)
            stability = derive_stability_factor(
                current,
                stability_factor_value=stability_value,
                ts_ns=int(observation.ts_ns),
            )
            try:
                await self._cache.set_factor(stability)
                await self._cache.set_current_regime(
                    current, ts_ns=int(observation.ts_ns)
                )
            except MarketStabilityCacheError:
                # Log and re-raise; the supervisor consumes the typed
                # error.
                _LOG.warning(
                    "regime_market_stability_cache_failed",
                    regime=current,
                    factor=stability_value,
                )
                raise

            # Commit the new regime *after* successful publication and
            # cache write so a transient failure does not silently
            # advance the edge-trigger state (the next call retries).
            self._current_regime = current
            if self._warmup_remaining > 0:
                self._warmup_remaining -= 1
            self._evaluations += 1

            return RegimeEvaluation(
                prior=prior,
                current=current,
                changed=changed,
                emitted=emit,
                stability=stability,
                published_at_ns=published_at_ns,
            )

    # ----- periodic loop ---------------------------------------------------

    async def run(
        self,
        observation_provider: Callable[[], Awaitable[Optional[RegimeObservation]]],
        *,
        stop_event: Optional[asyncio.Event] = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        """Drive an evaluation every ``evaluation_interval_s`` seconds.

        The provider is awaited each tick; if it returns ``None`` the
        engine skips that tick (handy when the producer has no fresh
        data yet). The loop terminates when ``stop_event`` is set.

        This is the entry point used by the ``hedge-regime`` service
        binary defined in :file:`pyproject.toml`. A test harness can
        also drive it directly via a stop event:

            stop = asyncio.Event()
            task = asyncio.create_task(engine.run(provider, stop_event=stop))
            ...
            stop.set()
            await task

        Args:
            observation_provider: ``async def () -> Optional[RegimeObservation]``.
                The engine awaits it on each tick. ``None`` skips the
                tick so producers do not have to fabricate an
                observation when their inputs are stale.
            stop_event: Set externally to stop the loop cleanly. If
                omitted the loop runs until cancelled.
            sleep: Override of the per-tick delay function. The
                default is :func:`asyncio.sleep`; tests inject a
                deterministic stub.
        """
        interval = float(self._config.evaluation_interval_s)
        while True:
            if stop_event is not None and stop_event.is_set():
                return
            try:
                obs = await observation_provider()
            except Exception as exc:
                _LOG.warning("regime_observation_provider_failed", error=str(exc))
                obs = None
            if obs is not None:
                try:
                    await self.evaluate(obs)
                except RegimeClassificationError as exc:
                    _LOG.warning("regime_classification_rejected", error=str(exc))
                except RegimePublishError as exc:
                    _LOG.warning("regime_publish_failed_run_loop", error=str(exc))
                except MarketStabilityCacheError as exc:
                    _LOG.warning(
                        "regime_market_stability_cache_failed_run_loop",
                        error=str(exc),
                    )
            try:
                await sleep(interval)
            except asyncio.CancelledError:
                return


__all__ = [
    "MarketRegimeEngine",
    "RegimeEvaluation",
]
