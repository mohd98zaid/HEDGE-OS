"""Symbol_Priority_Engine — totality + edge-triggered emission (R14.1, R14.3).

The engine maintains the single source of truth for the current
priority tier of every tracked symbol and emits exactly one
``ai.priority.changed.<sym>`` event for each adjacent-pair tier
change in the observation stream of any one symbol (Property 8).

Totality invariant (R14.1)
--------------------------

Every tracked symbol carries exactly one tier of
``P1 | P2 | P3 | P4`` at all times. This is enforced structurally:

* The internal mapping ``_tiers: dict[str, PriorityTier]`` is the
  single source of truth.
* :meth:`SymbolPriorityEngine.track` is the **only** way to introduce
  a symbol; it requires an initial tier as part of registration.
* :meth:`SymbolPriorityEngine.untrack` is the **only** way to remove
  a symbol; it removes both the tier mapping *and* the cached
  inputs in one atomic step.
* No other method removes a symbol from ``_tiers`` without
  immediately replacing its value.

This file documents the invariant on every method that touches
``_tiers`` and the property test in task 23.2 fuzzes it.

Edge-triggered emission (R14.3, Property 8)
-------------------------------------------

The engine recomputes the tier on every input edge — trader intent,
regime change, news impact — and compares the new tier against the
last-seen tier. A :class:`PriorityChanged` event is emitted **only**
when the two differ; the count of emitted events therefore equals
the count of distinct adjacent-pair changes in the per-symbol
observation stream. The payload always carries ``from`` and ``to``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Final, Iterable, Mapping

import structlog

from ..schemas import (
    NewsImpact,
    PriorityChanged,
    RegimeChanged,
    TraderIntentPriority,
)
from ..schemas.ai_priority_changed import PriorityTier
from ..schemas.ai_regime_changed import Regime
from .allocation import (
    DEFAULT_ALLOCATION_TABLE,
    PRIORITY_TIERS,
    PriorityAllocation,
    PriorityAllocationTable,
)
from .cache import PriorityWarmCache
from .policy import (
    DefaultPriorityPolicy,
    PriorityInputs,
    PriorityPolicy,
)
from .publisher import (
    NoopPriorityChangedPublisher,
    PriorityChangedPublisher,
)

_LOG: Final = structlog.get_logger(__name__)


class UnknownSymbolError(KeyError):
    """Raised when an operation references an untracked symbol.

    Trader intents and news impacts received for an untracked symbol
    are *ignored* (see :meth:`SymbolPriorityEngine.on_trader_intent`
    / :meth:`on_news_impact` docstrings); raising this would give a
    crashing service surface to a misbehaving NATS publisher. This
    exception is reserved for direct API calls that misuse the
    engine.
    """


# ---------------------------------------------------------------------------
# Engine --------------------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _SymbolState:
    """Per-symbol latest input snapshot. Internal to the engine."""

    last_news: NewsImpact | None = None
    last_trader: TraderIntentPriority | None = None
    baseline: PriorityTier = "P3"


class SymbolPriorityEngine:
    """Symbol_Priority_Engine implementation.

    Construction
    ------------

    * ``allocation_table`` defaults to :data:`DEFAULT_ALLOCATION_TABLE`
      so engine instantiation does not require a config object in
      tests.
    * ``policy`` defaults to :class:`DefaultPriorityPolicy`.
    * ``publisher`` defaults to :class:`NoopPriorityChangedPublisher`
      so the engine can run in non-NATS contexts (Replay_Engine,
      offline backfills) without modification. Production wires
      :class:`NatsPriorityChangedPublisher`.
    * ``warm_cache`` is optional. When supplied, every applied tier
      change is mirrored into Redis so Hot_Path Rust consumers can
      read it via :class:`PriorityWarmCache` until task 44.x lands.
    * ``clock_ns`` is the monotonic nanosecond clock used to stamp
      :class:`PriorityChanged` events. Override in tests for
      determinism.

    Concurrency
    -----------

    The engine is **not** internally locked. The Warm_AI_Pipeline runs
    a single-threaded asyncio event loop per service, so each engine
    instance is owned by exactly one coroutine. If a future refactor
    fans out across threads, wrap the public methods with an
    :class:`asyncio.Lock`.
    """

    def __init__(
        self,
        *,
        allocation_table: PriorityAllocationTable = DEFAULT_ALLOCATION_TABLE,
        policy: PriorityPolicy | None = None,
        publisher: PriorityChangedPublisher | None = None,
        warm_cache: PriorityWarmCache | None = None,
        clock_ns: "callable | None" = None,
    ) -> None:
        self._allocation_table = allocation_table
        self._policy: PriorityPolicy = policy or DefaultPriorityPolicy()
        self._publisher: PriorityChangedPublisher = (
            publisher or NoopPriorityChangedPublisher()
        )
        self._warm_cache = warm_cache
        self._clock_ns = clock_ns or time.monotonic_ns

        # Totality invariant: a symbol is in _tiers iff it is tracked.
        # _states is keyed by exactly the same set of symbols.
        self._tiers: dict[str, PriorityTier] = {}
        self._states: dict[str, _SymbolState] = {}

        # Last-seen regime is global, not per-symbol.
        self._regime: Regime | None = None

    # ------------------------------------------------------------------
    # Read API
    # ------------------------------------------------------------------

    def tracked_symbols(self) -> tuple[str, ...]:
        """Return the tuple of tracked symbols (insertion order)."""
        return tuple(self._tiers.keys())

    def tier(self, symbol: str) -> PriorityTier:
        """Return the current tier for ``symbol``.

        Raises:
            UnknownSymbolError: ``symbol`` is not tracked.
        """
        if symbol not in self._tiers:
            raise UnknownSymbolError(symbol)
        return self._tiers[symbol]

    def allocation(self, symbol: str) -> PriorityAllocation:
        """Return the current :class:`PriorityAllocation` for ``symbol``."""
        return self._allocation_table.get(self.tier(symbol))

    def snapshot(self) -> Mapping[str, PriorityTier]:
        """Return an immutable view of the current ``symbol → tier`` map."""
        return dict(self._tiers)

    @property
    def allocation_table(self) -> PriorityAllocationTable:
        """The :class:`PriorityAllocationTable` driving this engine."""
        return self._allocation_table

    # ------------------------------------------------------------------
    # Tracking lifecycle
    # ------------------------------------------------------------------

    async def track(
        self,
        symbol: str,
        *,
        initial_tier: PriorityTier | None = None,
        baseline: PriorityTier | None = None,
    ) -> PriorityTier:
        """Begin tracking ``symbol`` and return its initial tier.

        The engine assigns ``initial_tier`` (defaulting to
        ``baseline``, which itself defaults to ``"P3"``) so the
        totality invariant holds the moment ``track`` returns. No
        :class:`PriorityChanged` event is emitted for the initial
        assignment — there is no prior tier to transition from. This
        matches the design's edge-triggered semantics: an event
        signals a *change*, not a *registration*.

        Calling ``track`` for an already-tracked symbol is a no-op
        and returns the existing tier.

        Args:
            symbol: Symbol identifier; non-empty.
            initial_tier: Optional explicit initial tier. If omitted,
                the engine uses ``baseline``.
            baseline: Fall-through tier the policy uses when no
                trader/regime/news input applies. Defaults to
                ``"P3"``.

        Returns:
            The tier the symbol now carries.
        """
        if not symbol:
            raise ValueError("symbol must be non-empty")
        if symbol in self._tiers:
            return self._tiers[symbol]
        chosen_baseline: PriorityTier = baseline or "P3"
        chosen_tier: PriorityTier = initial_tier or chosen_baseline
        # Totality: install the tier and the state in lock-step.
        self._tiers[symbol] = chosen_tier
        self._states[symbol] = _SymbolState(baseline=chosen_baseline)
        if self._warm_cache is not None:
            await self._warm_cache.put(
                symbol, chosen_tier, self._allocation_table.get(chosen_tier)
            )
        _LOG.info(
            "priority_track",
            symbol=symbol,
            tier=chosen_tier,
            baseline=chosen_baseline,
        )
        return chosen_tier

    def untrack(self, symbol: str) -> None:
        """Stop tracking ``symbol``.

        Removes both the tier and the input state in one step so the
        totality invariant is preserved (a symbol is either fully
        tracked or fully absent). The Redis cache entry is **not**
        deleted — the WarmCache crate's GC will reap it once the
        WarmCache replaces this stop-gap (see ``cache.py`` module
        docstring).

        Calling ``untrack`` for an unknown symbol is a no-op.
        """
        self._tiers.pop(symbol, None)
        self._states.pop(symbol, None)

    # ------------------------------------------------------------------
    # Input edges (R14.3)
    # ------------------------------------------------------------------

    async def on_trader_intent(self, intent: TraderIntentPriority) -> None:
        """Apply a trader-issued tier change.

        Per R21 (Authority_Hierarchy) the trader's tier overrides any
        AI recommendation. If the symbol is not yet tracked the
        engine starts tracking it at ``intent.to`` (no event is
        emitted because there is no prior tier).
        """
        if intent.symbol not in self._tiers:
            await self.track(intent.symbol, initial_tier=intent.to)
            self._states[intent.symbol].last_trader = intent
            return
        self._states[intent.symbol].last_trader = intent
        await self._recompute_and_emit(intent.symbol)

    async def on_regime_change(self, change: RegimeChanged) -> None:
        """Apply a regime change to every tracked symbol.

        Regime is global, so the engine recomputes every symbol's
        tier. Edge-triggered emission still applies per symbol — only
        symbols whose tier changes produce an
        :class:`ai.priority.changed.<sym>` event.
        """
        self._regime = change.to
        # Iterate over a snapshot so an emit-induced state mutation
        # (none in the current design, but future-proof) cannot
        # corrupt the iteration.
        for symbol in tuple(self._tiers.keys()):
            await self._recompute_and_emit(symbol)

    async def on_news_impact(self, impact: NewsImpact) -> None:
        """Apply a news-impact event for one symbol.

        News for an untracked symbol is ignored: the engine only
        ranks symbols the trader has registered. The Memory_RAG_Layer
        still persists the headline regardless (R19.1) — that is
        out of scope here.
        """
        if impact.symbol not in self._tiers:
            return
        self._states[impact.symbol].last_news = impact
        await self._recompute_and_emit(impact.symbol)

    async def recompute_all(self) -> None:
        """Force a recompute over every tracked symbol.

        Useful at startup after seeding inputs from the Replay_Engine
        or when the policy is swapped at runtime (rare).
        """
        for symbol in tuple(self._tiers.keys()):
            await self._recompute_and_emit(symbol)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _recompute_and_emit(self, symbol: str) -> None:
        """Recompute ``symbol``'s tier; emit on change; mirror to cache.

        Totality invariant: this method **never** removes ``symbol``
        from ``_tiers``. It either keeps the existing entry or
        overwrites it with a new tier — the symbol is always present.
        """
        prior = self._tiers[symbol]
        state = self._states[symbol]
        inputs = PriorityInputs(
            trader_intent=state.last_trader,
            regime=self._regime,
            news=state.last_news,
            baseline=state.baseline,
        )
        new_tier = self._policy.assign(symbol=symbol, inputs=inputs)
        if new_tier == prior:
            return  # Edge-triggered: only emit on a change.
        # Replace tier in lock-step with the cache update so the
        # observable state ``(tier, allocation)`` always matches.
        self._tiers[symbol] = new_tier
        if self._warm_cache is not None:
            try:
                await self._warm_cache.put(
                    symbol, new_tier, self._allocation_table.get(new_tier)
                )
            except Exception as exc:  # pragma: no cover - logged
                # Cache write failures are not fatal: the canonical
                # source of truth is _tiers in-process, and the
                # Self_Healing_Supervisor owns Redis recovery (R25.2).
                _LOG.warning(
                    "priority_warm_cache_put_failed",
                    symbol=symbol,
                    tier=new_tier,
                    error=str(exc),
                )
        # Edge-emit. The model uses ``from_`` because ``from`` is a
        # Python keyword; the canonical JSON wire representation is
        # ``{"from": ...}`` via ``model_dump(by_alias=True)``.
        event = PriorityChanged.model_validate(
            {
                "symbol": symbol,
                "from": prior,
                "to": new_tier,
                "ts_ns": int(self._clock_ns()),
            }
        )
        await self._publisher.publish_changed(event)
        _LOG.info(
            "priority_changed",
            symbol=symbol,
            **{"from": prior, "to": new_tier},
        )

    # ------------------------------------------------------------------
    # Bulk seeding (testing / replay)
    # ------------------------------------------------------------------

    async def seed(
        self,
        initial: Iterable[tuple[str, PriorityTier]],
        *,
        baseline: PriorityTier = "P3",
    ) -> None:
        """Convenience: track many symbols at once with explicit tiers.

        Used by the Replay_Engine and tests; production code seeds
        symbols one-at-a-time as they enter the universe.
        """
        for symbol, tier in initial:
            await self.track(symbol, initial_tier=tier, baseline=baseline)


__all__ = [
    "PRIORITY_TIERS",
    "SymbolPriorityEngine",
    "UnknownSymbolError",
]
