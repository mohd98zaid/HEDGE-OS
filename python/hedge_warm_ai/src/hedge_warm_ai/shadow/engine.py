"""AI_Shadow_Mode service — task 29.1 (R23.1, R23.2, R23.3).

Responsibilities
================

1. **Read the shadow flag** (R24.3 → R23.1, R23.2): on startup and on
   every poll cycle, refresh the in-memory snapshot of which
   components are currently shadowed by reading the
   :mod:`hedge_warm_ai.governance.subjects.DEFAULT_SHADOW_FLAG_NAMESPACE`
   namespace from the interim WarmCache surface (Redis). The
   AI_Governance_Engine writes flags into this namespace; the shadow
   service is the consumer.

2. **Provide an ``is_shadowed`` query** (R23.1, R23.2): the upstream
   Warm_AI_Pipeline engines (ranking, regime, news, etc.) consult
   :meth:`ShadowModeService.is_shadowed` before publishing. When the
   query returns ``True``, the engine sets ``shadow=True`` on the
   wire payload it then publishes — the canonical schemas already
   carry the field where applicable (verified for
   :class:`hedge_warm_ai.schemas.RankedSignal`; the README captures a
   follow-up note for the schemas that do not yet carry the field).

3. **Persist shadowed outputs** (R23.1): when an upstream engine
   produces a shadowed emission, it hands the
   :class:`ShadowedOutput` to :meth:`ShadowModeService.persist_output`.
   The service:

   * forwards the output to the
     :class:`hedge_warm_ai.shadow.persistence.ShadowedOutputSink`,
     which writes the row to the matching Timescale hypertable
     (``ai_scores``, ``regime_history``, …) with the timestamp
     verbatim (R23.1); and
   * forwards the same output to the
     :class:`hedge_warm_ai.shadow.governance_observer.GovernanceObserver`,
     which feeds the AI_Governance_Engine's
     :meth:`observe` API so the engine's accuracy-metric path sees
     the shadowed emission (R23.3).

4. **Provide the UI gateway filter callable** (R23.2): the service
   re-exports :class:`ShadowFilter` for the UI gateway (task 36.1)
   to compose with its ``/signals`` topic-subscription protocol. The
   filter is pure on the wire payload's ``shadow`` field — see
   :mod:`hedge_warm_ai.shadow.filter` for rationale.

Lifecycle and threading
=======================

The service is async-first. The ``hedge-shadow`` console-script
binary drives :meth:`start` / :meth:`stop`; while running, an
internal poll task refreshes the snapshot every
``config.poll_interval_s`` seconds. :meth:`is_shadowed` and
:meth:`persist_output` are safe to call concurrently — the snapshot
is replaced atomically by reference assignment, and the persistence
buffer is guarded by an :class:`asyncio.Lock`.

Authority + Hot_Path purity
===========================

The service is **strictly off the Hot_Path** (Property 2 — Authority
Hierarchy and Hot_Path Purity, R30): it reads the WarmCache and
writes to TimescaleDB / forwards to the governance engine. The
Hot_Path Risk_Engine and Signal_Engine are not aware of the shadow
service. The UI gateway is the only consumer of the
:class:`ShadowFilter`; it is not awaited by any Hot_Path consumer.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Final, Iterable, Optional

import structlog

from .config import ShadowModeConfig
from .filter import ShadowFilter
from .flag_source import (
    InMemoryShadowFlagSource,
    ShadowFlagSource,
)
from .governance_observer import (
    GovernanceObserver,
    NoopGovernanceObserver,
)
from .persistence import NoopShadowedOutputSink, ShadowedOutputSink
from .state import (
    EMPTY_SHADOW_SNAPSHOT,
    ShadowSnapshot,
    ShadowedOutput,
)

_LOG: Final = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Service ------------------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass
class ShadowModeService:
    """The AI_Shadow_Mode service (R23.1, R23.2, R23.3).

    Construction:

    * ``config`` — resolved :class:`ShadowModeConfig`.
    * ``flag_source`` — concrete :class:`ShadowFlagSource`.
    * ``persistence_sink`` — concrete :class:`ShadowedOutputSink`.
    * ``governance_observer`` — concrete :class:`GovernanceObserver`.
    * ``clock_ns`` — wall-clock ns callable; used for timestamping
      the snapshot's :attr:`refreshed_at_ns` field and for
      defaulting :attr:`ShadowedOutput.ts_ns` when the upstream
      engine hands the service a payload without one.
    * ``poll_components`` — iterable of canonical component names
      polled on every refresh. Defaults to the seven values of
      :class:`hedge_warm_ai.governance.state.GovernedComponent`.

    Lifecycle: :meth:`start` launches the internal poll task;
    :meth:`stop` cancels it. Both are idempotent. The service can
    also be used in poll-once mode by calling :meth:`refresh`
    directly without starting the loop — useful when the embedding
    process drives its own scheduler.
    """

    config: ShadowModeConfig
    flag_source: ShadowFlagSource = field(
        default_factory=InMemoryShadowFlagSource
    )
    persistence_sink: ShadowedOutputSink = field(
        default_factory=NoopShadowedOutputSink
    )
    governance_observer: GovernanceObserver = field(
        default_factory=NoopGovernanceObserver
    )
    clock_ns: Callable[[], int] = field(default=time.time_ns)
    poll_components: Optional[tuple[str, ...]] = None

    _snapshot: ShadowSnapshot = field(
        default=EMPTY_SHADOW_SNAPSHOT, init=False
    )
    _persist_buffer: deque[ShadowedOutput] = field(
        default_factory=deque, init=False
    )
    _persist_lock: asyncio.Lock = field(
        default_factory=asyncio.Lock, init=False
    )
    _poll_task: Optional[asyncio.Task[None]] = field(default=None, init=False)
    _started: bool = field(default=False, init=False)
    _stopped: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        # Seed the snapshot with the static components from
        # :class:`HedgeConfig.ai.shadow_components`. This guarantees
        # that, even before the first refresh cycle, an upstream
        # engine that calls :meth:`is_shadowed` for a seed component
        # gets the right answer.
        seed = self.config.normalised_seed_components()
        if seed:
            self._snapshot = ShadowSnapshot(
                components=seed,
                refreshed_at_ns=int(self.clock_ns()),
            )

    # -----------------------------------------------------------------
    # Public — read API
    # -----------------------------------------------------------------

    @property
    def snapshot(self) -> ShadowSnapshot:
        """Return the most recent :class:`ShadowSnapshot`."""
        return self._snapshot

    def is_shadowed(self, component: str) -> bool:
        """Return whether ``component`` is currently shadowed.

        The query is synchronous and lock-free — the snapshot is
        replaced atomically by reference assignment in
        :meth:`refresh`. Upstream engines call this on the hot
        publication path of the Warm_AI_Pipeline (task 26.1, 22.1,
        21.1, 25.1, 23.1, 27.1) so the call site cannot afford a
        round-trip to Redis on every emission.
        """
        return self._snapshot.is_shadowed(component)

    def shadowed_components(self) -> frozenset[str]:
        """Return the current set of shadowed components."""
        return self._snapshot.components

    @property
    def filter(self) -> ShadowFilter:
        """Return a fresh :class:`ShadowFilter` for the UI gateway."""
        return ShadowFilter(log_dropped=True, channel_label="/signals")

    # -----------------------------------------------------------------
    # Public — write API
    # -----------------------------------------------------------------

    async def persist_output(self, output: ShadowedOutput) -> None:
        """Persist a shadowed output and forward to governance.

        R23.1 — the row goes to the matching Timescale hypertable
        with a timestamp.
        R23.3 — the same output is forwarded to the governance
        engine via :class:`GovernanceObserver` so its accuracy
        metrics include shadowed emissions.

        The two side effects run sequentially: persistence first
        (so a downstream consumer reading the row sees the same
        observation the governance engine then scores), then the
        governance forward. Failures in either are logged + dropped
        — neither aborts the other.
        """
        if not _is_marked_shadow(output):
            _LOG.warning(
                "shadow_persist_rejected_non_shadow_payload",
                component=output.component,
                kind=output.kind.value,
            )
            return

        # 1. Persistence — best-effort.
        try:
            await self.persistence_sink.persist(output)
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "shadow_persist_call_failed",
                component=output.component,
                kind=output.kind.value,
                error=str(exc),
            )
            await self._buffer_for_retry(output)
        # 2. Governance forwarding — best-effort. R23.3 invariant:
        #    shadowed outputs are NOT filtered out of the governance
        #    metric path; only the UI ranked-signal channel is
        #    filtered. We always call the observer regardless of
        #    whether persistence succeeded.
        try:
            await self.governance_observer.forward(output)
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "shadow_governance_forward_call_failed",
                component=output.component,
                kind=output.kind.value,
                error=str(exc),
            )

    async def _buffer_for_retry(self, output: ShadowedOutput) -> None:
        """Store an output in the bounded retry buffer."""
        async with self._persist_lock:
            self._persist_buffer.append(output)
            while len(self._persist_buffer) > self.config.persistence_buffer:
                evicted = self._persist_buffer.popleft()
                _LOG.warning(
                    "shadow_persist_buffer_overflow",
                    evicted_component=evicted.component,
                    evicted_kind=evicted.kind.value,
                    evicted_ts_ns=evicted.ts_ns,
                    buffer_capacity=self.config.persistence_buffer,
                )

    async def drain_persist_buffer(self) -> int:
        """Try to flush the retry buffer; return the number successfully flushed.

        Called by the service's poll loop; can also be called
        directly by tests or by the Self_Healing_Supervisor when it
        wants to retry after a Timescale recovery.
        """
        flushed = 0
        async with self._persist_lock:
            pending = list(self._persist_buffer)
            self._persist_buffer.clear()
        for output in pending:
            try:
                await self.persistence_sink.persist(output)
                flushed += 1
            except Exception as exc:  # pragma: no cover - logged + re-buffered
                _LOG.warning(
                    "shadow_persist_drain_failed",
                    component=output.component,
                    kind=output.kind.value,
                    error=str(exc),
                )
                await self._buffer_for_retry(output)
        return flushed

    # -----------------------------------------------------------------
    # Public — lifecycle
    # -----------------------------------------------------------------

    async def start(self) -> None:
        """Launch the background poll task. Idempotent."""
        if self._started:
            return
        self._started = True
        self._stopped = False
        # Refresh once synchronously so :meth:`is_shadowed` reflects
        # the live state immediately after :meth:`start` returns.
        await self.refresh()
        loop = asyncio.get_running_loop()
        self._poll_task = loop.create_task(
            self._poll_loop(), name="hedge-shadow-poll"
        )

    async def stop(self) -> None:
        """Cancel the background poll task. Idempotent."""
        if self._stopped:
            return
        self._stopped = True
        task = self._poll_task
        self._poll_task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    async def refresh(self) -> ShadowSnapshot:
        """Refresh the shadowed-set snapshot from the flag source.

        On a transient flag-source failure, the previous snapshot is
        retained and a structured warning is logged — the design's
        documented fail-open behaviour (a stalled WarmCache cannot
        wedge the entire pipeline). A tighter fail-closed policy can
        be added as a follow-up if operators decide they need it.
        """
        components = self._effective_components()
        try:
            shadowed = await self.flag_source.fetch_all(components)
        except Exception as exc:  # pragma: no cover - logged + retained
            _LOG.warning(
                "shadow_refresh_failed_retaining_snapshot",
                error=str(exc),
                previous_count=len(self._snapshot.components),
            )
            return self._snapshot
        # Union with the static seed list so a component that the
        # governance engine has not yet flagged still surfaces as
        # shadowed when an operator put it on the seed list.
        seed = self.config.normalised_seed_components()
        merged = frozenset(shadowed) | seed
        self._snapshot = ShadowSnapshot(
            components=merged,
            refreshed_at_ns=int(self.clock_ns()),
        )
        return self._snapshot

    # -----------------------------------------------------------------
    # Internals
    # -----------------------------------------------------------------

    async def _poll_loop(self) -> None:
        interval = float(self.config.poll_interval_s)
        try:
            while not self._stopped:
                await asyncio.sleep(interval)
                if self._stopped:
                    return
                await self.refresh()
                # Best-effort retry of the persistence buffer in the
                # same poll cadence so a transient Timescale outage
                # is recovered without a separate scheduler.
                if self._persist_buffer:
                    flushed = await self.drain_persist_buffer()
                    if flushed:
                        _LOG.info(
                            "shadow_persist_buffer_drained",
                            flushed=flushed,
                        )
        except asyncio.CancelledError:
            raise

    def _effective_components(self) -> tuple[str, ...]:
        if self.poll_components is not None:
            return tuple(self.poll_components)
        # Default: every canonical governed component plus any extra
        # the seed list pins.
        from ..governance.state import DEFAULT_COMPONENTS

        canon = tuple(c.value for c in DEFAULT_COMPONENTS)
        seen: set[str] = set(canon)
        result = list(canon)
        for raw in self.config.seed_components:
            if raw and raw not in seen:
                result.append(raw)
                seen.add(raw)
        return tuple(result)


# ---------------------------------------------------------------------------
# Helpers ------------------------------------------------------------------
# ---------------------------------------------------------------------------


def _is_marked_shadow(output: ShadowedOutput) -> bool:
    """Whether ``output.payload`` carries a truthy ``shadow`` field.

    The service refuses to persist a payload whose ``shadow`` flag
    is missing or falsy — the persistence path is for shadowed
    emissions only, and a non-shadowed payload here would indicate
    a bug at the upstream engine's call site.
    """
    payload = output.payload
    return bool(payload.get("shadow", False))


def chain_filters(
    *filters: Callable[[object], bool],
) -> Callable[[object], bool]:
    """Compose multiple positive filters with short-circuit AND.

    Convenience for the UI gateway when it has its own per-channel
    filter and wants to compose it with the shadow filter without
    writing a custom higher-order helper.
    """

    def _composed(payload: object) -> bool:
        for f in filters:
            if not f(payload):
                return False
        return True

    return _composed


__all__ = [
    "ShadowModeService",
    "chain_filters",
]


_ = Iterable  # silence unused-import lint while keeping the public alias.
