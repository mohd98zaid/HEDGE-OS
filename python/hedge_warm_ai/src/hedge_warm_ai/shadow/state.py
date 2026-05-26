"""Value types consumed and produced by the AI_Shadow_Mode service.

Three immutable, frozen ``slots=True`` dataclasses live here:

* :class:`ShadowedOutput` — one persisted shadowed emission. The
  service's :meth:`ShadowModeService.persist_output` API takes one of
  these per upstream-engine emission; the persistence sink writes
  them into the matching Timescale hypertable
  (``ai_scores`` for ``RankedSignal``, ``regime_history`` for
  ``RegimeChanged``, etc.).
* :class:`ShadowSnapshot` — the in-memory view of which components
  are currently shadowed. The service refreshes it from the interim
  WarmCache flag namespace on every poll cycle. Upstream engines
  consult :meth:`ShadowModeService.is_shadowed` (which reads from a
  snapshot) before publishing.
* :class:`ShadowKind` — discriminator the service uses to select the
  right Timescale hypertable for a given canonical schema. Each
  named value maps onto one of the wire payloads listed in the
  shadow design (R23.1).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Final, Mapping


class ShadowKind(str, Enum):
    """Discriminator linking a wire payload to a persistence lane.

    The names match the canonical ``ai.*`` / ``mem.*`` subjects so a
    consumer can pick the right Timescale hypertable without
    embedding a string-to-string lookup table elsewhere. The
    enumeration is intentionally bounded to the seven canonical
    Warm_AI_Pipeline emissions plus a generic ``OTHER`` slot for
    components that publish under a non-canonical subject (the
    persistence sink will log a warning and skip the row).
    """

    AI_RANK = "ai_rank"
    AI_REGIME_CHANGED = "ai_regime_changed"
    AI_PSYCH_STABILITY = "ai_psych_stability"
    AI_PRIORITY_CHANGED = "ai_priority_changed"
    AI_NEWS_IMPACT = "ai_news_impact"
    AI_JOURNAL_ENTRY = "ai_journal_entry"
    MEM_PREV_DAY = "mem_prev_day"
    OTHER = "other"


@dataclass(frozen=True, slots=True)
class ShadowedOutput:
    """One shadowed Warm_AI_Pipeline emission destined for persistence.

    The service does not own the canonical wire payload; it carries
    a pre-validated dict copy plus enough metadata to route it to
    the right hypertable. ``payload`` is the JSON-compatible mapping
    a consumer would receive on the wire — the upstream-engine
    publisher constructs it via ``model_dump(mode="json")`` before
    handing the entry to the shadow service.

    Attributes:
        kind: Discriminator selecting the Timescale lane.
        component: Engine-internal component name. Mirrors the
            value from :class:`hedge_warm_ai.governance.state.GovernedComponent`.
        payload: Pre-validated JSON-compatible mapping representing
            the wire payload. The mapping's ``shadow`` key must be
            ``True`` for a payload accepted by the service (the
            service rejects entries whose ``shadow`` flag is missing
            or set to ``False`` so a non-shadowed emission cannot
            slip through the persistence path).
        ts_ns: Wall-clock nanoseconds at the moment the shadowed
            emission was produced. Falls back to the service's
            clock when the payload does not carry a ``ts_ns``.
        correlation_id: Optional correlation id for the emission.
            Used by the AI_Governance_Engine's
            :class:`ShadowedOutputObserver` adaptor to score the
            shadowed output against subsequent realised outcomes
            (R23.3). ``None`` when the emission has no correlation
            id (e.g. ``ai.regime.changed``).
    """

    kind: ShadowKind
    component: str
    payload: Mapping[str, Any]
    ts_ns: int
    correlation_id: str | None = None


@dataclass(frozen=True, slots=True)
class ShadowSnapshot:
    """Immutable view of the shadowed-component set at a point in time.

    Constructed by :meth:`ShadowModeService.refresh` from the union
    of the static seed list and the live flag namespace. Upstream
    engines treat the snapshot as a read-only set; mutation goes
    through the service.
    """

    components: frozenset[str] = field(default_factory=frozenset)
    refreshed_at_ns: int = 0

    def is_shadowed(self, component: str) -> bool:
        """Return whether ``component`` is currently shadowed."""
        return component in self.components

    def __contains__(self, item: object) -> bool:  # noqa: D401
        if not isinstance(item, str):
            return False
        return self.is_shadowed(item)

    def __iter__(self):  # noqa: D401
        return iter(self.components)

    def __len__(self) -> int:  # noqa: D401
        return len(self.components)


#: Empty :class:`ShadowSnapshot` used as the default before the
#: service has performed its first refresh.
EMPTY_SHADOW_SNAPSHOT: Final[ShadowSnapshot] = ShadowSnapshot(
    components=frozenset(),
    refreshed_at_ns=0,
)


__all__ = [
    "EMPTY_SHADOW_SNAPSHOT",
    "ShadowKind",
    "ShadowSnapshot",
    "ShadowedOutput",
]
