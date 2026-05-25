"""Persistence sink for the AI_Governance_Engine (R23.3, R24.1).

The engine writes one :class:`hedge_memory_rag.timescale.GovernanceMetricSample`
row per:

* metric snapshot (continued tracking, ``action`` is ``None``);
* edge transition (``action`` is one of
  ``reduce_influence | shadow_mode | rollback``); and
* prediction-quality outcome correlated with
  ``exec.trade.closed`` / ``pos.update.<sym>``
  (``correlation_id`` populated, ``metric_kind == "prediction_quality"``).

The sink is decoupled behind a small Protocol so the engine never
imports :mod:`hedge_memory_rag` at module-import time and unit tests
can substitute fakes. The production binding wraps
:class:`hedge_memory_rag.timescale.TimescaleWriter.write_governance_metric`.

All operations are async and fail-soft: if persistence raises, the
engine logs at ``warning`` level and proceeds with the rest of the
publish + cache pipeline so a single down dependency cannot cause the
governance engine to drop the emission.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Final, Optional, Protocol

import structlog

from .ladder import GovernanceLevel
from .state import GovernedComponent, MetricKind, wire_metric_for

if TYPE_CHECKING:  # pragma: no cover - typing only
    from hedge_memory_rag.timescale import TimescaleWriter

_LOG: Final = structlog.get_logger(__name__)


def _ns_to_utc(ts_ns: int) -> datetime:
    return datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=timezone.utc)


# ---------------------------------------------------------------------------
# Protocol -----------------------------------------------------------------
# ---------------------------------------------------------------------------


class GovernanceMetricSink(Protocol):
    """Persists one governance metric sample to TimescaleDB."""

    async def write_governance_metric(
        self,
        *,
        component: GovernedComponent,
        kind: MetricKind,
        value: float,
        threshold: float,
        level: GovernanceLevel,
        action: Optional[str],
        correlation_id: Optional[str],
        sample_count: int,
        ts_ns: int,
    ) -> None: ...


# ---------------------------------------------------------------------------
# No-op sink ---------------------------------------------------------------
# ---------------------------------------------------------------------------


class NoopGovernanceMetricSink:
    """Drop-in stub used when Timescale is not wired yet."""

    async def write_governance_metric(
        self,
        *,
        component: GovernedComponent,
        kind: MetricKind,
        value: float,
        threshold: float,
        level: GovernanceLevel,
        action: Optional[str],
        correlation_id: Optional[str],
        sample_count: int,
        ts_ns: int,
    ) -> None:  # noqa: D401
        return


# ---------------------------------------------------------------------------
# Production binding -------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass
class TimescaleGovernanceMetricSink:
    """Wraps :class:`TimescaleWriter.write_governance_metric`.

    Failures are logged + swallowed so a transient Timescale outage
    cannot drop the engine's emission. The typed
    :class:`GovernancePersistenceError` is reserved for the
    Self_Healing_Supervisor's eventual handler — today the sink only
    logs because the engine layer above already classifies failures
    via the warning-channel structlog event.
    """

    writer: "TimescaleWriter"

    async def write_governance_metric(
        self,
        *,
        component: GovernedComponent,
        kind: MetricKind,
        value: float,
        threshold: float,
        level: GovernanceLevel,
        action: Optional[str],
        correlation_id: Optional[str],
        sample_count: int,
        ts_ns: int,
    ) -> None:
        # Lazy import keeps :mod:`hedge_memory_rag` out of the import
        # path until first use, mirroring the journal subpackage's
        # pattern (``journal_entry_to_row``).
        try:
            from hedge_memory_rag.timescale import (
                GovernanceMetricSample as _GovernanceMetricRow,
            )
        except ImportError:
            _LOG.warning(
                "governance_metric_persist_unavailable",
                component=component.value,
                metric_kind=kind.value,
            )
            return

        row = _GovernanceMetricRow(
            ts=_ns_to_utc(ts_ns),
            component=component.value,
            metric=wire_metric_for(kind),  # type: ignore[arg-type]
            metric_kind=kind.value,  # type: ignore[arg-type]
            value=float(value),
            threshold=float(threshold),
            level=level.value,  # type: ignore[arg-type]
            action=action,  # type: ignore[arg-type]
            correlation_id=correlation_id,
            sample_count=int(sample_count),
        )
        try:
            await self.writer.write_governance_metric(row)
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "governance_metric_persist_failed",
                component=component.value,
                metric_kind=kind.value,
                value=value,
                threshold=threshold,
                level=level.value,
                action=action,
                error=str(exc),
            )


__all__ = [
    "GovernanceMetricSink",
    "NoopGovernanceMetricSink",
    "TimescaleGovernanceMetricSink",
]
