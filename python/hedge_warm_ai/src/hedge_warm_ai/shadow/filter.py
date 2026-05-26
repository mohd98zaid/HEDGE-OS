"""UI gateway filter for shadowed Warm_AI_Pipeline outputs (R23.2).

R23.2 requires that the Human_Control_UI **does not** use a
shadowed component's outputs to influence the ranked-signal display
shown to the trader. This module is the small composable callable
the UI gateway (task 36.1) applies to its ``/signals`` channel.

Design choice — *one callable, multiple consumers*:

* The filter is implemented as a tiny ``Callable[[Mapping], bool]``
  so the UI gateway can compose it with its existing
  topic-subscription protocol — the gateway wraps every
  per-subscription handler with the filter and only forwards the
  payload to the trader's WebSocket if :meth:`__call__` returns
  ``True``.
* The filter is *positive*: ``True`` means "let this through",
  ``False`` means "drop". This matches the gateway's existing
  callback shape (``Callable[[Payload], bool]``) and is the same
  convention used by :class:`hedge_warm_ai.regime.publisher`.
* The filter does **not** consult the shadowed-set snapshot. It
  reads the canonical ``shadow: bool`` field on the wire payload
  itself. This is a deliberate decoupling: the upstream engines
  produce payloads tagged ``shadow=True`` after consulting
  :meth:`ShadowModeService.is_shadowed`, and the UI filter is
  pure — given the same input it returns the same output. The
  filter therefore works correctly even when the shadow service is
  unreachable from the UI gateway (the design's documented
  fail-open behaviour: the UI shows a slightly stale view but
  never violates R23.2 because the wire payload itself is the
  source of truth).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Final, Mapping

import structlog

_LOG: Final = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Pure helpers -------------------------------------------------------------
# ---------------------------------------------------------------------------


def is_payload_shadowed(payload: object) -> bool:
    """Return ``True`` when ``payload`` carries ``shadow=True``.

    The function is intentionally permissive about the payload
    shape — any mapping with a truthy ``shadow`` field counts as
    shadowed; any mapping without one (or with ``shadow=False``)
    counts as not-shadowed. Non-mapping payloads (e.g. control
    messages) count as not-shadowed.

    Examples::

        >>> is_payload_shadowed({"shadow": True})
        True
        >>> is_payload_shadowed({"shadow": False})
        False
        >>> is_payload_shadowed({"trade_confidence_score": 0.7})
        False
        >>> is_payload_shadowed("ping")
        False
    """
    if isinstance(payload, Mapping):
        return bool(payload.get("shadow", False))
    return False


def passes_ui_filter(payload: object) -> bool:
    """Return ``True`` when ``payload`` is safe to surface to the trader.

    A thin negation of :func:`is_payload_shadowed` — kept as a named
    function so the UI gateway can pass it directly into a
    ``filter(...)`` call without an inline lambda.
    """
    return not is_payload_shadowed(payload)


# ---------------------------------------------------------------------------
# Reusable callable --------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass
class ShadowFilter:
    """Drop-shadowed-payloads filter for the UI gateway's ``/signals`` channel.

    The filter is a frozen callable so the UI gateway can keep one
    instance per ``/signals`` subscription. Its ``__call__`` returns
    ``True`` when the payload is *not* shadowed (i.e. the gateway
    should forward it).

    Attributes:
        log_dropped: When ``True`` (the default), every dropped
            payload emits a structured ``ui_shadow_drop`` log so
            operators can see the filter is active in production.
        channel_label: Optional channel tag for the structured log
            line. Useful when a single :class:`ShadowFilter`
            instance is reused across multiple subscriptions.
    """

    log_dropped: bool = True
    channel_label: str = "/signals"

    def __call__(self, payload: object) -> bool:
        if is_payload_shadowed(payload):
            if self.log_dropped:
                _LOG.info(
                    "ui_shadow_drop",
                    channel=self.channel_label,
                    component=_extract_component(payload),
                )
            return False
        return True

    @staticmethod
    def as_callable(
        *, log_dropped: bool = True, channel_label: str = "/signals"
    ) -> Callable[[object], bool]:
        """Return a fresh :class:`ShadowFilter` typed as ``Callable[[object], bool]``.

        Convenience for callers that want to bind the filter into a
        higher-order pipeline (``map``, ``filter``, async stream
        operator) without exposing the dataclass type at the call
        site.
        """
        return ShadowFilter(log_dropped=log_dropped, channel_label=channel_label)


def _extract_component(payload: object) -> str | None:
    """Best-effort component label for the structured drop log."""
    if isinstance(payload, Mapping):
        for key in ("component", "source_component", "engine"):
            value = payload.get(key)
            if isinstance(value, str) and value:
                return value
    return None


__all__ = [
    "ShadowFilter",
    "is_payload_shadowed",
    "passes_ui_filter",
]


_ = Any  # silence unused-import lint while keeping the public type alias.
