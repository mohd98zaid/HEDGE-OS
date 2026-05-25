"""Exception hierarchy for :mod:`hedge_warm_ai.ollama_client`.

Why a dedicated hierarchy rather than re-exporting ``httpx.HTTPError``?
The Warm_AI_Pipeline must distinguish *unresponsiveness* (R10.9 — fallback
trigger) from *bad request* (R10 — caller bug, no fallback). A
narrow hierarchy makes that distinction explicit at every call site.

Class layout::

    OllamaClientError                     <- base, never raised directly
    ├── OllamaTimeoutError                <- wall-clock timeout exceeded
    ├── OllamaConnectError                <- TCP/DNS refused or reset
    ├── OllamaHttpError                   <- non-2xx HTTP status
    └── OllamaAllFallbacksExhaustedError  <- whole chain failed
"""

from __future__ import annotations

from typing import Any


class OllamaClientError(Exception):
    """Base class for every error raised by :class:`OllamaClient`.

    Holds the *role* and *model* identifiers so callers and structured
    log scrapers can filter on them without having to parse the message.
    """

    def __init__(self, message: str, *, role: str, model: str) -> None:
        super().__init__(message)
        self.role = role
        self.model = model

    def __reduce__(self) -> tuple[Any, ...]:  # pragma: no cover - pickling
        return (self.__class__, (str(self), self.role, self.model))


class OllamaTimeoutError(OllamaClientError):
    """Raised when an Ollama request exceeds its per-model timeout (R10.9).

    Maps to ``OllamaDegraded.reason == "timeout"`` when emitted on the
    ``ai.ollama.degraded`` subject.
    """


class OllamaConnectError(OllamaClientError):
    """Raised when the daemon refuses the connection or the TCP stream resets.

    Maps to ``OllamaDegraded.reason == "unresponsive"``.
    """


class OllamaHttpError(OllamaClientError):
    """Raised on any non-2xx HTTP status from the Ollama daemon.

    Carries the HTTP status code so 5xx (treated as unresponsive — the
    daemon crashed or panicked) can be distinguished from 4xx (caller bug,
    no fallback). The ``classify_reason`` helper below returns the
    ``OllamaDegraded.reason`` value to use when emitting a degraded event;
    callers may use it to decide whether to fall back at all.
    """

    def __init__(self, message: str, *, role: str, model: str, status_code: int) -> None:
        super().__init__(message, role=role, model=model)
        self.status_code = status_code

    @property
    def is_unresponsive(self) -> bool:
        """``True`` iff the status code indicates a server-side fault.

        5xx → server-side fault, fallback applies. 4xx → caller bug, do
        not fall back (would just loop) — re-raise to the application.
        """
        return 500 <= self.status_code < 600


class OllamaAllFallbacksExhaustedError(OllamaClientError):
    """Raised when every model in the fallback chain has failed.

    Holds the original failures so callers can log the full causal
    chain. The ``role`` and ``model`` on the base class refer to the
    *originally requested* role + model so the failure is attributed
    to the correct entry point.
    """

    def __init__(
        self,
        message: str,
        *,
        role: str,
        model: str,
        failures: list[tuple[str, OllamaClientError]],
    ) -> None:
        super().__init__(message, role=role, model=model)
        self.failures = failures
