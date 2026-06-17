"""Endpoint and routing-key types for :mod:`hedge_warm_ai.ollama_client`.

Each :class:`OllamaModelEndpoint` describes one Ollama daemon (URL +
model + per-model timeout). A :class:`OllamaClient` is parameterised
with a mapping of *role keys* to endpoints and a *fallback chain* that
specifies which role serves the request when the primary role is
unresponsive (R10.9).

The role keys are deliberately decoupled from :class:`hedge_warm_ai.
config.OllamaRole` so callers can register multiple endpoints under the
same role (e.g. two ``deep`` daemons on different hosts) if a future
deployment topology requires it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

#: Canonical role key type. A short ``str`` keeps the API ergonomic;
#: enforcing membership in a closed set is left to the caller (the
#: ``hedge_warm_ai.config.OllamaConfig`` registry validates roles
#: against :class:`hedge_warm_ai.config.OllamaRole` at config-load time).
OllamaRoleKey = str


@dataclass(frozen=True, slots=True)
class OllamaModelEndpoint:
    """One Ollama daemon endpoint with its own model and timeout.

    Attributes:
        base_url: Root URL of the daemon's HTTP API. The streaming
            inference endpoint is constructed by appending ``/api/generate``.
            Example: ``http://ollama-qwen:11434``.
        model: GGUF tag of the model the daemon was started with. Sent
            in the ``model`` field of every ``/api/generate`` request.
            Must match the tag the daemon was ``ollama pull``-ed with —
            otherwise the daemon returns HTTP 404 and the client raises
            :class:`OllamaHttpError`.
        timeout_s: Per-model wall-clock budget for *one* full streaming
            response. The Ollama daemon streams NDJSON tokens; the
            timeout applies end-to-end. ``None`` disables the timeout.
        connect_timeout_s: Connect-phase timeout. Defaults to ``2.0`` s
            so a dead container fails fast and triggers fallback.
            Always smaller than ``timeout_s`` in practice.
        max_retries: Per-endpoint transient-error retry count. ``0``
            disables retry (immediate fallback). Retries apply *before*
            the fallback chain is consulted; one retry attempt = one
            extra full streaming call against the same endpoint.

    Example::

        OllamaModelEndpoint(
            base_url="http://ollama-qwen:11434",
            model="qwen2.5:14b-instruct-q4_K_M",
            timeout_s=30.0,
        )
    """

    base_url: str
    model: str
    timeout_s: float | None = 30.0
    connect_timeout_s: float = 2.0
    max_retries: int = 0

    def __post_init__(self) -> None:
        if not self.base_url:
            raise ValueError("base_url must be a non-empty URL")
        if not self.model:
            raise ValueError("model must be a non-empty GGUF tag")
        if self.timeout_s is not None and self.timeout_s <= 0:
            raise ValueError(f"timeout_s must be > 0 or None, got {self.timeout_s!r}")
        if self.connect_timeout_s <= 0:
            raise ValueError(
                f"connect_timeout_s must be > 0, got {self.connect_timeout_s!r}"
            )
        if self.max_retries < 0:
            raise ValueError(f"max_retries must be >= 0, got {self.max_retries!r}")

    @property
    def generate_url(self) -> str:
        """URL for the streaming generation endpoint."""
        return f"{self.base_url.rstrip('/')}/api/generate"


# ---------------------------------------------------------------------------
# Defaults matching docker-compose.ollama.yml --------------------------------
# ---------------------------------------------------------------------------
#
# The container hostnames mirror the ``container_name`` keys in
# ``docker-compose.ollama.yml``. Timeouts are conservative defaults; a
# concrete service (Journal, News, Ranking) tunes its own values via
# ``hedge_warm_ai.config.HedgeConfig``.

_DEFAULT_QWEN: Final = OllamaModelEndpoint(
    base_url="http://host.docker.internal:11434",
    model="gemma4:31b-cloud",
    timeout_s=30.0,
)
_DEFAULT_MISTRAL: Final = OllamaModelEndpoint(
    base_url="http://host.docker.internal:11434",
    model="gemma4:31b-cloud",
    timeout_s=10.0,
)
_DEFAULT_DEEPSEEK: Final = OllamaModelEndpoint(
    base_url="http://host.docker.internal:11434",
    model="gemma4:31b-cloud",
    timeout_s=60.0,
)
_DEFAULT_PHI: Final = OllamaModelEndpoint(
    base_url="http://host.docker.internal:11434",
    model="gemma4:31b-cloud",
    timeout_s=5.0,
)


def default_endpoints() -> dict[OllamaRoleKey, OllamaModelEndpoint]:
    """Return the default role → endpoint mapping for the four daemons.

    The keys (``"qwen"``, ``"mistral"``, ``"deepseek"``, ``"phi"``)
    match the role mnemonics used in the design document and the
    ``HEDGE_OLLAMA_ROLE`` env var on each container.
    """
    return {
        "qwen": _DEFAULT_QWEN,
        "mistral": _DEFAULT_MISTRAL,
        "deepseek": _DEFAULT_DEEPSEEK,
        "phi": _DEFAULT_PHI,
    }


def default_fallback_chain() -> dict[OllamaRoleKey, OllamaRoleKey]:
    """Return the default fallback chain for the four daemons.

    Chain (R10.9):

        qwen → deepseek → mistral → phi → (terminal)

    The ``phi`` model has no fallback because it is the smallest
    daemon — if even Phi is unresponsive the host is in a bad state
    and we should surface the failure rather than silently loop.
    """
    return {
        "qwen": "deepseek",
        "deepseek": "mistral",
        "mistral": "phi",
    }


__all__ = [
    "OllamaModelEndpoint",
    "OllamaRoleKey",
    "default_endpoints",
    "default_fallback_chain",
]
