"""Async streaming client for the Ollama_Infrastructure microservices (R10).

Task 19.1 implements:

* Independent Docker containers ``ollama-qwen``, ``ollama-mistral``,
  ``ollama-deepseek``, ``ollama-phi`` (see ``docker-compose.ollama.yml``).
* Host-level egress firewall denying outbound traffic to public LLM
  providers (see ``infra/firewall/``).
* This Python module exposing async streaming inference, a configurable
  per-model timeout, and fallback routing on unresponsive service that
  emits an ``ai.ollama.degraded`` event on NATS.

The module is the **only** place in the Warm_AI_Pipeline that talks to an
Ollama HTTP endpoint. Every News, Journal, Governance, Shadow, and RAG
service routes its LLM calls through :class:`OllamaClient`.

References
----------
- Requirements §10 (Ollama AI Infrastructure), in particular R10.6
  ("GGUF Q4_K_M on GPU"), R10.7 ("streaming inference endpoints"),
  R10.8 ("no outbound calls to any cloud LLM provider"), and R10.9
  ("on unresponsive service emit a service-degraded event and route
  to a configured fallback").
- Design § Components § Ollama_Infrastructure.
- Schema mirror: ``hedge_warm_ai.schemas.ai_ollama_degraded.OllamaDegraded``.
"""

from __future__ import annotations

from .client import OllamaClient, OllamaResponseChunk
from .endpoint import OllamaModelEndpoint, OllamaRoleKey, default_endpoints, default_fallback_chain
from .errors import (
    OllamaAllFallbacksExhaustedError,
    OllamaClientError,
    OllamaConnectError,
    OllamaHttpError,
    OllamaTimeoutError,
)
from .publisher import (
    DegradedEventPublisher,
    InMemoryDegradedPublisher,
    NatsDegradedPublisher,
    NoopDegradedPublisher,
)

__all__ = [
    # Client
    "OllamaClient",
    "OllamaResponseChunk",
    # Endpoint
    "OllamaModelEndpoint",
    "OllamaRoleKey",
    "default_endpoints",
    "default_fallback_chain",
    # Errors
    "OllamaAllFallbacksExhaustedError",
    "OllamaClientError",
    "OllamaConnectError",
    "OllamaHttpError",
    "OllamaTimeoutError",
    # Publisher
    "DegradedEventPublisher",
    "InMemoryDegradedPublisher",
    "NatsDegradedPublisher",
    "NoopDegradedPublisher",
]
