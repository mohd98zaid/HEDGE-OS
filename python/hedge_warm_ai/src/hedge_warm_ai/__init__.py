"""PROJECT HEDGE Warm_AI_Pipeline package.

Concrete service modules (``news``, ``regime``, ``priority``,
``prev_day``, ``psychology``, ``ranking``, ``journal``, ``governance``,
``shadow``) are added by tasks 22.x and onward.

Top-level re-exports:

* :class:`OllamaClient`, :class:`OllamaModelEndpoint`,
  :class:`OllamaResponseChunk` — async streaming inference against the
  four local Ollama daemons (R10, task 19.1).
"""

from .ollama_client import (
    OllamaAllFallbacksExhaustedError,
    OllamaClient,
    OllamaClientError,
    OllamaConnectError,
    OllamaHttpError,
    OllamaModelEndpoint,
    OllamaResponseChunk,
    OllamaTimeoutError,
)

__version__ = "0.1.0"

__all__ = [
    "OllamaAllFallbacksExhaustedError",
    "OllamaClient",
    "OllamaClientError",
    "OllamaConnectError",
    "OllamaHttpError",
    "OllamaModelEndpoint",
    "OllamaResponseChunk",
    "OllamaTimeoutError",
]
