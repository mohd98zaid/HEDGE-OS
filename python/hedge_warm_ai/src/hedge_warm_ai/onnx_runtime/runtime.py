"""ONNX Runtime session wrapper used by the Warm_AI_Pipeline.

Design rules:

* Sessions are created once per ``model_name`` and cached. Cache lookups
  are O(1) and thread-safe.
* Sessions are configured with ``intra_op_num_threads=1`` and
  ``ORT_ENABLE_ALL`` graph optimisation per design § Warm_AI_Pipeline
  Architecture (R11.3, R11.4).
* CUDA execution is preferred when the ``CUDAExecutionProvider`` is
  available; otherwise the runtime falls back to ``CPUExecutionProvider``
  silently and structured-logs the choice.
* Synchronous ONNX inference is dispatched via :func:`asyncio.to_thread`
  onto a bounded thread-pool executor (max 4 workers per design's
  Warm_AI_Pipeline budget). The event loop is never blocked.
* Every async inference call is wrapped in a :class:`LatencyTracer` so a
  ``LatencyRecord`` is published on ``obs.latency.ai_<stage>`` for every
  call (R11.4, R27.4).

The runtime is **stage-name aware**: callers pass a ``stage`` string
(``finbert``, ``xgboost``, ...) that drives the latency subject suffix
and looks up the per-stage budget in
:data:`DEFAULT_AI_LATENCY_BUDGETS_NS`.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final, Mapping, Optional, Sequence, Union

import numpy as np
import structlog

from .latency import (
    AiLatencyEmitter,
    LatencyTracer,
    NoopAiLatencyEmitter,
)

_LOG: Final = structlog.get_logger(__name__)

# Per-design latency budgets for Warm_AI_Pipeline AI stages.
#
# The design specifies:
#   * Fast NLP scoring (FinBERT, DistilBERT) p95 < 10 ms (R11.4, R12.2).
#   * Classical ML (XGBoost, LightGBM, Isolation Forest) p95 < 5 ms.
#   * Tiny LSTM p95 < 8 ms.
#
# We use the p95 ceilings as the per-call budgets. A p95 budget on a
# per-call breach is intentionally loose — Prometheus aggregations
# remain the authoritative SLO surface.
DEFAULT_AI_LATENCY_BUDGETS_NS: Final[Mapping[str, int]] = {
    "finbert": 10_000_000,            # 10 ms
    "distilbert": 10_000_000,         # 10 ms
    "xgboost": 5_000_000,             # 5 ms
    "lightgbm": 5_000_000,            # 5 ms
    "isolation_forest": 5_000_000,    # 5 ms
    "tiny_lstm": 8_000_000,           # 8 ms
}


@dataclass(frozen=True, slots=True)
class OnnxRuntimeConfig:
    """Configuration knobs for an :class:`OnnxRuntime` instance.

    Attributes:
        intra_op_num_threads: Per-session intra-op thread count.
            Forced to ``1`` per design to keep p95 deterministic.
        graph_optimization_level: ONNX Runtime graph optimisation
            preset. Defaults to ``ORT_ENABLE_ALL``.
        prefer_cuda: When ``True``, sessions try
            ``CUDAExecutionProvider`` first and fall back to
            ``CPUExecutionProvider``.
        max_thread_pool_workers: Bound on the inference thread pool.
            The Warm_AI_Pipeline budget is 4 workers.
        log_provider_choice: When ``True``, log the chosen provider on
            session creation. Useful in production diagnostics.
    """

    intra_op_num_threads: int = 1
    graph_optimization_level: str = "ORT_ENABLE_ALL"
    prefer_cuda: bool = True
    max_thread_pool_workers: int = 4
    log_provider_choice: bool = True


@dataclass(slots=True)
class SessionHandle:
    """A cached ONNX Runtime session and its metadata."""

    model_name: str
    stage: str
    session: Any  # onnxruntime.InferenceSession
    input_names: tuple[str, ...]
    output_names: tuple[str, ...]
    providers: tuple[str, ...]


def _import_onnxruntime() -> Any:
    """Import :mod:`onnxruntime` lazily and raise a helpful error if missing."""
    try:
        import onnxruntime as ort  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - exercised at runtime, not in tests
        raise ImportError(
            "onnxruntime is not installed. Add `onnxruntime>=1.18` (or "
            "`onnxruntime-gpu` for CUDA) to the active Python environment."
        ) from exc
    return ort


def _resolve_providers(ort: Any, prefer_cuda: bool) -> list[str]:
    """Return the provider list to pass to ``InferenceSession``."""
    available = set(ort.get_available_providers())
    chosen: list[str] = []
    if prefer_cuda and "CUDAExecutionProvider" in available:
        chosen.append("CUDAExecutionProvider")
    chosen.append("CPUExecutionProvider")
    return chosen


def _session_options(ort: Any, config: OnnxRuntimeConfig) -> Any:
    """Build a configured ``SessionOptions`` instance."""
    opts = ort.SessionOptions()
    opts.intra_op_num_threads = max(1, int(config.intra_op_num_threads))
    # Map the string preset to ONNX Runtime's enum value. We accept the
    # preset name so callers don't need to import onnxruntime themselves.
    level = config.graph_optimization_level
    enum_value = getattr(ort.GraphOptimizationLevel, level, None)
    if enum_value is None:
        # Fall back to ORT_ENABLE_ALL if the supplied name is unknown,
        # log the substitution for the operator.
        _LOG.warning(
            "unknown_graph_optimization_level",
            requested=level,
            fallback="ORT_ENABLE_ALL",
        )
        enum_value = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
    opts.graph_optimization_level = enum_value
    # Surface OpenMP / MKL noise as warnings only — keep stdout clean for
    # the structured logger.
    opts.log_severity_level = 3
    return opts


class OnnxRuntime:
    """Cache + dispatcher for ONNX Runtime inference sessions.

    Instances are safe to share across coroutines: session creation is
    serialised by an asyncio lock and the inference dispatch fans out to
    a bounded thread-pool executor.
    """

    def __init__(
        self,
        *,
        config: Optional[OnnxRuntimeConfig] = None,
        emitter: Optional[AiLatencyEmitter] = None,
        budgets_ns: Optional[Mapping[str, int]] = None,
    ) -> None:
        self._config = config or OnnxRuntimeConfig()
        self._emitter: AiLatencyEmitter = emitter or NoopAiLatencyEmitter()
        self._budgets_ns = dict(budgets_ns) if budgets_ns else dict(DEFAULT_AI_LATENCY_BUDGETS_NS)
        self._sessions: dict[str, SessionHandle] = {}
        self._sessions_lock = threading.RLock()
        self._loader_lock = asyncio.Lock()
        self._executor = ThreadPoolExecutor(
            max_workers=self._config.max_thread_pool_workers,
            thread_name_prefix="hedge-onnx",
        )
        self._closed = False

    @property
    def config(self) -> OnnxRuntimeConfig:
        return self._config

    @property
    def emitter(self) -> AiLatencyEmitter:
        return self._emitter

    def budget_ns(self, stage: str) -> int:
        """Return the configured per-call budget for *stage* (0 = unbudgeted)."""
        return int(self._budgets_ns.get(stage, 0))

    # -- session loading ----------------------------------------------------

    def get_cached(self, model_name: str) -> Optional[SessionHandle]:
        """Return the cached :class:`SessionHandle` or ``None``."""
        with self._sessions_lock:
            return self._sessions.get(model_name)

    async def load_session(
        self,
        model_name: str,
        model_path_or_bytes: Union[str, Path, bytes, bytearray, memoryview],
        *,
        stage: Optional[str] = None,
    ) -> SessionHandle:
        """Load a model into the cache. Idempotent.

        Loading happens off the event loop via :func:`asyncio.to_thread`
        because :class:`onnxruntime.InferenceSession` construction can
        block for tens of milliseconds while it parses the graph.
        """
        if self._closed:
            raise RuntimeError("OnnxRuntime is closed")

        # Fast path: hit the cache without taking the loader lock.
        cached = self.get_cached(model_name)
        if cached is not None:
            return cached

        async with self._loader_lock:
            cached = self.get_cached(model_name)
            if cached is not None:
                return cached
            handle = await asyncio.to_thread(
                self._load_session_blocking,
                model_name,
                model_path_or_bytes,
                stage or model_name,
            )
            with self._sessions_lock:
                self._sessions[model_name] = handle
            return handle

    def _load_session_blocking(
        self,
        model_name: str,
        model_path_or_bytes: Union[str, Path, bytes, bytearray, memoryview],
        stage: str,
    ) -> SessionHandle:
        """Synchronously construct a session. Runs in the loader thread."""
        ort = _import_onnxruntime()
        opts = _session_options(ort, self._config)
        providers = _resolve_providers(ort, self._config.prefer_cuda)

        # ONNX Runtime accepts a path *or* a bytes-like model. We branch
        # so callers can ship in-memory models (used by the test suite).
        model_arg: Union[str, bytes]
        if isinstance(model_path_or_bytes, (bytes, bytearray, memoryview)):
            model_arg = bytes(model_path_or_bytes)
        else:
            model_arg = str(model_path_or_bytes)

        try:
            session = ort.InferenceSession(model_arg, sess_options=opts, providers=providers)
        except Exception as exc:
            # Some platforms (notably plain CPU wheels) reject the
            # ``CUDAExecutionProvider`` registration. Retry CPU-only.
            if "CUDAExecutionProvider" in providers and "CPUExecutionProvider" in providers:
                _LOG.warning(
                    "cuda_provider_unavailable_falling_back_to_cpu",
                    model=model_name,
                    error=str(exc),
                )
                providers = ["CPUExecutionProvider"]
                session = ort.InferenceSession(
                    model_arg, sess_options=opts, providers=providers
                )
            else:
                raise

        input_names = tuple(node.name for node in session.get_inputs())
        output_names = tuple(node.name for node in session.get_outputs())
        active_providers = tuple(session.get_providers())

        if self._config.log_provider_choice:
            _LOG.info(
                "onnx_session_loaded",
                model=model_name,
                stage=stage,
                providers=active_providers,
                inputs=input_names,
                outputs=output_names,
            )

        return SessionHandle(
            model_name=model_name,
            stage=stage,
            session=session,
            input_names=input_names,
            output_names=output_names,
            providers=active_providers,
        )

    # -- inference ----------------------------------------------------------

    async def infer_classical(
        self,
        model_name: str,
        features: np.ndarray,
        *,
        stage: Optional[str] = None,
        feed_name: Optional[str] = None,
        output_index: int = 0,
    ) -> np.ndarray:
        """Run a classical-ML inference and return the chosen output.

        Args:
            model_name: Cache key. The session must have been loaded via
                :meth:`load_session`.
            features: Input array. Coerced to ``float32`` if the source
                dtype differs (matches ONNX defaults).
            stage: Latency-tracing stage suffix; defaults to *model_name*.
            feed_name: Override the input feed name. Defaults to the
                first declared input of the session.
            output_index: Index into the session outputs to return.

        Returns:
            A NumPy array containing the selected output.
        """
        handle = self._require_session(model_name)
        stage_name = stage or handle.stage
        prepared = self._coerce_features(features)
        feed = self._build_feed(handle, prepared, feed_name)

        with LatencyTracer(stage_name, self._emitter, budget_ns=self.budget_ns(stage_name)):
            outputs = await asyncio.to_thread(self._run_blocking, handle, feed)

        if output_index < 0 or output_index >= len(outputs):
            raise IndexError(
                f"output_index {output_index} out of range for model {model_name!r} "
                f"({len(outputs)} outputs)"
            )
        result = outputs[output_index]
        return np.asarray(result)

    async def infer_classical_full(
        self,
        model_name: str,
        features: np.ndarray,
        *,
        stage: Optional[str] = None,
        feed_name: Optional[str] = None,
    ) -> dict[str, np.ndarray]:
        """Variant of :meth:`infer_classical` returning every output by name."""
        handle = self._require_session(model_name)
        stage_name = stage or handle.stage
        prepared = self._coerce_features(features)
        feed = self._build_feed(handle, prepared, feed_name)

        with LatencyTracer(stage_name, self._emitter, budget_ns=self.budget_ns(stage_name)):
            outputs = await asyncio.to_thread(self._run_blocking, handle, feed)

        return {name: np.asarray(arr) for name, arr in zip(handle.output_names, outputs)}

    async def infer_nlp(
        self,
        model_name: str,
        feed: Mapping[str, np.ndarray],
        *,
        stage: Optional[str] = None,
    ) -> dict[str, np.ndarray]:
        """Run an NLP-model forward pass and return every named output.

        NLP models receive a multi-input feed (typically ``input_ids``,
        ``attention_mask``, optionally ``token_type_ids``). Tokenisation
        is the caller's responsibility — see
        :class:`hedge_warm_ai.onnx_runtime.nlp.FinBERTSentiment` and
        :class:`~.nlp.DistilBERTEmbedding`.
        """
        handle = self._require_session(model_name)
        stage_name = stage or handle.stage
        # Coerce all inputs to the dtype the session expects. We cannot
        # know the dtype without inspecting the input metadata, but
        # tokenisers typically emit int64 tensors — we leave them
        # unchanged and only coerce float arrays.
        prepared_feed: dict[str, np.ndarray] = {}
        for name, arr in feed.items():
            if name not in handle.input_names:
                raise KeyError(
                    f"Input {name!r} not declared by model {model_name!r}; "
                    f"expected one of {handle.input_names}"
                )
            prepared_feed[name] = np.ascontiguousarray(arr)

        with LatencyTracer(stage_name, self._emitter, budget_ns=self.budget_ns(stage_name)):
            outputs = await asyncio.to_thread(self._run_blocking, handle, prepared_feed)

        return {name: np.asarray(arr) for name, arr in zip(handle.output_names, outputs)}

    # -- helpers ------------------------------------------------------------

    def _require_session(self, model_name: str) -> SessionHandle:
        handle = self.get_cached(model_name)
        if handle is None:
            raise KeyError(
                f"Model {model_name!r} is not loaded. Call OnnxRuntime.load_session "
                f"before inference."
            )
        return handle

    @staticmethod
    def _coerce_features(features: np.ndarray) -> np.ndarray:
        if not isinstance(features, np.ndarray):
            features = np.asarray(features)
        if features.dtype != np.float32:
            features = features.astype(np.float32, copy=False)
        if features.ndim == 1:
            features = features.reshape(1, -1)
        return np.ascontiguousarray(features)

    @staticmethod
    def _build_feed(
        handle: SessionHandle,
        features: np.ndarray,
        feed_name: Optional[str],
    ) -> dict[str, np.ndarray]:
        if not handle.input_names:
            raise RuntimeError(f"Model {handle.model_name!r} declares no inputs")
        name = feed_name or handle.input_names[0]
        return {name: features}

    @staticmethod
    def _run_blocking(handle: SessionHandle, feed: Mapping[str, np.ndarray]) -> Sequence[Any]:
        return handle.session.run(None, dict(feed))

    # -- lifecycle ----------------------------------------------------------

    async def close(self) -> None:
        """Release the cached sessions and the inference executor."""
        if self._closed:
            return
        self._closed = True
        with self._sessions_lock:
            self._sessions.clear()
        # ThreadPoolExecutor.shutdown can block; offload it.
        await asyncio.to_thread(self._executor.shutdown, True)

    async def __aenter__(self) -> "OnnxRuntime":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()


__all__ = [
    "OnnxRuntime",
    "OnnxRuntimeConfig",
    "SessionHandle",
    "DEFAULT_AI_LATENCY_BUDGETS_NS",
]
