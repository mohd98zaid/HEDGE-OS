"""ONNX Runtime wrappers for classical ML and fast NLP scoring.

This sub-package implements **task 20.1** of the PROJECT HEDGE spec:

* Wrap ONNX Runtime for XGBoost, LightGBM, Isolation Forest, and Tiny LSTM
  classical models (R11.1).
* Wrap ONNX Runtime for FinBERT and DistilBERT fast NLP models (R11.2).
* Provide async inference functions used by the News_Intelligence_Engine
  fast path (R11.3, R12.2).
* Mirror the Rust ``hedge-obs::LatencyTracer`` API in Python so that every
  AI stage call publishes an ``obs.latency.ai_<stage>`` record (R11.4,
  R27.4) and an ``obs.budget.breach.ai_<stage>`` event when the configured
  per-call budget is exceeded (R28.6).

The module also exposes a :class:`ModelArtefactLayout` resolver that
mirrors the convention documented in
:mod:`hedge_warm_ai.onnx_runtime.cli`: artefacts live under
``$HEDGE_HOME/models/onnx/<name>`` (or
``/var/lib/hedge/models/onnx/<name>`` on the production deploy) and are
**never** fetched at runtime.

Heavy ML dependencies (``onnxruntime``, ``transformers``, ``xgboost``,
``lightgbm``, ``optimum``, ``torch``) are imported **lazily** at the
boundary of each wrapper so this module can be imported in environments
that only need the latency-tracing helpers.
"""

from __future__ import annotations

from .classical import (
    ClassicalMLModel,
    IsolationForestModel,
    LightGBMModel,
    TinyLSTMModel,
    XGBoostModel,
    convert_isolation_forest_to_onnx,
    convert_lightgbm_to_onnx,
    convert_tiny_lstm_to_onnx,
    convert_xgboost_to_onnx,
)
from .latency import (
    AiLatencyEmitter,
    AiLatencyRecord,
    InMemoryAiLatencyEmitter,
    LatencyTracer,
    NatsAiLatencyEmitter,
    NoopAiLatencyEmitter,
    correlation_id_from_bytes,
    current_correlation_id,
    new_correlation_id,
    set_correlation_id,
)
from .nlp import (
    DEFAULT_FINBERT_LABELS,
    DistilBERTEmbedding,
    FinBERTSentiment,
    NLPModel,
    SentimentResult,
    convert_distilbert_to_onnx,
    convert_finbert_to_onnx,
)
from .registry import ModelArtefactLayout, resolve_layout
from .runtime import (
    DEFAULT_AI_LATENCY_BUDGETS_NS,
    OnnxRuntime,
    OnnxRuntimeConfig,
    SessionHandle,
)

__all__ = [
    # runtime
    "OnnxRuntime",
    "OnnxRuntimeConfig",
    "SessionHandle",
    "DEFAULT_AI_LATENCY_BUDGETS_NS",
    # classical
    "ClassicalMLModel",
    "XGBoostModel",
    "LightGBMModel",
    "IsolationForestModel",
    "TinyLSTMModel",
    "convert_xgboost_to_onnx",
    "convert_lightgbm_to_onnx",
    "convert_isolation_forest_to_onnx",
    "convert_tiny_lstm_to_onnx",
    # nlp
    "NLPModel",
    "FinBERTSentiment",
    "DistilBERTEmbedding",
    "SentimentResult",
    "DEFAULT_FINBERT_LABELS",
    "convert_finbert_to_onnx",
    "convert_distilbert_to_onnx",
    # latency
    "LatencyTracer",
    "AiLatencyEmitter",
    "AiLatencyRecord",
    "InMemoryAiLatencyEmitter",
    "NoopAiLatencyEmitter",
    "NatsAiLatencyEmitter",
    "correlation_id_from_bytes",
    "current_correlation_id",
    "new_correlation_id",
    "set_correlation_id",
    # registry
    "ModelArtefactLayout",
    "resolve_layout",
]
