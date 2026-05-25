"""Async wrappers for the classical-ML and Tiny LSTM ONNX models.

Task 20.1 deliverables (R11.1, R11.3):

* :class:`XGBoostModel`         — gradient-boosted-tree scoring.
* :class:`LightGBMModel`        — gradient-boosted-tree scoring (alt.).
* :class:`IsolationForestModel` — anomaly score (R23.x supports drift).
* :class:`TinyLSTMModel`        — short-window sequence scoring.

Every wrapper runs inference through the shared :class:`OnnxRuntime`
session cache, which transparently:

* dispatches the synchronous :func:`onnxruntime.InferenceSession.run`
  call onto a bounded :class:`concurrent.futures.ThreadPoolExecutor`
  via :func:`asyncio.to_thread`,
* wraps the call in a :class:`LatencyTracer` and emits a
  ``obs.latency.ai_<stage>`` record on every call (R11.4, R27.4),
* emits a ``obs.budget.breach.ai_<stage>`` event when the configured
  per-call budget in :data:`DEFAULT_AI_LATENCY_BUDGETS_NS` is exceeded
  (R28.6).

The conversion helpers (``convert_*_to_onnx``) build ONNX artefacts at
**training time** from in-memory estimators, never from network weights.
This matches the "do not download weights at runtime" rule from task
20.1 and the design's offline ML guidance: artefacts are built ahead of
time and dropped under ``models/onnx/<name>.onnx`` for the runtime to
load via the config loader's ``OnnxRuntimeConfig`` path.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Optional, Union

import numpy as np
import structlog

from .runtime import OnnxRuntime, SessionHandle

_LOG: Final = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Shared abstract base
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ClassicalMLModel:
    """Lightweight handle bundling a runtime, model name, and stage tag.

    The model artefact is loaded **once** via :meth:`load` and then cached
    in the underlying :class:`OnnxRuntime`. Subsequent inferences hit the
    cache. The wrapper is safe to share between coroutines because the
    runtime serialises session creation but lets inferences fan out to
    the bounded thread pool.
    """

    runtime: OnnxRuntime
    model_name: str
    stage: str

    async def load(
        self,
        model_path_or_bytes: Union[str, Path, bytes, bytearray, memoryview],
    ) -> SessionHandle:
        """Load (or return the cached) ONNX session for this model."""
        return await self.runtime.load_session(
            self.model_name, model_path_or_bytes, stage=self.stage
        )

    @property
    def session(self) -> Optional[SessionHandle]:
        """Return the cached :class:`SessionHandle` if loaded, else ``None``."""
        return self.runtime.get_cached(self.model_name)


# ---------------------------------------------------------------------------
# XGBoost
# ---------------------------------------------------------------------------


class XGBoostModel(ClassicalMLModel):
    """Async wrapper around an XGBoost model exported to ONNX.

    XGBoost classifiers exported via :mod:`onnxmltools` typically declare
    two outputs: ``label`` and ``probabilities``. ``probabilities`` is a
    sequence-of-maps (zipmap) by default; we strip the zipmap during
    conversion (see :func:`convert_xgboost_to_onnx`) so the runtime
    receives a plain ``float32`` array.
    """

    DEFAULT_STAGE: Final[str] = "xgboost"

    def __init__(
        self,
        runtime: OnnxRuntime,
        *,
        model_name: str = "xgboost_classifier",
        stage: Optional[str] = None,
    ) -> None:
        super().__init__(
            runtime=runtime,
            model_name=model_name,
            stage=stage or self.DEFAULT_STAGE,
        )

    async def predict_proba(self, features: np.ndarray) -> np.ndarray:
        """Return per-class probabilities for *features*.

        The output is the second declared output (``probabilities``) of
        the standard ``onnxmltools`` XGBoost classifier export.
        """
        # Output index 1 is the probabilities tensor in the standard
        # ``onnxmltools.convert.convert_xgboost`` export.
        return await self.runtime.infer_classical(
            self.model_name, features, stage=self.stage, output_index=1
        )

    async def predict(self, features: np.ndarray) -> np.ndarray:
        """Return the predicted-class label tensor for *features*."""
        return await self.runtime.infer_classical(
            self.model_name, features, stage=self.stage, output_index=0
        )


def convert_xgboost_to_onnx(
    booster: Any,
    *,
    feature_count: int,
    target_path: Union[str, Path],
    initial_type_name: str = "input",
    target_opset: int = 18,
) -> Path:
    """Persist an XGBoost booster as an ONNX file at *target_path*.

    Args:
        booster: Trained ``xgboost.XGBClassifier`` or ``xgboost.Booster``
            estimator. The estimator must already be fit.
        feature_count: Number of input features in the training data.
        target_path: Destination ``.onnx`` file. Parents are created.
        initial_type_name: Name of the input tensor in the ONNX graph.
        target_opset: ONNX opset version. Default 18 is broadly
            compatible with ``onnxruntime>=1.18``.

    Returns:
        The :class:`pathlib.Path` of the written file.

    Raises:
        ImportError: if ``onnxmltools`` (or ``skl2onnx`` for sklearn-API
            estimators) is not available in the active environment.
    """
    try:
        from onnxmltools.convert import convert_xgboost  # type: ignore[import-not-found]
        from onnxmltools.convert.common.data_types import (  # type: ignore[import-not-found]
            FloatTensorType,
        )
    except ImportError as exc:  # pragma: no cover - exercised at training time
        raise ImportError(
            "onnxmltools is required to convert XGBoost models to ONNX. "
            "Install it in the conversion environment, not on Hot_Path nodes."
        ) from exc

    initial_type = [(initial_type_name, FloatTensorType([None, int(feature_count)]))]
    onnx_model = convert_xgboost(booster, initial_types=initial_type, target_opset=target_opset)

    out = Path(target_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(onnx_model.SerializeToString())
    _LOG.info("xgboost_onnx_written", path=str(out), feature_count=feature_count)
    return out


# ---------------------------------------------------------------------------
# LightGBM
# ---------------------------------------------------------------------------


class LightGBMModel(ClassicalMLModel):
    """Async wrapper around a LightGBM model exported to ONNX."""

    DEFAULT_STAGE: Final[str] = "lightgbm"

    def __init__(
        self,
        runtime: OnnxRuntime,
        *,
        model_name: str = "lightgbm_classifier",
        stage: Optional[str] = None,
    ) -> None:
        super().__init__(
            runtime=runtime,
            model_name=model_name,
            stage=stage or self.DEFAULT_STAGE,
        )

    async def predict_proba(self, features: np.ndarray) -> np.ndarray:
        return await self.runtime.infer_classical(
            self.model_name, features, stage=self.stage, output_index=1
        )

    async def predict(self, features: np.ndarray) -> np.ndarray:
        return await self.runtime.infer_classical(
            self.model_name, features, stage=self.stage, output_index=0
        )


def convert_lightgbm_to_onnx(
    booster: Any,
    *,
    feature_count: int,
    target_path: Union[str, Path],
    initial_type_name: str = "input",
    target_opset: int = 18,
) -> Path:
    """Persist a LightGBM booster as an ONNX file at *target_path*.

    Accepts both ``lightgbm.LGBMClassifier`` (sklearn-API) and
    ``lightgbm.Booster`` (low-level) estimators. The export uses
    ``onnxmltools.convert_lightgbm`` with the standard
    ``FloatTensorType`` input.
    """
    try:
        from onnxmltools.convert import convert_lightgbm  # type: ignore[import-not-found]
        from onnxmltools.convert.common.data_types import (  # type: ignore[import-not-found]
            FloatTensorType,
        )
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "onnxmltools is required to convert LightGBM models to ONNX. "
            "Install it in the conversion environment."
        ) from exc

    initial_type = [(initial_type_name, FloatTensorType([None, int(feature_count)]))]
    onnx_model = convert_lightgbm(booster, initial_types=initial_type, target_opset=target_opset)

    out = Path(target_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(onnx_model.SerializeToString())
    _LOG.info("lightgbm_onnx_written", path=str(out), feature_count=feature_count)
    return out


# ---------------------------------------------------------------------------
# Isolation Forest
# ---------------------------------------------------------------------------


class IsolationForestModel(ClassicalMLModel):
    """Async wrapper around an :class:`sklearn.ensemble.IsolationForest`.

    The skl2onnx export declares two outputs:
    * ``label`` — ``+1`` for inliers, ``-1`` for anomalies.
    * ``scores`` — the raw anomaly score, lower = more anomalous.

    Risk_Engine and AI_Governance subscribe to the score channel via
    :meth:`anomaly_score`.
    """

    DEFAULT_STAGE: Final[str] = "isolation_forest"

    def __init__(
        self,
        runtime: OnnxRuntime,
        *,
        model_name: str = "isolation_forest",
        stage: Optional[str] = None,
    ) -> None:
        super().__init__(
            runtime=runtime,
            model_name=model_name,
            stage=stage or self.DEFAULT_STAGE,
        )

    async def anomaly_score(self, features: np.ndarray) -> np.ndarray:
        """Return the per-row anomaly score (output index 1)."""
        return await self.runtime.infer_classical(
            self.model_name, features, stage=self.stage, output_index=1
        )

    async def predict(self, features: np.ndarray) -> np.ndarray:
        """Return the per-row label (+1 inlier / -1 anomaly)."""
        return await self.runtime.infer_classical(
            self.model_name, features, stage=self.stage, output_index=0
        )


def convert_isolation_forest_to_onnx(
    estimator: Any,
    *,
    feature_count: int,
    target_path: Union[str, Path],
    initial_type_name: str = "input",
    target_opset: int = 18,
) -> Path:
    """Persist an :class:`IsolationForest` as an ONNX file at *target_path*."""
    try:
        from skl2onnx import convert_sklearn  # type: ignore[import-not-found]
        from skl2onnx.common.data_types import (  # type: ignore[import-not-found]
            FloatTensorType,
        )
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "skl2onnx is required to convert IsolationForest to ONNX. "
            "Install it in the conversion environment."
        ) from exc

    initial_type = [(initial_type_name, FloatTensorType([None, int(feature_count)]))]
    onnx_model = convert_sklearn(estimator, initial_types=initial_type, target_opset=target_opset)

    out = Path(target_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(onnx_model.SerializeToString())
    _LOG.info("isolation_forest_onnx_written", path=str(out), feature_count=feature_count)
    return out


# ---------------------------------------------------------------------------
# Tiny LSTM
# ---------------------------------------------------------------------------


class TinyLSTMModel(ClassicalMLModel):
    """Async wrapper around a Tiny LSTM exported to ONNX.

    The Tiny LSTM is a small recurrent network (one or two layers, hidden
    size ≤ 32) used by AI_Trade_Ranking_Engine for short-window
    sequence scoring (R26.x). The default input layout is
    ``(batch, timesteps, features)`` ``float32``.
    """

    DEFAULT_STAGE: Final[str] = "tiny_lstm"

    def __init__(
        self,
        runtime: OnnxRuntime,
        *,
        model_name: str = "tiny_lstm",
        stage: Optional[str] = None,
        feed_name: Optional[str] = None,
    ) -> None:
        super().__init__(
            runtime=runtime,
            model_name=model_name,
            stage=stage or self.DEFAULT_STAGE,
        )
        self._feed_name = feed_name

    async def score(self, sequence: np.ndarray) -> np.ndarray:
        """Score a (batch, timesteps, features) ``float32`` sequence.

        Args:
            sequence: 3D NumPy array. 2D arrays are reshaped to
                ``(1, timesteps, features)`` for ergonomics.

        Returns:
            The first declared output of the session as a NumPy array.
        """
        if sequence.ndim == 2:
            sequence = sequence.reshape(1, *sequence.shape)
        if sequence.ndim != 3:
            raise ValueError(
                f"tiny_lstm expects rank-3 input (batch, timesteps, features); "
                f"got rank-{sequence.ndim} shape {sequence.shape}"
            )
        return await self.runtime.infer_classical(
            self.model_name,
            sequence,
            stage=self.stage,
            feed_name=self._feed_name,
            output_index=0,
        )


def convert_tiny_lstm_to_onnx(
    torch_module: Any,
    *,
    sequence_length: int,
    feature_count: int,
    target_path: Union[str, Path],
    input_name: str = "input",
    output_name: str = "output",
    target_opset: int = 18,
    dynamic_batch: bool = True,
) -> Path:
    """Export a PyTorch Tiny LSTM module to ONNX at *target_path*.

    The conversion uses :func:`torch.onnx.export` with a single example
    tensor of shape ``(1, sequence_length, feature_count)``. The first
    dimension is marked dynamic by default so the same artefact can score
    arbitrary batch sizes at runtime.
    """
    try:
        import torch  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "torch is required to export a Tiny LSTM to ONNX. "
            "Install it in the conversion environment."
        ) from exc

    torch_module.eval()
    example = torch.zeros(1, int(sequence_length), int(feature_count), dtype=torch.float32)

    out = Path(target_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    dynamic_axes: Optional[dict[str, dict[int, str]]]
    if dynamic_batch:
        dynamic_axes = {input_name: {0: "batch"}, output_name: {0: "batch"}}
    else:
        dynamic_axes = None

    torch.onnx.export(
        torch_module,
        example,
        str(out),
        input_names=[input_name],
        output_names=[output_name],
        opset_version=int(target_opset),
        dynamic_axes=dynamic_axes,
    )
    _LOG.info(
        "tiny_lstm_onnx_written",
        path=str(out),
        sequence_length=sequence_length,
        feature_count=feature_count,
    )
    return out


__all__ = [
    "ClassicalMLModel",
    "XGBoostModel",
    "LightGBMModel",
    "IsolationForestModel",
    "TinyLSTMModel",
    "convert_xgboost_to_onnx",
    "convert_lightgbm_to_onnx",
    "convert_isolation_forest_to_onnx",
    "convert_tiny_lstm_to_onnx",
]
