"""Async wrappers for the FinBERT and DistilBERT fast-NLP ONNX models.

Task 20.1 deliverables (R11.2, R11.3, R11.4):

* :class:`FinBERTSentiment`     — short-headline financial-sentiment scoring
  used by the News_Intelligence_Engine fast path (R12.2).
* :class:`DistilBERTEmbedding`  — drop-in fast embedder for headline / topic
  vectorisation that feeds the AI_Trade_Ranking and Memory_RAG layers.
* Conversion helpers (``convert_finbert_to_onnx``, ``convert_distilbert_to_onnx``)
  build ONNX artefacts ahead of time from local checkpoints. **Weights are
  never downloaded from the internet at runtime.** The conversion script
  in :mod:`hedge_warm_ai.onnx_runtime.cli` is the canonical entry point.

The wrappers tokenise on the caller's thread (tokenisation is < 100 µs for
short headlines) and dispatch the ONNX forward pass through the shared
:class:`OnnxRuntime` thread pool with a :class:`LatencyTracer` envelope.
The tracer publishes a ``obs.latency.ai_finbert`` (resp. ``ai_distilbert``)
record on every call (R27.4) and a ``obs.budget.breach.ai_*`` event when
the configured 10 ms ceiling is exceeded (R28.6, R11.4).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Optional, Sequence, Union

import numpy as np
import structlog

from .runtime import OnnxRuntime, SessionHandle

_LOG: Final = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Sentiment result
# ---------------------------------------------------------------------------


# Default ordering used by the ProsusAI/finbert checkpoint:
# label 0 = positive, 1 = negative, 2 = neutral.
DEFAULT_FINBERT_LABELS: Final[tuple[str, str, str]] = ("positive", "negative", "neutral")


@dataclass(frozen=True, slots=True)
class SentimentResult:
    """Outcome of a single FinBERT scoring call.

    Attributes:
        label:     Highest-probability class string from
                   :attr:`FinBERTSentiment.labels`.
        score:     Class probability ∈ [0, 1] for *label*.
        sentiment: Bipolar sentiment ∈ [-1, 1] (positive − negative).
                   Matches ``ai.news.impact.sentiment`` per design.
        probabilities: Per-label probability map. Stable ordering
                   matches :attr:`FinBERTSentiment.labels`.
    """

    label: str
    score: float
    sentiment: float
    probabilities: dict[str, float]


# ---------------------------------------------------------------------------
# Shared base
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class NLPModel:
    """Lightweight base bundling a runtime, model name, and stage tag."""

    runtime: OnnxRuntime
    model_name: str
    stage: str

    async def load(
        self,
        model_path_or_bytes: Union[str, Path, bytes, bytearray, memoryview],
    ) -> SessionHandle:
        return await self.runtime.load_session(
            self.model_name, model_path_or_bytes, stage=self.stage
        )

    @property
    def session(self) -> Optional[SessionHandle]:
        return self.runtime.get_cached(self.model_name)


# ---------------------------------------------------------------------------
# Tokeniser loading
# ---------------------------------------------------------------------------


def _load_tokenizer(tokenizer_dir: Union[str, Path], *, max_length: int) -> Any:
    """Load a fast :class:`AutoTokenizer` from a **local** directory.

    Loading fast tokenisers from a directory avoids any network access:
    the directory is expected to contain ``tokenizer.json`` plus the
    standard transformer config files. The conversion script in
    :mod:`hedge_warm_ai.onnx_runtime.cli` writes both the ONNX file and
    the tokenizer directory side-by-side.
    """
    try:
        from transformers import AutoTokenizer  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - exercised at runtime
        raise ImportError(
            "transformers is required to tokenise inputs for FinBERT/DistilBERT. "
            "Install it in the active Python environment."
        ) from exc

    tokenizer = AutoTokenizer.from_pretrained(
        str(tokenizer_dir),
        local_files_only=True,
    )
    tokenizer.model_max_length = int(max_length)
    return tokenizer


def _encode_batch(
    tokenizer: Any,
    texts: Sequence[str],
    *,
    max_length: int,
    include_token_type_ids: bool,
) -> dict[str, np.ndarray]:
    """Encode a list of strings into the int64 ndarray feed expected by ONNX."""
    enc = tokenizer(
        list(texts),
        padding=True,
        truncation=True,
        max_length=int(max_length),
        return_tensors="np",
        return_token_type_ids=include_token_type_ids,
    )
    feed: dict[str, np.ndarray] = {
        "input_ids": np.ascontiguousarray(enc["input_ids"].astype(np.int64)),
        "attention_mask": np.ascontiguousarray(enc["attention_mask"].astype(np.int64)),
    }
    if include_token_type_ids and "token_type_ids" in enc:
        feed["token_type_ids"] = np.ascontiguousarray(enc["token_type_ids"].astype(np.int64))
    return feed


# ---------------------------------------------------------------------------
# FinBERT
# ---------------------------------------------------------------------------


def _softmax(logits: np.ndarray) -> np.ndarray:
    """Numerically stable row-wise softmax."""
    shifted = logits - logits.max(axis=-1, keepdims=True)
    exp = np.exp(shifted, dtype=np.float64)
    return (exp / exp.sum(axis=-1, keepdims=True)).astype(np.float32)


class FinBERTSentiment(NLPModel):
    """Async FinBERT sentiment scorer used by the News fast path (R12.2)."""

    DEFAULT_STAGE: Final[str] = "finbert"

    def __init__(
        self,
        runtime: OnnxRuntime,
        *,
        tokenizer_dir: Union[str, Path],
        model_name: str = "finbert",
        stage: Optional[str] = None,
        max_length: int = 128,
        labels: Sequence[str] = DEFAULT_FINBERT_LABELS,
    ) -> None:
        super().__init__(
            runtime=runtime,
            model_name=model_name,
            stage=stage or self.DEFAULT_STAGE,
        )
        if len(labels) != 3:
            raise ValueError(
                f"FinBERT expects exactly 3 labels (positive/negative/neutral); "
                f"got {len(labels)}: {labels!r}"
            )
        self._tokenizer_dir = Path(tokenizer_dir)
        self._max_length = int(max_length)
        self._labels: tuple[str, ...] = tuple(labels)
        self._tokenizer: Any = None

    @property
    def labels(self) -> tuple[str, ...]:
        return self._labels

    def _ensure_tokenizer(self) -> Any:
        if self._tokenizer is None:
            self._tokenizer = _load_tokenizer(
                self._tokenizer_dir, max_length=self._max_length
            )
        return self._tokenizer

    async def score(self, text: str) -> SentimentResult:
        """Score a single headline. Wrapped in a :class:`LatencyTracer`."""
        results = await self.score_batch([text])
        return results[0]

    async def score_batch(self, texts: Sequence[str]) -> list[SentimentResult]:
        """Score a batch of headlines. Tokenises on the caller's thread."""
        if not texts:
            return []
        tokenizer = self._ensure_tokenizer()
        feed = _encode_batch(
            tokenizer,
            texts,
            max_length=self._max_length,
            include_token_type_ids=True,
        )
        # FinBERT exports do not always emit ``token_type_ids``. Drop the
        # feed entry when the session does not declare the input.
        feed = self._intersect_feed(feed)

        outputs = await self.runtime.infer_nlp(self.model_name, feed, stage=self.stage)
        # Logits is the sole declared output of HuggingFace classification heads.
        logits = next(iter(outputs.values()))
        probs = _softmax(np.asarray(logits, dtype=np.float32))
        return [self._row_to_result(probs[i]) for i in range(probs.shape[0])]

    # -- helpers ------------------------------------------------------------

    def _intersect_feed(self, feed: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
        handle = self.runtime.get_cached(self.model_name)
        if handle is None:
            # Tokenisation may run before the first ``load_session`` call;
            # skip the intersection in that case and let the runtime raise.
            return feed
        declared = set(handle.input_names)
        return {k: v for k, v in feed.items() if k in declared}

    def _row_to_result(self, row: np.ndarray) -> SentimentResult:
        if row.shape != (3,):
            raise ValueError(f"FinBERT logit row must have shape (3,); got {row.shape}")
        probabilities = {label: float(row[i]) for i, label in enumerate(self._labels)}
        # Find label_index by max probability without depending on dict ordering.
        label_index = int(np.argmax(row))
        label = self._labels[label_index]
        score = float(row[label_index])
        positive = probabilities.get("positive", 0.0)
        negative = probabilities.get("negative", 0.0)
        sentiment = float(np.clip(positive - negative, -1.0, 1.0))
        return SentimentResult(
            label=label,
            score=score,
            sentiment=sentiment,
            probabilities=probabilities,
        )


def convert_finbert_to_onnx(
    *,
    checkpoint_dir: Union[str, Path],
    target_dir: Union[str, Path],
    target_opset: int = 18,
    max_length: int = 128,
) -> Path:
    """Convert a local FinBERT checkpoint to ONNX.

    Args:
        checkpoint_dir: Local directory containing ``config.json``,
            ``pytorch_model.bin`` (or ``model.safetensors``), and the
            tokenizer files. The directory must exist locally — the
            converter does not pull weights from the internet.
        target_dir: Output directory. The function writes:

            * ``model.onnx``     — the converted ONNX graph,
            * ``tokenizer/``     — the slim tokenizer copy used at runtime,
            * ``conversion.json`` — provenance metadata.
        target_opset: ONNX opset version. Default 18.
        max_length: Sequence length used for the dummy export trace.

    Returns:
        Path of the written ``model.onnx`` file.
    """
    try:
        import torch  # type: ignore[import-not-found]
        from transformers import (  # type: ignore[import-not-found]
            AutoConfig,
            AutoModelForSequenceClassification,
            AutoTokenizer,
        )
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "transformers and torch are required for the FinBERT conversion. "
            "Install them in the conversion environment."
        ) from exc

    src = Path(checkpoint_dir).expanduser().resolve()
    if not src.exists():
        raise FileNotFoundError(f"FinBERT checkpoint directory not found: {src}")

    out = Path(target_dir).expanduser()
    out.mkdir(parents=True, exist_ok=True)
    onnx_path = out / "model.onnx"
    tokenizer_dir = out / "tokenizer"

    config = AutoConfig.from_pretrained(str(src), local_files_only=True)
    tokenizer = AutoTokenizer.from_pretrained(str(src), local_files_only=True)
    model = AutoModelForSequenceClassification.from_pretrained(
        str(src), config=config, local_files_only=True
    )
    model.eval()

    tokenizer.save_pretrained(str(tokenizer_dir))

    dummy = tokenizer(
        ["onnx export trace"],
        padding="max_length",
        truncation=True,
        max_length=int(max_length),
        return_tensors="pt",
    )
    inputs: list[str] = ["input_ids", "attention_mask"]
    args: tuple[Any, ...] = (dummy["input_ids"], dummy["attention_mask"])
    if "token_type_ids" in dummy:
        inputs.append("token_type_ids")
        args = args + (dummy["token_type_ids"],)

    dynamic_axes = {name: {0: "batch", 1: "sequence"} for name in inputs}
    dynamic_axes["logits"] = {0: "batch"}

    torch.onnx.export(
        model,
        args,
        str(onnx_path),
        input_names=inputs,
        output_names=["logits"],
        opset_version=int(target_opset),
        dynamic_axes=dynamic_axes,
        do_constant_folding=True,
    )

    _write_conversion_metadata(
        out / "conversion.json",
        kind="finbert",
        source=str(src),
        opset=int(target_opset),
        max_length=int(max_length),
    )
    _LOG.info("finbert_onnx_written", path=str(onnx_path))
    return onnx_path


# ---------------------------------------------------------------------------
# DistilBERT (embedding)
# ---------------------------------------------------------------------------


class DistilBERTEmbedding(NLPModel):
    """Async DistilBERT embedder.

    Returns the mean-pooled hidden state per row, attention-mask aware,
    suitable for vector-store ingestion (Qdrant) and downstream topic /
    similarity scoring inside the Warm_AI_Pipeline.
    """

    DEFAULT_STAGE: Final[str] = "distilbert"

    def __init__(
        self,
        runtime: OnnxRuntime,
        *,
        tokenizer_dir: Union[str, Path],
        model_name: str = "distilbert",
        stage: Optional[str] = None,
        max_length: int = 256,
    ) -> None:
        super().__init__(
            runtime=runtime,
            model_name=model_name,
            stage=stage or self.DEFAULT_STAGE,
        )
        self._tokenizer_dir = Path(tokenizer_dir)
        self._max_length = int(max_length)
        self._tokenizer: Any = None

    def _ensure_tokenizer(self) -> Any:
        if self._tokenizer is None:
            self._tokenizer = _load_tokenizer(
                self._tokenizer_dir, max_length=self._max_length
            )
        return self._tokenizer

    async def embed(self, text: str) -> np.ndarray:
        """Embed a single string and return a 1D ``float32`` vector."""
        batch = await self.embed_batch([text])
        return batch[0]

    async def embed_batch(self, texts: Sequence[str]) -> np.ndarray:
        """Embed a batch and return a 2D ``(n, hidden_size)`` ``float32`` array."""
        if not texts:
            return np.empty((0, 0), dtype=np.float32)
        tokenizer = self._ensure_tokenizer()
        feed = _encode_batch(
            tokenizer,
            texts,
            max_length=self._max_length,
            include_token_type_ids=False,
        )
        feed = self._intersect_feed(feed)

        outputs = await self.runtime.infer_nlp(self.model_name, feed, stage=self.stage)

        # The conventional DistilBERT export emits ``last_hidden_state`` of
        # shape (batch, seq_len, hidden). Some exports emit ``pooler_output``
        # directly — handle both by name.
        hidden = (
            outputs.get("last_hidden_state")
            or outputs.get("hidden_states")
            or next(iter(outputs.values()))
        )
        hidden = np.asarray(hidden, dtype=np.float32)
        if hidden.ndim == 2:
            return hidden
        if hidden.ndim != 3:
            raise ValueError(
                f"unexpected DistilBERT output rank: {hidden.ndim} (shape={hidden.shape})"
            )
        attention_mask = feed["attention_mask"].astype(np.float32)[..., None]
        masked = hidden * attention_mask
        denom = np.clip(attention_mask.sum(axis=1), 1e-6, None)
        pooled = masked.sum(axis=1) / denom
        return pooled.astype(np.float32, copy=False)

    def _intersect_feed(self, feed: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
        handle = self.runtime.get_cached(self.model_name)
        if handle is None:
            return feed
        declared = set(handle.input_names)
        return {k: v for k, v in feed.items() if k in declared}


def convert_distilbert_to_onnx(
    *,
    checkpoint_dir: Union[str, Path],
    target_dir: Union[str, Path],
    target_opset: int = 18,
    max_length: int = 256,
) -> Path:
    """Convert a local DistilBERT checkpoint to ONNX.

    See :func:`convert_finbert_to_onnx` for the layout written under
    *target_dir*. The exported graph emits ``last_hidden_state`` with a
    dynamic batch and sequence dimension.
    """
    try:
        import torch  # type: ignore[import-not-found]
        from transformers import (  # type: ignore[import-not-found]
            AutoConfig,
            AutoModel,
            AutoTokenizer,
        )
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "transformers and torch are required for the DistilBERT conversion. "
            "Install them in the conversion environment."
        ) from exc

    src = Path(checkpoint_dir).expanduser().resolve()
    if not src.exists():
        raise FileNotFoundError(f"DistilBERT checkpoint directory not found: {src}")

    out = Path(target_dir).expanduser()
    out.mkdir(parents=True, exist_ok=True)
    onnx_path = out / "model.onnx"
    tokenizer_dir = out / "tokenizer"

    config = AutoConfig.from_pretrained(str(src), local_files_only=True)
    tokenizer = AutoTokenizer.from_pretrained(str(src), local_files_only=True)
    model = AutoModel.from_pretrained(str(src), config=config, local_files_only=True)
    model.eval()

    tokenizer.save_pretrained(str(tokenizer_dir))

    dummy = tokenizer(
        ["onnx export trace"],
        padding="max_length",
        truncation=True,
        max_length=int(max_length),
        return_tensors="pt",
    )

    inputs = ["input_ids", "attention_mask"]
    args = (dummy["input_ids"], dummy["attention_mask"])

    dynamic_axes = {name: {0: "batch", 1: "sequence"} for name in inputs}
    dynamic_axes["last_hidden_state"] = {0: "batch", 1: "sequence"}

    torch.onnx.export(
        model,
        args,
        str(onnx_path),
        input_names=inputs,
        output_names=["last_hidden_state"],
        opset_version=int(target_opset),
        dynamic_axes=dynamic_axes,
        do_constant_folding=True,
    )

    _write_conversion_metadata(
        out / "conversion.json",
        kind="distilbert",
        source=str(src),
        opset=int(target_opset),
        max_length=int(max_length),
    )
    _LOG.info("distilbert_onnx_written", path=str(onnx_path))
    return onnx_path


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _write_conversion_metadata(
    path: Path,
    *,
    kind: str,
    source: str,
    opset: int,
    max_length: int,
) -> None:
    """Write a small JSON sidecar describing how this artefact was built."""
    import json
    from datetime import datetime, timezone

    payload = {
        "kind": kind,
        "source": source,
        "opset": int(opset),
        "max_length": int(max_length),
        "converted_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


__all__ = [
    "NLPModel",
    "FinBERTSentiment",
    "DistilBERTEmbedding",
    "SentimentResult",
    "DEFAULT_FINBERT_LABELS",
    "convert_finbert_to_onnx",
    "convert_distilbert_to_onnx",
]
