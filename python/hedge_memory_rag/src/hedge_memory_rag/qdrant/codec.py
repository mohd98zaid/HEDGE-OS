"""CBOR codec for embedding payloads (R19.1, design § Data Models).

Per the design (``Data Models`` — *Warm_AI_Pipeline payloads are JSON
for ergonomics, except embeddings which are CBOR*), every embedding
that lands inside a Qdrant payload is wrapped in a deterministic CBOR
byte-string under the well-known key
:data:`EMBEDDING_PAYLOAD_KEY` (``"embedding_cbor"``).

Why CBOR rather than JSON for the vector copy?

* JSON serialises 32-bit floats as decimal strings, so a 768-dim
  vector triples in size and loses bit-exact round-trip semantics.
* CBOR's typed array tags (RFC 8746) preserve the raw IEEE-754 bytes
  with at most a 4-byte header, satisfying *Property 5: Serialization
  and Persistence Round-Trip* (design § Correctness Properties).

Why store the embedding twice — once as the indexed Qdrant vector and
once in the payload as CBOR?

* Qdrant returns the indexed vector only when explicitly asked
  (``with_vectors=True``) and lossy quantisation may be applied to it
  for memory efficiency. The CBOR copy is the **canonical** value
  used for replay and round-trip property tests.
* The payload-side CBOR also supports *companion* vectors (e.g. a
  pre-quantisation full-precision copy alongside an int8 index) when
  later tasks need it.
"""

from __future__ import annotations

from typing import Final, Iterable, Sequence

import cbor2
import numpy as np

from .errors import QdrantClientError

#: Canonical payload key under which the CBOR-encoded embedding lives.
EMBEDDING_PAYLOAD_KEY: Final[str] = "embedding_cbor"

# Internal CBOR tag namespace. The map carries:
#   0 -> "f4" / "f8" — IEEE-754 float width (32 or 64 bit).
#   1 -> int dim count.
#   2 -> raw little-endian bytes.
# A small typed map (rather than a bare typed-array tag) keeps the
# format self-describing without forcing every reader to know the
# RFC-8746 tag table by heart.
_DTYPE_F32: Final[str] = "f4"
_DTYPE_F64: Final[str] = "f8"
_DTYPE_KEY: Final[int] = 0
_DIM_KEY: Final[int] = 1
_DATA_KEY: Final[int] = 2


# ---------------------------------------------------------------------------
# Errors --------------------------------------------------------------------
# ---------------------------------------------------------------------------


class EmbeddingEncodeError(QdrantClientError):
    """Raised when an embedding cannot be encoded to CBOR.

    Causes: empty vector, NaN/Inf with ``allow_non_finite=False``,
    unsupported numpy dtype.
    """


class EmbeddingDecodeError(QdrantClientError):
    """Raised when a CBOR payload cannot be decoded back to a numpy vector.

    Causes: malformed CBOR bytes, missing required keys, unknown dtype
    sentinel, length mismatch between declared dim and raw byte count.
    """


# ---------------------------------------------------------------------------
# Public API ----------------------------------------------------------------
# ---------------------------------------------------------------------------


def encode_embedding_cbor(
    embedding: Sequence[float] | np.ndarray,
    *,
    dtype: str = _DTYPE_F32,
    allow_non_finite: bool = False,
) -> bytes:
    """Encode a 1-D embedding vector to CBOR bytes.

    Args:
        embedding: The vector to encode. Accepts any iterable of floats,
            including a :class:`numpy.ndarray`. Higher-dimensional arrays
            are rejected — use :func:`encode_batch_cbor` for stacks.
        dtype: ``"f4"`` for IEEE-754 single precision (4 bytes per scalar)
            or ``"f8"`` for double precision (8 bytes per scalar).
            Single precision is the default because the Warm_AI_Pipeline
            embedders (DistilBERT, FinBERT) emit ``float32``.
        allow_non_finite: When ``False`` (the default), ``NaN``/``Inf``
            values raise :class:`EmbeddingEncodeError`. The Memory_RAG_Layer
            should reject these on ingest because cosine similarity
            against a NaN vector is meaningless and silently corrupts kNN.

    Returns:
        Compact CBOR-encoded bytes ready to drop into a Qdrant payload
        under :data:`EMBEDDING_PAYLOAD_KEY`.

    Raises:
        EmbeddingEncodeError: on empty input, unsupported dtype,
            multi-dim array, or non-finite value when not allowed.
    """
    if dtype not in (_DTYPE_F32, _DTYPE_F64):
        raise EmbeddingEncodeError(
            f"unsupported dtype {dtype!r} (expected 'f4' or 'f8')"
        )

    np_dtype = np.float32 if dtype == _DTYPE_F32 else np.float64
    arr = np.asarray(embedding, dtype=np_dtype)
    if arr.ndim != 1:
        raise EmbeddingEncodeError(
            f"embedding must be 1-D, got shape {arr.shape!r}"
        )
    if arr.size == 0:
        raise EmbeddingEncodeError("embedding must not be empty")
    if not allow_non_finite and not np.all(np.isfinite(arr)):
        raise EmbeddingEncodeError("embedding contains NaN or Inf and allow_non_finite=False")

    # Use little-endian byte order explicitly so the wire representation
    # is reproducible across architectures (x86_64 native + ARM64
    # development laptops). In numpy 2.x the ``ndarray.newbyteorder``
    # shortcut was removed; the canonical replacement is to construct
    # the target byte-ordered dtype and ``astype`` into it.
    le_dtype = np.dtype(np_dtype).newbyteorder("<")
    arr_le = arr.astype(le_dtype, copy=False)
    payload = {
        _DTYPE_KEY: dtype,
        _DIM_KEY: int(arr.size),
        _DATA_KEY: arr_le.tobytes(),
    }
    # canonical=True forces deterministic key ordering, matching the
    # design's "structurally equal value" round-trip guarantee.
    return cbor2.dumps(payload, canonical=True)


def decode_embedding_cbor(blob: bytes) -> np.ndarray:
    """Decode a CBOR-encoded embedding produced by :func:`encode_embedding_cbor`.

    Returns:
        A 1-D :class:`numpy.ndarray` whose dtype matches the original
        encoded width (``float32`` or ``float64``). The array is a
        fresh copy and is safe to mutate.

    Raises:
        EmbeddingDecodeError: on malformed input, missing keys,
            unknown dtype sentinel, or dim/byte-length mismatch.
    """
    if not isinstance(blob, (bytes, bytearray, memoryview)):
        raise EmbeddingDecodeError(
            f"expected bytes-like input, got {type(blob).__name__}"
        )

    try:
        payload = cbor2.loads(bytes(blob))
    except Exception as exc:  # cbor2 raises CBORDecodeError + others
        raise EmbeddingDecodeError(f"malformed CBOR: {exc}") from exc

    if not isinstance(payload, dict):
        raise EmbeddingDecodeError(
            f"expected CBOR map at top level, got {type(payload).__name__}"
        )
    try:
        dtype_str = payload[_DTYPE_KEY]
        dim = payload[_DIM_KEY]
        data = payload[_DATA_KEY]
    except KeyError as exc:
        raise EmbeddingDecodeError(f"missing required key {exc.args[0]!r}") from exc

    if dtype_str == _DTYPE_F32:
        np_dtype = np.float32
        elem_size = 4
    elif dtype_str == _DTYPE_F64:
        np_dtype = np.float64
        elem_size = 8
    else:
        raise EmbeddingDecodeError(f"unknown dtype sentinel {dtype_str!r}")

    if not isinstance(dim, int) or dim <= 0:
        raise EmbeddingDecodeError(f"dim must be a positive int, got {dim!r}")
    if not isinstance(data, (bytes, bytearray)):
        raise EmbeddingDecodeError(
            f"data field must be CBOR byte-string, got {type(data).__name__}"
        )
    if len(data) != dim * elem_size:
        raise EmbeddingDecodeError(
            f"data length {len(data)} does not match dim={dim} * elem_size={elem_size}"
        )

    return np.frombuffer(bytes(data), dtype=np.dtype(np_dtype).newbyteorder("<")).astype(
        np_dtype, copy=True
    )


# ---------------------------------------------------------------------------
# Helpers -------------------------------------------------------------------
# ---------------------------------------------------------------------------


def vector_to_floats(vector: Sequence[float] | np.ndarray) -> list[float]:
    """Coerce ``vector`` to a Python ``list[float]`` for the Qdrant client.

    The qdrant-client async API accepts ``list[float]`` directly and
    handles numpy arrays via duck typing, but normalising up-front keeps
    the wire format predictable and easier to mock in unit tests.
    """
    if isinstance(vector, np.ndarray):
        if vector.ndim != 1:
            raise EmbeddingEncodeError(
                f"vector must be 1-D, got shape {vector.shape!r}"
            )
        return vector.astype(np.float64, copy=False).tolist()
    out = [float(x) for x in vector]
    if not out:
        raise EmbeddingEncodeError("vector must not be empty")
    return out


def attach_embedding_cbor(
    payload: dict[str, object] | None,
    embedding: Sequence[float] | np.ndarray,
    *,
    dtype: str = _DTYPE_F32,
    allow_non_finite: bool = False,
) -> dict[str, object]:
    """Return a copy of ``payload`` with the CBOR embedding embedded under :data:`EMBEDDING_PAYLOAD_KEY`.

    A new dict is returned (the input is never mutated) so callers can
    keep the original metadata payload pristine.
    """
    blob = encode_embedding_cbor(embedding, dtype=dtype, allow_non_finite=allow_non_finite)
    new_payload: dict[str, object] = dict(payload or {})
    new_payload[EMBEDDING_PAYLOAD_KEY] = blob
    return new_payload


def extract_embedding_cbor(payload: dict[str, object] | None) -> np.ndarray | None:
    """Decode the CBOR embedding from a Qdrant payload, if present.

    Returns ``None`` when the payload is missing or does not carry an
    :data:`EMBEDDING_PAYLOAD_KEY` entry. The Memory_RAG_Layer relies on
    this asymmetry so callers can request payload-only retrieval and
    still recover the canonical embedding when needed.
    """
    if not payload:
        return None
    blob = payload.get(EMBEDDING_PAYLOAD_KEY)
    if blob is None:
        return None
    if not isinstance(blob, (bytes, bytearray, memoryview)):
        raise EmbeddingDecodeError(
            f"{EMBEDDING_PAYLOAD_KEY!r} must be bytes, got {type(blob).__name__}"
        )
    return decode_embedding_cbor(bytes(blob))


def iter_embedding_cbor(blobs: Iterable[bytes]) -> Iterable[np.ndarray]:
    """Decode an iterable of CBOR blobs lazily.

    Useful when iterating over a kNN result set without materialising
    every embedding at once.
    """
    for blob in blobs:
        yield decode_embedding_cbor(blob)


__all__ = [
    "EMBEDDING_PAYLOAD_KEY",
    "EmbeddingDecodeError",
    "EmbeddingEncodeError",
    "attach_embedding_cbor",
    "decode_embedding_cbor",
    "encode_embedding_cbor",
    "extract_embedding_cbor",
    "iter_embedding_cbor",
    "vector_to_floats",
]
