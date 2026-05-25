"""Compact JSON codec for :mod:`hedge_memory_rag.redis_cache`.

Design § Data Models — "All Hot_Path payloads are FlatBuffers (R1.5)
for zero-copy reads. All Warm_AI_Pipeline payloads are JSON for
ergonomics, except embeddings which are CBOR." — so Memory_RAG_Layer
non-embedding caches use compact JSON.

The codec is intentionally narrow:

* Encodes :class:`pydantic.BaseModel` instances via ``model_dump_json``
  (which honours ``alias=`` and bound enums and emits canonical JSON
  for all types declared by Hedge schemas). Plain ``dict`` / ``list``
  / ``str`` / ``int`` / ``float`` / ``bool`` / ``None`` are also
  accepted to keep the ``set_regime`` / ``set_stability_score`` APIs
  ergonomic for primitive payloads.
* Decodes back to a Python value via :func:`json.loads`. Pydantic
  reconstruction is the caller's responsibility — the cache stores
  payloads, not types.
* Wraps every encode/decode failure in :class:`RedisCacheCodecError`
  so the cache never raises a raw :class:`json.JSONDecodeError` to
  callers. Operationally, a decode failure means a corrupt entry at
  rest; surfacing a typed error lets callers distinguish "missing"
  from "unreadable".
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel

from .errors import RedisCacheCodecError


def encode_payload(value: Any, *, op: str, key: str | None = None) -> bytes:
    """Encode an arbitrary value into a compact JSON byte string.

    Args:
        value: The value to encode. May be a :class:`pydantic.BaseModel`
            (encoded via ``model_dump_json``), a JSON-compatible
            primitive, dict, or list.
        op: Logical cache operation name, propagated into the raised
            error for log filtering (``"cache_trade"``, ``"set_regime"``,
            …).
        key: Optional Redis key the encode targets.

    Returns:
        UTF-8 encoded JSON bytes ready to be stored in Redis.

    Raises:
        RedisCacheCodecError: ``value`` was not JSON-encodable or a
            pydantic ``model_dump_json`` round-trip failed.
    """
    try:
        if isinstance(value, BaseModel):
            return value.model_dump_json(by_alias=True).encode("utf-8")
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode(
            "utf-8"
        )
    except (TypeError, ValueError) as exc:
        raise RedisCacheCodecError(
            f"failed to encode payload for op={op!r}: {exc}",
            op=op,
            key=key,
        ) from exc


def decode_payload(raw: bytes | str | None, *, op: str, key: str | None = None) -> Any:
    """Decode a JSON byte string into a Python value.

    Args:
        raw: Bytes (or str) returned by :mod:`redis.asyncio`. ``None``
            is returned verbatim — callers use it to detect a missing
            key.
        op: Logical cache operation name, propagated into the raised
            error for log filtering.
        key: Optional Redis key the decode targets.

    Returns:
        The decoded value, or ``None`` if ``raw`` is ``None``.

    Raises:
        RedisCacheCodecError: ``raw`` was not valid UTF-8 JSON.
    """
    if raw is None:
        return None
    if isinstance(raw, bytes):
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise RedisCacheCodecError(
                f"failed to decode payload bytes for op={op!r}: {exc}",
                op=op,
                key=key,
            ) from exc
    else:
        text = raw
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise RedisCacheCodecError(
            f"failed to parse JSON for op={op!r}: {exc}",
            op=op,
            key=key,
        ) from exc


__all__ = ["encode_payload", "decode_payload"]
