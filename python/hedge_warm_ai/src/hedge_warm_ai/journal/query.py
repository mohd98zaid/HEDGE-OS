"""Request-reply schema for ``mem.journal.query`` (R18.3).

The AI_Trade_Journal_Engine exposes two read paths to the rest of the
system:

* **Subscription** — ``ai.journal.entry`` is published once per closed
  trade so live consumers (UI gateway, AI_Governance, AI_Shadow_Mode,
  retrieval pipeline) can stream the journal as it happens.
* **Query** — ``mem.journal.query`` request-reply for one-shot
  windowed reads. The reply payload is JSON, mirroring the query API
  established by :class:`hedge_warm_ai.prev_day.engine.PrevDayQueryRequest`.

The query supports two modes (mutually compatible — both can run in a
single request):

1. **Time-window read** via
   :meth:`hedge_memory_rag.timescale.TimescaleReader.read_journal_entries`.
   Returns persisted entries in ``[start_ts, end_ts)`` order. Optional
   ``symbol`` and ``trade_id`` narrow the scan further.
2. **Similarity search** via
   :meth:`hedge_memory_rag.qdrant.MemoryRagQdrant.knn_search` against
   the :data:`CollectionName.JOURNAL_ENTRIES` collection. Returns the
   top-``k`` nearest journal entries to the supplied query vector or
   query string. The query string is embedded with the same
   embedder used for ingestion (DistilBERT) so the similarity space
   matches the persisted vectors.

Both modes are optional — a request that supplies neither is treated
as a windowed read with no symbol filter.

The reply payload is canonical JSON; it does not flow through the
:class:`AiJournalEntry` wire schema directly because that schema
represents one *fresh* publication and does not carry the
``score`` field returned by kNN. Instead, the reply uses a
:class:`JournalQueryHit` envelope that wraps each entry with optional
similarity metadata.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Annotated, Any, Sequence

from pydantic import BaseModel, ConfigDict, Field

from ..schemas import AiJournalEntry


class JournalQueryError(RuntimeError):
    """Raised when a ``mem.journal.query`` payload is malformed."""


# ---------------------------------------------------------------------------
# Request / reply DTOs ------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class JournalQueryRequest:
    """Decoded ``mem.journal.query`` request payload.

    Attributes:
        start_ts_ns: Optional inclusive window start. ``None``
            defaults to ``end_ts_ns - 86_400_000_000_000`` (24 h).
        end_ts_ns: Optional exclusive window end. ``None`` defaults
            to "now" at decode time.
        symbol: Optional symbol filter forwarded to Timescale.
        trade_id: Optional trade-id filter forwarded to Timescale.
        limit: Maximum rows returned by the windowed read. Defaults
            to :data:`DEFAULT_QUERY_LIMIT`.
        similarity_query: Optional natural-language query string to
            embed and run as a kNN search against Qdrant. ``None``
            disables similarity search.
        similarity_vector: Optional pre-computed query vector.
            Mutually exclusive with ``similarity_query``; when both
            are provided, ``similarity_vector`` wins (no embedder
            call needed). When neither is provided, similarity search
            is skipped.
        similarity_k: Top-k for similarity search. Defaults to
            :data:`DEFAULT_KNN_K`.
        similarity_filter: Optional payload-equality filter for the
            kNN query (e.g. ``{"symbol": "RELIANCE"}``). Mirrors
            :meth:`MemoryRagQdrant.knn_search`'s flat-mapping form.
    """

    start_ts_ns: int | None = None
    end_ts_ns: int | None = None
    symbol: str | None = None
    trade_id: str | None = None
    limit: int = 256

    similarity_query: str | None = None
    similarity_vector: tuple[float, ...] | None = None
    similarity_k: int = 5
    similarity_filter: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.limit <= 0:
            raise ValueError(f"limit must be > 0, got {self.limit!r}")
        if self.similarity_k <= 0:
            raise ValueError(f"similarity_k must be > 0, got {self.similarity_k!r}")
        if self.start_ts_ns is not None and self.end_ts_ns is not None:
            if self.start_ts_ns > self.end_ts_ns:
                raise ValueError(
                    f"start_ts_ns ({self.start_ts_ns}) must be <= end_ts_ns "
                    f"({self.end_ts_ns})"
                )

    @classmethod
    def from_json_bytes(cls, payload: bytes) -> "JournalQueryRequest":
        try:
            obj = json.loads(payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise JournalQueryError(f"malformed query payload: {exc}") from exc
        if not isinstance(obj, dict):
            raise JournalQueryError(
                f"query payload must be a JSON object, got {type(obj).__name__}"
            )

        def _opt_int(name: str) -> int | None:
            v = obj.get(name)
            return None if v is None else int(v)

        def _opt_str(name: str) -> str | None:
            v = obj.get(name)
            return None if v is None else str(v)

        sim_vec_raw = obj.get("similarity_vector")
        sim_vec: tuple[float, ...] | None
        if sim_vec_raw is None:
            sim_vec = None
        else:
            try:
                sim_vec = tuple(float(x) for x in sim_vec_raw)
            except (TypeError, ValueError) as exc:
                raise JournalQueryError(
                    f"similarity_vector must be a list of numbers: {exc}"
                ) from exc
            if not sim_vec:
                raise JournalQueryError("similarity_vector must be non-empty")

        sim_filter_raw = obj.get("similarity_filter") or {}
        if not isinstance(sim_filter_raw, dict):
            raise JournalQueryError(
                f"similarity_filter must be a JSON object, got {type(sim_filter_raw).__name__}"
            )

        return cls(
            start_ts_ns=_opt_int("start_ts_ns"),
            end_ts_ns=_opt_int("end_ts_ns"),
            symbol=_opt_str("symbol"),
            trade_id=_opt_str("trade_id"),
            limit=int(obj.get("limit", DEFAULT_QUERY_LIMIT)),
            similarity_query=_opt_str("similarity_query"),
            similarity_vector=sim_vec,
            similarity_k=int(obj.get("similarity_k", DEFAULT_KNN_K)),
            similarity_filter=dict(sim_filter_raw),
        )

    def resolve_window(self, *, now_ns: int) -> tuple[int, int]:
        """Resolve ``[start_ts_ns, end_ts_ns)`` with sensible defaults.

        ``now_ns`` is the server-side wall-clock; passing it in keeps
        the resolution deterministic for tests.
        """
        end_ns = self.end_ts_ns if self.end_ts_ns is not None else int(now_ns)
        if self.start_ts_ns is not None:
            start_ns = self.start_ts_ns
        else:
            # Default look-back: 24 hours.
            start_ns = max(0, end_ns - 86_400_000_000_000)
        return start_ns, end_ns


class JournalQueryHit(BaseModel):
    """One hit returned in a :class:`JournalQueryReply`.

    Wraps the canonical :class:`AiJournalEntry` payload with optional
    similarity metadata so callers can branch on the source (windowed
    read vs. kNN search) without having to merge two reply shapes.
    """

    model_config = ConfigDict(extra="forbid")

    entry: AiJournalEntry
    source: Annotated[str, Field(min_length=1, max_length=16)]
    score: float | None = None


class JournalQueryReply(BaseModel):
    """Decoded ``mem.journal.query`` reply payload."""

    model_config = ConfigDict(extra="forbid")

    window_hits: list[JournalQueryHit] = Field(default_factory=list)
    similarity_hits: list[JournalQueryHit] = Field(default_factory=list)
    ts_ns: Annotated[int, Field(ge=0)]

    def to_json_bytes(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"), separators=(",", ":")
        ).encode("utf-8")


# ---------------------------------------------------------------------------
# Helpers -------------------------------------------------------------------
# ---------------------------------------------------------------------------


#: Default windowed-read row cap.
DEFAULT_QUERY_LIMIT: int = 256

#: Default similarity-search ``k``.
DEFAULT_KNN_K: int = 5


def row_to_entry(row: Any) -> AiJournalEntry:
    """Project a persisted :class:`JournalEntryRow` back to the wire shape.

    Inverse of :func:`hedge_warm_ai.journal.persistence.journal_entry_to_row`.
    Used by the engine when streaming Timescale rows back to a query
    requester.
    """
    ts: datetime = row.ts
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    ts_ns = int(ts.timestamp() * 1_000_000_000)
    return AiJournalEntry(
        correlation_id=row.correlation_id,
        trade_id=row.trade_id,
        symbol=row.symbol,
        side=row.side,
        quantity=int(row.quantity),
        entry_paise=int(row.entry_paise),
        exit_paise=int(row.exit_paise),
        pnl_inr=float(row.pnl_inr),
        narrative=row.narrative,
        ts_ns=ts_ns,
    )


def hit_from_qdrant_payload(
    payload: Sequence | dict, score: float
) -> JournalQueryHit | None:
    """Build a :class:`JournalQueryHit` from a Qdrant kNN payload.

    Returns ``None`` when the payload is missing the required fields
    (e.g. an entry written by a previous engine version that did not
    include the canonical projection). The engine logs the skip
    rather than raising so a partially-corrupt collection does not
    break the query API.
    """
    if not isinstance(payload, dict):
        return None
    required = (
        "correlation_id",
        "trade_id",
        "symbol",
        "side",
        "quantity",
        "entry_paise",
        "exit_paise",
        "pnl_inr",
        "narrative",
        "ts_ns",
    )
    if not all(k in payload for k in required):
        return None
    try:
        entry = AiJournalEntry(
            correlation_id=str(payload["correlation_id"]),
            trade_id=str(payload["trade_id"]),
            symbol=str(payload["symbol"]),
            side=str(payload["side"]),  # type: ignore[arg-type]
            quantity=int(payload["quantity"]),
            entry_paise=int(payload["entry_paise"]),
            exit_paise=int(payload["exit_paise"]),
            pnl_inr=float(payload["pnl_inr"]),
            narrative=str(payload["narrative"]),
            ts_ns=int(payload["ts_ns"]),
        )
    except Exception:
        return None
    return JournalQueryHit(entry=entry, source="similarity", score=float(score))


__all__ = [
    "DEFAULT_KNN_K",
    "DEFAULT_QUERY_LIMIT",
    "JournalQueryError",
    "JournalQueryHit",
    "JournalQueryReply",
    "JournalQueryRequest",
    "hit_from_qdrant_payload",
    "row_to_entry",
]
