"""Unit tests for :mod:`hedge_warm_ai.ollama_client` (task 19.1).

These tests exercise the client against an in-process
:class:`httpx.MockTransport`, so no real Ollama daemon is required.

The full property-based coverage (one ``ai.ollama.degraded`` event per
unresponsive condition with fallback rerouting) lives in task 19.2; this
file establishes the example-based contract:

* Streaming yields one chunk per NDJSON line and the trailing chunk
  carries ``done=True`` plus the daemon's metrics.
* A timeout, a 5xx, and a connection refusal each trigger fallback
  routing **and** publish exactly one ``ai.ollama.degraded`` event.
* A 4xx response is **not** rerouted (caller bug).
* Configuration validation rejects unknown roles.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Iterable, Iterator

import httpx
import pytest

from hedge_warm_ai.ollama_client import (
    InMemoryDegradedPublisher,
    OllamaAllFallbacksExhaustedError,
    OllamaClient,
    OllamaHttpError,
    OllamaModelEndpoint,
    OllamaResponseChunk,
)


# ---------------------------------------------------------------------------
# Fixtures and helpers ------------------------------------------------------
# ---------------------------------------------------------------------------


def _ndjson_body(lines: Iterable[dict[str, Any]]) -> bytes:
    return ("\n".join(json.dumps(line) for line in lines) + "\n").encode("utf-8")


def _ok_streaming_response(model: str, tokens: list[str]) -> httpx.Response:
    body_lines: list[dict[str, Any]] = [
        {"model": model, "response": tok, "done": False} for tok in tokens
    ]
    body_lines.append(
        {
            "model": model,
            "response": "",
            "done": True,
            "total_duration": 1234,
            "eval_count": len(tokens),
            "eval_duration": 999,
            "done_reason": "stop",
        }
    )
    return httpx.Response(200, content=_ndjson_body(body_lines))


def _make_endpoints() -> dict[str, OllamaModelEndpoint]:
    return {
        "qwen": OllamaModelEndpoint(
            base_url="http://ollama-qwen:11434",
            model="qwen2.5:14b-instruct-q4_K_M",
            timeout_s=2.0,
            connect_timeout_s=0.5,
        ),
        "mistral": OllamaModelEndpoint(
            base_url="http://ollama-mistral:11434",
            model="mistral:7b-instruct-q4_K_M",
            timeout_s=1.0,
            connect_timeout_s=0.5,
        ),
        "phi": OllamaModelEndpoint(
            base_url="http://ollama-phi:11434",
            model="phi3:mini-q4_K_M",
            timeout_s=1.0,
            connect_timeout_s=0.5,
        ),
    }


def _build_client(handler) -> OllamaClient:
    publisher = InMemoryDegradedPublisher()
    transport = httpx.MockTransport(handler)
    http = httpx.AsyncClient(transport=transport)
    client = OllamaClient(
        endpoints=_make_endpoints(),
        fallback_chain={"qwen": "mistral", "mistral": "phi"},
        publisher=publisher,
        http_client=http,
    )
    # Attach for ergonomic test access.
    client._publisher_for_tests = publisher  # type: ignore[attr-defined]
    return client


# ---------------------------------------------------------------------------
# Happy path ----------------------------------------------------------------
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_generate_yields_tokens_then_done_chunk() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/generate"
        body = json.loads(request.content)
        assert body["model"] == "qwen2.5:14b-instruct-q4_K_M"
        assert body["stream"] is True
        return _ok_streaming_response(body["model"], ["he", "llo"])

    async with _build_client(handler) as client:
        chunks: list[OllamaResponseChunk] = []
        async for chunk in client.stream_generate("qwen", prompt="hi"):
            chunks.append(chunk)

    assert [c.text for c in chunks] == ["he", "llo", ""]
    assert chunks[-1].done is True
    assert chunks[-1].metrics is not None
    assert chunks[-1].metrics["eval_count"] == 2
    assert all(c.role == "qwen" for c in chunks)


@pytest.mark.asyncio
async def test_options_reserved_keys_rejected() -> None:
    async with _build_client(lambda r: _ok_streaming_response("m", [])) as client:
        with pytest.raises(ValueError, match="reserved key"):
            async for _ in client.stream_generate(
                "qwen", prompt="hi", options={"model": "evil"}
            ):
                pass  # pragma: no cover - generator never advances


@pytest.mark.asyncio
async def test_unknown_role_raises_keyerror() -> None:
    async with _build_client(lambda r: _ok_streaming_response("m", [])) as client:
        with pytest.raises(KeyError):
            async for _ in client.stream_generate("nope", prompt="hi"):
                pass  # pragma: no cover


# ---------------------------------------------------------------------------
# Fallback paths ------------------------------------------------------------
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_5xx_triggers_fallback_with_one_degraded_event() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if "qwen" in str(request.url):
            return httpx.Response(500, text="boom")
        return _ok_streaming_response("mistral:7b-instruct-q4_K_M", ["ok"])

    client = _build_client(handler)
    async with client:
        chunks = [c async for c in client.stream_generate("qwen", prompt="hi")]

    assert chunks, "fallback should have produced at least one chunk"
    assert all(c.role == "mistral" for c in chunks)
    pub: InMemoryDegradedPublisher = client._publisher_for_tests  # type: ignore[attr-defined]
    assert len(pub.events) == 1
    evt = pub.events[0]
    assert evt.model == "qwen2.5:14b-instruct-q4_K_M"
    assert evt.fallback_model == "mistral:7b-instruct-q4_K_M"
    assert evt.reason == "crashed"  # 5xx → crashed
    assert evt.ts_ns >= 0


@pytest.mark.asyncio
async def test_4xx_is_not_rerouted() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, text="model not found")

    client = _build_client(handler)
    async with client:
        with pytest.raises(OllamaHttpError) as ei:
            async for _ in client.stream_generate("qwen", prompt="hi"):
                pass  # pragma: no cover
    assert ei.value.status_code == 404
    pub: InMemoryDegradedPublisher = client._publisher_for_tests  # type: ignore[attr-defined]
    assert pub.events == []


@pytest.mark.asyncio
async def test_connection_failure_triggers_fallback_unresponsive() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if "qwen" in str(request.url):
            raise httpx.ConnectError("refused", request=request)
        return _ok_streaming_response("mistral:7b-instruct-q4_K_M", ["ok"])

    client = _build_client(handler)
    async with client:
        chunks = [c async for c in client.stream_generate("qwen", prompt="hi")]

    assert chunks
    assert all(c.role == "mistral" for c in chunks)
    pub: InMemoryDegradedPublisher = client._publisher_for_tests  # type: ignore[attr-defined]
    assert len(pub.events) == 1
    assert pub.events[0].reason == "unresponsive"


@pytest.mark.asyncio
async def test_read_timeout_triggers_fallback_timeout() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if "qwen" in str(request.url):
            raise httpx.ReadTimeout("slow", request=request)
        return _ok_streaming_response("mistral:7b-instruct-q4_K_M", ["ok"])

    client = _build_client(handler)
    async with client:
        chunks = [c async for c in client.stream_generate("qwen", prompt="hi")]

    pub: InMemoryDegradedPublisher = client._publisher_for_tests  # type: ignore[attr-defined]
    assert len(pub.events) == 1
    assert pub.events[0].reason == "timeout"
    assert all(c.role == "mistral" for c in chunks)


@pytest.mark.asyncio
async def test_all_fallbacks_exhausted_raises() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="all dead")

    client = _build_client(handler)
    async with client:
        with pytest.raises(OllamaAllFallbacksExhaustedError) as ei:
            async for _ in client.stream_generate("qwen", prompt="hi"):
                pass  # pragma: no cover
    # qwen → mistral → phi → (terminal) — 2 hops, 3 failures.
    assert len(ei.value.failures) == 3
    pub: InMemoryDegradedPublisher = client._publisher_for_tests  # type: ignore[attr-defined]
    # One event per fallback hop.
    assert len(pub.events) == 2


@pytest.mark.asyncio
async def test_fallback_chain_referencing_unknown_role_rejected() -> None:
    with pytest.raises(ValueError, match="unregistered"):
        OllamaClient(
            endpoints=_make_endpoints(),
            fallback_chain={"qwen": "ghost"},
        )


@pytest.mark.asyncio
async def test_fallback_chain_with_cycle_does_not_loop_forever() -> None:
    # Build a cyclic chain by overriding it after construction-time
    # validation — simulates a runtime registry edit.
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="dead")

    transport = httpx.MockTransport(handler)
    http = httpx.AsyncClient(transport=transport)
    publisher = InMemoryDegradedPublisher()
    client = OllamaClient(
        endpoints=_make_endpoints(),
        fallback_chain={"qwen": "mistral", "mistral": "qwen"},  # cycle
        publisher=publisher,
        http_client=http,
    )
    async with client:
        with pytest.raises(OllamaAllFallbacksExhaustedError):
            async for _ in client.stream_generate("qwen", prompt="hi"):
                pass  # pragma: no cover


# ---------------------------------------------------------------------------
# Endpoint validation -------------------------------------------------------
# ---------------------------------------------------------------------------


def test_endpoint_rejects_empty_url_or_model() -> None:
    with pytest.raises(ValueError):
        OllamaModelEndpoint(base_url="", model="x")
    with pytest.raises(ValueError):
        OllamaModelEndpoint(base_url="http://x", model="")


def test_endpoint_rejects_non_positive_timeouts() -> None:
    with pytest.raises(ValueError):
        OllamaModelEndpoint(base_url="http://x", model="m", timeout_s=0.0)
    with pytest.raises(ValueError):
        OllamaModelEndpoint(base_url="http://x", model="m", connect_timeout_s=0.0)


def test_endpoint_generate_url_is_well_formed() -> None:
    e = OllamaModelEndpoint(base_url="http://ollama-qwen:11434/", model="m")
    assert e.generate_url == "http://ollama-qwen:11434/api/generate"


# ---------------------------------------------------------------------------
# Publisher -----------------------------------------------------------------
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_in_memory_publisher_collects_events() -> None:
    pub = InMemoryDegradedPublisher()
    from hedge_warm_ai.schemas import OllamaDegraded

    evt = OllamaDegraded(
        model="qwen2.5:14b", fallback_model="mistral:7b", reason="timeout", ts_ns=42
    )
    await pub.publish_degraded(evt)
    assert pub.events == [evt]
    pub.reset()
    assert pub.events == []


@pytest.mark.asyncio
async def test_nats_publisher_serialises_event_as_compact_json() -> None:
    from hedge_warm_ai.ollama_client import NatsDegradedPublisher
    from hedge_warm_ai.schemas import OllamaDegraded

    captured: list[tuple[str, bytes]] = []

    async def fake_publish(subject: str, payload: bytes) -> None:
        captured.append((subject, payload))

    pub = NatsDegradedPublisher(async_publish=fake_publish)
    evt = OllamaDegraded(
        model="qwen2.5:14b", fallback_model="mistral:7b", reason="timeout", ts_ns=42
    )
    await pub.publish_degraded(evt)
    assert len(captured) == 1
    subject, payload = captured[0]
    assert subject == "ai.ollama.degraded"
    parsed = json.loads(payload)
    assert parsed == {
        "model": "qwen2.5:14b",
        "fallback_model": "mistral:7b",
        "reason": "timeout",
        "ts_ns": 42,
    }
