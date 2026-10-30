import asyncio
import json
import unittest
from collections.abc import AsyncIterator
from typing import Any

import httpx

from gateway import (
    MAX_PENDING_CHARS,
    StreamingPIIRedactor,
    _redact_sse_stream,
    create_app,
    redact_pii,
)


class ChunkedByteStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            yield chunk

    async def aclose(self) -> None:
        return None


def _sse(content: str) -> bytes:
    payload = {
        "id": "chatcmpl-demo",
        "object": "chat.completion.chunk",
        "choices": [{"index": 0, "delta": {"content": content}, "finish_reason": None}],
    }
    return (
        "data: "
        + json.dumps(payload, separators=(",", ":"))
        + "\n\n"
    ).encode()


def _parse_sse_content(body: bytes) -> tuple[str, list[Any]]:
    text_chunks: list[str] = []
    payloads: list[Any] = []
    for event in body.decode().split("\n\n"):
        if not event:
            continue
        data_lines = [
            line[5:].lstrip()
            for line in event.splitlines()
            if line.startswith("data:")
        ]
        if not data_lines:
            continue
        data = "\n".join(data_lines)
        if data == "[DONE]":
            continue
        payload = json.loads(data)
        if not isinstance(payload, dict):
            continue
        payloads.append(payload)
        for choice in payload.get("choices", []):
            delta = choice.get("delta", {})
            if isinstance(delta.get("content"), str):
                text_chunks.append(delta["content"])
    return "".join(text_chunks), payloads


class StreamingRedactorTests(unittest.TestCase):
    def test_redacts_sensitive_values_in_normal_text(self) -> None:
        text = (
            "Email alice.smith@example.com, SSN 123-45-6789, "
            "card 4111 1111 1111 1111."
        )
        result = redact_pii(text)

        self.assertEqual(
            result,
            f"Email {REDACTED}, SSN {REDACTED}, card {REDACTED}.",
        )
        self.assertNotIn("alice.smith@example.com", result)
        self.assertNotIn("123-45-6789", result)
        self.assertNotIn("4111 1111 1111 1111", result)

    def test_does_not_redact_non_luhn_numbers_or_non_sensitive_text(self) -> None:
        text = "Order 4111 1111 1111 1112; reference 123456789012"
        self.assertEqual(redact_pii(text), text)

    def test_redacts_patterns_split_across_arbitrary_chunks(self) -> None:
        redactor = StreamingPIIRedactor()
        chunks = [
            "Contact alice",
            ".smith@exam",
            "ple.com and SSN 123-",
            "45-6789, card 4111 1111 1111 ",
            "1111!",
        ]
        result = "".join(redactor.feed(chunk) for chunk in chunks) + redactor.flush()

        self.assertEqual(
            result,
            f"Contact {REDACTED} and SSN {REDACTED}, card {REDACTED}!",
        )
        self.assertNotIn("alice.smith@example.com", result)
        self.assertNotIn("123-45-6789", result)
        self.assertNotIn("4111 1111 1111 1111", result)

    def test_prefix_is_released_without_waiting_for_full_completion(self) -> None:
        redactor = StreamingPIIRedactor()
        first_output = redactor.feed("Hello there, this is a normal ")
        self.assertEqual(first_output, "Hello there, this is a normal ")

    def test_pending_memory_is_bounded(self) -> None:
        redactor = StreamingPIIRedactor()
        output = redactor.feed("word" * (MAX_PENDING_CHARS + 100))
        self.assertTrue(output)
        self.assertLessEqual(redactor.pending_characters, MAX_PENDING_CHARS)


class StreamingGatewayTests(unittest.IsolatedAsyncioTestCase):
    async def test_safe_prefix_is_emitted_before_provider_stream_finishes(self) -> None:
        allow_provider_to_continue = asyncio.Event()

        async def provider_stream() -> AsyncIterator[bytes]:
            yield _sse("Hello there")
            await allow_provider_to_continue.wait()
            yield _sse(" friend.")
            yield b"data: [DONE]\n\n"

        redacted_stream = _redact_sse_stream(provider_stream())
        first_event = await anext(redacted_stream)
        first_payload = json.loads(first_event.decode().split("data: ", 1)[1])

        self.assertEqual(
            first_payload["choices"][0]["delta"]["content"],
            "Hello ",
        )
        allow_provider_to_continue.set()
        remaining_events = [chunk async for chunk in redacted_stream]
        complete_text, _ = _parse_sse_content(first_event + b"".join(remaining_events))
        self.assertEqual(complete_text, "Hello there friend.")

    async def test_stream_redacts_pii_across_sse_event_and_byte_boundaries(self) -> None:
        events = [
            _sse("Contact alice.smith@example"),
            _sse(".com and SSN 123-"),
            _sse("45-6789, card 4111 1111 1111 "),
            _sse("1111!"),
            b"data: [DONE]\n\n",
        ]
        byte_chunks = [piece for event in events for piece in (event[:9], event[9:])]

        def provider(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.method, "POST")
            self.assertEqual(request.url.path, "/v1/chat/completions")
            self.assertEqual(
                request.headers["authorization"],
                "Bearer provider-test-key",
            )
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream; charset=utf-8"},
                stream=ChunkedByteStream(byte_chunks),
            )

        provider_client = httpx.AsyncClient(transport=httpx.MockTransport(provider))
        app = create_app(
            upstream_url="https://provider.test/v1/chat/completions",
            api_key="provider-test-key",
            upstream_client=provider_client,
        )
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://gateway",
        )
        try:
            response = await client.post(
                "/v1/chat/completions",
                json={
                    "model": "demo-model",
                    "stream": True,
                    "messages": [{"role": "user", "content": "Write a response."}],
                },
            )
        finally:
            await client.aclose()
            await provider_client.aclose()

        self.assertEqual(response.status_code, 200)
        self.assertIn("text/event-stream", response.headers["content-type"])
        text, payloads = _parse_sse_content(response.content)
        self.assertEqual(
            text,
            f"Contact {REDACTED} and SSN {REDACTED}, card {REDACTED}!",
        )
        self.assertIn("[REDACTED]", response.text)
        self.assertNotIn("alice.smith@example.com", response.text)
        self.assertNotIn("123-45-6789", response.text)
        self.assertNotIn("4111 1111 1111 1111", response.text)
        self.assertTrue(payloads)
        self.assertIn("data: [DONE]", response.text)

    async def test_provider_errors_are_relayed_without_leaking_gateway_tracebacks(
        self,
    ) -> None:
        error_body = b'{"error":{"message":"provider rate limited"}}'

        def provider(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                429,
                headers={"content-type": "application/json"},
                stream=ChunkedByteStream([error_body]),
            )

        provider_client = httpx.AsyncClient(transport=httpx.MockTransport(provider))
        app = create_app(
            upstream_url="https://provider.test/v1/chat/completions",
            upstream_client=provider_client,
        )
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://gateway",
        )
        try:
            response = await client.post("/v1/chat/completions", json={"stream": True})
        finally:
            await client.aclose()
            await provider_client.aclose()

        self.assertEqual(response.status_code, 429)
        self.assertIn("provider rate limited", response.text)
        self.assertNotIn("Traceback", response.text)

    async def test_provider_timeout_returns_sanitized_gateway_error(self) -> None:
        def provider(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectTimeout("private provider details", request=request)

        provider_client = httpx.AsyncClient(transport=httpx.MockTransport(provider))
        app = create_app(
            upstream_url="https://provider.test/v1/chat/completions",
            upstream_client=provider_client,
        )
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://gateway",
        )
        try:
            response = await client.post("/v1/chat/completions", json={"stream": True})
        finally:
            await client.aclose()
            await provider_client.aclose()

        self.assertEqual(response.status_code, 504)
        self.assertEqual(
            response.json(),
            {"error": {"message": "The LLM provider timed out."}},
        )
        self.assertNotIn("private provider details", response.text)


REDACTED = "[REDACTED]"


if __name__ == "__main__":
    unittest.main()
