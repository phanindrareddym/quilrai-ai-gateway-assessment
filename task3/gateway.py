"""Streaming LLM gateway that redacts PII in OpenAI-compatible SSE deltas."""

import copy
import json
import os
import re
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse


REDACTED = "[REDACTED]"
MAX_PENDING_CHARS = 320
MAX_SSE_EVENT_BYTES = 1_048_576
_EMAIL_LOCAL_CHARS = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.!#$%&'*+/=?^_`{|}~-@"
)
_EMAIL_RE = re.compile(
    r"(?<![A-Za-z0-9.!#$%&'*+/=?^_`{|}~-])"
    r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]{1,64}"
    r"@[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+"
    r"(?![A-Za-z0-9_-])"
)
_SSN_RE = re.compile(
    r"(?<!\d)(?!000|666|9\d\d)\d{3}-(?!00)\d{2}-(?!0000)\d{4}(?!\d)"
)
_CARD_RE = re.compile(r"(?<!\d)(?:\d[ -]?){12,18}\d(?!\d)")
_HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "content-length",
    "content-encoding",
}


def _passes_luhn(candidate: str) -> bool:
    digits = [int(character) for character in candidate if character.isdigit()]
    if not 13 <= len(digits) <= 19:
        return False

    checksum = 0
    parity = len(digits) % 2
    for index, digit in enumerate(digits):
        if index % 2 == parity:
            digit *= 2
            if digit > 9:
                digit -= 9
        checksum += digit
    return checksum % 10 == 0


def redact_pii(text: str) -> str:
    """Redact complete email, SSN, and Luhn-valid card-number patterns."""
    redacted = _EMAIL_RE.sub(REDACTED, text)
    redacted = _SSN_RE.sub(REDACTED, redacted)
    return _CARD_RE.sub(
        lambda match: REDACTED if _passes_luhn(match.group()) else match.group(),
        redacted,
    )


class StreamingPIIRedactor:
    """Incrementally redact PII while retaining only a bounded ambiguous suffix."""

    def __init__(self) -> None:
        self._pending = ""

    @property
    def pending_characters(self) -> int:
        return len(self._pending)

    def _safe_prefix_length(self) -> int:
        safe_end = len(self._pending)

        email_start = safe_end
        while email_start > 0 and self._pending[email_start - 1] in _EMAIL_LOCAL_CHARS:
            email_start -= 1
        if safe_end - email_start <= MAX_PENDING_CHARS:
            safe_end = min(safe_end, email_start)

        card_end = len(self._pending)
        while card_end > 0 and self._pending[card_end - 1] in ".,;:!?)]}\"'":
            card_end -= 1
        card_start = card_end
        while card_start > 0 and self._pending[card_start - 1] in "0123456789 -":
            card_start -= 1
        if (
            card_start < card_end
            and any(character.isdigit() for character in self._pending[card_start:card_end])
            and card_end - card_start <= 64
        ):
            safe_end = min(safe_end, card_start)

        if len(self._pending) - safe_end > MAX_PENDING_CHARS:
            safe_end = len(self._pending) - MAX_PENDING_CHARS
        return safe_end

    def feed(self, chunk: str) -> str:
        self._pending += chunk
        safe_end = self._safe_prefix_length()
        if safe_end <= 0:
            return ""

        safe_text = self._pending[:safe_end]
        self._pending = self._pending[safe_end:]
        return redact_pii(safe_text)

    def flush(self) -> str:
        remaining = self._pending
        self._pending = ""
        return redact_pii(remaining)


def _sse_event(fields: list[str], payload: Any) -> bytes:
    lines = [line for line in fields if not line.startswith("data:")]
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    lines.extend(f"data: {line}" for line in encoded.splitlines() or [""])
    return ("\n".join(lines) + "\n\n").encode("utf-8")


def _decode_sse_event(event: bytes) -> tuple[list[str], str | None]:
    lines = event.decode("utf-8", errors="replace").splitlines()
    fields: list[str] = []
    data: list[str] = []
    for line in lines:
        if line.startswith(":"):
            fields.append(line)
        elif line.startswith("data:"):
            value = line[5:]
            data.append(value[1:] if value.startswith(" ") else value)
        elif line:
            fields.append(line)
    return fields, "\n".join(data) if data else None


def _choice_index(choice: Mapping[str, Any], fallback: int) -> int:
    index = choice.get("index", fallback)
    return index if isinstance(index, int) and not isinstance(index, bool) else fallback


async def _redact_sse_stream(
    source: AsyncIterator[bytes],
) -> AsyncIterator[bytes]:
    event_buffer = bytearray()
    redactors: dict[int, StreamingPIIRedactor] = {}
    last_payload: dict[str, Any] | None = None
    last_fields: list[str] = []
    emitted_done = False

    async def flush_redactors() -> list[bytes]:
        if last_payload is None:
            return []
        payload = copy.deepcopy(last_payload)
        choices = payload.get("choices")
        if not isinstance(choices, list):
            return []

        output: list[bytes] = []
        has_text = False
        for fallback_index, choice in enumerate(choices):
            if not isinstance(choice, dict):
                continue
            index = _choice_index(choice, fallback_index)
            delta = choice.get("delta")
            if not isinstance(delta, dict):
                continue
            redactor = redactors.get(index)
            tail = redactor.flush() if redactor is not None else ""
            if tail:
                delta["content"] = tail
                has_text = True
            elif isinstance(delta.get("content"), str):
                delta["content"] = ""

        if has_text:
            output.append(_sse_event(last_fields, payload))
        return output

    async def process_event(event: bytes) -> list[bytes]:
        nonlocal last_payload, last_fields, emitted_done
        fields, data = _decode_sse_event(event)
        if data is None:
            return [event + b"\n\n"]
        if data == "[DONE]":
            result = await flush_redactors()
            event_fields = [line for line in fields if not line.startswith("data:")]
            event_fields.append("data: [DONE]")
            result.append(("\n".join(event_fields) + "\n\n").encode("utf-8"))
            emitted_done = True
            return result

        try:
            payload = json.loads(data)
        except (json.JSONDecodeError, TypeError):
            return [event + b"\n\n"]
        if not isinstance(payload, dict):
            return [event + b"\n\n"]

        choices = payload.get("choices")
        if not isinstance(choices, list):
            return [_sse_event(fields, payload)]

        has_text_delta = False
        for fallback_index, choice in enumerate(choices):
            if not isinstance(choice, dict):
                continue
            delta = choice.get("delta")
            if not isinstance(delta, dict) or not isinstance(delta.get("content"), str):
                continue
            has_text_delta = True
            index = _choice_index(choice, fallback_index)
            redactor = redactors.setdefault(index, StreamingPIIRedactor())
            delta["content"] = redactor.feed(delta["content"])

        if has_text_delta:
            last_payload = copy.deepcopy(payload)
            last_fields = fields
        return [_sse_event(fields, payload)]

    async def process_buffered_events() -> AsyncIterator[bytes]:
        while True:
            delimiter_length = 0
            delimiter_index = event_buffer.find(b"\n\n")
            crlf_index = event_buffer.find(b"\r\n\r\n")
            if crlf_index >= 0 and (delimiter_index < 0 or crlf_index < delimiter_index):
                delimiter_index = crlf_index
                delimiter_length = 4
            elif delimiter_index >= 0:
                delimiter_length = 2

            if delimiter_index < 0:
                if len(event_buffer) > MAX_SSE_EVENT_BYTES:
                    raise ValueError("Upstream SSE event exceeded the configured size limit.")
                break
            if delimiter_index > MAX_SSE_EVENT_BYTES:
                raise ValueError("Upstream SSE event exceeded the configured size limit.")

            event = bytes(event_buffer[:delimiter_index])
            del event_buffer[: delimiter_index + delimiter_length]
            for output in await process_event(event):
                yield output

    async for source_chunk in source:
        event_buffer.extend(source_chunk)
        async for output in process_buffered_events():
            yield output

    if event_buffer:
        for output in await process_event(bytes(event_buffer)):
            yield output
    if not emitted_done:
        for output in await flush_redactors():
            yield output


def _filtered_headers(headers: httpx.Headers) -> dict[str, str]:
    return {
        name: value
        for name, value in headers.items()
        if name.lower() not in _HOP_BY_HOP_HEADERS
    }


def create_app(
    *,
    upstream_url: str | None = None,
    api_key: str | None = None,
    upstream_client: httpx.AsyncClient | None = None,
) -> FastAPI:
    destination = upstream_url or os.environ.get(
        "LLM_UPSTREAM_URL", "https://api.openai.com/v1/chat/completions"
    )
    secret = api_key if api_key is not None else os.environ.get("LLM_API_KEY", "")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if upstream_client is None:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(60.0, connect=10.0)
            ) as client:
                app.state.upstream_client = client
                yield
        else:
            app.state.upstream_client = upstream_client
            yield

    app = FastAPI(title="QuilrAI Streaming PII Guardrail", lifespan=lifespan)
    app.state.upstream_client = upstream_client

    @app.post("/v1/chat/completions")
    async def chat_completions(request: Request) -> Response:
        try:
            body = await request.body()
            payload = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return JSONResponse(
                status_code=400,
                content={"error": {"message": "Request body must be valid JSON."}},
            )
        if not isinstance(payload, dict):
            return JSONResponse(
                status_code=400,
                content={"error": {"message": "Request body must be a JSON object."}},
            )

        headers = {"content-type": request.headers.get("content-type", "application/json")}
        if secret:
            headers["authorization"] = f"Bearer {secret}"
        client: httpx.AsyncClient = request.app.state.upstream_client
        upstream_request = client.build_request(
            "POST",
            destination,
            content=body,
            headers=headers,
        )
        try:
            upstream_response = await client.send(upstream_request, stream=True)
        except httpx.TimeoutException:
            return JSONResponse(
                status_code=504,
                content={"error": {"message": "The LLM provider timed out."}},
            )
        except httpx.RequestError:
            return JSONResponse(
                status_code=502,
                content={"error": {"message": "The LLM provider is unavailable."}},
            )

        response_headers = _filtered_headers(upstream_response.headers)
        is_event_stream = "text/event-stream" in upstream_response.headers.get(
            "content-type", ""
        ).lower()
        if upstream_response.status_code >= 400 or not is_event_stream:
            async def relay_raw_response() -> AsyncIterator[bytes]:
                try:
                    async for chunk in upstream_response.aiter_raw():
                        yield chunk
                finally:
                    await upstream_response.aclose()

            return StreamingResponse(
                relay_raw_response(),
                status_code=upstream_response.status_code,
                headers=response_headers,
                media_type=None,
                background=None,
            )

        async def stream_and_close() -> AsyncIterator[bytes]:
            try:
                async for chunk in _redact_sse_stream(upstream_response.aiter_bytes()):
                    yield chunk
            finally:
                await upstream_response.aclose()

        return StreamingResponse(
            stream_and_close(),
            status_code=upstream_response.status_code,
            headers=response_headers,
            media_type=None,
        )

    return app


app = create_app()
