"""SQLite-backed token limiter and resilient asynchronous LLM router."""

import asyncio
import hashlib
import json
import math
import os
import secrets
import sqlite3
import time
import uuid
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any

import httpx
import tiktoken
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse


WINDOW_SECONDS = 60.0
FALLBACK_TIMEOUT_SECONDS = 3.0
DEFAULT_TOKEN_LIMIT = 50_000
DEFAULT_MAX_COMPLETION_TOKENS = 1_024
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
    "authorization",
    "host",
}


def _tenant_hash(tenant: str) -> str:
    return hashlib.sha256(tenant.encode("utf-8")).hexdigest()


def load_tenant_keys(raw: str | None = None) -> dict[str, str]:
    encoded = os.environ.get("TENANT_API_KEYS", "") if raw is None else raw
    if not encoded:
        return {}
    try:
        config = json.loads(encoded)
    except json.JSONDecodeError as error:
        raise ValueError("TENANT_API_KEYS must contain a JSON object.") from error
    if not isinstance(config, dict) or not config:
        raise ValueError("TENANT_API_KEYS must be a non-empty JSON object.")

    keys: dict[str, str] = {}
    for api_key, tenant in config.items():
        if (
            not isinstance(api_key, str)
            or not api_key.strip()
            or not isinstance(tenant, str)
            or not tenant.strip()
        ):
            raise ValueError("API keys and tenant identifiers must be non-empty strings.")
        keys[api_key] = tenant
    return keys


def resolve_tenant(
    authorization: str | None,
    tenant_keys: Mapping[str, str],
) -> str | None:
    if not authorization:
        return None
    scheme, separator, supplied_key = authorization.partition(" ")
    if (
        not separator
        or scheme.lower() != "bearer"
        or not supplied_key
        or any(character.isspace() for character in supplied_key)
    ):
        return None

    matched_tenant: str | None = None
    for configured_key, tenant in tenant_keys.items():
        if secrets.compare_digest(supplied_key, configured_key):
            matched_tenant = tenant
    return matched_tenant


def estimate_request_tokens(payload: Mapping[str, Any]) -> int:
    model = payload.get("model")
    try:
        encoding = (
            tiktoken.encoding_for_model(model)
            if isinstance(model, str)
            else tiktoken.get_encoding("cl100k_base")
        )
    except KeyError:
        encoding = tiktoken.get_encoding("cl100k_base")

    prompt_tokens = len(
        encoding.encode(
            json.dumps(payload, ensure_ascii=False),
            disallowed_special=(),
        )
    )
    completion_limit = payload.get(
        "max_completion_tokens",
        payload.get("max_tokens", DEFAULT_MAX_COMPLETION_TOKENS),
    )
    return prompt_tokens + completion_limit


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"Invalid JSON constant: {value}")


class SQLiteSlidingWindowLimiter:
    """Atomically reserve token budgets using a persistent SQLite ledger."""

    def __init__(
        self,
        database_path: str,
        *,
        token_limit: int = DEFAULT_TOKEN_LIMIT,
        window_seconds: float = WINDOW_SECONDS,
    ) -> None:
        if token_limit <= 0 or window_seconds <= 0:
            raise ValueError("Token limit and sliding-window duration must be positive.")
        self.database_path = database_path
        self.token_limit = token_limit
        self.window_seconds = window_seconds

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.database_path,
            timeout=10.0,
            isolation_level=None,
        )
        connection.execute("PRAGMA busy_timeout = 10000")
        return connection

    def _initialize_sync(self) -> None:
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS token_reservations (
                    request_id TEXT PRIMARY KEY,
                    tenant_hash TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    tokens INTEGER NOT NULL CHECK(tokens > 0)
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_token_reservations_tenant_time
                ON token_reservations (tenant_hash, created_at)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_token_reservations_time
                ON token_reservations (created_at)
                """
            )
        finally:
            connection.close()

    async def initialize(self) -> None:
        await asyncio.to_thread(self._initialize_sync)

    def _reserve_sync(
        self,
        tenant_hash: str,
        tokens: int,
        now: float,
    ) -> tuple[bool, int, int]:
        if tokens <= 0:
            raise ValueError("Token reservation must be positive.")

        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            try:
                cutoff = now - self.window_seconds
                connection.execute(
                    "DELETE FROM token_reservations WHERE created_at <= ?",
                    (cutoff,),
                )
                rows = connection.execute(
                    """
                    SELECT created_at, tokens
                    FROM token_reservations
                    WHERE tenant_hash = ?
                    ORDER BY created_at ASC, request_id ASC
                    """,
                    (tenant_hash,),
                ).fetchall()
                current = sum(row[1] for row in rows)

                if tokens > self.token_limit:
                    connection.execute("COMMIT")
                    return False, current, 0

                if current + tokens > self.token_limit:
                    tokens_to_expire = current + tokens - self.token_limit
                    expired_tokens = 0
                    retry_after = math.ceil(self.window_seconds)
                    for created_at, reserved_tokens in rows:
                        expired_tokens += reserved_tokens
                        if expired_tokens >= tokens_to_expire:
                            retry_after = max(
                                1,
                                math.ceil(created_at + self.window_seconds - now),
                            )
                            break
                    connection.execute("COMMIT")
                    return False, current, retry_after

                connection.execute(
                    """
                    INSERT INTO token_reservations
                    (request_id, tenant_hash, created_at, tokens)
                    VALUES (?, ?, ?, ?)
                    """,
                    (uuid.uuid4().hex, tenant_hash, now, tokens),
                )
                connection.execute("COMMIT")
                return True, current + tokens, 0
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        finally:
            connection.close()

    async def reserve(
        self,
        tenant: str,
        tokens: int,
        *,
        now: float | None = None,
    ) -> tuple[bool, int, int]:
        return await asyncio.to_thread(
            self._reserve_sync,
            _tenant_hash(tenant),
            tokens,
            time.time() if now is None else now,
        )


def _safe_error(
    code: str,
    message: str,
    request_id: str,
    *,
    status_code: int,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    safe_headers: dict[str, str] = {}
    if headers and "retry-after" in headers:
        value = headers["retry-after"]
        if value.isdigit() and int(value) > 0:
            safe_headers["Retry-After"] = value
    return JSONResponse(
        status_code=status_code,
        headers=safe_headers,
        content={
            "error": {
                "code": code,
                "message": message,
                "request_id": request_id,
            }
        },
    )


def _relay_headers(headers: httpx.Headers) -> dict[str, str]:
    return {
        name: value
        for name, value in headers.items()
        if name.lower() not in _HOP_BY_HOP_HEADERS
    }


def _completion_limit(payload: Mapping[str, Any]) -> int:
    value = payload.get(
        "max_completion_tokens",
        payload.get("max_tokens", DEFAULT_MAX_COMPLETION_TOKENS),
    )
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("Completion token limit must be a positive integer.")
    return value


def create_app(
    *,
    tenant_keys: Mapping[str, str] | None = None,
    primary_url: str | None = None,
    backup_url: str | None = None,
    primary_api_key: str | None = None,
    backup_api_key: str | None = None,
    database_path: str | None = None,
    token_limit: int = DEFAULT_TOKEN_LIMIT,
    upstream_timeout: float = FALLBACK_TIMEOUT_SECONDS,
    upstream_client: httpx.AsyncClient | None = None,
) -> FastAPI:
    configured_tenants = dict(load_tenant_keys() if tenant_keys is None else tenant_keys)
    if upstream_timeout <= 0:
        raise ValueError("Upstream timeout must be positive.")

    primary_destination = primary_url or os.environ.get(
        "LLM_PRIMARY_URL", "http://127.0.0.1:9001/v1/chat/completions"
    )
    backup_destination = backup_url or os.environ.get(
        "LLM_BACKUP_URL", "http://127.0.0.1:9002/v1/chat/completions"
    )
    primary_secret = (
        primary_api_key
        if primary_api_key is not None
        else os.environ.get("LLM_PRIMARY_API_KEY", "")
    )
    backup_secret = (
        backup_api_key
        if backup_api_key is not None
        else os.environ.get("LLM_BACKUP_API_KEY", "")
    )
    db_path = database_path or os.environ.get(
        "RATE_LIMIT_DB_PATH", "gateway_usage.sqlite3"
    )
    limiter = SQLiteSlidingWindowLimiter(db_path, token_limit=token_limit)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await limiter.initialize()
        if upstream_client is None:
            async with httpx.AsyncClient(timeout=None) as client:
                app.state.upstream_client = client
                yield
        else:
            app.state.upstream_client = upstream_client
            yield

    app = FastAPI(title="QuilrAI LLM Fallback Router", lifespan=lifespan)
    app.state.upstream_client = upstream_client
    app.state.limiter = limiter

    async def send_provider(
        client: httpx.AsyncClient,
        url: str,
        api_key: str,
        body: bytes,
        content_type: str,
    ) -> httpx.Response:
        headers = {"content-type": content_type}
        if api_key:
            headers["authorization"] = f"Bearer {api_key}"
        request = client.build_request(
            "POST",
            url,
            content=body,
            headers=headers,
            timeout=upstream_timeout,
        )
        return await client.send(request, stream=True)

    async def open_provider_stream(
        client: httpx.AsyncClient,
        url: str,
        api_key: str,
        body: bytes,
        content_type: str,
    ) -> tuple[httpx.Response, AsyncIterator[bytes] | None, bytes | None]:
        response: httpx.Response | None = None
        try:
            async with asyncio.timeout(upstream_timeout):
                response = await send_provider(
                    client,
                    url,
                    api_key,
                    body,
                    content_type,
                )
                if response.status_code >= 400:
                    return response, None, None
                stream = response.aiter_bytes()
                try:
                    first_chunk = await anext(stream)
                except StopAsyncIteration:
                    first_chunk = None
                return response, stream, first_chunk
        except BaseException:
            if response is not None:
                await response.aclose()
            raise

    @app.post("/v1/chat/completions")
    async def route_completion(request: Request) -> Response:
        request_id = uuid.uuid4().hex
        try:
            body = await request.body()
            payload = json.loads(body, parse_constant=_reject_json_constant)
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
            return _safe_error(
                "invalid_request",
                "Request body must be valid JSON.",
                request_id,
                status_code=400,
            )
        if (
            not isinstance(payload, dict)
            or not isinstance(payload.get("messages"), list)
            or not payload["messages"]
        ):
            return _safe_error(
                "invalid_request",
                "A JSON object with a non-empty messages array is required.",
                request_id,
                status_code=400,
            )
        try:
            _completion_limit(payload)
        except ValueError:
            return _safe_error(
                "invalid_request",
                "max_tokens or max_completion_tokens must be a positive integer.",
                request_id,
                status_code=400,
            )

        tenant = resolve_tenant(request.headers.get("authorization"), configured_tenants)
        if tenant is None:
            return _safe_error(
                "unauthorized",
                "A valid tenant API key is required.",
                request_id,
                status_code=401,
            )
        try:
            requested_tokens = estimate_request_tokens(payload)
        except (TypeError, ValueError):
            return _safe_error(
                "invalid_request",
                "Completion request contains invalid tokenizable data.",
                request_id,
                status_code=400,
            )
        allowed, used_tokens, retry_after = await limiter.reserve(
            tenant,
            requested_tokens,
        )
        if not allowed:
            return _safe_error(
                "rate_limit_exceeded",
                "Tenant token budget exceeded for the rolling 60-second window.",
                request_id,
                status_code=429,
                headers={"retry-after": str(retry_after)},
            )

        client: httpx.AsyncClient = request.app.state.upstream_client
        content_type = request.headers.get("content-type", "application/json")
        primary_response: httpx.Response | None = None
        primary_stream: AsyncIterator[bytes] | None = None
        primary_first_chunk: bytes | None = None
        primary_failed_over = False
        try:
            (
                primary_response,
                primary_stream,
                primary_first_chunk,
            ) = await open_provider_stream(
                client,
                primary_destination,
                primary_secret,
                body,
                content_type,
            )
            if primary_response.status_code == 429:
                primary_failed_over = True
                await primary_response.aclose()
            elif primary_response.status_code >= 400:
                await primary_response.aclose()
                return _safe_error(
                    "upstream_error",
                    "Primary LLM provider could not complete the request.",
                    request_id,
                    status_code=502,
                )
            else:
                return _stream_provider_response(
                    primary_response,
                    primary_stream,
                    primary_first_chunk,
                )
        except (httpx.TimeoutException, TimeoutError):
            primary_failed_over = True
        except httpx.RequestError:
            return _safe_error(
                "upstream_unavailable",
                "Primary LLM provider is unavailable.",
                request_id,
                status_code=502,
            )

        if not primary_failed_over:
            return _safe_error(
                "upstream_unavailable",
                "LLM provider request failed.",
                request_id,
                status_code=502,
            )

        try:
            (
                backup_response,
                backup_stream,
                backup_first_chunk,
            ) = await open_provider_stream(
                client,
                backup_destination,
                backup_secret,
                body,
                content_type,
            )
        except (httpx.TimeoutException, TimeoutError):
            return _safe_error(
                "upstream_timeout",
                "Primary and backup LLM providers timed out.",
                request_id,
                status_code=504,
            )
        except httpx.RequestError:
            return _safe_error(
                "upstream_unavailable",
                "Backup LLM provider is unavailable.",
                request_id,
                status_code=502,
            )

        if backup_response.status_code >= 400:
            status_code = (
                503 if backup_response.status_code == 429 else 502
            )
            code = (
                "upstream_rate_limited"
                if backup_response.status_code == 429
                else "upstream_error"
            )
            await backup_response.aclose()
            return _safe_error(
                code,
                "Backup LLM provider could not complete the request.",
                request_id,
                status_code=status_code,
                headers=backup_response.headers,
            )
        return _stream_provider_response(
            backup_response,
            backup_stream,
            backup_first_chunk,
        )

    return app


def _stream_provider_response(
    upstream_response: httpx.Response,
    upstream_stream: AsyncIterator[bytes] | None,
    first_chunk: bytes | None,
) -> StreamingResponse:
    async def stream_and_close() -> AsyncIterator[bytes]:
        try:
            if first_chunk is not None:
                yield first_chunk
            if upstream_stream is not None:
                async for chunk in upstream_stream:
                    yield chunk
        finally:
            await upstream_response.aclose()

    return StreamingResponse(
        stream_and_close(),
        status_code=upstream_response.status_code,
        headers=_relay_headers(upstream_response.headers),
        media_type=None,
    )


app = create_app()
