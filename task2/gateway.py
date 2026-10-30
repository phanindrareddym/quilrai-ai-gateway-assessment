"""Role-aware HTTP JSON-RPC proxy for a downstream MCP server."""

import json
import os
import secrets
from collections.abc import Mapping
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response


_HOP_BY_HOP_HEADERS = {
    b"connection",
    b"keep-alive",
    b"proxy-authenticate",
    b"proxy-authorization",
    b"te",
    b"trailer",
    b"transfer-encoding",
    b"upgrade",
    b"host",
    b"content-length",
    b"authorization",
}


def load_token_roles(raw: str | None = None) -> dict[str, str]:
    """Load opaque bearer tokens mapped to the only supported roles."""
    encoded = os.environ.get("GATEWAY_TOKENS", "") if raw is None else raw
    if not encoded:
        return {}

    try:
        config = json.loads(encoded)
    except json.JSONDecodeError as error:
        raise ValueError("GATEWAY_TOKENS must contain a JSON object.") from error

    if not isinstance(config, dict) or not config:
        raise ValueError("GATEWAY_TOKENS must be a non-empty JSON object.")

    roles: dict[str, str] = {}
    for token, role in config.items():
        if not isinstance(token, str) or not token.strip():
            raise ValueError("Configured bearer tokens must be non-empty strings.")
        if not isinstance(role, str) or role not in {"admin", "viewer"}:
            raise ValueError("Configured token roles must be 'admin' or 'viewer'.")
        roles[token] = role
    return roles


def _extract_bearer_role(authorization: str | None, token_roles: Mapping[str, str]) -> str | None:
    if not authorization:
        return None

    scheme, separator, token = authorization.partition(" ")
    if not separator or scheme.lower() != "bearer" or not token or any(
        character.isspace() for character in token
    ):
        return None

    matched_role: str | None = None
    for configured_token, role in token_roles.items():
        if secrets.compare_digest(token, configured_token):
            matched_role = role
    return matched_role


def _request_id(message: Any) -> Any:
    if not isinstance(message, dict):
        return None
    request_id = message.get("id")
    if request_id is None or isinstance(request_id, (str, int, float)) and not isinstance(
        request_id, bool
    ):
        return request_id
    return None


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"Invalid JSON constant: {value}")


def _valid_request(message: Any) -> bool:
    if not isinstance(message, dict):
        return False
    if message.get("jsonrpc") != "2.0":
        return False
    if not isinstance(message.get("method"), str) or not message["method"]:
        return False

    if "id" in message:
        request_id = message["id"]
        if isinstance(request_id, bool) or not isinstance(
            request_id, (str, int, float, type(None))
        ):
            return False
        if isinstance(request_id, float) and not (
            request_id == request_id and abs(request_id) != float("inf")
        ):
            return False

    params = message.get("params")
    return "params" not in message or isinstance(params, (dict, list))


def _rpc_error(
    request_id: Any,
    code: int,
    message: str,
    *,
    status_code: int = 200,
    data: Any = None,
) -> JSONResponse:
    error: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return JSONResponse(
        status_code=status_code,
        content={"jsonrpc": "2.0", "id": request_id, "error": error},
    )


def _notification(message: Any) -> bool:
    return isinstance(message, dict) and "id" not in message


def _is_admin_tool_call(message: Any) -> bool:
    if not isinstance(message, dict) or message.get("method") != "tools/call":
        return False
    params = message.get("params")
    if not isinstance(params, dict):
        return False
    tool_name = params.get("name")
    return isinstance(tool_name, str) and tool_name.startswith("admin_")


def _invalid_tool_call(message: Any) -> bool:
    if not isinstance(message, dict) or message.get("method") != "tools/call":
        return False
    params = message.get("params")
    return not isinstance(params, dict) or not isinstance(params.get("name"), str)


def _rpc_message_error(message: Any) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": _request_id(message),
        "error": {"code": -32600, "message": "Invalid Request"},
    }


def _response_headers(headers: httpx.Headers) -> dict[str, str]:
    return {
        name: value
        for name, value in headers.items()
        if name.lower().encode("ascii") not in _HOP_BY_HOP_HEADERS
    }


def create_app(
    *,
    token_roles: Mapping[str, str] | None = None,
    upstream_url: str | None = None,
    upstream_client: httpx.AsyncClient | None = None,
) -> FastAPI:
    """Create the gateway, allowing upstream clients to be injected in tests."""
    roles = dict(load_token_roles() if token_roles is None else token_roles)
    destination = upstream_url or os.environ.get(
        "MCP_UPSTREAM_URL", "http://127.0.0.1:8001/mcp"
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if upstream_client is None:
            async with httpx.AsyncClient(timeout=10.0) as client:
                app.state.upstream_client = client
                yield
        else:
            app.state.upstream_client = upstream_client
            yield

    app = FastAPI(title="QuilrAI MCP Security Gateway", lifespan=lifespan)
    app.state.upstream_client = upstream_client

    async def send_upstream(request: Request, body: bytes) -> httpx.Response:
        forwarded_headers = [
            (name.decode("latin-1"), value.decode("latin-1"))
            for name, value in request.headers.raw
            if name.lower() not in _HOP_BY_HOP_HEADERS
        ]
        return await request.app.state.upstream_client.request(
            method=request.method,
            url=destination,
            content=body,
            headers=forwarded_headers,
        )

    async def relay(
        request: Request,
        body: bytes,
        request_ids: list[Any],
        *,
        is_batch: bool,
    ) -> Response:
        try:
            upstream_response = await send_upstream(request, body)
        except httpx.TimeoutException:
            if not is_batch and len(request_ids) == 1:
                return _rpc_error(
                    request_ids[0],
                    -32000,
                    "Downstream MCP server timed out.",
                    status_code=504,
                )
            if not request_ids:
                return Response(status_code=204)
            return JSONResponse(
                status_code=504,
                content=[
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {
                            "code": -32000,
                            "message": "Downstream MCP server timed out.",
                        },
                    }
                    for request_id in request_ids
                    if request_id is not None
                ],
            )
        except httpx.RequestError:
            if not is_batch and len(request_ids) == 1:
                return _rpc_error(
                    request_ids[0],
                    -32000,
                    "Downstream MCP server is unavailable.",
                    status_code=502,
                )
            if not request_ids:
                return Response(status_code=204)
            return JSONResponse(
                status_code=502,
                content=[
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {
                            "code": -32000,
                            "message": "Downstream MCP server is unavailable.",
                        },
                    }
                    for request_id in request_ids
                    if request_id is not None
                ],
            )

        return Response(
            content=upstream_response.content,
            status_code=upstream_response.status_code,
            headers=_response_headers(upstream_response.headers),
        )

    @app.post("/mcp")
    async def proxy(request: Request) -> Response:
        body = await request.body()
        try:
            payload = json.loads(
                body,
                parse_constant=_reject_json_constant,
            )
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
            return _rpc_error(None, -32700, "Parse error", status_code=400)

        is_batch = isinstance(payload, list)
        messages = payload if is_batch else [payload]
        if is_batch and not messages:
            return _rpc_error(None, -32600, "Invalid Request", status_code=400)

        invalid_responses: list[dict[str, Any]] = []
        valid_messages: list[Any] = []
        for message in messages:
            if _valid_request(message):
                valid_messages.append(message)
            else:
                invalid_responses.append(_rpc_message_error(message))

        if not is_batch and invalid_responses:
            invalid = invalid_responses[0]
            return JSONResponse(status_code=400, content=invalid)

        if not valid_messages:
            return JSONResponse(status_code=200, content=invalid_responses)

        role = _extract_bearer_role(
            request.headers.get("authorization"),
            roles,
        )
        if role is None:
            auth_errors = [
                {
                    "jsonrpc": "2.0",
                    "id": _request_id(message),
                    "error": {"code": -32001, "message": "Unauthorized"},
                }
                for message in valid_messages
                if not _notification(message)
            ]
            if not is_batch:
                if auth_errors:
                    return JSONResponse(status_code=401, content=auth_errors[0])
                return Response(status_code=204)
            responses = invalid_responses + auth_errors
            if not responses:
                return Response(status_code=204)
            return JSONResponse(status_code=401, content=responses)

        invalid_tool_calls = [
            message for message in valid_messages if _invalid_tool_call(message)
        ]
        invalid_tool_responses = [
            {
                "jsonrpc": "2.0",
                "id": _request_id(message),
                "error": {"code": -32602, "message": "Invalid params"},
            }
            for message in invalid_tool_calls
            if not _notification(message)
        ]
        valid_messages = [
            message for message in valid_messages if message not in invalid_tool_calls
        ]
        if not is_batch and invalid_tool_calls:
            if invalid_tool_responses:
                return JSONResponse(status_code=200, content=invalid_tool_responses[0])
            return Response(status_code=204)

        denied = [
            message
            for message in valid_messages
            if _is_admin_tool_call(message) and role != "admin"
        ]
        denied_responses = [
            {
                "jsonrpc": "2.0",
                "id": _request_id(message),
                "error": {"code": -32001, "message": "Unauthorized Tool Call"},
            }
            for message in denied
            if not _notification(message)
        ]
        forwarded = [message for message in valid_messages if message not in denied]

        if not is_batch and denied:
            if denied_responses:
                return JSONResponse(status_code=200, content=denied_responses[0])
            return Response(status_code=204)

        if not forwarded:
            responses = invalid_responses + invalid_tool_responses + denied_responses
            if not responses:
                return Response(status_code=204)
            return JSONResponse(status_code=200, content=responses)

        request_ids = [
            _request_id(message)
            for message in forwarded
            if not _notification(message)
        ]
        if is_batch:
            forwarded_body = json.dumps(forwarded, separators=(",", ":")).encode()
        else:
            # Preserve the original wire representation for transparent proxying.
            forwarded_body = body
        upstream = await relay(
            request,
            forwarded_body,
            request_ids,
            is_batch=is_batch,
        )

        if not is_batch:
            return upstream

        upstream_responses: list[Any] = []
        if upstream.body:
            try:
                decoded_upstream = json.loads(upstream.body)
            except (json.JSONDecodeError, UnicodeDecodeError):
                return upstream
            upstream_responses = (
                decoded_upstream if isinstance(decoded_upstream, list) else [decoded_upstream]
            )
        combined_responses = (
            invalid_responses
            + invalid_tool_responses
            + denied_responses
            + upstream_responses
        )
        if not combined_responses:
            return Response(status_code=204)
        combined_status = 200 if upstream.status_code == 204 else upstream.status_code
        return JSONResponse(
            status_code=combined_status,
            content=combined_responses,
            headers={
                name: value
                for name, value in _response_headers(upstream.headers).items()
                if name.lower() in {"mcp-session-id", "mcp-protocol-version"}
            },
        )

    return app


app = create_app()
