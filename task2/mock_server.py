"""Small downstream JSON-RPC server for demonstrating the gateway."""

from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response


def _handle(message: Any) -> dict[str, Any] | None:
    if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
        return {
            "jsonrpc": "2.0",
            "id": None,
            "error": {"code": -32600, "message": "Invalid Request"},
        }

    request_id = message.get("id")
    if "id" not in message:
        return None

    method = message.get("method")
    if method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "tools": [
                    {"name": "lookup_customer", "description": "Look up a demo customer."},
                    {"name": "admin_reset_key", "description": "Reset a demo API key."},
                ]
            },
        }

    if method == "tools/call":
        params = message.get("params")
        if not isinstance(params, dict) or not isinstance(params.get("name"), str):
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32602, "message": "Invalid params"},
            }
        name = params["name"]
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {"content": [{"type": "text", "text": f"Mock tool executed: {name}"}]},
        }

    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "error": {"code": -32601, "message": "Method not found"},
    }


def create_mock_app() -> FastAPI:
    app = FastAPI(title="QuilrAI Mock MCP Server")
    app.state.received_messages = []
    app.state.received_bodies = []
    app.state.received_authorization_headers = []

    @app.post("/mcp")
    async def handle_mcp(request: Request) -> Response:
        raw_body = await request.body()
        payload = await request.json()
        messages = payload if isinstance(payload, list) else [payload]
        app.state.received_messages.extend(messages)
        app.state.received_bodies.append(raw_body)
        app.state.received_authorization_headers.append(
            request.headers.get("authorization")
        )
        responses = [response for message in messages if (response := _handle(message))]
        if not responses:
            return Response(status_code=204)
        content: Any = responses if isinstance(payload, list) else responses[0]
        return JSONResponse(content=content)

    return app


app = create_mock_app()
