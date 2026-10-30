import json
import unittest
from typing import Any

import httpx

from gateway import create_app, load_token_roles
from mock_server import create_mock_app


class GatewayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.mock_app = create_mock_app()
        self.downstream_client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.mock_app),
            base_url="http://downstream",
        )
        self.gateway_app = create_app(
            token_roles={
                "admin-test-token": "admin",
                "viewer-test-token": "viewer",
            },
            upstream_url="http://downstream/mcp",
            upstream_client=self.downstream_client,
        )
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.gateway_app),
            base_url="http://gateway",
        )

    async def asyncTearDown(self) -> None:
        await self.client.aclose()
        await self.downstream_client.aclose()

    async def post_rpc(
        self,
        message: dict[str, Any] | list[Any],
        *,
        token: str = "viewer-test-token",
        content: bytes | None = None,
    ) -> httpx.Response:
        headers = {"Authorization": f"Bearer {token}"}
        if content is not None:
            return await self.client.post("/mcp", headers=headers, content=content)
        return await self.client.post("/mcp", headers=headers, json=message)

    async def test_tools_list_is_forwarded_without_rewriting(self) -> None:
        body = b'{ "jsonrpc" : "2.0", "id" : "list-1", "method" : "tools/list", "params" : {} }'
        message = json.loads(body)
        response = await self.post_rpc(message, content=body)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["id"], "list-1")
        self.assertEqual(self.mock_app.state.received_messages, [message])
        self.assertEqual(self.mock_app.state.received_bodies, [body])
        self.assertEqual(self.mock_app.state.received_authorization_headers, [None])

    async def test_viewer_admin_call_is_denied_without_downstream_call(self) -> None:
        message = {
            "jsonrpc": "2.0",
            "id": 42,
            "method": "tools/call",
            "params": {"name": "admin_reset_key", "arguments": {}},
        }
        response = await self.post_rpc(message)

        self.assertEqual(
            response.json(),
            {
                "jsonrpc": "2.0",
                "id": 42,
                "error": {"code": -32001, "message": "Unauthorized Tool Call"},
            },
        )
        self.assertEqual(self.mock_app.state.received_messages, [])

    async def test_admin_can_call_admin_tool(self) -> None:
        message = {
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/call",
            "params": {"name": "admin_reset_key", "arguments": {}},
        }
        response = await self.post_rpc(message, token="admin-test-token")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["id"], 7)
        self.assertEqual(self.mock_app.state.received_messages, [message])

    async def test_viewer_can_call_non_admin_tool(self) -> None:
        message = {
            "jsonrpc": "2.0",
            "id": 8,
            "method": "tools/call",
            "params": {"name": "lookup_customer", "arguments": {}},
        }
        response = await self.post_rpc(message)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["id"], 8)
        self.assertEqual(self.mock_app.state.received_messages, [message])

    async def test_missing_or_invalid_token_is_rejected(self) -> None:
        message = {"jsonrpc": "2.0", "id": 9, "method": "tools/list"}
        for headers in ({}, {"Authorization": "Bearer unknown-token"}):
            with self.subTest(headers=headers):
                response = await self.client.post("/mcp", headers=headers, json=message)
                self.assertEqual(response.status_code, 401)
                self.assertEqual(response.json()["error"]["code"], -32001)
        self.assertEqual(self.mock_app.state.received_messages, [])

    async def test_json_constant_nan_is_rejected_as_parse_error(self) -> None:
        response = await self.post_rpc(
            {},
            content=b'{"jsonrpc":"2.0","id":NaN,"method":"tools/list"}',
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"]["code"], -32700)

    async def test_malformed_tool_call_params_are_rejected_without_forwarding(self) -> None:
        message = {
            "jsonrpc": "2.0",
            "id": 13,
            "method": "tools/call",
            "params": {"name": 42},
        }
        response = await self.post_rpc(message)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["error"]["code"], -32602)
        self.assertEqual(self.mock_app.state.received_messages, [])

    async def test_malformed_json_and_invalid_json_rpc_are_rejected(self) -> None:
        malformed = await self.post_rpc(
            {},
            content=b'{"jsonrpc":',
        )
        self.assertEqual(malformed.status_code, 400)
        self.assertEqual(malformed.json()["error"]["code"], -32700)

        invalid = await self.post_rpc(
            {"jsonrpc": "1.0", "id": 10, "method": "tools/list"}
        )
        self.assertEqual(invalid.status_code, 400)
        self.assertEqual(invalid.json()["error"]["code"], -32600)
        self.assertEqual(self.mock_app.state.received_messages, [])

    async def test_batch_keeps_allowed_calls_and_intercepts_admin_call(self) -> None:
        batch = [
            {
                "jsonrpc": "2.0",
                "id": 11,
                "method": "tools/call",
                "params": {"name": "admin_reset_key", "arguments": {}},
            },
            {
                "jsonrpc": "2.0",
                "id": 12,
                "method": "tools/list",
            },
        ]
        response = await self.post_rpc(batch)

        self.assertEqual(response.status_code, 200)
        replies = {item["id"]: item for item in response.json()}
        self.assertEqual(replies[11]["error"]["code"], -32001)
        self.assertIn("result", replies[12])
        self.assertEqual(self.mock_app.state.received_messages, [batch[1]])

    async def test_upstream_connection_failure_is_sanitized(self) -> None:
        def fail_upstream(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("private upstream hostname", request=request)

        failing_client = httpx.AsyncClient(
            transport=httpx.MockTransport(fail_upstream),
            base_url="http://downstream",
        )
        failing_app = create_app(
            token_roles={"viewer-test-token": "viewer"},
            upstream_url="http://downstream/mcp",
            upstream_client=failing_client,
        )
        failing_gateway = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=failing_app),
            base_url="http://gateway",
        )
        try:
            response = await failing_gateway.post(
                "/mcp",
                headers={"Authorization": "Bearer viewer-test-token"},
                json={"jsonrpc": "2.0", "id": 14, "method": "tools/list"},
            )
        finally:
            await failing_gateway.aclose()
            await failing_client.aclose()

        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.json()["error"]["code"], -32000)
        self.assertEqual(
            response.json()["error"]["message"],
            "Downstream MCP server is unavailable.",
        )
        self.assertNotIn("private upstream hostname", response.text)


class TokenConfigurationTests(unittest.TestCase):
    def test_token_configuration_requires_supported_roles(self) -> None:
        self.assertEqual(load_token_roles('{"admin-key":"admin"}'), {"admin-key": "admin"})
        with self.assertRaises(ValueError):
            load_token_roles('{"viewer-key":"owner"}')
        with self.assertRaises(ValueError):
            load_token_roles('{"viewer-key":[]} ')
        with self.assertRaises(ValueError):
            load_token_roles("[]")


if __name__ == "__main__":
    unittest.main()
