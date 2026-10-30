import asyncio
import json
import sys
import unittest
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from server import CustomerRecordInput, RefundInput


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class InputValidationTests(unittest.TestCase):
    def test_customer_id_requires_ascii_digits(self) -> None:
        with self.assertRaises(ValidationError):
            CustomerRecordInput.model_validate({"customer_id": "CUST-１２３４５"})

    def test_amount_rejects_non_finite_and_overflowing_values(self) -> None:
        for amount in (float("inf"), float("nan"), 10**400, "1.25", True):
            with self.subTest(amount=type(amount).__name__):
                with self.assertRaises(ValidationError):
                    RefundInput.model_validate(
                        {
                            "customer_id": "CUST-12345",
                            "amount": amount,
                            "reason": "Duplicate purchase",
                        }
                    )


class StdioServerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "server",
            cwd=PROJECT_ROOT,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self.responses: list[dict[str, Any]] = []
        await self._request(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-03-26",
                    "capabilities": {},
                    "clientInfo": {"name": "task1-tests", "version": "0.1.0"},
                },
            }
        )
        await self._notify({"jsonrpc": "2.0", "method": "notifications/initialized"})

    async def asyncTearDown(self) -> None:
        if self.process.stdin is not None:
            self.process.stdin.close()
        try:
            await asyncio.wait_for(self.process.wait(), timeout=5)
        except TimeoutError:
            self.process.kill()
            await self.process.wait()
            self.fail("MCP server did not stop after stdin closed")

        if self.process.stdout is not None:
            trailing_stdout = await self.process.stdout.read()
            self.assertEqual(trailing_stdout, b"", "unexpected non-protocol stdout")
        if self.process.stderr is not None:
            await self.process.stderr.read()

    async def _send(self, message: dict[str, Any]) -> None:
        assert self.process.stdin is not None
        self.process.stdin.write(
            (json.dumps(message, allow_nan=False) + "\n").encode("utf-8")
        )
        await self.process.stdin.drain()

    async def _read_response(self) -> dict[str, Any]:
        assert self.process.stdout is not None
        line = await asyncio.wait_for(self.process.stdout.readline(), timeout=5)
        self.assertTrue(line, "server exited without a JSON-RPC response")
        response = json.loads(line)
        self.assertEqual(response.get("jsonrpc"), "2.0")
        self.responses.append(response)
        return response

    async def _request(self, message: dict[str, Any]) -> dict[str, Any]:
        await self._send(message)
        return await self._read_response()

    async def _notify(self, message: dict[str, Any]) -> None:
        await self._send(message)

    async def test_stdio_tools_and_json_rpc_validation_errors(self) -> None:
        initialization = self.responses[0]
        self.assertIn("result", initialization)

        tools_response = await self._request(
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}
        )
        tools = tools_response["result"]["tools"]
        self.assertEqual(
            {tool["name"] for tool in tools},
            {"get_customer_record", "trigger_refund"},
        )

        customer_response = await self._request(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "get_customer_record",
                    "arguments": {"customer_id": "CUST-12345"},
                },
            }
        )
        self.assertEqual(
            customer_response["result"]["structuredContent"]["customer_id"],
            "CUST-12345",
        )

        refund_response = await self._request(
            {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "trigger_refund",
                    "arguments": {
                        "customer_id": "CUST-12345",
                        "amount": 12.5,
                        "reason": "Duplicate purchase",
                    },
                },
            }
        )
        self.assertEqual(
            refund_response["result"]["structuredContent"]["status"],
            "simulated",
        )

        invalid_calls = [
            {
                "name": "get_customer_record",
                "arguments": {"customer_id": "cust-12345"},
            },
            {
                "name": "get_customer_record",
                "arguments": {"customer_id": "CUST-１２３４５"},
            },
            {
                "name": "get_customer_record",
                "arguments": {
                    "customer_id": "CUST-12345",
                    "unexpected": "field",
                },
            },
            {
                "name": "trigger_refund",
                "arguments": {
                    "customer_id": "CUST-12345",
                    "amount": 0,
                    "reason": "Duplicate purchase",
                },
            },
            {
                "name": "trigger_refund",
                "arguments": {
                    "customer_id": "CUST-12345",
                    "amount": -1,
                    "reason": "Duplicate purchase",
                },
            },
            {
                "name": "trigger_refund",
                "arguments": {
                    "customer_id": "CUST-12345",
                    "amount": True,
                    "reason": "Duplicate purchase",
                },
            },
            {
                "name": "trigger_refund",
                "arguments": {
                    "customer_id": "CUST-12345",
                    "amount": "12.5",
                    "reason": "Duplicate purchase",
                },
            },
            {
                "name": "trigger_refund",
                "arguments": {
                    "customer_id": "CUST-12345",
                    "amount": 1.0,
                    "reason": "Too short",
                },
            },
        ]

        for request_id, call in enumerate(invalid_calls, start=5):
            with self.subTest(call=call):
                response = await self._request(
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "method": "tools/call",
                        "params": call,
                    }
                )
                self.assertEqual(response["error"]["code"], -32602)
                self.assertEqual(response["id"], request_id)
                self.assertNotIn("result", response)

        for response in self.responses:
            self.assertIn("jsonrpc", response)
            self.assertNotIn("Traceback", json.dumps(response))


if __name__ == "__main__":
    unittest.main()
