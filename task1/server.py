"""Strictly validated MCP tools served over stdio."""

import json
import logging
import math
import sys
from typing import Any

import anyio
from mcp import types
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.shared.exceptions import McpError
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator


class CustomerRecordInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    customer_id: str = Field(pattern=r"^CUST-[0-9]{5}$")


class RefundInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    customer_id: str = Field(pattern=r"^CUST-[0-9]{5}$")
    amount: float = Field(gt=0, allow_inf_nan=False)
    reason: str = Field(min_length=10)

    @field_validator("amount", mode="before")
    @classmethod
    def validate_amount(cls, value: Any) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("amount must be a finite positive number")

        try:
            amount = float(value)
        except OverflowError as error:
            raise ValueError("amount must be a finite positive number") from error

        if not math.isfinite(amount) or amount <= 0:
            raise ValueError("amount must be a finite positive number")
        return amount

server = Server("quilrai-task1", version="0.1.0")


@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="get_customer_record",
            description="Look up a demo customer record by customer ID.",
            inputSchema=CustomerRecordInput.model_json_schema(),
        ),
        types.Tool(
            name="trigger_refund",
            description="Simulate a refund request for a customer.",
            inputSchema=RefundInput.model_json_schema(),
        ),
    ]


def _tool_error(message: str) -> types.ServerResult:
    return types.ServerResult(
        types.CallToolResult(
            content=[types.TextContent(type="text", text=message)],
            isError=True,
        )
    )


def _invalid_params(error: ValidationError) -> McpError:
    issues = [
        {"field": ".".join(str(part) for part in issue["loc"]), "message": issue["msg"]}
        for issue in error.errors(include_input=False)
    ]
    return McpError(
        types.ErrorData(
            code=types.INVALID_PARAMS,
            message="Invalid tool arguments.",
            data={"issues": issues},
        )
    )


async def _handle_call_tool(request: types.CallToolRequest) -> types.ServerResult:
    name = request.params.name
    arguments = request.params.arguments

    try:
        if name == "get_customer_record":
            parsed = CustomerRecordInput.model_validate(arguments)
            result = {
                "customer_id": parsed.customer_id,
                "name": "Demo Customer",
                "status": "active",
            }
        elif name == "trigger_refund":
            parsed = RefundInput.model_validate(arguments)
            result = {
                "status": "simulated",
                "customer_id": parsed.customer_id,
                "amount": parsed.amount,
                "reason": parsed.reason,
            }
        else:
            return _tool_error(f"Unknown tool: {name}")
    except ValidationError as error:
        raise _invalid_params(error) from error

    return types.ServerResult(
        types.CallToolResult(
            content=[
                types.TextContent(
                    type="text",
                    text=json.dumps(result, ensure_ascii=False, sort_keys=True),
                )
            ],
            structuredContent=result,
        )
    )


# The SDK's call_tool decorator turns validation exceptions into isError tool
# results. Registering this low-level handler lets invalid arguments use the
# JSON-RPC Invalid params error required by this assessment.
server.request_handlers[types.CallToolRequest] = _handle_call_tool


async def _run() -> None:
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


def main() -> None:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    anyio.run(_run)


if __name__ == "__main__":
    main()
