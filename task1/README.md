# QuilrAI Assessment - Task 1

A runnable Python MCP server exposing `get_customer_record` and `trigger_refund` over stdio. It uses the official MCP Python SDK and Pydantic for strict input validation.

## Requirements

- Python 3.11+

## Setup and run

From this directory:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e .
python -m server
```

The server communicates over stdin/stdout using MCP's stdio transport. Connect using an MCP client or the MCP Inspector; do not type shell commands into the running server's stdin.

Run the integration tests with:

```powershell
python -m unittest discover -s tests -v
```

## Tools and validation

| Tool | Inputs | Behavior |
|---|---|---|
| `get_customer_record` | `customer_id` | Returns a deterministic demo customer record. |
| `trigger_refund` | `customer_id`, `amount`, `reason` | Returns a simulated refund result; it does not move money or call a payment provider. |

- Customer IDs must match `CUST-` followed by exactly five ASCII digits (for example, `CUST-12345`).
- Amount must be a finite JSON number greater than zero. Booleans, strings, zero, negative values, and infinities are rejected.
- Reason must contain at least 10 characters.
- Unknown fields are rejected.
- Invalid tool arguments return JSON-RPC `Invalid params` (`-32602`), with field-level validation details that omit submitted values.

The low-level MCP request handler is intentional: the SDK's convenience tool decorator represents schema-validation failures as tool results, whereas this assessment requires a JSON-RPC error. Valid calls still return normal MCP tool results.

## Stdio and logging

Stdout is reserved for MCP JSON-RPC messages. Application logging is configured for stderr, the server does not print diagnostics to stdout. The subprocess integration test initializes the server, lists and calls both tools, checks invalid calls' JSON-RPC error codes, and rejects any extra stdout.
