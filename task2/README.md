# QuilrAI Assessment — Task 2

A lightweight HTTP/JSON-RPC MCP gateway that authorizes tool calls by the role assigned to a bearer token. A separate mock MCP server is included so the proxy can be exercised locally without credentials to an external service.

## Requirements

- Python 3.11+

## Setup

From this directory in PowerShell:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e .
$env:GATEWAY_TOKENS = '{"demo-admin-token":"admin","demo-viewer-token":"viewer"}'
```

These are local demonstration credentials, not production secrets. For deployment, provide unique, high-entropy tokens through a secret manager or environment configuration.

## Run the mock server and gateway

Open two terminals in this directory. Start the downstream mock server first:

```powershell
.\.venv\Scripts\python.exe -m uvicorn mock_server:app --host 127.0.0.1 --port 8001
```

Then start the gateway:

```powershell
.\.venv\Scripts\python.exe -m uvicorn gateway:app --host 127.0.0.1 --port 8000
```

The gateway listens on `POST /mcp`; `MCP_UPSTREAM_URL` overrides the default downstream URL `http://127.0.0.1:8001/mcp`.

## Try it

List tools as a viewer:

```powershell
$body = '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'
Invoke-RestMethod -Uri http://127.0.0.1:8000/mcp -Method Post -ContentType 'application/json' -Headers @{ Authorization = 'Bearer demo-viewer-token' } -Body $body
```

Try a privileged tool as a viewer (gateway returns JSON-RPC `-32001` and does not forward the call):

```powershell
$body = '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"admin_reset_key","arguments":{}}}'
Invoke-RestMethod -Uri http://127.0.0.1:8000/mcp -Method Post -ContentType 'application/json' -Headers @{ Authorization = 'Bearer demo-viewer-token' } -Body $body
```

Repeat with `Bearer demo-admin-token` to see the mock downstream server receive the admin call.

## Security and protocol behavior

- Tokens are opaque strings mapped to `admin` or `viewer` using the `GATEWAY_TOKENS` JSON environment variable; the gateway does not trust a role string supplied directly by a caller.
- The proxy rejects missing/unknown bearer credentials, forwards `tools/list`, and permits normal tool calls to both roles.
- A `tools/call` whose string `params.name` starts with `admin_` requires the `admin` role. Denied calls return JSON-RPC `-32001` directly without contacting the downstream server.
- Single JSON-RPC request bodies are forwarded byte-for-byte; valid batches retain allowed messages and return intercepted denials alongside downstream responses.
- Malformed JSON and invalid JSON-RPC envelopes return `-32700` and `-32600`, respectively. Upstream timeouts and connection failures use sanitized gateway errors rather than exception details.
- Bearer authorization and hop-by-hop HTTP headers are not forwarded downstream.

Run the automated gateway and mock-server integration tests:

```powershell
.\.venv\Scripts\python.exe -m unittest -v tests.test_gateway
```
