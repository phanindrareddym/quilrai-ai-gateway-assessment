# QuilrAI Assessment — Task 4

An asynchronous completion router with a persistent SQLite token-budget ledger and automatic failover from a primary LLM provider to a backup provider.

## Requirements

- Python 3.11+
- SQLite (included with Python)

## Setup

From this directory in PowerShell:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e .
$env:TENANT_API_KEYS = '{"demo-tenant-key":"tenant-a"}'
$env:LLM_PRIMARY_URL = 'https://primary.example/v1/chat/completions'
$env:LLM_BACKUP_URL = 'https://backup.example/v1/chat/completions'
$env:LLM_PRIMARY_API_KEY = 'primary-provider-key'
$env:LLM_BACKUP_API_KEY = 'backup-provider-key'
$env:RATE_LIMIT_DB_PATH = '.\gateway_usage.sqlite3'
```

Use secret-manager credentials in a real deployment; the example values above are placeholders.

## Run

```powershell
.\.venv\Scripts\python.exe -m uvicorn router:app --host 127.0.0.1 --port 8000
```

Submit an OpenAI-compatible JSON body to `POST /v1/chat/completions`, including a `messages` array and, preferably, `max_tokens` or `max_completion_tokens`. Authenticate with `Authorization: Bearer <tenant-api-key>`.

## Design and operational behavior

- The default limit is 50,000 reserved tokens per tenant in a rolling 60-second window. Prompt tokens are counted with `tiktoken`; the requested maximum completion tokens are added as a conservative reservation.
- SQLite stores token reservations under a SHA-256 tenant identifier, not the raw API key or tenant name. Expired reservations are evicted on admission checks. `BEGIN IMMEDIATE` serializes limit-check-plus-insert transactions across concurrent requests and worker processes; blocking SQLite calls run using `asyncio.to_thread`.
- If the primary returns HTTP 429 or fails to return response headers or its first response bytes within three seconds, the request is sent once to the configured backup provider. Once response bytes have been sent to the caller, a later mid-stream timeout cannot be transparently retried; other primary failures do not trigger failover.
- Successful upstream response bodies are streamed to the caller. Upstream error bodies are not returned; the gateway emits a consistent `{ "error": { "code", "message", "request_id" } }` payload without provider stack traces or raw error details.
- A token reservation accounts for the requested maximum completion budget, even if the request later fails or uses fewer tokens; this conservative accounting avoids exceeding the configured budget.

Automated tests use mock providers and a temporary SQLite database; no provider credentials are needed:

```powershell
.\.venv\Scripts\python.exe -m unittest -v tests.test_router
```
