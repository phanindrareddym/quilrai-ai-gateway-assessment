# QuilrAI Assessment — Task 3

An OpenAI-compatible streaming LLM gateway that redacts email addresses, US Social Security numbers, and Luhn-valid payment-card numbers in streamed `choices[].delta.content` before returning SSE events to the caller.

## Requirements

- Python 3.11+

## Setup

From this directory in PowerShell:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e .
$env:LLM_UPSTREAM_URL = 'https://api.openai.com/v1/chat/completions'
$env:LLM_API_KEY = 'your-provider-api-key'
```

Provide a real provider key only in your local environment or secret manager; do not commit it.

## Run

```powershell
.\.venv\Scripts\python.exe -m uvicorn gateway:app --host 127.0.0.1 --port 8000
```

Send OpenAI-compatible JSON to `POST http://127.0.0.1:8000/v1/chat/completions` with `"stream": true`. The gateway sends the configured provider API key upstream, not the caller's `Authorization` header.

## Streaming and guardrail design

- The gateway forwards a successful `text/event-stream` response as it arrives, parsing complete SSE events and rewriting only string fields at `choices[].delta.content`.
- Each choice has a streaming redactor that holds only an ambiguous suffix (up to 320 characters) so emails and numbers split across provider chunks/events are not sent before validation. Safe prefixes are emitted immediately; the whole model completion is never accumulated.
- Email patterns and SSNs are matched across the retained text. Payment-card candidates are redacted only when they have 13–19 digits and pass the Luhn checksum.
- SSE parsing is incremental and a single event is capped at 1 MiB to bound memory. The final pending text is redacted before the `[DONE]` event is returned.
- Provider errors and non-SSE responses are relayed unchanged. Provider connection failures return generic gateway errors.

Run tests without an external provider key:

```powershell
.\.venv\Scripts\python.exe -m unittest -v tests.test_gateway
```
