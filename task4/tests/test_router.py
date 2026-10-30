import asyncio
import json
import sqlite3
import tempfile
import unittest
from collections.abc import AsyncIterator
from contextlib import closing
from pathlib import Path
from typing import Any

import httpx

from router import (
    FALLBACK_TIMEOUT_SECONDS,
    SQLiteSlidingWindowLimiter,
    create_app,
    estimate_request_tokens,
    load_tenant_keys,
)


class DelayedByteStream(httpx.AsyncByteStream):
    def __init__(self, content: bytes, delay: float) -> None:
        self.content = content
        self.delay = delay

    async def __aiter__(self) -> AsyncIterator[bytes]:
        await asyncio.sleep(self.delay)
        yield self.content

    async def aclose(self) -> None:
        return None


class SQLiteLimiterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.temp_dir.name) / "usage.sqlite3")
        self.limiter = SQLiteSlidingWindowLimiter(
            self.db_path,
            token_limit=100,
            window_seconds=60,
        )
        await self.limiter.initialize()

    async def asyncTearDown(self) -> None:
        self.temp_dir.cleanup()

    async def test_reserves_budget_and_evicts_expired_window_entries(self) -> None:
        self.assertEqual(await self.limiter.reserve("tenant-a", 70, now=1000), (True, 70, 0))
        self.assertEqual(
            await self.limiter.reserve("tenant-a", 40, now=1010),
            (False, 70, 50),
        )
        self.assertEqual(await self.limiter.reserve("tenant-a", 40, now=1061), (True, 40, 0))

    async def test_tenants_have_independent_token_budgets(self) -> None:
        self.assertEqual(await self.limiter.reserve("tenant-a", 80, now=1000), (True, 80, 0))
        self.assertEqual(await self.limiter.reserve("tenant-b", 80, now=1000), (True, 80, 0))

    async def test_concurrent_reservations_cannot_exceed_token_limit(self) -> None:
        results = await asyncio.gather(
            *(
                self.limiter.reserve("tenant-a", 30, now=1000)
                for _ in range(5)
            )
        )
        admitted = [result for result in results if result[0]]
        rejected = [result for result in results if not result[0]]

        self.assertEqual(len(admitted), 3)
        self.assertEqual(len(rejected), 2)
        self.assertLessEqual(max(result[1] for result in admitted), 100)

    async def test_over_limit_single_reservation_is_rejected(self) -> None:
        self.assertEqual(
            await self.limiter.reserve("tenant-a", 101, now=1000),
            (False, 0, 0),
        )

    async def test_database_stores_tenant_hash_not_api_key(self) -> None:
        await self.limiter.reserve("tenant-private-key", 5, now=1000)
        with closing(sqlite3.connect(self.db_path)) as connection:
            stored = connection.execute(
                "SELECT tenant_hash FROM token_reservations"
            ).fetchone()[0]
        self.assertNotEqual(stored, "tenant-private-key")
        self.assertEqual(len(stored), 64)


class RouterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.temp_dir.name) / "usage.sqlite3")
        self.requests: list[dict[str, Any]] = []
        self.responses: dict[str, Any] = {}

        def provider(request: httpx.Request) -> httpx.Response:
            self.requests.append(
                {
                    "url": str(request.url),
                    "headers": dict(request.headers),
                    "body": json.loads(request.content),
                    "timeout": request.extensions["timeout"],
                }
            )
            response = self.responses.get(request.url.host)
            if isinstance(response, BaseException):
                raise response
            if callable(response):
                return response(request)
            return response or httpx.Response(
                200,
                headers={"content-type": "application/json"},
                json={"id": "completion-demo", "choices": []},
            )

        self.upstream_client = httpx.AsyncClient(
            transport=httpx.MockTransport(provider),
        )
        self.app = create_app(
            tenant_keys={
                "tenant-a-key": "tenant-a",
                "tenant-a-key-alias": "tenant-a",
                "tenant-b-key": "tenant-b",
            },
            primary_url="https://primary.test/v1/chat/completions",
            backup_url="https://backup.test/v1/chat/completions",
            primary_api_key="primary-provider-secret",
            backup_api_key="backup-provider-secret",
            database_path=self.db_path,
            token_limit=50_000,
            upstream_timeout=FALLBACK_TIMEOUT_SECONDS,
            upstream_client=self.upstream_client,
        )
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app),
            base_url="http://gateway",
        )
        await self.app.state.limiter.initialize()

    async def asyncTearDown(self) -> None:
        await self.client.aclose()
        await self.upstream_client.aclose()
        self.temp_dir.cleanup()

    def payload(self, *, max_tokens: int = 100) -> dict[str, Any]:
        return {
            "model": "gpt-4o-mini",
            "messages": [{"role": "user", "content": "Hello."}],
            "max_tokens": max_tokens,
        }

    async def post(
        self,
        *,
        api_key: str = "tenant-a-key",
        payload: dict[str, Any] | None = None,
    ) -> httpx.Response:
        return await self.client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}"},
            json=self.payload() if payload is None else payload,
        )

    async def test_primary_success_streams_without_calling_backup(self) -> None:
        response = await self.post()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["id"], "completion-demo")
        self.assertEqual(len(self.requests), 1)
        self.assertIn("primary-provider-secret", self.requests[0]["headers"]["authorization"])

    async def test_primary_429_fails_over_to_backup(self) -> None:
        self.responses["primary.test"] = httpx.Response(
            429,
            headers={"retry-after": "5"},
            json={"error": {"message": "provider-internal"}},
        )

        response = await self.post()

        self.assertEqual(response.status_code, 200)
        self.assertEqual([request["url"] for request in self.requests], [
            "https://primary.test/v1/chat/completions",
            "https://backup.test/v1/chat/completions",
        ])
        self.assertIn("backup-provider-secret", self.requests[1]["headers"]["authorization"])

    async def test_primary_three_second_timeout_fails_over_to_backup(self) -> None:
        self.responses["primary.test"] = httpx.ConnectTimeout(
            "private primary timeout details"
        )

        response = await self.post()

        self.assertEqual(response.status_code, 200)
        self.assertEqual([request["url"].split("/")[2] for request in self.requests], [
            "primary.test",
            "backup.test",
        ])
        self.assertEqual(
            self.requests[0]["timeout"]["read"],
            FALLBACK_TIMEOUT_SECONDS,
        )

    async def test_no_first_token_within_timeout_fails_over(self) -> None:
        self.responses["primary.test"] = httpx.Response(
            200,
            stream=DelayedByteStream(b'{"choices":[]}', delay=0.1),
        )
        timeout_app = create_app(
            tenant_keys={"tenant-a-key": "tenant-a"},
            primary_url="https://primary.test/v1/chat/completions",
            backup_url="https://backup.test/v1/chat/completions",
            database_path=self.db_path,
            upstream_timeout=0.01,
            upstream_client=self.upstream_client,
        )
        timeout_client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=timeout_app),
            base_url="http://gateway",
        )
        await timeout_app.state.limiter.initialize()
        try:
            response = await timeout_client.post(
                "/v1/chat/completions",
                headers={"Authorization": "Bearer tenant-a-key"},
                json=self.payload(),
            )
        finally:
            await timeout_client.aclose()

        self.assertEqual(response.status_code, 200)
        self.assertEqual([request["url"].split("/")[2] for request in self.requests], [
            "primary.test",
            "backup.test",
        ])

    async def test_backup_timeout_returns_standardized_sanitized_error(self) -> None:
        self.responses["primary.test"] = httpx.ConnectTimeout("primary private details")
        self.responses["backup.test"] = httpx.ConnectTimeout("backup private details")

        response = await self.post()

        self.assertEqual(response.status_code, 504)
        self.assertEqual(response.json()["error"]["code"], "upstream_timeout")
        self.assertNotIn("private details", response.text)
        self.assertIn("request_id", response.json()["error"])

    async def test_backup_429_returns_standardized_error_without_upstream_body(self) -> None:
        self.responses["primary.test"] = httpx.Response(429, json={"error": "secret"})
        self.responses["backup.test"] = httpx.Response(
            429,
            headers={"retry-after": "8"},
            json={"error": "backup private details"},
        )

        response = await self.post()

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.headers["retry-after"], "8")
        self.assertEqual(response.json()["error"]["code"], "upstream_rate_limited")
        self.assertNotIn("private details", response.text)

    async def test_primary_provider_errors_are_sanitized_without_fallback(self) -> None:
        self.responses["primary.test"] = httpx.Response(
            500,
            json={"error": {"message": "provider stack trace private details"}},
        )

        response = await self.post()

        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.json()["error"]["code"], "upstream_error")
        self.assertNotIn("private details", response.text)
        self.assertEqual(len(self.requests), 1)

    async def test_invalid_key_and_bad_request_do_not_call_provider(self) -> None:
        unauthorized = await self.client.post(
            "/v1/chat/completions",
            json=self.payload(),
        )
        invalid = await self.post(payload={"model": "gpt-4o-mini", "messages": []})

        self.assertEqual(unauthorized.status_code, 401)
        self.assertEqual(invalid.status_code, 400)
        self.assertEqual(self.requests, [])

    async def test_non_standard_json_numbers_are_rejected(self) -> None:
        response = await self.client.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer tenant-a-key"},
            content=b'{"model":"gpt-4o-mini","messages":[],"max_tokens":NaN}',
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"]["code"], "invalid_request")
        self.assertEqual(self.requests, [])

    async def test_invalid_token_limit_does_not_consume_quota(self) -> None:
        invalid_payload = self.payload(max_tokens=True)
        invalid = await self.post(payload=invalid_payload)
        valid = await self.post(payload=self.payload(max_tokens=100))

        self.assertEqual(invalid.status_code, 400)
        self.assertEqual(valid.status_code, 200)
        self.assertEqual(len(self.requests), 1)

    async def test_token_budget_is_enforced_per_tenant_key(self) -> None:
        limited_app = create_app(
            tenant_keys={"tenant-a-key": "tenant-a", "tenant-b-key": "tenant-b"},
            primary_url="https://primary.test/v1/chat/completions",
            backup_url="https://backup.test/v1/chat/completions",
            database_path=self.db_path,
            token_limit=150,
            upstream_client=self.upstream_client,
        )
        limited_client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=limited_app),
            base_url="http://gateway",
        )
        await limited_app.state.limiter.initialize()
        request_payload = self.payload(max_tokens=100)
        try:
            first = await limited_client.post(
                "/v1/chat/completions",
                headers={"Authorization": "Bearer tenant-a-key"},
                json=request_payload,
            )
            second = await limited_client.post(
                "/v1/chat/completions",
                headers={"Authorization": "Bearer tenant-a-key"},
                json=request_payload,
            )
            other_tenant = await limited_client.post(
                "/v1/chat/completions",
                headers={"Authorization": "Bearer tenant-b-key"},
                json=request_payload,
            )
        finally:
            await limited_client.aclose()

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 429)
        self.assertEqual(second.json()["error"]["code"], "rate_limit_exceeded")
        self.assertIn("retry-after", second.headers)
        self.assertEqual(other_tenant.status_code, 200)

    async def test_request_token_estimate_counts_prompt_and_completion_budget(self) -> None:
        request = self.payload(max_tokens=100)
        estimate = estimate_request_tokens(request)
        self.assertGreaterEqual(estimate, 100)


class ConfigurationTests(unittest.TestCase):
    def test_tenant_key_configuration_validation(self) -> None:
        self.assertEqual(
            load_tenant_keys('{"api-key":"tenant-a"}'),
            {"api-key": "tenant-a"},
        )
        with self.assertRaises(ValueError):
            load_tenant_keys('{"api-key":""}')
        with self.assertRaises(ValueError):
            load_tenant_keys("[]")


if __name__ == "__main__":
    unittest.main()
