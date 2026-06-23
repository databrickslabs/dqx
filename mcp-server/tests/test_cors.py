"""Unit tests for the MCP server CORS policy.

These tests drive the real Starlette CORSMiddleware using the same
CORS_ALLOWED_ORIGIN_REGEX wired into the production app, asserting observable
CORS behaviour (which origins are reflected) rather than the regex string.
"""

import pytest
from starlette.applications import Starlette
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from server.app import CORS_ALLOWED_ORIGIN_REGEX


def _make_client() -> TestClient:
    """Minimal app wrapped with the production CORS config."""

    async def ok(request):  # noqa: ANN001, ANN202 - test stub
        return PlainTextResponse("ok")

    app = Starlette(routes=[Route("/", ok, methods=["GET", "POST"])])
    app.add_middleware(
        CORSMiddleware,
        allow_origin_regex=CORS_ALLOWED_ORIGIN_REGEX,
        allow_methods=["*"],
        allow_headers=["*"],
        allow_credentials=True,
    )
    return TestClient(app)


ALLOWED_ORIGINS = [
    "https://adb-7405605361901583.3.azuredatabricks.net",  # Azure (the deployed workspace)
    "https://myworkspace.cloud.databricks.com",  # AWS
    "https://1234567890.gcp.databricks.com",  # GCP
    "https://mcp-dqx-1234.eastus.databricksapps.com",  # Databricks Apps host
]

REJECTED_ORIGINS = [
    "https://evil.example.com",
    "https://databricks.com.evil.example.com",  # suffix-spoof attempt
    "http://myworkspace.cloud.databricks.com",  # http (not https)
    "https://notdatabricks.net",
]


class TestCorsPreflight:
    @pytest.mark.parametrize("origin", ALLOWED_ORIGINS)
    def test_databricks_origin_reflected_with_credentials(self, origin: str):
        client = _make_client()
        resp = client.options(
            "/",
            headers={
                "origin": origin,
                "access-control-request-method": "POST",
                "access-control-request-headers": "content-type",
            },
        )
        assert resp.status_code == 200
        # Spec-compliant with credentials: the exact origin is reflected, never "*".
        assert resp.headers["access-control-allow-origin"] == origin
        assert resp.headers["access-control-allow-credentials"] == "true"

    @pytest.mark.parametrize("origin", REJECTED_ORIGINS)
    def test_non_databricks_origin_rejected(self, origin: str):
        client = _make_client()
        resp = client.options(
            "/",
            headers={
                "origin": origin,
                "access-control-request-method": "POST",
            },
        )
        # Starlette returns 400 for a disallowed preflight and omits the allow-origin header.
        assert resp.status_code == 400
        assert "access-control-allow-origin" not in resp.headers


class TestCorsSimpleRequest:
    def test_allowed_origin_reflected_on_actual_request(self):
        client = _make_client()
        resp = client.get("/", headers={"origin": ALLOWED_ORIGINS[0]})
        assert resp.status_code == 200
        assert resp.headers["access-control-allow-origin"] == ALLOWED_ORIGINS[0]
        assert resp.headers["access-control-allow-credentials"] == "true"

    def test_disallowed_origin_not_reflected_on_actual_request(self):
        client = _make_client()
        resp = client.get("/", headers={"origin": "https://evil.example.com"})
        # The request itself still succeeds, but the browser gets no allow-origin header.
        assert resp.status_code == 200
        assert "access-control-allow-origin" not in resp.headers
