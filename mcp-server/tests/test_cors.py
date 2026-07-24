"""Unit tests for the MCP server CORS policy.

These tests drive the real Starlette CORSMiddleware using the same exact-origin
allow-list the production app builds, asserting observable CORS behaviour (which
origins are reflected) rather than the configuration value. The key property: the
credentialed allow-list is scoped to specific tenant hosts, NOT the multi-tenant
``*.databricksapps.com`` domain.
"""

import pytest
from starlette.applications import Starlette
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from server.app import get_allowed_cors_origins

# An example workspace origin (its DATABRICKS_HOST). Genie Code's browser UI runs at this origin
# and issues the cross-origin preflight to the app. (Illustrative host — not a real workspace.)
WORKSPACE_HOST = "https://adb-1234567890123456.7.azuredatabricks.net"
# A second, operator-configured origin (e.g. a local dev front-end).
EXTRA_ORIGIN = "http://localhost:3000"

ALLOWED_ORIGINS = [WORKSPACE_HOST, EXTRA_ORIGIN]

REJECTED_ORIGINS = [
    # The crux of Marcin's review: another tenant's app on the shared multi-tenant
    # databricksapps.com domain must NOT be an allowed credentialed origin.
    "https://someone-elses-app-9999.eastus.databricksapps.com",
    # A different workspace.
    "https://adb-0000000000000000.0.azuredatabricks.net",
    "https://evil.example.com",
    "https://databricks.com.evil.example.com",  # suffix-spoof attempt
    "http://adb-1234567890123456.7.azuredatabricks.net",  # http downgrade of the allowed host
]


def _make_client() -> TestClient:
    """Minimal app wrapped with the production exact-origin CORS config."""

    async def ok(request):  # noqa: ANN001, ANN202 - test stub
        return PlainTextResponse("ok")

    app = Starlette(routes=[Route("/", ok, methods=["GET", "POST"])])
    app.add_middleware(
        CORSMiddleware,
        allow_origins=get_allowed_cors_origins(WORKSPACE_HOST, EXTRA_ORIGIN),
        allow_methods=["*"],
        allow_headers=["*"],
        allow_credentials=True,
    )
    return TestClient(app)


class TestAllowedOriginsBuilder:
    def test_includes_workspace_host_and_extras(self):
        origins = get_allowed_cors_origins(WORKSPACE_HOST, EXTRA_ORIGIN)
        assert WORKSPACE_HOST in origins
        assert EXTRA_ORIGIN in origins

    def test_bare_host_gets_https_scheme(self):
        origins = get_allowed_cors_origins("adb-123.4.azuredatabricks.net", "")
        assert origins == ["https://adb-123.4.azuredatabricks.net"]

    def test_trailing_slash_stripped_and_deduped(self):
        origins = get_allowed_cors_origins(WORKSPACE_HOST + "/", WORKSPACE_HOST)
        assert origins == [WORKSPACE_HOST]

    def test_empty_host_yields_no_origins(self):
        assert get_allowed_cors_origins("", "") == []

    def test_multiple_extra_origins_split_on_comma(self):
        origins = get_allowed_cors_origins("", "https://a.example.com, https://b.example.com")
        assert origins == ["https://a.example.com", "https://b.example.com"]

    def test_no_wildcard_databricksapps_origin(self):
        # The allow-list must never contain a bare/wildcard databricksapps.com entry.
        origins = get_allowed_cors_origins(WORKSPACE_HOST, EXTRA_ORIGIN)
        assert all("databricksapps.com" not in o for o in origins)


class TestCorsPreflight:
    @pytest.mark.parametrize("origin", ALLOWED_ORIGINS)
    def test_allowed_origin_reflected_with_credentials(self, origin: str):
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
    def test_disallowed_origin_rejected(self, origin: str):
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
        resp = client.get("/", headers={"origin": WORKSPACE_HOST})
        assert resp.status_code == 200
        assert resp.headers["access-control-allow-origin"] == WORKSPACE_HOST
        assert resp.headers["access-control-allow-credentials"] == "true"

    def test_other_tenant_app_not_reflected_on_actual_request(self):
        client = _make_client()
        resp = client.get("/", headers={"origin": "https://someone-elses-app-9999.eastus.databricksapps.com"})
        # The request itself still succeeds, but the browser gets no allow-origin header.
        assert resp.status_code == 200
        assert "access-control-allow-origin" not in resp.headers
