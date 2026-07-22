"""HTTP/transport integration tests driving the real ASGI app (combined_app).

These exercise the production stack the way Genie Code / an MCP client hits it over
Streamable HTTP: the health route, the MCP endpoint (initialize, tools/list), and OBO
token propagation through OBOAuthMiddleware -> contextvar -> get_obo_client. The workspace
boundary (warehouse/SQL) is faked so no live workspace is needed. CORS is covered separately
in test_cors.py.

A single module-scoped TestClient runs the app lifespan once (the FastMCP Streamable HTTP
session manager can only be started once per app instance).
"""

import json
from unittest.mock import patch

import pytest
from starlette.testclient import TestClient

from server.app import combined_app

_MCP_HEADERS = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}


@pytest.fixture(scope="module")
def client():
    with TestClient(combined_app) as c:
        yield c


def _rpc(method: str, params: dict) -> dict:
    return {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}


def _parse(resp) -> dict:
    text = resp.text
    if "data:" in text[:32]:
        text = text.split("data:", 1)[1].strip()
    return json.loads(text)


class TestHealth:
    def test_health_endpoint(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert resp.json()["status"] == "healthy"


class TestMcpOverHttp:
    def test_initialize(self, client):
        resp = client.post(
            "/mcp",
            headers=_MCP_HEADERS,
            json=_rpc(
                "initialize",
                {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}},
            ),
        )
        body = _parse(resp)
        assert body["result"]["protocolVersion"]
        assert body["result"]["capabilities"]["tools"] is not None

    def test_tools_list(self, client):
        resp = client.post("/mcp", headers=_MCP_HEADERS, json=_rpc("tools/list", {}))
        body = _parse(resp)
        names = {t["name"] for t in body["result"]["tools"]}
        assert {"get_table_schema", "run_checks", "save_checks", "generate_rules_from_contract"} <= names


class TestOboPropagation:
    """The forwarded user token must flow: header -> OBOAuthMiddleware -> contextvar -> get_obo_client."""

    _describe = [
        {"col_name": "id", "data_type": "int", "comment": ""},
        {"col_name": "name", "data_type": "string", "comment": ""},
    ]

    def test_obo_token_present_allows_governed_call(self, client):
        with (
            patch("server.tools.utils.get_warehouse_id", return_value="wh123"),
            patch("server.tools.utils.execute_sql", return_value=self._describe),
            patch.dict("os.environ", {"DATABRICKS_HOST": "https://host.example.com"}),
        ):
            resp = client.post(
                "/mcp",
                headers={**_MCP_HEADERS, "X-Forwarded-Access-Token": "user-obo-token"},
                json=_rpc("tools/call", {"name": "get_table_schema", "arguments": {"table_name": "c.s.t"}}),
            )
        result = _parse(resp)["result"]
        assert not result.get("isError"), f"unexpected error: {result}"
        data = result.get("structuredContent") or json.loads(result["content"][0]["text"])
        assert data["table_name"] == "c.s.t"
        assert len(data["columns"]) == 2

    def test_missing_obo_token_is_rejected(self, client):
        with patch.dict("os.environ", {"DATABRICKS_HOST": "https://host.example.com"}):
            resp = client.post(
                "/mcp",
                headers=_MCP_HEADERS,  # no X-Forwarded-Access-Token
                json=_rpc("tools/call", {"name": "get_table_schema", "arguments": {"table_name": "c.s.t"}}),
            )
        # The tool raises ("No OBO token available..."); surfaced as an MCP error result.
        assert "obo" in resp.text.lower()
