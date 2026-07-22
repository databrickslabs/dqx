"""Protocol-level unit tests driving the server through FastMCP's in-memory Client.

Unlike test_tools.py (which calls the registered handler functions directly), these run
the REAL MCP protocol in-process: capability negotiation, tool registration/schemas, and
call_tool dispatch — exactly what a client (Claude, Genie Code, Cursor) exercises — with the
workspace boundary (utils.*) faked so they stay deterministic and need no workspace.
"""

from unittest.mock import patch

import pytest

from fastmcp import Client
from server.app import mcp_server


# anyio is used as the async test runner (matches the existing async tests in test_utils.py).
@pytest.fixture
def anyio_backend():
    return "asyncio"


_ENV = {"DQX_RUNNER_JOB_ID": "42", "DQX_CATALOG": "dqx_mcp", "DQX_TMP_SCHEMA": "dqx_mcp_tmp"}

EXPECTED_TOOLS = {
    "get_table_schema",
    "profile_table",
    "generate_rules",
    "generate_rules_from_contract",
    "load_checks",
    "save_checks",
    "validate_checks",
    "run_checks",
    "apply_checks_and_save_to_table",
    "get_run_result",
    "list_available_checks",
    "get_workflow",
}


class TestToolDiscovery:
    @pytest.mark.anyio
    async def test_list_tools_exposes_all_tools(self):
        async with Client(mcp_server) as client:
            tools = await client.list_tools()
        names = {t.name for t in tools}
        assert EXPECTED_TOOLS <= names, f"missing: {EXPECTED_TOOLS - names}"

    @pytest.mark.anyio
    async def test_tools_have_description_and_schema(self):
        async with Client(mcp_server) as client:
            tools = await client.list_tools()
        for t in tools:
            assert t.description, f"{t.name} has no description"
            assert t.inputSchema and t.inputSchema.get("type") == "object", f"{t.name} has no input schema"


class TestToolInvocation:
    @pytest.mark.anyio
    async def test_get_workflow_returns_steps(self):
        async with Client(mcp_server) as client:
            res = await client.call_tool("get_workflow", {})
        assert "steps" in res.data
        assert len(res.data["steps"]) == 5

    @pytest.mark.anyio
    async def test_generate_rules_submits_job(self):
        with patch("server.tools.utils.submit_job_async", return_value=7) as mock_submit:
            async with Client(mcp_server) as client:
                res = await client.call_tool("generate_rules", {"profiles": [{"name": "p1"}], "criticality": "warn"})
        mock_submit.assert_called_once_with("generate_rules", {"profiles": [{"name": "p1"}], "criticality": "warn"})
        assert res.data["status"] == "submitted"
        assert res.data["run_id"] == 7

    @pytest.mark.anyio
    async def test_profile_table_creates_view_and_submits(self):
        with (
            patch("server.tools.utils.get_obo_client"),
            patch("server.tools.utils.get_warehouse_id", return_value="wh123"),
            patch("server.tools.utils.create_temp_view", return_value="dqx_mcp.dqx_mcp_tmp.v_abc"),
            patch("server.tools.utils.submit_job_async", return_value=999) as mock_submit,
            patch.dict("os.environ", _ENV),
        ):
            async with Client(mcp_server) as client:
                res = await client.call_tool("profile_table", {"table_name": "catalog.schema.table"})
        op, params = mock_submit.call_args[0]
        assert op == "profile_table"
        assert params["view_name"] == "dqx_mcp.dqx_mcp_tmp.v_abc"
        assert params["table_name"] == "catalog.schema.table"
        assert res.data["run_id"] == 999

    @pytest.mark.anyio
    async def test_save_checks_submits_job(self):
        with (
            patch("server.tools.utils.submit_job_async", return_value=23) as mock_submit,
            patch.dict("os.environ", _ENV),
        ):
            async with Client(mcp_server) as client:
                res = await client.call_tool(
                    "save_checks", {"checks": [{"check": "foo"}], "output_name": "my_checks", "mode": "overwrite"}
                )
        mock_submit.assert_called_once_with(
            "save_checks",
            {
                "checks": [{"check": "foo"}],
                "output_name": "my_checks",
                "run_config_name": "default",
                "mode": "overwrite",
                "catalog": "dqx_mcp",
                # no OBO user context in the in-memory client, so there is nobody to grant to
                "grant_to": None,
            },
        )
        assert res.data["run_id"] == 23
