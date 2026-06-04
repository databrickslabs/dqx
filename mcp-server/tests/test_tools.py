import asyncio
from unittest.mock import patch

from fastmcp import FastMCP


def _load_tools():
    """Load tools onto a fresh MCP server and return it."""
    mcp = FastMCP(name="test-dqx")
    from server.tools import load_tools
    load_tools(mcp)
    return mcp


def _get_tool_fn(mcp: FastMCP, name: str):
    """Retrieve the raw function registered under the given tool name."""
    tool = asyncio.run(mcp.local_provider.get_tool(name))
    return tool.fn


class TestJobBasedTools:
    """Tools that delegate to submit_notebook_job()."""

    @patch("server.utils.submit_notebook_job")
    def test_get_table_schema_delegates_to_job(self, mock_submit):
        mock_submit.return_value = {"table_name": "t", "columns": [], "row_count": 0}

        mcp = _load_tools()
        tool_fn = _get_tool_fn(mcp, "get_table_schema")
        result = tool_fn(table_name="catalog.schema.table")

        mock_submit.assert_called_once_with("get_table_schema", {"table_name": "catalog.schema.table"})
        assert result == {"table_name": "t", "columns": [], "row_count": 0}

    @patch("server.utils.submit_notebook_job")
    def test_profile_table_delegates_to_job(self, mock_submit):
        mock_submit.return_value = {"table_name": "t", "summary_stats": {}, "profiles": []}

        mcp = _load_tools()
        tool_fn = _get_tool_fn(mcp, "profile_table")
        result = tool_fn(table_name="t", columns=["a"], options={"limit": 100})

        mock_submit.assert_called_once_with("profile_table", {
            "table_name": "t",
            "columns": ["a"],
            "options": {"limit": 100},
        })
        assert result == {"table_name": "t", "summary_stats": {}, "profiles": []}

    @patch("server.utils.submit_notebook_job")
    def test_generate_rules_delegates_to_job(self, mock_submit):
        profiles = [{"name": "is_not_null", "column": "id"}]
        mock_submit.return_value = {"rules": [], "count": 0}

        mcp = _load_tools()
        tool_fn = _get_tool_fn(mcp, "generate_rules")
        result = tool_fn(profiles=profiles, criticality="warn")

        mock_submit.assert_called_once_with("generate_rules", {
            "profiles": profiles,
            "criticality": "warn",
        })
        assert result == {"rules": [], "count": 0}

    @patch("server.utils.submit_notebook_job")
    def test_run_checks_delegates_to_job(self, mock_submit):
        checks = [{"check": {"function": "is_not_null", "arguments": {"column": "id"}}}]
        mock_submit.return_value = {"total_rows": 100, "valid_rows": 95, "invalid_rows": 5}

        mcp = _load_tools()
        tool_fn = _get_tool_fn(mcp, "run_checks")
        result = tool_fn(table_name="t", checks=checks, sample_size=10)

        mock_submit.assert_called_once_with("run_checks", {
            "table_name": "t",
            "checks": checks,
            "sample_size": 10,
        })
        assert result == {"total_rows": 100, "valid_rows": 95, "invalid_rows": 5}


class TestInProcessTools:
    """Tools that stay in-process (no job submission)."""

    def test_list_available_checks_returns_checks(self):
        mcp = _load_tools()
        tool_fn = _get_tool_fn(mcp, "list_available_checks")
        result = tool_fn()
        assert "checks" in result
        assert "count" in result
        assert result["count"] > 0

    def test_validate_checks_valid(self):
        mcp = _load_tools()
        tool_fn = _get_tool_fn(mcp, "validate_checks")
        result = tool_fn(checks=[{
            "check": {"function": "is_not_null", "arguments": {"column": "id"}},
            "criticality": "error",
        }])
        assert result["valid"] is True

    def test_get_workflow_returns_steps(self):
        mcp = _load_tools()
        tool_fn = _get_tool_fn(mcp, "get_workflow")
        result = tool_fn()
        assert "steps" in result
        assert len(result["steps"]) == 5
