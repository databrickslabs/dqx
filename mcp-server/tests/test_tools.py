import json
from unittest.mock import MagicMock, create_autospec, patch

import pytest

from databricks.sdk import WorkspaceClient


_ENV = {
    "DQX_RUNNER_JOB_ID": "42",
    "DQX_CATALOG": "dqx_mcp",
    "DQX_TMP_SCHEMA": "tmp",
}


def _register_tools():
    """Helper to register tools and return them as a dict."""
    from server.tools import load_tools

    mock_mcp = MagicMock()
    registered_tools = {}

    def capture_tool(func):
        registered_tools[func.__name__] = func
        return func

    mock_mcp.tool = capture_tool
    load_tools(mock_mcp)
    return registered_tools


class TestGetTableSchema:
    """Test that get_table_schema uses direct SQL via OBO, not a job."""

    def test_calls_execute_sql_with_obo_client(self):
        tools = _register_tools()

        describe_rows = [
            {"col_name": "id", "data_type": "int", "comment": ""},
            {"col_name": "name", "data_type": "string", "comment": ""},
        ]

        with patch("server.tools.utils.get_obo_client") as mock_obo, \
             patch("server.tools.utils.get_warehouse_id", return_value="wh123"), \
             patch("server.tools.utils.execute_sql", return_value=describe_rows) as mock_sql, \
             patch.dict("os.environ", _ENV):
            result = tools["get_table_schema"]("catalog.schema.table")

        mock_obo.assert_called_once()
        mock_sql.assert_called_once()
        assert result["table_name"] == "catalog.schema.table"
        assert len(result["columns"]) == 2
        assert result["columns"][0] == {"name": "id", "type": "int", "comment": ""}

    def test_filters_metadata_rows(self):
        tools = _register_tools()

        describe_rows = [
            {"col_name": "id", "data_type": "int", "comment": ""},
            {"col_name": "# Partition Information", "data_type": "", "comment": ""},
            {"col_name": "# col_name", "data_type": "data_type", "comment": "comment"},
        ]

        with patch("server.tools.utils.get_obo_client"), \
             patch("server.tools.utils.get_warehouse_id", return_value="wh123"), \
             patch("server.tools.utils.execute_sql", return_value=describe_rows), \
             patch.dict("os.environ", _ENV):
            result = tools["get_table_schema"]("catalog.schema.table")

        assert len(result["columns"]) == 1
        assert result["columns"][0]["name"] == "id"


class TestProfileTable:
    """Test that profile_table creates a view, submits job, then cleans up."""

    def test_creates_view_submits_job_drops_view(self):
        tools = _register_tools()

        with patch("server.tools.utils.get_obo_client") as mock_obo, \
             patch("server.tools.utils.get_warehouse_id", return_value="wh123"), \
             patch("server.tools.utils.create_temp_view", return_value="dqx_mcp.tmp.v_abc") as mock_create, \
             patch("server.tools.utils.submit_notebook_job", return_value={"profiles": []}) as mock_job, \
             patch("server.tools.utils.drop_view") as mock_drop, \
             patch("server.tools.utils._get_sp_client") as mock_sp, \
             patch.dict("os.environ", _ENV):
            result = tools["profile_table"]("catalog.schema.table")

        mock_obo.assert_called_once()
        mock_create.assert_called_once()
        mock_job.assert_called_once()
        job_params = mock_job.call_args[0][1]
        assert job_params["view_name"] == "dqx_mcp.tmp.v_abc"
        assert result["table_name"] == "catalog.schema.table"
        mock_drop.assert_called_once_with(mock_sp.return_value, "dqx_mcp.tmp.v_abc", warehouse_id="wh123")

    def test_drops_view_even_on_job_failure(self):
        tools = _register_tools()

        with patch("server.tools.utils.get_obo_client"), \
             patch("server.tools.utils.get_warehouse_id", return_value="wh123"), \
             patch("server.tools.utils.create_temp_view", return_value="dqx_mcp.tmp.v_abc"), \
             patch("server.tools.utils.submit_notebook_job", side_effect=RuntimeError("job failed")), \
             patch("server.tools.utils.drop_view") as mock_drop, \
             patch("server.tools.utils._get_sp_client") as mock_sp, \
             patch.dict("os.environ", _ENV):
            with pytest.raises(RuntimeError, match="job failed"):
                tools["profile_table"]("catalog.schema.table")

        mock_drop.assert_called_once()


class TestRunChecks:
    """Test that run_checks creates a view, submits job, then cleans up."""

    def test_creates_view_submits_job_drops_view(self):
        tools = _register_tools()

        with patch("server.tools.utils.get_obo_client"), \
             patch("server.tools.utils.get_warehouse_id", return_value="wh123"), \
             patch("server.tools.utils.create_temp_view", return_value="dqx_mcp.tmp.v_abc") as mock_create, \
             patch("server.tools.utils.submit_notebook_job", return_value={"total_rows": 100, "valid_rows": 90, "invalid_rows": 10}) as mock_job, \
             patch("server.tools.utils.drop_view") as mock_drop, \
             patch("server.tools.utils._get_sp_client"), \
             patch.dict("os.environ", _ENV):
            result = tools["run_checks"]("catalog.schema.table", [{"check": "foo"}])

        mock_create.assert_called_once()
        job_params = mock_job.call_args[0][1]
        assert job_params["view_name"] == "dqx_mcp.tmp.v_abc"
        assert job_params["checks"] == [{"check": "foo"}]
        assert result["table_name"] == "catalog.schema.table"
        mock_drop.assert_called_once()

    def test_drops_view_even_on_job_failure(self):
        tools = _register_tools()

        with patch("server.tools.utils.get_obo_client"), \
             patch("server.tools.utils.get_warehouse_id", return_value="wh123"), \
             patch("server.tools.utils.create_temp_view", return_value="dqx_mcp.tmp.v_abc"), \
             patch("server.tools.utils.submit_notebook_job", side_effect=RuntimeError("job failed")), \
             patch("server.tools.utils.drop_view") as mock_drop, \
             patch("server.tools.utils._get_sp_client"), \
             patch.dict("os.environ", _ENV):
            with pytest.raises(RuntimeError, match="job failed"):
                tools["run_checks"]("catalog.schema.table", [])

        mock_drop.assert_called_once()


class TestJobOnlyTools:
    """Test tools that just delegate to submit_notebook_job without views."""

    def test_generate_rules(self):
        tools = _register_tools()

        with patch("server.tools.utils.submit_notebook_job", return_value={"rules": [], "count": 0}) as mock_job:
            result = tools["generate_rules"]([{"name": "p1"}], "warn")

        mock_job.assert_called_once_with("generate_rules", {"profiles": [{"name": "p1"}], "criticality": "warn"})
        assert result == {"rules": [], "count": 0}

    def test_validate_checks(self):
        tools = _register_tools()

        with patch("server.tools.utils.submit_notebook_job", return_value={"valid": True, "errors": []}) as mock_job:
            result = tools["validate_checks"]([{"check": "foo"}])

        mock_job.assert_called_once_with("validate_checks", {"checks": [{"check": "foo"}]})
        assert result["valid"] is True

    def test_list_available_checks(self):
        tools = _register_tools()

        with patch("server.tools.utils.submit_notebook_job", return_value={"checks": [], "count": 0}) as mock_job:
            result = tools["list_available_checks"]()

        mock_job.assert_called_once_with("list_available_checks", {})

    def test_get_workflow_returns_steps(self):
        tools = _register_tools()
        result = tools["get_workflow"]()
        assert "steps" in result
        assert len(result["steps"]) == 5
