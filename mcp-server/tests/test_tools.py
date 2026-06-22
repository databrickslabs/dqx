from unittest.mock import MagicMock, patch


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

        with (
            patch("server.tools.utils.get_obo_client") as mock_obo,
            patch("server.tools.utils.get_warehouse_id", return_value="wh123"),
            patch("server.tools.utils.execute_sql", return_value=describe_rows) as mock_sql,
            patch.dict("os.environ", _ENV),
        ):
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

        with (
            patch("server.tools.utils.get_obo_client"),
            patch("server.tools.utils.get_warehouse_id", return_value="wh123"),
            patch("server.tools.utils.execute_sql", return_value=describe_rows),
            patch.dict("os.environ", _ENV),
        ):
            result = tools["get_table_schema"]("catalog.schema.table")

        assert len(result["columns"]) == 1
        assert result["columns"][0]["name"] == "id"


class TestProfileTable:
    """Test that profile_table creates a view and submits a job, returning a run_id."""

    def test_creates_view_and_submits_job(self):
        tools = _register_tools()

        with (
            patch("server.tools.utils.get_obo_client") as mock_obo,
            patch("server.tools.utils.get_warehouse_id", return_value="wh123"),
            patch("server.tools.utils.create_temp_view", return_value="dqx_mcp.tmp.v_abc") as mock_create,
            patch("server.tools.utils.submit_job_async", return_value=999) as mock_submit,
            patch.dict("os.environ", _ENV),
        ):
            result = tools["profile_table"]("catalog.schema.table")

        mock_obo.assert_called_once()
        mock_create.assert_called_once()
        assert mock_submit.call_args[0][0] == "profile_table"
        job_params = mock_submit.call_args[0][1]
        assert job_params["view_name"] == "dqx_mcp.tmp.v_abc"
        metadata = mock_submit.call_args.kwargs["metadata"]
        assert metadata["view_fqn"] == "dqx_mcp.tmp.v_abc"
        assert metadata["warehouse_id"] == "wh123"
        assert metadata["table_name"] == "catalog.schema.table"
        assert result["status"] == "submitted"
        assert result["run_id"] == 999


class TestRunChecks:
    """Test that run_checks creates a view and submits a job, returning a run_id."""

    def test_creates_view_and_submits_job(self):
        tools = _register_tools()

        with (
            patch("server.tools.utils.get_obo_client"),
            patch("server.tools.utils.get_warehouse_id", return_value="wh123"),
            patch("server.tools.utils.create_temp_view", return_value="dqx_mcp.tmp.v_abc") as mock_create,
            patch("server.tools.utils.submit_job_async", return_value=999) as mock_submit,
            patch.dict("os.environ", _ENV),
        ):
            result = tools["run_checks"]("catalog.schema.table", [{"check": "foo"}])

        mock_create.assert_called_once()
        assert mock_submit.call_args[0][0] == "run_checks"
        job_params = mock_submit.call_args[0][1]
        assert job_params["view_name"] == "dqx_mcp.tmp.v_abc"
        assert job_params["checks"] == [{"check": "foo"}]
        assert job_params["sample_size"] == 50
        assert result["status"] == "submitted"
        assert result["run_id"] == 999


class TestGetRunResult:
    """Test that get_run_result delegates to utils.get_run_status."""

    def test_delegates_to_get_run_status(self):
        tools = _register_tools()

        with patch(
            "server.tools.utils.get_run_status",
            return_value={"status": "completed", "run_id": 5, "result": {}},
        ) as mock_status:
            result = tools["get_run_result"](5)

        mock_status.assert_called_once_with(5)
        assert result["status"] == "completed"


class TestJobOnlyTools:
    """Test tools that just submit a job (no view) and return a run_id."""

    def test_generate_rules(self):
        tools = _register_tools()

        with patch("server.tools.utils.submit_job_async", return_value=7) as mock_submit:
            result = tools["generate_rules"]([{"name": "p1"}], "warn")

        mock_submit.assert_called_once_with("generate_rules", {"profiles": [{"name": "p1"}], "criticality": "warn"})
        assert result["status"] == "submitted"
        assert result["run_id"] == 7

    def test_validate_checks(self):
        tools = _register_tools()

        with patch("server.tools.utils.submit_job_async", return_value=8) as mock_submit:
            result = tools["validate_checks"]([{"check": "foo"}])

        mock_submit.assert_called_once_with("validate_checks", {"checks": [{"check": "foo"}]})
        assert result["status"] == "submitted"
        assert result["run_id"] == 8

    def test_list_available_checks(self):
        tools = _register_tools()

        with patch("server.tools.utils.submit_job_async", return_value=9) as mock_submit:
            result = tools["list_available_checks"]()

        mock_submit.assert_called_once_with("list_available_checks", {})
        assert result["status"] == "submitted"
        assert result["run_id"] == 9

    def test_get_workflow_returns_steps(self):
        tools = _register_tools()
        result = tools["get_workflow"]()
        assert "steps" in result
        assert len(result["steps"]) == 5
