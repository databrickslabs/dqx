from unittest.mock import MagicMock, patch

_ENV = {
    "DQX_RUNNER_JOB_ID": "42",
    "DQX_CATALOG": "dqx_mcp",
    "DQX_TMP_SCHEMA": "dqx_mcp_tmp",
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

    def test_partition_columns_not_duplicated(self):
        tools = _register_tools()

        # DESCRIBE TABLE on a partitioned table re-lists each partition column
        # (here "dt") under "# Partition Information" without a "#" prefix.
        describe_rows = [
            {"col_name": "id", "data_type": "int", "comment": ""},
            {"col_name": "name", "data_type": "string", "comment": ""},
            {"col_name": "dt", "data_type": "date", "comment": ""},
            {"col_name": "", "data_type": "", "comment": ""},
            {"col_name": "# Partition Information", "data_type": "", "comment": ""},
            {"col_name": "# col_name", "data_type": "data_type", "comment": "comment"},
            {"col_name": "dt", "data_type": "date", "comment": ""},
        ]

        with (
            patch("server.tools.utils.get_obo_client"),
            patch("server.tools.utils.get_warehouse_id", return_value="wh123"),
            patch("server.tools.utils.execute_sql", return_value=describe_rows),
            patch.dict("os.environ", _ENV),
        ):
            result = tools["get_table_schema"]("catalog.schema.table")

        assert [c["name"] for c in result["columns"]] == ["id", "name", "dt"]


class TestProfileTable:
    """Test that profile_table creates a view and submits a job, returning a run_id."""

    def test_creates_view_and_submits_job(self):
        tools = _register_tools()

        with (
            patch("server.tools.utils.get_obo_client") as mock_obo,
            patch("server.tools.utils.get_warehouse_id", return_value="wh123"),
            patch("server.tools.utils.create_temp_view", return_value="dqx_mcp.dqx_mcp_tmp.v_abc") as mock_create,
            patch("server.tools.utils.submit_job_async", return_value=999) as mock_submit,
            patch.dict("os.environ", _ENV),
        ):
            result = tools["profile_table"]("catalog.schema.table")

        mock_obo.assert_called_once()
        mock_create.assert_called_once()
        assert mock_submit.call_args[0][0] == "profile_table"
        job_params = mock_submit.call_args[0][1]
        assert job_params["view_name"] == "dqx_mcp.dqx_mcp_tmp.v_abc"
        # table_name travels in params (echoed back by the runner), not in server-side state.
        assert job_params["table_name"] == "catalog.schema.table"
        assert "metadata" not in mock_submit.call_args.kwargs
        assert result["status"] == "submitted"
        assert result["run_id"] == 999

    def test_drops_temp_view_if_submit_fails(self):
        """If submission throws after the OBO view is created, the view is dropped (no leak)."""
        import pytest

        tools = _register_tools()
        with (
            patch("server.tools.utils.get_obo_client"),
            patch("server.tools.utils.get_warehouse_id", return_value="wh123"),
            patch("server.tools.utils.create_temp_view", return_value="dqx_mcp.dqx_mcp_tmp.v_leak"),
            patch("server.tools.utils.submit_job_async", side_effect=RuntimeError("submit boom")),
            patch("server.tools.utils.drop_view") as mock_drop,
            patch.dict("os.environ", _ENV),
        ):
            with pytest.raises(RuntimeError, match="submit boom"):
                tools["profile_table"]("catalog.schema.table")

        # The exact view that was created is dropped (as the OBO client) on the submit failure.
        mock_drop.assert_called_once()
        assert mock_drop.call_args[0][1] == "dqx_mcp.dqx_mcp_tmp.v_leak"


class TestRunChecks:
    """Test that run_checks creates a view and submits a job, returning a run_id."""

    def test_creates_view_and_submits_job(self):
        tools = _register_tools()

        with (
            patch("server.tools.utils.get_obo_client"),
            patch("server.tools.utils.get_warehouse_id", return_value="wh123"),
            patch("server.tools.utils.create_temp_view", return_value="dqx_mcp.dqx_mcp_tmp.v_abc") as mock_create,
            patch("server.tools.utils.submit_job_async", return_value=999) as mock_submit,
            patch.dict("os.environ", _ENV),
        ):
            result = tools["run_checks"]("catalog.schema.table", [{"check": "foo"}])

        mock_create.assert_called_once()
        assert mock_submit.call_args[0][0] == "run_checks"
        job_params = mock_submit.call_args[0][1]
        assert job_params["view_name"] == "dqx_mcp.dqx_mcp_tmp.v_abc"
        assert job_params["table_name"] == "catalog.schema.table"
        assert job_params["checks"] == [{"check": "foo"}]
        assert job_params["sample_size"] == 50
        assert "metadata" not in mock_submit.call_args.kwargs
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

    def test_list_available_checks_forwards_filter(self):
        tools = _register_tools()

        with patch("server.tools.utils.submit_job_async", return_value=9) as mock_submit:
            tools["list_available_checks"](filter="regex")

        # A filter is forwarded to the runner so it can narrow the (large) registry server-side.
        mock_submit.assert_called_once_with("list_available_checks", {"filter": "regex"})

    def test_get_workflow_returns_steps(self):
        tools = _register_tools()
        result = tools["get_workflow"]()
        assert "steps" in result
        assert len(result["steps"]) == 5

    def test_generate_rules_from_contract_file_is_staged(self):
        tools = _register_tools()

        with (
            patch("server.tools.utils.get_obo_client"),
            patch("server.tools.utils.verify_obo_read_access") as mock_verify,
            patch("server.tools.utils.read_file_via_obo", return_value=b"contract-bytes") as mock_read,
            patch(
                "server.tools.utils.stage_bytes_to_results_volume",
                return_value="/Volumes/c/dqx_mcp_tmp/mcp_results/staged_x.yml",
            ) as mock_stage,
            patch("server.tools.utils.submit_job_async", return_value=21) as mock_submit,
        ):
            result = tools["generate_rules_from_contract"](contract_file="/Volumes/c/s/v/contract.yml")

        # The caller's read access is enforced, the file is read AS the caller, and a copy is staged
        # to a runner-readable volume — the staged path (not the caller's path) goes to the runner.
        assert mock_verify.call_args[0][1] == "/Volumes/c/s/v/contract.yml"
        assert mock_read.call_args[0][1] == "/Volumes/c/s/v/contract.yml"
        assert mock_stage.call_args[0][0] == b"contract-bytes"
        # Deterministic-only: process_text_rules is not exposed and not sent to the runner.
        mock_submit.assert_called_once_with(
            "generate_rules_from_contract",
            {
                "contract_file": "/Volumes/c/dqx_mcp_tmp/mcp_results/staged_x.yml",
                "contract_format": "odcs",
                "default_criticality": "error",
            },
        )
        assert result["run_id"] == 21

    def test_generate_rules_from_contract_inline_content(self):
        tools = _register_tools()

        with (
            patch("server.tools.utils.get_obo_client") as mock_obo,
            patch(
                "server.tools.utils.stage_bytes_to_results_volume",
                return_value="/Volumes/c/dqx_mcp_tmp/mcp_results/staged_y.yaml",
            ) as mock_stage,
            patch("server.tools.utils.submit_job_async", return_value=22) as mock_submit,
        ):
            result = tools["generate_rules_from_contract"](contract_content="kind: DataContract")

        # Inline content needs no file access at all — no OBO client, just stage + submit.
        mock_obo.assert_not_called()
        assert mock_stage.call_args[0][0] == b"kind: DataContract"
        assert mock_submit.call_args[0][1]["contract_file"] == "/Volumes/c/dqx_mcp_tmp/mcp_results/staged_y.yaml"
        assert result["run_id"] == 22

    def test_generate_rules_from_contract_requires_exactly_one_source(self):
        import pytest

        tools = _register_tools()
        with pytest.raises(ValueError, match="Provide either"):
            tools["generate_rules_from_contract"]()
        with pytest.raises(ValueError, match="only one"):
            tools["generate_rules_from_contract"](contract_file="/Volumes/x", contract_content="y")

    def test_generate_rules_from_contract_has_no_text_rules_param(self):
        """The deterministic-only tool must not expose a process_text_rules knob."""
        import inspect

        tools = _register_tools()
        params = inspect.signature(tools["generate_rules_from_contract"]).parameters
        assert "process_text_rules" not in params

    def test_load_checks_table_backend_routes_through_obo_view(self):
        tools = _register_tools()

        with (
            patch("server.tools.utils.get_obo_client"),
            patch("server.tools.utils.get_warehouse_id", return_value="wh123"),
            patch("server.tools.utils.create_temp_view", return_value="dqx_mcp.dqx_mcp_tmp.v_chk") as mock_create,
            patch("server.tools.utils.submit_job_async", return_value=22) as mock_submit,
            patch.dict("os.environ", _ENV),
        ):
            result = tools["load_checks"]("catalog.schema.checks")

        # Table-backed reads go through a definer's-rights OBO view; the runner drops it.
        mock_create.assert_called_once()
        mock_submit.assert_called_once_with(
            "load_checks",
            {
                "run_config_name": "default",
                "location": "dqx_mcp.dqx_mcp_tmp.v_chk",
                "view_name": "dqx_mcp.dqx_mcp_tmp.v_chk",
            },
        )
        assert result["run_id"] == 22

    def test_load_checks_file_backend_verifies_read_access(self):
        tools = _register_tools()

        with (
            patch("server.tools.utils.get_obo_client"),
            patch("server.tools.utils.verify_obo_read_access") as mock_verify,
            patch("server.tools.utils.create_temp_view") as mock_create,
            patch("server.tools.utils.submit_job_async", return_value=22) as mock_submit,
        ):
            result = tools["load_checks"]("/Volumes/c/s/v/checks.yml")

        # No view for file backends — read access is verified directly, location passes through.
        mock_create.assert_not_called()
        assert mock_verify.call_args[0][1] == "/Volumes/c/s/v/checks.yml"
        mock_submit.assert_called_once_with(
            "load_checks", {"run_config_name": "default", "location": "/Volumes/c/s/v/checks.yml"}
        )
        assert result["run_id"] == 22

    def test_save_checks(self):
        tools = _register_tools()

        with (
            patch("server.tools.utils.submit_job_async", return_value=23) as mock_submit,
            patch("server.tools.utils.get_user_email", return_value="user@example.com"),
            patch.dict("os.environ", _ENV),
        ):
            result = tools["save_checks"]([{"check": "foo"}], "my_checks", mode="overwrite")

        # Outputs go to the caller's per-user schema in the configured catalog — no write pre-check,
        # only the bare output name + catalog + caller travel to the runner.
        mock_submit.assert_called_once_with(
            "save_checks",
            {
                "checks": [{"check": "foo"}],
                "output_name": "my_checks",
                "run_config_name": "default",
                "mode": "overwrite",
                "catalog": "dqx_mcp",
                "grant_to": "user@example.com",
            },
        )
        assert result["run_id"] == 23

    def test_save_checks_rejects_fqn_output_name(self):
        """A dotted / path-like output name is rejected — outputs are bare names in the user schema."""
        import pytest

        tools = _register_tools()
        with patch.dict("os.environ", _ENV):
            with pytest.raises(ValueError, match="Invalid output name"):
                tools["save_checks"]([{"check": "foo"}], "catalog.schema.table")

    def test_save_checks_rejects_bad_mode(self):
        import pytest

        tools = _register_tools()
        with patch.dict("os.environ", _ENV):
            with pytest.raises(ValueError, match="Invalid mode"):
                tools["save_checks"]([{"check": "foo"}], "my_checks", mode="upsert")


class TestApplyChecksAndSaveToTable:
    """Test that apply_checks_and_save_to_table creates a view and submits a job."""

    def test_creates_view_and_submits_job(self):
        tools = _register_tools()

        with (
            patch("server.tools.utils.get_obo_client"),
            patch("server.tools.utils.get_warehouse_id", return_value="wh123"),
            patch("server.tools.utils.create_temp_view", return_value="dqx_mcp.dqx_mcp_tmp.v_abc") as mock_create,
            patch("server.tools.utils.submit_job_async", return_value=24) as mock_submit,
            patch("server.tools.utils.get_user_email", return_value="user@example.com"),
            patch.dict("os.environ", _ENV),
        ):
            result = tools["apply_checks_and_save_to_table"](
                "catalog.schema.orders",
                [{"check": "foo"}],
                "orders_out",
                quarantine_name="orders_quarantine",
            )

        mock_create.assert_called_once()
        assert mock_submit.call_args[0][0] == "apply_checks_and_save_to_table"
        job_params = mock_submit.call_args[0][1]
        assert job_params["view_name"] == "dqx_mcp.dqx_mcp_tmp.v_abc"
        assert job_params["checks"] == [{"check": "foo"}]
        # Bare output names + the configured catalog travel to the runner, which resolves them to
        # the caller's per-user schema. No caller-supplied FQN, no write pre-check.
        assert job_params["output_name"] == "orders_out"
        assert job_params["quarantine_name"] == "orders_quarantine"
        assert job_params["catalog"] == "dqx_mcp"
        assert job_params["mode"] == "append"
        # table_name travels in params; the runner drops the temp view itself (no server metadata).
        assert job_params["table_name"] == "catalog.schema.orders"
        # the calling user is forwarded so the runner can grant them access to the outputs
        assert job_params["grant_to"] == "user@example.com"
        assert "metadata" not in mock_submit.call_args.kwargs
        assert result["run_id"] == 24

    def test_rejects_fqn_output_name(self):
        import pytest

        tools = _register_tools()
        with (
            patch("server.tools.utils.get_obo_client"),
            patch("server.tools.utils.get_warehouse_id", return_value="wh123"),
            patch.dict("os.environ", _ENV),
        ):
            with pytest.raises(ValueError, match="Invalid output name"):
                tools["apply_checks_and_save_to_table"](
                    "catalog.schema.orders", [{"check": "foo"}], "catalog.schema.orders_out"
                )
