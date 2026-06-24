import json
from unittest.mock import MagicMock, create_autospec, patch

import pytest

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    Run,
    RunOutput,
    NotebookOutput,
    RunState,
    RunLifeCycleState,
    RunResultState,
)


_JOB_ID_ENV = {"DQX_RUNNER_JOB_ID": "42"}


def _make_terminated_run(result_state: RunResultState, run_id: int = 123) -> MagicMock:
    """Build a mock Run that has finished, with a single task."""
    run = MagicMock(spec=Run)
    run.run_id = run_id
    run.run_page_url = f"https://workspace.databricks.com/jobs/{run_id}"
    run.state = MagicMock(spec=RunState)
    run.state.life_cycle_state = RunLifeCycleState.TERMINATED
    run.state.result_state = result_state
    run.state.state_message = "boom"
    task_run = MagicMock()
    task_run.run_id = 456
    run.tasks = [task_run]
    return run


class TestGetOboClient:
    def test_returns_obo_client_when_token_present(self):
        from server.utils import get_obo_client, _user_token_var

        token = _user_token_var.set(("https://host.databricks.com", "user-token-123"))
        try:
            client = get_obo_client()
            assert client is not None
        finally:
            _user_token_var.reset(token)

    def test_raises_when_no_token(self):
        from server.utils import get_obo_client, _user_token_var

        token = _user_token_var.set(None)
        try:
            with pytest.raises(RuntimeError, match="No OBO token"):
                get_obo_client()
        finally:
            _user_token_var.reset(token)


class TestExecuteSql:
    def test_executes_query_and_returns_rows(self):
        from server.utils import execute_sql

        ws = create_autospec(WorkspaceClient)
        mock_result = MagicMock()
        mock_result.status.state = "SUCCEEDED"
        mock_col_a = MagicMock()
        mock_col_a.name = "col_name"
        mock_col_b = MagicMock()
        mock_col_b.name = "data_type"
        mock_result.manifest.schema.columns = [mock_col_a, mock_col_b]
        mock_result.result.data_array = [["id", "INT"], ["name", "STRING"]]
        ws.statement_execution.execute_statement.return_value = mock_result

        rows = execute_sql(ws, "DESCRIBE TABLE catalog.schema.table", warehouse_id="wh123")
        assert rows == [{"col_name": "id", "data_type": "INT"}, {"col_name": "name", "data_type": "STRING"}]

    def test_raises_on_failed_query(self):
        from server.utils import execute_sql

        ws = create_autospec(WorkspaceClient)
        mock_result = MagicMock()
        mock_result.status.state = "FAILED"
        mock_result.status.error = MagicMock()
        mock_result.status.error.message = "Table not found"
        ws.statement_execution.execute_statement.return_value = mock_result

        with pytest.raises(RuntimeError, match="SQL query failed"):
            execute_sql(ws, "DESCRIBE TABLE bad.table", warehouse_id="wh123")

    def test_returns_empty_list_when_no_results(self):
        from server.utils import execute_sql

        ws = create_autospec(WorkspaceClient)
        mock_result = MagicMock()
        mock_result.status.state = "SUCCEEDED"
        mock_result.manifest.schema.columns = []
        mock_result.result = None
        ws.statement_execution.execute_statement.return_value = mock_result

        rows = execute_sql(ws, "DROP VIEW IF EXISTS foo", warehouse_id="wh123")
        assert rows == []


class TestTempViews:
    def test_create_temp_view_returns_fqn(self):
        from server.utils import create_temp_view

        ws = create_autospec(WorkspaceClient)
        mock_result = MagicMock()
        mock_result.status.state = "SUCCEEDED"
        mock_result.manifest.schema.columns = []
        mock_result.result = None
        ws.statement_execution.execute_statement.return_value = mock_result

        view_fqn = create_temp_view(
            ws,
            "my_catalog.my_schema.my_table",
            catalog="dqx_mcp",
            schema="tmp",
            warehouse_id="wh123",
        )

        assert view_fqn.startswith("dqx_mcp.tmp.v_")
        call_args = ws.statement_execution.execute_statement.call_args
        sql = call_args.kwargs["statement"]
        assert "CREATE VIEW" in sql
        assert "`my_catalog`.`my_schema`.`my_table`" in sql

    def test_drop_view(self):
        from server.utils import drop_view

        ws = create_autospec(WorkspaceClient)
        mock_result = MagicMock()
        mock_result.status.state = "SUCCEEDED"
        mock_result.manifest.schema.columns = []
        mock_result.result = None
        ws.statement_execution.execute_statement.return_value = mock_result

        drop_view(ws, "dqx_mcp.tmp.v_abc123", warehouse_id="wh123")

        call_args = ws.statement_execution.execute_statement.call_args
        sql = call_args.kwargs["statement"]
        assert "DROP VIEW IF EXISTS" in sql
        assert "`dqx_mcp`.`tmp`.`v_abc123`" in sql

    def test_create_temp_view_validates_table_name(self):
        from server.utils import create_temp_view

        ws = create_autospec(WorkspaceClient)

        with pytest.raises(ValueError, match="must be fully qualified"):
            create_temp_view(ws, "just_a_table", catalog="c", schema="s", warehouse_id="wh")

    @pytest.mark.parametrize(
        "malicious_name",
        [
            "cat.sch.t; DROP TABLE x",
            "cat.sch.t WHERE 1=1 UNION SELECT * FROM secrets",
            "cat.sch.t`; --",
            "cat.sch.t OR 1=1",
            "cat`.`sch`.`t",
            "cat.sch.t\nDROP TABLE x",
        ],
    )
    def test_create_temp_view_rejects_sql_injection(self, malicious_name):
        from server.utils import create_temp_view

        ws = create_autospec(WorkspaceClient)

        with pytest.raises(ValueError, match="Invalid|must be fully qualified"):
            create_temp_view(ws, malicious_name, catalog="c", schema="s", warehouse_id="wh")

    def test_create_temp_view_backtick_quotes_identifiers(self):
        from server.utils import create_temp_view

        ws = create_autospec(WorkspaceClient)
        mock_result = MagicMock()
        mock_result.status.state = "SUCCEEDED"
        mock_result.manifest.schema.columns = []
        mock_result.result = None
        ws.statement_execution.execute_statement.return_value = mock_result

        create_temp_view(ws, "my_cat.my_sch.my_table", catalog="dqx", schema="tmp", warehouse_id="wh")

        sql = ws.statement_execution.execute_statement.call_args.kwargs["statement"]
        assert "`my_cat`.`my_sch`.`my_table`" in sql
        assert "`dqx`.`tmp`." in sql

    def test_drop_view_swallows_errors(self):
        from server.utils import drop_view

        ws = create_autospec(WorkspaceClient)
        ws.statement_execution.execute_statement.side_effect = RuntimeError("connection lost")

        # Should not raise
        drop_view(ws, "dqx_mcp.tmp.v_abc123", warehouse_id="wh123")


class TestSubmitJobAsync:
    def test_returns_run_id_and_triggers_runner_job(self):
        from server.utils import submit_job_async

        ws = create_autospec(WorkspaceClient)
        ws.jobs.run_now.return_value = MagicMock(run_id=123)

        with (
            patch("server.utils._get_sp_client", return_value=ws),
            patch("server.utils._maybe_sweep_stale_views"),
            patch.dict("os.environ", _JOB_ID_ENV),
        ):
            run_id = submit_job_async("profile_table", {"view_name": "c.s.v_abc"})

        assert run_id == 123
        ws.jobs.run_now.assert_called_once()
        assert ws.jobs.run_now.call_args.kwargs["job_id"] == 42

    def test_passes_operation_and_params_as_notebook_params(self):
        from server.utils import submit_job_async

        ws = create_autospec(WorkspaceClient)
        ws.jobs.run_now.return_value = MagicMock(run_id=123)

        with (
            patch("server.utils._get_sp_client", return_value=ws),
            patch("server.utils._maybe_sweep_stale_views"),
            patch.dict("os.environ", _JOB_ID_ENV),
        ):
            submit_job_async("profile_table", {"view_name": "c.s.v_abc", "columns": ["a"]})

        notebook_params = ws.jobs.run_now.call_args.kwargs["notebook_params"]
        assert notebook_params["operation"] == "profile_table"
        assert json.loads(notebook_params["params"]) == {"view_name": "c.s.v_abc", "columns": ["a"]}

    def test_raises_when_no_job_id(self):
        from server.utils import submit_job_async

        ws = create_autospec(WorkspaceClient)

        with (
            patch("server.utils._get_sp_client", return_value=ws),
            patch("server.utils._maybe_sweep_stale_views"),
            patch.dict("os.environ", {}, clear=True),
        ):
            with pytest.raises(RuntimeError, match="DQX_RUNNER_JOB_ID not set"):
                submit_job_async("profile_table", {"view_name": "c.s.v_abc"})
        ws.jobs.run_now.assert_not_called()


class TestGetRunStatus:
    def test_completed_returns_result_with_table_name(self):
        from server.utils import get_run_status

        ws = create_autospec(WorkspaceClient)
        ws.jobs.get_run.return_value = _make_terminated_run(RunResultState.SUCCESS)
        output = MagicMock(spec=RunOutput)
        output.notebook_output = MagicMock(spec=NotebookOutput)
        # table_name is echoed by the runner into its result; get_run_status returns it as-is.
        output.notebook_output.result = json.dumps({"profiles": [], "table_name": "c.s.t"})
        ws.jobs.get_run_output.return_value = output

        with patch("server.utils._get_sp_client", return_value=ws):
            result = get_run_status(123)

        assert result["status"] == "completed"
        assert result["run_id"] == 123
        assert result["result"]["profiles"] == []
        assert result["result"]["table_name"] == "c.s.t"
        ws.jobs.get_run_output.assert_called_once_with(456)

    def test_does_not_drop_view_or_keep_state(self):
        # Cleanup moved into the runner job; get_run_status must not attempt any view drop.
        from server.utils import get_run_status

        ws = create_autospec(WorkspaceClient)
        ws.jobs.get_run.return_value = _make_terminated_run(RunResultState.SUCCESS)
        output = MagicMock(spec=RunOutput)
        output.notebook_output = MagicMock(spec=NotebookOutput)
        output.notebook_output.result = json.dumps({"ok": True})
        ws.jobs.get_run_output.return_value = output

        with (
            patch("server.utils._get_sp_client", return_value=ws),
            patch("server.utils.drop_view") as mock_drop,
        ):
            result = get_run_status(123)

        assert result["status"] == "completed"
        mock_drop.assert_not_called()

    def test_failed_run_returns_error(self):
        from server.utils import get_run_status

        ws = create_autospec(WorkspaceClient)
        ws.jobs.get_run.return_value = _make_terminated_run(RunResultState.FAILED)

        with patch("server.utils._get_sp_client", return_value=ws):
            result = get_run_status(123)

        assert result["status"] == "failed"
        assert "boom" in result["error"]
        ws.jobs.get_run_output.assert_not_called()


class TestSweepStaleViews:
    def test_drops_only_views_older_than_ttl(self):
        import time
        from server.utils import sweep_stale_views

        now = int(time.time())
        ws = create_autospec(WorkspaceClient)
        rows = [
            {"viewName": f"v_{now - 100000}_abc123"},  # stale -> drop
            {"viewName": f"v_{now}_def456"},  # fresh -> keep
            {"viewName": "some_other_table"},  # not a temp view -> ignore
        ]

        with (
            patch("server.utils.execute_sql", return_value=rows),
            patch("server.utils.drop_view") as mock_drop,
        ):
            dropped = sweep_stale_views(ws, "cat", "tmp", "wh", ttl_seconds=3600)

        assert dropped == 1
        mock_drop.assert_called_once_with(ws, f"cat.tmp.v_{now - 100000}_abc123", warehouse_id="wh")

    def test_returns_zero_when_listing_fails(self):
        from server.utils import sweep_stale_views

        ws = create_autospec(WorkspaceClient)
        with (
            patch("server.utils.execute_sql", side_effect=RuntimeError("no warehouse")),
            patch("server.utils.drop_view") as mock_drop,
        ):
            dropped = sweep_stale_views(ws, "cat", "tmp", "wh")

        assert dropped == 0
        mock_drop.assert_not_called()

    def test_still_running_returns_running(self):
        from server.utils import get_run_status

        ws = create_autospec(WorkspaceClient)
        run = MagicMock(spec=Run)
        run.state = MagicMock(spec=RunState)
        run.state.life_cycle_state = RunLifeCycleState.RUNNING
        ws.jobs.get_run.return_value = run

        with patch("server.utils._get_sp_client", return_value=ws), patch("time.sleep"):
            result = get_run_status(123)

        assert result["status"] == "running"
        assert result["run_id"] == 123


class TestOBOAuthMiddleware:
    @pytest.mark.anyio
    async def test_extracts_token_and_email(self):
        from server.utils import OBOAuthMiddleware, _user_token_var, _user_email_var

        captured = {}

        async def app(scope, receive, send):
            captured["token"] = _user_token_var.get(None)
            captured["email"] = _user_email_var.get(None)

        middleware = OBOAuthMiddleware(app)
        scope = {
            "type": "http",
            "headers": [
                (b"x-forwarded-access-token", b"user-token"),
                (b"x-forwarded-email", b"user@example.com"),
            ],
        }

        with patch.dict("os.environ", {"DATABRICKS_HOST": "https://host.com"}):
            await middleware(scope, None, None)

        assert captured["token"] == ("https://host.com", "user-token")
        assert captured["email"] == "user@example.com"

    @pytest.mark.anyio
    async def test_no_token_sets_none(self):
        from server.utils import OBOAuthMiddleware, _user_token_var, _user_email_var

        captured = {}

        async def app(scope, receive, send):
            captured["token"] = _user_token_var.get(None)
            captured["email"] = _user_email_var.get(None)

        middleware = OBOAuthMiddleware(app)
        scope = {"type": "http", "headers": []}

        await middleware(scope, None, None)

        assert captured["token"] is None
        assert captured["email"] is None

    @pytest.mark.anyio
    async def test_passes_through_non_http(self):
        from server.utils import OBOAuthMiddleware

        called = {"count": 0}

        async def app(scope, receive, send):
            called["count"] += 1

        middleware = OBOAuthMiddleware(app)
        await middleware({"type": "lifespan"}, None, None)

        assert called["count"] == 1
