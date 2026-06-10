import json
from unittest.mock import MagicMock, create_autospec, patch

import pytest

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    Job, JobSettings, Run, RunOutput, NotebookOutput, NotebookTask,
    RunState, RunLifeCycleState, RunResultState, Task,
)


def _make_mock_ws(notebook_output_json: str, run_id: int = 123) -> MagicMock:
    """Create a mock WorkspaceClient that simulates a successful submit() call."""
    ws = create_autospec(WorkspaceClient)

    # Mock jobs.get() to return the pre-deployed job definition
    job = MagicMock(spec=Job)
    job.settings = MagicMock(spec=JobSettings)
    task = MagicMock(spec=Task)
    task.notebook_task = MagicMock(spec=NotebookTask)
    task.notebook_task.notebook_path = "/Workspace/Users/user@co.com/.bundle/mcp-dqx/dev/files/notebooks/runner"
    job.settings.tasks = [task]
    ws.jobs.get.return_value = job

    # Mock the run object returned by .result()
    run = MagicMock(spec=Run)
    run.run_id = run_id
    run.run_page_url = f"https://workspace.databricks.com/jobs/{run_id}"
    run.state = MagicMock(spec=RunState)
    run.state.life_cycle_state = RunLifeCycleState.TERMINATED
    run.state.result_state = RunResultState.SUCCESS

    # Mock submit() -> Wait -> .result() -> Run
    wait_obj = MagicMock()
    wait_obj.result.return_value = run
    ws.jobs.submit.return_value = wait_obj

    # Mock get_run_output()
    output = MagicMock(spec=RunOutput)
    output.notebook_output = MagicMock(spec=NotebookOutput)
    output.notebook_output.result = notebook_output_json
    output.notebook_output.truncated = False
    task_run = MagicMock()
    task_run.run_id = 456
    run.tasks = [task_run]
    ws.jobs.get_run_output.return_value = output

    return ws


_JOB_ID_ENV = {"DQX_RUNNER_JOB_ID": "42"}


def _reset_notebook_path_cache():
    """Reset the cached notebook path between tests."""
    import server.utils as utils_module
    utils_module._notebook_path = None


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
            ws, "my_catalog.my_schema.my_table",
            catalog="dqx_mcp", schema="tmp",
            warehouse_id="wh123",
        )

        assert view_fqn.startswith("dqx_mcp.tmp.v_")
        call_args = ws.statement_execution.execute_statement.call_args
        sql = call_args.kwargs["statement"]
        assert "CREATE VIEW" in sql
        assert "my_catalog.my_schema.my_table" in sql

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
        assert "dqx_mcp.tmp.v_abc123" in sql

    def test_create_temp_view_validates_table_name(self):
        from server.utils import create_temp_view

        ws = create_autospec(WorkspaceClient)

        with pytest.raises(ValueError, match="must be fully qualified"):
            create_temp_view(ws, "just_a_table", catalog="c", schema="s", warehouse_id="wh")

    def test_drop_view_swallows_errors(self):
        from server.utils import drop_view

        ws = create_autospec(WorkspaceClient)
        ws.statement_execution.execute_statement.side_effect = RuntimeError("connection lost")

        # Should not raise
        drop_view(ws, "dqx_mcp.tmp.v_abc123", warehouse_id="wh123")


class TestSubmitNotebookJob:
    def setup_method(self):
        _reset_notebook_path_cache()

    def test_successful_submission(self):
        from server.utils import submit_notebook_job

        expected_result = {"columns": [], "row_count": 0}
        ws = _make_mock_ws(json.dumps(expected_result))

        with patch("server.utils._get_sp_client", return_value=ws), \
             patch.dict("os.environ", _JOB_ID_ENV):
            result = submit_notebook_job("profile_table", {"view_name": "c.s.v_abc"})

        assert result == expected_result
        ws.jobs.submit.assert_called_once()
        # No run_as in the call
        call_kwargs = ws.jobs.submit.call_args.kwargs
        assert "run_as" not in call_kwargs

    def test_passes_operation_and_params_to_notebook(self):
        from server.utils import submit_notebook_job

        ws = _make_mock_ws(json.dumps({"result": "ok"}))

        with patch("server.utils._get_sp_client", return_value=ws), \
             patch.dict("os.environ", _JOB_ID_ENV):
            submit_notebook_job("profile_table", {"view_name": "c.s.v_abc", "columns": ["a"]})

        call_kwargs = ws.jobs.submit.call_args.kwargs
        tasks = call_kwargs["tasks"]
        base_params = tasks[0].notebook_task.base_parameters
        assert base_params["operation"] == "profile_table"
        assert json.loads(base_params["params"]) == {"view_name": "c.s.v_abc", "columns": ["a"]}

    def test_raises_when_no_job_id(self):
        from server.utils import submit_notebook_job

        ws = _make_mock_ws(json.dumps({"ok": True}))

        with patch("server.utils._get_sp_client", return_value=ws), \
             patch.dict("os.environ", {}, clear=True):
            with pytest.raises(RuntimeError, match="DQX_RUNNER_JOB_ID not set"):
                submit_notebook_job("profile_table", {"view_name": "c.s.v_abc"})

    def test_job_failure_raises_error(self):
        from server.utils import submit_notebook_job

        ws = _make_mock_ws("{}")
        run = MagicMock(spec=Run)
        run.run_id = 999
        run.run_page_url = "https://workspace.databricks.com/jobs/999"
        run.tasks = [MagicMock(run_id=1000)]
        wait_obj = MagicMock()
        wait_obj.result.return_value = run
        ws.jobs.submit.return_value = wait_obj

        output = MagicMock(spec=RunOutput)
        output.notebook_output = None
        output.error = "Something went wrong"
        ws.jobs.get_run_output.return_value = output

        with patch("server.utils._get_sp_client", return_value=ws), \
             patch.dict("os.environ", _JOB_ID_ENV):
            with pytest.raises(RuntimeError, match="DQX job failed"):
                submit_notebook_job("run_checks", {"view_name": "c.s.v_abc", "checks": []})

    def test_resolves_notebook_path_from_job(self):
        from server.utils import submit_notebook_job

        ws = _make_mock_ws(json.dumps({"ok": True}))

        with patch("server.utils._get_sp_client", return_value=ws), \
             patch.dict("os.environ", _JOB_ID_ENV):
            submit_notebook_job("validate_checks", {"checks": []})

        ws.jobs.get.assert_called_once_with(42)

    def test_caches_notebook_path(self):
        from server.utils import submit_notebook_job

        ws = _make_mock_ws(json.dumps({"ok": True}))

        with patch("server.utils._get_sp_client", return_value=ws), \
             patch.dict("os.environ", _JOB_ID_ENV):
            submit_notebook_job("validate_checks", {"checks": []})
            submit_notebook_job("list_available_checks", {})

        ws.jobs.get.assert_called_once()


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
