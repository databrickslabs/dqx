import json
from unittest.mock import MagicMock, create_autospec, patch

import pytest

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Run, RunOutput, NotebookOutput, RunState, RunLifeCycleState, RunResultState


def _make_mock_ws(notebook_output_json: str, run_id: int = 123) -> MagicMock:
    """Create a mock WorkspaceClient that simulates a successful job run."""
    ws = create_autospec(WorkspaceClient)

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


class TestSubmitNotebookJob:
    def test_successful_submission(self):
        from server.utils import submit_notebook_job

        expected_result = {"table_name": "catalog.schema.table", "columns": [], "row_count": 0}
        ws = _make_mock_ws(json.dumps(expected_result))

        with patch("server.utils._get_sp_client", return_value=ws):
            result = submit_notebook_job("get_table_schema", {"table_name": "catalog.schema.table"})

        assert result == expected_result
        ws.jobs.submit.assert_called_once()

    def test_passes_operation_and_params_to_notebook(self):
        from server.utils import submit_notebook_job

        ws = _make_mock_ws(json.dumps({"result": "ok"}))

        with patch("server.utils._get_sp_client", return_value=ws):
            submit_notebook_job("profile_table", {"table_name": "t", "columns": ["a"]})

        call_kwargs = ws.jobs.submit.call_args.kwargs
        tasks = call_kwargs["tasks"]
        assert len(tasks) == 1
        base_params = tasks[0].notebook_task.base_parameters
        assert base_params["operation"] == "profile_table"
        assert json.loads(base_params["params"]) == {"table_name": "t", "columns": ["a"]}

    def test_run_as_user_when_email_present(self):
        from server.utils import submit_notebook_job, _user_email_var

        ws = _make_mock_ws(json.dumps({"ok": True}))

        token = _user_email_var.set("user@company.com")
        try:
            with patch("server.utils._get_sp_client", return_value=ws):
                submit_notebook_job("get_table_schema", {"table_name": "t"})
        finally:
            _user_email_var.reset(token)

        call_kwargs = ws.jobs.submit.call_args.kwargs
        assert call_kwargs["run_as"].user_name == "user@company.com"

    def test_no_run_as_when_no_email(self):
        from server.utils import submit_notebook_job, _user_email_var

        ws = _make_mock_ws(json.dumps({"ok": True}))

        token = _user_email_var.set(None)
        try:
            with patch("server.utils._get_sp_client", return_value=ws):
                submit_notebook_job("get_table_schema", {"table_name": "t"})
        finally:
            _user_email_var.reset(token)

        call_kwargs = ws.jobs.submit.call_args.kwargs
        assert call_kwargs["run_as"] is None

    def test_job_failure_raises_error(self):
        from server.utils import submit_notebook_job

        ws = create_autospec(WorkspaceClient)

        run = MagicMock(spec=Run)
        run.run_id = 999
        run.run_page_url = "https://workspace.databricks.com/jobs/999"
        run.state = MagicMock(spec=RunState)
        run.state.life_cycle_state = RunLifeCycleState.TERMINATED
        run.state.result_state = RunResultState.FAILED
        run.state.state_message = "Something went wrong"
        run.tasks = [MagicMock(run_id=1000)]

        wait_obj = MagicMock()
        wait_obj.result.return_value = run
        ws.jobs.submit.return_value = wait_obj

        output = MagicMock(spec=RunOutput)
        output.notebook_output = None
        output.error = "Something went wrong"
        ws.jobs.get_run_output.return_value = output

        with patch("server.utils._get_sp_client", return_value=ws):
            with pytest.raises(RuntimeError, match="DQX job failed"):
                submit_notebook_job("run_checks", {"table_name": "t", "checks": []})

    def test_notebook_returns_error_dict(self):
        from server.utils import submit_notebook_job

        error_result = {"error": "AnalysisException: Table not found"}
        ws = _make_mock_ws(json.dumps(error_result))

        with patch("server.utils._get_sp_client", return_value=ws):
            result = submit_notebook_job("get_table_schema", {"table_name": "bad.table"})

        assert result == error_result

    def test_uses_configured_notebook_path(self):
        from server.utils import submit_notebook_job

        ws = _make_mock_ws(json.dumps({"ok": True}))

        with patch("server.utils._get_sp_client", return_value=ws), \
             patch.dict("os.environ", {"DQX_RUNNER_NOTEBOOK_PATH": "/custom/path/runner"}):
            submit_notebook_job("get_table_schema", {"table_name": "t"})

        tasks = ws.jobs.submit.call_args.kwargs["tasks"]
        assert tasks[0].notebook_task.notebook_path == "/custom/path/runner"
