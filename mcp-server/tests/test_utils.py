import json
import logging
import os
from unittest.mock import MagicMock, call, create_autospec, patch

import pytest

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    JobParameter,
    Run,
    RunOutput,
    RunState,
    RunLifeCycleState,
    RunResultState,
)

_JOB_ID_ENV = {"DQX_RUNNER_JOB_ID": "42", "DQX_CATALOG": "cat", "DQX_TMP_SCHEMA": "dqx_mcp_tmp"}


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
    # Recorded submitter matches the caller the TestGetRunStatus autouse fixture sets.
    run.job_parameters = [JobParameter(name="requesting_user", value="user@example.com")]
    return run


def _make_running_run(run_id: int = 123) -> MagicMock:
    """Build a mock Run that is still in progress."""
    run = MagicMock(spec=Run)
    run.run_id = run_id
    run.run_page_url = f"https://workspace.databricks.com/jobs/{run_id}"
    run.state = MagicMock(spec=RunState)
    run.state.life_cycle_state = RunLifeCycleState.RUNNING
    run.state.result_state = None
    run.state.state_message = ""
    run.tasks = []
    run.job_parameters = [JobParameter(name="requesting_user", value="user@example.com")]
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


class TestGetUserEmail:
    def test_returns_email_when_present(self):
        from server.utils import get_user_email, _user_email_var

        token = _user_email_var.set("alice@example.com")
        try:
            assert get_user_email() == "alice@example.com"
        finally:
            _user_email_var.reset(token)

    def test_returns_none_without_user_context(self):
        from server.utils import get_user_email, _user_email_var

        token = _user_email_var.set(None)
        try:
            assert get_user_email() is None
        finally:
            _user_email_var.reset(token)


class TestRequestContextFilter:
    def _record(self) -> logging.LogRecord:
        return logging.LogRecord("n", logging.INFO, "f", 1, "msg", None, None)

    def test_injects_defaults_outside_request(self):
        from server.utils import RequestContextFilter

        record = self._record()
        assert RequestContextFilter().filter(record) is True
        assert record.request_id == "-"
        assert record.user == "-"

    def test_injects_request_id_and_user(self):
        from server.utils import RequestContextFilter, _request_id_var, _user_email_var

        rid = _request_id_var.set("abc123")
        em = _user_email_var.set("alice@example.com")
        try:
            record = self._record()
            RequestContextFilter().filter(record)
            assert record.request_id == "abc123"
            assert record.user == "alice@example.com"
        finally:
            _request_id_var.reset(rid)
            _user_email_var.reset(em)

    def test_sanitizes_newlines_in_user(self):
        from server.utils import RequestContextFilter, _user_email_var

        em = _user_email_var.set("a@b.com\nINJECTED log line")
        try:
            record = self._record()
            RequestContextFilter().filter(record)
            assert "\n" not in record.user and "\r" not in record.user
        finally:
            _user_email_var.reset(em)


class TestConfigureLogging:
    def test_idempotent_and_attaches_filter(self):
        import server.utils as u

        saved_handlers = logging.getLogger().handlers[:]
        saved_flag = u._logging_configured
        try:
            u._logging_configured = False
            u.configure_logging()
            root = logging.getLogger()
            assert any(isinstance(f, u.RequestContextFilter) for h in root.handlers for f in h.filters)
            count = len(root.handlers)
            u.configure_logging()  # second call must be a no-op
            assert len(root.handlers) == count
        finally:
            logging.getLogger().handlers[:] = saved_handlers
            u._logging_configured = saved_flag


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
        mock_result.result.next_chunk_index = None  # single chunk
        ws.statement_execution.execute_statement.return_value = mock_result

        rows = execute_sql(ws, "DESCRIBE TABLE catalog.schema.table", warehouse_id="wh123")
        assert rows == [{"col_name": "id", "data_type": "INT"}, {"col_name": "name", "data_type": "STRING"}]
        # A single-chunk result must not fetch further chunks.
        ws.statement_execution.get_statement_result_chunk_n.assert_not_called()

    def test_paginates_across_chunks(self):
        """A multi-chunk result set is fully read by following next_chunk_index (not truncated)."""
        from server.utils import execute_sql

        ws = create_autospec(WorkspaceClient)
        mock_result = MagicMock()
        mock_result.status.state = "SUCCEEDED"
        mock_result.statement_id = "stmt-1"
        col = MagicMock()
        col.name = "viewName"
        mock_result.manifest.schema.columns = [col]
        # First chunk (inline in execute_statement) points at chunk 1.
        mock_result.result.data_array = [["v_1"], ["v_2"]]
        mock_result.result.next_chunk_index = 1
        ws.statement_execution.execute_statement.return_value = mock_result

        # Second (final) chunk fetched via get_statement_result_chunk_n; no further chunk.
        chunk1 = MagicMock()
        chunk1.data_array = [["v_3"]]
        chunk1.next_chunk_index = None
        ws.statement_execution.get_statement_result_chunk_n.return_value = chunk1

        rows = execute_sql(ws, "SHOW VIEWS IN c.s", warehouse_id="wh123")
        assert rows == [{"viewName": "v_1"}, {"viewName": "v_2"}, {"viewName": "v_3"}]
        ws.statement_execution.get_statement_result_chunk_n.assert_called_once_with("stmt-1", 1)

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

    def test_none_status_raises_clean_error_not_attributeerror(self):
        """A result with status=None must raise a clean RuntimeError, not AttributeError on .error."""
        from server.utils import execute_sql

        ws = create_autospec(WorkspaceClient)
        mock_result = MagicMock()
        mock_result.status = None  # some error/edge states return no status
        mock_result.statement_id = "s1"
        ws.statement_execution.execute_statement.return_value = mock_result

        with pytest.raises(RuntimeError, match="SQL query failed: UNKNOWN"):
            execute_sql(ws, "SELECT 1", warehouse_id="wh123")


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
            schema="dqx_mcp_tmp",
            warehouse_id="wh123",
        )

        assert view_fqn.startswith("dqx_mcp.dqx_mcp_tmp.v_")
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

        drop_view(ws, "dqx_mcp.dqx_mcp_tmp.v_abc123", warehouse_id="wh123")

        call_args = ws.statement_execution.execute_statement.call_args
        sql = call_args.kwargs["statement"]
        assert "DROP VIEW IF EXISTS" in sql
        assert "`dqx_mcp`.`dqx_mcp_tmp`.`v_abc123`" in sql

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

        create_temp_view(ws, "my_cat.my_sch.my_table", catalog="dqx", schema="dqx_mcp_tmp", warehouse_id="wh")

        sql = ws.statement_execution.execute_statement.call_args.kwargs["statement"]
        assert "`my_cat`.`my_sch`.`my_table`" in sql
        assert "`dqx`.`dqx_mcp_tmp`." in sql

    def test_drop_view_swallows_errors(self):
        from server.utils import drop_view

        ws = create_autospec(WorkspaceClient)
        ws.statement_execution.execute_statement.side_effect = RuntimeError("connection lost")

        # Should not raise
        drop_view(ws, "dqx_mcp.dqx_mcp_tmp.v_abc123", warehouse_id="wh123")


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

    def test_passes_operation_params_and_results_volume_as_job_parameters(self):
        from server.utils import submit_job_async

        ws = create_autospec(WorkspaceClient)
        ws.jobs.run_now.return_value = MagicMock(run_id=123)

        with (
            patch("server.utils._get_sp_client", return_value=ws),
            patch("server.utils._maybe_sweep_stale_views"),
            patch.dict("os.environ", _JOB_ID_ENV),
        ):
            submit_job_async("profile_table", {"view_name": "c.s.v_abc", "columns": ["a"]})

        # Wheel task → job_parameters (not notebook_params), incl. the results volume path.
        job_parameters = ws.jobs.run_now.call_args.kwargs["job_parameters"]
        assert job_parameters["operation"] == "profile_table"
        assert json.loads(job_parameters["params"]) == {"view_name": "c.s.v_abc", "columns": ["a"]}
        assert job_parameters["results_volume"] == "/Volumes/cat/dqx_mcp_tmp/mcp_results"
        # requesting_user is recorded on the run so get_run_status can authorize the reader (IDOR guard).
        assert "requesting_user" in job_parameters

    def test_stamps_requesting_user_from_obo_email(self):
        from server.utils import submit_job_async, _user_email_var

        ws = create_autospec(WorkspaceClient)
        ws.jobs.run_now.return_value = MagicMock(run_id=123)
        tok = _user_email_var.set("alice@example.com")
        try:
            with (
                patch("server.utils._get_sp_client", return_value=ws),
                patch("server.utils._maybe_sweep_stale_views"),
                patch.dict("os.environ", _JOB_ID_ENV),
            ):
                submit_job_async("profile_table", {"view_name": "c.s.v"})
        finally:
            _user_email_var.reset(tok)
        assert ws.jobs.run_now.call_args.kwargs["job_parameters"]["requesting_user"] == "alice@example.com"

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
    @pytest.fixture(autouse=True)
    def _caller(self):
        """Default caller identity for these tests — matches _make_terminated_run's requesting_user
        so the IDOR ownership guard passes on the happy paths."""
        from server.utils import _user_email_var

        tok = _user_email_var.set("user@example.com")
        yield
        _user_email_var.reset(tok)

    def test_completed_reads_result_file_from_volume(self):
        from server.utils import get_run_status

        ws = create_autospec(WorkspaceClient)
        ws.jobs.get_run.return_value = _make_terminated_run(RunResultState.SUCCESS)
        # table_name is echoed by the runner into its result; get_run_status returns it as-is.
        ws.files.download.return_value.contents.read.return_value = json.dumps(
            {"profiles": [], "table_name": "c.s.t"}
        ).encode()

        with (
            patch("server.utils._get_sp_client", return_value=ws),
            patch.dict("os.environ", {"DQX_CATALOG": "cat", "DQX_TMP_SCHEMA": "dqx_mcp_tmp"}),
        ):
            result = get_run_status(123)

        assert result["status"] == "completed"
        assert result["run_id"] == 123
        assert result["result"]["profiles"] == []
        assert result["result"]["table_name"] == "c.s.t"
        # Result is read from the results volume, keyed by run id.
        assert ws.files.download.call_args[0][0] == "/Volumes/cat/dqx_mcp_tmp/mcp_results/123.json"

    def test_completed_but_missing_result_file_is_failed(self):
        from server.utils import get_run_status

        ws = create_autospec(WorkspaceClient)
        ws.jobs.get_run.return_value = _make_terminated_run(RunResultState.SUCCESS)
        ws.files.download.side_effect = Exception("not found")

        with (
            patch("server.utils._get_sp_client", return_value=ws),
            patch.dict("os.environ", {"DQX_CATALOG": "cat", "DQX_TMP_SCHEMA": "dqx_mcp_tmp"}),
        ):
            result = get_run_status(123)

        assert result["status"] == "failed"
        assert "result file could not be read" in result["error"]

    def test_failed_run_surfaces_task_detail(self):
        from server.utils import get_run_status

        ws = create_autospec(WorkspaceClient)
        ws.jobs.get_run.return_value = _make_terminated_run(RunResultState.FAILED)
        out = MagicMock(spec=RunOutput)
        out.error = "PERMISSION_DENIED: User does not have SELECT on Table foo. SQLSTATE: 42501"
        ws.jobs.get_run_output.return_value = out

        with patch("server.utils._get_sp_client", return_value=ws):
            result = get_run_status(123)

        assert result["status"] == "failed"
        assert "boom" in result["error"]  # job-level state_message
        assert "PERMISSION_DENIED" in result["error"]  # task-level detail now surfaced
        ws.jobs.get_run_output.assert_called_once_with(456)

    def test_waits_for_a_running_job_then_returns_its_result(self):
        """get_run_status absorbs the wait, because an agent cannot sleep between calls.

        Regression guard. It used to poll once and tell the caller to "call again after a short
        wait" — but an LLM agent has no sleep primitive, so it called straight back, its own
        loop detection saw a repeated no-progress action, and it stopped to ask the user. Observed
        against a live deployment: every job-backed tool stalled that way.
        """
        from server.utils import get_run_status

        ws = create_autospec(WorkspaceClient)
        ws.jobs.get_run.side_effect = [
            _make_running_run(),
            _make_running_run(),
            _make_terminated_run(RunResultState.SUCCESS),
        ]
        ws.files.download.return_value.contents.read.return_value = json.dumps({"profiles": []}).encode()

        with (
            patch("server.utils._get_sp_client", return_value=ws),
            patch("server.utils.time.sleep") as sleep,
            patch.dict("os.environ", {"DQX_CATALOG": "cat", "DQX_TMP_SCHEMA": "dqx_mcp_tmp"}),
        ):
            result = get_run_status(123)

        assert result["status"] == "completed", result
        assert ws.jobs.get_run.call_count == 3, "should keep polling until the run is terminal"
        assert sleep.call_count == 2, "should sleep between polls rather than spinning"

    def test_still_running_tells_the_caller_to_call_straight_back(self):
        """The 'running' message must not tell an agent to wait — it cannot, and it will stop."""
        from server.utils import get_run_status

        ws = create_autospec(WorkspaceClient)
        ws.jobs.get_run.return_value = _make_running_run()

        with (
            patch("server.utils._get_sp_client", return_value=ws),
            patch("server.utils.time.sleep"),
        ):
            result = get_run_status(123, wait_seconds=0)

        assert result["status"] == "running"
        message = result["message"].lower()
        assert "again" in message, message
        # The whole point: never instruct a sleepless caller to wait, or to check with the user.
        assert "wait a moment" not in message, message
        assert "confirm" not in message or "do not ask" in message, message

    def test_running_message_reports_the_actual_wait_budget_not_the_default(self):
        """The 'waited Ns' message must reflect the caller's wait_seconds, not the module default.

        Regression guard: the message hardcoded _RUN_WAIT_SECONDS (30s), so a call overriding
        wait_seconds reported a wait that never happened. It must state the budget it actually blocked
        for — the agent reads this number to decide the call was progress, not a stalled retry.
        """
        from server.utils import get_run_status, _RUN_WAIT_SECONDS

        ws = create_autospec(WorkspaceClient)
        ws.jobs.get_run.return_value = _make_running_run()

        # Drive monotonic so the 5s budget expires right after the first poll — deterministic, and no
        # busy-spin (sleep is a no-op): deadline = 1000 + 5, then the loop sees monotonic == 1005.
        with (
            patch("server.utils._get_sp_client", return_value=ws),
            patch("server.utils.time.sleep"),
            patch("server.utils.time.monotonic", side_effect=[1000.0, 1005.0]),
        ):
            result = get_run_status(123, wait_seconds=5)

        assert result["status"] == "running"
        message = result["message"]
        assert "waited 5s" in message, message
        # Guard against the old bug specifically: the default must not leak in when overridden.
        assert f"waited {int(_RUN_WAIT_SECONDS)}s" not in message, message

    def test_the_wait_budget_is_clamped_below_the_client_timeout(self):
        """A configured wait above the ceiling must be capped, not honoured.

        The wait is only safe because it stays under the ~60s at which MCP clients and the Apps front
        door abandon an idle request. An operator setting DQX_RUN_WAIT_SECONDS=300 to "be patient"
        would instead turn every poll of a slow run into a hung request, which is strictly worse than
        reporting 'running' and being called back.
        """
        import importlib

        import server.utils as utils_module

        with patch.dict("os.environ", {"DQX_RUN_WAIT_SECONDS": "300"}):
            reloaded = importlib.reload(utils_module)
            try:
                assert reloaded._RUN_WAIT_SECONDS == reloaded._RUN_WAIT_CEILING_SECONDS
                assert reloaded._RUN_WAIT_CEILING_SECONDS < 60, "the ceiling must stay under the client timeout"
            finally:
                importlib.reload(utils_module)  # restore the default for every later test

    def test_wait_seconds_zero_polls_once(self):
        """An operator (or a test) can opt out of waiting entirely."""
        from server.utils import get_run_status

        ws = create_autospec(WorkspaceClient)
        ws.jobs.get_run.return_value = _make_running_run()

        with (
            patch("server.utils._get_sp_client", return_value=ws),
            patch("server.utils.time.sleep") as sleep,
        ):
            get_run_status(123, wait_seconds=0)

        assert ws.jobs.get_run.call_count == 1
        sleep.assert_not_called()

    def test_not_found_when_run_does_not_exist(self):
        from server.utils import get_run_status
        from databricks.sdk.errors import ResourceDoesNotExist

        ws = create_autospec(WorkspaceClient)
        ws.jobs.get_run.side_effect = ResourceDoesNotExist("Run 999 does not exist.")

        with patch("server.utils._get_sp_client", return_value=ws):
            result = get_run_status(999)

        assert result["status"] == "not_found"
        assert result["run_id"] == 999
        assert "999" in result["error"]
        ws.jobs.get_run_output.assert_not_called()

    def test_unexpected_databricks_error_is_reraised(self):
        from server.utils import get_run_status
        from databricks.sdk.errors import PermissionDenied

        ws = create_autospec(WorkspaceClient)
        ws.jobs.get_run.side_effect = PermissionDenied("nope")

        with patch("server.utils._get_sp_client", return_value=ws):
            with pytest.raises(PermissionDenied):
                get_run_status(123)

    def test_not_found_when_run_belongs_to_other_job(self):
        from server.utils import get_run_status

        ws = create_autospec(WorkspaceClient)
        run = _make_terminated_run(RunResultState.SUCCESS)
        run.job_id = 999  # not the runner job (DQX_RUNNER_JOB_ID=42)
        ws.jobs.get_run.return_value = run

        with (
            patch("server.utils._get_sp_client", return_value=ws),
            patch.dict("os.environ", _JOB_ID_ENV),
        ):
            result = get_run_status(123)

        assert result["status"] == "not_found"
        ws.jobs.get_run_output.assert_not_called()

    def test_matching_job_id_is_accepted(self):
        from server.utils import get_run_status

        ws = create_autospec(WorkspaceClient)
        run = _make_terminated_run(RunResultState.SUCCESS)
        run.job_id = 42  # matches DQX_RUNNER_JOB_ID
        ws.jobs.get_run.return_value = run
        ws.files.download.return_value.contents.read.return_value = json.dumps({"ok": True}).encode()

        with (
            patch("server.utils._get_sp_client", return_value=ws),
            patch.dict("os.environ", _JOB_ID_ENV),
        ):
            result = get_run_status(123)

        assert result["status"] == "completed"

    def test_denies_run_submitted_by_another_user(self):
        """IDOR guard: a caller may not read a run someone else submitted (not_found, no disclosure)."""
        from server.utils import get_run_status, _user_email_var

        ws = create_autospec(WorkspaceClient)
        run = _make_terminated_run(RunResultState.SUCCESS)
        run.job_id = 42
        run.job_parameters = [JobParameter(name="requesting_user", value="alice@example.com")]
        ws.jobs.get_run.return_value = run

        tok = _user_email_var.set("bob@example.com")  # a different caller
        try:
            with (
                patch("server.utils._get_sp_client", return_value=ws),
                patch.dict("os.environ", _JOB_ID_ENV),
            ):
                result = get_run_status(123)
        finally:
            _user_email_var.reset(tok)

        assert result["status"] == "not_found"
        # The result file is never even read for a run the caller doesn't own.
        ws.files.download.assert_not_called()

    def test_allows_run_submitted_by_same_user(self):
        from server.utils import get_run_status, _user_email_var

        ws = create_autospec(WorkspaceClient)
        run = _make_terminated_run(RunResultState.SUCCESS)
        run.job_id = 42
        run.job_parameters = [JobParameter(name="requesting_user", value="alice@example.com")]
        ws.jobs.get_run.return_value = run
        ws.files.download.return_value.contents.read.return_value = json.dumps({"ok": True}).encode()

        tok = _user_email_var.set("Alice@example.com")  # same user (case-insensitive)
        try:
            with (
                patch("server.utils._get_sp_client", return_value=ws),
                patch.dict("os.environ", _JOB_ID_ENV),
            ):
                result = get_run_status(123)
        finally:
            _user_email_var.reset(tok)

        assert result["status"] == "completed"

    def test_denies_when_caller_has_no_identity(self):
        """A caller with no OBO identity must not read a run, even one with a recorded submitter."""
        from server.utils import get_run_status, _user_email_var

        ws = create_autospec(WorkspaceClient)
        run = _make_terminated_run(RunResultState.SUCCESS)  # requesting_user=user@example.com
        run.job_id = 42
        ws.jobs.get_run.return_value = run

        tok = _user_email_var.set(None)  # no caller identity
        try:
            with (
                patch("server.utils._get_sp_client", return_value=ws),
                patch.dict("os.environ", _JOB_ID_ENV),
            ):
                result = get_run_status(123)
        finally:
            _user_email_var.reset(tok)

        assert result["status"] == "not_found"
        ws.files.download.assert_not_called()

    def test_denies_when_run_has_no_submitter(self):
        """A run with no recorded submitter is unowned and must not be readable by any caller."""
        from server.utils import get_run_status

        ws = create_autospec(WorkspaceClient)
        run = _make_terminated_run(RunResultState.SUCCESS)
        run.job_id = 42
        run.job_parameters = []  # no requesting_user recorded (older run / non-OBO submit)
        ws.jobs.get_run.return_value = run

        # autouse caller is user@example.com, but the empty submitter must still be denied
        with (
            patch("server.utils._get_sp_client", return_value=ws),
            patch.dict("os.environ", _JOB_ID_ENV),
        ):
            result = get_run_status(123)

        assert result["status"] == "not_found"
        ws.files.download.assert_not_called()


class TestSweepStaleViews:
    def test_drops_only_views_older_than_ttl(self):
        import time
        from server.utils import sweep_stale_views

        now = int(time.time())
        ws = create_autospec(WorkspaceClient)
        rows = [
            {"table_name": f"v_{now - 100000}_abc123"},  # stale -> drop
            {"table_name": f"v_{now}_def456"},  # fresh -> keep
            {"table_name": "some_other_table"},  # not a temp view -> ignore
        ]

        with (
            patch("server.utils.execute_sql", return_value=rows),
            patch("server.utils.drop_view") as mock_drop,
        ):
            dropped = sweep_stale_views(ws, "cat", "dqx_mcp_tmp", "wh", ttl_seconds=3600)

        assert dropped == 1
        mock_drop.assert_called_once_with(ws, f"cat.dqx_mcp_tmp.v_{now - 100000}_abc123", warehouse_id="wh")

    def test_lists_views_without_a_cross_catalog_show(self):
        """The listing query must be executable against a shared, session-less warehouse.

        `SHOW VIEWS IN <catalog>.<schema>` parses but Databricks rejects it at runtime with
        CROSS_CATALOG_SCHEMA_REFERENCE_NOT_SUPPORTED ("Run 'USE CATALOG <c>' first"), since SHOW
        resolves the schema against the session's current catalog. The sweeper has no session to set
        one on, so it must qualify through information_schema instead. Asserted here because the
        failure is otherwise invisible: sweep_stale_views swallows it and returns 0, which looks
        exactly like "no stale views".
        """
        from server.utils import sweep_stale_views

        ws = create_autospec(WorkspaceClient)
        with patch("server.utils.execute_sql", return_value=[]) as mock_sql:
            sweep_stale_views(ws, "cat", "dqx_mcp_tmp", "wh")

        query = mock_sql.call_args.args[1]
        assert "SHOW VIEWS" not in query.upper(), f"cross-catalog SHOW is rejected at runtime: {query}"
        # The catalog is an identifier, so it is interpolated (backtick-quoted by
        # _validate_sql_identifier); a parameter marker cannot stand in for an identifier.
        assert "information_schema.views" in query, query
        assert "`cat`." in query, query

    def test_never_drops_a_view_young_enough_to_belong_to_a_queued_job(self):
        """A short configured TTL must not delete the view of a job that has not started yet.

        The sweep runs on the job-SUBMISSION path, moments after create_temp_view, so it sees the
        view of the job being submitted right now. A queued job can start minutes later on a busy
        workspace. Before the floor existed, DQX_SWEEP_TTL_SECONDS=1 (set by the coverage bundle
        target to exercise the sweeper) deleted live views and failed runs with
        TABLE_OR_VIEW_NOT_FOUND.
        """
        import time

        from server.utils import sweep_stale_views

        now = int(time.time())
        ws = create_autospec(WorkspaceClient)
        rows = [
            {"table_name": f"v_{now - 2}_aaaaaaaaaaaa"},  # just submitted — must survive
            {"table_name": f"v_{now - 300}_bbbbbbbbbbbb"},  # 5 min old, could still be queued
            {"table_name": f"v_{now - 100000}_cccccccccccc"},  # genuinely orphaned
        ]
        with (
            patch("server.utils.execute_sql", return_value=rows),
            patch("server.utils.drop_view") as mock_drop,
        ):
            dropped = sweep_stale_views(ws, "cat", "dqx_mcp_tmp", "wh", ttl_seconds=1)

        assert dropped == 1, "only the genuinely orphaned view may be dropped"
        dropped_names = [call.args[1] for call in mock_drop.call_args_list]
        assert dropped_names == [f"cat.dqx_mcp_tmp.v_{now - 100000}_cccccccccccc"], dropped_names

    def test_binds_the_schema_as_a_parameter(self):
        """The schema is a VALUE in the query, so it must be bound, not interpolated.

        Binding keeps the schema name out of the statement's structure entirely — no quoting or
        escaping to get right, and a name can never alter the query even if the identifier
        validation upstream were ever weakened or removed.
        """
        from server.utils import sweep_stale_views

        ws = create_autospec(WorkspaceClient)
        with patch("server.utils.execute_sql", return_value=[]) as mock_sql:
            sweep_stale_views(ws, "cat", "dqx_mcp_tmp", "wh")

        query = mock_sql.call_args.args[1]
        assert "'dqx_mcp_tmp'" not in query, f"the schema must not be interpolated as a literal: {query}"
        assert ":schema_name" in query, query
        bound = {p.name: p.value for p in mock_sql.call_args.kwargs["parameters"]}
        assert bound == {"schema_name": "dqx_mcp_tmp"}, bound

    def test_returns_zero_when_listing_fails(self):
        from server.utils import sweep_stale_views

        ws = create_autospec(WorkspaceClient)
        with (
            patch("server.utils.execute_sql", side_effect=RuntimeError("no warehouse")),
            patch("server.utils.drop_view") as mock_drop,
        ):
            dropped = sweep_stale_views(ws, "cat", "dqx_mcp_tmp", "wh")

        assert dropped == 0
        mock_drop.assert_not_called()


class TestSweepStaleResultFiles:
    """The results volume holds two kinds of file and only one may be swept by age."""

    @staticmethod
    def _entry(path: str, age_seconds: int) -> MagicMock:
        import time

        entry = MagicMock()
        entry.path = path
        entry.is_directory = False
        entry.last_modified = int((time.time() - age_seconds) * 1000)  # epoch millis
        return entry

    def test_sweeps_stale_results_but_not_staged_inputs_a_queued_job_may_read(self):
        """A staged input must survive the normal TTL: its job may still be queued."""
        from server.utils import sweep_stale_result_files

        ws = create_autospec(WorkspaceClient)
        volume = "/Volumes/cat/dqx_mcp_tmp/mcp_results"
        ws.files.list_directory_contents.return_value = [
            self._entry(f"{volume}/111.json", 100_000),  # stale result -> delete
            self._entry(f"{volume}/222.json", 5),  # fresh result -> keep
            # Older than the result TTL but well inside the staged TTL: a queued job may not have
            # read it yet, so deleting it would fail that run with "Contract file not found".
            # (3600 < age < 24h — 100_000s would be past the staged TTL and legitimately swept.)
            self._entry(f"{volume}/staged_abc123.yaml", 7_200),
        ]

        with patch("server.utils._get_results_volume", return_value=volume):
            dropped = sweep_stale_result_files(ws, ttl_seconds=3600)

        assert dropped == 1
        ws.files.delete.assert_called_once_with(f"{volume}/111.json")

    def test_eventually_reclaims_long_abandoned_staged_inputs(self):
        """Staged inputs are not exempt forever — excluding them entirely leaked the volume.

        Beyond the staged TTL no job could still be pending, so the input is abandoned and must be
        reclaimed; otherwise every inline-contract call leaves a file behind permanently.
        """
        from server.utils import _STAGED_TTL_SECONDS, sweep_stale_result_files

        ws = create_autospec(WorkspaceClient)
        volume = "/Volumes/cat/dqx_mcp_tmp/mcp_results"
        ws.files.list_directory_contents.return_value = [
            self._entry(f"{volume}/staged_old.yaml", _STAGED_TTL_SECONDS + 60),  # abandoned -> delete
            self._entry(f"{volume}/staged_recent.yaml", 120),  # maybe queued -> keep
        ]

        with patch("server.utils._get_results_volume", return_value=volume):
            dropped = sweep_stale_result_files(ws, ttl_seconds=3600)

        assert dropped == 1
        ws.files.delete.assert_called_once_with(f"{volume}/staged_old.yaml")

    def test_leaves_unrecognised_files_alone(self):
        """Only files this server creates are swept; anything else is not ours to delete."""
        from server.utils import sweep_stale_result_files

        ws = create_autospec(WorkspaceClient)
        volume = "/Volumes/cat/dqx_mcp_tmp/mcp_results"
        ws.files.list_directory_contents.return_value = [
            self._entry(f"{volume}/someone_elses_notes.txt", 10_000_000),
        ]

        with patch("server.utils._get_results_volume", return_value=volume):
            dropped = sweep_stale_result_files(ws, ttl_seconds=1)

        assert dropped == 0
        ws.files.delete.assert_not_called()

    def test_returns_zero_when_listing_fails(self):
        from server.utils import sweep_stale_result_files

        ws = create_autospec(WorkspaceClient)
        ws.files.list_directory_contents.side_effect = RuntimeError("volume gone")
        with patch("server.utils._get_results_volume", return_value="/Volumes/cat/s/v"):
            dropped = sweep_stale_result_files(ws, ttl_seconds=1)

        assert dropped == 0
        ws.files.delete.assert_not_called()

    def test_unreadable_run_is_not_found_not_permission_denied(self):
        """A run the app SP cannot see must look identical to one that does not exist.

        Found by an integration test: the job ACL denies reading a foreign job's run before the
        job_id guard can, and the raw PERMISSION_DENIED was surfacing to the caller. That confirms
        the run EXISTS, which is the disclosure the ownership guard exists to prevent — run ids are
        guessable integers.
        """
        from databricks.sdk.errors.base import DatabricksError

        from server.utils import get_run_status

        ws = create_autospec(WorkspaceClient)
        err = DatabricksError("User ... does not have View or Manage Run permissions on job 123")
        err.error_code = "PERMISSION_DENIED"
        ws.jobs.get_run.side_effect = err

        with patch("server.utils._get_sp_client", return_value=ws), patch.dict(os.environ, _JOB_ID_ENV):
            result = get_run_status(999)

        assert result["status"] == "not_found", result
        assert "permission" not in json.dumps(result).lower(), f"leaked the denial reason: {result}"

    def test_still_running_returns_running(self):
        from server.utils import get_run_status, _user_email_var

        ws = create_autospec(WorkspaceClient)
        run = MagicMock(spec=Run)
        run.state = MagicMock(spec=RunState)
        run.state.life_cycle_state = RunLifeCycleState.RUNNING
        run.job_parameters = [JobParameter(name="requesting_user", value="user@example.com")]
        ws.jobs.get_run.return_value = run

        tok = _user_email_var.set("user@example.com")  # caller owns the run (IDOR guard passes)
        try:
            with (
                patch("server.utils._get_sp_client", return_value=ws),
                patch("server.utils.time.sleep"),
            ):
                result = get_run_status(123)
        finally:
            _user_email_var.reset(tok)

        assert result["status"] == "running"
        assert result["run_id"] == 123
        # Polls repeatedly within the wait budget (see get_run_status: an agent cannot sleep, so the
        # server absorbs the wait), and still reports 'running' if the job outlives it.
        assert ws.jobs.get_run.call_count >= 1
        assert ws.jobs.get_run.call_args == call(123)


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

    @pytest.mark.anyio
    async def test_honors_inbound_request_id(self):
        from server.utils import OBOAuthMiddleware, _request_id_var

        captured = {}

        async def app(scope, receive, send):
            captured["rid"] = _request_id_var.get(None)

        middleware = OBOAuthMiddleware(app)
        scope = {"type": "http", "method": "POST", "path": "/mcp", "headers": [(b"x-request-id", b"trace-123")]}
        await middleware(scope, None, None)

        assert captured["rid"] == "trace-123"

    @pytest.mark.anyio
    async def test_generates_request_id_when_absent(self):
        from server.utils import OBOAuthMiddleware, _request_id_var

        captured = {}

        async def app(scope, receive, send):
            captured["rid"] = _request_id_var.get(None)

        middleware = OBOAuthMiddleware(app)
        await middleware({"type": "http", "method": "GET", "path": "/mcp", "headers": []}, None, None)

        assert captured["rid"] and len(captured["rid"]) > 0

    @pytest.mark.anyio
    async def test_resets_context_after_request(self):
        # Context must not leak across requests on a reused ASGI worker.
        from server.utils import OBOAuthMiddleware, _user_email_var, _user_token_var, _request_id_var

        async def app(scope, receive, send):
            pass

        middleware = OBOAuthMiddleware(app)
        scope = {
            "type": "http",
            "method": "POST",
            "path": "/mcp",
            "headers": [(b"x-forwarded-email", b"u@example.com"), (b"x-forwarded-access-token", b"tok")],
        }
        with patch.dict("os.environ", {"DATABRICKS_HOST": "https://host.com"}):
            await middleware(scope, None, None)

        assert _user_email_var.get(None) is None
        assert _user_token_var.get(None) is None
        assert _request_id_var.get(None) is None

    @pytest.mark.anyio
    async def test_captures_response_status(self):
        from server.utils import OBOAuthMiddleware

        sent = []

        async def app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

        async def send(message):
            sent.append(message)

        middleware = OBOAuthMiddleware(app)
        scope = {"type": "http", "method": "POST", "path": "/mcp", "headers": []}
        await middleware(scope, None, send)

        # send_wrapper must forward every message untouched to the real send.
        assert [m["type"] for m in sent] == ["http.response.start", "http.response.body"]


class TestClassifyLocation:
    def test_table(self):
        from server.utils import classify_location

        assert classify_location("catalog.schema.table") == "table"

    def test_volume(self):
        from server.utils import classify_location

        assert classify_location("/Volumes/c/s/v/checks.yml") == "volume"

    def test_workspace(self):
        from server.utils import classify_location

        assert classify_location("/Workspace/Users/me/checks.yml") == "workspace"


class TestVerifyOboReadAccess:
    def test_volume_calls_files_get_metadata(self):
        from server.utils import verify_obo_read_access

        ws = create_autospec(WorkspaceClient)
        verify_obo_read_access(ws, "/Volumes/c/s/v/f.yml")
        ws.files.get_metadata.assert_called_once_with("/Volumes/c/s/v/f.yml")

    def test_workspace_calls_get_status(self):
        from server.utils import verify_obo_read_access

        ws = create_autospec(WorkspaceClient)
        verify_obo_read_access(ws, "/Workspace/x.yml")
        ws.workspace.get_status.assert_called_once_with("/Workspace/x.yml")

    def test_table_is_noop(self):
        from server.utils import verify_obo_read_access

        ws = create_autospec(WorkspaceClient)
        verify_obo_read_access(ws, "c.s.t")
        ws.files.get_metadata.assert_not_called()
        ws.workspace.get_status.assert_not_called()

    def test_raises_permission_error_on_failure(self):
        from server.utils import verify_obo_read_access

        ws = create_autospec(WorkspaceClient)
        ws.files.get_metadata.side_effect = Exception("denied")
        with pytest.raises(PermissionError):
            verify_obo_read_access(ws, "/Volumes/c/s/v/f.yml")


class TestValidateOutputName:
    """Outputs go to the caller's per-user schema, so the tool takes a bare name — not an FQN."""

    @pytest.mark.parametrize("name", ["my_checks", "orders_out", "T1", "a_b_c123"])
    def test_accepts_bare_identifiers(self, name: str):
        from server.utils import validate_output_name

        assert validate_output_name(name) == name

    @pytest.mark.parametrize(
        "name",
        ["catalog.schema.table", "/Volumes/c/s/v/x.yml", "has space", "drop`table", "", "a-b", "2024_results", "1t"],
    )
    def test_rejects_non_identifiers(self, name: str):
        from server.utils import validate_output_name

        # Includes leading-digit names: the FQN is used UNQUOTED, so a digit-leading table part is a
        # SQL parse error — must be rejected up front.
        with pytest.raises(ValueError, match="Invalid output name"):
            validate_output_name(name)


class TestValidateWriteMode:
    @pytest.mark.parametrize("mode", ["append", "overwrite"])
    def test_accepts_supported_modes(self, mode: str):
        from server.utils import validate_write_mode

        assert validate_write_mode(mode) == mode

    @pytest.mark.parametrize("mode", ["upsert", "merge", "APPEND", ""])
    def test_rejects_unsupported_modes(self, mode: str):
        from server.utils import validate_write_mode

        with pytest.raises(ValueError, match="Invalid mode"):
            validate_write_mode(mode)


class TestReadFileViaObo:
    def test_volume_uses_files_download(self):
        from server.utils import read_file_via_obo

        ws = create_autospec(WorkspaceClient)
        ws.files.download.return_value.contents.read.return_value = b"data"
        assert read_file_via_obo(ws, "/Volumes/c/s/v/f.yml") == b"data"
        ws.files.download.assert_called_once_with("/Volumes/c/s/v/f.yml")

    def test_workspace_uses_export_and_b64decode(self):
        import base64

        from server.utils import read_file_via_obo

        ws = create_autospec(WorkspaceClient)
        ws.workspace.export.return_value.content = base64.b64encode(b"hi there").decode()
        assert read_file_via_obo(ws, "/Workspace/Users/me/f.yml") == b"hi there"

    def test_table_is_rejected(self):
        from server.utils import read_file_via_obo

        ws = create_autospec(WorkspaceClient)
        with pytest.raises(ValueError, match="table name"):
            read_file_via_obo(ws, "c.s.t")


class TestStageBytesToResultsVolume:
    def test_uploads_via_sp_and_returns_volume_path(self):
        from server.utils import stage_bytes_to_results_volume

        sp = create_autospec(WorkspaceClient)
        with (
            patch("server.utils._get_results_volume", return_value="/Volumes/c/dqx_mcp_tmp/mcp_results"),
            patch("server.utils._get_sp_client", return_value=sp),
        ):
            path = stage_bytes_to_results_volume(b"payload", suffix=".yaml")

        assert path.startswith("/Volumes/c/dqx_mcp_tmp/mcp_results/staged_")
        assert path.endswith(".yaml")
        # Uploaded (as the app SP) to exactly the returned path.
        assert sp.files.upload.call_args[0][0] == path
