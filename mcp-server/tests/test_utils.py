import json
import logging
from unittest.mock import MagicMock, create_autospec, patch

import pytest

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    Run,
    RunOutput,
    RunState,
    RunLifeCycleState,
    RunResultState,
)

_JOB_ID_ENV = {"DQX_RUNNER_JOB_ID": "42", "DQX_CATALOG": "cat", "DQX_TMP_SCHEMA": "tmp"}


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
        assert job_parameters["results_volume"] == "/Volumes/cat/tmp/mcp_results"

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
            patch.dict("os.environ", {"DQX_CATALOG": "cat", "DQX_TMP_SCHEMA": "tmp"}),
        ):
            result = get_run_status(123)

        assert result["status"] == "completed"
        assert result["run_id"] == 123
        assert result["result"]["profiles"] == []
        assert result["result"]["table_name"] == "c.s.t"
        # Result is read from the results volume, keyed by run id.
        assert ws.files.download.call_args[0][0] == "/Volumes/cat/tmp/mcp_results/123.json"

    def test_completed_but_missing_result_file_is_failed(self):
        from server.utils import get_run_status

        ws = create_autospec(WorkspaceClient)
        ws.jobs.get_run.return_value = _make_terminated_run(RunResultState.SUCCESS)
        ws.files.download.side_effect = Exception("not found")

        with (
            patch("server.utils._get_sp_client", return_value=ws),
            patch.dict("os.environ", {"DQX_CATALOG": "cat", "DQX_TMP_SCHEMA": "tmp"}),
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

        with patch("server.utils._get_sp_client", return_value=ws):
            result = get_run_status(123)

        assert result["status"] == "running"
        assert result["run_id"] == 123
        # Single non-blocking poll — exactly one get_run call, no internal wait.
        ws.jobs.get_run.assert_called_once_with(123)


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


class TestToLocalFusePath:
    def test_volume_unchanged(self):
        from server.utils import to_local_fuse_path

        assert to_local_fuse_path("/Volumes/c/s/v/contract.yml") == "/Volumes/c/s/v/contract.yml"

    def test_already_workspace_prefixed_unchanged(self):
        from server.utils import to_local_fuse_path

        assert to_local_fuse_path("/Workspace/Users/me/contract.yml") == "/Workspace/Users/me/contract.yml"

    def test_workspace_path_gets_prefix(self):
        from server.utils import to_local_fuse_path

        assert to_local_fuse_path("/Users/me/contract.yml") == "/Workspace/Users/me/contract.yml"

    def test_table_unchanged(self):
        from server.utils import to_local_fuse_path

        # Not a file path; classify_location -> table, so it is returned as-is.
        assert to_local_fuse_path("c.s.t") == "c.s.t"


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


class TestVerifyOboWriteAccessTable:
    def test_owner_match_allows(self):
        from server.utils import verify_obo_write_access, _user_email_var

        ws = create_autospec(WorkspaceClient)
        with patch("server.utils.execute_sql", return_value=[{"col_name": "Owner", "data_type": "me@x.com"}]):
            tok = _user_email_var.set("me@x.com")
            try:
                verify_obo_write_access(ws, "c.s.t", "wh")  # must not raise
            finally:
                _user_email_var.reset(tok)

    def test_owner_mismatch_raises(self):
        from server.utils import verify_obo_write_access, _user_email_var

        ws = create_autospec(WorkspaceClient)
        with patch("server.utils.execute_sql", return_value=[{"col_name": "Owner", "data_type": "someone@x.com"}]):
            tok = _user_email_var.set("me@x.com")
            try:
                with pytest.raises(PermissionError, match="owned by"):
                    verify_obo_write_access(ws, "c.s.t", "wh")
            finally:
                _user_email_var.reset(tok)

    def test_nonexistent_table_create_probe_success_allows(self):
        from server.utils import verify_obo_write_access

        ws = create_autospec(WorkspaceClient)
        queries: list[str] = []

        def fake_sql(_ws, query, warehouse_id):  # noqa: ANN001, ANN202
            queries.append(query)
            if query.startswith("DESCRIBE"):
                raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND")
            return []

        with patch("server.utils.execute_sql", side_effect=fake_sql):
            verify_obo_write_access(ws, "c.s.newt", "wh")  # must not raise

        assert any(q.startswith("CREATE TABLE") for q in queries)
        assert any(q.startswith("DROP TABLE IF EXISTS") for q in queries)

    def test_existing_table_no_access_denied(self):
        from server.utils import verify_obo_write_access, _user_email_var

        ws = create_autospec(WorkspaceClient)
        # DESCRIBE as the caller fails with a permission error → table exists, caller can't touch it.
        perm_err = "UNAUTHORIZED_ACCESS PERMISSION_DENIED: User does not have SELECT on Table foo. SQLSTATE: 42501"
        with patch("server.utils.execute_sql", side_effect=RuntimeError(perm_err)):
            tok = _user_email_var.set("me@x.com")
            try:
                with pytest.raises(PermissionError, match="do not have access"):
                    verify_obo_write_access(ws, "c.s.foreign", "wh")
            finally:
                _user_email_var.reset(tok)

    def test_create_probe_failure_raises(self):
        from server.utils import verify_obo_write_access

        ws = create_autospec(WorkspaceClient)

        def fake_sql(_ws, query, warehouse_id):  # noqa: ANN001, ANN202
            if query.startswith("DESCRIBE"):
                raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND")
            if query.startswith("CREATE TABLE"):
                raise RuntimeError("permission denied")
            return []

        with patch("server.utils.execute_sql", side_effect=fake_sql):
            with pytest.raises(PermissionError, match="CREATE TABLE"):
                verify_obo_write_access(ws, "c.s.newt", "wh")


class TestVerifyOboWriteAccessFile:
    def test_volume_write_probe_uploads_and_deletes(self):
        from server.utils import verify_obo_write_access

        ws = create_autospec(WorkspaceClient)
        verify_obo_write_access(ws, "/Volumes/c/s/v/checks.yml", "wh")
        assert ws.files.upload.called
        assert ws.files.delete.called

    def test_volume_write_probe_denied_raises(self):
        from server.utils import verify_obo_write_access

        ws = create_autospec(WorkspaceClient)
        ws.files.upload.side_effect = Exception("denied")
        with pytest.raises(PermissionError):
            verify_obo_write_access(ws, "/Volumes/c/s/v/checks.yml", "wh")

    def test_workspace_write_probe_uploads_and_deletes(self):
        from server.utils import verify_obo_write_access

        ws = create_autospec(WorkspaceClient)
        verify_obo_write_access(ws, "/Workspace/Users/me/checks.yml", "wh")
        assert ws.workspace.upload.called
        assert ws.workspace.delete.called
