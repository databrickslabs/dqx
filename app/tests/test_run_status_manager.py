"""Tests for ``run_status_manager`` — pure SQL-driven helpers.

We mock the ``SqlExecutor.query`` / ``execute`` calls and verify both the
return-shape behaviour and the literal SQL that gets executed (so any
future regression in escaping or column ordering is caught early).
"""

from __future__ import annotations


from databricks_labs_dqx_app.backend.run_status_manager import (
    RunMetadata,
    get_run_metadata,
    get_run_owner,
    get_run_view_fqn,
    has_terminal_result,
    update_run_status,
)


# ---------------------------------------------------------------------------
# update_run_status
# ---------------------------------------------------------------------------


class TestUpdateRunStatus:
    def test_minimal_update_emits_expected_sql(self, sql_executor_mock, app_config):
        update_run_status(
            sql=sql_executor_mock,
            app_conf=app_config,
            table_name="dq_validation_runs",
            run_id="abc-123",
            status="SUCCESS",
            error_message=None,
        )
        sql_executor_mock.execute.assert_called_once()
        sql = sql_executor_mock.execute.call_args.args[0]
        assert "UPDATE " in sql
        assert "WHERE run_id = 'abc-123'" in sql
        assert "status = 'SUCCESS'" in sql
        assert "AND status = 'RUNNING'" in sql
        assert "canceled_by" not in sql

    def test_with_canceled_by_appends_canceled_clause(self, sql_executor_mock, app_config):
        update_run_status(
            sql=sql_executor_mock,
            app_conf=app_config,
            table_name="dq_validation_runs",
            run_id="abc",
            status="CANCELED",
            error_message="user requested",
            canceled_by="alice@x",
        )
        sql = sql_executor_mock.execute.call_args.args[0]
        assert "canceled_by = 'alice@x'" in sql
        assert "error_message = 'user requested'" in sql

    def test_quotes_in_run_id_are_escaped(self, sql_executor_mock, app_config):
        update_run_status(
            sql=sql_executor_mock,
            app_conf=app_config,
            table_name="dq_validation_runs",
            run_id="a'b",  # injection attempt
            status="FAILED",
            error_message=None,
        )
        sql = sql_executor_mock.execute.call_args.args[0]
        assert "run_id = 'a''b'" in sql  # doubled single-quote

    def test_swallows_executor_errors(self, sql_executor_mock, app_config):
        sql_executor_mock.execute.side_effect = RuntimeError("boom")
        # Must NOT raise — the function logs the error and returns.
        update_run_status(
            sql=sql_executor_mock,
            app_conf=app_config,
            table_name="dq_validation_runs",
            run_id="x",
            status="FAILED",
            error_message=None,
        )


# ---------------------------------------------------------------------------
# get_run_metadata / get_run_owner / get_run_view_fqn
# ---------------------------------------------------------------------------


class TestGetRunMetadata:
    def test_full_row_returned(self, sql_executor_mock, app_config):
        sql_executor_mock.query.return_value = [["main.tmp.v1", "alice@x", "12345"]]
        md = get_run_metadata(
            sql=sql_executor_mock,
            app_conf=app_config,
            table_name="dq_validation_runs",
            run_id="r1",
        )
        assert isinstance(md, RunMetadata)
        assert md.view_fqn == "main.tmp.v1"
        assert md.requesting_user == "alice@x"
        assert md.job_run_id == 12345

    def test_missing_row_returns_none_fields(self, sql_executor_mock, app_config):
        sql_executor_mock.query.return_value = []
        md = get_run_metadata(sql=sql_executor_mock, app_conf=app_config, table_name="dq_validation_runs", run_id="r1")
        assert md.view_fqn is None
        assert md.requesting_user is None
        assert md.job_run_id is None

    def test_null_job_run_id_is_handled(self, sql_executor_mock, app_config):
        sql_executor_mock.query.return_value = [["main.tmp.v1", "alice@x", None]]
        md = get_run_metadata(sql=sql_executor_mock, app_conf=app_config, table_name="dq_validation_runs", run_id="r1")
        assert md.job_run_id is None

    def test_executor_failure_yields_empty_metadata(self, sql_executor_mock, app_config):
        sql_executor_mock.query.side_effect = RuntimeError("dbo down")
        md = get_run_metadata(sql=sql_executor_mock, app_conf=app_config, table_name="dq_validation_runs", run_id="r1")
        assert md.view_fqn is None and md.requesting_user is None and md.job_run_id is None

    def test_query_orders_by_job_run_id_first(self, sql_executor_mock, app_config):
        # The implementation prefers terminal rows that have a non-null
        # job_run_id over the RUNNING placeholder.
        sql_executor_mock.query.return_value = [["v", "u", "1"]]
        get_run_metadata(sql=sql_executor_mock, app_conf=app_config, table_name="dq_validation_runs", run_id="r1")
        sql = sql_executor_mock.query.call_args.args[0]
        assert "ORDER BY job_run_id IS NOT NULL DESC" in sql


class TestGetRunOwner:
    def test_returns_user_when_present(self, sql_executor_mock, app_config):
        sql_executor_mock.query.return_value = [["alice@x"]]
        assert get_run_owner(sql_executor_mock, app_config, "dq_validation_runs", "r1") == "alice@x"

    def test_returns_none_when_missing(self, sql_executor_mock, app_config):
        sql_executor_mock.query.return_value = []
        assert get_run_owner(sql_executor_mock, app_config, "dq_validation_runs", "r1") is None


class TestGetRunViewFqn:
    def test_returns_pair(self, sql_executor_mock, app_config):
        sql_executor_mock.query.return_value = [["v1", "u@x"]]
        assert get_run_view_fqn(sql_executor_mock, app_config, "dq_validation_runs", "r1") == ("v1", "u@x")

    def test_missing_returns_none_pair(self, sql_executor_mock, app_config):
        sql_executor_mock.query.return_value = []
        assert get_run_view_fqn(sql_executor_mock, app_config, "dq_validation_runs", "r1") == (None, None)


# ---------------------------------------------------------------------------
# has_terminal_result
# ---------------------------------------------------------------------------


class TestHasTerminalResult:
    def test_returns_status_when_terminal_row_exists(self, sql_executor_mock, app_config):
        sql_executor_mock.query.return_value = [["SUCCESS"]]
        assert has_terminal_result(sql_executor_mock, app_config, "dq_validation_runs", "r1") == "SUCCESS"

    def test_returns_none_when_only_running(self, sql_executor_mock, app_config):
        sql_executor_mock.query.return_value = []
        assert has_terminal_result(sql_executor_mock, app_config, "dq_validation_runs", "r1") is None

    def test_failure_swallowed(self, sql_executor_mock, app_config):
        sql_executor_mock.query.side_effect = RuntimeError("nope")
        # Must not raise — the caller treats None as "still running".
        assert has_terminal_result(sql_executor_mock, app_config, "dq_validation_runs", "r1") is None

    def test_query_excludes_running_state(self, sql_executor_mock, app_config):
        sql_executor_mock.query.return_value = []
        has_terminal_result(sql_executor_mock, app_config, "dq_validation_runs", "r1")
        sql = sql_executor_mock.query.call_args.args[0]
        assert "status != 'RUNNING'" in sql


# ---------------------------------------------------------------------------
# RunMetadata helper
# ---------------------------------------------------------------------------


class TestRunMetadataDataclass:
    def test_roundtrips_fields(self):
        md = RunMetadata(view_fqn="v", requesting_user="u", job_run_id=42)
        assert md.view_fqn == "v"
        assert md.requesting_user == "u"
        assert md.job_run_id == 42

    def test_supports_none_defaults(self):
        md = RunMetadata(view_fqn=None, requesting_user=None, job_run_id=None)
        assert md.view_fqn is None
        assert md.requesting_user is None
        assert md.job_run_id is None
