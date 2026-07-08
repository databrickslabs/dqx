"""Tests for ``run_status_manager`` — pure SQL-driven helpers.

We mock the ``SqlExecutor.query`` / ``execute`` calls and verify both the
return-shape behaviour and the literal SQL that gets executed (so any
future regression in escaping or column ordering is caught early).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from databricks_labs_dqx_app.backend import run_status_manager
from databricks_labs_dqx_app.backend.run_status_manager import (
    RunMetadata,
    get_run_metadata,
    get_run_owner,
    get_run_view_fqn,
    has_terminal_result,
    reconcile_running_rows,
    update_run_status,
)


def _status(state: str, result_state: str | None = None, message: str | None = None) -> SimpleNamespace:
    """Build a ``RunStatus``-shaped stand-in for the Jobs-API status."""
    return SimpleNamespace(state=state, result_state=result_state, message=message)


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


# ---------------------------------------------------------------------------
# reconcile_running_rows
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_status_cache():
    """Reset the module-level job-status cache so tests don't leak into each other."""
    run_status_manager._status_cache.clear()
    yield
    run_status_manager._status_cache.clear()


class TestReconcileRunningRows:
    def test_terminal_failure_flips_row_and_persists(self, sql_executor_mock, app_config):
        # _get_running_job_run_ids returns the run_id -> job_run_id mapping.
        sql_executor_mock.query.return_value = [["r1", "111"]]
        rows: list[dict[str, str | None]] = [{"run_id": "r1", "status": "RUNNING", "error_message": None}]
        status_fn = Mock(return_value=_status("INTERNAL_ERROR", "FAILED", "boom"))

        reconcile_running_rows(sql_executor_mock, app_config, "dq_validation_runs", rows, status_fn)

        assert rows[0]["status"] == "FAILED"
        assert rows[0]["error_message"] == "boom"
        # The correction is persisted to the Delta row via an UPDATE.
        sql_executor_mock.execute.assert_called_once()
        upd = sql_executor_mock.execute.call_args.args[0]
        assert "status = 'FAILED'" in upd
        assert "WHERE run_id = 'r1'" in upd

    def test_canceled_result_maps_to_canceled(self, sql_executor_mock, app_config):
        sql_executor_mock.query.return_value = [["r1", "111"]]
        rows: list[dict[str, str | None]] = [{"run_id": "r1", "status": "RUNNING"}]
        status_fn = Mock(return_value=_status("TERMINATED", "CANCELED", "stopped"))

        reconcile_running_rows(sql_executor_mock, app_config, "dq_validation_runs", rows, status_fn)

        assert rows[0]["status"] == "CANCELED"

    def test_success_is_not_written_back(self, sql_executor_mock, app_config):
        # The runner owns the terminal SUCCESS row (with metrics); don't clobber.
        sql_executor_mock.query.return_value = [["r1", "111"]]
        rows: list[dict[str, str | None]] = [{"run_id": "r1", "status": "RUNNING"}]
        status_fn = Mock(return_value=_status("TERMINATED", "SUCCESS"))

        reconcile_running_rows(sql_executor_mock, app_config, "dq_validation_runs", rows, status_fn)

        assert rows[0]["status"] == "RUNNING"
        sql_executor_mock.execute.assert_not_called()

    def test_still_running_job_is_left_alone(self, sql_executor_mock, app_config):
        sql_executor_mock.query.return_value = [["r1", "111"]]
        rows: list[dict[str, str | None]] = [{"run_id": "r1", "status": "RUNNING"}]
        status_fn = Mock(return_value=_status("RUNNING", None))

        reconcile_running_rows(sql_executor_mock, app_config, "dq_validation_runs", rows, status_fn)

        assert rows[0]["status"] == "RUNNING"
        sql_executor_mock.execute.assert_not_called()

    def test_row_without_job_run_id_is_skipped(self, sql_executor_mock, app_config):
        # Mapping is empty -> no job_run_id to reconcile against; status_fn never called.
        sql_executor_mock.query.return_value = []
        rows: list[dict[str, str | None]] = [{"run_id": "r1", "status": "RUNNING"}]
        status_fn = Mock()

        reconcile_running_rows(sql_executor_mock, app_config, "dq_validation_runs", rows, status_fn)

        status_fn.assert_not_called()
        assert rows[0]["status"] == "RUNNING"

    def test_no_running_rows_short_circuits(self, sql_executor_mock, app_config):
        rows: list[dict[str, str | None]] = [{"run_id": "r1", "status": "SUCCESS"}]
        status_fn = Mock()

        reconcile_running_rows(sql_executor_mock, app_config, "dq_validation_runs", rows, status_fn)

        # Never even looks up job_run_ids when nothing is RUNNING.
        sql_executor_mock.query.assert_not_called()
        status_fn.assert_not_called()

    def test_status_fn_error_is_swallowed(self, sql_executor_mock, app_config):
        sql_executor_mock.query.return_value = [["r1", "111"]]
        rows: list[dict[str, str | None]] = [{"run_id": "r1", "status": "RUNNING"}]
        status_fn = Mock(side_effect=RuntimeError("jobs api down"))

        # Must not raise; row stays RUNNING.
        reconcile_running_rows(sql_executor_mock, app_config, "dq_validation_runs", rows, status_fn)

        assert rows[0]["status"] == "RUNNING"

    def test_status_is_cached_across_calls(self, sql_executor_mock, app_config):
        sql_executor_mock.query.return_value = [["r1", "111"]]
        status_fn = Mock(return_value=_status("RUNNING", None))

        # Two separate list requests for the same still-running job.
        reconcile_running_rows(
            sql_executor_mock, app_config, "dq_validation_runs", [{"run_id": "r1", "status": "RUNNING"}], status_fn
        )
        reconcile_running_rows(
            sql_executor_mock, app_config, "dq_validation_runs", [{"run_id": "r1", "status": "RUNNING"}], status_fn
        )

        # The Jobs API is hit only once thanks to the TTL cache.
        status_fn.assert_called_once()

    def test_reconcile_is_bounded(self, sql_executor_mock, app_config):
        # More RUNNING rows than the per-call cap: only the cap is looked up.
        cap = run_status_manager._MAX_RECONCILE_PER_CALL
        rows: list[dict[str, str | None]] = [{"run_id": f"r{i}", "status": "RUNNING"} for i in range(cap + 10)]
        sql_executor_mock.query.return_value = []
        status_fn = Mock()

        reconcile_running_rows(sql_executor_mock, app_config, "dq_validation_runs", rows, status_fn)

        in_clause = sql_executor_mock.query.call_args.args[0]
        # Exactly the cap number of run_ids appear in the IN (...) lookup.
        assert in_clause.count("'r") == cap
