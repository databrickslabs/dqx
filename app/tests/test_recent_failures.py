"""Unit tests for the recent-failures endpoints.

Both ``GET /dryrun/runs/recent-failures`` and ``GET /profiler/runs/recent-failures``
are thin filters over the existing list infrastructure. These tests verify:

1. Only FAILED rows are returned (not SUCCESS, RUNNING, CANCELED).
2. The result is bounded at _RECENT_FAILURES_LIMIT.
3. The response shape carries only the minimal fields (RunFailureOut).
4. Non-FAILED rows do not inflate the result count.
"""

from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, create_autospec

import pytest

from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.models import RunFailureOut
from databricks_labs_dqx_app.backend.routes.v1.dryrun import (
    list_recent_validation_failures,
    _RECENT_FAILURES_LIMIT as DRYRUN_LIMIT,
)
from databricks_labs_dqx_app.backend.routes.v1.profiler import (
    list_recent_profile_failures,
    _RECENT_FAILURES_LIMIT as PROFILER_LIMIT,
)
from databricks_labs_dqx_app.backend.services.job_service import JobService
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _recent_ts() -> str:
    """A timestamp 1 hour ago — recent enough to not trigger the stale-RUNNING fallback."""
    return (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S")


def _old_ts() -> str:
    """A timestamp 24 hours ago — old enough to trigger stale-RUNNING fallback."""
    return "2025-01-01T00:00:00"


def _make_row(
    run_id: str,
    status: str,
    fqn: str = "main.public.orders",
    created_at: str | None = None,
) -> dict[str, str | None]:
    # Use a recent timestamp for RUNNING rows so reconcile_running_rows doesn't
    # flip them to FAILED via the stale-age path. Non-RUNNING rows default to
    # an old timestamp (they won't be mutated by reconcile anyway).
    if created_at is None:
        created_at = _recent_ts() if status == "RUNNING" else _old_ts()
    return {
        "run_id": run_id,
        "status": status,
        "source_table_fqn": fqn,
        "created_at": created_at,
        # Extra fields that should NOT appear in RunFailureOut
        "error_message": "big error payload" if status == "FAILED" else None,
        "requesting_user": "user@example.com",
        "total_rows": "1000",
        "valid_rows": "900",
        "invalid_rows": "100",
        "error_rows": "50",
        "warning_rows": "50",
        "sample_size": None,
        "updated_at": None,
        "run_type": "dryrun",
        "canceled_by": None,
        "job_run_id": None,
        "duration_seconds": None,
    }


@pytest.fixture
def job_service_mock() -> MagicMock:
    return create_autospec(JobService, instance=True)


@pytest.fixture
def sql_executor() -> MagicMock:
    return create_autospec(SqlExecutor, instance=True)


@pytest.fixture
def app_conf() -> AppConfig:
    from databricks_labs_dqx_app.backend.config import conf

    return conf


# ---------------------------------------------------------------------------
# Validation recent-failures
# ---------------------------------------------------------------------------


class TestListRecentValidationFailures:
    """``GET /dryrun/runs/recent-failures`` filters to FAILED, bounds, minimal shape."""

    def test_returns_only_failed_rows(
        self,
        job_service_mock: MagicMock,
        sql_executor: MagicMock,
        app_conf: AppConfig,
    ) -> None:
        job_service_mock.list_dryrun_rows.return_value = [
            _make_row("run-success", "SUCCESS"),
            _make_row("run-running", "RUNNING"),
            _make_row("run-canceled", "CANCELED"),
            _make_row("run-failed", "FAILED"),
        ]
        # reconcile_running_rows is called but we don't want it to modify rows
        # (it operates on the list in-place; stubbing get_run_status is sufficient
        # to let the reconcile path pass without touching anything meaningful).
        sql_executor.query_dicts.return_value = []

        result = list_recent_validation_failures(
            job_svc=job_service_mock,
            app_conf=app_conf,
            user_catalogs=frozenset({"main"}),
            sql=sql_executor,
        )

        assert len(result) == 1
        assert result[0].run_id == "run-failed"
        assert result[0].status == "FAILED"

    def test_excludes_rows_from_inaccessible_catalogs(
        self,
        job_service_mock: MagicMock,
        sql_executor: MagicMock,
        app_conf: AppConfig,
    ) -> None:
        job_service_mock.list_dryrun_rows.return_value = [
            _make_row("run-visible", "FAILED", fqn="main.public.orders"),
            _make_row("run-hidden", "FAILED", fqn="restricted.public.orders"),
        ]
        sql_executor.query_dicts.return_value = []

        result = list_recent_validation_failures(
            job_svc=job_service_mock,
            app_conf=app_conf,
            user_catalogs=frozenset({"main"}),
            sql=sql_executor,
        )

        assert len(result) == 1
        assert result[0].run_id == "run-visible"

    def test_includes_sql_check_prefix_rows(
        self,
        job_service_mock: MagicMock,
        sql_executor: MagicMock,
        app_conf: AppConfig,
    ) -> None:
        """Cross-table SQL checks use the synthetic ``__sql_check__/`` FQN prefix
        and bypass the catalog visibility filter — they should always be included."""
        job_service_mock.list_dryrun_rows.return_value = [
            _make_row("run-sql-check", "FAILED", fqn="__sql_check__/orders_have_customers"),
        ]
        sql_executor.query_dicts.return_value = []

        result = list_recent_validation_failures(
            job_svc=job_service_mock,
            app_conf=app_conf,
            user_catalogs=frozenset(),  # empty — no catalog access
            sql=sql_executor,
        )

        assert len(result) == 1
        assert result[0].run_id == "run-sql-check"

    def test_result_bounded_at_limit(
        self,
        job_service_mock: MagicMock,
        sql_executor: MagicMock,
        app_conf: AppConfig,
    ) -> None:
        over_limit = DRYRUN_LIMIT + 5
        job_service_mock.list_dryrun_rows.return_value = [
            _make_row(f"run-{i}", "FAILED") for i in range(over_limit)
        ]
        sql_executor.query_dicts.return_value = []

        result = list_recent_validation_failures(
            job_svc=job_service_mock,
            app_conf=app_conf,
            user_catalogs=frozenset({"main"}),
            sql=sql_executor,
        )

        assert len(result) == DRYRUN_LIMIT

    def test_returns_minimal_fields_only(
        self,
        job_service_mock: MagicMock,
        sql_executor: MagicMock,
        app_conf: AppConfig,
    ) -> None:
        """RunFailureOut carries only run_id, source_table_fqn, status, created_at."""
        job_service_mock.list_dryrun_rows.return_value = [
            _make_row("r1", "FAILED", created_at="2025-06-01T12:00:00"),
        ]
        sql_executor.query_dicts.return_value = []

        result = list_recent_validation_failures(
            job_svc=job_service_mock,
            app_conf=app_conf,
            user_catalogs=frozenset({"main"}),
            sql=sql_executor,
        )

        row = result[0]
        assert isinstance(row, RunFailureOut)
        assert row.run_id == "r1"
        assert row.status == "FAILED"
        assert row.source_table_fqn == "main.public.orders"
        assert row.created_at == "2025-06-01T12:00:00"
        # Heavy fields must NOT be present on RunFailureOut
        assert not hasattr(row, "error_message")
        assert not hasattr(row, "total_rows")
        assert not hasattr(row, "checks")

    def test_empty_list_when_no_failures(
        self,
        job_service_mock: MagicMock,
        sql_executor: MagicMock,
        app_conf: AppConfig,
    ) -> None:
        job_service_mock.list_dryrun_rows.return_value = [
            _make_row("r1", "SUCCESS"),
            _make_row("r2", "RUNNING"),
        ]
        sql_executor.query_dicts.return_value = []

        result = list_recent_validation_failures(
            job_svc=job_service_mock,
            app_conf=app_conf,
            user_catalogs=frozenset({"main"}),
            sql=sql_executor,
        )

        assert result == []


# ---------------------------------------------------------------------------
# Profiler recent-failures
# ---------------------------------------------------------------------------


class TestListRecentProfileFailures:
    """``GET /profiler/runs/recent-failures`` filters to FAILED, bounds, minimal shape."""

    def test_returns_only_failed_rows(
        self,
        job_service_mock: MagicMock,
        app_conf: AppConfig,
    ) -> None:
        job_service_mock.list_run_rows.return_value = [
            _make_row("p-success", "SUCCESS"),
            _make_row("p-running", "RUNNING"),
            _make_row("p-canceled", "CANCELED"),
            _make_row("p-failed", "FAILED"),
        ]

        result = list_recent_profile_failures(
            job_svc=job_service_mock,
            app_conf=app_conf,
        )

        assert len(result) == 1
        assert result[0].run_id == "p-failed"
        assert result[0].status == "FAILED"

    def test_result_bounded_at_limit(
        self,
        job_service_mock: MagicMock,
        app_conf: AppConfig,
    ) -> None:
        over_limit = PROFILER_LIMIT + 5
        job_service_mock.list_run_rows.return_value = [
            _make_row(f"p-{i}", "FAILED") for i in range(over_limit)
        ]

        result = list_recent_profile_failures(
            job_svc=job_service_mock,
            app_conf=app_conf,
        )

        assert len(result) == PROFILER_LIMIT

    def test_returns_minimal_fields_only(
        self,
        job_service_mock: MagicMock,
        app_conf: AppConfig,
    ) -> None:
        job_service_mock.list_run_rows.return_value = [
            _make_row("p1", "FAILED", fqn="cat.sch.tbl", created_at="2025-06-15T08:00:00"),
        ]

        result = list_recent_profile_failures(
            job_svc=job_service_mock,
            app_conf=app_conf,
        )

        row = result[0]
        assert isinstance(row, RunFailureOut)
        assert row.run_id == "p1"
        assert row.status == "FAILED"
        assert row.source_table_fqn == "cat.sch.tbl"
        assert row.created_at == "2025-06-15T08:00:00"
        assert not hasattr(row, "error_message")
        assert not hasattr(row, "rows_profiled")
        assert not hasattr(row, "generated_rules")

    def test_empty_list_when_no_failures(
        self,
        job_service_mock: MagicMock,
        app_conf: AppConfig,
    ) -> None:
        job_service_mock.list_run_rows.return_value = [
            _make_row("p1", "SUCCESS"),
        ]

        result = list_recent_profile_failures(
            job_svc=job_service_mock,
            app_conf=app_conf,
        )

        assert result == []

    def test_non_failed_rows_before_failures_do_not_inflate_count(
        self,
        job_service_mock: MagicMock,
        app_conf: AppConfig,
    ) -> None:
        """Non-FAILED rows interspersed with FAILED ones are skipped, not counted."""
        rows: list[dict[str, str | None]] = []
        for i in range(PROFILER_LIMIT):
            rows.append(_make_row(f"f-{i}", "FAILED"))
            rows.append(_make_row(f"s-{i}", "SUCCESS"))
        job_service_mock.list_run_rows.return_value = rows

        result = list_recent_profile_failures(
            job_svc=job_service_mock,
            app_conf=app_conf,
        )

        assert len(result) == PROFILER_LIMIT
        assert all(r.status == "FAILED" for r in result)
