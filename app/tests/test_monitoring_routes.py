"""Unit tests for the external validation-status endpoints (``/monitoring``).

Uptime monitors rely on a strict contract: 200 for a clean completed run,
503 for anything else. Rows arrive from the Statement Execution API with every
value serialised as a string, so the tests feed string counts on purpose.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, create_autospec

import pytest
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.routes.v1.monitoring import (
    get_validation_status_by_run,
    get_validation_status_by_table,
)
from databricks_labs_dqx_app.backend.services.job_service import JobService


def _row(
    status: str = "SUCCESS",
    error_rows: str | None = "0",
    warning_rows: str | None = "0",
    updated_at: str | None = "2026-09-25T00:00:00Z",
) -> dict[str, str | None]:
    return {
        "status": status,
        "source_table_fqn": "main.sales.orders",
        "run_id": "run-1",
        "error_rows": error_rows,
        "warning_rows": warning_rows,
        "updated_at": updated_at,
    }


@pytest.fixture
def job_svc() -> MagicMock:
    return create_autospec(JobService, instance=True)


def test_clean_run_returns_200(job_svc: MagicMock, sql_executor_mock: MagicMock) -> None:
    job_svc.get_latest_completed_run_result_row.return_value = _row(error_rows="0")

    out = get_validation_status_by_table("main.sales.orders", job_svc, sql_executor_mock)

    assert out.status == "SUCCESS"
    assert out.error_rows == 0


def test_warnings_without_errors_return_200(job_svc: MagicMock, sql_executor_mock: MagicMock) -> None:
    job_svc.get_latest_completed_run_result_row.return_value = _row(error_rows="0", warning_rows="7")

    out = get_validation_status_by_table("main.sales.orders", job_svc, sql_executor_mock)

    assert out.status == "SUCCESS"
    assert out.warning_rows == 7
    assert out.error_rows == 0


def test_null_error_rows_on_success_is_clean(job_svc: MagicMock, sql_executor_mock: MagicMock) -> None:
    job_svc.get_run_result_row.return_value = _row(error_rows=None)

    out = get_validation_status_by_run("run-1", job_svc, sql_executor_mock)

    assert out.status == "SUCCESS"


def test_run_with_error_rows_returns_503(job_svc: MagicMock, sql_executor_mock: MagicMock) -> None:
    job_svc.get_latest_completed_run_result_row.return_value = _row(error_rows="3")

    with pytest.raises(HTTPException) as exc:
        get_validation_status_by_table("main.sales.orders", job_svc, sql_executor_mock)

    assert exc.value.status_code == 503


def test_failed_run_returns_503(job_svc: MagicMock, sql_executor_mock: MagicMock) -> None:
    job_svc.get_run_result_row.return_value = _row(status="FAILED")

    with pytest.raises(HTTPException) as exc:
        get_validation_status_by_run("run-1", job_svc, sql_executor_mock)

    assert exc.value.status_code == 503


def test_unknown_table_returns_404(job_svc: MagicMock, sql_executor_mock: MagicMock) -> None:
    job_svc.get_latest_completed_run_result_row.return_value = None

    with pytest.raises(HTTPException) as exc:
        get_validation_status_by_table("main.sales.missing", job_svc, sql_executor_mock)

    assert exc.value.status_code == 404


@pytest.mark.parametrize("table_fqn", ["main.sales", "main.sales.orders\\' OR 1=1 --", "main.`sales`x.orders"])
def test_malformed_table_name_returns_400(job_svc: MagicMock, sql_executor_mock: MagicMock, table_fqn: str) -> None:
    with pytest.raises(HTTPException) as exc:
        get_validation_status_by_table(table_fqn, job_svc, sql_executor_mock)

    assert exc.value.status_code == 400
    job_svc.get_latest_completed_run_result_row.assert_not_called()


@pytest.mark.parametrize("run_id", ["", "run-1\\' OR 1=1 --", "run 1", "a" * 257, "run-1\n"])
def test_malformed_run_id_returns_400(job_svc: MagicMock, sql_executor_mock: MagicMock, run_id: str) -> None:
    with pytest.raises(HTTPException) as exc:
        get_validation_status_by_run(run_id, job_svc, sql_executor_mock)

    assert exc.value.status_code == 400
    job_svc.get_run_result_row.assert_not_called()


def test_canceled_run_is_reported_when_asked_for_by_id(job_svc: MagicMock, sql_executor_mock: MagicMock) -> None:
    job_svc.get_run_result_row.return_value = _row(status="CANCELED")

    with pytest.raises(HTTPException) as exc:
        get_validation_status_by_run("run-1", job_svc, sql_executor_mock)

    assert exc.value.status_code == 503


@pytest.mark.parametrize("run_id", ["3f2b9c1e-7d4a-4c2e-9b1a-2f6e8d0c5a71", "nightly:2026-09-30.1"])
def test_observer_style_run_ids_are_accepted(job_svc: MagicMock, sql_executor_mock: MagicMock, run_id: str) -> None:
    job_svc.get_run_result_row.return_value = _row()

    out = get_validation_status_by_run(run_id, job_svc, sql_executor_mock)

    assert out.status == "SUCCESS"


def test_recent_clean_run_within_max_age_returns_200(job_svc: MagicMock, sql_executor_mock: MagicMock) -> None:
    completed = (datetime.now(UTC) - timedelta(minutes=5)).isoformat()
    job_svc.get_latest_completed_run_result_row.return_value = _row(updated_at=completed)

    out = get_validation_status_by_table("main.sales.orders", job_svc, sql_executor_mock, max_age_minutes=60)

    assert out.stale is False


@pytest.mark.parametrize("updated_at", ["2020-01-01T00:00:00Z", "2020-01-01 00:00:00", None, "not a timestamp"])
def test_clean_run_older_than_max_age_returns_503_stale(
    job_svc: MagicMock, sql_executor_mock: MagicMock, updated_at: str | None
) -> None:
    job_svc.get_latest_completed_run_result_row.return_value = _row(updated_at=updated_at)

    with pytest.raises(HTTPException) as exc:
        get_validation_status_by_table("main.sales.orders", job_svc, sql_executor_mock, max_age_minutes=60)

    assert exc.value.status_code == 503
    assert exc.value.detail["stale"] is True


def test_old_run_is_not_stale_without_max_age(job_svc: MagicMock, sql_executor_mock: MagicMock) -> None:
    job_svc.get_latest_completed_run_result_row.return_value = _row(updated_at="2020-01-01T00:00:00Z")

    out = get_validation_status_by_table("main.sales.orders", job_svc, sql_executor_mock)

    assert out.stale is False
