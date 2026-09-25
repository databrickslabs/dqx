"""Unit tests for the external validation-status endpoints (``/monitoring``).

Uptime monitors rely on a strict contract: 200 for a clean completed run,
503 for anything else. Rows arrive from the Statement Execution API with every
value serialised as a string, so the tests feed string counts on purpose.
"""

from __future__ import annotations

from unittest.mock import MagicMock, create_autospec

import pytest
from fastapi import HTTPException

from databricks_labs_dqx_app.backend.routes.v1.monitoring import (
    get_validation_status_by_run,
    get_validation_status_by_table,
)
from databricks_labs_dqx_app.backend.services.job_service import JobService


def _row(status: str = "SUCCESS", error_rows: str | None = "0") -> dict[str, str | None]:
    return {
        "status": status,
        "source_table_fqn": "main.sales.orders",
        "run_id": "run-1",
        "error_rows": error_rows,
        "warning_rows": "0",
        "updated_at": "2026-09-25T00:00:00Z",
    }


@pytest.fixture
def job_svc() -> MagicMock:
    return create_autospec(JobService, instance=True)


def test_clean_run_returns_200(job_svc: MagicMock, sql_executor_mock: MagicMock) -> None:
    job_svc.get_latest_run_result_row.return_value = _row(error_rows="0")

    out = get_validation_status_by_table("main.sales.orders", job_svc, sql_executor_mock)

    assert out.status == "SUCCESS"
    assert out.error_rows == 0


def test_null_error_rows_on_success_is_clean(job_svc: MagicMock, sql_executor_mock: MagicMock) -> None:
    job_svc.get_run_result_row.return_value = _row(error_rows=None)

    out = get_validation_status_by_run("run-1", job_svc, sql_executor_mock)

    assert out.status == "SUCCESS"


def test_run_with_error_rows_returns_503(job_svc: MagicMock, sql_executor_mock: MagicMock) -> None:
    job_svc.get_latest_run_result_row.return_value = _row(error_rows="3")

    with pytest.raises(HTTPException) as exc:
        get_validation_status_by_table("main.sales.orders", job_svc, sql_executor_mock)

    assert exc.value.status_code == 503


def test_failed_run_returns_503(job_svc: MagicMock, sql_executor_mock: MagicMock) -> None:
    job_svc.get_run_result_row.return_value = _row(status="FAILED")

    with pytest.raises(HTTPException) as exc:
        get_validation_status_by_run("run-1", job_svc, sql_executor_mock)

    assert exc.value.status_code == 503


def test_unknown_table_returns_404(job_svc: MagicMock, sql_executor_mock: MagicMock) -> None:
    job_svc.get_latest_run_result_row.return_value = None

    with pytest.raises(HTTPException) as exc:
        get_validation_status_by_table("main.sales.missing", job_svc, sql_executor_mock)

    assert exc.value.status_code == 404
