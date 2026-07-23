"""Tests for ``list_validation_runs`` — payload-weight fix.

Verifies that the list endpoint always returns ``checks == []`` even when the
underlying row's ``checks_json`` column carries a full checks payload. The
heavy field is omitted from the list response to reduce the ~276 kB payload
to a few hundred bytes per row; the per-run detail path (``getDryRunResults``)
remains the authoritative source for full check definitions.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, create_autospec

from databricks_labs_dqx_app.backend.config import AppConfig
from databricks_labs_dqx_app.backend.models import ValidationRunSummaryOut
from databricks_labs_dqx_app.backend.services.job_service import JobService
from databricks_labs_dqx_app.backend.services.review_status_service import ReviewStatusService
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor


def _run_row(
    run_id: str = "r1",
    status: str = "SUCCESS",
    checks_json: str | None = None,
    source_table_fqn: str = "cat.sch.tbl",
) -> dict[str, str | None]:
    """Build a minimal dq_validation_runs row dict as the service would return."""
    return {
        "run_id": run_id,
        "status": status,
        "source_table_fqn": source_table_fqn,
        "checks_json": checks_json,
        "requesting_user": "alice@example.com",
        "canceled_by": None,
        "updated_at": "2026-07-01 10:00:00",
        "created_at": "2026-07-01 09:55:00",
        "sample_size": None,
        "total_rows": None,
        "valid_rows": None,
        "invalid_rows": None,
        "error_rows": None,
        "warning_rows": None,
        "run_type": "dryrun",
        "error_message": None,
        "duration_seconds": None,
        "job_run_id": None,
    }


def _make_deps(
    app_config: AppConfig,
    rows: list[dict[str, str | None]],
) -> tuple[MagicMock, MagicMock, MagicMock, MagicMock]:
    """Return (job_svc_mock, review_svc_mock, sql_mock, response_mock)."""
    job_svc = create_autospec(JobService, instance=True)
    job_svc.list_dryrun_rows.return_value = rows
    # get_run_status is called by reconcile_running_rows — return a stand-in.
    from types import SimpleNamespace

    job_svc.get_run_status.return_value = SimpleNamespace(state="RUNNING", result_state=None, message=None)

    review_svc = create_autospec(ReviewStatusService, instance=True)
    review_svc.bulk_get_effective.return_value = {}

    sql = create_autospec(SqlExecutor, instance=True)
    # _get_running_job_run_ids returns an empty mapping (no RUNNING rows to reconcile)
    sql.query.return_value = []

    response = MagicMock()
    response.headers = {}

    return job_svc, review_svc, sql, response


class TestListValidationRunsChecksAlwaysEmpty:
    """``checks`` field must be ``[]`` in every list response row."""

    async def test_checks_empty_when_checks_json_has_content(self, app_config: AppConfig) -> None:
        """The heavy ``checks_json`` blob must NOT be parsed into the list row."""
        heavy_checks = [
            {
                "criticality": "error",
                "check": {"function": "is_not_null", "arguments": {"column": "id"}},
            }
        ] * 50  # simulate 50-rule payload
        row = _run_row(checks_json=json.dumps(heavy_checks))
        job_svc, review_svc, sql, response = _make_deps(app_config, [row])

        from databricks_labs_dqx_app.backend.routes.v1.dryrun import list_validation_runs

        results: list[ValidationRunSummaryOut] = await list_validation_runs(
            response=response,
            job_svc=job_svc,
            review_svc=review_svc,
            app_conf=app_config,
            user_catalogs=frozenset({"cat"}),
            sql=sql,
            review_status=None,
        )

        assert len(results) == 1
        assert results[0].checks == [], "checks must be empty in the list response"

    async def test_checks_empty_when_checks_json_is_none(self, app_config: AppConfig) -> None:
        """Rows without checks_json also yield an empty list (not None)."""
        row = _run_row(checks_json=None)
        job_svc, review_svc, sql, response = _make_deps(app_config, [row])

        from databricks_labs_dqx_app.backend.routes.v1.dryrun import list_validation_runs

        results = await list_validation_runs(
            response=response,
            job_svc=job_svc,
            review_svc=review_svc,
            app_conf=app_config,
            user_catalogs=frozenset({"cat"}),
            sql=sql,
            review_status=None,
        )

        assert results[0].checks == []

    async def test_other_fields_are_still_populated(self, app_config: AppConfig) -> None:
        """Non-checks fields must still be populated correctly."""
        row = _run_row(run_id="abc", status="FAILED", checks_json='[{"x":1}]')
        row["error_message"] = "boom"
        job_svc, review_svc, sql, response = _make_deps(app_config, [row])

        from databricks_labs_dqx_app.backend.routes.v1.dryrun import list_validation_runs

        results = await list_validation_runs(
            response=response,
            job_svc=job_svc,
            review_svc=review_svc,
            app_conf=app_config,
            user_catalogs=frozenset({"cat"}),
            sql=sql,
            review_status=None,
        )

        assert results[0].run_id == "abc"
        assert results[0].status == "FAILED"
        assert results[0].error_message == "boom"
        assert results[0].checks == []
