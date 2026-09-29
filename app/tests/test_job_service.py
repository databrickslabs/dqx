"""Runtime job-ID consumers for task execution and workspace links."""

import asyncio
from unittest.mock import MagicMock, create_autospec

import pytest

from databricks_labs_dqx_app.backend.dependencies import get_job_service
from databricks_labs_dqx_app.backend.runtime import rt
from databricks_labs_dqx_app.backend.routes.v1.config import get_workspace_host
from databricks_labs_dqx_app.backend.services.job_service import JobService
from databricks_labs_dqx_app.backend.setup.resources import ActiveResources, LakebaseConnection, VolumeLocation
from databricks_labs_dqx_app.backend.setup.runtime import setup_runtime
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor


def test_job_service_submits_to_resolved_setup_job_id(sql_executor_mock: MagicMock) -> None:
    """Submissions use the reconciled job ID, not the obsolete config binding."""
    workspace = MagicMock(name="WorkspaceClient")
    workspace.jobs.run_now.return_value.run_id = 17
    settings = MagicMock(name="AppSettingsService")
    settings.get_sql_warehouse_id.return_value = "warehouse-id"
    previous_job_id = setup_runtime.job_id
    previous_resources = rt.resources
    setup_runtime.job_id = 42
    rt.activate(
        ActiveResources(
            volume=VolumeLocation("catalog", "schema", "wheels", "/Volumes/catalog/schema/wheels"),
            lakebase=LakebaseConnection(
                endpoint="projects/project/branches/branch/endpoints/primary",
                host=None,
                port=5432,
                database="databricks_postgres",
                username=None,
                password=None,
                schema="dqx_studio",
            ),
            warehouse_id="warehouse-id",
            job_id=None,
            tmp_schema="dqx_studio_tmp",
            genie_schema="genie",
        )
    )
    # get_job_service reads all resolved Lakebase coordinates off the executor;
    # give them concrete string values (a bare MagicMock would leak un-serializable
    # attributes into the job parameters). Use schema/database distinct from the
    # ActiveResources values above to prove the executor is the source of truth.
    oltp_mock = MagicMock(name="oltp_sql")
    oltp_mock.endpoint = "projects/project/branches/branch/endpoints/primary"
    oltp_mock.host = "pg.example.databricks.com"
    oltp_mock.port = 5432
    oltp_mock.username = "sp-runner"
    oltp_mock.database = "pg_db_from_oltp"
    oltp_mock.schema = "pg_schema_from_oltp"
    try:
        service = asyncio.run(get_job_service(workspace, sql_executor_mock, oltp_mock, settings))
        result = service.submit_run("profile", "catalog.schema.view", {}, "run-1", "user@example.com")
    finally:
        setup_runtime.job_id = previous_job_id
        rt.resources = previous_resources

    assert result == 17
    assert workspace.jobs.run_now.call_args.kwargs["job_id"] == 42
    # schema/database are threaded from the executor, not the static resources.
    params = workspace.jobs.run_now.call_args.kwargs["job_parameters"]
    assert params["lakebase_schema"] == "pg_schema_from_oltp"
    assert params["lakebase_database"] == "pg_db_from_oltp"


def _job_service_with_failing_submit() -> tuple[JobService, MagicMock, MagicMock]:
    sql = create_autospec(SqlExecutor, instance=True)
    sql.catalog = "cat"
    sql.schema = "sch"
    sql.warehouse_id = "wh"
    oltp = create_autospec(SqlExecutor, instance=True)
    oltp.fqn.side_effect = lambda t: f"dqx_studio.{t}"
    # Staging (and thus the delete-on-failure path) only runs when Lakebase is
    # enabled — i.e. the OLTP executor's dialect is Postgres.
    oltp.dialect = "postgres"
    ws = MagicMock(name="WorkspaceClient")
    ws.jobs.run_now.side_effect = RuntimeError("job is disabled")
    return JobService(ws=ws, job_id="42", sql=sql, oltp_sql=oltp), ws, oltp


def test_submit_deletes_staged_row_when_submission_fails() -> None:
    """An oversized (staged) config that fails to submit deletes its orphaned row."""
    service, _ws, oltp = _job_service_with_failing_submit()
    big_config = {"checks": [{"name": f"rule_{i}", "check": {"function": "is_not_null"}} for i in range(300)]}

    with pytest.raises(RuntimeError):
        service.submit_run("dryrun", "cat.sch.view", big_config, "run-1", "user@example.com")

    oltp.delete.assert_called_once()
    assert oltp.delete.call_args.kwargs["where"] == {"run_id": "run-1"}


def test_submit_does_not_delete_when_config_inlined() -> None:
    """A small (inlined) config was never staged, so a submit failure deletes nothing."""
    service, _ws, oltp = _job_service_with_failing_submit()

    with pytest.raises(RuntimeError):
        service.submit_run("dryrun", "cat.sch.view", {"checks": [{"name": "c1"}]}, "run-2", "user@example.com")

    oltp.delete.assert_not_called()


def test_workspace_host_uses_resolved_setup_job_id() -> None:
    """Job deep links follow the job resolved by setup, not the environment binding."""
    previous_job_id = setup_runtime.job_id
    setup_runtime.job_id = 42
    try:
        response = get_workspace_host()
    finally:
        setup_runtime.job_id = previous_job_id

    assert response.job_id == "42"
