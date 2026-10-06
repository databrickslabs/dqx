"""Runtime job-ID consumers for task execution and workspace links."""

import asyncio
from unittest.mock import MagicMock, create_autospec
from databricks.sdk.service.jobs import Job, JobSettings, JobRunAs

import pytest

from databricks_labs_dqx_app.backend.setup.audience import resolve_audience
from databricks_labs_dqx_app.backend.dependencies import get_job_service, get_schedule_grant_service
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
    workspace.jobs.get.return_value = Job(settings=JobSettings(run_as=JobRunAs(service_principal_name="runner-id")))
    workspace.current_user.me.return_value.user_name = "app-id"
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
            demo_schema="studio_demo",
            audience=resolve_audience(["data-team"], "admins", allow_broad=False),
        )
    )
    try:
        service = asyncio.run(get_job_service(workspace, sql_executor_mock, settings))
        result = service.submit_run("profile", "catalog.schema.view", {}, "run-1", "user@example.com")
    finally:
        setup_runtime.job_id = previous_job_id
        rt.resources = previous_resources

    assert result == 17
    assert workspace.jobs.run_now.call_args.kwargs["job_id"] == 42
    params = workspace.jobs.run_now.call_args.kwargs["job_parameters"]
    assert not [name for name in params if name.startswith("lakebase_")]


def test_schedule_grants_use_resolved_job_identity() -> None:
    workspace = MagicMock()
    workspace.jobs.get.return_value = Job(settings=JobSettings(run_as=JobRunAs(service_principal_name="runner-id")))
    previous_job_id = setup_runtime.job_id
    previous_resources = rt.resources
    setup_runtime.job_id = 42
    rt.resources = ActiveResources(
        volume=VolumeLocation("catalog", "schema", "wheels", "/Volumes/catalog/schema/wheels"),
        lakebase=LakebaseConnection("projects/p/branches/b/endpoints/e", None, 5432, "db", None, None, "schema"),
        warehouse_id="warehouse",
        job_id=None,
        tmp_schema="tmp",
        genie_schema="genie",
        demo_schema="studio_demo",
        audience=resolve_audience(["data-team"], "admins", allow_broad=False),
    )
    try:
        service = asyncio.run(get_schedule_grant_service(workspace, workspace))
        assert service.task_runner_sp_id() == "runner-id"
        workspace.jobs.get.assert_called_once_with(42)
    finally:
        setup_runtime.job_id = previous_job_id
        rt.resources = previous_resources


def _job_service_with_failing_submit() -> tuple[JobService, MagicMock, MagicMock]:
    sql = create_autospec(SqlExecutor, instance=True)
    sql.catalog = "cat"
    sql.schema = "sch"
    sql.warehouse_id = "wh"
    sql.fqn.side_effect = lambda t: f"cat.sch.{t}"
    ws = MagicMock(name="WorkspaceClient")
    ws.jobs.run_now.side_effect = RuntimeError("job is disabled")
    return JobService(ws=ws, job_id="42", sql=sql), ws, sql


def test_submit_deletes_staged_row_when_submission_fails() -> None:
    """An oversized (staged) config that fails to submit deletes its orphaned row."""
    service, _ws, sql = _job_service_with_failing_submit()
    big_config = {"checks": [{"name": f"rule_{i}", "check": {"function": "is_not_null"}} for i in range(300)]}

    with pytest.raises(RuntimeError):
        service.submit_run("dryrun", "cat.sch.view", big_config, "run-1", "user@example.com")

    sql.delete.assert_called_once()
    assert sql.delete.call_args.kwargs["where"] == {"run_id": "run-1"}


def test_submit_does_not_delete_when_config_inlined() -> None:
    """A small (inlined) config was never staged, so a submit failure deletes nothing."""
    service, _ws, sql = _job_service_with_failing_submit()

    with pytest.raises(RuntimeError):
        service.submit_run("dryrun", "cat.sch.view", {"checks": [{"name": "c1"}]}, "run-2", "user@example.com")

    sql.delete.assert_not_called()


def test_workspace_host_uses_resolved_setup_job_id() -> None:
    """Job deep links follow the job resolved by setup, not the environment binding."""
    previous_job_id = setup_runtime.job_id
    setup_runtime.job_id = 42
    try:
        response = get_workspace_host()
    finally:
        setup_runtime.job_id = previous_job_id

    assert response.job_id == "42"
