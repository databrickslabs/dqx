"""Runtime job-ID consumers for task execution and workspace links."""

import asyncio
from unittest.mock import MagicMock

from databricks_labs_dqx_app.backend.dependencies import get_job_service
from databricks_labs_dqx_app.backend.runtime import rt
from databricks_labs_dqx_app.backend.routes.v1.config import get_workspace_host
from databricks_labs_dqx_app.backend.setup.resources import ActiveResources, LakebaseConnection, VolumeLocation
from databricks_labs_dqx_app.backend.setup.runtime import setup_runtime


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
    try:
        service = asyncio.run(get_job_service(workspace, sql_executor_mock, settings))
        result = service.submit_run("profile", "catalog.schema.view", {}, "run-1", "user@example.com")
    finally:
        setup_runtime.job_id = previous_job_id
        rt.resources = previous_resources

    assert result == 17
    assert workspace.jobs.run_now.call_args.kwargs["job_id"] == 42


def test_workspace_host_uses_resolved_setup_job_id() -> None:
    """Job deep links follow the job resolved by setup, not the environment binding."""
    previous_job_id = setup_runtime.job_id
    setup_runtime.job_id = 42
    try:
        response = get_workspace_host()
    finally:
        setup_runtime.job_id = previous_job_id

    assert response.job_id == "42"
