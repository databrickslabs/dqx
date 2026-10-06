"""Live verification of Studio setup resources for bundle and Marketplace installs."""

import asyncio
import os
from collections.abc import Callable
from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InvalidParameterValue, PermissionDenied
from databricks.sdk.service.catalog import PermissionsChange, Privilege
from databricks.sdk.service.jobs import JobRunAs

from tests.integration.conftest import AppLiveSetup, LiveJob, LiveResources, grant_runner_wheel_privileges

from databricks_labs_dqx_app.backend.migrations.postgres import PgMigrationRunner
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.compute_service import ComputeService
from databricks_labs_dqx_app.backend.setup.access import GENIE_ALLOWLIST, AudienceAccess
from databricks_labs_dqx_app.backend.setup.audience import resolve_audience
from databricks_labs_dqx_app.backend.setup.checks import ResourceCheckers
from databricks_labs_dqx_app.backend.setup.job_manager import TaskRunnerJobManager
from databricks_labs_dqx_app.backend.setup.models import SetupState, SetupStepId, StepState
from databricks_labs_dqx_app.backend.setup.resources import (
    BootstrapResources,
    LakebaseConnection,
    build_active_resources,
)
from databricks_labs_dqx_app.backend.setup.storage import StudioStorage
from databricks_labs_dqx_app.backend.startup import publish_wheels_to_volume
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

_EXPECTED_OLTP_TABLES = (
    "dq_migrations",
    "dq_app_settings",
    "dq_resolved_rules",
    "dq_resolved_rules_history",
    "dq_role_mappings",
    "dq_run_configs",
    "dq_comments",
    "dq_schedule_runs",
    "dq_schedule_configs",
    "dq_schedule_configs_history",
    "dq_rules",
    "dq_rule_versions",
    "dq_rules_history",
    "dq_monitored_tables",
    "dq_applied_rules",
    "dq_pending_applications",
    "dq_tag_auto_suppressions",
    "dq_monitored_table_versions",
    "dq_data_products",
    "dq_data_product_members",
    "dq_run_sets",
    "dq_run_set_members",
    "dq_run_review_status",
    "dq_run_review_status_history",
    "dq_role_mappings_history",
    "dq_object_grants",
    "dq_object_grants_history",
    "dq_score_cache",
    "dq_score_history",
    "dq_rule_embeddings",
)


def test_deployment_storage_is_verified_and_receives_wheels(live_resources: LiveResources) -> None:
    """Deployment-provided storage is verified without provisioning and receives setup wheels."""
    resources = live_resources.resources
    volume = live_resources.volume

    assert resources.volume.catalog == volume.catalog
    assert resources.volume.schema == volume.schema
    assert resources.volume.volume == volume.volume
    assert live_resources.bootstrap.check_app_identity().state == StepState.PASSED
    assert live_resources.bootstrap.check_lakebase().state == StepState.PASSED
    assert live_resources.bootstrap.ensure_lakebase_schema().state == StepState.PASSED
    assert live_resources.checkers.check_unity_catalog().state == StepState.PASSED
    assert live_resources.checkers.ensure_storage(provision=False).state == StepState.PASSED

    assert live_resources.workspace.schemas.get(f"{volume.catalog}.{resources.tmp_schema}").name == resources.tmp_schema
    assert (
        live_resources.workspace.schemas.get(f"{volume.catalog}.{resources.genie_schema}").name
        == resources.genie_schema
    )

    wheel_paths = asyncio.run(publish_wheels_to_volume(live_resources.workspace, volume.path))
    assert wheel_paths == [f"{volume.path}/{live_resources.wheel.name}"]
    uploaded = live_resources.workspace.files.download(wheel_paths[0]).contents
    assert uploaded is not None
    assert uploaded.read() == live_resources.wheel.read_bytes()


class _NoGenieSpaceSettings:
    """Settings without a provisioned Genie space, so Genie sharing is not applicable."""

    def get_setting(self, key: str) -> str | None:
        """Return no stored value."""
        return None

    def save_setting(self, key: str, value: str, *, user_email: str | None = None) -> None:
        """Ignore writes; the test never persists settings."""


def test_marketplace_setup_provisions_prefix_storage(
    ws: WorkspaceClient,
    make_marketplace_storage: Callable[[], StudioStorage],
    live_warehouse: object,
) -> None:
    """Marketplace setup creates every prefix-derived schema and the wheels volume, then verifies access.

    The test profile acts as the app service principal and owns the factory catalog. Audience
    grant verification runs only when DQX_TEST_AUDIENCE_GROUP names an existing account group
    assigned to the workspace; otherwise only storage is verified.
    """
    warehouse_id = getattr(getattr(live_warehouse, "response", None), "id", None)
    if not isinstance(warehouse_id, str) or not warehouse_id:
        raise RuntimeError("The test warehouse did not return an ID.")
    audience_group = os.environ.get("DQX_TEST_AUDIENCE_GROUP", "").strip()
    storage = make_marketplace_storage()
    lakebase = LakebaseConnection(
        "projects/unused/branches/unused/endpoints/unused", None, 5432, "databricks_postgres", None, None, "dqx_studio"
    )
    resources = build_active_resources(
        BootstrapResources(lakebase, warehouse_id, None),
        storage,
        resolve_audience([audience_group or "dqx-integration-audience"], "admins", allow_broad=False),
    )
    sql = SqlExecutor(ws=ws, warehouse_id=warehouse_id, catalog=storage.catalog, schema=storage.schema)
    app_identity = (ws.current_user.me().user_name or "").strip()
    if not app_identity:
        raise RuntimeError("The test profile did not return a workspace user name.")
    checkers = ResourceCheckers(
        resources=resources,
        workspace=ws,
        sql=sql,
        compute=ComputeService(sp_ws=ws, app_settings=create_autospec(AppSettingsService, instance=True)),
        app_sp_id=app_identity,
    )

    step = checkers.ensure_storage(provision=True)

    assert step.id == SetupStepId.STORAGE
    assert step.state == StepState.PASSED
    for schema in storage.schemas:
        assert ws.schemas.get(f"{storage.catalog}.{schema}").name == schema
    assert ws.volumes.read(f"{storage.catalog}.{storage.schema}.{storage.volume}").name == storage.volume
    assert checkers.ensure_storage(provision=False).state == StepState.PASSED

    if not audience_group:
        return
    assert checkers.check_unity_catalog().state == StepState.PASSED
    # Activation creates the Genie allowlist views; stand-in tables let grants be applied and verified.
    genie_schema = f"{sql.q(storage.catalog)}.{sql.q(storage.genie_schema)}"
    for name in GENIE_ALLOWLIST:
        sql.execute_no_schema(f"CREATE TABLE IF NOT EXISTS {genie_schema}.{sql.q(name)} (id INT)")
    access = AudienceAccess(
        resources=resources,
        workspace=ws,
        sql=sql,
        settings=_NoGenieSpaceSettings(),
        app_name="dqx-studio-integration",
        dashboard_id="",
    )

    result = access.reconcile_access()

    assert result.state == StepState.PASSED, result.instructions


def test_job_update_preserves_runner_identity(live_job: LiveJob) -> None:
    """A Studio job update leaves the external runner identity unchanged."""
    before = live_job.workspace.jobs.get(live_job.job_id).settings.run_as
    assert before is not None

    live_job.manager.configure(live_job.job_id, [])

    after = live_job.workspace.jobs.get(live_job.job_id).settings.run_as
    assert after == before


def test_warehouse_readiness_uses_the_bound_app_service_principal(live_resources: LiveResources) -> None:
    """A profile without an app-service-principal client skips rather than faking warehouse access."""
    result = live_resources.checkers.check_warehouse()
    if result.state != StepState.PASSED:
        pytest.skip("PROFILE must authenticate as the bound app service principal with CAN_USE on the test warehouse.")
    assert result.id == SetupStepId.WAREHOUSE


def test_job_setup_admin_grant_is_observable_when_profile_permits(live_job: LiveJob) -> None:
    """Granting the setup admin uses the real Jobs permissions API or explicitly skips."""
    try:
        live_job.manager.grant_setup_admin(live_job.job_id, live_job.admin_user)
    except (InvalidParameterValue, PermissionDenied):
        pytest.skip("PROFILE must permit a CAN_MANAGE grant on the factory-created task-runner job.")

    permissions = live_job.workspace.jobs.get_permissions(str(live_job.job_id)).access_control_list or []
    assert any(
        entry.user_name == live_job.admin_user
        and any(
            getattr(permission.permission_level, "value", permission.permission_level) in {"CAN_MANAGE", "IS_OWNER"}
            for permission in entry.all_permissions or []
        )
        for entry in permissions
    )


def test_job_runner_validation_requires_a_preconfigured_external_service_principal(
    ws: WorkspaceClient,
    make_preconfigured_job: Callable[..., int],
) -> None:
    """Validate a real run-as identity only when an external runner principal is supplied."""
    runner_principal = (os.environ.get("DQX_TEST_RUNNER_SERVICE_PRINCIPAL") or "").strip()
    if not runner_principal:
        pytest.skip("Set DQX_TEST_RUNNER_SERVICE_PRINCIPAL to a usable external runner service principal.")
    try:
        job_id = make_preconfigured_job(run_as=JobRunAs(service_principal_name=runner_principal))
    except (InvalidParameterValue, PermissionDenied) as err:
        pytest.skip(f"PROFILE cannot create a factory job with the external runner service principal: {err}")
    app_identity = (ws.current_user.me().user_name or "").strip()
    if not app_identity:
        raise RuntimeError("The test profile did not return a workspace user name.")

    result = TaskRunnerJobManager(ws).validate_run_as(job_id, app_identity)

    assert result.id == SetupStepId.TASK_RUNNER
    assert result.state == StepState.PASSED


@pytest.mark.parametrize("can_read_volume", [False, True])
def test_runner_volume_readiness_uses_external_principal_grants(
    live_resources: LiveResources,
    make_preconfigured_job: Callable[..., int],
    make_setup_schema: Callable[..., str],
    can_read_volume: bool,
) -> None:
    """Wheel readiness requires volume reads and usage on the existing temporary schema."""
    runner_principal = os.environ.get("DQX_TEST_RUNNER_SERVICE_PRINCIPAL", "").strip()
    if not runner_principal:
        pytest.skip("Set DQX_TEST_RUNNER_SERVICE_PRINCIPAL to a usable external runner service principal.")
    try:
        job_id = make_preconfigured_job(run_as=JobRunAs(service_principal_name=runner_principal))
    except (InvalidParameterValue, PermissionDenied):
        pytest.skip("PROFILE must permit a factory job with the external runner service principal.")
    resources = live_resources.resources
    make_setup_schema(catalog=resources.volume.catalog, schema=resources.tmp_schema)
    grant_runner_wheel_privileges(live_resources.workspace, resources, runner_principal)
    if not can_read_volume:
        volume = live_resources.volume
        live_resources.workspace.grants.update(
            "VOLUME",
            f"{volume.catalog}.{volume.schema}.{volume.volume}",
            changes=[PermissionsChange(principal=runner_principal, remove=[Privilege.READ_VOLUME])],
        )

    result = live_resources.checkers.check_runner_access(job_id)

    assert result.id == SetupStepId.TASK_RUNNER
    if can_read_volume:
        assert result.state == StepState.PASSED
    else:
        assert result.state == StepState.ACTION_REQUIRED
        assert result.code == "task_runner_permissions_missing"
        assert "READ VOLUME" in " ".join(result.instructions)


def test_postgres_migrations_create_oltp_tables(live_resources: LiveResources) -> None:
    """A real Lakebase endpoint receives every production OLTP migration table."""
    applied = PgMigrationRunner(live_resources.pg).run_all()

    assert applied > 0
    for table_name in _EXPECTED_OLTP_TABLES:
        assert live_resources.pg.query(
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{live_resources.resources.lakebase.schema}' "
            f"AND table_name = '{table_name}'"
        ) == [[table_name]]


def test_reconcile_applies_real_setup_actions(app_live_setup: AppLiveSetup) -> None:
    """An explicitly configured app-SP client reconciles the complete setup path."""
    reader_ws = app_live_setup.setup_workspace
    if token := os.environ.get("DQX_TEST_APPS_OBO_TOKEN"):
        reader_ws = WorkspaceClient(host=reader_ws.config.host, token=token, auth_type="pat")
    resources = app_live_setup.resources
    reader_sql = SqlExecutor(
        ws=reader_ws,
        warehouse_id=resources.warehouse_id,
        catalog=resources.volume.catalog,
        schema=resources.tmp_schema,
    )
    report = asyncio.run(
        app_live_setup.orchestrator.reconcile(setup_user=None, reader_ws=reader_ws, reader_sql=reader_sql)
    )

    assert report.state == SetupState.READY
    for step_id in (
        SetupStepId.LAKEBASE,
        SetupStepId.CONFIGURATION,
        SetupStepId.STORAGE,
        SetupStepId.TASK_RUNNER,
        SetupStepId.WHEELS,
        SetupStepId.MIGRATIONS,
    ):
        assert report.step(step_id).state == StepState.PASSED

    resources = app_live_setup.resources
    volume = app_live_setup.volume
    assert (
        app_live_setup.setup_workspace.schemas.get(f"{volume.catalog}.{resources.tmp_schema}").name
        == resources.tmp_schema
    )
    assert (
        app_live_setup.setup_workspace.schemas.get(f"{volume.catalog}.{resources.genie_schema}").name
        == resources.genie_schema
    )
    uploaded = app_live_setup.app_workspace.files.download(f"{volume.path}/{app_live_setup.wheel.name}").contents
    assert uploaded is not None
    assert uploaded.read() == app_live_setup.wheel.read_bytes()
    for table_name in _EXPECTED_OLTP_TABLES:
        assert app_live_setup.pg.query(
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{resources.lakebase.schema}' "
            f"AND table_name = '{table_name}'"
        ) == [[table_name]]
