"""Live verification of Marketplace-bound Studio setup resources."""

import asyncio
import os
from collections.abc import Callable

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InvalidParameterValue, PermissionDenied
from databricks.sdk.service.jobs import JobRunAs

from tests.integration.conftest import AppLiveSetup, LiveJob, LiveResources

from databricks_labs_dqx_app.backend.migrations.postgres import PgMigrationRunner
from databricks_labs_dqx_app.backend.setup.job_manager import TaskRunnerJobManager
from databricks_labs_dqx_app.backend.setup.models import SetupState, SetupStepId, StepState
from databricks_labs_dqx_app.backend.startup import publish_wheels_to_volume

_EXPECTED_OLTP_TABLES = (
    "dq_migrations",
    "dq_app_settings",
    "dq_quality_rules",
    "dq_quality_rules_history",
    "dq_role_mappings",
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


def test_bound_volume_derives_location_and_creates_siblings(live_resources: LiveResources) -> None:
    """A bound volume selects the actual catalog/schema and publishes setup wheels."""
    resources = live_resources.resources
    volume = live_resources.volume

    assert resources.volume.catalog == volume.catalog
    assert resources.volume.schema == volume.schema
    assert resources.volume.volume == volume.volume
    assert live_resources.checkers.check_app_identity().state == StepState.PASSED
    assert live_resources.checkers.check_volume().state == StepState.PASSED
    assert live_resources.checkers.check_unity_catalog().state == StepState.PASSED
    assert live_resources.checkers.ensure_sibling_schemas().state == StepState.PASSED
    assert live_resources.checkers.check_lakebase().state == StepState.PASSED
    assert live_resources.checkers.ensure_lakebase_schema().state == StepState.PASSED

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
            getattr(permission.permission_level, "value", permission.permission_level) == "CAN_MANAGE"
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
    report = asyncio.run(app_live_setup.orchestrator.reconcile(setup_user=None))

    assert report.state == SetupState.READY
    for step_id in (
        SetupStepId.VOLUME,
        SetupStepId.SCHEMAS,
        SetupStepId.LAKEBASE,
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
