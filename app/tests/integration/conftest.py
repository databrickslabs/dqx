"""Factory-managed live fixtures for Studio setup verification."""

from collections.abc import Callable, Generator
from dataclasses import dataclass, replace
from datetime import timedelta
import os
from pathlib import Path
import shutil

import pytest
from databricks.labs.pytester.fixtures.baseline import factory
from databricks.labs.lsql.backends import StatementExecutionBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.common import lro
from databricks.sdk.errors import InvalidParameterValue, NotFound, PermissionDenied
from databricks.sdk.service.catalog import PermissionsChange, Privilege, VolumeType
from databricks.sdk.service.compute import ClusterSpec
from databricks.sdk.service.jobs import JobRunAs, NotebookTask, Task
from databricks.sdk.service.postgres import Project, ProjectDefaultEndpointSettings, ProjectSpec

from databricks_labs_dqx_app.backend.migrations import MigrationRunner
from databricks_labs_dqx_app.backend.migrations.postgres import PgMigrationRunner
from databricks_labs_dqx_app.backend.pg_executor import PgExecutor, build_pg_executor_from_connection
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.services.compute_service import ComputeService
from databricks_labs_dqx_app.backend.setup.checks import ResourceCheckers
from databricks_labs_dqx_app.backend.setup.job_manager import TaskRunnerJobManager
from databricks_labs_dqx_app.backend.setup.models import StepState
from databricks_labs_dqx_app.backend.setup.orchestrator import SetupOrchestrator
from databricks_labs_dqx_app.backend.setup.resources import ActiveResources, LakebaseConnection, parse_volume_path
from databricks_labs_dqx_app.backend.setup.runtime import SetupRuntime
from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
from databricks_labs_dqx_app.backend.startup import publish_wheels_to_volume

APP_DIR = Path(__file__).resolve().parents[2]
_PROJECT_BRANCH = "dqx"
_PROJECT_ENDPOINT = "primary"


@dataclass(frozen=True)
class LakebaseProject:
    """A factory-managed Lakebase project and its primary endpoint."""

    project_id: str

    @property
    def endpoint(self) -> str:
        """Return the project's primary read-write endpoint path."""
        return f"projects/{self.project_id}/branches/{_PROJECT_BRANCH}/endpoints/{_PROJECT_ENDPOINT}"


@dataclass(frozen=True)
class SetupVolume:
    """A factory-managed volume and its Unity Catalog location."""

    catalog: str
    schema: str
    volume: str

    @property
    def path(self) -> str:
        """Return the volume filesystem path consumed by Studio setup."""
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"


@dataclass(frozen=True)
class LiveResources:
    """Live setup resources used to verify real capability checks and wheel publishing."""

    workspace: WorkspaceClient
    volume: SetupVolume
    resources: ActiveResources
    checkers: ResourceCheckers
    pg: PgExecutor
    wheel: Path


@dataclass(frozen=True)
class LiveJob:
    """Factory-managed task-runner job with a preconfigured external identity."""

    workspace: WorkspaceClient
    manager: TaskRunnerJobManager
    admin_user: str
    job_id: int


@dataclass(frozen=True)
class AppLiveSetup:
    """A real reconcile harness authenticated as the configured app service principal."""

    setup_workspace: WorkspaceClient
    app_workspace: WorkspaceClient
    volume: SetupVolume
    resources: ActiveResources
    pg: PgExecutor
    wheel: Path
    orchestrator: SetupOrchestrator


@pytest.fixture(scope="session")
def databricks_profile() -> str:
    """Require an explicit Databricks CLI profile for this opt-in suite."""
    import os

    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "").strip()
    if not profile:
        pytest.skip("Set DATABRICKS_CONFIG_PROFILE via make app-integration PROFILE=<profile>.")
    return profile


@pytest.fixture(scope="session")
def ws(databricks_profile: str) -> WorkspaceClient:
    """Provide the pytester factories with the requested authenticated workspace."""
    return WorkspaceClient(profile=databricks_profile)


@pytest.fixture
def app_workspace(databricks_profile: str, ws: WorkspaceClient) -> WorkspaceClient:
    """Require a distinct OAuth machine-to-machine app service-principal profile."""
    app_profile = os.environ.get("DQX_TEST_APP_PROFILE", "").strip()
    if not app_profile:
        pytest.skip("Set DQX_TEST_APP_PROFILE to a Databricks App service-principal OAuth profile.")
    if app_profile == databricks_profile:
        pytest.skip("DQX_TEST_APP_PROFILE must differ from DATABRICKS_CONFIG_PROFILE.")

    app_client = WorkspaceClient(profile=app_profile)
    auth_type = (app_client.config.auth_type or "").strip()
    if auth_type != "oauth-m2m":
        pytest.skip("DQX_TEST_APP_PROFILE must authenticate with OAuth machine-to-machine credentials.")

    setup_identity = _workspace_identity(ws)
    app_identity = _workspace_identity(app_client)
    if app_identity == setup_identity:
        pytest.skip(
            "DQX_TEST_APP_PROFILE resolved to the setup profile identity, not a distinct app service principal."
        )
    return app_client


@pytest.fixture
def live_warehouse(make_warehouse: Callable[..., object]) -> object:
    """Create one factory-managed warehouse for test resources and Studio setup."""
    return make_warehouse(enable_serverless_compute=True)


@pytest.fixture
def sql_backend(ws: WorkspaceClient, live_warehouse: object) -> StatementExecutionBackend:
    """Direct pytester catalog factories to the test's managed SQL warehouse."""
    warehouse_id = getattr(getattr(live_warehouse, "response", None), "id", None)
    if not isinstance(warehouse_id, str) or not warehouse_id:
        raise RuntimeError("The test warehouse did not return an ID.")
    return StatementExecutionBackend(ws, warehouse_id)


@pytest.fixture
def make_setup_volume(
    ws: WorkspaceClient,
    make_catalog: Callable[[], object],
    make_random: Callable[[int], str],
) -> Generator[Callable[[], SetupVolume], None, None]:
    """Create a volume without the SQL retry semantics in pytester's general fixture."""

    def create() -> SetupVolume:
        catalog = make_catalog()
        catalog_name = getattr(catalog, "name", None)
        if not isinstance(catalog_name, str) or not catalog_name:
            raise RuntimeError("The test catalog did not return a name.")
        principal = (ws.current_user.me().user_name or "").strip()
        if not principal:
            raise RuntimeError("The test profile did not return a workspace user name.")
        schema = f"dqx_{make_random(10).lower()}"
        volume = f"wheels_{make_random(10).lower()}"
        ws.schemas.create(schema, catalog_name)
        ws.volumes.create(catalog_name, schema, volume, VolumeType.MANAGED)
        _grant_setup_privileges(ws, catalog_name, schema, volume, principal)
        return SetupVolume(catalog=catalog_name, schema=schema, volume=volume)

    def delete(volume: SetupVolume) -> None:
        try:
            ws.volumes.delete(f"{volume.catalog}.{volume.schema}.{volume.volume}")
        except NotFound:
            pass
        try:
            ws.schemas.delete(f"{volume.catalog}.{volume.schema}", force=True)
        except NotFound:
            pass

    yield from factory("setup volume", create, delete)


@pytest.fixture
def make_lakebase_project(
    ws: WorkspaceClient,
    make_random: Callable[[int], str],
) -> Generator[Callable[[], LakebaseProject], None, None]:
    """Create and purge an isolated Lakebase project through a pytester factory."""

    def create() -> LakebaseProject:
        project_id = f"dqx-it-{make_random(12).lower()}"
        ws.postgres.create_project(
            Project(
                spec=ProjectSpec(
                    display_name=f"DQX integration {project_id}",
                    pg_version=16,
                    default_branch=f"projects/{project_id}/branches/{_PROJECT_BRANCH}",
                    default_endpoint_settings=ProjectDefaultEndpointSettings(
                        autoscaling_limit_min_cu=0.5,
                        autoscaling_limit_max_cu=0.5,
                    ),
                )
            ),
            project_id,
        ).wait(lro.LroOptions(timeout=timedelta(minutes=20)))
        return LakebaseProject(project_id=project_id)

    def delete(project: LakebaseProject) -> None:
        try:
            ws.postgres.delete_project(f"projects/{project.project_id}", purge=True).wait(
                lro.LroOptions(timeout=timedelta(minutes=20))
            )
        except NotFound:
            pass

    yield from factory("Lakebase project", create, delete)


@pytest.fixture
def staged_task_runner_wheel() -> Generator[Path, None, None]:
    """Stage the committed Marketplace runner wheel at the production build location."""
    wheels = sorted((APP_DIR / "marketplace" / "tasks").glob("databricks_labs_dqx_task_runner-*.whl"))
    if len(wheels) != 1:
        raise RuntimeError("Expected exactly one committed task-runner wheel for live setup verification.")
    source = wheels[0]
    destination = APP_DIR / ".build" / "tasks" / source.name
    if destination.exists():
        raise RuntimeError(f"Refusing to replace an existing build wheel at {destination}.")
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    try:
        yield destination
    finally:
        destination.unlink(missing_ok=True)


@pytest.fixture
def live_resources(
    ws: WorkspaceClient,
    make_lakebase_project: Callable[[], LakebaseProject],
    make_random: Callable[[int], str],
    make_setup_volume: Callable[[], SetupVolume],
    live_warehouse: object,
    staged_task_runner_wheel: Path,
) -> Generator[LiveResources, None, None]:
    """Build isolated resources for production capability and wheel-publisher collaborators."""
    warehouse_id = getattr(getattr(live_warehouse, "response", None), "id", None)
    if not isinstance(warehouse_id, str) or not warehouse_id:
        raise RuntimeError("The test warehouse did not return an ID.")

    volume = make_setup_volume()
    bound_volume = parse_volume_path(volume.path)

    admin_user = (ws.current_user.me().user_name or "").strip()
    if not admin_user:
        raise RuntimeError("The test profile did not return a workspace user name.")
    lakebase_project = make_lakebase_project()
    tmp_schema = f"dqx_tmp_{make_random(10).lower()}"
    genie_schema = f"dqx_genie_{make_random(10).lower()}"
    resources = ActiveResources(
        volume=bound_volume,
        lakebase=LakebaseConnection(
            endpoint=lakebase_project.endpoint,
            host=None,
            port=5432,
            database="databricks_postgres",
            username=None,
            password=None,
            schema=f"dqx_{make_random(10).lower()}",
        ),
        warehouse_id=warehouse_id,
        job_id=None,
        tmp_schema=tmp_schema,
        genie_schema=genie_schema,
    )
    sql = SqlExecutor(ws=ws, warehouse_id=warehouse_id, catalog=bound_volume.catalog, schema=bound_volume.schema)
    pg = build_pg_executor_from_connection(ws, resources.lakebase)
    app_settings = AppSettingsService(pg)
    checkers = ResourceCheckers(
        resources=resources,
        workspace=ws,
        sql=sql,
        pg=pg,
        compute=ComputeService(sp_ws=ws, app_settings=app_settings),
    )
    try:
        yield LiveResources(
            workspace=ws,
            volume=volume,
            resources=resources,
            checkers=checkers,
            pg=pg,
            wheel=staged_task_runner_wheel,
        )
    finally:
        pg.close()


@pytest.fixture
def make_preconfigured_job(
    ws: WorkspaceClient,
    make_notebook: Callable[..., object],
    make_random: Callable[[int], str],
) -> Generator[Callable[..., int], None, None]:
    """Create a factory-cleaned job with its run-as identity set at creation time."""

    def create(*, run_as: JobRunAs) -> int:
        notebook_path = str(make_notebook(content="print('DQX setup integration placeholder')"))
        task = Task(
            task_key="placeholder",
            notebook_task=NotebookTask(notebook_path=notebook_path),
            new_cluster=ClusterSpec(
                num_workers=1,
                node_type_id=ws.clusters.select_node_type(local_disk=True, min_memory_gb=16),
                spark_version=ws.clusters.select_spark_version(latest=True),
            ),
        )
        response = ws.jobs.create(
            name=f"dqx-setup-{make_random(10).lower()}",
            tasks=[task],
            run_as=run_as,
        )
        job_id = getattr(response, "job_id", None)
        if not isinstance(job_id, int):
            raise RuntimeError("The test job did not return an ID.")
        return job_id

    yield from factory("preconfigured task-runner job", create, lambda job_id: ws.jobs.delete(job_id))


@pytest.fixture
def live_job(
    ws: WorkspaceClient,
    make_preconfigured_job: Callable[..., int],
) -> LiveJob:
    """Provide a real job with a preconfigured user run-as identity for preservation coverage."""
    admin_user = (ws.current_user.me().user_name or "").strip()
    if not admin_user:
        raise RuntimeError("The test profile did not return a workspace user name.")
    job_id = make_preconfigured_job(run_as=JobRunAs(user_name=admin_user))
    return LiveJob(
        workspace=ws,
        manager=TaskRunnerJobManager(workspace=ws, setup_user=admin_user),
        admin_user=admin_user,
        job_id=job_id,
    )


@pytest.fixture
def app_live_setup(
    app_workspace: WorkspaceClient,
    live_resources: LiveResources,
    make_preconfigured_job: Callable[..., int],
) -> Generator[AppLiveSetup, None, None]:
    """Compose production setup collaborators with a real app-SP profile and runner job."""
    runner_principal = os.environ.get("DQX_TEST_RUNNER_SERVICE_PRINCIPAL", "").strip()
    if not runner_principal:
        pytest.skip("Set DQX_TEST_RUNNER_SERVICE_PRINCIPAL to a usable external runner service principal.")
    try:
        job_id = make_preconfigured_job(run_as=JobRunAs(service_principal_name=runner_principal))
    except (InvalidParameterValue, PermissionDenied) as err:
        pytest.skip(f"PROFILE cannot create a factory job with the external runner service principal: {err}")

    resources = replace(live_resources.resources, job_id=str(job_id))
    app_identity = _workspace_identity(app_workspace)
    _grant_setup_privileges(
        live_resources.workspace,
        live_resources.volume.catalog,
        live_resources.volume.schema,
        live_resources.volume.volume,
        app_identity,
    )
    sql = SqlExecutor(
        ws=app_workspace,
        warehouse_id=resources.warehouse_id,
        catalog=resources.volume.catalog,
        schema=resources.volume.schema,
    )
    pg = build_pg_executor_from_connection(app_workspace, resources.lakebase)
    app_settings = AppSettingsService(pg)
    checkers = ResourceCheckers(
        resources=resources,
        workspace=app_workspace,
        sql=sql,
        pg=pg,
        compute=ComputeService(sp_ws=app_workspace, app_settings=app_settings),
    )
    warehouse = checkers.check_warehouse()
    if warehouse.state != StepState.PASSED:
        pg.close()
        pytest.skip("DQX_TEST_APP_PROFILE must have CAN_USE on the factory SQL warehouse.")
    try:
        app_workspace.jobs.get(job_id)
    except PermissionDenied as err:
        pg.close()
        pytest.skip(f"DQX_TEST_APP_PROFILE cannot manage the factory task-runner job: {err}")

    async def publish_wheels() -> list[str]:
        return await publish_wheels_to_volume(app_workspace, resources.volume.path)

    class NoBackgroundActivation:
        """Use the production activation boundary without starting persistent services in a test."""

        async def activate(self) -> None:
            """Acknowledge activation after all real setup work has completed."""

        async def start_background(self) -> None:
            """Avoid long-running scheduler and AI tasks in the live test process."""

    try:
        yield AppLiveSetup(
            setup_workspace=live_resources.workspace,
            app_workspace=app_workspace,
            volume=live_resources.volume,
            resources=resources,
            pg=pg,
            wheel=live_resources.wheel,
            orchestrator=SetupOrchestrator(
                runtime=SetupRuntime(),
                resources=resources,
                checkers=checkers,
                jobs=TaskRunnerJobManager(app_workspace),
                pg_migrations=PgMigrationRunner(pg),
                delta_migrations=MigrationRunner(sql),
                app_settings=app_settings,
                publish_wheels=publish_wheels,
                activation=NoBackgroundActivation(),
                app_sp_id=app_identity,
            ),
        )
    finally:
        pg.close()


def _grant_setup_privileges(
    workspace: WorkspaceClient,
    catalog: str,
    schema: str,
    volume: str,
    principal: str,
) -> None:
    """Grant the test principal the exact Unity Catalog privileges setup verifies."""
    workspace.grants.update(
        "CATALOG",
        catalog,
        changes=[PermissionsChange(principal=principal, add=[Privilege.USE_CATALOG, Privilege.CREATE_SCHEMA])],
    )
    workspace.grants.update(
        "SCHEMA",
        f"{catalog}.{schema}",
        changes=[PermissionsChange(principal=principal, add=[Privilege.USE_SCHEMA, Privilege.CREATE_TABLE])],
    )
    workspace.grants.update(
        "VOLUME",
        f"{catalog}.{schema}.{volume}",
        changes=[PermissionsChange(principal=principal, add=[Privilege.READ_VOLUME, Privilege.WRITE_VOLUME])],
    )


def _workspace_identity(workspace: WorkspaceClient) -> str:
    """Return the authenticated identity needed for Databricks permission checks."""
    identity = workspace.current_user.me()
    value = (identity.user_name or identity.id or "").strip()
    if not value:
        raise RuntimeError("The authenticated profile did not return an identity.")
    return value
