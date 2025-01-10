import logging
from unittest import skip
from unittest.mock import patch, create_autospec
import pytest

from integration.conftest import contains_expected_workflows
import databricks
from databricks.labs.dqx.installer.workflows_installer import WorkflowsDeployment
from databricks.labs.blueprint.installation import Installation, MockInstallation
from databricks.labs.blueprint.wheels import WheelsV2
from databricks.labs.dqx.installer.workflow_task import Task
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.blueprint.wheels import ProductInfo
from databricks.labs.dqx.config import WorkspaceConfig, RunConfig
from databricks.labs.dqx.installer.install import WorkspaceInstaller
from databricks.sdk.errors import NotFound
from databricks.sdk.service.jobs import CreateResponse
from databricks.sdk import WorkspaceClient


logger = logging.getLogger(__name__)


@pytest.fixture
def new_installation(ws, env_or_skip, make_random):
    cleanup = []

    def factory(
        installation: Installation | None = None,
        product_info: ProductInfo | None = None,
        environ: dict[str, str] | None = None,
        extend_prompts: dict[str, str] | None = None,
    ):
        logger.debug("Creating new installation...")
        if not product_info:
            product_info = ProductInfo.for_testing(WorkspaceConfig)
        if not environ:
            environ = {}

        prompts = MockPrompts(
            {
                r'Provide location for the input data *': '/',
                r'Do you want to uninstall DQX.*': 'yes',
                r".*": "",
            }
            | (extend_prompts or {})
        )

        if not installation:
            installation = Installation(ws, product_info.product_name())
        installer = WorkspaceInstaller(ws, environ).replace(
            installation=installation,
            product_info=product_info,
            prompts=prompts,
        )
        workspace_config = installer.configure()
        installation = product_info.current_installation(ws)
        installation.save(workspace_config)
        cleanup.append(installation)
        return installation

    yield factory

    for pending in cleanup:
        pending.remove()


def test_fresh_global_config_installation(ws, installation_ctx):
    product_name = installation_ctx.product_info.product_name()
    # patch the global installation to existing folder to avoid access permission issues in the workspace
    with patch.object(Installation, '_global_installation', return_value=f"/Shared/{product_name}"):
        installation_ctx.installation = Installation.assume_global(ws, product_name)
        installation_ctx.installation.save(installation_ctx.config)
        assert installation_ctx.workspace_installation.folder == f"/Shared/{product_name}"
        assert installation_ctx.workspace_installer.installation
        assert installation_ctx.workspace_installation.current(ws)
        assert installation_ctx.workspace_installation.config == installation_ctx.config


def test_fresh_user_config_installation(ws, installation_ctx):
    installation_ctx.installation.save(installation_ctx.config)
    assert (
        installation_ctx.workspace_installation.folder
        == f"/Users/{ws.current_user.me().user_name}/.{installation_ctx.product_info.product_name()}"
    )


def test_complete_installation(ws, installation_ctx):
    installation_ctx.workspace_installer.run(installation_ctx.config)
    assert installation_ctx.workspace_installer.installation
    assert installation_ctx.deployed_workflows.latest_job_status()


def test_installation(ws, installation_ctx):
    installation_ctx.workspace_installation.run()
    workflows = installation_ctx.deployed_workflows.latest_job_status()
    expected_workflows_state = [{'workflow': 'profiler', 'state': 'UNKNOWN', 'started': '<never run>'}]

    assert ws.workspace.get_status(installation_ctx.workspace_installation.folder)
    for state in expected_workflows_state:
        assert contains_expected_workflows(workflows, state)


def test_uninstallation(ws, installation_ctx):
    installation_ctx.workspace_installation.run()
    installation_ctx.workspace_installation.uninstall()
    with pytest.raises(NotFound):
        ws.workspace.get_status(installation_ctx.workspace_installation.folder)


def test_global_installation_on_existing_global_install(ws, installation_ctx):
    product_name = installation_ctx.product_info.product_name()
    # patch the global installation to existing folder to avoid access permission issues in the workspace
    with patch.object(Installation, '_global_installation', return_value=f"/Shared/{product_name}"):
        installation_ctx.installation = Installation.assume_global(ws, product_name)
        installation_ctx.installation.save(installation_ctx.config)
        assert installation_ctx.workspace_installation.folder == f"/Shared/{product_name}"
        installation_ctx.replace(
            extend_prompts={
                r".*Do you want to update the existing installation?.*": 'yes',
            },
        )
        installation_ctx.__dict__.pop("workspace_installer")
        installation_ctx.__dict__.pop("prompts")

        config = installation_ctx.workspace_installer.configure()
        config.connect = None
        assert config == WorkspaceConfig(
            log_level='INFO',
            run_configs=[
                RunConfig(
                    input_location="skipped",
                    input_format="delta",
                    output_table="skipped",
                    quarantine_table="skipped",
                    checks_file="checks.yml",
                    profile_summary_stats_file="profile_summary_stats.yml",
                )
            ],
        )


def test_user_installation_on_existing_global_install(ws, new_installation, make_random):
    # existing install at global level
    product_info = ProductInfo.for_testing(WorkspaceConfig)
    # patch the global installation to existing folder to avoid access permission issues in the workspace
    with patch.object(Installation, '_global_installation', return_value=f"/Shared/{product_info.product_name()}"):
        new_installation(
            product_info=product_info,
            installation=Installation.assume_global(ws, product_info.product_name()),
        )

        # warning to be thrown by installer if override environment variable present but no confirmation
        with pytest.raises(RuntimeWarning, match="DQX is already installed, but no confirmation"):
            new_installation(
                product_info=product_info,
                installation=Installation.assume_global(ws, product_info.product_name()),
                environ={'DQX_FORCE_INSTALL': 'user'},
                extend_prompts={
                    r".*DQX is already installed on this workspace.*": 'no',
                    r".*Do you want to update the existing installation?.*": 'yes',
                },
            )

        # successful override with confirmation
        reinstall_user_force = new_installation(
            product_info=product_info,
            installation=Installation.assume_global(ws, product_info.product_name()),
            environ={'DQX_FORCE_INSTALL': 'user'},
            extend_prompts={
                r".*DQX is already installed on this workspace.*": 'yes',
                r".*Do you want to update the existing installation?.*": 'yes',
            },
        )
        assert (
            reinstall_user_force.install_folder()
            == f"/Users/{ws.current_user.me().user_name}/.{product_info.product_name()}"
        )


def test_global_installation_on_existing_user_install(ws, new_installation):
    # existing installation at user level
    product_info = ProductInfo.for_testing(WorkspaceConfig)
    # patch the global installation to existing folder to avoid access permission issues in the workspace
    with patch.object(Installation, '_global_installation', return_value=f"/Shared/{product_info.product_name()}"):
        existing_user_installation = new_installation(
            product_info=product_info, installation=Installation.assume_user_home(ws, product_info.product_name())
        )
        assert (
            existing_user_installation.install_folder()
            == f"/Users/{ws.current_user.me().user_name}/.{product_info.product_name()}"
        )

        # warning to be thrown by installer if override environment variable present but no confirmation
        with pytest.raises(RuntimeWarning, match="DQX is already installed, but no confirmation"):
            new_installation(
                product_info=product_info,
                installation=Installation.assume_user_home(ws, product_info.product_name()),
                environ={'DQX_FORCE_INSTALL': 'global'},
                extend_prompts={
                    r".*DQX is already installed on this workspace.*": 'no',
                    r".*Do you want to update the existing installation?.*": 'yes',
                },
            )

        with pytest.raises(databricks.sdk.errors.NotImplemented, match="Migration needed. Not implemented yet."):
            new_installation(
                product_info=product_info,
                installation=Installation.assume_user_home(ws, product_info.product_name()),
                environ={'DQX_FORCE_INSTALL': 'global'},
                extend_prompts={
                    r".*DQX is already installed on this workspace.*": 'yes',
                    r".*Do you want to update the existing installation?.*": 'yes',
                },
            )


def test_compare_remote_local_install_versions(ws, installation_ctx):
    installation_ctx.workspace_installation.run()
    with pytest.raises(
        RuntimeWarning,
        match="DQX workspace remote and local install versions are same and no override is requested. Exiting...",
    ):
        installation_ctx.workspace_installer.configure()

    installation_ctx.replace(
        extend_prompts={
            r".*Do you want to update the existing installation?.*": 'yes',
        },
    )
    installation_ctx.__dict__.pop("workspace_installer")
    installation_ctx.__dict__.pop("prompts")
    installation_ctx.workspace_installer.configure()


def test_installation_stores_install_state_keys(ws, installation_ctx):
    """The installation should store the keys in the installation state."""
    expected_keys = ["jobs"]
    installation_ctx.workspace_installation.run()
    # Refresh the installation state since the installation context uses `@cached_property`
    install_state = InstallState.from_installation(installation_ctx.installation)
    for key in expected_keys:
        assert hasattr(install_state, key), f"Missing key in install state: {key}"
        assert getattr(install_state, key), f"Installation state is empty: {key}"


def side_effect_remove_after_in_tags_settings(**settings) -> CreateResponse:
    tags = settings.get("tags", {})
    _ = tags["RemoveAfter"]  # KeyError side effect
    return CreateResponse(job_id=1)


def test_workflows_deployment_creates_jobs_with_remove_after_tag():
    ws = create_autospec(WorkspaceClient)
    ws.jobs.create.side_effect = side_effect_remove_after_in_tags_settings
    config = WorkspaceConfig([RunConfig()])
    mock_installation = MockInstallation()
    install_state = InstallState.from_installation(mock_installation)
    wheels = create_autospec(WheelsV2)
    product_info = ProductInfo.for_testing(WorkspaceConfig)
    tasks = [Task("workflow", "task", "docs", lambda *_: None)]
    workflows_deployment = WorkflowsDeployment(
        config,
        config.get_run_config().name,
        mock_installation,
        install_state,
        ws,
        wheels,
        product_info,
        tasks=tasks,
    )
    try:
        workflows_deployment.create_jobs()
    except KeyError as e:
        assert False, f"RemoveAfter tag not present: {e}"
    wheels.assert_not_called()
