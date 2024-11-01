import logging
import pytest
import databricks
from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.blueprint.wheels import ProductInfo
from databricks.labs.dqx.config import WorkspaceConfig
from databricks.labs.dqx.install import WorkspaceInstaller
from databricks.sdk.errors import NotFound

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
    installation_ctx.installation = Installation.assume_global(ws, installation_ctx.product_info.product_name())
    installation_ctx.installation.save(installation_ctx.config)
    assert (
        installation_ctx.workspace_installation.folder
        == f"/Applications/{installation_ctx.product_info.product_name()}"
    )


def test_fresh_user_config_installation(ws, installation_ctx):
    installation_ctx.installation.save(installation_ctx.config)
    assert (
        installation_ctx.workspace_installation.folder
        == f"/Users/{ws.current_user.me().user_name}/.{installation_ctx.product_info.product_name()}"
    )


def test_installation(ws, installation_ctx):
    installation_ctx.workspace_installation.run()
    assert ws.workspace.get_status(installation_ctx.workspace_installation.folder)


def test_uninstallation(ws, installation_ctx):
    installation_ctx.workspace_installation.run()
    installation_ctx.workspace_installation.uninstall()
    with pytest.raises(NotFound):
        ws.workspace.get_status(installation_ctx.workspace_installation.folder)


def test_global_installation_on_existing_global_install(ws, installation_ctx):
    installation_ctx.installation = Installation.assume_global(ws, installation_ctx.product_info.product_name())
    installation_ctx.installation.save(installation_ctx.config)
    assert (
        installation_ctx.workspace_installation.folder
        == f"/Applications/{installation_ctx.product_info.product_name()}"
    )
    installation_ctx.replace(
        extend_prompts={
            r".*Do you want to update the existing installation?.*": 'yes',
        },
    )
    installation_ctx.__dict__.pop("workspace_installer")
    installation_ctx.__dict__.pop("prompts")
    installation_ctx.workspace_installer.configure()


def test_user_installation_on_existing_global_install(ws, new_installation, make_random):
    # existing install at global level
    product_info = ProductInfo.for_testing(WorkspaceConfig)
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
