from unittest.mock import patch

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.wheels import ProductInfo

from databricks.labs.dqx.config import WorkspaceConfig
from databricks.labs.dqx.config_loader import RunConfigLoader


def test_load_run_config_from_user_installation(ws, installation_ctx):
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    run_config = RunConfigLoader(ws).load_run_config(
        run_config_name="default", assume_user=True, product_name=product_name
    )
    expected_run_config = installation_ctx.config.get_run_config("default")

    assert run_config == expected_run_config


def test_load_run_config_from_global_installation(ws, installation_ctx):
    product_name = installation_ctx.product_info.product_name()
    expected_run_config = installation_ctx.config.get_run_config("default")

    with patch.object(Installation, '_global_installation', return_value=f"/Shared/{product_name}"):
        installation_ctx.installation = Installation.assume_global(ws, product_name)
        installation_ctx.installation.save(installation_ctx.config)

        run_config = RunConfigLoader(ws).load_run_config(
            run_config_name="default", assume_user=False, product_name=product_name
        )

        assert run_config == expected_run_config


def test_load_run_config_from_custom_folder_installation(ws, installation_ctx_custom_install_folder):
    installation_ctx_custom_install_folder.installation.save(installation_ctx_custom_install_folder.config)
    product_name = installation_ctx_custom_install_folder.product_info.product_name()

    run_config = RunConfigLoader(ws).load_run_config(
        run_config_name="default",
        assume_user=True,
        product_name=product_name,
        install_folder=installation_ctx_custom_install_folder.install_folder,
    )
    expected_run_config = installation_ctx_custom_install_folder.config.get_run_config("default")

    assert run_config == expected_run_config


def test_get_custom_installation(ws, make_directory):
    product_info = ProductInfo.for_testing(WorkspaceConfig)
    custom_folder = str(make_directory().absolute())

    custom_installation = RunConfigLoader.get_custom_installation(ws, product_info.product_name(), custom_folder)
    custom_installation.install_folder()

    assert custom_installation.install_folder() == custom_folder
    assert ws.workspace.get_status(custom_folder)
