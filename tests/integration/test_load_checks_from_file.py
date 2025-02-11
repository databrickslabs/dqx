from unittest.mock import patch
import pytest
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk.errors import NotFound
from databricks.labs.blueprint.installation import NotInstalled
from databricks.labs.blueprint.installation import Installation


def test_load_checks_when_checks_file_does_not_exist_in_workspace(ws, installation_ctx):
    installation_ctx.installation.save(installation_ctx.config)
    with pytest.raises(NotFound, match="Checks file checks.yml missing"):
        DQEngine(ws).load_checks_from_workspace_file(
            workspace_path=f"{installation_ctx.installation.install_folder()}/"
            f"{installation_ctx.config.get_run_config().checks_file}"
        )


def test_load_checks_from_installation_when_checks_file_does_not_exist_in_workspace(ws, installation_ctx):
    installation_ctx.installation.save(installation_ctx.config)
    with pytest.raises(NotFound, match="Checks file checks.yml missing"):
        DQEngine(ws).load_checks_from_installation(
            run_config_name="default", assume_user=True, product_name=installation_ctx.installation.product()
        )


def test_load_checks_from_yml_file(ws, installation_ctx, make_check_file_as_yml, expected_checks):
    installation_ctx.installation.save(installation_ctx.config)
    install_dir = installation_ctx.installation.install_folder()
    make_check_file_as_yml(install_dir=install_dir)

    checks = DQEngine(ws).load_checks_from_workspace_file(
        workspace_path=f"{install_dir}/{installation_ctx.config.get_run_config().checks_file}"
    )

    assert checks == expected_checks, "Checks were not loaded correctly"


def test_load_checks_from_json_file(ws, installation_ctx, make_check_file_as_json, expected_checks):
    installation_ctx.installation.save(installation_ctx.config)
    install_dir = installation_ctx.installation.install_folder()
    make_check_file_as_json(install_dir=install_dir)

    checks = DQEngine(ws).load_checks_from_workspace_file(workspace_path=f"{install_dir}/checks.json")

    assert checks == expected_checks, "Checks were not loaded correctly"


def test_load_invalid_checks_from_yml_file(ws, installation_ctx, make_invalid_check_file_as_yml, expected_checks):
    installation_ctx.installation.save(installation_ctx.config)
    install_dir = installation_ctx.installation.install_folder()
    workspace_file_path = make_invalid_check_file_as_yml(install_dir=install_dir)
    with pytest.raises(ValueError, match=f"Invalid or no checks in workspace file: {workspace_file_path}"):
        DQEngine(ws).load_checks_from_workspace_file(
            workspace_path=f"{install_dir}/{installation_ctx.config.get_run_config().checks_file}"
        )


def test_load_invalid_checks_from_json_file(ws, installation_ctx, make_invalid_check_file_as_json, expected_checks):
    installation_ctx.installation.save(installation_ctx.config)
    install_dir = installation_ctx.installation.install_folder()
    workspace_file_path = make_invalid_check_file_as_json(install_dir=install_dir)
    with pytest.raises(ValueError, match=f"Invalid or no checks in workspace file: {workspace_file_path}"):
        DQEngine(ws).load_checks_from_workspace_file(workspace_path=f"{install_dir}/checks.json")


def test_load_checks_from_user_installation(ws, installation_ctx, make_check_file_as_yml, expected_checks):
    installation_ctx.installation.save(installation_ctx.config)
    make_check_file_as_yml(install_dir=installation_ctx.installation.install_folder())

    checks = DQEngine(ws).load_checks_from_installation(
        run_config_name="default", assume_user=True, product_name=installation_ctx.installation.product()
    )
    assert checks == expected_checks, "Checks were not loaded correctly"


def test_load_invalid_checks_from_user_installation(
    ws, installation_ctx, make_invalid_check_file_as_yml, expected_checks
):
    installation_ctx.installation.save(installation_ctx.config)
    workspace_file_path = make_invalid_check_file_as_yml(install_dir=installation_ctx.installation.install_folder())
    with pytest.raises(ValueError, match=f"Invalid or no checks in workspace file: {workspace_file_path}"):
        DQEngine(ws).load_checks_from_installation(
            run_config_name="default", assume_user=True, product_name=installation_ctx.installation.product()
        )


def test_load_checks_from_global_installation(ws, installation_ctx, make_check_file_as_yml):
    product_name = installation_ctx.product_info.product_name()
    install_dir = f"/Shared/{product_name}"
    # patch the global installation to existing folder to avoid access permission issues in the workspace
    with patch.object(Installation, '_global_installation', return_value=install_dir):
        installation_ctx.installation = Installation.assume_global(ws, product_name)
        installation_ctx.installation.save(installation_ctx.config)
        make_check_file_as_yml(install_dir=install_dir)
        checks = DQEngine(ws).load_checks_from_installation(
            run_config_name="default", assume_user=False, product_name=product_name
        )
        assert checks, "Checks were not loaded correctly"
        assert installation_ctx.workspace_installation.folder == f"/Shared/{product_name}"


def test_load_checks_when_global_installation_missing(ws):
    with pytest.raises(NotInstalled, match="Application not installed: dqx"):
        DQEngine(ws).load_checks_from_installation(run_config_name="default", assume_user=False)


def test_load_checks_when_user_installation_missing(ws):
    with pytest.raises(NotFound):
        DQEngine(ws).load_checks_from_installation(run_config_name="default", assume_user=True)
