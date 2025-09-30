from unittest.mock import patch
import pytest

from databricks.labs.dqx.config import InstallationChecksStorageConfig, WorkspaceFileChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk.errors import NotFound
from databricks.labs.blueprint.installation import Installation, NotInstalled
from databricks.labs.dqx.errors import InvalidCheckError


def test_load_checks_when_checks_file_does_not_exist_in_workspace(ws, installation_ctx, spark):
    installation_ctx.installation.save(installation_ctx.config)
    location = (
        f"{installation_ctx.installation.install_folder()}/{installation_ctx.config.get_run_config().checks_location}"
    )

    with pytest.raises(NotFound, match=f"Checks file {location} missing"):
        DQEngine(ws, spark).load_checks(config=WorkspaceFileChecksStorageConfig(location=location))


def test_load_checks_from_installation_when_checks_file_does_not_exist_in_workspace(ws, installation_ctx, spark):
    installation_ctx.installation.save(installation_ctx.config)
    location = (
        f"{installation_ctx.installation.install_folder()}/{installation_ctx.config.get_run_config().checks_location}"
    )

    with pytest.raises(NotFound, match=f"Checks file {location} missing"):
        config = InstallationChecksStorageConfig(
            run_config_name="default",
            assume_user=True,
            product_name=installation_ctx.installation.product(),
        )
        DQEngine(ws).load_checks(config=config)


def test_load_checks_from_yaml_file(ws, installation_ctx, make_check_file_as_yaml, expected_checks, spark):
    installation_ctx.installation.save(installation_ctx.config)
    install_dir = installation_ctx.installation.install_folder()
    make_check_file_as_yaml(install_dir=install_dir)

    checks = DQEngine(ws, spark).load_checks(
        config=WorkspaceFileChecksStorageConfig(
            location=f"{install_dir}/{installation_ctx.config.get_run_config().checks_location}"
        ),
    )

    assert checks == expected_checks, "Checks were not loaded correctly"


def test_load_checks_from_json_file(ws, installation_ctx, make_check_file_as_json, expected_checks, spark):
    installation_ctx.installation.save(installation_ctx.config)
    install_dir = installation_ctx.installation.install_folder()
    make_check_file_as_json(install_dir=install_dir)

    checks = DQEngine(ws, spark).load_checks(
        config=WorkspaceFileChecksStorageConfig(location=f"{install_dir}/checks.json")
    )

    assert checks == expected_checks, "Checks were not loaded correctly"


def test_load_invalid_checks_from_yaml_file(
    ws, installation_ctx, make_invalid_check_file_as_yaml, expected_checks, spark
):
    installation_ctx.installation.save(installation_ctx.config)
    install_dir = installation_ctx.installation.install_folder()
    workspace_file_path = make_invalid_check_file_as_yaml(install_dir=install_dir)
    with pytest.raises(InvalidCheckError, match=f"Invalid checks in file: {workspace_file_path}"):
        DQEngine(ws, spark).load_checks(
            config=WorkspaceFileChecksStorageConfig(
                location=f"{install_dir}/{installation_ctx.config.get_run_config().checks_location}"
            ),
        )


def test_load_invalid_checks_from_json_file(
    ws, installation_ctx, make_invalid_check_file_as_json, expected_checks, spark
):
    installation_ctx.installation.save(installation_ctx.config)
    install_dir = installation_ctx.installation.install_folder()
    workspace_file_path = make_invalid_check_file_as_json(install_dir=install_dir)
    with pytest.raises(InvalidCheckError, match=f"Invalid checks in file: {workspace_file_path}"):
        DQEngine(ws, spark).load_checks(config=WorkspaceFileChecksStorageConfig(location=f"{install_dir}/checks.json"))


def test_load_checks_from_user_installation(ws, installation_ctx, make_check_file_as_yaml, expected_checks, spark):
    installation_ctx.installation.save(installation_ctx.config)
    make_check_file_as_yaml(install_dir=installation_ctx.installation.install_folder())

    config = InstallationChecksStorageConfig(
        run_config_name="default",
        assume_user=True,
        product_name=installation_ctx.installation.product(),
    )
    checks = DQEngine(ws, spark).load_checks(config=config)

    assert checks == expected_checks, "Checks were not loaded correctly"


def test_load_checks_from_custom_folder_installation(
    ws, installation_ctx_custom_install_folder, make_check_file_as_yaml, expected_checks, spark
):
    installation_ctx_custom_install_folder.installation.save(installation_ctx_custom_install_folder.config)
    make_check_file_as_yaml(install_dir=installation_ctx_custom_install_folder.installation.install_folder())

    config = InstallationChecksStorageConfig(
        run_config_name="default",
        assume_user=True,
        product_name=installation_ctx_custom_install_folder.installation.product(),
        install_folder=installation_ctx_custom_install_folder.installation.install_folder(),
    )
    checks = DQEngine(ws, spark).load_checks(config=config)

    assert checks == expected_checks, "Checks were not loaded correctly"


def test_load_checks_from_absolute_path(ws, installation_ctx, make_check_file_as_yaml, expected_checks, spark):
    checks_location = make_check_file_as_yaml()
    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.checks_location = checks_location
    installation_ctx.installation.save(config)

    config = InstallationChecksStorageConfig(
        run_config_name="default",
        assume_user=True,
        product_name=installation_ctx.installation.product(),
    )
    checks = DQEngine(ws, spark).load_checks(config=config)

    assert checks == expected_checks, "Checks were not loaded correctly"


def test_load_invalid_checks_from_user_installation(
    ws, installation_ctx, make_invalid_check_file_as_yaml, expected_checks, spark
):
    installation_ctx.installation.save(installation_ctx.config)
    workspace_file_path = make_invalid_check_file_as_yaml(install_dir=installation_ctx.installation.install_folder())
    with pytest.raises(InvalidCheckError, match=f"Invalid checks in file: {workspace_file_path}"):
        config = InstallationChecksStorageConfig(
            run_config_name="default",
            assume_user=True,
            product_name=installation_ctx.installation.product(),
        )
        DQEngine(ws).load_checks(config=config)


def test_load_checks_from_global_installation(ws, installation_ctx, make_check_file_as_yaml, spark):
    product_name = installation_ctx.product_info.product_name()
    install_dir = f"/Shared/{product_name}"
    # patch the global installation to existing folder to avoid access permission issues in the workspace
    with patch.object(Installation, '_global_installation', return_value=install_dir):
        installation_ctx.installation = Installation.assume_global(ws, product_name)
        installation_ctx.installation.save(installation_ctx.config)
        make_check_file_as_yaml(install_dir=install_dir)
        config = InstallationChecksStorageConfig(
            run_config_name="default", assume_user=False, product_name=product_name
        )
        checks = DQEngine(ws).load_checks(config=config)
        assert checks, "Checks were not loaded correctly"
        assert installation_ctx.installation_service.install_folder == f"/Shared/{product_name}"


def test_load_checks_when_global_installation_missing(ws, spark):
    with pytest.raises(NotInstalled, match="Application not installed: dqx"):
        config = InstallationChecksStorageConfig(run_config_name="default", assume_user=False)
        DQEngine(ws, spark).load_checks(config=config)


def test_load_checks_when_user_installation_missing(ws, spark):
    with pytest.raises(NotFound):
        config = InstallationChecksStorageConfig(run_config_name="default", assume_user=True)
        DQEngine(ws, spark).load_checks(config=config)
