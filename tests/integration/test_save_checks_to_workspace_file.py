import json
from unittest.mock import patch
import pytest
import yaml
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.config import (
    InstallationChecksStorageConfig,
    WorkspaceFileChecksStorageConfig,
)
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk.errors import NotFound
from databricks.labs.blueprint.installation import NotInstalled
from databricks.labs.blueprint.installation import Installation


TEST_CHECKS = [
    {
        "criticality": "error",
        "check": {"function": "is_not_null", "for_each_column": ["col1", "col2"], "arguments": {}},
    }
]


def test_save_checks_in_workspace_file_as_yaml(ws, spark, installation_ctx):
    installation_ctx.installation.save(installation_ctx.config)
    install_dir = installation_ctx.installation.install_folder()

    dq_engine = DQEngine(ws, spark)
    checks_path = f"{install_dir}/{installation_ctx.config.get_run_config().checks_location}"
    dq_engine.save_checks(TEST_CHECKS, config=WorkspaceFileChecksStorageConfig(location=checks_path))
    _verify_workspace_file_is_valid(ws, checks_path, file_format="yaml")

    checks = dq_engine.load_checks(config=WorkspaceFileChecksStorageConfig(location=checks_path))
    assert TEST_CHECKS == checks, "Checks were not saved correctly"


def test_save_checks_in_workspace_file_as_json(ws, spark, installation_ctx):
    installation_ctx.config.run_configs[0].checks_location = "checks.json"
    installation_ctx.installation.save(installation_ctx.config)
    install_dir = installation_ctx.installation.install_folder()

    dq_engine = DQEngine(ws, spark)
    checks_path = f"{install_dir}/{installation_ctx.config.get_run_config().checks_location}"

    dq_engine.save_checks(TEST_CHECKS, config=WorkspaceFileChecksStorageConfig(location=checks_path))
    _verify_workspace_file_is_valid(ws, checks_path, file_format="json")

    checks = dq_engine.load_checks(config=WorkspaceFileChecksStorageConfig(location=checks_path))
    assert TEST_CHECKS == checks, "Checks were not saved correctly"


def test_save_checks_in_user_installation_in_yaml_file(ws, spark, installation_ctx):
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    dq_engine = DQEngine(ws, spark)
    config = InstallationChecksStorageConfig(run_config_name="default", assume_user=True, product_name=product_name)
    dq_engine.save_checks(TEST_CHECKS, config=config)

    install_dir = installation_ctx.installation.install_folder()
    checks_path = f"{install_dir}/{installation_ctx.config.get_run_config().checks_location}"
    _verify_workspace_file_is_valid(ws, checks_path, file_format="yaml")

    checks = dq_engine.load_checks(config=config)
    assert TEST_CHECKS == checks, "Checks were not saved correctly"


def test_save_checks_in_user_installation_in_json_file(ws, spark, installation_ctx):
    installation_ctx.config.run_configs[0].checks_location = "checks.json"
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    dq_engine = DQEngine(ws, spark)
    config = InstallationChecksStorageConfig(run_config_name="default", assume_user=True, product_name=product_name)
    dq_engine.save_checks(TEST_CHECKS, config=config)

    install_dir = installation_ctx.installation.install_folder()
    checks_path = f"{install_dir}/{installation_ctx.config.get_run_config().checks_location}"
    _verify_workspace_file_is_valid(ws, checks_path, file_format="json")

    checks = dq_engine.load_checks(config=config)
    assert TEST_CHECKS == checks, "Checks were not saved correctly"


def test_save_checks_in_global_installation_as_yaml(ws, spark, installation_ctx):
    product_name = installation_ctx.product_info.product_name()
    install_dir = f"/Shared/{product_name}"
    # patch the global installation to existing folder to avoid access permission issues in the workspace
    with patch.object(Installation, '_global_installation', return_value=install_dir):
        installation_ctx.installation = Installation.assume_global(ws, product_name)
        installation_ctx.installation.save(installation_ctx.config)

        dq_engine = DQEngine(ws, spark)
        config = InstallationChecksStorageConfig(
            run_config_name="default", assume_user=False, product_name=product_name
        )
        dq_engine.save_checks(TEST_CHECKS, config=config)

        checks_path = f"{install_dir}/{installation_ctx.config.get_run_config().checks_location}"
        _verify_workspace_file_is_valid(ws, checks_path, file_format="yaml")

        checks = dq_engine.load_checks(config=config)
        assert TEST_CHECKS == checks, "Checks were not saved correctly"
        assert installation_ctx.installation_service.install_folder == f"/Shared/{product_name}"


def test_save_checks_when_global_installation_missing(ws, spark):
    with pytest.raises(NotInstalled, match="Application not installed: dqx"):
        config = InstallationChecksStorageConfig(run_config_name="default", assume_user=False)
        DQEngine(ws, spark).save_checks(TEST_CHECKS, config=config)


def test_save_checks_in_custom_folder_installation_in_yaml_file(ws, spark, installation_ctx_custom_install_folder):
    installation_ctx_custom_install_folder.installation.save(installation_ctx_custom_install_folder.config)
    product_name = installation_ctx_custom_install_folder.product_info.product_name()

    dq_engine = DQEngine(ws, spark)
    config = InstallationChecksStorageConfig(
        run_config_name="default",
        assume_user=True,
        product_name=product_name,
        install_folder=installation_ctx_custom_install_folder.installation.install_folder(),
    )
    dq_engine.save_checks(TEST_CHECKS, config=config)

    install_dir = installation_ctx_custom_install_folder.installation.install_folder()
    checks_path = f"{install_dir}/{installation_ctx_custom_install_folder.config.get_run_config().checks_location}"
    _verify_workspace_file_is_valid(ws, checks_path, file_format="yaml")

    checks = dq_engine.load_checks(config=config)
    assert TEST_CHECKS == checks, "Checks were not saved correctly"


def test_save_checks_when_user_installation_missing(ws, spark):
    with pytest.raises(NotFound):
        config = InstallationChecksStorageConfig(run_config_name="default", assume_user=True)
        DQEngine(ws, spark).save_checks(TEST_CHECKS, config=config)


def _verify_workspace_file_is_valid(ws: WorkspaceClient, location: str, file_format: str = "yaml") -> None:
    try:
        file_info = ws.workspace.get_status(location)
    except Exception as e:
        raise FileNotFoundError(f"Could not find the file {location} in the workspace: {e}") from e

    if not file_info or not file_info.object_type or file_info.object_type.value != "FILE":
        raise FileNotFoundError(f"The path {location} exists but is not a file.")

    try:
        content_bytes = ws.workspace.download(location).read()
        content_str = content_bytes.decode("utf-8")
    except Exception as e:
        raise FileNotFoundError(f"Failed to read the file {location} from workspace: {e}") from e

    if not content_str.strip():
        raise ValueError(f"The file {location} is empty, expected valid JSON or YAML.")

    if file_format == "json":
        json.loads(content_str)
    yaml.safe_load(content_str)
