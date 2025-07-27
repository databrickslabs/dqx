from unittest.mock import patch
import pytest

from databricks.labs.dqx.config import ChecksStorageConfig
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


def test_save_checks_in_workspace_file(ws, spark, installation_ctx):
    installation_ctx.installation.save(installation_ctx.config)
    install_dir = installation_ctx.installation.install_folder()

    dq_engine = DQEngine(ws, spark)
    checks_path = f"{install_dir}/{installation_ctx.config.get_run_config().checks_file}"

    dq_engine.save_checks(TEST_CHECKS, method="workspace_file", config=ChecksStorageConfig(location=checks_path))

    checks = dq_engine.load_checks(method="workspace_file", config=ChecksStorageConfig(location=checks_path))

    assert TEST_CHECKS == checks, "Checks were not saved correctly"


def test_save_checks_in_user_installation_in_file(ws, spark, installation_ctx):
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    dq_engine = DQEngine(ws, spark)
    config = ChecksStorageConfig(
        location="installation", run_config_name="default", assume_user=True, product_name=product_name
    )
    dq_engine.save_checks(TEST_CHECKS, method="installation", config=config)

    checks = dq_engine.load_checks(method="installation", config=config)
    assert TEST_CHECKS == checks, "Checks were not saved correctly"


def test_save_checks_in_global_installation(ws, spark, installation_ctx):
    product_name = installation_ctx.product_info.product_name()
    install_dir = f"/Shared/{product_name}"
    # patch the global installation to existing folder to avoid access permission issues in the workspace
    with patch.object(Installation, '_global_installation', return_value=install_dir):
        installation_ctx.installation = Installation.assume_global(ws, product_name)
        installation_ctx.installation.save(installation_ctx.config)

        dq_engine = DQEngine(ws, spark)

        config = ChecksStorageConfig(
            location="installation", run_config_name="default", assume_user=False, product_name=product_name
        )
        dq_engine.save_checks(TEST_CHECKS, method="installation", config=config)

        checks = dq_engine.load_checks(method="installation", config=config)
        assert TEST_CHECKS == checks, "Checks were not saved correctly"
        assert installation_ctx.workspace_installation.folder == f"/Shared/{product_name}"


def test_save_checks_when_global_installation_missing(ws, spark):
    with pytest.raises(NotInstalled, match="Application not installed: dqx"):
        config = ChecksStorageConfig(location="installation", run_config_name="default", assume_user=False)
        DQEngine(ws, spark).save_checks(TEST_CHECKS, method="installation", config=config)


def test_load_checks_when_user_installation_missing(ws, spark):
    with pytest.raises(NotFound):
        config = ChecksStorageConfig(location="installation", run_config_name="default", assume_user=True)
        DQEngine(ws, spark).save_checks(TEST_CHECKS, method="installation", config=config)
