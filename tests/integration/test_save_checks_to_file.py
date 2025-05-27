from unittest.mock import patch
import pytest
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


def test_save_checks_in_workspace_file(ws, installation_ctx):
    installation_ctx.installation.save(installation_ctx.config)
    install_dir = installation_ctx.installation.install_folder()

    dq_engine = DQEngine(ws)
    checks_path = f"{install_dir}/{installation_ctx.config.get_run_config().checks_file}"

    dq_engine.save_checks_in_workspace_file(TEST_CHECKS, checks_path)

    checks = dq_engine.load_checks_from_workspace_file(checks_path)

    assert TEST_CHECKS == checks, "Checks were not saved correctly"


def test_save_checks_in_user_installation(ws, installation_ctx):
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    dq_engine = DQEngine(ws)
    dq_engine.save_checks_in_installation(
        TEST_CHECKS, run_config_name="default", assume_user=True, product_name=product_name
    )

    checks = dq_engine.load_checks_from_installation(
        run_config_name="default", assume_user=True, product_name=product_name
    )
    assert TEST_CHECKS == checks, "Checks were not saved correctly"


def test_save_checks_in_global_installation(ws, installation_ctx):
    product_name = installation_ctx.product_info.product_name()
    install_dir = f"/Shared/{product_name}"
    # patch the global installation to existing folder to avoid access permission issues in the workspace
    with patch.object(Installation, '_global_installation', return_value=install_dir):
        installation_ctx.installation = Installation.assume_global(ws, product_name)
        installation_ctx.installation.save(installation_ctx.config)

        dq_engine = DQEngine(ws)
        dq_engine.save_checks_in_installation(
            TEST_CHECKS, run_config_name="default", assume_user=False, product_name=product_name
        )

        checks = dq_engine.load_checks_from_installation(
            run_config_name="default", assume_user=False, product_name=product_name
        )
        assert TEST_CHECKS == checks, "Checks were not saved correctly"
        assert installation_ctx.workspace_installation.folder == f"/Shared/{product_name}"


def test_save_checks_when_global_installation_missing(ws):
    with pytest.raises(NotInstalled, match="Application not installed: dqx"):
        DQEngine(ws).save_checks_in_installation(TEST_CHECKS, run_config_name="default", assume_user=False)


def test_load_checks_when_user_installation_missing(ws):
    with pytest.raises(NotFound):
        DQEngine(ws).save_checks_in_installation(TEST_CHECKS, run_config_name="default", assume_user=True)
