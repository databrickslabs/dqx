import pytest
from databricks.labs.dqx.engine import load_checks_from_file
from databricks.labs.blueprint.installation import Installation
from databricks.sdk.errors import NotFound


def test_load_check_from_workspace_file_not_found(ws, installation_ctx):
    installation_ctx.installation.save(installation_ctx.config)
    installation = Installation(ws, "dqx", install_folder=installation_ctx.installation.install_folder())

    with pytest.raises(NotFound, match=f"Checks file {installation_ctx.config.checks_file} missing"):
        load_checks_from_file(installation)


def test_load_check_from_workspace_file_defined_in_config(ws, installation_ctx, make_check_file_as_yaml):
    installation_ctx.installation.save(installation_ctx.config)
    make_check_file_as_yaml(install_dir=installation_ctx.installation.install_folder())
    installation = Installation(ws, "dqx", install_folder=installation_ctx.installation.install_folder())

    checks = load_checks_from_file(installation)

    assert checks, "Checks were not loaded correctly"
