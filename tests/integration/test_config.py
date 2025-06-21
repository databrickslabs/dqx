from unittest.mock import patch
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.blueprint.installation import Installation


def test_load_run_config_from_user_installation(ws, installation_ctx, spark):
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    run_config = DQEngine(ws, spark).load_run_config(
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

        run_config = DQEngine(ws).load_run_config(
            run_config_name="default", assume_user=False, product_name=product_name
        )

        assert run_config == expected_run_config
