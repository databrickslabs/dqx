from unittest.mock import patch

from databricks.labs.blueprint.installation import Installation

from databricks.labs.dqx.config import WorkspaceConfig, RunConfig
from databricks.labs.dqx.config_serializer import ConfigSerializer


def test_load_workspace_config_from_user_installation(ws, serverless_installation_ctx, spark):
    serverless_installation_ctx.installation.save(serverless_installation_ctx.config)
    product_name = serverless_installation_ctx.product_info.product_name()
    config = ConfigSerializer(ws).load_config(assume_user=True, product_name=product_name)
    expected_config = serverless_installation_ctx.config
    assert config == expected_config


def test_load_workspace_config_from_global_installation(ws, serverless_installation_ctx):
    product_name = serverless_installation_ctx.product_info.product_name()
    expected_config = serverless_installation_ctx.config

    with patch.object(Installation, '_global_installation', return_value=f"/Shared/{product_name}"):
        serverless_installation_ctx.installation = Installation.assume_global(ws, product_name)
        serverless_installation_ctx.installation.save(serverless_installation_ctx.config)
        config = ConfigSerializer(ws).load_config(assume_user=False, product_name=product_name)
        assert config == expected_config


def test_load_workspace_config_from_custom_folder_installation(ws, installation_ctx_custom_install_folder):
    installation_ctx_custom_install_folder.installation.save(installation_ctx_custom_install_folder.config)

    config = ConfigSerializer(ws).load_config(install_folder=installation_ctx_custom_install_folder.install_folder)
    expected_config = installation_ctx_custom_install_folder.config
    assert config == expected_config


def test_load_run_config_from_user_installation(ws, serverless_installation_ctx):
    serverless_installation_ctx.installation.save(serverless_installation_ctx.config)
    product_name = serverless_installation_ctx.product_info.product_name()

    run_config = ConfigSerializer(ws).load_run_config(
        run_config_name="default", assume_user=True, product_name=product_name
    )
    expected_run_config = serverless_installation_ctx.config.get_run_config("default")

    assert run_config == expected_run_config


def test_load_run_config_from_global_installation(ws, serverless_installation_ctx):
    product_name = serverless_installation_ctx.product_info.product_name()
    expected_run_config = serverless_installation_ctx.config.get_run_config("default")

    with patch.object(Installation, '_global_installation', return_value=f"/Shared/{product_name}"):
        serverless_installation_ctx.installation = Installation.assume_global(ws, product_name)
        serverless_installation_ctx.installation.save(serverless_installation_ctx.config)

        run_config = ConfigSerializer(ws).load_run_config(
            run_config_name="default", assume_user=False, product_name=product_name
        )

        assert run_config == expected_run_config


def test_load_run_config_from_custom_folder_installation(ws, installation_ctx_custom_install_folder):
    installation_ctx_custom_install_folder.installation.save(installation_ctx_custom_install_folder.config)

    run_config = ConfigSerializer(ws).load_run_config(
        run_config_name="default",
        install_folder=installation_ctx_custom_install_folder.install_folder,
    )
    expected_run_config = installation_ctx_custom_install_folder.config.get_run_config("default")

    assert run_config == expected_run_config


def test_save_workspace_config_in_user_installation(ws, installation_ctx, spark):
    installation_ctx.installation.save(installation_ctx.config)

    run_config = RunConfig(name="fake")
    config = WorkspaceConfig(run_configs=[run_config])

    product_name = installation_ctx.product_info.product_name()
    ConfigSerializer(ws).save_config(config=config, assume_user=True, product_name=product_name)

    actual_config = ConfigSerializer(ws).load_config(assume_user=True, product_name=product_name)
    assert actual_config == config


def test_update_workspace_config_in_user_installation(ws, installation_ctx, spark):
    installation_ctx.installation.save(installation_ctx.config)

    config = installation_ctx.config
    run_config = installation_ctx.config.get_run_config("default")
    run_config.checks_location = "fake_location"

    product_name = installation_ctx.product_info.product_name()
    ConfigSerializer(ws).save_config(config=config, assume_user=True, product_name=product_name)

    actual_config = ConfigSerializer(ws).load_config(assume_user=True, product_name=product_name)

    assert actual_config == config
    assert actual_config.get_run_config("default") == run_config


def test_save_workspace_config_in_global_installation(ws, installation_ctx):
    product_name = installation_ctx.product_info.product_name()
    with patch.object(Installation, '_global_installation', return_value=f"/Shared/{product_name}"):
        installation_ctx.installation = Installation.assume_global(ws, product_name)
        installation_ctx.installation.save(installation_ctx.config)

        run_config = RunConfig(name="fake")
        config = WorkspaceConfig(run_configs=[run_config])

        ConfigSerializer(ws).save_config(config=config, assume_user=False, product_name=product_name)

        actual_config = ConfigSerializer(ws).load_config(assume_user=False, product_name=product_name)
        assert actual_config == config


def test_save_run_config_new_in_user_installation(ws, installation_ctx):
    installation_ctx.installation.save(installation_ctx.config)

    run_config = RunConfig(name="fake")
    product_name = installation_ctx.product_info.product_name()
    ConfigSerializer(ws).save_run_config(run_config=run_config, assume_user=True, product_name=product_name)

    actual_run_config = ConfigSerializer(ws).load_run_config(
        run_config_name="fake", assume_user=True, product_name=product_name
    )
    assert actual_run_config == run_config

    actual_default_run_config = ConfigSerializer(ws).load_run_config(
        run_config_name="default", assume_user=True, product_name=product_name
    )
    assert actual_default_run_config == installation_ctx.config.get_run_config("default")


def test_update_run_config_in_user_installation(ws, installation_ctx):
    installation_ctx.installation.save(installation_ctx.config)

    run_config = installation_ctx.config.get_run_config("default")
    run_config.checks_location = "fake_location"
    product_name = installation_ctx.product_info.product_name()
    ConfigSerializer(ws).save_run_config(run_config=run_config, assume_user=True, product_name=product_name)

    actual_run_config = ConfigSerializer(ws).load_run_config(
        run_config_name="default", assume_user=True, product_name=product_name
    )
    assert actual_run_config == installation_ctx.config.get_run_config("default")


def test_save_run_config_in_global_installation(ws, installation_ctx):
    product_name = installation_ctx.product_info.product_name()
    with patch.object(Installation, '_global_installation', return_value=f"/Shared/{product_name}"):
        installation_ctx.installation = Installation.assume_global(ws, product_name)
        installation_ctx.installation.save(installation_ctx.config)

        run_config = RunConfig(name="fake")
        ConfigSerializer(ws).save_run_config(run_config=run_config, assume_user=False, product_name=product_name)

        actual_run_config = ConfigSerializer(ws).load_run_config(
            run_config_name="fake", assume_user=False, product_name=product_name
        )
        assert actual_run_config == run_config

        actual_default_run_config = ConfigSerializer(ws).load_run_config(
            run_config_name="default", assume_user=False, product_name=product_name
        )
        assert actual_default_run_config == installation_ctx.config.get_run_config("default")


def test_save_run_config_in_custom_folder_installation(ws, installation_ctx_custom_install_folder):
    installation_ctx_custom_install_folder.installation.save(installation_ctx_custom_install_folder.config)

    run_config = RunConfig(name="fake")
    ConfigSerializer(ws).save_run_config(
        run_config=run_config, install_folder=installation_ctx_custom_install_folder.install_folder
    )

    actual_run_config = ConfigSerializer(ws).load_run_config(
        run_config_name="fake", install_folder=installation_ctx_custom_install_folder.install_folder
    )
    assert actual_run_config == run_config

    actual_default_run_config = ConfigSerializer(ws).load_run_config(
        run_config_name="default", install_folder=installation_ctx_custom_install_folder.install_folder
    )
    assert actual_default_run_config == installation_ctx_custom_install_folder.config.get_run_config("default")


def test_save_workspace_config_with_extra_fields_in_user_installation(ws, installation_ctx, spark):
    installation_ctx.installation.save(installation_ctx.config)

    run_config = RunConfig(name="fake")
    config = WorkspaceConfig(run_configs=[run_config])
    # extra configs are skipped
    config.extra_field = "extra_value"

    product_name = installation_ctx.product_info.product_name()
    ConfigSerializer(ws).save_config(config=config, assume_user=True, product_name=product_name)

    actual_config = ConfigSerializer(ws).load_config(assume_user=True, product_name=product_name)
    assert actual_config == config
