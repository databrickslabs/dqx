from databricks.labs.dqx.config import InstallationChecksStorageConfig


def test_data_quality_workflow_e2e(ws, spark, setup_workflows):
    installation_ctx, run_config = setup_workflows

    installation_ctx.deployed_workflows.run_workflow("quality_checker", run_config.name)

    config = InstallationChecksStorageConfig(
        run_config_name=run_config.name,
        assume_user=True,
        product_name=installation_ctx.installation.product(),
    )
    print(config)


def test_data_quality_workflow_e2e_serverless(ws, spark, setup_serverless_workflows):
    installation_ctx, run_config = setup_serverless_workflows

    installation_ctx.deployed_workflows.run_workflow("quality_checker", run_config.name)

    config = InstallationChecksStorageConfig(
        run_config_name=run_config.name,
        assume_user=True,
        product_name=installation_ctx.installation.product(),
    )
    print(config)
