from databricks.labs.dqx.config import InstallationChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine


def test_data_quality_workflow_e2e(ws, spark, setup_workflows):
    installation_ctx, run_config = setup_workflows

    installation_ctx.deployed_workflows.run_workflow("quality", run_config.name)

    config = InstallationChecksStorageConfig(
        run_config_name=run_config.name,
        assume_user=True,
        product_name=installation_ctx.installation.product(),
    )


def test_data_quality_workflow_e2e_serverless(ws, spark, setup_serverless_workflows):
    installation_ctx, run_config = setup_serverless_workflows

    installation_ctx.deployed_workflows.run_workflow("quality", run_config.name)

    config = InstallationChecksStorageConfig(
        run_config_name=run_config.name,
        assume_user=True,
        product_name=installation_ctx.installation.product(),
    )
    checks = DQEngine(ws, spark).load_checks(config=config)
    assert checks, "Checks were not loaded correctly"

    install_folder = installation_ctx.installation.install_folder()
    status = ws.workspace.get_status(f"{install_folder}/{run_config.profiler_config.summary_stats_file}")
    assert status, f"Profile summary stats file {run_config.profiler_config.summary_stats_file} does not exist."
