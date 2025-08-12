from databricks.labs.dqx.config import InstallationChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine


def test_end_to_end_workflow(ws, spark, setup_workflows, expected_quality_checking_output):
    installation_ctx, run_config = setup_workflows()

    installation_ctx.deployed_workflows.run_workflow("end_to_end", run_config.name)

    config = InstallationChecksStorageConfig(
        run_config_name=run_config.name,
        assume_user=True,
        product_name=installation_ctx.installation.product(),
    )
    checks = DQEngine(ws, spark).load_checks(config=config)
    assert checks, "Checks were not loaded correctly"

    checked_df = spark.table(run_config.output_config.location)
    input_df = spark.table(run_config.input_config.location)

    # this is sanity check only, we cannot predict the exact output as it depends on the generated rules
    assert checked_df.count() == input_df.count(), "Output table is empty"


def test_end_to_end_workflow_serverless(ws, spark, setup_serverless_workflows, expected_quality_checking_output):
    installation_ctx, run_config = setup_serverless_workflows(quarantine=True)

    installation_ctx.deployed_workflows.run_workflow("end_to_end", run_config.name)

    config = InstallationChecksStorageConfig(
        run_config_name=run_config.name,
        assume_user=True,
        product_name=installation_ctx.installation.product(),
    )
    checks = DQEngine(ws, spark).load_checks(config=config)
    assert checks, "Checks were not loaded correctly"

    output_df = spark.table(run_config.output_config.location)
    assert output_df.count() > 0, "Output table is empty"

    quarantine_df = spark.table(run_config.quarantine_config.location)
    assert quarantine_df.count() > 0, "Output table is empty"
