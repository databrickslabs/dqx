import copy
from databricks.labs.dqx.config import InstallationChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine


def test_e2e_workflow(ws, spark, setup_workflows, expected_quality_checking_output):
    installation_ctx, run_config = setup_workflows()

    installation_ctx.deployed_workflows.run_workflow("e2e", run_config.name)

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
    assert checked_df.count() > 0, "Output table is empty"
    assert checked_df.count() == input_df.count(), "Output table is empty"


def test_e2e_workflow_for_multiple_run_configs(ws, spark, setup_workflows, expected_quality_checking_output):
    installation_ctx, run_config = setup_workflows()

    second_run_config = copy.deepcopy(run_config)
    second_run_config.name = "second"
    second_run_config.checks_location = "second_checks.yml"
    second_run_config.profiler_config.summary_stats_file = "second_profile_summary_stats.yml"
    second_run_config.output_config.location = run_config.output_config.location + "_second"
    installation_ctx.config.run_configs.append(second_run_config)

    # overwrite config in the installation folder
    installation_ctx.installation.save(installation_ctx.config)

    installation_ctx.deployed_workflows.run_workflow("e2e", run_config_name="")

    dq_engine = DQEngine(ws, spark)

    # assert first run config results
    config = InstallationChecksStorageConfig(
        run_config_name=run_config.name,
        assume_user=True,
        product_name=installation_ctx.installation.product(),
    )
    checks = dq_engine.load_checks(config=config)
    assert checks, f"Checks from the {run_config.name} run config were not loaded correctly"

    input_df = spark.table(run_config.input_config.location)
    checked_df = spark.table(run_config.output_config.location)

    # this is sanity check only, we cannot predict the exact output as it depends on the generated rules
    assert checked_df.count() > 0, f"Output table from the {run_config.name} run config is empty"
    assert checked_df.count() == input_df.count(), f"Output table from the {run_config.name} run config is empty"

    # assert second run config results
    config = InstallationChecksStorageConfig(
        run_config_name=second_run_config.name,
        assume_user=True,
        product_name=installation_ctx.installation.product(),
    )
    checks = dq_engine.load_checks(config=config)
    assert checks, f"Checks from the {second_run_config.name} run config were not loaded correctly"

    checked_df = spark.table(second_run_config.output_config.location)

    assert checked_df.count() > 0, f"Output table from the {second_run_config.name} run config is empty"
    assert checked_df.count() == input_df.count(), f"Output table from the {second_run_config.name} run config is empty"


def test_e2e_workflow_serverless(ws, spark, setup_serverless_workflows, expected_quality_checking_output):
    installation_ctx, run_config = setup_serverless_workflows(quarantine=True)

    installation_ctx.deployed_workflows.run_workflow("e2e", run_config.name)

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


def test_e2e_workflow_with_custom_install_folder(
    ws, spark, setup_workflows_with_custom_folder, expected_quality_checking_output
):
    installation_ctx, run_config = setup_workflows_with_custom_folder()

    installation_ctx.deployed_workflows.run_workflow("e2e", run_config.name)

    config = InstallationChecksStorageConfig(
        run_config_name=run_config.name,
        assume_user=True,
        product_name=installation_ctx.installation.product(),
        install_folder=installation_ctx.installation.install_folder(),
    )
    checks = DQEngine(ws, spark).load_checks(config=config)
    assert checks, "Checks were not loaded correctly"

    checked_df = spark.table(run_config.output_config.location)
    input_df = spark.table(run_config.input_config.location)

    # this is sanity check only, we cannot predict the exact output as it depends on the generated rules
    assert checked_df.count() > 0, "Output table is empty"
    assert checked_df.count() == input_df.count(), "Output table is empty"
