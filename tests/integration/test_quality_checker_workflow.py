from chispa.dataframe_comparer import assert_df_equality  # type: ignore


def test_data_quality_workflow_e2e(ws, spark, setup_workflows_with_checks, expected_output):
    installation_ctx, run_config = setup_workflows_with_checks

    installation_ctx.deployed_workflows.run_workflow("quality_checker", run_config.name)

    checked = spark.table(run_config.output_config.location)
    assert_df_equality(checked, expected_output, ignore_nullable=True)


def test_data_quality_workflow_e2e_serverless(ws, spark, setup_serverless_workflows_with_checks, expected_output):
    installation_ctx, run_config = setup_serverless_workflows_with_checks

    installation_ctx.deployed_workflows.run_workflow("quality_checker", run_config.name)

    checked = spark.table(run_config.output_config.location)
    assert_df_equality(checked, expected_output, ignore_nullable=True)
