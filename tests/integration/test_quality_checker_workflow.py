import pytest
from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from databricks.labs.blueprint.parallel import ManyError

from databricks.labs.dqx.engine import DQEngine


def test_quality_checker_workflow_e2e(ws, spark, setup_workflows, expected_quality_checking_output):
    installation_ctx, run_config = setup_workflows(checks=True)

    installation_ctx.deployed_workflows.run_workflow("quality_checker", run_config.name)

    checked = spark.table(run_config.output_config.location)
    assert_df_equality(checked, expected_quality_checking_output, ignore_nullable=True)


def test_quality_checker_workflow_e2e_serverless(
    ws, spark, setup_serverless_workflows, expected_quality_checking_output
):
    installation_ctx, run_config = setup_serverless_workflows(checks=True)

    installation_ctx.deployed_workflows.run_workflow("quality_checker", run_config.name)

    checked = spark.table(run_config.output_config.location)
    assert_df_equality(checked, expected_quality_checking_output, ignore_nullable=True)


def test_quality_checker_workflow_e2e_with_quarantine(
    ws, spark, setup_serverless_workflows, expected_quality_checking_output
):
    installation_ctx, run_config = setup_serverless_workflows(quarantine=True, checks=True)

    installation_ctx.deployed_workflows.run_workflow("quality_checker", run_config.name)

    dq_engine = DQEngine(ws, spark)
    expected_output = dq_engine.get_valid(expected_quality_checking_output)
    expected_quarantine = dq_engine.get_invalid(expected_quality_checking_output)

    output = spark.table(run_config.output_config.location)
    assert_df_equality(output, expected_output, ignore_nullable=True)

    quarantine = spark.table(run_config.quarantine_config.location)
    assert_df_equality(quarantine, expected_quarantine, ignore_nullable=True)


def test_quality_checker_workflow_e2e_when_missing_checks_file(ws, setup_serverless_workflows):
    installation_ctx, run_config = setup_serverless_workflows()

    with pytest.raises(ManyError) as failure:
        installation_ctx.deployed_workflows.run_workflow("quality_checker", run_config.name)

    checks_location = f"{installation_ctx.installation.install_folder()}/{run_config.checks_location}"
    assert f"Checks file {checks_location} missing" in str(failure.value)
