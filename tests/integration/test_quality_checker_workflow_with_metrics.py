from datetime import timedelta
from chispa.dataframe_comparer import assert_df_equality  # type: ignore

from databricks.labs.dqx.config import WorkspaceFileChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.metrics_observer import OBSERVATION_TABLE_SCHEMA
from tests.integration.conftest import assert_quarantine_and_output_dfs, assert_output_df, RUN_TIME


def test_quality_checker_workflow_with_metrics(spark, setup_workflows_with_metrics, expected_quality_checking_output):
    """Test that quality checker workflow saves metrics when configured."""
    ctx, run_config = setup_workflows_with_metrics()
    ctx.deployed_workflows.run_workflow("quality-checker", run_config.name)

    checks_location = f"{ctx.installation.install_folder()}/{run_config.checks_location}"
    expected_metrics = [
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": checks_location,
            "metric_name": "input_row_count",
            "metric_value": "10",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": checks_location,
            "metric_name": "error_row_count",
            "metric_value": "3",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": checks_location,
            "metric_name": "warning_row_count",
            "metric_value": "0",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": checks_location,
            "metric_name": "valid_row_count",
            "metric_value": "7",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).orderBy(
        "metric_name"
    )
    actual_metrics_df = spark.table(run_config.metrics_config.location).orderBy("metric_name")
    assert_df_equality(expected_metrics_df, actual_metrics_df)

    assert_output_df(spark, expected_quality_checking_output, run_config.output_config)


def test_quality_checker_workflow_with_quarantine_and_metrics(
    ws, spark, setup_workflows_with_metrics, expected_quality_checking_output
):
    """Test workflow with both quarantine and metrics configurations."""
    ctx, run_config = setup_workflows_with_metrics(quarantine=True, custom_metrics=["count(1) as total_ids"])

    ctx.deployed_workflows.run_workflow("quality-checker", run_config.name)

    checks_location = f"{ctx.installation.install_folder()}/{run_config.checks_location}"
    expected_metrics = [
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": run_config.quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "input_row_count",
            "metric_value": "10",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": run_config.quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "error_row_count",
            "metric_value": "3",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": run_config.quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "warning_row_count",
            "metric_value": "0",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": run_config.quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "valid_row_count",
            "metric_value": "7",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": run_config.quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "total_ids",
            "metric_value": "10",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).orderBy(
        "metric_name"
    )
    actual_metrics_df = spark.table(run_config.metrics_config.location).orderBy("metric_name")
    assert_df_equality(expected_metrics_df, actual_metrics_df)

    assert_quarantine_and_output_dfs(
        ws, spark, expected_quality_checking_output, run_config.output_config, run_config.quarantine_config
    )


def test_e2e_workflow_with_metrics(spark, setup_workflows_with_metrics, expected_quality_checking_output):
    """Test that e2e workflow generates checks and applies them with metrics."""
    ctx, run_config = setup_workflows_with_metrics(custom_metrics=["max(id) as max_id", "min(id) as min_id"])

    ctx.deployed_workflows.run_workflow("e2e", run_config.name)

    checks_location = f"{ctx.installation.install_folder()}/{run_config.checks_location}"
    expected_metrics = [
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": checks_location,
            "metric_name": "input_row_count",
            "metric_value": "10",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": checks_location,
            "metric_name": "max_id",
            "metric_value": "6",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": checks_location,
            "metric_name": "min_id",
            "metric_value": "1",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).orderBy(
        "metric_name"
    )
    actual_metrics_df = (
        spark.table(run_config.metrics_config.location)
        .where("metric_name NOT IN ('error_row_count', 'warning_row_count', 'valid_row_count')")
        .orderBy("metric_name")
    )
    assert_df_equality(expected_metrics_df, actual_metrics_df)
    assert expected_quality_checking_output.count() == spark.table(run_config.output_config.location).count()


def test_custom_metrics_in_workflow_for_all_run_configs(
    spark, setup_workflows_with_metrics, expected_quality_checking_output
):
    """Test workflow with custom metrics for all run configs."""
    custom_metrics = [
        "min(id) as min_id",
        "max(id) as max_id",
        "sum(id) as total_sum_id",
        "count(1) as total_ids",
        "max(id) - min(id) as id_range",
    ]

    ctx, run_config = setup_workflows_with_metrics(custom_metrics=custom_metrics)

    checks_location = f"{ctx.installation.install_folder()}/{run_config.checks_location}"
    expected_metrics = [
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": checks_location,
            "metric_name": "input_row_count",
            "metric_value": "10",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": checks_location,
            "metric_name": "min_id",
            "metric_value": "1",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": checks_location,
            "metric_name": "max_id",
            "metric_value": "6",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": checks_location,
            "metric_name": "total_sum_id",
            "metric_value": "27",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": checks_location,
            "metric_name": "total_ids",
            "metric_value": "10",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": checks_location,
            "metric_name": "id_range",
            "metric_value": "5",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    ctx.deployed_workflows.run_workflow("quality-checker", run_config_name="")  # run for all run configs

    expected_metrics_df = spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).orderBy(
        "metric_name"
    )
    actual_metrics_df = (
        spark.table(run_config.metrics_config.location)
        .where("metric_name NOT IN ('error_row_count', 'warning_row_count', 'valid_row_count')")
        .orderBy("metric_name")
    )
    assert_df_equality(expected_metrics_df, actual_metrics_df)

    assert_output_df(spark, expected_quality_checking_output, run_config.output_config)


def test_quality_checker_workflow_with_streaming_quarantine_and_metrics(
    ws, spark, setup_workflows_with_metrics, expected_quality_checking_output
):
    """Test workflow with both quarantine and metrics configurations in streaming."""
    ctx, run_config = setup_workflows_with_metrics(
        quarantine=True,
        custom_metrics=["count(1) as total_ids"],
        is_streaming=True,
    )

    ctx.deployed_workflows.run_workflow("quality-checker", run_config.name)

    checks_location = f"{ctx.installation.install_folder()}/{run_config.checks_location}"
    expected_metrics = [
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": run_config.quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "input_row_count",
            "metric_value": "10",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": run_config.quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "error_row_count",
            "metric_value": "3",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": run_config.quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "warning_row_count",
            "metric_value": "0",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": run_config.quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "valid_row_count",
            "metric_value": "7",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": run_config.quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "total_ids",
            "metric_value": "10",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = (
        spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).drop("run_time").orderBy("metric_name")
    )
    actual_metrics_df = spark.table(run_config.metrics_config.location).drop("run_time").orderBy("metric_name")
    assert_df_equality(expected_metrics_df, actual_metrics_df)

    assert_quarantine_and_output_dfs(
        ws, spark, expected_quality_checking_output, run_config.output_config, run_config.quarantine_config
    )


def test_quality_checker_workflow_with_continuous_streaming_quarantine_and_metrics(
    ws, spark, setup_workflows_with_metrics, expected_quality_checking_output
):
    """Test workflow with both quarantine and metrics configurations in continuous streaming."""
    ctx, run_config = setup_workflows_with_metrics(
        quarantine=True,
        custom_metrics=["count(1) as total_ids"],
        is_streaming=True,
        is_continuous_streaming=True,
    )

    try:
        ctx.deployed_workflows.run_workflow("quality-checker", run_config.name, max_wait=timedelta(minutes=10))
    except TimeoutError:
        print("Stopped workflow running with continuous streaming")

    checks_location = f"{ctx.installation.install_folder()}/{run_config.checks_location}"
    expected_metrics = [
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": run_config.quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "input_row_count",
            "metric_value": "10",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": run_config.quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "error_row_count",
            "metric_value": "3",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": run_config.quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "warning_row_count",
            "metric_value": "0",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": run_config.quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "valid_row_count",
            "metric_value": "7",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": run_config.quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "total_ids",
            "metric_value": "10",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = (
        spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).drop("run_time").orderBy("metric_name")
    )
    actual_metrics_df = spark.table(run_config.metrics_config.location).drop("run_time").orderBy("metric_name")
    assert_df_equality(expected_metrics_df, actual_metrics_df)

    assert_quarantine_and_output_dfs(
        ws, spark, expected_quality_checking_output, run_config.output_config, run_config.quarantine_config
    )


def test_quality_checker_workflow_with_quarantine_and_metrics_for_patterns(
    ws, spark, installation_ctx, setup_workflows_with_metrics, expected_quality_checking_output
):
    """Test workflow with both quarantine and metrics configurations for patterns."""
    ctx, run_config = setup_workflows_with_metrics(quarantine=True)

    input_table = run_config.input_config.location
    catalog_name, schema_name, _ = input_table.split('.')

    dq_engine = DQEngine(ws, spark)
    checks = dq_engine.load_checks(
        config=WorkspaceFileChecksStorageConfig(location=f"{installation_ctx.installation.install_folder()}/checks.yml")
    )
    checks_location = f"{installation_ctx.installation.install_folder()}/{input_table}.yml"
    dq_engine.save_checks(
        checks=checks,
        config=WorkspaceFileChecksStorageConfig(location=checks_location),
    )
    ws.workspace.delete(f"{installation_ctx.installation.install_folder()}/checks.yml")

    # run for all run configs defined
    ctx.deployed_workflows.run_workflow(
        "quality-checker", run_config_name=run_config.name, patterns=f"{catalog_name}.{schema_name}.*"
    )

    output_location = input_table + "_dq_output"
    quarantine_location = input_table + "_dq_quarantine"

    expected_metrics = [
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": output_location,
            "quarantine_location": quarantine_location,
            "checks_location": checks_location,
            "metric_name": "input_row_count",
            "metric_value": "10",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": output_location,
            "quarantine_location": quarantine_location,
            "checks_location": checks_location,
            "metric_name": "error_row_count",
            "metric_value": "3",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": output_location,
            "quarantine_location": quarantine_location,
            "checks_location": checks_location,
            "metric_name": "warning_row_count",
            "metric_value": "0",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": output_location,
            "quarantine_location": quarantine_location,
            "checks_location": checks_location,
            "metric_name": "valid_row_count",
            "metric_value": "7",
            "run_time": RUN_TIME,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).orderBy(
        "metric_name"
    )
    actual_metrics_df = spark.table(run_config.metrics_config.location).orderBy("metric_name")
    assert_df_equality(expected_metrics_df, actual_metrics_df)

    dq_engine = DQEngine(ws, spark)
    expected_output_df = dq_engine.get_valid(expected_quality_checking_output)
    expected_quarantine_df = dq_engine.get_invalid(expected_quality_checking_output)

    output_df = spark.table(input_table + "_dq_output")
    assert_df_equality(output_df, expected_output_df, ignore_nullable=True)

    quarantine_df = spark.table(input_table + "_dq_quarantine")
    assert_df_equality(quarantine_df, expected_quarantine_df, ignore_nullable=True)
