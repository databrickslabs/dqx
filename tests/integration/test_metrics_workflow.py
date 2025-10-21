from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from databricks.labs.dqx.metrics_observer import OBSERVATION_TABLE_SCHEMA


def test_quality_checker_workflow_with_metrics(spark, setup_workflows_with_metrics):
    """Test that quality checker workflow saves metrics when configured."""
    ctx, run_config = setup_workflows_with_metrics(metrics=True)
    ctx.deployed_workflows.run_workflow("quality-checker", run_config.name)

    expected_metrics = [
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "input_row_count",
            "metric_value": "10",
            "run_ts": None,  # Will be set at runtime
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "error_row_count",
            "metric_value": "3",
            "run_ts": None,  # Will be set at runtime
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "warning_row_count",
            "metric_value": "0",
            "run_ts": None,  # Will be set at runtime
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "valid_row_count",
            "metric_value": "7",
            "run_ts": None,  # Will be set at runtime
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = (
        spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).drop("run_ts").orderBy("metric_name")
    )
    actual_metrics_df = spark.table(run_config.metrics_config.location).drop("run_ts").orderBy("metric_name")
    assert_df_equality(expected_metrics_df, actual_metrics_df)


def test_quality_checker_workflow_with_quarantine_and_metrics(spark, setup_workflows_with_metrics):
    """Test workflow with both quarantine and metrics configurations."""
    ctx, run_config = setup_workflows_with_metrics(
        quarantine=True, metrics=True, custom_metrics=["count(1) as total_ids"]
    )

    ctx.deployed_workflows.run_workflow("quality-checker", run_config.name)

    expected_metrics = [
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": run_config.quarantine_config.location,
            "checks_location": None,
            "metric_name": "input_row_count",
            "metric_value": "10",
            "run_ts": None,  # Will be set at runtime
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": run_config.quarantine_config.location,
            "checks_location": None,
            "metric_name": "error_row_count",
            "metric_value": "3",
            "run_ts": None,  # Will be set at runtime
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": run_config.quarantine_config.location,
            "checks_location": None,
            "metric_name": "warning_row_count",
            "metric_value": "0",
            "run_ts": None,  # Will be set at runtime
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": run_config.quarantine_config.location,
            "checks_location": None,
            "metric_name": "valid_row_count",
            "metric_value": "7",
            "run_ts": None,  # Will be set at runtime
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": run_config.quarantine_config.location,
            "checks_location": None,
            "metric_name": "total_ids",
            "metric_value": "10",
            "run_ts": None,  # Will be set at runtime
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = (
        spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).drop("run_ts").orderBy("metric_name")
    )
    actual_metrics_df = spark.table(run_config.metrics_config.location).drop("run_ts").orderBy("metric_name")
    assert_df_equality(expected_metrics_df, actual_metrics_df)


def test_e2e_workflow_with_metrics(spark, setup_workflows_with_metrics):
    """Test that e2e workflow generates checks and applies them with metrics."""
    ctx, run_config = setup_workflows_with_metrics(
        metrics=True, custom_metrics=["max(id) as max_id", "min(id) as min_id"]
    )

    ctx.deployed_workflows.run_workflow("e2e", run_config.name)

    expected_metrics = [
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "input_row_count",
            "metric_value": "10",
            "run_ts": None,  # Will be set at runtime
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "max_id",
            "metric_value": "6",
            "run_ts": None,  # Will be set at runtime
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "min_id",
            "metric_value": "1",
            "run_ts": None,  # Will be set at runtime
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = (
        spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).drop("run_ts").orderBy("metric_name")
    )
    actual_metrics_df = (
        spark.table(run_config.metrics_config.location)
        .where("metric_name NOT IN ('error_row_count', 'warning_row_count', 'valid_row_count')")
        .drop("run_ts")
        .orderBy("metric_name")
    )
    assert_df_equality(expected_metrics_df, actual_metrics_df)


def test_custom_metrics_in_workflow(spark, setup_workflows_with_metrics):
    """Test workflow with custom metrics."""
    custom_metrics = [
        "min(id) as min_id",
        "max(id) as max_id",
        "sum(id) as total_sum_id",
        "count(1) as total_ids",
        "max(id) - min(id) as id_range",
    ]

    ctx, run_config = setup_workflows_with_metrics(metrics=True, custom_metrics=custom_metrics)

    expected_metrics = [
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "input_row_count",
            "metric_value": "10",
            "run_ts": None,  # Will be set at runtime
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "min_id",
            "metric_value": "1",
            "run_ts": None,  # Will be set at runtime
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "max_id",
            "metric_value": "6",
            "run_ts": None,  # Will be set at runtime
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "total_sum_id",
            "metric_value": "27",
            "run_ts": None,  # Will be set at runtime
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "total_ids",
            "metric_value": "10",
            "run_ts": None,  # Will be set at runtime
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "id_range",
            "metric_value": "5",
            "run_ts": None,  # Will be set at runtime
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    ctx.deployed_workflows.run_workflow("quality-checker", run_config.name)

    expected_metrics_df = (
        spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).drop("run_ts").orderBy("metric_name")
    )
    actual_metrics_df = (
        spark.table(run_config.metrics_config.location)
        .where("metric_name NOT IN ('error_row_count', 'warning_row_count', 'valid_row_count')")
        .drop("run_ts")
        .orderBy("metric_name")
    )
    assert_df_equality(expected_metrics_df, actual_metrics_df)


def test_quality_checker_workflow_without_metrics_config(setup_workflows_with_metrics):
    """Test that workflow works normally when metrics config is not provided."""
    _, run_config = setup_workflows_with_metrics(metrics=False)
    assert run_config.metrics_config is None
