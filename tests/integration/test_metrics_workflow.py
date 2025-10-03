from tests.integration.conftest import validate_metrics


def test_quality_checker_workflow_with_metrics(spark, setup_workflows_with_metrics):
    """Test that quality checker workflow saves metrics when configured."""
    ctx, run_config = setup_workflows_with_metrics(metrics=True)

    ctx.deployed_workflows.run_workflow("quality-checker", run_config.name)
    output_df = spark.table(run_config.output_config.location)
    output_count = output_df.count()

    expected_metrics = [
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "input_row_count",
            "metric_value": str(output_count),
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
            "metric_value": str(output_count),
            "run_ts": None,  # Will be set at runtime
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    metrics_rows = spark.table(run_config.metrics_config.location).collect()
    assert len(metrics_rows) == 4

    actual_metrics_dict = {row["metric_name"]: row.asDict() for row in metrics_rows}
    expected_metrics_dict = {metric["metric_name"]: metric for metric in expected_metrics}
    validate_metrics(actual_metrics_dict, expected_metrics_dict)


def test_quality_checker_workflow_with_quarantine_and_metrics(spark, setup_workflows_with_metrics):
    """Test workflow with both quarantine and metrics configurations."""
    ctx, run_config = setup_workflows_with_metrics(
        quarantine=True, metrics=True, custom_metrics=["count(1) as total_names"]
    )

    ctx.deployed_workflows.run_workflow("quality-checker", run_config.name)
    output_df = spark.table(run_config.output_config.location)
    output_count = output_df.count()
    quarantine_df = spark.table(run_config.quarantine_config.location)
    quarantine_count = quarantine_df.count()

    expected_metrics = [
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": run_config.quarantine_config.location,
            "checks_location": None,
            "metric_name": "input_row_count",
            "metric_value": str(output_count + quarantine_count),
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
            "metric_value": str(quarantine_count),
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
            "metric_value": str(output_count),
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
            "metric_name": "total_names",
            "metric_value": "4",
            "run_ts": None,  # Will be set at runtime
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    metrics_rows = spark.table(run_config.metrics_config.location).collect()
    assert len(metrics_rows) == 5

    actual_metrics_dict = {row["metric_name"]: row.asDict() for row in metrics_rows}
    expected_metrics_dict = {metric["metric_name"]: metric for metric in expected_metrics}
    validate_metrics(actual_metrics_dict, expected_metrics_dict)


def test_e2e_workflow_with_metrics(spark, setup_workflows_with_metrics):
    """Test that e2e workflow generates checks and applies them with metrics."""
    ctx, run_config = setup_workflows_with_metrics(
        metrics=True, custom_metrics=["max(id) as max_id", "min(id) as min_id"]
    )

    ctx.deployed_workflows.run_workflow("e2e", run_config.name)
    output_df = spark.table(run_config.output_config.location)
    output_count = output_df.count()

    expected_metrics = [
        {
            "run_name": "dqx",
            "input_location": run_config.input_config.location,
            "output_location": run_config.output_config.location,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "input_row_count",
            "metric_value": str(output_count),
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
            "metric_value": str(output_count),
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
            "metric_name": "min_id",
            "metric_value": "0",
            "run_ts": None,  # Will be set at runtime
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    metrics_rows = spark.table(run_config.metrics_config.location).collect()
    assert len(metrics_rows) == 6

    actual_metrics_dict = {row["metric_name"]: row.asDict() for row in metrics_rows}
    expected_metrics_dict = {metric["metric_name"]: metric for metric in expected_metrics}
    validate_metrics(actual_metrics_dict, expected_metrics_dict)


def test_custom_metrics_in_workflow(spark, setup_workflows_with_metrics):
    """Test workflow with custom metrics."""
    custom_metrics = [
        "avg(id) as average_id",
        "sum(id) as total_id",
        "count(distinct name) as unique_names",
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
            "metric_name": "error_row_count",
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
            "metric_name": "average_id",
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
            "metric_name": "total_id",
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
            "metric_name": "unique_names",
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
            "metric_name": "id_range",
            "metric_value": "0",
            "run_ts": None,  # Will be set at runtime
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    ctx.deployed_workflows.run_workflow("quality-checker", run_config.name)
    metrics_df = spark.table(run_config.metrics_config.location)
    metrics_rows = metrics_df.collect()
    assert len(metrics_rows) == 8

    actual_metrics_dict = {row["metric_name"]: row.asDict() for row in metrics_rows}
    expected_metrics_dict = {metric["metric_name"]: metric for metric in expected_metrics}
    validate_metrics(actual_metrics_dict, expected_metrics_dict)


def test_quality_checker_workflow_without_metrics_config(setup_workflows_with_metrics):
    """Test that workflow works normally when metrics config is not provided."""
    _, run_config = setup_workflows_with_metrics(metrics=False)
    assert run_config.metrics_config is None
