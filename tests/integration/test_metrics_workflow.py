def test_quality_checker_workflow_with_metrics(spark, setup_workflows_with_metrics):
    """Test that quality checker workflow saves metrics when configured."""
    ctx, run_config = setup_workflows_with_metrics(metrics=True)

    ctx.deployed_workflows.run_workflow("quality-checker", run_config.name)
    output_df = spark.table(run_config.output_config.location)
    output_count = output_df.count()

    expected_metrics = {
        "input_count": output_count,
        "error_count": 0,
        "warning_count": 0,
        "valid_count": output_count,
    }

    metrics_rows = spark.table(run_config.metrics_config.location).collect()
    assert len(metrics_rows) == 1

    actual_metrics = metrics_rows[0].asDict()
    assert actual_metrics == expected_metrics


def test_quality_checker_workflow_with_quarantine_and_metrics(spark, setup_workflows_with_metrics):
    """Test workflow with both quarantine and metrics configurations."""
    ctx, run_config = setup_workflows_with_metrics(
        quarantine=True, metrics=True, custom_metrics=["count(distinct name) as unique_names"]
    )

    ctx.deployed_workflows.run_workflow("quality-checker", run_config.name)
    output_df = spark.table(run_config.output_config.location)
    output_count = output_df.count()
    quarantine_df = spark.table(run_config.quarantine_config.location)
    quarantine_count = quarantine_df.count()

    expected_metrics = {
        "input_count": output_count + quarantine_count,
        "error_count": quarantine_count,
        "warning_count": 0,
        "valid_count": output_count,
        "unique_names": 0,
    }

    metrics_rows = spark.table(run_config.metrics_config.location).collect()
    assert len(metrics_rows) == 1

    actual_metrics = metrics_rows[0].asDict()
    assert actual_metrics == expected_metrics


def test_e2e_workflow_with_metrics(spark, setup_workflows_with_metrics):
    """Test that e2e workflow generates checks and applies them with metrics."""
    ctx, run_config = setup_workflows_with_metrics(
        metrics=True, custom_metrics=["max(id) as max_id", "min(id) as min_id"]
    )

    ctx.deployed_workflows.run_workflow("e2e", run_config.name)
    output_df = spark.table(run_config.output_config.location)
    output_count = output_df.count()

    expected_metrics = {
        "input_count": output_count,
        "error_count": 0,
        "warning_count": 0,
        "valid_count": output_count,
        "max_id": 1,
        "min_id": 0,
    }

    metrics_rows = spark.table(run_config.metrics_config.location).collect()
    assert len(metrics_rows) == 1

    actual_metrics = metrics_rows[0].asDict()
    assert actual_metrics == expected_metrics


def test_custom_metrics_in_workflow(spark, setup_workflows_with_metrics):
    """Test workflow with custom metrics."""
    custom_metrics = [
        "avg(id) as average_id",
        "sum(id) as total_id",
        "count(distinct name) as unique_names",
        "max(id) - min(id) as id_range",
    ]

    ctx, run_config = setup_workflows_with_metrics(metrics=True, custom_metrics=custom_metrics)

    expected_metrics = {
        "input_count": 0,
        "error_count": 0,
        "warning_count": 0,
        "valid_count": 0,
        "average_id": 0,
        "total_id": 0,
        "unique_names": 0,
        "id_range": 0,
    }
    ctx.deployed_workflows.run_workflow("quality-checker", run_config.name)
    metrics_df = spark.table(run_config.metrics_config.location)
    metrics_rows = metrics_df.collect()
    assert len(metrics_rows) == 1

    actual_metrics = metrics_rows[0].asDict()
    assert actual_metrics == expected_metrics


def test_quality_checker_workflow_without_metrics_config(ws, setup_workflows_with_metrics):
    """Test that workflow works normally when metrics config is not provided."""
    _, run_config = setup_workflows_with_metrics(metrics=False)
    assert run_config.metrics_config is None
