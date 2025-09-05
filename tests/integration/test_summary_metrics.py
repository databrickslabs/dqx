from datetime import datetime, timezone
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from databricks.labs.dqx.config import InputConfig, OutputConfig, ExtraParams
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.metrics_observer import DQMetricsObserver


RUN_TIME = datetime(2025, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
EXTRA_PARAMS = ExtraParams(run_time=RUN_TIME.isoformat())
TEST_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("salary", IntegerType(), True),
    ]
)
TEST_CHECKS = [
    {
        "name": "id_is_not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "id"}},
    },
    {
        "name": "name_is_not_null_and_not_empty",
        "criticality": "warn",
        "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "name"}},
    },
]


def test_engine_with_observer_before_action(ws, spark):
    """Test that summary metrics are empty before running a Spark action."""
    observer = DQMetricsObserver(name="test_observer")
    dq_engine = DQEngine(workspace_client=ws, spark=spark, observer=observer, extra_params=EXTRA_PARAMS)

    test_df = spark.createDataFrame(
        [
            [1, "Alice", 30, 50000],
            [2, "Bob", 25, 45000],
            [None, "Charlie", 35, 60000],  # This will trigger an error
            [4, None, 28, 55000],  # This will trigger a warning
        ],
        TEST_SCHEMA,
    )
    _, observation = dq_engine.apply_checks_by_metadata(test_df, TEST_CHECKS)

    actual_metrics = observation.get
    assert actual_metrics == {}


def test_engine_with_observer_after_action(ws, spark):
    """Test that summary metrics can be accessed after running a Spark action like df.count()."""
    custom_metrics = [
        "avg(case when _errors is not null then age else null end) as avg_error_age",
        "sum(case when _warnings is not null then salary else null end) as total_warning_salary",
    ]
    observer = DQMetricsObserver(name="test_observer", custom_metrics=custom_metrics)
    dq_engine = DQEngine(workspace_client=ws, spark=spark, observer=observer, extra_params=EXTRA_PARAMS)

    test_df = spark.createDataFrame(
        [
            [1, "Alice", 30, 50000],
            [2, "Bob", 25, 45000],
            [None, "Charlie", 35, 60000],
            [4, None, 28, 55000],
        ],
        TEST_SCHEMA,
    )
    checked_df, observation = dq_engine.apply_checks_by_metadata(test_df, TEST_CHECKS)
    checked_df.count()  # Trigger an action to get the metrics

    expected_metrics = {
        "input_count": 4,
        "error_count": 1,
        "warning_count": 1,
        "valid_count": 2,
        "avg_error_age": 35.0,
        "total_warning_salary": 55000,
    }
    actual_metrics = observation.get
    assert actual_metrics == expected_metrics


def test_engine_metrics_saved_to_table(ws, spark, make_schema, make_random):
    """Test that summary metrics are written to the table defined in metrics_config."""
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    input_table_name = f"{catalog_name}.{schema.name}.input_{make_random(6).lower()}"
    output_table_name = f"{catalog_name}.{schema.name}.output_{make_random(6).lower()}"
    metrics_table_name = f"{catalog_name}.{schema.name}.metrics_{make_random(6).lower()}"

    custom_metrics = [
        "avg(case when _errors is not null then age else null end) as avg_error_age",
        "sum(case when _warnings is not null then salary else null end) as total_warning_salary",
    ]
    observer = DQMetricsObserver(name="test_observer", custom_metrics=custom_metrics)
    dq_engine = DQEngine(workspace_client=ws, spark=spark, observer=observer, extra_params=EXTRA_PARAMS)

    test_df = spark.createDataFrame(
        [
            [1, "Alice", 30, 50000],
            [2, "Bob", 25, 45000],
            [None, "Charlie", 35, 60000],
            [4, None, 28, 55000],
        ],
        TEST_SCHEMA,
    )

    test_df.write.saveAsTable(input_table_name)
    input_config = InputConfig(location=input_table_name)
    output_config = OutputConfig(location=output_table_name)
    metrics_config = OutputConfig(location=metrics_table_name)

    dq_engine.apply_checks_by_metadata_and_save_in_table(
        checks=TEST_CHECKS, input_config=input_config, output_config=output_config, metrics_config=metrics_config
    )

    expected_metrics = {
        "input_count": 4,
        "error_count": 1,
        "warning_count": 1,
        "valid_count": 2,
        "avg_error_age": 35.0,
        "total_warning_salary": 55000,
    }
    actual_metrics = spark.table(metrics_table_name).collect()[0].asDict()

    assert actual_metrics == expected_metrics


def test_engine_metrics_with_quarantine_and_metrics(ws, spark, make_schema, make_random):
    """Test that metrics work correctly when using both quarantine and metrics configs."""
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    input_table_name = f"{catalog_name}.{schema.name}.input_{make_random(6).lower()}"
    output_table_name = f"{catalog_name}.{schema.name}.output_{make_random(6).lower()}"
    quarantine_table_name = f"{catalog_name}.{schema.name}.quarantine_{make_random(6).lower()}"
    metrics_table_name = f"{catalog_name}.{schema.name}.metrics_{make_random(6).lower()}"

    custom_metrics = [
        "avg(case when _errors is not null then age else null end) as avg_error_age",
        "sum(case when _warnings is not null then salary else null end) as total_warning_salary",
    ]
    observer = DQMetricsObserver(name="test_observer", custom_metrics=custom_metrics)
    dq_engine = DQEngine(workspace_client=ws, spark=spark, observer=observer, extra_params=EXTRA_PARAMS)

    test_df = spark.createDataFrame(
        [
            [1, "Alice", 30, 50000],
            [2, "Bob", 25, 45000],
            [None, "Charlie", 35, 60000],
            [4, None, 28, 55000],
        ],
        TEST_SCHEMA,
    )

    test_df.write.saveAsTable(input_table_name)
    input_config = InputConfig(location=input_table_name)
    output_config = OutputConfig(location=output_table_name)
    quarantine_config = OutputConfig(location=quarantine_table_name)
    metrics_config = OutputConfig(location=metrics_table_name)

    dq_engine.apply_checks_by_metadata_and_save_in_table(
        checks=TEST_CHECKS,
        input_config=input_config,
        output_config=output_config,
        quarantine_config=quarantine_config,
        metrics_config=metrics_config,
    )

    expected_metrics = {
        "input_count": 4,
        "error_count": 1,
        "warning_count": 1,
        "valid_count": 2,
        "avg_error_age": 35.0,
        "total_warning_salary": 55000,
    }
    actual_metrics = spark.table(metrics_table_name).collect()[0].asDict()

    assert actual_metrics == expected_metrics


def test_engine_without_observer_no_metrics_saved(ws, spark, make_schema, make_random):
    """Test that no metrics are saved when observer is not configured."""
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    input_table_name = f"{catalog_name}.{schema.name}.input_{make_random(6).lower()}"
    output_table_name = f"{catalog_name}.{schema.name}.output_{make_random(6).lower()}"
    metrics_table_name = f"{catalog_name}.{schema.name}.metrics_{make_random(6).lower()}"

    dq_engine = DQEngine(workspace_client=ws, spark=spark, extra_params=EXTRA_PARAMS)

    test_df = spark.createDataFrame(
        [
            [1, "Alice", 30, 50000],
            [2, "Bob", 25, 45000],
        ],
        TEST_SCHEMA,
    )

    test_df.write.saveAsTable(input_table_name)

    input_config = InputConfig(location=input_table_name)
    output_config = OutputConfig(location=output_table_name)
    metrics_config = OutputConfig(location=metrics_table_name)

    dq_engine.apply_checks_by_metadata_and_save_in_table(
        checks=TEST_CHECKS, input_config=input_config, output_config=output_config, metrics_config=metrics_config
    )

    assert not ws.tables.exists(metrics_table_name).table_exists


def test_engine_streaming_metrics_saved_to_table(ws, spark, make_schema, make_random, make_volume):
    """Test that summary metrics are written to the table when using streaming with metrics_config."""
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    input_table_name = f"{catalog_name}.{schema.name}.input_{make_random(6).lower()}"
    output_table_name = f"{catalog_name}.{schema.name}.output_{make_random(6).lower()}"
    metrics_table_name = f"{catalog_name}.{schema.name}.metrics_{make_random(6).lower()}"
    volume = make_volume(catalog_name=catalog_name, schema_name=schema.name)
    checkpoint_location = f"/Volumes/{volume.catalog_name}/{volume.schema_name}/{volume.name}/{make_random(8).lower()}"

    custom_metrics = [
        "avg(case when _errors is not null then age else null end) as avg_error_age",
        "sum(case when _warnings is not null then salary else null end) as total_warning_salary",
    ]
    observer = DQMetricsObserver(name="test_streaming_observer", custom_metrics=custom_metrics)
    dq_engine = DQEngine(workspace_client=ws, spark=spark, observer=observer, extra_params=EXTRA_PARAMS)

    test_df = spark.createDataFrame(
        [
            [1, "Alice", 30, 50000],
            [2, "Bob", 25, 45000],
            [None, "Charlie", 35, 60000],
            [4, None, 28, 55000],
        ],
        TEST_SCHEMA,
    )

    test_df.write.saveAsTable(input_table_name)
    input_config = InputConfig(location=input_table_name, is_streaming=True)
    output_config = OutputConfig(
        location=output_table_name, options={"checkPointLocation": checkpoint_location}, trigger={"availableNow": True}
    )
    metrics_config = OutputConfig(location=metrics_table_name)

    dq_engine.apply_checks_by_metadata_and_save_in_table(
        checks=TEST_CHECKS, input_config=input_config, output_config=output_config, metrics_config=metrics_config
    )

    expected_metrics = {
        "input_count": 4,
        "error_count": 1,
        "warning_count": 1,
        "valid_count": 2,
        "avg_error_age": 35.0,
        "total_warning_salary": 55000,
    }
    actual_metrics = spark.table(metrics_table_name).collect()[0].asDict()
    assert actual_metrics == expected_metrics


def test_engine_streaming_with_quarantine_and_metrics(ws, spark, make_schema, make_random, make_volume):
    """Test that streaming metrics work correctly when using both quarantine and metrics configs."""
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    input_table_name = f"{catalog_name}.{schema.name}.input_{make_random(6).lower()}"
    output_table_name = f"{catalog_name}.{schema.name}.output_{make_random(6).lower()}"
    quarantine_table_name = f"{catalog_name}.{schema.name}.quarantine_{make_random(6).lower()}"
    metrics_table_name = f"{catalog_name}.{schema.name}.metrics_{make_random(6).lower()}"
    volume = make_volume(catalog_name=catalog_name, schema_name=schema.name)

    custom_metrics = [
        "avg(case when _errors is not null then age else null end) as avg_error_age",
        "sum(case when _warnings is not null then salary else null end) as total_warning_salary",
    ]
    observer = DQMetricsObserver(name="test_streaming_quarantine_observer", custom_metrics=custom_metrics)
    dq_engine = DQEngine(workspace_client=ws, spark=spark, observer=observer, extra_params=EXTRA_PARAMS)

    test_df = spark.createDataFrame(
        [
            [1, "Alice", 30, 50000],
            [2, "Bob", 25, 45000],
            [None, "Charlie", 35, 60000],
            [4, None, 28, 55000],
        ],
        TEST_SCHEMA,
    )

    test_df.write.saveAsTable(input_table_name)
    input_config = InputConfig(location=input_table_name, is_streaming=True)
    output_config = OutputConfig(
        location=output_table_name,
        options={"checkPointLocation": f"/Volumes/{volume.catalog_name}/{volume.schema_name}/{volume.name}/output"},
        trigger={"availableNow": True},
    )
    quarantine_config = OutputConfig(
        location=quarantine_table_name,
        options={"checkPointLocation": f"/Volumes/{volume.catalog_name}/{volume.schema_name}/{volume.name}/quarantine"},
        trigger={"availableNow": True},
    )
    metrics_config = OutputConfig(location=metrics_table_name)

    dq_engine.apply_checks_by_metadata_and_save_in_table(
        checks=TEST_CHECKS,
        input_config=input_config,
        output_config=output_config,
        quarantine_config=quarantine_config,
        metrics_config=metrics_config,
    )

    expected_metrics = {
        "input_count": 4,
        "error_count": 1,
        "warning_count": 1,
        "valid_count": 2,
        "avg_error_age": 35.0,
        "total_warning_salary": 55000,
    }
    actual_metrics = spark.table(metrics_table_name).collect()[0].asDict()
    assert actual_metrics == expected_metrics
