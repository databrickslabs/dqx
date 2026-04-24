import json
import time
from datetime import datetime

import pytest
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.testing.utils import assertDataFrameEqual

from databricks.sdk.errors import NotFound
from databricks.labs.dqx.config import InputConfig, OutputConfig, ExtraParams
from databricks.labs.dqx.checks_serializer import deserialize_checks
from databricks.labs.dqx.rule_fingerprint import compute_rule_set_fingerprint_by_metadata
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.metrics_observer import DQMetricsObserver, OBSERVATION_TABLE_SCHEMA
from databricks.labs.dqx.reporting_columns import ColumnArguments

from tests.constants import TEST_CATALOG
from tests.integration.conftest import EXTRA_PARAMS

# Test constants
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
TEST_CHECKS_RULE_SET_FINGERPRINT = compute_rule_set_fingerprint_by_metadata(TEST_CHECKS)
TEST_OBSERVER_NAME = "test_observer"
# Expected check_metrics JSON value for TEST_CHECKS with standard 4-row test data
# (row 3 has id=None → error, row 4 has name=None → warning)
TEST_CHECK_METRICS_VALUE = (
    '[{"check_name":"id_is_not_null","error_count":1,"warning_count":0},'
    '{"check_name":"name_is_not_null_and_not_empty","error_count":0,"warning_count":1}]'
)


def test_observer_custom_column_names(ws, spark):
    """Test that observers have the correct column names when DQEngine is created with custom column names."""
    errors_column = "dq_errors"
    warnings_column = "dq_warnings"
    engine_params = ExtraParams(
        result_column_names={
            ColumnArguments.ERRORS.value: errors_column,
            ColumnArguments.WARNINGS.value: warnings_column,
        },
    )
    observer = DQMetricsObserver(name="test_observer")
    _ = DQEngine(workspace_client=ws, spark=spark, extra_params=engine_params, observer=observer)

    metrics = observer.get_metrics()
    assert f"count(case when {errors_column} is not null then 1 end) as error_row_count" in metrics
    assert f"count(case when {warnings_column} is not null then 1 end) as warning_row_count" in metrics
    assert (
        f"count(case when {errors_column} is null and {warnings_column} is null then 1 end) as valid_row_count"
        in metrics
    )


@pytest.mark.parametrize("apply_checks_method", [DQEngine.apply_checks, DQEngine.apply_checks_by_metadata])
def test_observer_metrics_before_action(ws, spark, apply_checks_method):
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

    if apply_checks_method == DQEngine.apply_checks:
        checks = deserialize_checks(TEST_CHECKS)
        checked_df, observation = dq_engine.apply_checks(test_df, checks)
    elif apply_checks_method == DQEngine.apply_checks_by_metadata:
        checked_df, observation = dq_engine.apply_checks_by_metadata(test_df, TEST_CHECKS)
    else:
        raise ValueError("Invalid 'apply_checks_method' used for testing observable metrics.")

    # Read metrics BEFORE any action — must be empty.
    assert observation.get == {}
    # Trigger the action so Spark Connect can complete the observation's lifecycle and
    # release server-side state. Leaving the attached DataFrame GC'd without executing
    # it has been observed to destabilize the shared session for subsequent tests.
    checked_df.count()
    assert observation.get == {
        "input_row_count": 4,
        "error_row_count": 1,
        "warning_row_count": 1,
        "valid_row_count": 2,
        "check_metrics": TEST_CHECK_METRICS_VALUE,
    }


@pytest.mark.parametrize("apply_checks_method", [DQEngine.apply_checks, DQEngine.apply_checks_by_metadata])
def test_observer_metrics(ws, spark, apply_checks_method):
    """Test that summary metrics can be accessed after running a Spark action like df.count()."""
    observer = DQMetricsObserver(name="test_observer")
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

    if apply_checks_method == DQEngine.apply_checks:
        checks = deserialize_checks(TEST_CHECKS)
        checked_df, observation = dq_engine.apply_checks(test_df, checks)
    elif apply_checks_method == DQEngine.apply_checks_by_metadata:
        checked_df, observation = dq_engine.apply_checks_by_metadata(test_df, TEST_CHECKS)
    else:
        raise ValueError("Invalid 'apply_checks_method' used for testing observable metrics.")

    checked_df.count()  # Trigger an action to get the metrics
    actual_metrics = observation.get
    assert actual_metrics == {
        "input_row_count": 4,
        "error_row_count": 1,
        "warning_row_count": 1,
        "valid_row_count": 2,
        "check_metrics": TEST_CHECK_METRICS_VALUE,
    }


@pytest.mark.parametrize("apply_checks_method", [DQEngine.apply_checks, DQEngine.apply_checks_by_metadata])
def test_observer_metrics_empty_checks(ws, spark, apply_checks_method):
    """Test that summary metrics can be accessed after running a Spark action like df.count()."""
    observer = DQMetricsObserver(name="test_observer")
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

    if apply_checks_method == DQEngine.apply_checks:
        checked_df, observation = dq_engine.apply_checks(test_df, [])
    elif apply_checks_method == DQEngine.apply_checks_by_metadata:
        checked_df, observation = dq_engine.apply_checks_by_metadata(test_df, [])
    else:
        raise ValueError("Invalid 'apply_checks_method' used for testing observable metrics.")

    checked_df.count()  # Trigger an action to get the metrics
    expected_metrics = {
        "input_row_count": 4,
        "error_row_count": 0,
        "warning_row_count": 0,
        "valid_row_count": 4,
    }
    actual_metrics = observation.get
    assert actual_metrics == expected_metrics


@pytest.mark.parametrize("apply_checks_method", [DQEngine.apply_checks, DQEngine.apply_checks_by_metadata])
def test_observer_custom_metrics(ws, spark, apply_checks_method):
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

    if apply_checks_method == DQEngine.apply_checks:
        checks = deserialize_checks(TEST_CHECKS)
        checked_df, observation = dq_engine.apply_checks(test_df, checks)
    elif apply_checks_method == DQEngine.apply_checks_by_metadata:
        checked_df, observation = dq_engine.apply_checks_by_metadata(test_df, TEST_CHECKS)
    else:
        raise ValueError("Invalid 'apply_checks_method' used for testing observable metrics.")

    checked_df.count()  # Trigger an action to get the metrics
    actual_metrics = observation.get
    assert actual_metrics == {
        "input_row_count": 4,
        "error_row_count": 1,
        "warning_row_count": 1,
        "valid_row_count": 2,
        "check_metrics": TEST_CHECK_METRICS_VALUE,
        "avg_error_age": 35.0,
        "total_warning_salary": 55000,
    }


@pytest.mark.parametrize(
    "apply_checks_method",
    [DQEngine.apply_checks_and_save_in_table, DQEngine.apply_checks_by_metadata_and_save_in_table],
)
def test_engine_without_observer_no_metrics_saved(ws, spark, make_schema, make_random, apply_checks_method):
    """Test that no metrics are saved when observer is not configured."""
    catalog_name = TEST_CATALOG
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
    output_config = OutputConfig(location=output_table_name, mode="overwrite")
    metrics_config = OutputConfig(location=metrics_table_name, mode="overwrite")

    if apply_checks_method == DQEngine.apply_checks_and_save_in_table:
        checks = deserialize_checks(TEST_CHECKS)
        dq_engine.apply_checks_and_save_in_table(
            checks=checks, input_config=input_config, output_config=output_config, metrics_config=metrics_config
        )
    elif apply_checks_method == DQEngine.apply_checks_by_metadata_and_save_in_table:
        dq_engine.apply_checks_by_metadata_and_save_in_table(
            checks=TEST_CHECKS, input_config=input_config, output_config=output_config, metrics_config=metrics_config
        )
    else:
        raise ValueError("Invalid 'apply_checks_method' used for testing observable metrics.")

    with pytest.raises(NotFound):
        ws.tables.get(full_name=metrics_table_name)


def test_save_summary_metrics(ws, spark, make_schema, make_random):
    schema_name = make_schema(catalog_name=TEST_CATALOG).name
    metrics_table_name = f"{TEST_CATALOG}.{schema_name}.metrics_{make_random(6).lower()}"

    observer_name = "test_observer"
    observer = DQMetricsObserver(name=observer_name)

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

    input_config = InputConfig(location="input_table")
    output_config = OutputConfig(location="output_table")
    quarantine_config = OutputConfig(location="quarantine_table")
    metrics_config = OutputConfig(location=metrics_table_name, mode="overwrite")
    checks_location = "checks_location"

    dq_engine.save_summary_metrics(
        observed_metrics=observation.get,
        metrics_config=metrics_config,
        input_config=input_config,
        output_config=output_config,
        quarantine_config=quarantine_config,
        checks_location=checks_location,
    )
    actual_metrics_df = spark.table(metrics_config.location).orderBy("metric_name")

    expected_metrics = [
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "rule_set_fingerprint": None,
            "metric_name": "check_metrics",
            "metric_value": TEST_CHECK_METRICS_VALUE,
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "rule_set_fingerprint": None,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "rule_set_fingerprint": None,
            "metric_name": "error_row_count",
            "metric_value": "1",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "rule_set_fingerprint": None,
            "metric_name": "warning_row_count",
            "metric_value": "1",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "rule_set_fingerprint": None,
            "metric_name": "valid_row_count",
            "metric_value": "2",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).orderBy(
        "metric_name"
    )

    assertDataFrameEqual(expected_metrics_df, actual_metrics_df)


def test_save_summary_metrics_custom_metrics_and_params(ws, spark_keep_alive, make_schema, make_random):
    spark = spark_keep_alive.spark
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    metrics_table_name = f"{catalog_name}.{schema_name}.metrics_{make_random(6).lower()}"

    observer_name = "test_observer"
    observer = DQMetricsObserver(
        name=observer_name,
        custom_metrics=[
            "avg(case when dq_errors is not null then age else null end) as avg_error_age",
            "sum(case when dq_warnings is not null then salary else null end) as total_warning_salary",
        ],
    )

    user_metadata = {"key1": "value1", "key2": "value2"}

    extra_params_custom = ExtraParams(
        run_time_overwrite=EXTRA_PARAMS.run_time_overwrite,
        result_column_names={"errors": "dq_errors", "warnings": "dq_warnings"},
        user_metadata=user_metadata,
        run_id_overwrite=EXTRA_PARAMS.run_id_overwrite,
    )

    dq_engine = DQEngine(workspace_client=ws, spark=spark, observer=observer, extra_params=extra_params_custom)

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

    input_config = InputConfig(location="input_table")
    output_config = OutputConfig(location="output_table")
    quarantine_config = OutputConfig(location="quarantine_table")
    metrics_config = OutputConfig(location=metrics_table_name, mode="overwrite")
    checks_location = "checks_location"

    dq_engine.save_summary_metrics(
        observed_metrics=observation.get,
        metrics_config=metrics_config,
        input_config=input_config,
        output_config=output_config,
        quarantine_config=quarantine_config,
        checks_location=checks_location,
    )

    expected_metrics = [
        {
            "run_id": extra_params_custom.run_id_overwrite,
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "rule_set_fingerprint": None,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_time": datetime.fromisoformat(extra_params_custom.run_time_overwrite),
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_id": extra_params_custom.run_id_overwrite,
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "rule_set_fingerprint": None,
            "metric_name": "error_row_count",
            "metric_value": "1",
            "run_time": datetime.fromisoformat(extra_params_custom.run_time_overwrite),
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_id": extra_params_custom.run_id_overwrite,
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "rule_set_fingerprint": None,
            "metric_name": "warning_row_count",
            "metric_value": "1",
            "run_time": datetime.fromisoformat(extra_params_custom.run_time_overwrite),
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_id": extra_params_custom.run_id_overwrite,
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "rule_set_fingerprint": None,
            "metric_name": "valid_row_count",
            "metric_value": "2",
            "run_time": datetime.fromisoformat(extra_params_custom.run_time_overwrite),
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_id": extra_params_custom.run_id_overwrite,
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "rule_set_fingerprint": None,
            "metric_name": "avg_error_age",
            "metric_value": "35.0",
            "run_time": datetime.fromisoformat(extra_params_custom.run_time_overwrite),
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_id": extra_params_custom.run_id_overwrite,
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "rule_set_fingerprint": None,
            "metric_name": "total_warning_salary",
            "metric_value": "55000",
            "run_time": datetime.fromisoformat(extra_params_custom.run_time_overwrite),
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_id": extra_params_custom.run_id_overwrite,
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "rule_set_fingerprint": None,
            "metric_name": "check_metrics",
            "metric_value": TEST_CHECK_METRICS_VALUE,
            "run_time": datetime.fromisoformat(extra_params_custom.run_time_overwrite),
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
    ]

    expected_metrics_df = spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).orderBy(
        "metric_name"
    )
    actual_metrics_df = spark.table(metrics_config.location).orderBy("metric_name")
    assertDataFrameEqual(expected_metrics_df, actual_metrics_df)


def test_save_summary_metrics_with_streaming_and_custom_params(ws, spark, make_schema, make_volume, make_random):
    schema_name = make_schema(catalog_name=TEST_CATALOG).name
    volume_name = make_volume(catalog_name=TEST_CATALOG, schema_name=schema_name).name

    input_config = InputConfig(location=f"{TEST_CATALOG}.{schema_name}.{make_random(6).lower()}", is_streaming=True)
    output_config = OutputConfig(
        location=f"{TEST_CATALOG}.{schema_name}.{make_random(6).lower()}",
        options={"checkPointLocation": f"/Volumes/{TEST_CATALOG}/{schema_name}/{volume_name}/{make_random(6).lower()}"},
        trigger={"availableNow": True},
    )
    quarantine_config = OutputConfig(
        location=f"{TEST_CATALOG}.{schema_name}.{make_random(6).lower()}",
        options={"checkPointLocation": f"/Volumes/{TEST_CATALOG}/{schema_name}/{volume_name}/{make_random(6).lower()}"},
        trigger={"availableNow": True},
    )
    metrics_config = OutputConfig(location=f"{TEST_CATALOG}.{schema_name}.{make_random(6).lower()}", mode="overwrite")

    user_metadata = {"key1": "value1", "key2": "value2"}
    dq_engine = DQEngine(
        workspace_client=ws,
        spark=spark,
        observer=DQMetricsObserver(
            name=TEST_OBSERVER_NAME,
            custom_metrics=[
                "avg(case when dq_errors is not null then age else null end) as avg_error_age",
                "sum(case when dq_warnings is not null then salary else null end) as total_warning_salary",
            ],
        ),
        extra_params=ExtraParams(
            run_time_overwrite=EXTRA_PARAMS.run_time_overwrite,
            result_column_names={"errors": "dq_errors", "warnings": "dq_warnings"},
            user_metadata=user_metadata,
            run_id_overwrite=EXTRA_PARAMS.run_id_overwrite,
        ),
    )

    test_df = spark.createDataFrame(
        [
            [1, "Alice", 30, 50000],
            [2, "Bob", 25, 45000],
            [None, "Charlie", 35, 60000],
            [4, None, 28, 55000],
        ],
        TEST_SCHEMA,
    )
    test_df.write.mode("overwrite").saveAsTable(input_config.location)

    input_df = spark.readStream.table(input_config.location)
    valid_df, quarantine_df, _ = dq_engine.apply_checks_by_metadata_and_split(input_df, TEST_CHECKS)

    output_query = (
        valid_df.writeStream.format(output_config.format)
        .outputMode(output_config.mode)
        .options(**output_config.options)
        .trigger(**output_config.trigger)
        .toTable(output_config.location)
    )
    quarantine_query = (
        quarantine_df.writeStream.format(quarantine_config.format)
        .outputMode(quarantine_config.mode)
        .options(**quarantine_config.options)
        .trigger(**quarantine_config.trigger)
        .toTable(quarantine_config.location)
    )

    checks_location = "fake_location"
    listener = dq_engine.get_streaming_metrics_listener(
        input_config=input_config,
        output_config=output_config,
        quarantine_config=quarantine_config,
        metrics_config=metrics_config,
        target_query_id=quarantine_query.id,
        checks_location=checks_location,
    )
    spark.streams.addListener(listener)

    output_query.awaitTermination()
    quarantine_query.awaitTermination()
    time.sleep(30)

    expected_metrics = [
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": TEST_OBSERVER_NAME,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "rule_set_fingerprint": None,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": TEST_OBSERVER_NAME,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "rule_set_fingerprint": None,
            "metric_name": "error_row_count",
            "metric_value": "1",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": TEST_OBSERVER_NAME,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "rule_set_fingerprint": None,
            "metric_name": "warning_row_count",
            "metric_value": "1",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": TEST_OBSERVER_NAME,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "rule_set_fingerprint": None,
            "metric_name": "valid_row_count",
            "metric_value": "2",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": TEST_OBSERVER_NAME,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "rule_set_fingerprint": None,
            "metric_name": "avg_error_age",
            "metric_value": "35.0",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": TEST_OBSERVER_NAME,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "rule_set_fingerprint": None,
            "metric_name": "total_warning_salary",
            "metric_value": "55000",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": TEST_OBSERVER_NAME,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "rule_set_fingerprint": None,
            "metric_name": "check_metrics",
            "metric_value": TEST_CHECK_METRICS_VALUE,
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
    ]

    expected_metrics_df = (
        spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).drop("run_time").orderBy("metric_name")
    )
    actual_metrics_df = spark.table(metrics_config.location).drop("run_time").orderBy("metric_name")
    assertDataFrameEqual(expected_metrics_df, actual_metrics_df)


@pytest.mark.parametrize(
    "apply_checks_method",
    [DQEngine.apply_checks_and_save_in_table, DQEngine.apply_checks_by_metadata_and_save_in_table],
)
def test_observer_metrics_output_with_empty_checks(
    skip_if_classic_compute, apply_checks_method, spark, ws, make_schema, make_random
):
    # NOTE: This test is skipped during the 'integration' workflow. Data quality summary metrics are not supported on classic compute in Dedicated access mode for DBR versions < 17.
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    input_table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    output_table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    metrics_table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

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
    output_config = OutputConfig(location=output_table_name, mode="overwrite")
    metrics_config = OutputConfig(location=metrics_table_name, mode="overwrite")

    if apply_checks_method == DQEngine.apply_checks_and_save_in_table:
        dq_engine.apply_checks_and_save_in_table(
            checks=[], input_config=input_config, output_config=output_config, metrics_config=metrics_config
        )
    elif apply_checks_method == DQEngine.apply_checks_by_metadata_and_save_in_table:
        dq_engine.apply_checks_by_metadata_and_save_in_table(
            checks=[], input_config=input_config, output_config=output_config, metrics_config=metrics_config
        )
    else:
        raise ValueError("Invalid 'apply_checks_method' used for testing observable metrics.")

    expected_metrics = [
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": None,
            "rule_set_fingerprint": None,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": None,
            "rule_set_fingerprint": None,
            "metric_name": "error_row_count",
            "metric_value": "0",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": None,
            "rule_set_fingerprint": None,
            "metric_name": "warning_row_count",
            "metric_value": "0",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": None,
            "rule_set_fingerprint": None,
            "metric_name": "valid_row_count",
            "metric_value": "4",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": None,
            "rule_set_fingerprint": None,
            "metric_name": "avg_error_age",
            "metric_value": None,
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": None,
            "rule_set_fingerprint": None,
            "metric_name": "total_warning_salary",
            "metric_value": None,
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).orderBy(
        "metric_name"
    )
    actual_metrics_df = spark.table(metrics_table_name).orderBy("metric_name")

    assertDataFrameEqual(expected_metrics_df, actual_metrics_df)
    assert (
        spark.table(output_config.location).count() == 4
    ), f"Output table {output_config.location} has {spark.table(output_config.location).count()} rows"


@pytest.mark.parametrize(
    "apply_checks_method",
    [DQEngine.apply_checks_and_save_in_table, DQEngine.apply_checks_by_metadata_and_save_in_table],
)
def test_observer_metrics_output_with_quarantine_with_empty_checks(
    skip_if_classic_compute, apply_checks_method, spark, ws, make_schema, make_random
):
    # NOTE: This test is skipped during the 'integration' workflow. Data quality summary metrics are not supported on classic compute in Dedicated access mode for DBR versions < 17.
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    input_table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    output_table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    quarantine_table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    metrics_table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

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
    output_config = OutputConfig(location=output_table_name, mode="overwrite")
    quarantine_config = OutputConfig(location=quarantine_table_name, mode="overwrite")
    metrics_config = OutputConfig(location=metrics_table_name, mode="overwrite")

    if apply_checks_method == DQEngine.apply_checks_and_save_in_table:
        dq_engine.apply_checks_and_save_in_table(
            checks=[],
            input_config=input_config,
            output_config=output_config,
            quarantine_config=quarantine_config,
            metrics_config=metrics_config,
        )
    elif apply_checks_method == DQEngine.apply_checks_by_metadata_and_save_in_table:
        dq_engine.apply_checks_by_metadata_and_save_in_table(
            checks=[],
            input_config=input_config,
            output_config=output_config,
            quarantine_config=quarantine_config,
            metrics_config=metrics_config,
        )
    else:
        raise ValueError("Invalid 'apply_checks_method' used for testing observable metrics.")

    expected_metrics = [
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "rule_set_fingerprint": None,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "rule_set_fingerprint": None,
            "metric_name": "error_row_count",
            "metric_value": "0",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "rule_set_fingerprint": None,
            "metric_name": "warning_row_count",
            "metric_value": "0",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "rule_set_fingerprint": None,
            "metric_name": "valid_row_count",
            "metric_value": "4",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "rule_set_fingerprint": None,
            "metric_name": "avg_error_age",
            "metric_value": None,
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "rule_set_fingerprint": None,
            "metric_name": "total_warning_salary",
            "metric_value": None,
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).orderBy(
        "metric_name"
    )
    actual_metrics_df = spark.table(metrics_table_name).orderBy("metric_name")

    assertDataFrameEqual(expected_metrics_df, actual_metrics_df)
    assert spark.table(output_config.location).count() == 4
    assert spark.table(quarantine_config.location).count() == 0


@pytest.mark.parametrize(
    "apply_checks_method",
    [DQEngine.apply_checks_and_save_in_table, DQEngine.apply_checks_by_metadata_and_save_in_table],
)
def test_observer_metrics_output(skip_if_classic_compute, apply_checks_method, spark, ws, make_schema, make_random):
    # NOTE: This test is skipped during the 'integration' workflow. Data quality summary metrics are not supported on classic compute in Dedicated access mode for DBR versions < 17.
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    input_table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    output_table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    metrics_table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

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
    output_config = OutputConfig(location=output_table_name, mode="overwrite")
    metrics_config = OutputConfig(location=metrics_table_name, mode="overwrite")
    checks_location = "fake.yml"

    if apply_checks_method == DQEngine.apply_checks_and_save_in_table:
        checks = deserialize_checks(TEST_CHECKS)
        dq_engine.apply_checks_and_save_in_table(
            checks=checks,
            input_config=input_config,
            output_config=output_config,
            metrics_config=metrics_config,
            checks_location=checks_location,
        )
    elif apply_checks_method == DQEngine.apply_checks_by_metadata_and_save_in_table:
        dq_engine.apply_checks_by_metadata_and_save_in_table(
            checks=TEST_CHECKS,
            input_config=input_config,
            output_config=output_config,
            metrics_config=metrics_config,
            checks_location=checks_location,
        )
    else:
        raise ValueError("Invalid 'apply_checks_method' used for testing observable metrics.")

    expected_metrics = [
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "error_row_count",
            "metric_value": "1",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "warning_row_count",
            "metric_value": "1",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "valid_row_count",
            "metric_value": "2",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "avg_error_age",
            "metric_value": "35.0",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "total_warning_salary",
            "metric_value": "55000",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "check_metrics",
            "metric_value": TEST_CHECK_METRICS_VALUE,
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).orderBy(
        "metric_name"
    )
    actual_metrics_df = spark.table(metrics_table_name).orderBy("metric_name")

    assertDataFrameEqual(expected_metrics_df, actual_metrics_df)
    assert (
        spark.table(output_config.location).count() == 4
    ), f"Output table {output_config.location} has {spark.table(output_config.location).count()} rows"


@pytest.mark.parametrize(
    "apply_checks_method",
    [DQEngine.apply_checks_and_save_in_table, DQEngine.apply_checks_by_metadata_and_save_in_table],
)
def test_observer_metrics_output_with_quarantine(
    skip_if_classic_compute, apply_checks_method, spark, ws, make_schema, make_random
):
    # NOTE: This test is skipped during the 'integration' workflow. Data quality summary metrics are not supported on classic compute in Dedicated access mode for DBR versions < 17.
    schema_name = make_schema(catalog_name=TEST_CATALOG).name
    input_table_name = f"{TEST_CATALOG}.{schema_name}.{make_random(6).lower()}"
    output_table_name = f"{TEST_CATALOG}.{schema_name}.{make_random(6).lower()}"
    quarantine_table_name = f"{TEST_CATALOG}.{schema_name}.{make_random(6).lower()}"
    metrics_table_name = f"{TEST_CATALOG}.{schema_name}.{make_random(6).lower()}"

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
    output_config = OutputConfig(location=output_table_name, mode="overwrite")
    quarantine_config = OutputConfig(location=quarantine_table_name, mode="overwrite")
    metrics_config = OutputConfig(location=metrics_table_name, mode="overwrite")
    checks_location = "fake.yml"

    if apply_checks_method == DQEngine.apply_checks_and_save_in_table:
        checks = deserialize_checks(TEST_CHECKS)
        dq_engine.apply_checks_and_save_in_table(
            checks=checks,
            input_config=input_config,
            output_config=output_config,
            quarantine_config=quarantine_config,
            metrics_config=metrics_config,
            checks_location=checks_location,
        )
    elif apply_checks_method == DQEngine.apply_checks_by_metadata_and_save_in_table:
        dq_engine.apply_checks_by_metadata_and_save_in_table(
            checks=TEST_CHECKS,
            input_config=input_config,
            output_config=output_config,
            quarantine_config=quarantine_config,
            metrics_config=metrics_config,
            checks_location=checks_location,
        )
    else:
        raise ValueError("Invalid 'apply_checks_method' used for testing observable metrics.")

    expected_metrics = [
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "error_row_count",
            "metric_value": "1",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "warning_row_count",
            "metric_value": "1",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "valid_row_count",
            "metric_value": "2",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "avg_error_age",
            "metric_value": "35.0",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "total_warning_salary",
            "metric_value": "55000",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "check_metrics",
            "metric_value": TEST_CHECK_METRICS_VALUE,
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).orderBy(
        "metric_name"
    )
    actual_metrics_df = spark.table(metrics_table_name).orderBy("metric_name")
    assertDataFrameEqual(expected_metrics_df, actual_metrics_df)
    assert (
        spark.table(output_config.location).count() == 3
    ), f"Output table {output_config.location} has {spark.table(output_config.location).count()} rows"
    assert (
        spark.table(quarantine_config.location).count() == 2
    ), f"Quarantine table {quarantine_config.location} has {spark.table(quarantine_config.location).count()} rows"


@pytest.mark.parametrize(
    "apply_checks_method",
    [DQEngine.apply_checks_and_split, DQEngine.apply_checks_by_metadata_and_split],
)
def test_save_results_in_table_batch_with_metrics(
    skip_if_classic_compute, apply_checks_method, spark, ws, make_schema, make_random
):
    # NOTE: This test is skipped during the 'integration' workflow. Data quality summary metrics are not supported on classic compute in Dedicated access mode for DBR versions < 17.
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    output_table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    quarantine_table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    metrics_table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    observer = DQMetricsObserver(name="test_save_batch_observer")
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

    if apply_checks_method == DQEngine.apply_checks_and_split:
        checks = deserialize_checks(TEST_CHECKS)
        output_df, quarantine_df, observation = dq_engine.apply_checks_and_split(test_df, checks)
    elif apply_checks_method == DQEngine.apply_checks_by_metadata_and_split:
        output_df, quarantine_df, observation = dq_engine.apply_checks_by_metadata_and_split(test_df, TEST_CHECKS)
    else:
        raise ValueError("Invalid 'apply_checks_method' used for testing observable metrics.")

    output_config = OutputConfig(location=output_table_name)
    quarantine_config = OutputConfig(location=quarantine_table_name, mode="overwrite")
    metrics_config = OutputConfig(location=metrics_table_name, mode="overwrite")

    dq_engine.save_results_in_table(
        output_df=output_df,
        quarantine_df=quarantine_df,
        observation=observation,
        output_config=output_config,
        quarantine_config=quarantine_config,
        metrics_config=metrics_config,
        rule_set_fingerprint=TEST_CHECKS_RULE_SET_FINGERPRINT,
    )

    expected_metrics = [
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_save_batch_observer",
            "input_location": None,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_save_batch_observer",
            "input_location": None,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "error_row_count",
            "metric_value": "1",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_save_batch_observer",
            "input_location": None,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "warning_row_count",
            "metric_value": "1",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_save_batch_observer",
            "input_location": None,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "valid_row_count",
            "metric_value": "2",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_save_batch_observer",
            "input_location": None,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "check_metrics",
            "metric_value": TEST_CHECK_METRICS_VALUE,
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).orderBy(
        "metric_name"
    )
    actual_metrics_df = spark.table(metrics_table_name).orderBy("metric_name")
    assertDataFrameEqual(expected_metrics_df, actual_metrics_df)
    assert (
        spark.table(output_config.location).count() == 3
    ), f"Output table {output_config.location} has {spark.table(output_config.location).count()} rows"
    assert (
        spark.table(quarantine_config.location).count() == 2
    ), f"Quarantine table {quarantine_config.location} has {spark.table(quarantine_config.location).count()} rows"


def test_save_results_in_table_batch_with_rule_set_fingerprint(
    skip_if_classic_compute, spark, ws, make_schema, make_random
):
    """Verify that rule_set_fingerprint passed to save_results_in_table is written to the metrics table."""
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    output_table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    quarantine_table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    metrics_table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    observer = DQMetricsObserver(name="test_save_batch_observer")
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

    output_df, quarantine_df, observation = dq_engine.apply_checks_by_metadata_and_split(test_df, TEST_CHECKS)

    output_config = OutputConfig(location=output_table_name)
    quarantine_config = OutputConfig(location=quarantine_table_name, mode="overwrite")
    metrics_config = OutputConfig(location=metrics_table_name, mode="overwrite")

    rule_set_fingerprint = "abc123def456789012345678901234567890123456789012345678901234abcd"

    dq_engine.save_results_in_table(
        output_df=output_df,
        quarantine_df=quarantine_df,
        observation=observation,
        output_config=output_config,
        quarantine_config=quarantine_config,
        metrics_config=metrics_config,
        rule_set_fingerprint=rule_set_fingerprint,
    )

    actual_metrics_df = spark.table(metrics_table_name)
    actual_fingerprints = actual_metrics_df.select("rule_set_fingerprint").distinct().collect()
    assert len(actual_fingerprints) == 1
    assert actual_fingerprints[0]["rule_set_fingerprint"] == rule_set_fingerprint


def test_save_summary_metrics_with_rule_set_fingerprint(ws, spark, make_schema, make_random):
    """Verify that rule_set_fingerprint passed to save_summary_metrics is written to the metrics table."""
    schema_name = make_schema(catalog_name=TEST_CATALOG).name
    metrics_table_name = f"{TEST_CATALOG}.{schema_name}.metrics_{make_random(6).lower()}"

    observer = DQMetricsObserver(name="test_observer")
    dq_engine = DQEngine(workspace_client=ws, spark=spark, observer=observer, extra_params=EXTRA_PARAMS)

    test_df = spark.createDataFrame(
        [[1, "Alice", 30, 50000], [2, "Bob", 25, 45000]],
        TEST_SCHEMA,
    )
    checked_df, observation = dq_engine.apply_checks_by_metadata(test_df, TEST_CHECKS)
    checked_df.count()

    metrics_config = OutputConfig(location=metrics_table_name, mode="overwrite")
    rule_set_fingerprint = "fingerprint_sha256_64chars_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

    dq_engine.save_summary_metrics(
        observed_metrics=observation.get,
        metrics_config=metrics_config,
        rule_set_fingerprint=rule_set_fingerprint,
    )

    actual_metrics_df = spark.table(metrics_table_name)
    actual_fingerprints = actual_metrics_df.select("rule_set_fingerprint").distinct().collect()
    assert len(actual_fingerprints) == 1
    assert actual_fingerprints[0]["rule_set_fingerprint"] == rule_set_fingerprint


@pytest.mark.parametrize(
    "apply_checks_method",
    [DQEngine.apply_checks_and_save_in_table, DQEngine.apply_checks_by_metadata_and_save_in_table],
)
def test_apply_checks_and_save_in_table_writes_rule_set_fingerprint(
    skip_if_classic_compute, apply_checks_method, spark, ws, make_schema, make_random
):
    """Verify that apply_checks_and_save_in_table / apply_checks_by_metadata_and_save_in_table write non-null rule_set_fingerprint."""
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    input_table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    output_table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    metrics_table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    observer = DQMetricsObserver(name="test_observer")
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
    output_config = OutputConfig(location=output_table_name, mode="overwrite")
    metrics_config = OutputConfig(location=metrics_table_name, mode="overwrite")

    expected_fingerprint = compute_rule_set_fingerprint_by_metadata(TEST_CHECKS)

    if apply_checks_method == DQEngine.apply_checks_and_save_in_table:
        checks = deserialize_checks(TEST_CHECKS)
        dq_engine.apply_checks_and_save_in_table(
            checks=checks,
            input_config=input_config,
            output_config=output_config,
            metrics_config=metrics_config,
        )
    else:
        dq_engine.apply_checks_by_metadata_and_save_in_table(
            checks=TEST_CHECKS,
            input_config=input_config,
            output_config=output_config,
            metrics_config=metrics_config,
        )

    actual_metrics_df = spark.table(metrics_table_name)
    actual_fingerprints = actual_metrics_df.select("rule_set_fingerprint").distinct().collect()
    assert len(actual_fingerprints) == 1
    assert actual_fingerprints[0]["rule_set_fingerprint"] is not None
    assert actual_fingerprints[0]["rule_set_fingerprint"] == expected_fingerprint


@pytest.mark.parametrize(
    "apply_checks_method",
    [DQEngine.apply_checks_and_split, DQEngine.apply_checks_by_metadata_and_split],
)
def test_save_results_in_table_streaming_with_metrics(
    skip_if_classic_compute, apply_checks_method, spark, ws, make_schema, make_volume, make_random
):
    schema_name = make_schema(catalog_name=TEST_CATALOG).name
    volume_name = make_volume(catalog_name=TEST_CATALOG, schema_name=schema_name).name

    input_table_name = f"{TEST_CATALOG}.{schema_name}.{make_random(6).lower()}"
    metrics_table_name = f"{TEST_CATALOG}.{schema_name}.{make_random(6).lower()}"

    observer = DQMetricsObserver(name="test_save_batch_observer")
    dq_engine = DQEngine(workspace_client=ws, spark=spark, observer=observer, extra_params=EXTRA_PARAMS)

    input_df = spark.createDataFrame(
        [
            [1, "Alice", 30, 50000],
            [2, "Bob", 25, 45000],
            [None, "Charlie", 35, 60000],  # This will trigger an error
            [4, None, 28, 55000],  # This will trigger a warning
        ],
        TEST_SCHEMA,
    )
    input_df.write.format("delta").saveAsTable(input_table_name)
    test_df = spark.readStream.table(input_table_name)

    if apply_checks_method == DQEngine.apply_checks_and_split:
        checks = deserialize_checks(TEST_CHECKS)
        output_df, quarantine_df, _ = dq_engine.apply_checks_and_split(test_df, checks)
    elif apply_checks_method == DQEngine.apply_checks_by_metadata_and_split:
        output_df, quarantine_df, _ = dq_engine.apply_checks_by_metadata_and_split(test_df, TEST_CHECKS)
    else:
        raise ValueError("Invalid 'apply_checks_method' used for testing observable metrics.")

    output_config = OutputConfig(
        location=f"{TEST_CATALOG}.{schema_name}.{make_random(6).lower()}",
        options={"checkPointLocation": f"/Volumes/{TEST_CATALOG}/{schema_name}/{volume_name}/{make_random(6).lower()}"},
        trigger={"availableNow": True},
    )
    quarantine_config = OutputConfig(
        location=f"{TEST_CATALOG}.{schema_name}.{make_random(6).lower()}",
        options={
            "checkPointLocation": f"/Volumes/{TEST_CATALOG}/{schema_name}/{volume_name}/quarantine_{make_random(6).lower()}"
        },
        trigger={"availableNow": True},
    )
    metrics_config = OutputConfig(location=metrics_table_name, mode="overwrite")

    dq_engine.save_results_in_table(
        output_df=output_df,
        quarantine_df=quarantine_df,
        output_config=output_config,
        quarantine_config=quarantine_config,
        metrics_config=metrics_config,
        rule_set_fingerprint=TEST_CHECKS_RULE_SET_FINGERPRINT,
    )

    time.sleep(30)
    expected_metrics = [
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_save_batch_observer",
            "input_location": None,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": None,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_save_batch_observer",
            "input_location": None,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": None,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "error_row_count",
            "metric_value": "1",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_save_batch_observer",
            "input_location": None,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": None,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "warning_row_count",
            "metric_value": "1",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_save_batch_observer",
            "input_location": None,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": None,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "valid_row_count",
            "metric_value": "2",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_save_batch_observer",
            "input_location": None,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": None,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "check_metrics",
            "metric_value": TEST_CHECK_METRICS_VALUE,
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = (
        spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).drop("run_time").orderBy("metric_name")
    )
    actual_metrics_df = spark.table(metrics_table_name).drop("run_time").orderBy("metric_name")
    assertDataFrameEqual(expected_metrics_df, actual_metrics_df)
    assert (
        spark.table(output_config.location).count() == 3
    ), f"Output table {output_config.location} has {spark.table(output_config.location).count()} rows"
    assert (
        spark.table(quarantine_config.location).count() == 2
    ), f"Quarantine table {quarantine_config.location} has {spark.table(quarantine_config.location).count()} rows"


@pytest.mark.parametrize(
    "apply_checks_method",
    [DQEngine.apply_checks_and_save_in_table, DQEngine.apply_checks_by_metadata_and_save_in_table],
)
def test_streaming_observer_metrics_output(apply_checks_method, spark, ws, make_schema, make_volume, make_random):
    schema_name = make_schema(catalog_name=TEST_CATALOG).name
    volume_name = make_volume(catalog_name=TEST_CATALOG, schema_name=schema_name).name

    metrics_table_name = f"{TEST_CATALOG}.{schema_name}.{make_random(6).lower()}"
    checkpoint_location = f"/Volumes/{TEST_CATALOG}/{schema_name}/{volume_name}/{make_random(6).lower()}"

    dq_engine = DQEngine(
        workspace_client=ws,
        spark=spark,
        observer=DQMetricsObserver(
            name="test_streaming_observer",
            custom_metrics=[
                "avg(case when _errors is not null then age else null end) as avg_error_age",
                "sum(case when _warnings is not null then salary else null end) as total_warning_salary",
            ],
        ),
        extra_params=EXTRA_PARAMS,
    )

    input_config = InputConfig(location=f"{TEST_CATALOG}.{schema_name}.{make_random(6).lower()}", is_streaming=True)
    output_config = OutputConfig(
        location=f"{TEST_CATALOG}.{schema_name}.{make_random(6).lower()}",
        options={"checkPointLocation": checkpoint_location},
        trigger={"availableNow": True},
    )
    metrics_config = OutputConfig(location=metrics_table_name)
    checks_location = "fake.yml"

    test_df = spark.createDataFrame(
        [
            [1, "Alice", 30, 50000],
            [2, "Bob", 25, 45000],
            [None, "Charlie", 35, 60000],
            [4, None, 28, 55000],
        ],
        TEST_SCHEMA,
    )

    test_df.write.mode("overwrite").saveAsTable(input_config.location)

    if apply_checks_method == DQEngine.apply_checks_and_save_in_table:
        checks = deserialize_checks(TEST_CHECKS)
        dq_engine.apply_checks_and_save_in_table(
            checks=checks,
            input_config=input_config,
            output_config=output_config,
            metrics_config=metrics_config,
            checks_location=checks_location,
        )
    elif apply_checks_method == DQEngine.apply_checks_by_metadata_and_save_in_table:
        dq_engine.apply_checks_by_metadata_and_save_in_table(
            checks=TEST_CHECKS,
            input_config=input_config,
            output_config=output_config,
            metrics_config=metrics_config,
            checks_location=checks_location,
        )
    else:
        raise ValueError("Invalid 'apply_checks_method' used for testing observable metrics.")

    time.sleep(30)

    actual_metrics_df = spark.table(metrics_table_name).orderBy("metric_name")
    expected_metrics = [
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_streaming_observer",
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": None,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_streaming_observer",
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": None,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "error_row_count",
            "metric_value": "1",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_streaming_observer",
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": None,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "warning_row_count",
            "metric_value": "1",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_streaming_observer",
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": None,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "valid_row_count",
            "metric_value": "2",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_streaming_observer",
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": None,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "avg_error_age",
            "metric_value": "35.0",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_streaming_observer",
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": None,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "total_warning_salary",
            "metric_value": "55000",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_streaming_observer",
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": None,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "check_metrics",
            "metric_value": TEST_CHECK_METRICS_VALUE,
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).orderBy(
        "metric_name"
    )

    assertDataFrameEqual(expected_metrics_df, actual_metrics_df)
    assert (
        spark.table(output_config.location).count() == 4
    ), f"Output table {output_config.location} has {spark.table(output_config.location).count()} rows"


@pytest.mark.parametrize(
    "apply_checks_method",
    [DQEngine.apply_checks_and_save_in_table, DQEngine.apply_checks_by_metadata_and_save_in_table],
)
def test_streaming_observer_metrics_output_and_quarantine(
    apply_checks_method, spark, ws, make_schema, make_volume, make_random
):
    schema_name = make_schema(catalog_name=TEST_CATALOG).name
    volume_name = make_volume(catalog_name=TEST_CATALOG, schema_name=schema_name).name

    input_table_name = f"{TEST_CATALOG}.{schema_name}.{make_random(6).lower()}"
    output_table_name = f"{TEST_CATALOG}.{schema_name}.{make_random(6).lower()}"
    quarantine_table_name = f"{TEST_CATALOG}.{schema_name}.{make_random(6).lower()}"
    metrics_table_name = f"{TEST_CATALOG}.{schema_name}.{make_random(6).lower()}"

    observer = DQMetricsObserver(
        name="test_streaming_observer_with_quarantine",
        custom_metrics=[
            "avg(case when _errors is not null then age else null end) as avg_error_age",
            "sum(case when _warnings is not null then salary else null end) as total_warning_salary",
        ],
    )
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
        options={"checkPointLocation": f"/Volumes/{TEST_CATALOG}/{schema_name}/{volume_name}/{make_random(6).lower()}"},
        trigger={"availableNow": True},
    )
    quarantine_config = OutputConfig(
        location=quarantine_table_name,
        options={"checkPointLocation": f"/Volumes/{TEST_CATALOG}/{schema_name}/{volume_name}/{make_random(6).lower()}"},
        trigger={"availableNow": True},
    )
    metrics_config = OutputConfig(location=metrics_table_name)
    checks_location = "fake.yml"

    if apply_checks_method == DQEngine.apply_checks_and_save_in_table:
        checks = deserialize_checks(TEST_CHECKS)
        dq_engine.apply_checks_and_save_in_table(
            checks=checks,
            input_config=input_config,
            output_config=output_config,
            quarantine_config=quarantine_config,
            metrics_config=metrics_config,
            checks_location=checks_location,
        )
    elif apply_checks_method == DQEngine.apply_checks_by_metadata_and_save_in_table:
        dq_engine.apply_checks_by_metadata_and_save_in_table(
            checks=TEST_CHECKS,
            input_config=input_config,
            output_config=output_config,
            quarantine_config=quarantine_config,
            metrics_config=metrics_config,
            checks_location=checks_location,
        )
    else:
        raise ValueError("Invalid 'apply_checks_method' used for testing observable metrics.")

    time.sleep(30)
    expected_metrics = [
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_streaming_observer_with_quarantine",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_streaming_observer_with_quarantine",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "error_row_count",
            "metric_value": "1",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_streaming_observer_with_quarantine",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "warning_row_count",
            "metric_value": "1",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_streaming_observer_with_quarantine",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "valid_row_count",
            "metric_value": "2",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_streaming_observer_with_quarantine",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "avg_error_age",
            "metric_value": "35.0",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_streaming_observer_with_quarantine",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "total_warning_salary",
            "metric_value": "55000",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_streaming_observer_with_quarantine",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": checks_location,
            "rule_set_fingerprint": TEST_CHECKS_RULE_SET_FINGERPRINT,
            "metric_name": "check_metrics",
            "metric_value": TEST_CHECK_METRICS_VALUE,
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = (
        spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).drop("run_time").orderBy("metric_name")
    )
    actual_metrics_df = spark.table(metrics_table_name).drop("run_time").orderBy("metric_name")
    assertDataFrameEqual(expected_metrics_df, actual_metrics_df)
    assert (
        spark.table(output_config.location).count() == 3
    ), f"Output table {output_config.location} has {spark.table(output_config.location).count()} rows"
    assert (
        spark.table(quarantine_config.location).count() == 2
    ), f"Quarantine table {quarantine_config.location} has {spark.table(quarantine_config.location).count()} rows"


@pytest.mark.parametrize(
    "apply_checks_method",
    [DQEngine.apply_checks_and_save_in_table, DQEngine.apply_checks_by_metadata_and_save_in_table],
)
def test_streaming_observer_metrics_output_with_empty_checks(
    apply_checks_method, spark, ws, make_schema, make_volume, make_random
):
    schema_name = make_schema(catalog_name=TEST_CATALOG).name
    volume_name = make_volume(catalog_name=TEST_CATALOG, schema_name=schema_name).name

    input_table_name = f"{TEST_CATALOG}.{schema_name}.{make_random(6).lower()}"
    output_table_name = f"{TEST_CATALOG}.{schema_name}.{make_random(6).lower()}"
    metrics_table_name = f"{TEST_CATALOG}.{schema_name}.{make_random(6).lower()}"

    dq_engine = DQEngine(
        workspace_client=ws,
        spark=spark,
        observer=DQMetricsObserver(
            name="test_streaming_observer",
            custom_metrics=[
                "avg(case when _errors is not null then age else null end) as avg_error_age",
                "sum(case when _warnings is not null then salary else null end) as total_warning_salary",
            ],
        ),
        extra_params=EXTRA_PARAMS,
    )

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
        options={"checkPointLocation": f"/Volumes/{TEST_CATALOG}/{schema_name}/{volume_name}/{make_random(6).lower()}"},
        trigger={"availableNow": True},
    )
    metrics_config = OutputConfig(location=metrics_table_name)

    if apply_checks_method == DQEngine.apply_checks_and_save_in_table:
        dq_engine.apply_checks_and_save_in_table(
            checks=[], input_config=input_config, output_config=output_config, metrics_config=metrics_config
        )
    elif apply_checks_method == DQEngine.apply_checks_by_metadata_and_save_in_table:
        dq_engine.apply_checks_by_metadata_and_save_in_table(
            checks=[], input_config=input_config, output_config=output_config, metrics_config=metrics_config
        )
    else:
        raise ValueError("Invalid 'apply_checks_method' used for testing observable metrics.")

    time.sleep(30)
    expected_metrics = [
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": None,
            "rule_set_fingerprint": None,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": None,
            "rule_set_fingerprint": None,
            "metric_name": "error_row_count",
            "metric_value": "0",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": None,
            "rule_set_fingerprint": None,
            "metric_name": "warning_row_count",
            "metric_value": "0",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": None,
            "rule_set_fingerprint": None,
            "metric_name": "valid_row_count",
            "metric_value": "4",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": None,
            "rule_set_fingerprint": None,
            "metric_name": "avg_error_age",
            "metric_value": None,
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": None,
            "rule_set_fingerprint": None,
            "metric_name": "total_warning_salary",
            "metric_value": None,
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = (
        spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).drop("run_time").orderBy("metric_name")
    )
    actual_metrics_df = spark.table(metrics_table_name).drop("run_time").orderBy("metric_name")
    assertDataFrameEqual(expected_metrics_df, actual_metrics_df)
    assert (
        spark.table(output_config.location).count() == 4
    ), f"Output table {output_config.location} has {spark.table(output_config.location).count()} rows"


@pytest.mark.parametrize(
    "apply_checks_method",
    [DQEngine.apply_checks_and_save_in_table, DQEngine.apply_checks_by_metadata_and_save_in_table],
)
def test_streaming_observer_metrics_output_and_quarantine_with_empty_checks(
    apply_checks_method, spark_keep_alive, ws, make_schema, make_volume, make_random
):
    spark = spark_keep_alive.spark
    schema_name = make_schema(catalog_name=TEST_CATALOG).name
    volume_name = make_volume(catalog_name=TEST_CATALOG, schema_name=schema_name).name

    input_table_name = f"{TEST_CATALOG}.{schema_name}.{make_random(6).lower()}"
    output_table_name = f"{TEST_CATALOG}.{schema_name}.{make_random(6).lower()}"
    quarantine_table_name = f"{TEST_CATALOG}.{schema_name}.{make_random(6).lower()}"
    metrics_table_name = f"{TEST_CATALOG}.{schema_name}.{make_random(6).lower()}"

    observer = DQMetricsObserver(
        name="test_streaming_observer",
        custom_metrics=[
            "avg(case when dq_errors is not null then age else null end) as avg_error_age",
            "sum(case when dq_warnings is not null then salary else null end) as total_warning_salary",
        ],
    )
    user_metadata = {"key1": "value1", "key2": "value2"}
    extra_params_custom = ExtraParams(
        run_time_overwrite=EXTRA_PARAMS.run_time_overwrite,
        result_column_names={"errors": "dq_errors", "warnings": "dq_warnings"},
        user_metadata=user_metadata,
        run_id_overwrite=EXTRA_PARAMS.run_id_overwrite,
    )
    dq_engine = DQEngine(workspace_client=ws, spark=spark, observer=observer, extra_params=extra_params_custom)

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
        options={"checkPointLocation": f"/Volumes/{TEST_CATALOG}/{schema_name}/{volume_name}/{make_random(6).lower()}"},
        trigger={"availableNow": True},
    )
    quarantine_config = OutputConfig(
        location=quarantine_table_name,
        options={"checkPointLocation": f"/Volumes/{TEST_CATALOG}/{schema_name}/{volume_name}/{make_random(6).lower()}"},
        trigger={"availableNow": True},
    )
    metrics_config = OutputConfig(location=metrics_table_name, mode="overwrite")

    if apply_checks_method == DQEngine.apply_checks_and_save_in_table:
        dq_engine.apply_checks_and_save_in_table(
            checks=[],
            input_config=input_config,
            output_config=output_config,
            quarantine_config=quarantine_config,
            metrics_config=metrics_config,
        )
    elif apply_checks_method == DQEngine.apply_checks_by_metadata_and_save_in_table:
        dq_engine.apply_checks_by_metadata_and_save_in_table(
            checks=[],
            input_config=input_config,
            output_config=output_config,
            quarantine_config=quarantine_config,
            metrics_config=metrics_config,
        )
    else:
        raise ValueError("Invalid 'apply_checks_method' used for testing observable metrics.")

    time.sleep(30)
    expected_metrics = [
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "rule_set_fingerprint": None,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "rule_set_fingerprint": None,
            "metric_name": "error_row_count",
            "metric_value": "0",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "rule_set_fingerprint": None,
            "metric_name": "warning_row_count",
            "metric_value": "0",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "rule_set_fingerprint": None,
            "metric_name": "valid_row_count",
            "metric_value": "4",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "rule_set_fingerprint": None,
            "metric_name": "avg_error_age",
            "metric_value": None,
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "rule_set_fingerprint": None,
            "metric_name": "total_warning_salary",
            "metric_value": None,
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
    ]

    assertDataFrameEqual(
        spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA)
        .drop("run_time")
        .orderBy("metric_name"),
        spark.table(metrics_table_name).drop("run_time").orderBy("metric_name"),
    )
    assert (
        spark.table(output_config.location).count() == 4
    ), f"Output table {output_config.location} has {spark.table(output_config.location).count()} rows"
    assert (
        spark.table(quarantine_config.location).count() == 0
    ), f"Quarantine table {quarantine_config.location} has {spark.table(quarantine_config.location).count()} rows"


@pytest.mark.parametrize("apply_checks_method", [DQEngine.apply_checks, DQEngine.apply_checks_by_metadata])
def test_observer_check_metrics(ws, spark, apply_checks_method):
    """Test that per-check metrics are included as a compact JSON check_metrics value."""
    observer = DQMetricsObserver(name="test_observer")
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

    if apply_checks_method == DQEngine.apply_checks:
        checks = deserialize_checks(TEST_CHECKS)
        checked_df, observation = dq_engine.apply_checks(test_df, checks)
    elif apply_checks_method == DQEngine.apply_checks_by_metadata:
        checked_df, observation = dq_engine.apply_checks_by_metadata(test_df, TEST_CHECKS)
    else:
        raise ValueError("Invalid 'apply_checks_method' used for testing observable metrics.")

    checked_df.count()  # Trigger an action to get the metrics
    actual_metrics = observation.get

    # Default metrics
    assert actual_metrics["input_row_count"] == 4
    assert actual_metrics["error_row_count"] == 1
    assert actual_metrics["warning_row_count"] == 1
    assert actual_metrics["valid_row_count"] == 2

    # Per-check metrics as compact JSON
    check_metrics = json.loads(actual_metrics["check_metrics"])
    assert check_metrics == [
        {"check_name": "id_is_not_null", "error_count": 1, "warning_count": 0},
        {"check_name": "name_is_not_null_and_not_empty", "error_count": 0, "warning_count": 1},
    ]


@pytest.mark.parametrize("apply_checks_method", [DQEngine.apply_checks, DQEngine.apply_checks_by_metadata])
def test_observer_check_metrics_with_auto_derived_names(ws, spark, apply_checks_method):
    """Test that check_metrics uses the correct auto-derived name when check.name is not provided."""
    checks_without_names = [
        {
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "id"}},
        },
        {
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "name"}},
        },
    ]

    observer = DQMetricsObserver(name="test_observer")
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

    if apply_checks_method == DQEngine.apply_checks:
        checks = deserialize_checks(checks_without_names)
        checked_df, observation = dq_engine.apply_checks(test_df, checks)
    elif apply_checks_method == DQEngine.apply_checks_by_metadata:
        checked_df, observation = dq_engine.apply_checks_by_metadata(test_df, checks_without_names)
    else:
        raise ValueError("Invalid 'apply_checks_method' used for testing observable metrics.")

    checked_df.count()
    actual_metrics = observation.get

    assert actual_metrics["input_row_count"] == 4

    # Auto-derived names come from the check condition alias (e.g. is_not_null("id") -> "id_is_null")
    check_metrics = json.loads(actual_metrics["check_metrics"])
    auto_derived_names = [m["check_name"] for m in check_metrics]
    assert len(auto_derived_names) == 2
    # Verify that names were auto-derived (not empty) and match the check condition aliases
    assert auto_derived_names == ["id_is_null", "name_is_null_or_empty"]


def test_observer_check_metrics_change_between_runs(ws, spark):
    """Test that check_metrics reflect the correct checks when the rule set changes between runs."""
    test_df = spark.createDataFrame(
        [
            [1, "Alice", 30, 50000],
            [2, "Bob", 25, 45000],
            [None, "Charlie", 35, 60000],
            [4, None, 28, 55000],
        ],
        TEST_SCHEMA,
    )

    # First run: both checks
    observer1 = DQMetricsObserver(name="test_observer")
    dq_engine1 = DQEngine(workspace_client=ws, spark=spark, observer=observer1, extra_params=EXTRA_PARAMS)
    checked_df1, observation1 = dq_engine1.apply_checks_by_metadata(test_df, TEST_CHECKS)
    checked_df1.count()
    _assert_check_metrics(observation1.get, ["id_is_not_null", "name_is_not_null_and_not_empty"])

    # Second run: reduced checks — a fresh observer/engine picks up the new rule set
    observer2 = DQMetricsObserver(name="test_observer")
    dq_engine2 = DQEngine(workspace_client=ws, spark=spark, observer=observer2, extra_params=EXTRA_PARAMS)
    checked_df2, observation2 = dq_engine2.apply_checks_by_metadata(test_df, [TEST_CHECKS[0]])
    checked_df2.count()
    _assert_check_metrics(observation2.get, ["id_is_not_null"])


def test_save_results_in_table_with_observer_no_observation(ws, spark, make_schema, make_random):
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    quarantine_config = OutputConfig(location=quarantine_table, mode="overwrite")

    quarantine_df = spark.createDataFrame([[3, 4]], "a: int, b: int")

    engine = DQEngine(ws, spark, observer=DQMetricsObserver())
    engine.save_results_in_table(
        quarantine_df=quarantine_df,
        quarantine_config=quarantine_config,
    )

    loaded = spark.table(quarantine_table)
    assertDataFrameEqual(quarantine_df, loaded)


def _assert_check_metrics(actual_metrics: dict, expected_check_names: list[str]) -> None:
    """Assert that check_metrics contains exactly the expected check names."""
    check_metrics = json.loads(actual_metrics["check_metrics"])
    actual_names = [m["check_name"] for m in check_metrics]
    assert actual_names == expected_check_names
