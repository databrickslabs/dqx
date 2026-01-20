import time
from datetime import datetime

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pytest

from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from databricks.labs.dqx.config import InputConfig, OutputConfig, ExtraParams
from databricks.labs.dqx.checks_serializer import deserialize_checks
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.io import save_dataframe_as_table
from databricks.labs.dqx.metrics_observer import DQMetricsObserver, OBSERVATION_TABLE_SCHEMA
from databricks.labs.dqx.rule import ColumnArguments
from tests.integration.conftest import EXTRA_PARAMS

from tests.conftest import TEST_CATALOG

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
TEST_OBSERVER_NAME = "test_observer"


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

    assert f"count(case when {errors_column} is not null then 1 end) as error_row_count" in observer.metrics
    assert f"count(case when {warnings_column} is not null then 1 end) as warning_row_count" in observer.metrics
    assert (
        f"count(case when {errors_column} is null and {warnings_column} is null then 1 end) as valid_row_count"
        in observer.metrics
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
        _, observation = dq_engine.apply_checks(test_df, checks)
    elif apply_checks_method == DQEngine.apply_checks_by_metadata:
        _, observation = dq_engine.apply_checks_by_metadata(test_df, TEST_CHECKS)
    else:
        raise ValueError("Invalid 'apply_checks_method' used for testing observable metrics.")

    actual_metrics = observation.get
    assert actual_metrics == {}


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
    expected_metrics = {
        "input_row_count": 4,
        "error_row_count": 1,
        "warning_row_count": 1,
        "valid_row_count": 2,
    }
    actual_metrics = observation.get
    assert actual_metrics == expected_metrics


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
    expected_metrics = {
        "input_row_count": 4,
        "error_row_count": 1,
        "warning_row_count": 1,
        "valid_row_count": 2,
        "avg_error_age": 35.0,
        "total_warning_salary": 55000,
    }
    actual_metrics = observation.get
    assert actual_metrics == expected_metrics


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

    assert not spark.catalog.tableExists(metrics_table_name)


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

    assert_df_equality(expected_metrics_df, actual_metrics_df)


def test_save_summary_metrics_custom_metrics_and_params(ws, spark, make_schema, make_random):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    metrics_table_name = f"{catalog_name}.{schema_name}.metrics_{make_random(6).lower()}"

    custom_metrics = [
        "avg(case when dq_errors is not null then age else null end) as avg_error_age",
        "sum(case when dq_warnings is not null then salary else null end) as total_warning_salary",
    ]
    observer_name = "test_observer"
    observer = DQMetricsObserver(name=observer_name, custom_metrics=custom_metrics)

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
            "metric_name": "total_warning_salary",
            "metric_value": "55000",
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
    assert_df_equality(expected_metrics_df, actual_metrics_df)


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
            "metric_name": "total_warning_salary",
            "metric_value": "55000",
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
    assert_df_equality(expected_metrics_df, actual_metrics_df)


@pytest.mark.parametrize(
    "apply_checks_method",
    [DQEngine.apply_checks_and_save_in_table, DQEngine.apply_checks_by_metadata_and_save_in_table],
)
def test_observer_metrics_output_with_empty_checks(apply_checks_method, spark, ws, make_schema, make_random):
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

    assert_df_equality(expected_metrics_df, actual_metrics_df)
    assert (
        spark.table(output_config.location).count() == 4
    ), f"Output table {output_config.location} has {spark.table(output_config.location).count()} rows"


@pytest.mark.parametrize(
    "apply_checks_method",
    [DQEngine.apply_checks_and_save_in_table, DQEngine.apply_checks_by_metadata_and_save_in_table],
)
def test_observer_metrics_output_with_quarantine_with_empty_checks(
    apply_checks_method, spark, ws, make_schema, make_random
):
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

    assert_df_equality(expected_metrics_df, actual_metrics_df)
    assert spark.table(output_config.location).count() == 4
    assert spark.table(quarantine_config.location).count() == 0


@pytest.mark.parametrize(
    "apply_checks_method",
    [DQEngine.apply_checks_and_save_in_table, DQEngine.apply_checks_by_metadata_and_save_in_table],
)
def test_observer_metrics_output(apply_checks_method, spark, ws, make_schema, make_random):
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
            "metric_name": "total_warning_salary",
            "metric_value": "55000",
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

    assert_df_equality(expected_metrics_df, actual_metrics_df)
    assert (
        spark.table(output_config.location).count() == 4
    ), f"Output table {output_config.location} has {spark.table(output_config.location).count()} rows"


@pytest.mark.parametrize(
    "apply_checks_method",
    [DQEngine.apply_checks_and_save_in_table, DQEngine.apply_checks_by_metadata_and_save_in_table],
)
def test_observer_metrics_output_with_quarantine(apply_checks_method, spark, ws, make_schema, make_random):
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
            "metric_name": "total_warning_salary",
            "metric_value": "55000",
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
    assert_df_equality(expected_metrics_df, actual_metrics_df)
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
def test_save_results_in_table_batch_with_metrics(apply_checks_method, spark, ws, make_schema, make_random):
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
    )

    expected_metrics = [
        {
            "run_id": EXTRA_PARAMS.run_id_overwrite,
            "run_name": "test_save_batch_observer",
            "input_location": None,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
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
    actual_metrics_df = spark.table(metrics_table_name).orderBy("metric_name")
    assert_df_equality(expected_metrics_df, actual_metrics_df)
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
def test_save_results_in_table_streaming_with_metrics(
    apply_checks_method, spark, ws, make_schema, make_volume, make_random
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
    input_df.write.saveAsTable(input_table_name)
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

    _ = save_dataframe_as_table(output_df, output_config)
    _ = save_dataframe_as_table(quarantine_df, quarantine_config)

    dq_engine.save_results_in_table(
        output_df=output_df,
        quarantine_df=quarantine_df,
        output_config=output_config,
        quarantine_config=quarantine_config,
        metrics_config=metrics_config,
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
            "metric_name": "valid_row_count",
            "metric_value": "2",
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
    assert_df_equality(expected_metrics_df, actual_metrics_df)
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
            "metric_name": "total_warning_salary",
            "metric_value": "55000",
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).orderBy(
        "metric_name"
    )

    assert_df_equality(expected_metrics_df, actual_metrics_df)
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
            "metric_name": "total_warning_salary",
            "metric_value": "55000",
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
    assert_df_equality(expected_metrics_df, actual_metrics_df)
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
    assert_df_equality(expected_metrics_df, actual_metrics_df)
    assert (
        spark.table(output_config.location).count() == 4
    ), f"Output table {output_config.location} has {spark.table(output_config.location).count()} rows"


@pytest.mark.parametrize(
    "apply_checks_method",
    [DQEngine.apply_checks_and_save_in_table, DQEngine.apply_checks_by_metadata_and_save_in_table],
)
def test_streaming_observer_metrics_output_and_quarantine_with_empty_checks(
    apply_checks_method, spark, ws, make_schema, make_volume, make_random
):
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
            "metric_name": "total_warning_salary",
            "metric_value": None,
            "run_time": datetime.fromisoformat(EXTRA_PARAMS.run_time_overwrite),
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
    ]

    expected_metrics_df = (
        spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).drop("run_time").orderBy("metric_name")
    )
    actual_metrics_df = spark.table(metrics_table_name).drop("run_time").orderBy("metric_name")

    assert_df_equality(expected_metrics_df, actual_metrics_df)
    assert (
        spark.table(output_config.location).count() == 4
    ), f"Output table {output_config.location} has {spark.table(output_config.location).count()} rows"
    assert (
        spark.table(quarantine_config.location).count() == 0
    ), f"Quarantine table {quarantine_config.location} has {spark.table(quarantine_config.location).count()} rows"
