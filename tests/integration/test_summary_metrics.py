from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pytest
from databricks.labs.dqx.config import InputConfig, OutputConfig, ExtraParams
from databricks.labs.dqx.checks_serializer import deserialize_checks
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.labs.dqx.rule import ColumnArguments
from tests.integration.conftest import EXTRA_PARAMS, RUN_TIME

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


def test_observer_custom_column_names(ws, spark):
    """Test that observers have the correct column names when DQEngine is created with custom column names."""
    errors_column = "dq_errors"
    warnings_column = "dq_warnings"
    extra_params = ExtraParams(
        run_time=RUN_TIME.isoformat(),
        result_column_names={
            ColumnArguments.ERRORS.value: errors_column,
            ColumnArguments.WARNINGS.value: warnings_column,
        },
    )
    observer = DQMetricsObserver(name="test_observer")
    _ = DQEngine(workspace_client=ws, spark=spark, extra_params=extra_params, observer=observer)

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

    assert not ws.tables.exists(metrics_table_name).table_exists


@pytest.mark.parametrize(
    "apply_checks_method",
    [DQEngine.apply_checks_and_save_in_table, DQEngine.apply_checks_by_metadata_and_save_in_table],
)
def test_apply_checks_by_metadata_and_save_in_table_raises_error_for_sparkconnect(
    ws, spark, make_schema, make_random, apply_checks_method
):
    """Test that apply_checks_by_metadata_and_save_in_table raises TypeError for SparkConnect DataFrames when observer is configured."""
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    input_table_name = f"{catalog_name}.{schema.name}.input_{make_random(6).lower()}"
    output_table_name = f"{catalog_name}.{schema.name}.output_{make_random(6).lower()}"
    metrics_table_name = f"{catalog_name}.{schema.name}.metrics_{make_random(6).lower()}"

    observer = DQMetricsObserver(name="test_observer")
    dq_engine = DQEngine(workspace_client=ws, spark=spark, observer=observer, extra_params=EXTRA_PARAMS)

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

    # SparkConnect sessions do not currently create metrics for commands (e.g. df.write). When a user calls
    # apply_checks_by_metadata_and_save_in_table with an observer and a metrics config, we will raise a TypeError to
    # inform the user to use a Spark cluster with Dedicated access mode to collect metrics.
    with pytest.raises(
        TypeError,
        match="Metrics collection is not supported for SparkConnect sessions. "
        "Use a Spark cluster with Dedicated access mode to collect metrics.",
    ):
        if apply_checks_method == DQEngine.apply_checks_and_save_in_table:
            checks = deserialize_checks(TEST_CHECKS)
            dq_engine.apply_checks_and_save_in_table(
                checks=checks, input_config=input_config, output_config=output_config, metrics_config=metrics_config
            )
        elif apply_checks_method == DQEngine.apply_checks_by_metadata_and_save_in_table:
            dq_engine.apply_checks_by_metadata_and_save_in_table(
                checks=TEST_CHECKS,
                input_config=input_config,
                output_config=output_config,
                metrics_config=metrics_config,
            )
        else:
            raise ValueError("Invalid 'apply_checks_method' used for testing observable metrics.")


def test_save_results_in_table_raises_error_for_sparkconnect(ws, spark, make_schema, make_random):
    """Test that save_results_in_table raises TypeError for SparkConnect DataFrames when observer is configured."""
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table_name = f"{catalog_name}.{schema.name}.output_{make_random(6).lower()}"
    quarantine_table_name = f"{catalog_name}.{schema.name}.quarantine_{make_random(6).lower()}"
    metrics_table_name = f"{catalog_name}.{schema.name}.metrics_{make_random(6).lower()}"

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

    output_df, quarantine_df, observation = dq_engine.apply_checks_by_metadata_and_split(test_df, TEST_CHECKS)

    output_config = OutputConfig(location=output_table_name)
    quarantine_config = OutputConfig(location=quarantine_table_name)
    metrics_config = OutputConfig(location=metrics_table_name)

    # SparkConnect sessions do not currently create metrics for commands (e.g. df.write). When a user calls
    # save_results_in_table with an observer and a metrics config, we will raise a TypeError to inform the user to use
    # a Spark cluster with Dedicated access mode to collect metrics.
    with pytest.raises(
        TypeError,
        match="Metrics collection is not supported for SparkConnect sessions. "
        "Use a Spark cluster with Dedicated access mode to collect metrics.",
    ):
        dq_engine.save_results_in_table(
            output_df=output_df,
            quarantine_df=quarantine_df,
            observation=observation,
            output_config=output_config,
            quarantine_config=quarantine_config,
            metrics_config=metrics_config,
        )
