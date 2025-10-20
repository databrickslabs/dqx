# Databricks notebook source

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")
%pip install 'databricks-labs-dqx @ {dbutils.widgets.get("test_library_ref")}' chispa==0.10.1

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from chispa import assert_df_equality

from databricks.labs.dqx.config import InputConfig, OutputConfig, ExtraParams
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.metrics_observer import DQMetricsObserver, OBSERVATION_TABLE_SCHEMA
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# Test constants
run_time = datetime(2025, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
extra_params = ExtraParams(run_time=run_time.isoformat())
test_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("age", IntegerType()),
        StructField("salary", IntegerType()),
    ]
)
test_checks = [
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
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
volume_name = dbutils.widgets.get("volume_name")
ws = WorkspaceClient()

# COMMAND ----------
# DBTITLE 1,test_observer_metrics_output

def test_observer_metrics_output():
    input_table_name = f"{catalog_name}.{schema_name}.input_table"
    output_table_name = f"{catalog_name}.{schema_name}.output_table"
    metrics_table_name = f"{catalog_name}.{schema_name}.metrics_table"

    custom_metrics = [
        "avg(case when _errors is not null then age else null end) as avg_error_age",
        "sum(case when _warnings is not null then salary else null end) as total_warning_salary",
    ]
    observer = DQMetricsObserver(name="test_observer", custom_metrics=custom_metrics)
    dq_engine = DQEngine(workspace_client=ws, spark=spark, observer=observer, extra_params=extra_params)

    test_df = spark.createDataFrame(
        [
            [1, "Alice", 30, 50000],
            [2, "Bob", 25, 45000],
            [None, "Charlie", 35, 60000],
            [4, None, 28, 55000],
        ],
        test_schema,
    )

    test_df.write.saveAsTable(input_table_name)
    input_config = InputConfig(location=input_table_name)
    output_config = OutputConfig(location=output_table_name)
    metrics_config = OutputConfig(location=metrics_table_name)

    dq_engine.apply_checks_by_metadata_and_save_in_table(
        checks=test_checks, input_config=input_config, output_config=output_config, metrics_config=metrics_config
    )

    expected_metrics = [
        {
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_ts": None,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "error_row_count",
            "metric_value": "1",
            "run_ts": None,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "warning_row_count",
            "metric_value": "1",
            "run_ts": None,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "valid_row_count",
            "metric_value": "2",
            "run_ts": None,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "avg_error_age",
            "metric_value": "35.0",
            "run_ts": None,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "total_warning_salary",
            "metric_value": "55000",
            "run_ts": None,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]
    
    expected_metrics_df = spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).drop("run_ts")
    actual_metrics_df = spark.table(metrics_table_name).drop("run_ts")
    assert_df_equality(expected_metrics_df, actual_metrics_df)

test_observer_metrics_output()

# COMMAND ----------
# DBTITLE 1,test_observer_metrics_output_with_quarantine

def test_observer_metrics_output_with_quarantine():
    input_table_name = f"{catalog_name}.{schema_name}.input_table"
    output_table_name = f"{catalog_name}.{schema_name}.output_table"
    quarantine_table_name = f"{catalog_name}.{schema_name}.quarantine_table"
    metrics_table_name = f"{catalog_name}.{schema_name}.metrics_table"

    custom_metrics = [
        "avg(case when _errors is not null then age else null end) as avg_error_age",
        "sum(case when _warnings is not null then salary else null end) as total_warning_salary",
    ]
    observer = DQMetricsObserver(name="test_observer", custom_metrics=custom_metrics)
    dq_engine = DQEngine(workspace_client=ws, spark=spark, observer=observer, extra_params=extra_params)

    test_df = spark.createDataFrame(
        [
            [1, "Alice", 30, 50000],
            [2, "Bob", 25, 45000],
            [None, "Charlie", 35, 60000],
            [4, None, 28, 55000],
        ],
        test_schema,
    )

    test_df.write.saveAsTable(input_table_name)
    input_config = InputConfig(location=input_table_name)
    output_config = OutputConfig(location=output_table_name)
    quarantine_config = OutputConfig(location=quarantine_table_name)
    metrics_config = OutputConfig(location=metrics_table_name)

    dq_engine.apply_checks_by_metadata_and_save_in_table(
        checks=test_checks,
        input_config=input_config,
        output_config=output_config,
        quarantine_config=quarantine_config,
        metrics_config=metrics_config,
    )

    expected_metrics = [
        {
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_ts": None,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "metric_name": "error_row_count",
            "metric_value": "1",
            "run_ts": None,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "metric_name": "warning_row_count",
            "metric_value": "1",
            "run_ts": None,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "metric_name": "valid_row_count",
            "metric_value": "2",
            "run_ts": None,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "metric_name": "avg_error_age",
            "metric_value": "35.0",
            "run_ts": None,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "metric_name": "total_warning_salary",
            "metric_value": "55000",
            "run_ts": None,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]
    
    expected_metrics_df = spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).drop("run_ts")
    actual_metrics_df = spark.table(metrics_table_name).drop("run_ts")
    assert_df_equality(expected_metrics_df, actual_metrics_df)

test_observer_metrics_output_with_quarantine()

# COMMAND ----------
# DBTITLE 1,test_save_results_in_table_batch_with_metrics

def test_save_results_in_table_batch_with_metrics():
    output_table_name = f"{catalog_name}.{schema_name}.output_table"
    quarantine_table_name = f"{catalog_name}.{schema_name}.quarantine_table"
    metrics_table_name = f"{catalog_name}.{schema_name}.metrics_table"

    observer = DQMetricsObserver(name="test_save_batch_observer")
    dq_engine = DQEngine(workspace_client=ws, spark=spark, observer=observer, extra_params=extra_params)

    test_df = spark.createDataFrame(
        [
            [1, "Alice", 30, 50000],
            [2, "Bob", 25, 45000],
            [None, "Charlie", 35, 60000],  # This will trigger an error
            [4, None, 28, 55000],  # This will trigger a warning
        ],
        test_schema,
    )
    output_df, quarantine_df, observation = dq_engine.apply_checks_by_metadata_and_split(test_df, test_checks)

    output_config = OutputConfig(location=output_table_name)
    quarantine_config = OutputConfig(location=quarantine_table_name)
    metrics_config = OutputConfig(location=metrics_table_name)

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
            "run_name": "test_save_batch_observer",
            "input_location": None,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_ts": None,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_save_batch_observer",
            "input_location": None,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "metric_name": "error_row_count",
            "metric_value": "1",
            "run_ts": None,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_save_batch_observer",
            "input_location": None,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "metric_name": "warning_row_count",
            "metric_value": "1",
            "run_ts": None,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_save_batch_observer",
            "input_location": None,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "metric_name": "valid_row_count",
            "metric_value": "2",
            "run_ts": None,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]
    
    expected_metrics_df = spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).drop("run_ts")
    actual_metrics_df = spark.table(metrics_table_name).drop("run_ts")
    assert_df_equality(expected_metrics_df, actual_metrics_df)

test_save_results_in_table_batch_with_metrics()

# COMMAND ----------
# DBTITLE 1,test_streaming_observer_metrics_output

def test_streaming_observer_metrics_output():
    input_table_name = f"{catalog_name}.{schema_name}.input_table"
    output_table_name = f"{catalog_name}.{schema_name}.output_table"
    metrics_table_name = f"{catalog_name}.{schema_name}.metrics_table"
    checkpoint_location = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{str(uuid4()).replace('-', '_')}"

    custom_metrics = [
        "avg(case when _errors is not null then age else null end) as avg_error_age",
        "sum(case when _warnings is not null then salary else null end) as total_warning_salary",
    ]
    observer = DQMetricsObserver(name="test_streaming_observer", custom_metrics=custom_metrics)
    dq_engine = DQEngine(workspace_client=ws, spark=spark, observer=observer, extra_params=extra_params)

    test_df = spark.createDataFrame(
        [
            [1, "Alice", 30, 50000],
            [2, "Bob", 25, 45000],
            [None, "Charlie", 35, 60000],
            [4, None, 28, 55000],
        ],
        test_schema,
    )

    test_df.write.saveAsTable(input_table_name)
    input_config = InputConfig(location=input_table_name, is_streaming=True)
    output_config = OutputConfig(
        location=output_table_name, options={"checkPointLocation": checkpoint_location}, trigger={"availableNow": True}
    )
    metrics_config = OutputConfig(location=metrics_table_name)

    dq_engine.apply_checks_by_metadata_and_save_in_table(
        checks=test_checks, input_config=input_config, output_config=output_config, metrics_config=metrics_config
    )

    expected_metrics = [
        {
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_ts": None,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "error_row_count",
            "metric_value": "1",
            "run_ts": None,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "warning_row_count",
            "metric_value": "1",
            "run_ts": None,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "valid_row_count",
            "metric_value": "2",
            "run_ts": None,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "avg_error_age",
            "metric_value": "35.0",
            "run_ts": None,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "total_warning_salary",
            "metric_value": "55000",
            "run_ts": None,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]
    
    expected_metrics_df = spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).drop("run_ts")
    actual_metrics_df = spark.table(metrics_table_name).drop("run_ts")
    assert_df_equality(expected_metrics_df, actual_metrics_df)

test_streaming_observer_metrics_output()
