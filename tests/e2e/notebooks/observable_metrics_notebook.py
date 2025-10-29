# Databricks notebook source

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")
%pip install 'databricks-labs-dqx @ {dbutils.widgets.get("test_library_ref")}' chispa==0.10.1

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import time
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from chispa import assert_df_equality

from databricks.labs.dqx.config import InputConfig, OutputConfig, ExtraParams
from databricks.labs.dqx.checks_serializer import deserialize_checks
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.io import save_dataframe_as_table
from databricks.labs.dqx.metrics_observer import DQMetricsObserver, OBSERVATION_TABLE_SCHEMA
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# Test constants
run_time = datetime(2025, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
extra_params = ExtraParams(run_time_overwrite=run_time.isoformat())
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

dbutils.widgets.text("catalog_name", "", "Test Catalog Name")
dbutils.widgets.text("schema_name", "", "Test Schema Name")
dbutils.widgets.text("volume_name", "", "Test Volume Name")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
volume_name = dbutils.widgets.get("volume_name")

ws = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,test_save_summary_metrics_default
def test_save_summary_metrics_default():
    """Test that default summary metrics can be accessed after running a Spark action like df.count()."""
    metrics_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"

    observer_name = "test_observer"
    observer = DQMetricsObserver(name=observer_name)

    dq_engine = DQEngine(workspace_client=ws, spark=spark, observer=observer)

    test_df = spark.createDataFrame(
        [
            [1, "Alice", 30, 50000],
            [2, "Bob", 25, 45000],
            [None, "Charlie", 35, 60000],
            [4, None, 28, 55000],
        ],
        test_schema,
    )

    checked_df, observation = dq_engine.apply_checks_by_metadata(test_df, test_checks)
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

    # Extract run_time field from the results, as it is auto-generated
    actual_run_time = None
    for run_time_row in actual_metrics_df.select("run_time").collect():
        if run_time_row:
            actual_run_time = run_time_row[0]
            break

    expected_metrics = [
        {
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_time": actual_run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "error_row_count",
            "metric_value": "1",
            "run_time": actual_run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "warning_row_count",
            "metric_value": "1",
            "run_time": actual_run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "valid_row_count",
            "metric_value": "2",
            "run_time": actual_run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = (
        spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).orderBy("metric_name")
    )
    
    assert_df_equality(expected_metrics_df, actual_metrics_df)

test_save_summary_metrics_default()

# COMMAND ----------

# DBTITLE 1,test_save_summary_metrics
def test_save_summary_metrics():
    """Test that summary metrics can be accessed after running a Spark action like df.count()."""
    metrics_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"

    custom_metrics = [
        "avg(case when dq_errors is not null then age else null end) as avg_error_age",
        "sum(case when dq_warnings is not null then salary else null end) as total_warning_salary",
    ]
    observer_name = "test_observer"
    observer = DQMetricsObserver(name=observer_name, custom_metrics=custom_metrics)

    user_metadata = {"key1": "value1", "key2": "value2"}
    extra_params_custom = ExtraParams(
      run_time_overwrite=run_time.isoformat(),
      result_column_names={"errors": "dq_errors", "warnings": "dq_warnings"},
      user_metadata=user_metadata,
    )

    dq_engine = DQEngine(workspace_client=ws, spark=spark, observer=observer, extra_params=extra_params_custom)

    test_df = spark.createDataFrame(
        [
            [1, "Alice", 30, 50000],
            [2, "Bob", 25, 45000],
            [None, "Charlie", 35, 60000],
            [4, None, 28, 55000],
        ],
        test_schema,
    )

    checked_df, observation = dq_engine.apply_checks_by_metadata(test_df, test_checks)
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
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_time": run_time,
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "error_row_count",
            "metric_value": "1",
            "run_time": run_time,
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "warning_row_count",
            "metric_value": "1",
            "run_time": run_time,
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "valid_row_count",
            "metric_value": "2",
            "run_time": run_time,
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "avg_error_age",
            "metric_value": "35.0",
            "run_time": run_time,
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "total_warning_salary",
            "metric_value": "55000",
            "run_time": run_time,
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
    ]

    expected_metrics_df = (
        spark.createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA).orderBy("metric_name")
    )
    actual_metrics_df = spark.table(metrics_config.location).orderBy("metric_name")
    assert_df_equality(expected_metrics_df, actual_metrics_df)

test_save_summary_metrics()

# COMMAND ----------

# DBTITLE 1,test_save_summary_metrics_with_streaming
def test_save_summary_metrics_with_streaming():
    """Test that summary metrics can be accessed after running a Spark action like df.count()."""
    input_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    output_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    quarantine_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    metrics_table_name = (
        f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    )
    
    checkpoint_location = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{str(uuid4()).replace('-', '_')}"
    quarantine_checkpoint_location = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{str(uuid4()).replace('-', '_')}"

    input_config = InputConfig(location=input_table_name, is_streaming=True)
    output_config = OutputConfig(location=output_table_name, options={"checkPointLocation": checkpoint_location}, trigger={"availableNow": True})
    quarantine_config = OutputConfig(location=quarantine_table_name, options={"checkPointLocation": quarantine_checkpoint_location}, trigger={"availableNow": True})
    metrics_config = OutputConfig(location=metrics_table_name, mode="overwrite")

    custom_metrics = [
        "avg(case when dq_errors is not null then age else null end) as avg_error_age",
        "sum(case when dq_warnings is not null then salary else null end) as total_warning_salary",
    ]
    observer_name = "test_observer"
    observer = DQMetricsObserver(name=observer_name, custom_metrics=custom_metrics)

    user_metadata = {"key1": "value1", "key2": "value2"}
    extra_params_custom = ExtraParams(
        run_time_overwrite=run_time.isoformat(),
        result_column_names={"errors": "dq_errors", "warnings": "dq_warnings"},
        user_metadata=user_metadata,
    )

    dq_engine = DQEngine(workspace_client=ws, spark=spark, observer=observer, extra_params=extra_params_custom)

    test_df = spark.createDataFrame(
        [
            [1, "Alice", 30, 50000],
            [2, "Bob", 25, 45000],
            [None, "Charlie", 35, 60000],
            [4, None, 28, 55000],
        ],
        test_schema,
    )
    test_df.write.saveAsTable(input_table_name, data=test_df, mode="overwrite")

    input_df = spark.readStream.table(input_config.location)
    
    valid_df, quarantine_df, observation = dq_engine.apply_checks_by_metadata_and_split(input_df, test_checks)

    output_query = valid_df.writeStream.format(output_config.format).outputMode(output_config.mode).options(**output_config.options).trigger(**output_config.trigger).toTable(output_config.location)
    quarantine_query = quarantine_df.writeStream.format(quarantine_config.format).outputMode(quarantine_config.mode).options(**quarantine_config.options).trigger(**quarantine_config.trigger).toTable(quarantine_config.location)

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
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_time": run_time,
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "error_row_count",
            "metric_value": "1",
            "run_time": run_time,
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "warning_row_count",
            "metric_value": "1",
            "run_time": run_time,
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "valid_row_count",
            "metric_value": "2",
            "run_time": run_time,
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "avg_error_age",
            "metric_value": "35.0",
            "run_time": run_time,
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_name": observer_name,
            "input_location": input_config.location,
            "output_location": output_config.location,
            "quarantine_location": quarantine_config.location,
            "checks_location": checks_location,
            "metric_name": "total_warning_salary",
            "metric_value": "55000",
            "run_time": run_time,
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
    ]

    expected_metrics_df = spark.createDataFrame(
        expected_metrics, schema=OBSERVATION_TABLE_SCHEMA
    ).drop("run_time").orderBy("metric_name")
    actual_metrics_df = spark.table(metrics_config.location).drop("run_time").orderBy("metric_name")
    assert_df_equality(expected_metrics_df, actual_metrics_df)

test_save_summary_metrics_with_streaming()

# COMMAND ----------

# DBTITLE 1,test_observer_metrics_output_with_empty_checks
def test_observer_metrics_output_with_empty_checks(apply_checks_method):
    input_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    output_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    metrics_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"

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
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_time": run_time,
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
            "metric_value": "0",
            "run_time": run_time,
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
            "metric_value": "0",
            "run_time": run_time,
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
            "metric_value": "4",
            "run_time": run_time,
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
            "metric_value": None,
            "run_time": run_time,
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
            "metric_value": None,
            "run_time": run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = (
        spark
        .createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA)
        .orderBy("metric_name")
    )
    actual_metrics_df = (
        spark
        .table(metrics_table_name)
        .orderBy("metric_name")
    )

    assert_df_equality(expected_metrics_df, actual_metrics_df)
    assert spark.table(output_config.location).count() == 4

for method in [DQEngine.apply_checks_and_save_in_table, DQEngine.apply_checks_by_metadata_and_save_in_table]:
    test_observer_metrics_output_with_empty_checks(method)

# COMMAND ----------

# DBTITLE 1,test_observer_metrics_output_with_quarantine_with_empty_checks
def test_observer_metrics_output_with_quarantine_with_empty_checks(apply_checks_method):
    input_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    output_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    quarantine_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    metrics_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"

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
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_time": run_time,
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
            "metric_value": "0",
            "run_time": run_time,
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
            "metric_value": "0",
            "run_time": run_time,
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
            "metric_value": "4",
            "run_time": run_time,
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
            "metric_value": None,
            "run_time": run_time,
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
            "metric_value": None,
            "run_time": run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = (
        spark
        .createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA)
        .orderBy("metric_name")
    )
    actual_metrics_df = (
        spark
        .table(metrics_table_name)
        .orderBy("metric_name")
    )

    assert_df_equality(expected_metrics_df, actual_metrics_df)
    assert spark.table(output_config.location).count() == 4
    assert spark.table(quarantine_config.location).count() == 0

for method in [DQEngine.apply_checks_and_save_in_table, DQEngine.apply_checks_by_metadata_and_save_in_table]:
    test_observer_metrics_output_with_quarantine_with_empty_checks(method)

# COMMAND ----------

# DBTITLE 1,test_observer_metrics_output
def test_observer_metrics_output(apply_checks_method):
    input_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    output_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    metrics_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"

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
    output_config = OutputConfig(location=output_table_name, mode="overwrite")
    metrics_config = OutputConfig(location=metrics_table_name, mode="overwrite")
    checks_location = "fake.yml"

    if apply_checks_method == DQEngine.apply_checks_and_save_in_table:
        checks = deserialize_checks(test_checks)
        dq_engine.apply_checks_and_save_in_table(
            checks=checks, input_config=input_config, output_config=output_config, metrics_config=metrics_config, checks_location=checks_location
        )
    elif apply_checks_method == DQEngine.apply_checks_by_metadata_and_save_in_table:
        dq_engine.apply_checks_by_metadata_and_save_in_table(
            checks=test_checks, input_config=input_config, output_config=output_config, metrics_config=metrics_config, checks_location=checks_location
        )
    else:
        raise ValueError("Invalid 'apply_checks_method' used for testing observable metrics.")

    expected_metrics = [
        {
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": checks_location,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_time": run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": checks_location,
            "metric_name": "error_row_count",
            "metric_value": "1",
            "run_time": run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": checks_location,
            "metric_name": "warning_row_count",
            "metric_value": "1",
            "run_time": run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": checks_location,
            "metric_name": "valid_row_count",
            "metric_value": "2",
            "run_time": run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": checks_location,
            "metric_name": "avg_error_age",
            "metric_value": "35.0",
            "run_time": run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": checks_location,
            "metric_name": "total_warning_salary",
            "metric_value": "55000",
            "run_time": run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = (
        spark
        .createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA)
        .orderBy("metric_name")
    )
    actual_metrics_df = (
        spark
        .table(metrics_table_name)
        .orderBy("metric_name")
    )

    assert_df_equality(expected_metrics_df, actual_metrics_df)
    assert spark.table(output_config.location).count() == 4
    

for method in [DQEngine.apply_checks_and_save_in_table, DQEngine.apply_checks_by_metadata_and_save_in_table]:
    test_observer_metrics_output(method)

# COMMAND ----------

# DBTITLE 1,test_observer_metrics_output_with_quarantine
def test_observer_metrics_output_with_quarantine(apply_checks_method):
    input_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    output_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    quarantine_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    metrics_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"

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
    output_config = OutputConfig(location=output_table_name, mode="overwrite")
    quarantine_config = OutputConfig(location=quarantine_table_name, mode="overwrite")
    metrics_config = OutputConfig(location=metrics_table_name, mode="overwrite")
    checks_location = "fake.yml"

    if apply_checks_method == DQEngine.apply_checks_and_save_in_table:
        checks = deserialize_checks(test_checks)
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
            checks=test_checks,
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
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": checks_location,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_time": run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": checks_location,
            "metric_name": "error_row_count",
            "metric_value": "1",
            "run_time": run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": checks_location,
            "metric_name": "warning_row_count",
            "metric_value": "1",
            "run_time": run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": checks_location,
            "metric_name": "valid_row_count",
            "metric_value": "2",
            "run_time": run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": checks_location,
            "metric_name": "avg_error_age",
            "metric_value": "35.0",
            "run_time": run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": checks_location,
            "metric_name": "total_warning_salary",
            "metric_value": "55000",
            "run_time": run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = (
        spark
        .createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA)
        .orderBy("metric_name")
    )
    actual_metrics_df = (
        spark
        .table(metrics_table_name)
        .orderBy("metric_name")
    )
    assert_df_equality(expected_metrics_df, actual_metrics_df)
    assert spark.table(output_config.location).count() == 3
    assert spark.table(quarantine_config.location).count() == 2

for method in [DQEngine.apply_checks_and_save_in_table, DQEngine.apply_checks_by_metadata_and_save_in_table]:
    test_observer_metrics_output_with_quarantine(method)

# COMMAND ----------

# DBTITLE 1,test_save_results_in_table_batch_with_metrics
def test_save_results_in_table_batch_with_metrics(apply_checks_method):
    output_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    quarantine_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    metrics_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"

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

    if method == DQEngine.apply_checks_and_split:
        checks = deserialize_checks(test_checks)
        output_df, quarantine_df, observation = dq_engine.apply_checks_and_split(test_df, checks)
    elif method == DQEngine.apply_checks_by_metadata_and_split:
        output_df, quarantine_df, observation = dq_engine.apply_checks_by_metadata_and_split(test_df, test_checks)
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
            "run_name": "test_save_batch_observer",
            "input_location": None,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_time": run_time,
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
            "run_time": run_time,
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
            "run_time": run_time,
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
            "run_time": run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = (
        spark
        .createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA)
        .orderBy("metric_name")
    )
    actual_metrics_df = (
        spark
        .table(metrics_table_name)
        .orderBy("metric_name")
    )
    assert_df_equality(expected_metrics_df, actual_metrics_df)
    assert spark.table(output_config.location).count() == 3
    assert spark.table(quarantine_config.location).count() == 2

for method in [DQEngine.apply_checks_and_split, DQEngine.apply_checks_by_metadata_and_split]:
    test_save_results_in_table_batch_with_metrics(method)


# COMMAND ----------

# DBTITLE 1,test_save_results_in_table_streaming_with_metrics
def test_save_results_in_table_streaming_with_metrics(apply_checks_method):
    input_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    output_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    quarantine_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    metrics_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    checkpoint_location = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{str(uuid4()).replace('-', '_')}"
    checkpoint_location_quarantine = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/quarantine_{str(uuid4()).replace('-', '_')}"

    observer = DQMetricsObserver(name="test_save_batch_observer")
    dq_engine = DQEngine(workspace_client=ws, spark=spark, observer=observer, extra_params=extra_params)

    input_df = spark.createDataFrame(
        [
            [1, "Alice", 30, 50000],
            [2, "Bob", 25, 45000],
            [None, "Charlie", 35, 60000],  # This will trigger an error
            [4, None, 28, 55000],  # This will trigger a warning
        ],
        test_schema,
    )

    input_df.write.saveAsTable(input_table_name)

    test_df = spark.readStream.table(input_table_name)

    if method == DQEngine.apply_checks_and_split:
        checks = deserialize_checks(test_checks)
        output_df, quarantine_df, observation = dq_engine.apply_checks_and_split(test_df, checks)
    elif method == DQEngine.apply_checks_by_metadata_and_split:
        output_df, quarantine_df, observation = dq_engine.apply_checks_by_metadata_and_split(test_df, test_checks)
    else:
        raise ValueError("Invalid 'apply_checks_method' used for testing observable metrics.")

    output_config = OutputConfig(location=output_table_name, options={"checkPointLocation": checkpoint_location}, trigger={"availableNow": True})
    quarantine_config = OutputConfig(location=quarantine_table_name, options={"checkPointLocation": checkpoint_location_quarantine}, trigger={"availableNow": True})
    metrics_config = OutputConfig(location=metrics_table_name)

    output_query = save_dataframe_as_table(output_df, output_config)
    quarantine_query = save_dataframe_as_table(quarantine_df, quarantine_config)

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
            "run_name": "test_save_batch_observer",
            "input_location": None,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_time": run_time,
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
            "run_time": run_time,
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
            "run_time": run_time,
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
            "run_time": run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = (
        spark
        .createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA)
        .drop("run_time")
        .orderBy("metric_name")
    )
    actual_metrics_df = (
        spark
        .table(metrics_table_name)
        .drop("run_time")
        .orderBy("metric_name")
    )
    assert_df_equality(expected_metrics_df, actual_metrics_df)
    assert spark.table(output_config.location).count() == 3
    assert spark.table(quarantine_config.location).count() == 2

for method in [DQEngine.apply_checks_and_split, DQEngine.apply_checks_by_metadata_and_split]:
    test_save_results_in_table_streaming_with_metrics(method)


# COMMAND ----------

# DBTITLE 1,test_streaming_observer_metrics_output
def test_streaming_observer_metrics_output(apply_checks_method):
    input_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    output_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    metrics_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
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
    checks_location = "fake.yml"

    if apply_checks_method == DQEngine.apply_checks_and_save_in_table:
        checks = deserialize_checks(test_checks)
        dq_engine.apply_checks_and_save_in_table(
            checks=checks, input_config=input_config, output_config=output_config, metrics_config=metrics_config, checks_location=checks_location
        )
    elif apply_checks_method == DQEngine.apply_checks_by_metadata_and_save_in_table:
        dq_engine.apply_checks_by_metadata_and_save_in_table(
            checks=test_checks, input_config=input_config, output_config=output_config, metrics_config=metrics_config, checks_location=checks_location
        )
    else:
        raise ValueError("Invalid 'apply_checks_method' used for testing observable metrics.")

    time.sleep(30)

    actual_metrics_df = (
        spark
        .table(metrics_table_name)
        .orderBy("metric_name")
    )

    # Extract run_time field from the results, as it is auto-generated
    actual_run_time = None
    for run_time_row in actual_metrics_df.select("run_time").collect():
        if run_time_row:
            actual_run_time = run_time_row[0]
            break

    expected_metrics = [
        {
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": checks_location,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_time": actual_run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": checks_location,
            "metric_name": "error_row_count",
            "metric_value": "1",
            "run_time": actual_run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": checks_location,
            "metric_name": "warning_row_count",
            "metric_value": "1",
            "run_time": actual_run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": checks_location,
            "metric_name": "valid_row_count",
            "metric_value": "2",
            "run_time": actual_run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": checks_location,
            "metric_name": "avg_error_age",
            "metric_value": "35.0",
            "run_time": actual_run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": checks_location,
            "metric_name": "total_warning_salary",
            "metric_value": "55000",
            "run_time": actual_run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = (
        spark
        .createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA)
        .orderBy("metric_name")
    )

    assert_df_equality(expected_metrics_df, actual_metrics_df)
    assert spark.table(output_config.location).count() == 4

for method in [DQEngine.apply_checks_and_save_in_table, DQEngine.apply_checks_by_metadata_and_save_in_table]:
    test_streaming_observer_metrics_output(method)

# COMMAND ----------

# DBTITLE 1,test_streaming_observer_metrics_output_and_quarantine
def test_streaming_observer_metrics_output_and_quarantine(apply_checks_method):
    input_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    output_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    quarantine_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    metrics_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    checkpoint_location = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{str(uuid4()).replace('-', '_')}"
    checkpoint_location_quarantine = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/quarantine_{str(uuid4()).replace('-', '_')}"

    custom_metrics = [
        "avg(case when _errors is not null then age else null end) as avg_error_age",
        "sum(case when _warnings is not null then salary else null end) as total_warning_salary",
    ]
    observer = DQMetricsObserver(name="test_streaming_observer_with_quarantine", custom_metrics=custom_metrics)
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
    quarantine_config = OutputConfig(
        location=quarantine_table_name, options={"checkPointLocation": checkpoint_location_quarantine}, trigger={"availableNow": True}
    )
    metrics_config = OutputConfig(location=metrics_table_name)
    checks_location = "fake.yml"

    if apply_checks_method == DQEngine.apply_checks_and_save_in_table:
        checks = deserialize_checks(test_checks)
        dq_engine.apply_checks_and_save_in_table(
            checks=checks, input_config=input_config, output_config=output_config, quarantine_config=quarantine_config, metrics_config=metrics_config, checks_location=checks_location
        )
    elif apply_checks_method == DQEngine.apply_checks_by_metadata_and_save_in_table:
        dq_engine.apply_checks_by_metadata_and_save_in_table(
            checks=test_checks, input_config=input_config, output_config=output_config, quarantine_config=quarantine_config, metrics_config=metrics_config, checks_location=checks_location
        )
    else:
        raise ValueError("Invalid 'apply_checks_method' used for testing observable metrics.")

    time.sleep(30)
    expected_metrics = [
        {
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": checks_location,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_time": run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": checks_location,
            "metric_name": "error_row_count",
            "metric_value": "1",
            "run_time": run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": checks_location,
            "metric_name": "warning_row_count",
            "metric_value": "1",
            "run_time": run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": checks_location,
            "metric_name": "valid_row_count",
            "metric_value": "2",
            "run_time": run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": checks_location,
            "metric_name": "avg_error_age",
            "metric_value": "35.0",
            "run_time": run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
        {
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": checks_location,
            "metric_name": "total_warning_salary",
            "metric_value": "55000",
            "run_time": run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = (
        spark
        .createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA)
        .drop("run_time")
        .orderBy("metric_name")
    )
    actual_metrics_df = (
        spark
        .table(metrics_table_name)
        .drop("run_time")
        .orderBy("metric_name")
    )
    assert_df_equality(expected_metrics_df, actual_metrics_df)
    assert spark.table(output_config.location).count() == 3
    assert spark.table(quarantine_config.location).count() == 2

for method in [DQEngine.apply_checks_and_save_in_table, DQEngine.apply_checks_by_metadata_and_save_in_table]:
    test_streaming_observer_metrics_output(method)

# COMMAND ----------

# DBTITLE 1,test_streaming_observer_metrics_output_with_empty_checks
def test_streaming_observer_metrics_output_with_empty_checks(apply_checks_method):
    input_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    output_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    metrics_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
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

    if apply_checks_method == DQEngine.apply_checks_and_save_in_table:
        checks = deserialize_checks(test_checks)
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
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": None,
            "checks_location": None,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_time": run_time,
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
            "metric_value": "0",
            "run_time": run_time,
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
            "metric_value": "0",
            "run_time": run_time,
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
            "metric_value": "4",
            "run_time": run_time,
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
            "metric_value": None,
            "run_time": run_time,
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
            "metric_value": None,
            "run_time": run_time,
            "error_column_name": "_errors",
            "warning_column_name": "_warnings",
            "user_metadata": None,
        },
    ]

    expected_metrics_df = (
        spark
        .createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA)
        .drop("run_time")
        .orderBy("metric_name")
    )
    actual_metrics_df = (
        spark
        .table(metrics_table_name)
        .drop("run_time")
        .orderBy("metric_name")
    )
    assert_df_equality(expected_metrics_df, actual_metrics_df)
    assert spark.table(output_config.location).count() == 4

for method in [DQEngine.apply_checks_and_save_in_table, DQEngine.apply_checks_by_metadata_and_save_in_table]:
    test_streaming_observer_metrics_output_with_empty_checks(method)

# COMMAND ----------

# DBTITLE 1,test_streaming_observer_metrics_output_and_quarantine_with_empty_checks
def test_streaming_observer_metrics_output_and_quarantine_with_empty_checks(apply_checks_method):
    input_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    output_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    quarantine_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    metrics_table_name = f"{catalog_name}.{schema_name}.{str(uuid4()).replace("-", "_")}"
    checkpoint_location = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{str(uuid4()).replace('-', '_')}"
    checkpoint_location_quarantine = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/quarantine_{str(uuid4()).replace('-', '_')}"

    custom_metrics = [
        "avg(case when dq_errors is not null then age else null end) as avg_error_age",
        "sum(case when dq_warnings is not null then salary else null end) as total_warning_salary",
    ]
    observer = DQMetricsObserver(name="test_streaming_observer", custom_metrics=custom_metrics)
    user_metadata = {"key1": "value1", "key2": "value2"}
    extra_params_custom = ExtraParams(
      run_time_overwrite=run_time.isoformat(),
      result_column_names={"errors": "dq_errors", "warnings": "dq_warnings"},
      user_metadata=user_metadata,
    )
    dq_engine = DQEngine(workspace_client=ws, spark=spark, observer=observer, extra_params=extra_params_custom)

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
    quarantine_config = OutputConfig(
        location=quarantine_table_name, options={"checkPointLocation": checkpoint_location_quarantine}, trigger={"availableNow": True}
    )
    metrics_config = OutputConfig(location=metrics_table_name)

    if apply_checks_method == DQEngine.apply_checks_and_save_in_table:
        checks = deserialize_checks(test_checks)
        dq_engine.apply_checks_and_save_in_table(
            checks=[], input_config=input_config, output_config=output_config, quarantine_config=quarantine_config, metrics_config=metrics_config
        )
    elif apply_checks_method == DQEngine.apply_checks_by_metadata_and_save_in_table:
        dq_engine.apply_checks_by_metadata_and_save_in_table(
            checks=[], input_config=input_config, output_config=output_config, quarantine_config=quarantine_config, metrics_config=metrics_config
        )
    else:
        raise ValueError("Invalid 'apply_checks_method' used for testing observable metrics.")

    time.sleep(30)
    expected_metrics = [
        {
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "metric_name": "input_row_count",
            "metric_value": "4",
            "run_time": run_time,
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "metric_name": "error_row_count",
            "metric_value": "0",
            "run_time": run_time,
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "metric_name": "warning_row_count",
            "metric_value": "0",
            "run_time": run_time,
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "metric_name": "valid_row_count",
            "metric_value": "4",
            "run_time": run_time,
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "metric_name": "avg_error_age",
            "metric_value": None,
            "run_time": run_time,
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
        {
            "run_name": "test_streaming_observer",
            "input_location": input_table_name,
            "output_location": output_table_name,
            "quarantine_location": quarantine_table_name,
            "checks_location": None,
            "metric_name": "total_warning_salary",
            "metric_value": None,
            "run_time": run_time,
            "error_column_name": "dq_errors",
            "warning_column_name": "dq_warnings",
            "user_metadata": user_metadata,
        },
    ]

    expected_metrics_df = (
        spark
        .createDataFrame(expected_metrics, schema=OBSERVATION_TABLE_SCHEMA)
        .drop("run_time")
        .orderBy("metric_name")
    )
    actual_metrics_df = (
        spark
        .table(metrics_table_name)
        .drop("run_time")
        .orderBy("metric_name")
    )

    assert_df_equality(expected_metrics_df, actual_metrics_df)
    assert spark.table(output_config.location).count() == 4
    assert spark.table(quarantine_config.location).count() == 0

for method in [DQEngine.apply_checks_and_save_in_table, DQEngine.apply_checks_by_metadata_and_save_in_table]:
    test_streaming_observer_metrics_output_and_quarantine_with_empty_checks(method)
