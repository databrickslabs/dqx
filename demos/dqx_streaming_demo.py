# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC ### Environment Setup for DQX with Structured Streaming
# MAGIC
# MAGIC #### 1\. Library Installation
# MAGIC
# MAGIC **Option A — Notebook-scoped installation**
# MAGIC
# MAGIC - Use notebook-scoped pip installs, which take effect only for the current interactive notebook session.
# MAGIC
# MAGIC ```py
# MAGIC # Install DQX library in the current notebook session
# MAGIC # Run this cell before any imports from databricks-labs-dqx
# MAGIC
# MAGIC %pip install databricks-labs-dqx
# MAGIC ```
# MAGIC
# MAGIC - If you are using a cluster that was running prior to this install, you may need to restart the Python process for the changes to take full effect:
# MAGIC
# MAGIC ```py
# MAGIC # Optional: Force restart of the Python process if required by your environment
# MAGIC dbutils.library.restartPython()
# MAGIC ```
# MAGIC
# MAGIC **Option B — Cluster-scoped installation (recommended for production streaming jobs)**
# MAGIC
# MAGIC - Install the DQX library to your cluster via the Databricks Libraries UI or by specifying it as a PyPI library dependency during cluster creation.  
# MAGIC - This ensures the package is available for all jobs or notebooks attached to the cluster, including those running streaming workloads.  
# MAGIC - The syntax for specifying the package is:  
# MAGIC   - Library Source: PyPI  
# MAGIC   - Package: `databricks-labs-dqx`
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install DQX as Library <br>
# MAGIC For this demo, we will install DQX as library

# COMMAND ----------

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install '{dbutils.widgets.get("test_library_ref")}'
else:
    %pip install databricks-labs-dqx

%restart_python

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame
from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Stream
# MAGIC
# MAGIC This cell reads the NYC Taxi 2019 dataset from a Delta table as a streaming DataFrame, which will be used as the input for downstream data quality checks and transformations.

# COMMAND ----------

bronze_stream = (
    spark.readStream
    .format("delta")
    .load("/databricks-datasets/delta-sharing/samples/nyctaxi_2019")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Data Quality Checks
# MAGIC
# MAGIC This cell defines a set of data quality checks using YAML syntax. The checks are designed to validate key columns in the NYC Taxi dataset, such as `vendor_id`, `pickup_datetime`, `dropoff_datetime`, `passenger_count`, and `trip_distance`. Each check specifies a function, its arguments, a name, and a criticality level (error or warn). These checks will be used by the DQEngine to enforce data quality rules on the streaming data.

# COMMAND ----------

# Define Data Quality checks
import yaml


checks = yaml.safe_load("""
- check:
    function: is_not_null
    arguments:
      column: vendor_id
  name: vendor_id_is_null
  criticality: error
- check:
    function: is_not_null_and_not_empty
    arguments:
      column: vendor_id
      trim_strings: true
  name: vendor_id_is_null_or_empty
  criticality: error

- check:
    function: is_not_null
    arguments:
      column: pickup_datetime
  name: pickup_datetime_is_null
  criticality: error
- check:
    function: is_not_in_future
    arguments:
      column: pickup_datetime
  name: pickup_datetime_isnt_in_range
  criticality: warn

- check:
    function: is_not_in_future
    arguments:
      column: pickup_datetime
  name: pickup_datetime_not_in_future
  criticality: warn
- check:
    function: is_not_in_future
    arguments:
      column: dropoff_datetime
  name: dropoff_datetime_not_in_future
  criticality: warn
- check:
    function: is_not_null
    arguments:
      column: passenger_count
  name: passenger_count_is_null
  criticality: error
- check:
    function: is_in_range
    arguments:
      column: passenger_count
      min_limit: 0
      max_limit: 6
  name: passenger_incorrect_count
  criticality: warn
- check:
    function: is_not_null
    arguments:
      column: trip_distance
  name: trip_distance_is_null
  criticality: error
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply data quality checks to the bronze_stream DataFrame using the defined checks metadata
# MAGIC The resulting DataFrame, bronze_dq_checked, contains data with quality check results attached

# COMMAND ----------

dq_engine = DQEngine(WorkspaceClient())

def apply_data_quality(df: DataFrame) -> DataFrame:
    return dq_engine.apply_checks_by_metadata(df, checks)

bronze_dq_checked = apply_data_quality(bronze_stream)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract Valid Records for Silver Table
# MAGIC  
# MAGIC This cell defines a function `get_valid` that extracts valid records from the data quality-checked streaming DataFrame using the DQEngine. The resulting DataFrame, `silver_df`, contains only records that passed all data quality checks and is ready for further processing or writing to the silver table.

# COMMAND ----------

def get_valid(df: DataFrame) -> DataFrame:
    return dq_engine.get_valid(df)

silver_df = get_valid(bronze_dq_checked)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract Invalid Records for Quarantine Table
# MAGIC  
# MAGIC This cell defines a function `get_invalid` that extracts invalid records from the data quality-checked streaming DataFrame using the DQEngine. The resulting DataFrame, `quarantine_df`, contains only records that did not pass all data quality checks and is not ready for further processing or writing to the quarantine table.

# COMMAND ----------

def get_invalid(df: DataFrame) -> DataFrame:
    return dq_engine.get_invalid(df)

quarantine_df = get_invalid(bronze_dq_checked)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write streaming DataFrames to Delta tables with checkpointing
# MAGIC - silver_df: Valid records after data quality checks
# MAGIC - quarantine_df: Invalid records (quarantined)
# MAGIC
# MAGIC and
# MAGIC - Uses availableNow trigger for batch-like streaming
# MAGIC - Checkpoint and table locations are set via widgets

# COMMAND ----------

dbutils.widgets.text("silver_checkpoint", "/tmp/dq/structured/bronze_to_silver_checkpoint")
dbutils.widgets.text("silver_table", "/tmp/dq/structured/silver_table")
dbutils.widgets.text("quarantine_checkpoint", "/tmp/dq/structured/bronze_to_quarantine_checkpoint")
dbutils.widgets.text("quarantine_table", "/tmp/dq/structured/quarantine_table")

# COMMAND ----------

silver_checkpoint = dbutils.widgets.get("silver_checkpoint")
silver_table = dbutils.widgets.get("silver_table")
quarantine_checkpoint = dbutils.widgets.get("quarantine_checkpoint")
quarantine_table = dbutils.widgets.get("quarantine_table")

# COMMAND ----------

# Function to write a streaming DataFrame to a Delta table with checkpointing
def write_stream(df, checkpoint_location, target):
    # Configure the streaming write with Delta format, append mode, checkpointing, and trigger
    writer = (
        df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_location)
            .trigger(availableNow=True)
    )
    # Use toTable for managed tables, start for path-based tables
    if target.startswith("/") or target.startswith("dbfs:/"):
        return writer.start(target)
    else:
        return writer.toTable(target)

# Start streaming queries for silver and quarantine tables
silver_query: DataStreamWriter = write_stream(silver_df, silver_checkpoint, silver_table)
quarantine_query: DataStreamWriter = write_stream(quarantine_df, quarantine_checkpoint, quarantine_table)

# COMMAND ----------

# Wait for the streams to finish or keep them running for interactive sessions if required
silver_query.awaitTermination()
quarantine_query.awaitTermination()