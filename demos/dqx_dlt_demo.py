# Databricks notebook source

%pip install databricks-labs-dqx

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create DLT Pipeline
# MAGIC
# MAGIC Create new DLT Pipeline to execute this notebook (see [here](https://www.databricks.com/discover/pages/getting-started-with-delta-live-tables#define)).
# MAGIC
# MAGIC Go to `Workflows` tab > `Pipelines` > `Create pipeline`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define DLT Pipeline

# COMMAND ----------

import dlt
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

# COMMAND ----------

@dlt.view
def bronze():
  df = spark.readStream.format("delta") \
    .load("/databricks-datasets/delta-sharing/samples/nyctaxi_2019")
  return df

# COMMAND ----------

# Define Data Quality checks
import yaml


checks = yaml.safe_load("""
- check:
    function: "is_not_null"
    arguments:
      col_name: "vendor_id"
  name: "vendor_id_is_null"
  criticality: "error"
- check:
    function: "is_not_null_and_not_empty"
    arguments:
      col_name: "vendor_id"
      trim_strings: true
  name: "vendor_id_is_null_or_empty"
  criticality: "error"

- check:
    function: "is_not_null"
    arguments:
      col_name: "pickup_datetime"
  name: "pickup_datetime_is_null"
  criticality: "error"
- check:
    function: "is_not_in_future"
    arguments:
      col_name: "pickup_datetime"
  name: "pickup_datetime_isnt_in_range"
  criticality: "warn"

- check:
    function: "is_not_in_future"
    arguments:
      col_name: "pickup_datetime"
  name: "pickup_datetime_not_in_future"
  criticality: "warn"
- check:
    function: "is_not_in_future"
    arguments:
      col_name: "dropoff_datetime"
  name: "dropoff_datetime_not_in_future"
  criticality: "warn"
- check:
    function: "is_not_null"
    arguments:
      col_name: "passenger_count"
  name: "passenger_count_is_null"
  criticality: "error"
- check:
    function: "is_in_range"
    arguments:
      col_name: "passenger_count"
      min_limit: 0
      max_limit: 6
  name: "passenger_incorrect_count"
  criticality: "warn"
- check:
    function: "is_not_null"
    arguments:
      col_name: "trip_distance"
  name: "trip_distance_is_null"
  criticality: "error"
""")

# COMMAND ----------

dq_engine = DQEngine(WorkspaceClient())

# Read data from Bronze and apply checks
@dlt.view
def bronze_dq_check():
  df = dlt.read_stream("bronze")
  return dq_engine.apply_checks_by_metadata(df, checks)

# COMMAND ----------

# # get rows without errors or warnings, and drop auxiliary columns
@dlt.table
def silver():
  df = dlt.read_stream("bronze_dq_check")
  return dq_engine.get_valid(df)

# COMMAND ----------

# get only rows with errors or warnings
@dlt.table
def quarantine():
  df = dlt.read_stream("bronze_dq_check")
  return dq_engine.get_invalid(df)
