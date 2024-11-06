# Databricks notebook source
# 1. Install DQX in the workspace as per the instructions here: https://github.com/databrickslabs/dqx?tab=readme-ov-file#installation

# 2. Install DQX in the cluster
user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[2]
pip_install_path = f"/Workspace/Users/{user_name}/.dqx/wheels/databricks_labs_dqx-*.whl"
%pip install {pip_install_path}

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
from databricks.labs.dqx.engine import apply_checks_by_metadata, get_invalid, get_valid

# COMMAND ----------

@dlt.view
def bronze():
  df = spark.readStream.format("delta") \
    .load("/databricks-datasets/delta-sharing/samples/nyctaxi_2019")
  return df

# COMMAND ----------

# Define our Data Quality cheks
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
    function: "is_in_range"
    arguments:
      col_name: "pickup_datetime"
      min_limit: "2019-01-01T00:00:00.000000"
      max_limit: "2020-01-01T00:00:00.000000"
  name: "pickup_datetime_isnt_in_range"
  criticality: "warn"

- check:
    function: "is_not_null"
    arguments:
      col_name: "dropoff_datetime"
  name: "dropoff_datetime_is_null"
  criticality: "error"

- check:
    function: "is_in_range"
    arguments:
      col_name: "dropoff_datetime"
      min_limit: "2019-01-01T00:00:00.000000"
      max_limit: "2020-01-02T00:00:00.000000"
  name: "dropoff_datetime_isnt_in_range"
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

# Read data from Bronze and apply checks
@dlt.view
def bronze_dq_check():
  df = dlt.read_stream("bronze")
  return apply_checks_by_metadata(df, checks)

# COMMAND ----------

# # get rows without errors or warnings, and drop auxiliary columns
@dlt.table
def silver():
  df = dlt.read_stream("bronze_dq_check")
  return get_valid(df)

# COMMAND ----------

# get only rows with errors or warnings
@dlt.table
def quarantine():
  df = dlt.read_stream("bronze_dq_check")
  return get_invalid(df)
