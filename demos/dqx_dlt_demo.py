# Databricks notebook source

%pip install databricks-labs-dqx

# Alternatively, you can add DQX as a dependency directly in your pipeline: go to Settings > Environment > Edit Environment > Dependencies, and add databricks‑labs‑dqx
# In this case, you don't need to run %pip install in the notebook, and you won't have to restart the Python process. The library will already be available on your cluster at startup.
# See more here: https://docs.databricks.com/aws/en/dlt/dlt-multi-file-editor#environment

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Lakeflow Pipeline (formerly Delta Live Tables - DLT)
# MAGIC
# MAGIC Create new ETL Pipeline to execute this notebook (see [here](https://docs.databricks.com/aws/en/getting-started/data-pipeline-get-started)).
# MAGIC
# MAGIC Go to `Workflows` tab > `Create` > `ETL Pipeline`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Lakeflow Pipeline

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