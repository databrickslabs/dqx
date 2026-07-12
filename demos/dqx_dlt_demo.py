# Databricks notebook source
import dlt

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## DQX in a Lakeflow Pipeline (formerly Delta Live Tables - DLT)
# MAGIC
# MAGIC This demo applies DQX checks and reports issues as additional columns (`_errors` / `_warnings`),
# MAGIC persisting the checked data as a `silver` table. Summary metrics are then computed as a materialized
# MAGIC view over that table. For a pipeline that quarantines invalid records into a separate table, see
# MAGIC `dqx_dlt_demo_quarantine.py`.
# MAGIC
# MAGIC Create new ETL Pipeline to execute this notebook (see [here](https://docs.databricks.com/aws/en/getting-started/data-pipeline-get-started)):
# MAGIC 1. Upload the notebook to a Databricks Workspace
# MAGIC 2. Go to `Workflows` tab > `Create` > `ETL Pipeline` > `Add existing assets` > select the source code path and root directory
# MAGIC 3. Add DQX library as a [dependency](https://docs.databricks.com/aws/en/dlt/dlt-multi-file-editor#environment) to the pipeline: Go to `Settings` > `Edit environment` > Add `databricks‑labs‑dqx` as dependency
# MAGIC 4. Run the pipeline
# MAGIC
# MAGIC
# MAGIC As an alternative to setting the environment as described above, you can also [install](https://docs.databricks.com/aws/en/dlt/external-dependencies) DQX directly in the notebook. Put the below commands as first cells in the notebook:
# MAGIC
# MAGIC %`pip install databricks-labs-dqx`
# MAGIC
# MAGIC `dbutils.library.restartPython()`
# MAGIC

# COMMAND ----------

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.sdk import WorkspaceClient

# compute_summary_metrics requires an observer on the engine; it reads any custom_metrics from it.
dq_engine = DQEngine(WorkspaceClient(), observer=DQMetricsObserver())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Data Quality checks

# COMMAND ----------

# Define checks in YAML format. They can also be defined using classes or loaded from a file or table.
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
# MAGIC ## Define Lakeflow Pipeline (bronze -> silver -> metrics)

# COMMAND ----------

# Bronze: raw input as a streaming view.
@dlt.view
def bronze():
  return spark.readStream.format("delta") \
    .load("/databricks-datasets/delta-sharing/samples/nyctaxi_2019")

# COMMAND ----------

# Silver: apply checks and report issues as additional columns (_errors / _warnings).
# Persist as a table so it can be used downstream (including by the metrics view below).
@dlt.table
def silver():
  df = dlt.read_stream("bronze")
  return dq_engine.apply_checks_by_metadata(df, checks)

# COMMAND ----------

# Summary Metrics: materialized view computed by aggregation over the silver table.
# One row per metric (input / error / warning / valid row counts and per-check breakdown).
# Note: this MV is a cumulative snapshot over the whole table (input_row_count is the running
# total, not a per-run count). It refreshes incrementally only when the query is deterministic —
# set static run_time_overwrite / run_id_overwrite in ExtraParams for that. For per-run / per-window
# metrics on large or incrementally-checked tables, append from a windowed streaming table instead
# (see the "Snapshot vs. history" section of the Summary Metrics guide).
@dlt.table
def dq_summary_metrics():
  df = dlt.read("silver")
  return dq_engine.compute_summary_metrics(df, checks=checks)
