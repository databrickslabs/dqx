# Databricks notebook source
import dlt

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## DQX in a Lakeflow Pipeline (formerly Delta Live Tables - DLT) — data quality with quarantine pattern
# MAGIC
# MAGIC This demo applies DQX checks and **splits** the data into a valid `silver` table and a `quarantine`
# MAGIC table. It also persists the checked layer (`bronze_dq_check`) so summary metrics can be computed as
# MAGIC a materialized view over all rows. For a simpler pipeline that reports issues as columns without
# MAGIC quarantining, see `dqx_dlt_demo.py`.
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

# Define checks in YAML format. They can also be defined using classes or loaded from a file or a table.
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
# MAGIC ## Define Lakeflow Pipeline (bronze -> silver + quarantine -> metrics)

# COMMAND ----------

# Bronze: raw input as a streaming view.
@dlt.view
def bronze():
  return spark.readStream.format("delta") \
    .load("/databricks-datasets/delta-sharing/samples/nyctaxi_2019")

# COMMAND ----------

# Apply checks and persist the checked data as a table so the summary-metrics materialized view
# below can read the result columns (_errors/_warnings) after the pipeline writes them.
@dlt.table
def bronze_dq_check():
  df = dlt.read_stream("bronze")
  return dq_engine.apply_checks_by_metadata(df, checks)

# COMMAND ----------

# Silver: rows without errors or warnings, with the auxiliary result columns dropped.
@dlt.table
def silver():
  df = dlt.read_stream("bronze_dq_check")
  return dq_engine.get_valid(df)

# COMMAND ----------

# Quarantine: only rows with errors or warnings.
@dlt.table
def quarantine():
  df = dlt.read_stream("bronze_dq_check")
  return dq_engine.get_invalid(df)

# COMMAND ----------

# Summary Metrics: materialized view computed by aggregation over the checked table. 
# One row per metric (input / error / warning / valid row counts and per-check breakdown).
@dlt.table
def dq_summary_metrics():
  df = dlt.read("bronze_dq_check")
  return dq_engine.compute_summary_metrics(df, checks=checks)
