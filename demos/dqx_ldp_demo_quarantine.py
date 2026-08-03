  # Databricks notebook source
from pyspark import pipelines as dp

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Using DQX in a Lakeflow Declarative Pipeline (formerly Delta Live Tables - DLT) — data quality with quarantine pattern
# MAGIC
# MAGIC This demo applies DQX checks and **splits** the data into a valid `silver` table and a `quarantine`
# MAGIC table. It also persists the checked layer (`bronze_dq_check`) so summary metrics can be computed as
# MAGIC a materialized view over all rows. For a simpler pipeline that reports issues as columns without
# MAGIC quarantining, see `dqx_ldp_demo.py`.
# MAGIC
# MAGIC This is the recommended approach for quarantining data with DQX in a Lakeflow Declarative Pipeline. The
# MAGIC summary-metrics materialized view gives a cumulative snapshot over the whole table. If instead you want
# MAGIC summary metrics computed incrementally per micro-batch and appended as a history, use the foreachBatch
# MAGIC sink variant in `dqx_ldp_demo_foreach_batch_quarantine.py`.
# MAGIC
# MAGIC Create a new ETL Pipeline to execute this notebook (see [here](https://docs.databricks.com/aws/en/getting-started/data-pipeline-get-started)):
# MAGIC 1. Upload the notebook to a Databricks Workspace
# MAGIC 2. Go to `Workflows` tab > `Create` > `ETL Pipeline` > `Add existing assets` > select the source code path and root directory
# MAGIC 3. Add the DQX library as a [dependency](https://docs.databricks.com/aws/en/dlt/dlt-multi-file-editor#environment) to the pipeline: Go to `Settings` > `Edit environment` > Add `databricks‑labs‑dqx` as a dependency
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
from databricks.labs.dqx.config import ExtraParams
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.sdk import WorkspaceClient

# Set a static 'run_time' and 'run_id' to avoid recomputing any materialized views
extra_params = ExtraParams(
    run_time_overwrite="2025-01-01T00:00:00",
    run_id_overwrite="lakeflow_pipeline_run",
)
dq_engine = DQEngine(WorkspaceClient(), observer=DQMetricsObserver(), extra_params=extra_params)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Data Quality checks
# MAGIC Define your quality checks in YAML. These can also be defined using DQX classes or loaded from a table or file.

# COMMAND ----------

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
# MAGIC ## Bronze Layer
# MAGIC Stream the source data from a Delta table as a virtualized view.

# COMMAND ----------

@dp.view
def bronze():
  return spark.readStream.format("delta") \
    .load("/databricks-datasets/delta-sharing/samples/nyctaxi_2019")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Quality Checks
# MAGIC Stream the `bronze` data and apply quality checks using `dq_engine.apply_checks_by_metadata`. Pass the
# MAGIC streaming DataFrame and the checks. Issues are reported in additional metadata columns `_warnings` and
# MAGIC `_errors`. The checked data is persisted to a `bronze_dq_check` table so the valid, quarantine, and
# MAGIC summary-metrics datasets below can read it.

# COMMAND ----------

@dp.table
def bronze_dq_check():
  df = spark.readStream.table("bronze")
  return dq_engine.apply_checks_by_metadata(df, checks)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer
# MAGIC Read the checked `bronze_dq_check` data and write the valid rows without errors or warnings,
# MAGIC with the auxiliary result columns dropped, to a `silver` table using `dq_engine.get_valid`.

# COMMAND ----------

@dp.table
def silver():
  df = spark.readStream.table("bronze_dq_check")
  return dq_engine.get_valid(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantine Layer
# MAGIC Read the checked `bronze_dq_check` data and write only the rows with errors or warnings to a
# MAGIC `quarantine` table using `dq_engine.get_invalid`.

# COMMAND ----------

@dp.table
def quarantine():
  df = spark.readStream.table("bronze_dq_check")
  return dq_engine.get_invalid(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Metrics
# MAGIC
# MAGIC Compute data quality summary metrics as a materialized view by aggregating over the `bronze_dq_check` table.
# MAGIC Using `compute_summary_metrics` builds an output DataFrame with one row per metric. The metrics include
# MAGIC both a per-check breakdown and the input, error, warning, and valid row counts.
# MAGIC
# MAGIC This materialized view computes a **cumulative snapshot** over the entire checked table. The `input_row_count`
# MAGIC is the running total number of rows in the table, not a per-run count.  To keep a **per-batch history** of
# MAGIC metrics instead of a cumulative snapshot and to avoid re-aggregating the whole table on each update,
# MAGIC persist the metrics using a [foreachBatch sink](https://docs.databricks.com/aws/en/ldp/for-each-batch)
# MAGIC over each micro-batch instead. See `dqx_ldp_demo_foreach_batch_quarantine.py` for an example.

# COMMAND ----------

@dp.table
def dq_summary_metrics():
  df = spark.read.table("bronze_dq_check")
  return dq_engine.compute_summary_metrics(df, checks=checks)
