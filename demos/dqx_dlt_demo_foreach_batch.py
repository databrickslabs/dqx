# Databricks notebook source
from pyspark import pipelines as dp

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## DQX in a Lakeflow Pipeline using a foreachBatch sink
# MAGIC
# MAGIC This demo applies DQX checks in a streaming Lakeflow pipeline, reports issues as additional columns
# MAGIC (`_errors` / `_warnings`) in a `silver` table, and appends **per-batch summary metrics** — one set of
# MAGIC counts computed and appended for each micro-batch to a `dq_summary_metrics` table.
# MAGIC
# MAGIC **When to use this (foreachBatch sink) instead of the simple pipeline (`dqx_dlt_demo.py`):**
# MAGIC use it when you want summary metrics computed **incrementally per micro-batch** and appended as a
# MAGIC history (one row set per batch), rather than the cumulative full-table snapshot the materialized-view
# MAGIC demo produces. This is also **potentially more performant on large or growing tables**: metrics are
# MAGIC aggregated over only the current micro-batch, whereas the materialized view re-aggregates over the
# MAGIC whole checked table on each pipeline update. If you just need to apply checks and report issues into a
# MAGIC table, prefer the simpler `dqx_dlt_demo.py`.
# MAGIC
# MAGIC For the quarantine variant of this pattern, see `dqx_dlt_demo_foreach_batch_quarantine.py`.
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

# A foreachBatch sink writes to tables *outside* the pipeline, so it must use fully-qualified names
# (unqualified names would be captured as pipeline-managed streaming tables and rejected). The target
# catalog and schema are read from the pipeline configuration, defaulting to the demo schema.
output_catalog = spark.conf.get("demo_catalog", "main")
output_schema = spark.conf.get("demo_schema", "dqx_dlt_demo_foreach_batch")

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
# MAGIC ## Define Lakeflow Pipeline (bronze -> foreachBatch sink -> silver + metrics)

# COMMAND ----------

# Bronze: raw input as a streaming table.
@dp.table
def bronze():
  return spark.readStream.format("delta") \
    .load("/databricks-datasets/delta-sharing/samples/nyctaxi_2019")

# COMMAND ----------

# foreachBatch sink: apply checks per micro-batch, append the checked data, and append summary metrics
# for that batch. Computing metrics inside the sink gives a per-batch history, unlike a @dp.table
# materialized view which recomputes a cumulative snapshot over the whole table.
# The sink writes to fully-qualified tables outside the pipeline (see output_catalog/output_schema above).
@dp.foreach_batch_sink(name="silver_sink")
def silver_sink(batch_df, batch_id):
  # Apply checks and report issues as additional columns (_errors / _warnings).
  checked_df = dq_engine.apply_checks_by_metadata(batch_df, checks)
  checked_df.write.format("delta").mode("append").saveAsTable(f"{output_catalog}.{output_schema}.silver")

  # Summary metrics: computed per micro-batch and appended to a metrics table.
  # Note: with a foreachBatch sink these are per-batch counts (one set of rows per micro-batch), not a
  # cumulative snapshot; aggregate across batches downstream if you need running totals.
  metrics_df = dq_engine.compute_summary_metrics(checked_df, checks=checks)
  metrics_df.write.format("delta").mode("append").saveAsTable(f"{output_catalog}.{output_schema}.dq_summary_metrics")

# COMMAND ----------

# Wire the streaming bronze table into the sink via an append flow.
@dp.append_flow(target="silver_sink")
def silver_flow():
  return spark.readStream.table("bronze")
