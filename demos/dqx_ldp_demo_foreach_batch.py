# Databricks notebook source
from pyspark import pipelines as dp

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Using DQX in a Lakeflow Declarative Pipeline with a foreachBatch sink
# MAGIC
# MAGIC This demo applies DQX checks and reports issues as additional columns (`_errors` / `_warnings`),
# MAGIC persisting the checked data as a `silver` table. Summary metrics are then computed for each incremental
# MAGIC micro-batch and persisted to a table using a [foreachBatch sink](https://docs.databricks.com/aws/en/ldp/for-each-batch).
# MAGIC The metrics table contains **per-batch summary metrics** about the input data quality.
# MAGIC
# MAGIC **When to use this a foreachBatch sink:**
# MAGIC Use a foreachBatch sink when you want summary metrics computed **incrementally per micro-batch** and
# MAGIC appended as a history with one set of rows per batch, rather than a cumulative full-table snapshot. This
# MAGIC may be **more performant for large or growing tables**. While a materialized view re-aggregates over the
# MAGIC whole checked table on each pipeline update, this example aggregates metrics over only the current micro batch.
# MAGIC If you just need to apply checks and report issues into a table, prefer the simpler `dqx_ldp_demo.py`.
# MAGIC
# MAGIC To see how to use this pattern when quarantining data, see `dqx_ldp_demo_foreach_batch_quarantine.py`.
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
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.sdk import WorkspaceClient

# Create the DQEngine with a metrics observer to get summary metrics
dq_engine = DQEngine(WorkspaceClient(), observer=DQMetricsObserver())

# Read the target catalog and schema are read from the pipeline configuration
output_catalog = spark.conf.get("demo_catalog", "main")
output_schema = spark.conf.get("demo_schema", "dqx_ldp_demo_foreach_batch")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Data Quality checks
# MAGIC Define your quality checks in YAML. These can also be defined using DQX classes or loaded from a table of file.

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
# MAGIC Stream the source data from a Delta table and write it to a bronze-layer table.

# COMMAND ----------

# Bronze: raw input as a streaming table.
@dp.table
def bronze():
  return spark.readStream.format("delta") \
    .load("/databricks-datasets/delta-sharing/samples/nyctaxi_2019")

# COMMAND ----------

# MAGIC %md
# MAGIC ## foreachBatch Sink
# MAGIC Define a foreachBatch sink that applies quality checks using `dq_engine.apply_checks_by_metadata`,
# MAGIC appends the checked data to a silver-layer table, computes the quality metrics for the incremental batch,
# MAGIC and appends the computed metrics to a metrics table.
# MAGIC
# MAGIC Computing metrics with a foreachBatch sink creates a per-batch history (unlike a materialized view
# MAGIC defined using `@dp.table`, which recomputes a snapshot of the metrics over the entire table). This is useful
# MAGIC for tracking data quality of different batches and avoiding dataset-level aggregation over very large tables.

# COMMAND ----------

@dp.foreach_batch_sink(name="silver_sink")
def silver_sink(batch_df, batch_id):
  # Apply the quality checks, report issues in the `_warnings` and `_errors` columns, and append results to the silver table
  checked_df = dq_engine.apply_checks_by_metadata(batch_df, checks)
  checked_df.write.format("delta").mode("append").saveAsTable(f"{output_catalog}.{output_schema}.silver")

  # Compute the summary metrics for each input batch and append them to the metrics table
  # Note: A foreachBatch sink computes metrics as per-batch counts (one set of rows per micro-batch), not a
  # cumulative snapshot; Aggregate across batches downstream if you need running totals
  metrics_df = dq_engine.compute_summary_metrics(checked_df, checks=checks)
  metrics_df.write.format("delta").mode("append").saveAsTable(f"{output_catalog}.{output_schema}.dq_summary_metrics")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver and Metrics Layer
# MAGIC Use `@dp.append_flow` to read the bronze data and run the foreachBatch sink over each micro-batch.

# COMMAND ----------

@dp.append_flow(target="silver_sink")
def silver_flow():
  return spark.readStream.table("bronze")
