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
# MAGIC

# COMMAND ----------

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install '{dbutils.widgets.get("test_library_ref")}'
else:
    %pip install databricks-labs-dqx

%restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Data Quality Checks
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
# MAGIC ### Inputs
# MAGIC - Checkpoint and table locations are set via widgets

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h3>Checkpoint and Table Location Options</h3>
# MAGIC <ul>
# MAGIC   <li>
# MAGIC     <b>Checkpoint Location</b>:
# MAGIC     <ul>
# MAGIC       <li>Local path (e.g., <code>/tmp/checkpoints/...</code>)</li>
# MAGIC       <li>Workspace path (e.g., <code>/dbfs/mnt/...</code>)</li>
# MAGIC       <li>Unity Catalog (UC) volume path (e.g., <code>/Volumes/catalog/schema/volume/...</code>)</li>
# MAGIC     </ul>
# MAGIC   </li>
# MAGIC   <li>
# MAGIC     <b>Silver and Quarantine Table</b>:
# MAGIC     <ul>
# MAGIC       <li>Delta table path (e.g., <code>/mnt/delta/silver_table</code>)</li>
# MAGIC       <li>Unity Catalog table (e.g., <code>catalog.schema.table</code>)</li>
# MAGIC     </ul>
# MAGIC   </li>
# MAGIC </ul>
# MAGIC

# COMMAND ----------

dbutils.widgets.text("silver_checkpoint", "/tmp/dq/structured/bronze_to_silver_checkpoint")
dbutils.widgets.text("quarantine_checkpoint", "/tmp/dq/structured/bronze_to_quarantine_checkpoint")
dbutils.widgets.text("silver_table", "/tmp/dq/structured/silver_table")
dbutils.widgets.text("quarantine_table", "/tmp/dq/structured/quarantine_table")

# COMMAND ----------

silver_checkpoint = dbutils.widgets.get("silver_checkpoint")
quarantine_checkpoint = dbutils.widgets.get("quarantine_checkpoint")
silver_table = dbutils.widgets.get("silver_table")
quarantine_table = dbutils.widgets.get("quarantine_table")

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
# MAGIC ### Apply data quality checks and split the bronze stream into silver and quarantine DataFrames

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame

dq_engine = DQEngine(WorkspaceClient())

silver_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(bronze_stream, checks)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write streaming DataFrames to Delta tables with checkpointing

# COMMAND ----------

PATH_PREFIXES = ("/", "dbfs:/", "s3://", "abfss://", "gs://")

def write_stream(df, checkpoint_location, target):
    writer = (
        df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_location)
            .trigger(availableNow=True)  # stop the stream as soon as bronze data is processed
    )
    if target.startswith(PATH_PREFIXES):
        return writer.start(target)
    else:
        return writer.toTable(target)

silver_query = write_stream(silver_df, silver_checkpoint, silver_table)
quarantine_query = write_stream(quarantine_df, quarantine_checkpoint, quarantine_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wait for the streams to finish or keep them running for interactive sessions if required

# COMMAND ----------

silver_query.awaitTermination()
quarantine_query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo Clearup

# COMMAND ----------

dbutils.fs.rm(silver_checkpoint, True)
dbutils.fs.rm(quarantine_checkpoint, True)
dbutils.fs.rm(silver_table, True)
dbutils.fs.rm(quarantine_table, True)
spark.sql("DROP TABLE IF EXISTS silver_table")
spark.sql("DROP TABLE IF EXISTS quarantine_table")