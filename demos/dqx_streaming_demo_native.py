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
# MAGIC       <li>Unity Catalog table (e.g., <code>catalog.schema.table</code>)</li>
# MAGIC     </ul>
# MAGIC   </li>
# MAGIC </ul>
# MAGIC

# COMMAND ----------

# DBTITLE 1,Set Catalog,  Schema & checkpoint location for Demo Dataset
default_catalog_name = "main"
default_schema_name = "default"

dbutils.widgets.text("demo_catalog_name", default_catalog_name, "Catalog Name")
dbutils.widgets.text("demo_schema_name", default_schema_name, "Schema Name")

dbutils.widgets.text("silver_checkpoint", "/tmp/dq/structured/bronze_to_silver_checkpoint")
dbutils.widgets.text("quarantine_checkpoint", "/tmp/dq/structured/bronze_to_quarantine_checkpoint")

# COMMAND ----------

from uuid import uuid4

catalog = dbutils.widgets.get("demo_catalog_name")
schema = dbutils.widgets.get("demo_schema_name")

print(f"Selected Catalog for Demo Dataset: {catalog}")
print(f"Selected Schema for Demo Dataset: {schema}")

silver_checkpoint = dbutils.widgets.get("silver_checkpoint")
quarantine_checkpoint = dbutils.widgets.get("quarantine_checkpoint")
uuid = uuid4()
silver_table = f"{catalog}.{schema}.`silver_{uuid}`"
quarantine_table = f"{catalog}.{schema}.`quarantine_{uuid}`"

print(f"Demo Silver Table: {silver_table}")
print(f"Demo Quarantine Table: {quarantine_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply data quality checks to streaming daat using DQEngine Native method
# MAGIC This script performs data quality checks on a Parquet dataset using Databricks Labs DQX.
# MAGIC
# MAGIC 1. Reads a sample NYC Taxi dataset and writes it as Parquet files to a bronze location. This is needed for a sample parquet file(s) for streaming ingestion.
# MAGIC 2. Configures input for streaming ingestion using Auto Loader (cloudFiles) with schema tracking.
# MAGIC 3. Sets up output configurations for both the silver table and a quarantine table, specifying Delta format, checkpoint locations, and schema merging.
# MAGIC 4. Instantiates the DQEngine.
# MAGIC 5. Using DQEngine native method `apply_checks_by_metadata_and_save_in_table`, Applies data quality checks (defined in `checks`) to the input data stream, saving valid records to the silver table and invalid records to the quarantine table.

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import InputConfig, OutputConfig
from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame

# prepare sample test data
bronze_loc = "/tmp/dq/bronze"
bronze_df = spark.read.load("/databricks-datasets/delta-sharing/samples/nyctaxi_2019")
bronze_df.write.mode("overwrite").format("parquet").save(f"{bronze_loc}/data")

input_config=InputConfig(
      location=f"{bronze_loc}/data",
      format="cloudFiles", 
      options={"cloudFiles.format": "parquet", "cloudFiles.schemaLocation": f"{bronze_loc}/schema"},
      is_streaming=True
)

output_config=OutputConfig(
      location=silver_table,
      format="delta",
      mode="append",
      trigger={"availableNow": True},
      options={"checkpointLocation": f"{silver_checkpoint}", "mergeSchema": "true"}
)

quarantine_config=OutputConfig(
      location=quarantine_table,
      format="delta",
      mode="append",
      trigger={"availableNow": True},
      options={"checkpointLocation": f"{quarantine_checkpoint}", "mergeSchema": "true"}
)

dq_engine = DQEngine(WorkspaceClient())

dq_engine.apply_checks_by_metadata_and_save_in_table(
    checks=checks,
    input_config=input_config,
    output_config=output_config,
    quarantine_config=quarantine_config
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo Cleanup

# COMMAND ----------

dbutils.fs.rm(bronze_loc, True)
dbutils.fs.rm(silver_checkpoint, True)
dbutils.fs.rm(quarantine_checkpoint, True)
spark.sql(f"DROP TABLE IF EXISTS {silver_table}")
spark.sql(f"DROP TABLE IF EXISTS {quarantine_table}")