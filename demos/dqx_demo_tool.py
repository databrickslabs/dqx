# Databricks notebook source
# MAGIC %md
# MAGIC # Demonstrate DQX usage when installed in the workspace
# MAGIC ### Installation of DQX in the workspace
# MAGIC
# MAGIC Install DQX in the workspace using default user installation as per the instructions [here](https://github.com/databrickslabs/dqx?tab=readme-ov-file#installation).
# MAGIC
# MAGIC Run in your terminal: `databricks labs install dqx`
# MAGIC
# MAGIC When prompt provide the following:
# MAGIC * Input data location: `/databricks-datasets/delta-sharing/samples/nyctaxi_2019`
# MAGIC * Input format: `delta`
# MAGIC * Output table: valid qualified table name (catalog.schema.table or schema.table). The output data will be saved there as part of the demo.
# MAGIC * Quarantined table: valid qualified table name (catalog.schema.table or schema.table). The quarantined data will be saved there as part of the demo.
# MAGIC * Filename for data quality rules (checks): use default (`checks.yml`)
# MAGIC * Filename for profile summary statistics: use default (`profile_summary_stats.yml`)
# MAGIC
# MAGIC You can open the config and update it if needed after the installation.
# MAGIC
# MAGIC Run in your terminal: `databricks labs dqx open-remote-config`
# MAGIC
# MAGIC The config should look like this:
# MAGIC ```yaml
# MAGIC log_level: INFO
# MAGIC version: 1
# MAGIC run_configs:
# MAGIC - name: default
# MAGIC   input_config:
# MAGIC     location: /databricks-datasets/delta-sharing/samples/nyctaxi_2019
# MAGIC     format: delta
# MAGIC   output_config:
# MAGIC     location: main.nytaxi.output
# MAGIC     mode: overwrite
# MAGIC   quarantine_config:
# MAGIC     location: main.nytaxi.quarantine
# MAGIC     mode: overwrite
# MAGIC   checks_location: checks.yml
# MAGIC   profiler_config:
# MAGIC     summary_stats_file: profile_summary_stats.yml
# MAGIC   warehouse_id: your-warehouse-id
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Installation of DQX in the Databricks cluster
# MAGIC Once DQX is installed in the workspace, we install it in the DQX library in the cluster.

# COMMAND ----------

import glob
import os

user_name = spark.sql("select current_user() as user").collect()[0]["user"]
default_dqx_installation_path = f"/Workspace/Users/{user_name}/.dqx"
default_dqx_product_name = "dqx"

dbutils.widgets.text("dqx_installation_path", default_dqx_installation_path, "DQX Installation Folder")
dbutils.widgets.text("dqx_product_name", default_dqx_product_name, "DQX Product Name")

dqx_wheel_files_path = f"{dbutils.widgets.get('dqx_installation_path')}/wheels/databricks_labs_dqx-*.whl"
dqx_wheel_files = glob.glob(dqx_wheel_files_path)
try:
  dqx_latest_wheel = max(dqx_wheel_files, key=os.path.getctime)
except:
  raise ValueError(f"No files in path: {dqx_wheel_files_path}")

%pip install {dqx_latest_wheel}
%restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run profiler workflow to generate quality rule candidates
# MAGIC
# MAGIC The profiler generates and saves quality rule candidates (checks), offering an initial set of quality checks that can be customized and refined as needed.
# MAGIC
# MAGIC Run in your terminal: `databricks labs dqx profile --run-config "default"`
# MAGIC
# MAGIC You can also start the profiler by navigating to the Databricks Workflows UI.
# MAGIC
# MAGIC Note that using the profiler is optional. It is usually one-time operation and not a scheduled activity. The generated check candidates should be manually reviewed before being applied to the data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run profiler from the code to generate quality rule candidates
# MAGIC
# MAGIC You can also run the profiler from the code directly, instead of using the profiler.

# COMMAND ----------

import yaml
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import InstallationChecksStorageConfig, WorkspaceFileChecksStorageConfig
from databricks.labs.dqx.config_loader import RunConfigLoader
from databricks.labs.dqx.io import read_input_data
from databricks.sdk import WorkspaceClient


dqx_product_name = dbutils.widgets.get("dqx_product_name")

ws = WorkspaceClient()
dq_engine = DQEngine(ws)

# load the run configuration
run_config = RunConfigLoader(ws).load_run_config(run_config_name="default", product_name=dqx_product_name)

# read the input data, limit to 1000 rows for demo purpose
input_df = read_input_data(spark, run_config.input_config).limit(1000)

# profile the input data
profiler = DQProfiler(ws)
# sample 30% of the data and limit to 1000 records by default
summary_stats, profiles = profiler.profile(input_df)
print(summary_stats)
print(profiles)

# generate DQX quality rules/checks
generator = DQGenerator(ws)
checks = generator.generate_dq_rules(profiles)  # with default level "error"
print(yaml.safe_dump(checks))

# save generated checks to location specified in the default run configuration inside workspace installation folder
dq_engine.save_checks(checks, config=InstallationChecksStorageConfig(run_config_name="default", product_name=dqx_product_name))

# or save checks in arbitrary workspace location
#dq_engine.save_checks(checks, config=WorkspaceFileChecksStorageConfig(location="/Shared/App1/checks.yml"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare checks manually and save in the workspace (optional)
# MAGIC
# MAGIC You can modify the check candidates generated by the profiler to suit your needs. Alternatively, you can create checks manually, as demonstrated below, without using the profiler.

# COMMAND ----------

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.config import InstallationChecksStorageConfig, WorkspaceFileChecksStorageConfig


checks = yaml.safe_load("""
- check:
    function: is_not_null
    for_each_column:
    - vendor_id
    - pickup_datetime
    - dropoff_datetime
    - passenger_count
    - trip_distance
    - pickup_longitude
    - pickup_latitude
    - dropoff_longitude
    - dropoff_latitude
  criticality: warn
  filter: total_amount > 0
- check:
    function: is_not_less_than
    arguments:
      column: trip_distance
      limit: 1
  criticality: error
  filter: tip_amount > 0
- check:
    function: sql_expression
    arguments:
      expression: pickup_datetime <= dropoff_datetime
      msg: pickup time must not be greater than dropoff time
      name: pickup_datetime_greater_than_dropoff_datetime
  criticality: error
- check:
    function: is_not_in_future
    arguments:
      column: pickup_datetime
  name: pickup_datetime_not_in_future
  criticality: warn
""")

# validate the checks
status = DQEngine.validate_checks(checks)
print(status)
assert not status.has_errors

dq_engine = DQEngine(WorkspaceClient())

# save checks to location specified in the default run configuration inside workspace installation folder
dq_engine.save_checks(checks, config=InstallationChecksStorageConfig(run_config_name="default", product_name=dqx_product_name))

# or save checks in arbitrary workspace location
#dq_engine.save_checks(checks, config=WorkspaceFileChecksStorageConfig(location="/Shared/App1/checks.yml"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying quality rules (checks) in the Lakehouse medallion architecture

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.utils import read_input_data
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.config import InstallationChecksStorageConfig, WorkspaceFileChecksStorageConfig
from databricks.labs.dqx.config_loader import RunConfigLoader


dq_engine = DQEngine(WorkspaceClient())

# load the run configuration
run_config = RunConfigLoader(ws).load_run_config(run_config_name="default", assume_user=True, product_name=dqx_product_name)

# read the data, limit to 1000 rows for demo purpose
bronze_df = read_input_data(spark, run_config.input_config).limit(1000)

# apply your business logic here
bronze_transformed_df = bronze_df.filter("vendor_id in (1, 2)")

# load checks from location defined in the run configuration

checks = dq_engine.load_checks(config=InstallationChecksStorageConfig(assume_user=True, run_config_name="default", product_name=dqx_product_name))

# or load checks from arbitrary workspace file
#checks = dq_engine.load_checks(config=WorkspaceFileChecksStorageConfig(location="/Shared/App1/checks.yml"))
print(checks)

# Option 1: apply quality rules and quarantine invalid records
silver_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(bronze_transformed_df, checks)
display(quarantine_df)

# Option 2: apply quality rules and annotate invalid records as additional columns (`_warning` and `_error`)
#silver_valid_and_quarantine_df = dq_engine.apply_checks_by_metadata(bronze_transformed_df, checks)
#display(silver_valid_and_quarantine_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save quarantined data to a table
# MAGIC

# COMMAND ----------

dq_engine.save_results_in_table(
  output_df=silver_df,
  quarantine_df=quarantine_df,
  output_config=run_config.output_config,
  quarantine_config=run_config.quarantine_config,
  product_name=dqx_product_name
)

display(spark.sql(f"SELECT * FROM {run_config.output_config.location}"))
display(spark.sql(f"SELECT * FROM {run_config.quarantine_config.location}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### View data quality in DQX Dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC Note: Dashboard is only using quarantined data as input. If you apply checks to annotate invalid records without quarantining them (e.g. using the `apply_checks_by_metadata` method), ensure that the `quarantine_table` field in your run config is set to the same value as the `output_table` field.

# COMMAND ----------

dashboards_folder_link = f"{dbutils.widgets.get('dqx_installation_path')}/dashboards/"
print(f"Open a dashboard from the following folder and refresh it:")
print(dashboards_folder_link)
