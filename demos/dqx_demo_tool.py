# Databricks notebook source
# MAGIC %md
# MAGIC ## Install DQX

# COMMAND ----------

# MAGIC %md
# MAGIC # Using DQX when installed in the workspace
# MAGIC ### Installation of DQX in the workspace
# MAGIC
# MAGIC Install DQX as a tool in the workspace using default user installation as per the instructions [here](https://github.com/databrickslabs/dqx?tab=readme-ov-file#installation).
# MAGIC
# MAGIC Run in your terminal: `databricks labs install dqx`
# MAGIC
# MAGIC Use default filename for data quality rules (checks.yml). Provide a valid fully qualified Unity Catalog name for the quarantined table (catalog.schema.table).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Installation of DQX in the cluster

# COMMAND ----------

import glob
import os

user_name = spark.sql('select current_user() as user').collect()[0]['user']
dqx_wheel_files = glob.glob(f"/Workspace/Users/{user_name}/.dqx/wheels/databricks_labs_dqx-*.whl")
dqx_latest_wheel = max(dqx_wheel_files, key=os.path.getctime)
%pip install {dqx_latest_wheel}

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply quality rules from checks stored in a workspace file

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare test data frame

# COMMAND ----------

schema = "col1: int, col2: int, col3: int, col4 int"
input_df = spark.createDataFrame([[1, 3, 3, 1], [2, None, 3, 1], [3, 3, None, 2], [4, 4, None, 10], [5, 5, 5, 11]], schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run profiler job to generate quality rule candidates
# MAGIC
# MAGIC Note that profiling and generating quality rule candidates is normally a one-time operation and is executed as needed.

# COMMAND ----------

# MAGIC %md
# MAGIC Execute the profiler by executing the following command in your terminal: `databricks labs dqx profile --run-config "default"`
# MAGIC
# MAGIC You can also start the profiler by navigating to the Databricks Workflows UI.
# MAGIC
# MAGIC Note that using the profiler is optional. It is usually one-time operation and not a scheduled activity.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare checks manually without profiler job

# COMMAND ----------

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

checks = yaml.safe_load("""
- criticality: error
  check:
    function: is_not_null
    arguments:
      col_names:
      - col1
      - col2
- name: col_col3_is_null_or_empty
  criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments:
      col_name: col3
- criticality: warn
  check:
    function: value_is_in_list
    arguments:
      col_name: col4
      allowed:
      - 1
      - 2
""")

dq_engine = DQEngine(WorkspaceClient())

# you can save checks to location specified in the default run configuration as below or use them directly in the code
dq_engine.save_checks(checks, run_config_name="default")
# or save it to an arbitrary workspace location
#dq_engine.save_checks_in_workspace_file(checks, workspace_path="/Shared/App1/checks.yml")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply quality rules

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

dq_engine = DQEngine(WorkspaceClient())

# load checks from the installation folder from a location specified in the default run configuration
checks = dq_engine.load_checks(assume_user=True, run_config_name="default")
# or load checks from arbitrary workspace file
# checks = dq_engine.load_checks_from_workspace_file(workspace_path="/Shared/App1/checks.yml")
print(checks)

# Option 1: apply quality rules and flag invalid records as additional columns (`_warning` and `_error`)
valid_df, quarantined_df = dq_engine.apply_checks_by_metadata_and_split(input_df, checks)
display(valid_df)
display(quarantined_df)

# Option 2: apply quality rules and flag invalid records as additional columns (`_warning` and `_error`)
valid_and_quarantined_df = dq_engine.apply_checks_by_metadata(input_df, checks)
display(valid_and_quarantined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save quarantined data to Unity Catalog table

# COMMAND ----------

run_config = dq_engine.load_run_config(run_config="default", assume_user=True)

print(f"Saving quarantined data to {run_config.quarantine_table}")
quarantine_catalog, quarantine_schema, _ = run_config.quarantine_table.split('.')

spark.sql(f"CREATE CATALOG IF NOT EXISTS {quarantine_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quarantine_catalog}.{quarantine_schema}")

quarantined_df.write.mode("overwrite").saveAsTable(run_config.quarantine_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### View data quality in DQX Dashboards
# MAGIC
# MAGIC DQX dashboards are installed in the workspace when installing it as a tool using Databricks CLI.

# COMMAND ----------

from databricks.labs.dqx.contexts.workspace import WorkspaceContext

ctx = WorkspaceContext(WorkspaceClient())
dashboards_folder_link = f"{ctx.installation.workspace_link("")}dashboards/"
print(f"Open a dashboard from the following folder and refresh it:")
print(dashboards_folder_link)