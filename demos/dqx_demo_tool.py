# Databricks notebook source
# MAGIC %md
# MAGIC ## Install DQX

# COMMAND ----------

# MAGIC %md
# MAGIC # Using DQX as a Tool
# MAGIC ### Installation of DQX in the workspace
# MAGIC
# MAGIC Install DQX in the workspace (default user installation) as per the instructions [here](https://github.com/databrickslabs/dqx?tab=readme-ov-file#installation).
# MAGIC
# MAGIC Run in your terminal: `databricks labs install dqx`
# MAGIC
# MAGIC Use default filename for data quality rules (checks.yml).

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
# MAGIC **Note: Using DQX classes to profile, generate quality rules candidates and applying checks is the same as regradless if the tool is installed from PyPi directly or using the Databricks CLI installation. However, there are additional functionalities available when installing DQX:
# MAGIC * CLI commands, e.g. profiler (run profiler job to profile data and generate quality rules candidates), validation of checks.
# MAGIC * Configuration file (`config.yml`).
# MAGIC * Ability to read checks from the workspace location specified in the run config.
# MAGIC
# MAGIC The list will be expanding over time.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare test data frame

# COMMAND ----------

schema = "col1: int, col2: int, col3: int, col4 int"
input_df = spark.createDataFrame([[1, 3, 3, 1], [2, None, 4, 1]], schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save checks to a default location

# COMMAND ----------

# store checks in a workspace file

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat


data = yaml.safe_load("""
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

# Define the local filename and workspace path
local_file_path = "/tmp/checks.yml"
user_name = spark.sql('select current_user() as user').collect()[0]['user']
workspace_file_path = f"/Workspace/Users/{user_name}/.dqx/checks.yml"

# Save the YAML content to a local file
with open(local_file_path, "w") as file:
    yaml.dump(data, file)

# Upload the file to Databricks workspace
ws = WorkspaceClient()
print(f"Uploading checks to {workspace_file_path}")
with open(local_file_path, "rb") as file:
    raw = file.read()
ws.workspace.upload(workspace_file_path, raw, format=ImportFormat.AUTO, overwrite=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply quality rules

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

# use check file specified in the default installation config ('config.yml')
# if filename provided it's a relative path to the workspace installation directory
dq_engine = DQEngine(WorkspaceClient())

# load checks from the default run configuration
checks = dq_engine.load_checks_from_installation(assume_user=True)
#or you can also load checks from a workspace file
# checks = dq_engine.load_checks_from_workspace_file(workspace_file_path)
print(checks)

# Option 2: apply quality rules on the dataframe and report issues as additional columns (`_warning` and `_error`)
valid_and_quarantined_df = dq_engine.apply_checks_by_metadata(input_df, checks)
display(valid_and_quarantined_df)