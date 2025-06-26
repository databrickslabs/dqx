# Databricks notebook source
# MAGIC %md
# MAGIC # DQX - Use as library demo
# MAGIC
# MAGIC In this demo we demonstrate how to create and apply a set of rules from YAML configuration. 
# MAGIC
# MAGIC **Note:**
# MAGIC This notebook can be executed without any modifications when using the `VS Code Databricks Extension`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install DQX

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Required Modules

# COMMAND ----------

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession, Row

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configure Test Data
# MAGIC
# MAGIC The result of this step is `new_users_df`, which represents a dataframe of new users which requires quality validation.

# COMMAND ----------

spark = SparkSession.builder.appName("DQX_demo_library").getOrCreate()

# Create a sample DataFrame
new_users_sample_data = [
    Row(id=1, age=23, country='Germany'),
    Row(id=2, age=30, country='France'),
    Row(id=3, age=16, country='Germany'), # Invalid -> age - LT 18
    Row(id=None, age=29, country='France'), # Invalid -> id - NULL
    Row(id=4, age=29, country=''), # Invalid -> country - Empty
    Row(id=5, age=23, country='Italy'), # Invalid -> country - not in
    Row(id=6, age=123, country='France') # Invalid -> age - GT 120
]

new_users_df = spark.createDataFrame(new_users_sample_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demonstrating Functions
# MAGIC - `is_not_null_and_not_empty`
# MAGIC - `is_in_range`
# MAGIC - `is_in_list`
# MAGIC
# MAGIC You can find a list of all available built-in checks in the documentation [here](https://databrickslabs.github.io/dqx/docs/reference/quality_rules/) and in the code [here](https://github.com/databrickslabs/dqx/blob/main/src/databricks/labs/dqx/check_funcs.py).
# MAGIC
# MAGIC We are demonstrating creating and validating a set of `Quality Checks` defined declaratively using YAML.
# MAGIC
# MAGIC We can use `validate_checks` to verify that the checks are defined correctly.

# COMMAND ----------

checks_from_yaml = yaml.safe_load("""
- check:
    function: is_not_null_and_not_empty
    for_each_column:  # define check for multiple columns at once
      - id
      - age
      - country
  criticality: error
- check:
    function: is_in_range
    filter: country in ['Germany', 'France']
    arguments:
      column: age  # define check for a single column
      min_limit: 18
      max_limit: 120
  criticality: warn
  name: age_not_in_range  # optional check name, auto-generated if not provided
- check:
    function: is_in_list
    for_each_column:
      - country
    arguments:
      allowed:
        - Germany
        - France
  criticality: warn
""")

# Validate YAML checks
status = DQEngine.validate_checks(checks_from_yaml)
print(f"Checks from YAML: {status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup `DQEngine`

# COMMAND ----------

ws = WorkspaceClient()  # auto-authenticated inside Databricks
dq_engine = DQEngine(ws)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Rules
# MAGIC `apply_checks_by_metadata` results in one `DataFrame` with `_errors` and `_warnings` metadata columns added.

# COMMAND ----------

validated_df = dq_engine.apply_checks_by_metadata(new_users_df, checks_from_yaml)
display(validated_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Rules And Split
# MAGIC `apply_checks_by_metadata_and_split` results in a `tuple[DataFrame, DataFrame]` with `_errors` and `_warnings` metadata columns added. The first DF contains valid records, and the second invalid/quarantined records.

# COMMAND ----------

valid_records_df, invalid_records_df = dq_engine.apply_checks_by_metadata_and_split(new_users_df, checks_from_yaml)
display(valid_records_df)

# COMMAND ----------

display(invalid_records_df)