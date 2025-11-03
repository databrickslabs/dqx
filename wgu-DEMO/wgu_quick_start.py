# Databricks notebook source
# MAGIC %md
# MAGIC # DQX - Data Quality Checks for wgu_poc.wgu_bronze.students_data_super_dirty
# MAGIC
# MAGIC This notebook demonstrates how to apply a set of data quality rules (checks) using DQX against
# MAGIC the `wgu_poc.wgu_bronze.students_data_super_dirty` table.
# MAGIC
# MAGIC The rules are defined declaratively in YAML for maintainability and auditability.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install DQX

# COMMAND ----------

# MAGIC %skip
# MAGIC dbutils.widgets.text("test_library_ref", "", "Test Library Ref")
# MAGIC
# MAGIC if dbutils.widgets.get("test_library_ref") != "":
# MAGIC     %pip install '{dbutils.widgets.get("test_library_ref")}'
# MAGIC else:
# MAGIC     %pip install databricks-labs-dqx
# MAGIC
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Students Table

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

students_df = spark.table("wgu_poc.wgu_bronze.students_data_super_dirty")

display(students_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Checks Declaratively in YAML

# COMMAND ----------

import yaml
from databricks.labs.dqx.engine import DQEngine

checks_from_yaml = yaml.safe_load("""
# ==========================================================
# DQX Quality Checks for wgu_poc.wgu_bronze.students_data_super_dirty
# ==========================================================

# 1. Core fields must not be null
- check:
    function: is_not_null_and_not_empty
    for_each_column:
      - student_id
      - student_status
      - load_timestamp
      - month_end_date
      - paid
      - stays_on_campus
  criticality: error
  name: not_null_core_fields

# 2. Student ID must be unique
- check:
    function: is_unique
    arguments:
      columns:
        - student_id
  criticality: error
  name: unique_student_id

# 3.
- check:
    function: regex_match
    for_each_column:
      - student_id
    arguments:
      regex: '^[A-Za-z]{3}\\d{8}$'
      negate: false
  criticality: error
  name: valid_student_id_pattern

# 4. load_timestamp should not be in the future
- check:
    function: is_in_range
    arguments:
      column: load_timestamp
      min_limit: "date('1900-01-01')"
      max_limit: current_date()
  criticality: warn
  name: load_timestamp_not_future

# 5. student_status should be one of expected lifecycle values
- check:
    function: is_in_list
    for_each_column:
      - student_status
    arguments:
      allowed:
        - active
        - inactive
        - graduated
        - withdrawn
        - suspended
  criticality: error
  name: valid_student_status

# 6. paid should only contain boolean values (true/false)
- check:
    function: is_in_list
    for_each_column:
      - paid
    arguments:
      allowed:
        - true
        - false
  criticality: error
  name: valid_paid_boolean

# 7. stays_on_campus should only contain boolean values (true/false)
- check:
    function: is_in_list
    for_each_column:
      - stays_on_campus
    arguments:
      allowed:
        - true
        - false
  criticality: error
  name: valid_stays_on_campus_boolean
""")

status = DQEngine.validate_checks(checks_from_yaml)
print(f"Checks from YAML validated: {status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run DQX Checks

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

ws = WorkspaceClient()
dq_engine = DQEngine(ws)

valid_df, invalid_df = dq_engine.apply_checks_by_metadata_and_split(students_df, checks_from_yaml)

display(valid_df)
display(invalid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### (Optional) Write Valid/Invalid DataFrames Back to Tables

# COMMAND ----------

valid_df.write.mode("overwrite").saveAsTable("wgu_poc.dqx_output.valid_rows")
invalid_df.write.mode("overwrite").saveAsTable("wgu_poc.dqx_output.quarantined_rows")
