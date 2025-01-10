# Databricks notebook source
# MAGIC %md
# MAGIC # Using DQX as a Library

# COMMAND ----------

# MAGIC %md
# MAGIC ### Installation of DQX in the cluster

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate quality rule candidates using Profiler

# COMMAND ----------

from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.dlt_generator import DQDltGenerator
from databricks.sdk import WorkspaceClient
import yaml

schema = "col1: int, col2: int, col3: int, col4 int"
input_df = spark.createDataFrame([[1, 3, 3, 1], [2, None, 4, 1]], schema)

ws = WorkspaceClient()
profiler = DQProfiler(ws)
summary_stats, profiles = profiler.profile(input_df)
print(summary_stats)
print(profiles)

# generate DQX quality rules/checks
generator = DQGenerator(ws)
checks = generator.generate_dq_rules(profiles)  # with default level "error"
print(yaml.safe_dump(checks))

# generate DLT expectations
dlt_generator = DQDltGenerator(ws)
dlt_expectations = dlt_generator.generate_dlt_rules(profiles)
print(dlt_expectations)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate quality checks

# COMMAND ----------

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

checks = yaml.safe_load("""
- criticality: "invalid_criticality"
  check:
    function: "is_not_null"
    arguments:
      col_names:
        - "col1"
        - "col2"
""")

dq_engine = DQEngine(WorkspaceClient())
status = dq_engine.validate_checks(checks)
print(status.has_errors)
display(status.errors)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply quality rules using yaml-like dictionary

# COMMAND ----------

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

checks = yaml.safe_load("""
- criticality: "error"
  check:
    function: "is_not_null"
    arguments:
      col_names:
        - "col1"
        - "col2"

- criticality: "error"
  check:
    function: "is_not_null_and_not_empty"
    arguments:
      col_name: "col3"

- criticality: "warn"
  check:
    function: "value_is_in_list"
    arguments:
      col_name: "col4"
      allowed:
        - 1
        - 2
""")

schema = "col1: int, col2: int, col3: int, col4 int"
input_df = spark.createDataFrame([[1, 3, 3, 1], [2, None, 4, 1]], schema)

# Option 1: apply quality rules on the dataframe and provide valid and invalid (quarantined) dataframes, checks are validated automatically
#valid_df, quarantined_df = apply_checks_by_metadata_and_split(input_df, checks)

# Option 2: apply quality rules on the dataframe and report issues as additional columns (`_warning` and `_error`), checks are validated automatically
dq_engine = DQEngine(WorkspaceClient())
valid_and_quarantined_df = dq_engine.apply_checks_by_metadata(input_df, checks)
display(valid_and_quarantined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply quality rules using DQX classes

# COMMAND ----------

from databricks.labs.dqx.col_functions import is_not_null, is_not_null_and_not_empty, value_is_in_list
from databricks.labs.dqx.engine import DQEngine, DQRule, DQRuleColSet
from databricks.sdk import WorkspaceClient

checks = DQRuleColSet( # define rule for multiple columns at once
            columns=["col1", "col2"],
            criticality="error",
            check_func=is_not_null).get_rules() + [
         DQRule( # define rule for a single column
            name='col3_is_null_or_empty',
            criticality='error',
            check=is_not_null_and_not_empty('col3')),
         DQRule( # name auto-generated if not provided
            criticality='warn',
            check=value_is_in_list('col4', ['1', '2']))
        ]

schema = "col1: int, col2: int, col3: int, col4 int"
input_df = spark.createDataFrame([[1, 3, 3, 1], [2, None, 4, 1]], schema)

# Option 1: apply quality rules on the dataframe and provide valid and invalid (quarantined) dataframes
#valid_df, quarantined_df = apply_checks_and_split(input_df, checks)

# Option 2: apply quality rules on the dataframe and report issues as additional columns (`_warning` and `_error`)
dq_engine = DQEngine(WorkspaceClient())
valid_and_quarantined_df = dq_engine.apply_checks(input_df, checks)

display(valid_and_quarantined_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply checks in the medalion architecture

# COMMAND ----------

# Prepare bronze layer
bronze_path = "/tmp/dqx_demo/bronze"
df = spark.read.format("delta").load("/databricks-datasets/delta-sharing/samples/nyctaxi_2019")
df.write.format("delta").mode("overwrite").save(bronze_path)

# COMMAND ----------

# Define our Data Quality cheks
import yaml

checks = yaml.safe_load("""
- check:
    function: "is_not_null"
    arguments:
      col_names:
        - "vendor_id"
        - "pickup_datetime"
        - "dropoff_datetime"
        - "passenger_count"
  criticality: "error"

- check:
    function: "is_not_null_and_not_empty"
    arguments:
      col_name: "vendor_id"
      trim_strings: true
  name: "vendor_id_is_null_or_empty"
  criticality: "error"

- check:
    function: "not_in_future"
    arguments:
      col_name: "pickup_datetime"
  name: "pickup_datetime_not_in_future"
  criticality: "warn"

- check:
    function: "not_in_future"
    arguments:
      col_name: "dropoff_datetime"
  name: "dropoff_datetime_not_in_future"
  criticality: "warn"

- check:
    function: "is_in_range"
    arguments:
      col_name: "passenger_count"
      min_limit: 0
      max_limit: 6
  name: "passenger_incorrect_count"
  criticality: "warn"

- check:
    function: "is_not_null"
    arguments:
      col_name: "trip_distance"
  name: "trip_distance_is_null"
  criticality: "error"
""")

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

dq_engine = DQEngine(WorkspaceClient())

# Apply checks when processing to silver layer
bronze = spark.read.format("delta").load(bronze_path)
silver, quarantine = dq_engine.apply_checks_by_metadata_and_split(bronze, checks)

# COMMAND ----------

display(silver)

# COMMAND ----------

display(quarantine)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create own custom checks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create custom check function

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import Column
from databricks.labs.dqx.col_functions import make_condition

def ends_with_foo(col_name: str) -> Column:
    column = F.col(col_name)
    return make_condition(column.endswith("foo"), f"Column {col_name} ends with foo", f"{col_name}_ends_with_foo")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply custom check function

# COMMAND ----------

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.col_functions import *

# use built-in, custom and sql expression checks
checks = yaml.safe_load(
"""
- criticality: "error"
  check:
    function: "is_not_null_and_not_empty"
    arguments:
      col_name: "col1"
- criticality: "error"
  check:
    function: "ends_with_foo"
    arguments:
      col_name: "col1"
- criticality: "error"
  check:
    function: "sql_expression"
    arguments:
      expression: "col1 LIKE 'str%'"
      msg: "col1 starts with 'str'"
"""
)

schema = "col1: string"
input_df = spark.createDataFrame([["str1"], ["foo"], ["str3"]], schema)

dq_engine = DQEngine(WorkspaceClient())

valid_and_quarantined_df = dq_engine.apply_checks_by_metadata(input_df, checks, globals())
display(valid_and_quarantined_df)