# Databricks notebook source
# MAGIC %md
# MAGIC # Demonstrate DQX usage as a Library

# COMMAND ----------

# MAGIC %md
# MAGIC ## Installation of DQX in Databricks cluster

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generation of quality rule candidates using Profiler
# MAGIC Note that profiling and generating quality rule candidates is normally a one-time operation and is executed as needed.

# COMMAND ----------

from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.dlt_generator import DQDltGenerator
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
import yaml

schema = "col1: int, col2: int, col3: int, col4 int"
input_df = spark.createDataFrame([[1, 3, 3, 1], [2, None, 4, 1]], schema)

ws = WorkspaceClient()

# profile the input data
profiler = DQProfiler(ws)
summary_stats, profiles = profiler.profile(input_df)
print(yaml.safe_dump(summary_stats))
print(profiles)

# generate DQX quality rules/checks
generator = DQGenerator(ws)
checks = generator.generate_dq_rules(profiles)  # with default level "error"
print(yaml.safe_dump(checks))

# generate Delta Live Table (DLT) expectations
dlt_generator = DQDltGenerator(ws)

dlt_expectations = dlt_generator.generate_dlt_rules(profiles, language="SQL")
print(dlt_expectations)

dlt_expectations = dlt_generator.generate_dlt_rules(profiles, language="Python")
print(dlt_expectations)

dlt_expectations = dlt_generator.generate_dlt_rules(profiles, language="Python_Dict")
print(dlt_expectations)

# save generated checks in a workspace file
user_name = spark.sql('select current_user() as user').collect()[0]['user']
checks_file = f"/Workspace/Users/{user_name}/dqx_demo_checks.yml"
dq_engine = DQEngine(ws)
dq_engine.save_checks_in_workspace_file(checks, workspace_path=checks_file)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading checks and applying quality rules

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

input_df = spark.createDataFrame([[1, 3, 3, 2], [2, 3, None, 1]], schema)

# load checks
dq_engine = DQEngine(WorkspaceClient())
checks = dq_engine.load_checks_from_workspace_file(workspace_path=checks_file)

# Option 1: apply quality rules and quarantine invalid records
valid_df, quarantined_df = dq_engine.apply_checks_by_metadata_and_split(input_df, checks)
display(valid_df)
display(quarantined_df)

# Option 2: apply quality rules and flag invalid records as additional columns (`_warning` and `_error`)
valid_and_quarantined_df = dq_engine.apply_checks_by_metadata(input_df, checks)
display(valid_and_quarantined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validating quality checks definition

# COMMAND ----------

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

checks = yaml.safe_load("""
- criticality: invalid_criticality
  check:
    function: is_not_null
    arguments:
      col_names:
        - col1
        - col2
""")

dq_engine = DQEngine(WorkspaceClient())

status = dq_engine.validate_checks(checks)
print(status.has_errors)
print(status.errors)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying quality rules using yaml-like dictionary

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

- criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments:
      col_name: col3

- criticality: error
  filter: col1<3
  check:
    function: is_not_null_and_not_empty
    arguments:
      col_name: col4

- criticality: warn
  check:
    function: value_is_in_list
    arguments:
      col_name: col4
      allowed:
        - 1
        - 2
""")

# validate the checks
status = DQEngine.validate_checks(checks)
assert not status.has_errors

schema = "col1: int, col2: int, col3: int, col4 int"
input_df = spark.createDataFrame([[1, 3, 3, 1], [2, None, 4, 1]], schema)

dq_engine = DQEngine(WorkspaceClient())

# Option 1: apply quality rules and quarantine invalid records
valid_df, quarantined_df = dq_engine.apply_checks_by_metadata_and_split(input_df, checks)
display(valid_df)
display(quarantined_df)

# Option 2: apply quality rules and flag invalid records as additional columns (`_warning` and `_error`)
valid_and_quarantined_df = dq_engine.apply_checks_by_metadata(input_df, checks)
display(valid_and_quarantined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying quality rules using DQX classes

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
         DQRule( # define rule with a filter
            name='col_4_is_null_or_empty',
            criticality='error', 
            filter='col1<3',
            check=is_not_null_and_not_empty('col4')),
         DQRule( # name auto-generated if not provided
            criticality='warn',
            check=value_is_in_list('col4', ['1', '2']))
        ]

schema = "col1: int, col2: int, col3: int, col4 int"
input_df = spark.createDataFrame([[1, 3, 3, 1], [2, None, 4, 1]], schema)

dq_engine = DQEngine(WorkspaceClient())

# Option 1: apply quality rules and quarantine invalid records
valid_df, quarantined_df = dq_engine.apply_checks_and_split(input_df, checks)
display(valid_df)
display(quarantined_df)

# Option 2: apply quality rules and flag invalid records as additional columns (`_warning` and `_error`)
valid_and_quarantined_df = dq_engine.apply_checks(input_df, checks)
display(valid_and_quarantined_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying checks in the Lakehouse medallion architecture

# COMMAND ----------

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

checks = yaml.safe_load("""
- check:
    function: is_not_null
    arguments:
      col_names:
        - vendor_id
        - pickup_datetime
        - dropoff_datetime
        - passenger_count
        - trip_distance
  criticality: error
- check:
    function: is_not_null
    arguments:
      col_names:
        - pickup_longitude
        - pickup_latitude
        - dropoff_longitude
        - dropoff_latitude
  criticality: warn
- check:
    function: not_less_than
    arguments:
      col_name: trip_distance
      limit: 1
  criticality: error
- check:
    function: sql_expression
    arguments:
      expression: pickup_datetime > dropoff_datetime
      msg: pickup time must not be greater than dropff time
      name: pickup_datetime_greater_than_dropoff_datetime
  criticality: error
- check:
    function: not_in_future
    arguments:
      col_name: pickup_datetime
  name: pickup_datetime_not_in_future
  criticality: warn
""")

# validate the checks
status = DQEngine.validate_checks(checks)
assert not status.has_errors

dq_engine = DQEngine(WorkspaceClient())

# read the data, limit to 1000 rows for demo purpose
bronze_df = spark.read.format("delta").load("/databricks-datasets/delta-sharing/samples/nyctaxi_2019").limit(1000)

# apply your business logic here
bronze_transformed_df = bronze_df.filter("vendor_id in (1, 2)")

# apply quality checks
silver_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(bronze_transformed_df, checks)

# COMMAND ----------

display(silver_df)

# COMMAND ----------

display(quarantine_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating custom checks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating custom check function

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import Column
from databricks.labs.dqx.col_functions import make_condition

def ends_with_foo(col_name: str) -> Column:
    column = F.col(col_name)
    return make_condition(column.endswith("foo"), f"Column {col_name} ends with foo", f"{col_name}_ends_with_foo")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying custom check function

# COMMAND ----------

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.col_functions import *

# use built-in, custom and sql expression checks
checks = yaml.safe_load(
"""
- criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments:
      col_name: col1
- criticality: error
  check:
    function: ends_with_foo
    arguments:
      col_name: col1
- criticality: error
  check:
    function: sql_expression
    arguments:
      expression: col1 LIKE 'str%'
      msg: col1 starts with 'str'
"""
)

schema = "col1: string"
input_df = spark.createDataFrame([["str1"], ["foo"], ["str3"]], schema)

dq_engine = DQEngine(WorkspaceClient())

valid_and_quarantined_df = dq_engine.apply_checks_by_metadata(input_df, checks, globals())
display(valid_and_quarantined_df)