# Databricks notebook source
# MAGIC %md
# MAGIC # Demonstrate DQX usage as a Library

# COMMAND ----------

# MAGIC %md
# MAGIC ## Installation of DQX in Databricks cluster

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx
# MAGIC %restart_python

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

# generate DQX quality rules/checks candidates
# they should be manually reviewed before being applied to the data
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
user_name = spark.sql("select current_user() as user").collect()[0]["user"]
checks_file = f"/Workspace/Users/{user_name}/dqx_demo_checks.yml"
dq_engine = DQEngine(ws)
dq_engine.save_checks_in_workspace_file(checks, workspace_path=checks_file)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading checks and applying quality rules

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

input_df = spark.createDataFrame([[1, 3, 3, 2], [3, 3, None, 1]], schema)

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
# MAGIC ## Explore Quality Checks Output

# COMMAND ----------

import pyspark.sql.functions as F

# explore errors
results_df = quarantined_df.select(F.explode(F.col("_errors")).alias("dq")).select(F.expr("dq.*"))
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validating quality checks definition
# MAGIC This is typically run as part of CI/CD process to ensure checks are ready to use.

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

- criticality: warn
  check:
    function: is_not_null_and_not_empty
    arguments:
      col_name: col3

- criticality: warn
  filter: col1 < 3
  check:
    function: is_not_null_and_not_empty
    arguments:
      col_name: col4

- criticality: error
  check:
    function: is_in_list
    arguments:
      col_name: col1
      allowed:
        - 1
        - 2
""")

# validate the checks
status = DQEngine.validate_checks(checks)
assert not status.has_errors

schema = "col1: int, col2: int, col3: int, col4 int"
input_df = spark.createDataFrame([[1, 3, 3, None], [3, None, 4, 1]], schema)

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

from databricks.labs.dqx.col_functions import is_not_null, is_not_null_and_not_empty, is_in_list, is_in_range
from databricks.labs.dqx.engine import DQEngine, DQRule, DQRuleColSet
from databricks.sdk import WorkspaceClient

checks = [
         DQRule( # define rule for a single column
            name="col3_is_null_or_empty",
            criticality="warn",
            check_func=is_not_null_and_not_empty, 
            col_name="col3"),
         DQRule( # define rule with a filter
            name="col_4_is_null_or_empty",
            criticality="warn",
            filter="col1 < 3",
            check_func=is_not_null_and_not_empty, 
            col_name="col4"),
         DQRule( # define rule with name for the check auto-generated if not provided
            criticality="error",
            check_func=is_in_list, 
            col_name="col1",
            check_func_args=[["1", "2"]]),
         DQRule( # define rule using keyword arguments
             criticality="error",
             check_func=is_in_range,
             col_name="col2",
             check_func_kwargs={"min_limit": 1, "max_limit": 10},
         ),
        ] + DQRuleColSet( # define rule for multiple columns at once, name auto-generated if not provided
            columns=["col1", "col2"],
            criticality="error",
            check_func=is_not_null).get_rules()

schema = "col1: int, col2: int, col3: int, col4 int"
input_df = spark.createDataFrame([[1, 3, 3, None], [3, None, 4, 1]], schema)

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
    function: is_not_less_than
    arguments:
      col_name: trip_distance
      limit: 1
  criticality: error
- check:
    function: sql_expression
    arguments:
      expression: pickup_datetime <= dropoff_datetime
      msg: pickup time must not be greater than dropff time
      name: pickup_datetime_greater_than_dropoff_datetime
  criticality: error
- check:
    function: is_not_in_future
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
# MAGIC ### Applying custom check function using DQX classes

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.col_functions import *

# use built-in, custom and sql expression checks
checks = [
    DQRule(criticality="error", check_func=is_not_null_and_not_empty, col_name="col1"),
    DQRule(criticality="warn", check_func=ends_with_foo, col_name="col1"),
    DQRule(criticality="warn", check_func=sql_expression, check_func_kwargs={
        "expression": "col1 like 'str%'", "msg": "col1 not starting with 'str'"}),
]

schema = "col1: string, col2: string"
input_df = spark.createDataFrame([[None, "foo"], ["foo", None], [None, None]], schema)

dq_engine = DQEngine(WorkspaceClient())

valid_and_quarantined_df = dq_engine.apply_checks(input_df, checks)
display(valid_and_quarantined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying custom check function using YAML definition

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
- criticality: warn
  check:
    function: ends_with_foo
    arguments:
      col_name: col1
- criticality: warn
  check:
    function: sql_expression
    arguments:
      expression: col1 like 'str%'
      msg: col1 not starting with 'str'
"""
)

schema = "col1: string, col2: string"
input_df = spark.createDataFrame([[None, "foo"], ["foo", None], [None, None]], schema)

dq_engine = DQEngine(WorkspaceClient())

custom_check_functions = {"ends_with_foo": ends_with_foo}
# alternatively, you can also use globals to include all available functions
#custom_check_functions = globals()

status = dq_engine.validate_checks(checks, custom_check_functions)
assert not status.has_errors

valid_and_quarantined_df = dq_engine.apply_checks_by_metadata(input_df, checks, custom_check_functions)
display(valid_and_quarantined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying custom column names and adding user metadata

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import (
    DQEngine,
    ExtraParams,
    DQRule
)

from databricks.labs.dqx.col_functions import is_not_null_and_not_empty

user_metadata = {"key1": "value1", "key2": "value2"}
custom_column_names = {"errors": "dq_errors", "warnings": "dq_warnings"}

# using ExtraParams to configure optional parameters
extra_parameters = ExtraParams(column_names=custom_column_names, user_metadata=user_metadata)

ws = WorkspaceClient()
dq_engine = DQEngine(ws, extra_params=extra_parameters)

schema = "col1: string, col2: string"
input_df = spark.createDataFrame([[None, "foo"], ["foo", None], [None, None]], schema)

checks = [
    DQRule(criticality="error", check_func=is_not_null_and_not_empty, col_name="col1"),
    DQRule(criticality="warn", check_func=is_not_null_and_not_empty, col_name="col2"),
]

valid_and_quarantined_df = dq_engine.apply_checks(input_df, checks)
display(valid_and_quarantined_df)