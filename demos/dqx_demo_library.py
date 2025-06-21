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
# MAGIC ## Generation of quality rule/check candidates using Profiler
# MAGIC Data profiling is typically performed as a one-time action for the input dataset to discover the initial set of quality rule candidates.
# MAGIC This is not intended to be a continuously repeated or scheduled process, thereby also minimizing concerns regarding compute intensity and associated costs.

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
# change the default sample fraction from 30% to 100% for demo purpose
summary_stats, profiles = profiler.profile(input_df, opts={"sample_fraction": 1.0})
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
dq_engine.save_checks_in_workspace_file(checks=checks, workspace_path=checks_file)

# save generated checks in a Delta table
dq_engine.save_checks_in_table(checks=checks, table_name="main.default.dqx_checks_table", mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading and applying quality checks from a file

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

input_df = spark.createDataFrame([[1, 3, 3, 2], [3, 3, None, 1]], schema)

# load checks from a file
dq_engine = DQEngine(WorkspaceClient())
checks = dq_engine.load_checks_from_workspace_file(workspace_path=checks_file)

# Option 1: apply quality rules and quarantine invalid records
valid_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(input_df, checks)
display(valid_df)
display(quarantine_df)

# Option 2: apply quality rules and annotate invalid records as additional columns (`_warning` and `_error`)
valid_and_quarantine_df = dq_engine.apply_checks_by_metadata(input_df, checks)
display(valid_and_quarantine_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading and applying quality checks from a Delta table

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

input_df = spark.createDataFrame([[1, 3, 3, 2], [3, 3, None, 1]], schema)

# load checks from a Delta table
dq_engine = DQEngine(WorkspaceClient())
checks = dq_engine.load_checks_from_table(table_name="main.default.dqx_checks_table")

# Option 1: apply quality rules and quarantine invalid records
valid_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(input_df, checks)
display(valid_df)
display(quarantine_df)

# Option 2: apply quality rules and annotate invalid records as additional columns (`_warning` and `_error`)
valid_and_quarantine_df = dq_engine.apply_checks_by_metadata(input_df, checks)
display(valid_and_quarantine_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validating syntax of quality checks defined declaratively in yaml

# COMMAND ----------

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

checks = yaml.safe_load("""
- criticality: invalid_criticality
  check:
    function: is_not_null
    for_each_column:
    - col1
    - col2
""")

dq_engine = DQEngine(WorkspaceClient())

status = dq_engine.validate_checks(checks)
print(status.has_errors)
print(status.errors)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying quality checks defined in yaml

# COMMAND ----------

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

checks = yaml.safe_load("""
# check for a single column
- criticality: warn
  check:
    function: is_not_null_and_not_empty
    arguments:
      column: col3
# check for multiple column
- criticality: error
  check:
    function: is_not_null
    for_each_column:
    - col1
    - col2
# check with a filter
- criticality: warn
  filter: col1 < 3
  check:
    function: is_not_null_and_not_empty
    arguments:
      column: col4
# check with user metadata
- criticality: warn
  check:
    function: is_not_null_and_not_empty
    arguments:
      column: col5
  user_metadata:
    check_category: completeness
    responsible_data_steward: someone@email.com
# check with auto-generated name
- criticality: warn
  check:
    function: is_in_list
    arguments:
      column: col1
      allowed:
        - 1
        - 2
# check for a struct field
- check:
    function: is_not_null
    arguments:
      column: col7.field1
  # "error" criticality used if not provided
# check for a map element
- criticality: error
  check:
    function: is_not_null
    arguments:
      column: try_element_at(col5, 'key1')
# check for an array element
- criticality: error
  check:
    function: is_not_null
    arguments:
      column: try_element_at(col6, 1)
# check uniqueness of composite key, multi-column rule   
- criticality: error
  check:
    function: is_unique
    arguments:
      columns:
      - col1
      - col2
- criticality: error
  check:
    function: is_aggr_not_greater_than
    arguments:
      column: col1
      aggr_type: count
      limit: 10
- criticality: error
  check:
    function: is_aggr_not_less_than
    arguments:
      column: col1
      aggr_type: count
      limit: 1.2
""")

# validate the checks
status = DQEngine.validate_checks(checks)
assert not status.has_errors

schema = "col1: int, col2: int, col3: int, col4 int, col5: map<string, string>, col6: array<string>, col7: struct<field1: int>"
input_df = spark.createDataFrame([
    [1, 3, 3, None, {"key1": ""}, [""], {"field1": 1}],
    [3, None, 4, 1, {"key1": None}, [None], {"field1": None}],
    [None, None, None, None, None, None, None],
], schema)

dq_engine = DQEngine(WorkspaceClient())

# Option 1: apply quality rules and quarantine invalid records
valid_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(input_df, checks)
display(valid_df)
display(quarantine_df)

# Option 2: apply quality rules and annotate invalid records as additional columns (`_warning` and `_error`)
valid_and_quarantine_df = dq_engine.apply_checks_by_metadata(input_df, checks)
display(valid_and_quarantine_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying quality checks programmatically using DQX classes

# COMMAND ----------

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule, DQForEachColRule
from databricks.sdk import WorkspaceClient
import pyspark.sql.functions as F

checks = [
     DQRowRule(  # check for a single column
        name="col3_is_null_or_empty",
        criticality="warn",
        check_func=check_funcs.is_not_null_and_not_empty,
        column="col3"
     )] + \
         DQForEachColRule(  # check for multiple columns
         columns=["col1", "col2"],
         criticality="error",
         check_func=check_funcs.is_not_null).get_rules() + [
     DQRowRule(  # check with a filter
        name="col_4_is_null_or_empty",
        criticality="warn",
        filter="col1 < 3",
        check_func=check_funcs.is_not_null_and_not_empty,
        column="col4"
     ),
     DQRowRule(
        criticality="warn",
        check_func=check_funcs.is_not_null_and_not_empty,
        column='col3',
        user_metadata={
            "check_type": "completeness",
            "responsible_data_steward": "someone@email.com"
        }
     ),
     DQRowRule(  # provide check func arguments using positional arguments
         criticality="warn",
         check_func=check_funcs.is_in_list,
         column="col1",
         check_func_args=[[1, 2]]
     ),
     DQRowRule(  # provide check func arguments using keyword arguments
         criticality="warn",
         check_func=check_funcs.is_in_list,
         column="col2",
         check_func_kwargs={"allowed": [1, 2]}
     ),
     DQRowRule(  # check for a struct field
         # "error" criticality used if not provided
         check_func=check_funcs.is_not_null,
         column="col7.field1"
     ),
     DQRowRule(  # check for a map element
         criticality="error",
         check_func=check_funcs.is_not_null,
         column=F.try_element_at("col5", F.lit("key1"))
     ),
     DQRowRule(  # check for an array element
         criticality="error",
         check_func=check_funcs.is_not_null,
         column=F.try_element_at("col6", F.lit(1))
     ),
     DQDatasetRule(  # check uniqueness of composite key, multi-column rule
         criticality="error",
         check_func=check_funcs.is_unique,
         columns=["col1", "col2"]
     ),
     DQDatasetRule(
         criticality="error",
         check_func=check_funcs.is_aggr_not_greater_than,
         column="col1",
         check_func_kwargs={"aggr_type": "count", "limit": 10},
     ),
     DQDatasetRule(
         criticality="error",
         check_func=check_funcs.is_aggr_not_less_than,
         column="col1",
         check_func_kwargs={"aggr_type": "avg", "limit": 1.2},
     ),
]

schema = "col1: int, col2: int, col3: int, col4 int, col5: map<string, string>, col6: array<string>, col7: struct<field1: int>"
input_df = spark.createDataFrame([
    [1, 3, 3, None, {"key1": ""}, [""], {"field1": 1}],
    [3, None, 4, 1, {"key1": None}, [None], {"field1": None}],
    [None, None, None, None, None, None, None],
], schema)

dq_engine = DQEngine(WorkspaceClient())

# Option 1: apply quality rules and quarantine invalid records
valid_df, quarantine_df = dq_engine.apply_checks_and_split(input_df, checks)
display(valid_df)
display(quarantine_df)

# Option 2: apply quality rules and annotate invalid records as additional columns (`_warning` and `_error`)
valid_and_quarantine_df = dq_engine.apply_checks(input_df, checks)
display(valid_and_quarantine_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying quality checks in the Lakehouse medallion architecture

# COMMAND ----------

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

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
      msg: pickup time must not be greater than dropff time
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
assert not status.has_errors

dq_engine = DQEngine(WorkspaceClient())

# read the data, limit to 1000 rows for demo purpose
bronze_df = spark.read.format("delta").load("/databricks-datasets/delta-sharing/samples/nyctaxi_2019").limit(1000)

# apply your business logic here
bronze_transformed_df = bronze_df.filter("vendor_id in (1, 2)")

# apply quality checks
silver_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(bronze_transformed_df, checks)

# save results
dq_engine.save_results_in_table(
    output_df=silver_df,
    quarantine_df=quarantine_df,
    output_table="main.default.dqx_output",
    quarantine_table="main.default.dqx_quarantine",
    output_table_mode="overwrite",
    quarantine_table_mode="overwrite"
)

# COMMAND ----------

display(spark.table("main.default.dqx_output"))

# COMMAND ----------

display(spark.table("main.default.dqx_quarantine"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using Foreign key check

# COMMAND ----------

# Using DQX classes to define foreign key check
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient


checks = [
    DQDatasetRule(
        criticality="error",
        check_func=check_funcs.foreign_key,
        columns=["col1"],
        check_func_kwargs={
            "ref_columns": ["ref_col1"],
            # either provide reference DataFrame name
            "ref_df_name": "ref_df_key",
            # or provide name of the reference table
            #"ref_table": "catalog1.schema1.ref_table",
        },
    ),
        DQDatasetRule(
        name="foreign_key_check_on_composite_key",
        criticality="warn",
        check_func=check_funcs.foreign_key,
        columns=["col1", "col2"],  # composite key
        check_func_kwargs={
            "ref_columns": ["ref_col1", "ref_col2"],
            "ref_df_name": "ref_df_key",
        },
    ),
]

input_df = spark.createDataFrame([[1, 1], [2, 2], [None, None]], "col1: int, col2: int")
reference_df = spark.createDataFrame([[1, 1]], "ref_col1: int, ref_col2: int")

dq_engine = DQEngine(WorkspaceClient())

# When applying foreign key checks with a specified `ref_df_name` argument,
# you must pass a dictionary of reference DataFrame to the `apply_checks` or `apply_checks_and_split` methods
refs_dfs = {"ref_df_key": reference_df}

valid_and_quarantine_df = dq_engine.apply_checks(input_df, checks, ref_dfs=refs_dfs)
display(valid_and_quarantine_df)

# COMMAND ----------

# Using yaml to define the foreign key check
import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient


checks = yaml.safe_load(
  """
  - criticality: error
    check:
      function: foreign_key
      arguments:
        columns: 
        - col1
        ref_columns: 
        - ref_col1
        # either provide reference DataFrame name
        ref_df_name: ref_df_key
        # or provide name of the reference table
        #ref_table: catalog1.schema1.ref_table

  - criticality: warn
    name: foreign_key_check_on_composite_key
    check:
      function: foreign_key
      arguments:
        columns: 
        - col1
        - col2
        ref_columns:
        - ref_col1
        - ref_col2
        ref_df_name: ref_df_key
  """)

input_df = spark.createDataFrame([[1, 1], [2, 2], [None, None]], "col1: int, col2: int")
reference_df = spark.createDataFrame([[1, 1]], "ref_col1: int, ref_col2: int")

dq_engine = DQEngine(WorkspaceClient())

# When applying foreign key checks with a specified `ref_df_name` argument,
# you must pass a dictionary of reference DataFrame to the `apply_checks_by_metadata` or `apply_checks_by_metadata_and_split` methods
refs_dfs = {"ref_df_key": reference_df}

valid_and_quarantine_df = dq_engine.apply_checks_by_metadata(input_df, checks, ref_dfs=refs_dfs)
display(valid_and_quarantine_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating custom checks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating custom row-level check function

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import Column
from databricks.labs.dqx.check_funcs import make_condition


def not_ends_with(column: str, suffix: str) -> Column:
    col_expr = F.col(column)
    return make_condition(col_expr.endswith(suffix), f"Column {column} ends with {suffix}", f"{column}_ends_with_{suffix}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying custom row-level check function using DQX classes

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx.check_funcs import is_not_null_and_not_empty, sql_expression


checks = [
    # custom check
    DQRowRule(criticality="warn", check_func=not_ends_with, column="col1", check_func_kwargs={"suffix": "foo"}),
    # sql expression check
    DQRowRule(criticality="warn", check_func=sql_expression, check_func_kwargs={
            "expression": "col1 like 'str%'", "msg": "col1 not starting with 'str'"
        }
    ),
    # built-in check
    DQRowRule(criticality="error", check_func=is_not_null_and_not_empty, column="col1"),
]

schema = "col1: string, col2: string"
input_df = spark.createDataFrame([[None, "foo"], ["foo", None], [None, None]], schema)

dq_engine = DQEngine(WorkspaceClient())

valid_and_quarantine_df = dq_engine.apply_checks(input_df, checks)
display(valid_and_quarantine_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying custom row-level check function using YAML definition

# COMMAND ----------

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient


checks = yaml.safe_load(
"""
- criticality: warn
  check:
    function: not_ends_with
    arguments:
      column: col1
      suffix: foo
- criticality: warn
  check:
    function: sql_expression
    arguments:
      expression: col1 like 'str%'
      msg: col1 not starting with 'str'
- criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments:
      column: col1
"""
)

schema = "col1: string, col2: string"
input_df = spark.createDataFrame([[None, "foo"], ["foo", None], [None, None]], schema)

dq_engine = DQEngine(WorkspaceClient())

custom_check_functions = {"not_ends_with": not_ends_with}
# alternatively, you can also use globals to include all available functions
#custom_check_functions = globals()

status = dq_engine.validate_checks(checks, custom_check_functions)
assert not status.has_errors

valid_and_quarantine_df = dq_engine.apply_checks_by_metadata(input_df, checks, custom_check_functions)
display(valid_and_quarantine_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating custom dataset-level check function

# COMMAND ----------

from databricks.labs.dqx.rule import register_rule
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from collections.abc import Callable



@register_rule("dataset")  # must be registered as dataset-level check
def custom_dataset_check_func_is_not_null(column: str) -> tuple[Column, Callable]:
    condition_col = f"{column}_is_not_null_check"

    # Closure function must take as arguments:
    # * main DataFrame
    # * (optional) spark session
    # * (optional) reference DataFrames
    # Must return a DataFrame with a column used for condition
    def closure(df: DataFrame, spark: SparkSession) -> DataFrame:
        df.createOrReplaceTempView("temp_df")

        # Perform an arbitrary SQL query and return DataFrame with condition column used for evaluation
        # The condition column would typically be boolean column that indicates whether the condition is met for each row
        sql_query = f"""
                SELECT *,
                       CASE WHEN {column} IS NULL THEN TRUE ELSE FALSE END AS {condition_col}
                FROM temp_df
            """

        return spark.sql(sql_query)

    return (
        check_funcs.make_condition(
            condition=F.col(condition_col),  # check condition returns true
            message="null found",
            alias=f"{column}_is_not_null_check",
        ),
        closure,
    )

# COMMAND ----------

@register_rule("dataset")
def custom_dataset_check_func_aggr(extra_param: str) -> tuple[Column, Callable]:
    condition_col = f"__condition"

    def apply(df: DataFrame, spark: SparkSession) -> DataFrame:
        # Register main DataFrame as temp view
        df.createOrReplaceTempView("table1")

        # Use SQL to compute the aggregation and join back to original DataFrame
        result_df = spark.sql(f"""
            WITH aggr AS (
                SELECT
                    col2,
                    SUM(col1) > 0 AS condition_val
                FROM table1
                GROUP BY col2
            )
            SELECT
                t1.*,
                COALESCE(aggr.condition_val, false) AS {condition_col}
            FROM table1 t1  -- left join main DataFrame to the aggregation to retain all records
            LEFT JOIN aggr
            ON t1.col2 = aggr.col2
        """)

        return result_df

    return (
        check_funcs.make_condition(
            condition=F.col(condition_col),
            message=f"check failed with {extra_param}",
            alias="custom_agg_dataset_check",
        ),
        apply
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply custom dataset-level check function

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.rule import DQDatasetRule
from databricks.labs.dqx.check_funcs import is_not_null


# define checks
checks = [
    DQDatasetRule(criticality="error", check_func=custom_dataset_check_func_is_not_null, column="col2"),
    DQDatasetRule(criticality="error", check_func=custom_dataset_check_func_aggr, check_func_kwargs={"extra_param": "extra argument"}),
    # you can combine custom dataset rules with any other check
    DQRowRule(criticality="error", check_func=is_not_null, column="col2"),
]

input_df = spark.createDataFrame([[1, "foo"], [2, None]], "col1: int, col2: string")

dq_engine = DQEngine(WorkspaceClient())

# just passing main DataFrame, no reference DataFrames
valid_and_quarantine_df = dq_engine.apply_checks(input_df, checks)
display(valid_and_quarantine_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying checks on multiple data sets

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option 1: Combined DataFrames into a single DataFrame before applying the checks

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

schema = "col1: int, col2: int, col3: int, col4 string"
df1 = spark.createDataFrame([[1, 3, 3, "foo"], [2, None, 4, "foo"]], schema)
df2 = spark.createDataFrame([[0, 0, 0, "foo2"], [1, 2, 2, "foo3"], [2, None, 4, "foo4"]], schema)

df1.createOrReplaceTempView("table1")
df2.createOrReplaceTempView("table2")

# Join and filter two input tables
input_df = spark.sql("""
    SELECT t2.col1, t2.col2, t2.col3
    FROM (
        SELECT DISTINCT col1 FROM table1 WHERE col4 = 'foo'
    ) AS t1
    JOIN table2 AS t2
    ON t1.col1 = t2.col1
    WHERE t2.col1 > 0
""")

# Define and apply checks on the joined data sets
checks = yaml.safe_load("""
- criticality: error
  check:
    function: is_not_null
    arguments:
      column: col2
- criticality: error
  check:
    function: sql_expression
    arguments:
      expression: col3 >= col2 and col3 <= 10
      msg: col3 is less than col2 and col3 is greater than 10
      name: custom_output_name
      negate: false
""")

dq_engine = DQEngine(WorkspaceClient())

# Option 1: apply quality rules and quarantine invalid records
valid_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(input_df, checks)
display(valid_df)
display(quarantine_df)

# Option 2: apply quality rules and annotate invalid records as additional columns (`_warning` and `_error`)
valid_and_quarantine_df = dq_engine.apply_checks_by_metadata(input_df, checks)
display(valid_and_quarantine_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option 2: Use custom dataset-level rule to combine DataFrames inside the rule

# COMMAND ----------

from databricks.labs.dqx.rule import register_rule
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from collections.abc import Callable


@register_rule("dataset")
def custom_dataset_check_func_multiple_dfs() -> tuple[Column, Callable]:
    condition_col = f"__condition"

    def apply(df: DataFrame, spark: SparkSession, ref_dfs: dict[str, DataFrame] | None = None) -> DataFrame:
        ref_df = ref_dfs["ref_df"]
        # the reference DataFrames can also be fetched from a table directly
        # ref_df = spark.read.table("catalog.schema.table")

        # Register DataFrames as temp views
        df.createOrReplaceTempView("table1")
        ref_df.createOrReplaceTempView("table2")

        # SQL to compute aggregation + join back to original
        result_df = spark.sql(f"""
            WITH matched AS (
                SELECT DISTINCT t1.col1 AS matched_col1
                FROM (
                    SELECT col1 FROM table1 WHERE col2 = 'foo'
                ) AS t1
                JOIN table2 AS t2
                ON t1.col1 = t2.col1
                WHERE t2.col1 > 0
            )
            SELECT 
                t1.*, 
                CASE WHEN matched.matched_col1 IS NOT NULL THEN true ELSE false END AS {condition_col}
            FROM table1 t1   -- left join main DataFrame to the aggregation to retain all records
            LEFT JOIN matched
            ON t1.col1 = matched.matched_col1
        """)

        return result_df

    return (
        check_funcs.make_condition(
            condition=F.col(condition_col),
            message="check failed",
            alias=f"custom_dataset_check",
        ),
        apply
    )

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.rule import DQDatasetRule


checks = [
    DQDatasetRule(criticality="error", check_func=custom_dataset_check_func_multiple_dfs),
]

input_df = spark.createDataFrame([[1, "foo"], [2, "foo"]], "col1: int, col2: string")
reference_df = spark.createDataFrame([[1]], "col1: int")

dq_engine = DQEngine(WorkspaceClient())

# provide reference DataFrame(s)
refs_dfs = {"ref_df": reference_df}

valid_and_quarantine_df = dq_engine.apply_checks(input_df, checks, ref_dfs=refs_dfs)
display(valid_and_quarantine_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying custom column names and adding user metadata

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule, ExtraParams
from databricks.labs.dqx.check_funcs import is_not_null_and_not_empty

user_metadata = {"key1": "value1", "key2": "value2"}
custom_column_names = {"errors": "dq_errors", "warnings": "dq_warnings"}

# using ExtraParams to configure optional parameters
extra_parameters = ExtraParams(reporting_column_names=custom_column_names, user_metadata=user_metadata)

ws = WorkspaceClient()
dq_engine = DQEngine(ws, extra_params=extra_parameters)

schema = "col1: string, col2: string"
input_df = spark.createDataFrame([[None, "foo"], ["foo", None], [None, None]], schema)

checks = [
    DQRowRule(criticality="error", check_func=is_not_null_and_not_empty, column="col1"),
    DQRowRule(criticality="warn", check_func=is_not_null_and_not_empty, column="col2"),
]

valid_and_quarantine_df = dq_engine.apply_checks(input_df, checks)
display(valid_and_quarantine_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploring quality checks output

# COMMAND ----------

import pyspark.sql.functions as F

# explode errors
errors_df = valid_and_quarantine_df.select(F.explode(F.col("dq_errors")).alias("dq")).select(F.expr("dq.*"))
display(errors_df)

# explode warnings
warnings_df = valid_and_quarantine_df.select(F.explode(F.col("dq_warnings")).alias("dq")).select(F.expr("dq.*"))
display(warnings_df)