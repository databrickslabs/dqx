# Databricks notebook source
# MAGIC %md
# MAGIC # Demonstrate DQX usage as a Library

# COMMAND ----------

# MAGIC %md
# MAGIC ## Installation of DQX in Databricks cluster

# COMMAND ----------

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install '{dbutils.widgets.get("test_library_ref")}'
else:
    %pip install databricks-labs-dqx

%restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generation of quality rule/check candidates using Profiler
# MAGIC Data profiling is typically performed as a one-time action for the input dataset to discover the initial set of quality rule candidates.
# MAGIC This is not intended to be a continuously repeated or scheduled process, thereby also minimizing concerns regarding compute intensity and associated costs.

# COMMAND ----------

from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.dlt_generator import DQDltGenerator
from databricks.labs.dqx.config import WorkspaceFileChecksStorageConfig, TableChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
import os
import yaml

default_file_directory = os.getcwd()
default_catalog = "main"
default_schema = "default"

dbutils.widgets.text("demo_file_directory", default_file_directory, "File Directory")
dbutils.widgets.text("demo_catalog", default_catalog, "Catalog Name")
dbutils.widgets.text("demo_schema", default_schema, "Schema Name")

demo_file_directory = dbutils.widgets.get("demo_file_directory")
demo_catalog_name = dbutils.widgets.get("demo_catalog")
demo_schema_name = dbutils.widgets.get("demo_schema")

schema = "col1: int, col2: int, col3: int, col4 int"
input_df = spark.createDataFrame([[1, 3, 3, 1], [2, None, 4, 1]], schema)

ws = WorkspaceClient()

# profile the input data
profiler = DQProfiler(ws)
# change the default sample fraction from 30% to 100% for demo purpose
summary_stats, profiles = profiler.profile(input_df, options={"sample_fraction": 1.0})
print(yaml.safe_dump(summary_stats))
print(profiles)

# generate DQX quality rules/checks candidates
# they should be manually reviewed before being applied to the data
generator = DQGenerator(ws)
checks = generator.generate_dq_rules(profiles)  # with default level "error"
print(yaml.safe_dump(checks))

# generate Lakeflow Pipeline (formerly Delta Live Table (DLT)) expectations
dlt_generator = DQDltGenerator(ws)

dlt_expectations = dlt_generator.generate_dlt_rules(profiles, language="SQL")
print(dlt_expectations)

dlt_expectations = dlt_generator.generate_dlt_rules(profiles, language="Python")
print(dlt_expectations)

dlt_expectations = dlt_generator.generate_dlt_rules(profiles, language="Python_Dict")
print(dlt_expectations)

# save generated checks in a workspace file
user_name = spark.sql("select current_user() as user").collect()[0]["user"]
checks_file = f"{demo_file_directory}/dqx_demo_checks.yml"
dq_engine = DQEngine(ws)
dq_engine.save_checks(checks=checks, config=WorkspaceFileChecksStorageConfig(location=checks_file))

# save generated checks in a Delta table
dq_engine.save_checks(
    checks=checks,
    config=TableChecksStorageConfig(location=f"{demo_catalog_name}.{demo_schema_name}.dqx_checks_table", mode="overwrite")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading and applying quality checks from a file

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.config import WorkspaceFileChecksStorageConfig

input_df = spark.createDataFrame([[1, 3, 3, 2], [3, 3, None, 1]], schema)

# load checks from a file
dq_engine = DQEngine(WorkspaceClient())
checks = dq_engine.load_checks(config=WorkspaceFileChecksStorageConfig(location=checks_file))

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
from databricks.labs.dqx.config import TableChecksStorageConfig

input_df = spark.createDataFrame([[1, 3, 3, 2], [3, 3, None, 1]], schema)

# load checks from a Delta table
dq_engine = DQEngine(WorkspaceClient())
checks = dq_engine.load_checks(config=TableChecksStorageConfig(location=f"{demo_catalog_name}.{demo_schema_name}.dqx_checks_table"))

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
    ),
    *DQForEachColRule(  # check for multiple columns
        columns=["col1", "col2"],
        criticality="error",
        check_func=check_funcs.is_not_null).get_rules(),
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
from databricks.labs.dqx.config import InputConfig, OutputConfig
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
    output_config=OutputConfig(f"{demo_catalog_name}.{demo_schema_name}.dqx_output", mode="overwrite"),
    quarantine_config=OutputConfig(f"{demo_catalog_name}.{demo_schema_name}.dqx_quarantine", mode="overwrite")
)

# COMMAND ----------

display(spark.table(f"{demo_catalog_name}.{demo_schema_name}.dqx_output"))

# COMMAND ----------

display(spark.table(f"{demo_catalog_name}.{demo_schema_name}.dqx_quarantine"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## End-to-end quality checking

# COMMAND ----------

# end-to-end quality checking flow
dq_engine.apply_checks_by_metadata_and_save_in_table(
    input_config=InputConfig("/databricks-datasets/delta-sharing/samples/nyctaxi_2019"),
    checks=checks,
    output_config=OutputConfig(f"{demo_catalog_name}.{demo_schema_name}.dqx_e2e_output", mode="overwrite"),
    quarantine_config=OutputConfig(f"{demo_catalog_name}.{demo_schema_name}.dqx_e2e_quarantine", mode="overwrite")
)

# display the results saved to output and quarantine tables
display(spark.table(f"{demo_catalog_name}.{demo_schema_name}.dqx_e2e_output"))
display(spark.table(f"{demo_catalog_name}.{demo_schema_name}.dqx_e2e_quarantine"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using Foreign Key check

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
            # "ref_table": "catalog1.schema1.ref_table",
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
from databricks.labs.dqx.rule import register_rule


@register_rule("row")
def not_ends_with(column: str, suffix: str) -> Column:
    col_expr = F.col(column)
    return make_condition(col_expr.endswith(suffix), f"Column {column} ends with {suffix}",
                          f"{column}_ends_with_{suffix}")


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
    }),
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
    # custom python check
    - criticality: warn
      check:
        function: not_ends_with
        arguments:
          column: col1
          suffix: foo
    # sql expression check
    - criticality: warn
      check:
        function: sql_expression
        arguments:
          expression: col1 like 'str%'
          msg: col1 not starting with 'str'
    # built-in check
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
# custom_check_functions = globals()

status = dq_engine.validate_checks(checks, custom_check_functions)
assert not status.has_errors

valid_and_quarantine_df = dq_engine.apply_checks_by_metadata(input_df, checks, custom_check_functions)
display(valid_and_quarantine_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating custom dataset-level checks
# MAGIC Requirement: Fail all readings from a sensor if any reading for that sensor exceeds a specified threshold from the sensor specification table.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Define input data

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

# sensor data
sensor_df = spark.createDataFrame([
    [1, 1, 4],
    [1, 2, 1],
    [2, 2, 110]
], "measurement_id: int, sensor_id: int, reading_value: int")

# reference specs
sensor_specs_df = spark.createDataFrame([
    [1, 5],
    [2, 100],
], "sensor_id: int, min_threshold: int")

dq_engine = DQEngine(WorkspaceClient())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using `sql_query` check

# COMMAND ----------

# using DQX classes
from databricks.labs.dqx.rule import DQDatasetRule
from databricks.labs.dqx.check_funcs import sql_query

query = """
    WITH joined AS (
        SELECT
            sensor.*,
            COALESCE(specs.min_threshold, 100) AS effective_threshold
        FROM {{ sensor }} sensor
        LEFT JOIN {{ sensor_specs }} specs 
            ON sensor.sensor_id = specs.sensor_id
    )
    SELECT
        sensor_id,
        MAX(CASE WHEN reading_value > effective_threshold THEN 1 ELSE 0 END) = 1 AS condition
    FROM joined
    GROUP BY sensor_id
"""

checks = [
    DQDatasetRule(
        criticality="error",
        check_func=sql_query,
        check_func_kwargs={
            "query": query,
            "merge_columns": ["sensor_id"],
            "condition_column": "condition",  # the check fails if this column evaluates to True
            "msg": "one of the sensor reading is greater than limit",
            "name": "sensor_reading_check",
            "input_placeholder": "sensor",
        },
    ),
]

# Pass reference DataFrame with sensor specifications
ref_dfs = {"sensor_specs": sensor_specs_df}

valid_and_quarantine_df = dq_engine.apply_checks(sensor_df, checks, ref_dfs=ref_dfs)
display(valid_and_quarantine_df)

# COMMAND ----------

# using YAML declarative approach
checks = yaml.safe_load(
    """
    - criticality: error
      check:
        function: sql_query
        arguments:
          merge_columns:
            - sensor_id
          condition_column: condition
          msg: one of the sensor reading is greater than limit
          name: sensor_reading_check
          negate: false
          input_placeholder: sensor
          query: |
            WITH joined AS (
                SELECT
                    sensor.*,
                    COALESCE(specs.min_threshold, 100) AS effective_threshold
                FROM {{ sensor }} sensor
                LEFT JOIN {{ sensor_specs }} specs 
                    ON sensor.sensor_id = specs.sensor_id
            )
            SELECT
                sensor_id,
                MAX(CASE WHEN reading_value > effective_threshold THEN 1 ELSE 0 END) = 1 AS condition
            FROM joined
            GROUP BY sensor_id
    """
)

ref_dfs = {"sensor_specs": sensor_specs_df}

valid_and_quarantine_df = dq_engine.apply_checks_by_metadata(sensor_df, checks, ref_dfs=ref_dfs)
display(valid_and_quarantine_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Defining custom python dataset-level check

# COMMAND ----------

import uuid
from databricks.labs.dqx.rule import register_rule
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from collections.abc import Callable
from databricks.labs.dqx.check_funcs import make_condition


@register_rule("dataset")  # must be registered as dataset-level check
def sensor_reading_less_than(default_limit: int) -> tuple[Column, Callable]:
    # make sure any column added to the dataframe is unique
    condition_col = "condition" + uuid.uuid4().hex

    def apply(df: DataFrame, ref_dfs: dict[str, DataFrame]) -> DataFrame:
        """
        Validates that all readings per sensor are above sensor-specific threshold.
        If any are not, flags all readings of that sensor as failed.

        The closure function must take as arguments:
          * df: DataFrame
          * (Optional) spark: SparkSession
          * (Optional) ref_dfs: dict[str, DataFrame]
        """

        sensor_specs_df = ref_dfs["sensor_specs"]  # Should contain: sensor_id, min_threshold
        # you can also read from a Table directly:
        # sensor_specs_df = spark.table("catalog.schema.sensor_specs")

        # Join readings with specs
        sensor_df = df.join(sensor_specs_df, on="sensor_id", how="left")
        sensor_df = sensor_df.withColumn("effective_threshold",
                                         F.coalesce(F.col("min_threshold"), F.lit(default_limit)))

        # Check if any reading is below the spec-defined min_threshold per sensor
        aggr_df = (
            sensor_df
            .groupBy("sensor_id")
            .agg(
                (F.max(F.when(F.col("reading_value") > F.col("effective_threshold"), 1).otherwise(0)) == 1)
                .alias(condition_col)
            )
        )

        # Join back to input DataFrame
        return df.join(aggr_df, on="sensor_id", how="left")

    return (
        make_condition(
            condition=F.col(condition_col),  # check if condition column is True
            message=f"one of the sensor reading is greater than limit",
            alias="sensor_reading_check",
        ),
        apply
    )


# COMMAND ----------

# MAGIC %md Alternative implementation using `spark.sql`

# COMMAND ----------

import uuid
from databricks.labs.dqx.rule import register_rule
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from collections.abc import Callable
from databricks.labs.dqx.check_funcs import make_condition


@register_rule("dataset")  # must be registered as dataset-level check
def sensor_reading_less_than2(default_limit: int) -> tuple[Column, Callable]:
    # make sure any column added to the dataframe and registered temp views are unique
    # in case the check / function is applied multiple times
    unique_str = uuid.uuid4().hex
    condition_col = "condition_" + unique_str

    def apply(df: DataFrame, spark: SparkSession, ref_dfs: dict[str, DataFrame]) -> DataFrame:
        # Register the main and reference DataFrames as temporary views
        sensor_view_unique = "sensor_" + unique_str
        df.createOrReplaceTempView(sensor_view_unique)

        sensor_specs_view_unique = "sensor_specs_" + unique_str
        ref_dfs["sensor_specs"].createOrReplaceTempView(sensor_specs_view_unique)

        # Perform the check
        query = f"""
        WITH joined AS (
          SELECT
            sensor.*,
            COALESCE(specs.min_threshold, {default_limit}) AS effective_threshold
          FROM {sensor_view_unique} sensor
          -- we could also access Table directly: catalog.schema.sensor_specs
          LEFT JOIN {sensor_specs_view_unique} specs
            ON sensor.sensor_id = specs.sensor_id
        ),
        aggr AS (
          SELECT
            sensor_id,
            MAX(CASE WHEN reading_value > effective_threshold THEN 1 ELSE 0 END) = 1 AS {condition_col}
          FROM joined
          GROUP BY sensor_id
        )
        -- join back to the input DataFrame to retain original records
        SELECT
          sensor.*,
          aggr.{condition_col}
        FROM {sensor_view_unique} sensor
        LEFT JOIN aggr
          ON sensor.sensor_id = aggr.sensor_id
    """

        return spark.sql(query)

    return (
        make_condition(
            condition=F.col(condition_col),  # check if condition column is True
            message=f"one of the sensor reading is greater than limit",
            alias="sensor_reading_check",
        ),
        apply
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply custom dataset-level check function using DQX classes

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.rule import DQDatasetRule

checks = [
    DQDatasetRule(criticality="error", check_func=sensor_reading_less_than, name="sensor_reading_exceeded",
                  check_func_kwargs={
                      "default_limit": 100
                  }
                  ),
    # any other checks ...
]

# Pass reference DataFrame with sensor specifications
ref_dfs = {"sensor_specs": sensor_specs_df}

valid_and_quarantine_df = dq_engine.apply_checks(sensor_df, checks, ref_dfs=ref_dfs)
display(valid_and_quarantine_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply custom dataset-level check functions using YAML

# COMMAND ----------

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

checks = yaml.safe_load("""
- criticality: error
  check:
    function: sensor_reading_less_than
    arguments:
      default_limit: 100
# any other checks ...
""")

dq_engine = DQEngine(WorkspaceClient())

custom_check_functions = {"sensor_reading_less_than": sensor_reading_less_than}  # list of custom check functions
# or include all functions with globals() for simplicity
# custom_check_functions=globals()

# Pass reference DataFrame with sensor specifications
ref_dfs = {"sensor_specs": sensor_specs_df}

valid_and_quarantine_df = dq_engine.apply_checks_by_metadata(sensor_df, checks, custom_check_functions, ref_dfs=ref_dfs)
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

sensor_df.createOrReplaceTempView("sensor")
sensor_specs_df.createOrReplaceTempView("sensor_specs")

# Combine sensor readings with sensor specifications
input_df = spark.sql(f"""
  SELECT
    sensor.*,
    COALESCE(min_threshold, 100) AS effective_threshold
  FROM sensor
  LEFT JOIN sensor_specs
    ON sensor.sensor_id = sensor_specs.sensor_id
""")

# Define and apply row-level check
checks = yaml.safe_load("""
- criticality: error
  name: sensor_reading_exceeded
  check:
    function: sql_expression
    arguments:
      expression: MAX(reading_value) OVER (PARTITION BY sensor_id) > 100
      msg: one of the sensor reading is greater than 100
      negate: true
""")

dq_engine = DQEngine(WorkspaceClient())

valid_and_quarantine_df = dq_engine.apply_checks_by_metadata(input_df, checks)

display(valid_and_quarantine_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option 2: Using Dataset-level checks
# MAGIC
# MAGIC You can apply checks on multiple DataFrames using custom dataset-level rules as wel (see previous section).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comparison of datasets
# MAGIC
# MAGIC You can use DQX to compare two datasets (DataFrames or Tables) to identify differences at the row and column level. This supports use cases such as:
# MAGIC * Migration validation
# MAGIC * Drift detection
# MAGIC * Regression testing between pipeline versions
# MAGIC * Synchronization checks between source and target systems

# COMMAND ----------

from pyspark.sql import functions as F
from databricks.labs.dqx.rule import DQDatasetRule
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

schema = "id: int, id2: int, val: string, extra_col: string"
df = spark.createDataFrame(
    [

        [1, 1, "val1", "skipped col"],
        [1, None, "val1", "skipped col"],  # Null are not treated as unknown or distinct
        [3, 3, "val1", "skipped col"],  # extra record
    ],
    schema,
)

schema_ref = "id_ref: int, id2_ref: int, val: string, extra_col: string"
df2 = spark.createDataFrame(
    [
        [1, 1, "val1", "skipped ref col"],  # no change
        [1, None, "val2", "skipped ref col"],  # val changed
        [4, 4, "val3", "skipped ref col"],  # missing record
    ],
    schema_ref,
)

checks = [
    DQDatasetRule(
        criticality="error",
        check_func=check_funcs.compare_datasets,
        columns=["id", "id2"],  # columns for row matching with reference df
        check_func_kwargs={
          "ref_columns": ["id_ref", "id2_ref"], # columns for row matching with source df, matched with `ref_columns` by position
          "ref_df_name": "ref_df",
          "check_missing_records": True,  # if wanting to get info about missing records
          "exclude_columns": ["extra_col"]  # columns to exclude from the value comparison, but not from row matching 
        },
    )
]

ref_dfs = {"ref_df": df2}

dq_engine = DQEngine(WorkspaceClient())
output_df = dq_engine.apply_checks(df, checks, ref_dfs=ref_dfs)
display(output_df)

# COMMAND ----------

# MAGIC %md
# MAGIC The detailed log of differences is stored as a JSON string in the message field. It can be parsed into a structured format for easier inspection and analysis.

# COMMAND ----------

def safe_parse_json(col):
    message_schema = """
        struct<row_missing:boolean,
        row_extra:boolean,
        changed:map<string,map<string,string>>>
    """

    parsed = F.from_json(col, message_schema)
    return F.when(parsed.isNotNull(), parsed)

# Extract dataset differences from the "message" field into a structured field named "dataset_diffs"
output_df = output_df.withColumn(
    "_errors",
    F.transform(
        "_errors",
        lambda x: x.withField("dataset_diffs", safe_parse_json(x["message"]))
    )
).withColumn(
    "_warnings",
    F.transform(
        "_warnings",
        lambda x: x.withField("dataset_diffs", safe_parse_json(x["message"]))
    )
)

display(output_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying custom column names and adding user metadata

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx.config import ExtraParams
from databricks.labs.dqx.check_funcs import is_not_null_and_not_empty

user_metadata = {"key1": "value1", "key2": "value2"}
custom_column_names = {"errors": "dq_errors", "warnings": "dq_warnings"}

# using ExtraParams to configure optional parameters
extra_parameters = ExtraParams(result_column_names=custom_column_names, user_metadata=user_metadata)

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