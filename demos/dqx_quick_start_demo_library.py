# Databricks notebook source
# MAGIC %md
# MAGIC # DQX - Use as library demo
# MAGIC
# MAGIC In this demo we demonstrate how to create and apply a set of rules (checks) defined in YAML configuration and using DQX classes. 
# MAGIC
# MAGIC **Note:**
# MAGIC This notebook can be executed without any modifications when using the [Databricks extension for Visual Studio Code](https://docs.databricks.com/aws/en/dev-tools/vscode-ext/).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install DQX

# COMMAND ----------

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install '{dbutils.widgets.get("test_library_ref")}'
else:
    %pip install databricks-labs-dqx

%restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup Test Data
# MAGIC
# MAGIC The result of this step is `new_users_df`, which represents a DataFrame of new users which requires quality validation.

# COMMAND ----------

from pyspark.sql import SparkSession, Row


spark = SparkSession.builder.appName("DQX_demo_library").getOrCreate()

# Create a sample DataFrame
new_users = [
    Row(id=1, age=23, country='Germany'),
    Row(id=2, age=30, country='France'),
    Row(id=3, age=16, country='Germany'),    # Invalid -> age - less than 18
    Row(id=None, age=29, country='France'),  # Invalid -> id - NULL
    Row(id=4, age=29, country=''),           # Invalid -> country - Empty
    Row(id=5, age=23, country='Italy'),      # Invalid -> country - not in allowed
    Row(id=6, age=123, country='France'),    # Invalid -> age - greater than 120
    Row(id=2, age=23, country='Germany'),    # duplicated id
]

new_users_df = spark.createDataFrame(new_users)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demonstrating Check Functions
# MAGIC - `is_not_null_and_not_empty`
# MAGIC - `is_in_range`
# MAGIC - `is_in_list`
# MAGIC
# MAGIC You can find a list of all available built-in checks in the documentation [here](https://databrickslabs.github.io/dqx/docs/reference/quality_checks/).
# MAGIC
# MAGIC You can define checks in two ways:
# MAGIC * Declarative approach (YAML or JSON, or a table): Ideal for scenarios where checks are externalized from code.
# MAGIC * Code-first (DQX classes): Ideal if you need type-safety and better IDE support

# COMMAND ----------

# MAGIC %md
# MAGIC ### Defining Checks Declaratively in YAML
# MAGIC
# MAGIC We are demonstrating creating and validating a set of `quality rules (checks)` defined in YAML.
# MAGIC
# MAGIC We can use `validate_checks` to verify that the checks are defined correctly.

# COMMAND ----------

import yaml
from databricks.labs.dqx.engine import DQEngine


checks_from_yaml = yaml.safe_load("""
# 1. Ensure id, age, country are not null or empty (error-level)
- check:
    function: is_not_null_and_not_empty
    for_each_column:  # define check for multiple columns at once
      - id
      - age
      - country
  criticality: error

# 2. Warn if age is outside [18, 120] for Germany or France
- check:
    function: is_in_range
    filter: country in ['Germany', 'France']
    arguments:
      column: age  # define check for a single column
      min_limit: 18
      max_limit: 120
  criticality: warn
  name: age_not_in_range  # optional check name, auto-generated if not provided

# 3. Warn if country is not Germany or France
- check:
    function: is_in_list
    for_each_column:
      - country
    arguments:
      allowed:
        - Germany
        - France
  criticality: warn

# 4. Error if id is not unique across the dataset
- check:
    function: is_unique
    arguments:
      columns:
        - id
  criticality: error
""")

status = DQEngine.validate_checks(checks_from_yaml)
print(f"Checks from YAML: {status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup `DQEngine`

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient


ws = WorkspaceClient()  # auto-authenticated inside Databricks
dq_engine = DQEngine(ws)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Checks
# MAGIC
# MAGIC Apply checks using `apply_checks_by_metadata` to retains all original records and report data quality issues via additional `_errors` and `_warnings` columns in the DataFrame.

# COMMAND ----------

validated_df = dq_engine.apply_checks_by_metadata(new_users_df, checks_from_yaml)
display(validated_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Checks And Split
# MAGIC
# MAGIC Apply checks using `apply_checks_by_metadata_and_split` to quarantine invalid data. Data quality issues are captured in the invalid DataFrame (`invalid_df`) via `_errors` and `_warnings` columns. The valid DataFrame (`valid_df`) includes all records that passed (with either no errors or only warnings).

# COMMAND ----------

valid_df, invalid_df = dq_engine.apply_checks_by_metadata_and_split(new_users_df, checks_from_yaml)
display(valid_df)

# COMMAND ----------

display(invalid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Defining Checks using DQX classes
# MAGIC
# MAGIC We are demonstrating creating a set of `Quality Checks` using DQX classes.

# COMMAND ----------

from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule, DQForEachColRule, Criticality
from databricks.labs.dqx import check_funcs


checks = [
    # 1. Ensure id, age, country are not null or empty (error-level)
    *DQForEachColRule(
        columns=["id", "age", "country"],
        check_func=check_funcs.is_not_null_and_not_empty,
        criticality=Criticality.ERROR.value,
    ).get_rules(),

    # 2. Warn if age is outside [18, 120] for Germany or France
    DQRowRule(
        column="age",
        check_func=check_funcs.is_in_range,
        check_func_kwargs={"min_limit": 18, "max_limit": 120},
        filter="country IN ('Germany', 'France')",
        criticality=Criticality.WARN.value,
        name="age_not_in_range",
    ),

    # 3. Warn if country is not Germany or France
    *DQForEachColRule(
        columns=["country"],
        check_func=check_funcs.is_in_list,
        criticality=Criticality.WARN.value,
        check_func_kwargs={"allowed": ["Germany", "France"]},
    ).get_rules(),

    # 4. Error if id is not unique across the dataset
    DQDatasetRule(
        columns=["id"],
        check_func=check_funcs.is_unique,
        criticality=Criticality.ERROR.value,
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Checks
# MAGIC

# COMMAND ----------

validated_df = dq_engine.apply_checks(new_users_df, checks)
display(validated_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Checks And Split

# COMMAND ----------

valid_df, invalid_df = dq_engine.apply_checks_and_split(new_users_df, checks)
display(valid_df)

# COMMAND ----------

display(invalid_df)