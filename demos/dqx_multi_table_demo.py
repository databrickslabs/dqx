# Databricks notebook source
# MAGIC %md
# MAGIC # DQX Multi-Table Data Quality Checks Demo
# MAGIC 
# MAGIC This notebook demonstrates how to apply data quality checks to multiple tables using DQX.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Installing DQX

# COMMAND ----------

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install '{dbutils.widgets.get("test_library_ref")}'
else:
    %pip install databricks-labs-dqx

%restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

from databricks.labs.dqx.config import ApplyChecksConfig, InputConfig, OutputConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx import check_funcs
from databricks.sdk import WorkspaceClient

# Default configuration values
default_catalog = "main"
default_schema = "default"

# Create widgets for configuration
dbutils.widgets.text("demo_catalog_name", default_catalog, "Catalog Name")
dbutils.widgets.text("demo_schema_name", default_schema, "Schema Name")

# Get configuration values
demo_catalog_name = dbutils.widgets.get("demo_catalog_name")
demo_schema_name = dbutils.widgets.get("demo_schema_name")

print(f"Using catalog: {demo_catalog_name}")
print(f"Using schema: {demo_schema_name}")

# Initialize the DQX engine
ws = WorkspaceClient()
dq_engine = DQEngine(ws)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checking multiple tables using Dictionary-Based Checks (Metadata)

# COMMAND ----------

# Create a sample users table
users_data = [
    [1, "john@email.com", "John Doe", "2023-01-01"],
    [2, "invalid-email", "Jane Smith", "2023-02-01"],
    [3, "bob@email.com", "Bob Wilson", "2023-03-01"],
    [None, "alice@email.com", "Alice Brown", "2023-04-01"]
]

users_df = spark.createDataFrame(
    users_data, 
    schema="user_id int, email string, name string, created_on string"
)
users_df.write.mode("overwrite").saveAsTable(f"{demo_catalog_name}.{demo_schema_name}.users")

# Create a sample orders table
orders_data = [
    [1, 1, 100.50, "2023-01-15"],
    [2, 2, -10.00, "2023-02-15"],
    [3, 3, 75.25, "2023-03-15"],
    [None, 4, 50.00, "2023-04-15"]
]

orders_df = spark.createDataFrame(
    orders_data,
    schema="order_id int, user_id int, total_amount double, order_on string"
)
orders_df.write.mode("overwrite").saveAsTable(f"{demo_catalog_name}.{demo_schema_name}.orders")

# Define checks for different tables using dictionaries
user_checks = [
    {
        "criticality": "error",
        "check": {
            "function": "is_not_null",
            "arguments": {"column": "user_id"}
        }
    },
    {
        "criticality": "warn", 
        "check": {
            "function": "regex_match",
            "arguments": {
                "column": "email",
                "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
            }
        }
    }
]

order_checks = [
    {
        "criticality": "error",
        "check": {
            "function": "is_not_null",
            "arguments": {"column": "order_id"}
        }
    },
    {
        "criticality": "error",
        "check": {
            "function": "is_not_less_than",
            "arguments": {"column": "total_amount", "min_value": 0}
        }
    }
]

# Define the configs
configs = [
    ApplyChecksConfig(
        input_config=InputConfig(location=f"{demo_catalog_name}.{demo_schema_name}.users"),
        output_config=OutputConfig(location=f"{demo_catalog_name}.{demo_schema_name}.users_checked"),
        quarantine_config=OutputConfig(location=f"{demo_catalog_name}.{demo_schema_name}.users_quarantine"),
        checks=user_checks
    ),
    ApplyChecksConfig(
        input_config=InputConfig(location=f"{demo_catalog_name}.{demo_schema_name}.orders"),
        output_config=OutputConfig(location=f"{demo_catalog_name}.{demo_schema_name}.orders_checked"),
        quarantine_config=OutputConfig(location=f"{demo_catalog_name}.{demo_schema_name}.orders_quarantine"),
        checks=order_checks
    )
]

# Apply checks to multiple tables and save the results
dq_engine.apply_checks_and_save_in_tables(
    configs=configs, max_parallelism=4
)

display(spark.table(f"{demo_catalog_name}.{demo_schema_name}.users_checked"))
display(spark.table(f"{demo_catalog_name}.{demo_schema_name}.orders_checked"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checking multiple tables using DQRule Objects

# COMMAND ----------

# Define checks using DQRule objects
user_rule_checks = [
    DQRowRule(
        criticality="error",
        check_func=check_funcs.is_not_null,
        column="user_id"
    ),
    DQRowRule(
        criticality="warn",
        check_func=check_funcs.regex_match,
        column="email",
        check_func_kwargs={
            "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        }
    )
]

order_rule_checks = [
    DQRowRule(
        criticality="error",
        check_func=check_funcs.is_not_null,
        column="order_id"
    ),
    DQRowRule(
        criticality="error",
        check_func=check_funcs.is_not_less_than,
        column="total_amount",
        check_func_kwargs={"min_value": 0}
    )
]

# Define the configs
configs = [
    ApplyChecksConfig(
        input_config=InputConfig(location=f"{demo_catalog_name}.{demo_schema_name}.users"),
        output_config=OutputConfig(location=f"{demo_catalog_name}.{demo_schema_name}.users_validated"),
        quarantine_config=OutputConfig(location=f"{demo_catalog_name}.{demo_schema_name}.users_issues"),
        checks=user_rule_checks
    ),
    ApplyChecksConfig(
        input_config=InputConfig(location=f"{demo_catalog_name}.{demo_schema_name}.orders"),
        output_config=OutputConfig(location=f"{demo_catalog_name}.{demo_schema_name}.orders_validated"),
        quarantine_config=OutputConfig(location=f"{demo_catalog_name}.{demo_schema_name}.orders_issues"),
        checks=order_rule_checks
    )
]

# Apply checks to multiple tables and save the results
dq_engine.apply_checks_and_save_in_tables(
    configs=configs, max_parallelism=4
)

display(spark.table(f"{demo_catalog_name}.{demo_schema_name}.users_validated"))
display(spark.table(f"{demo_catalog_name}.{demo_schema_name}.orders_validated"))



# COMMAND ----------

# MAGIC %md
# MAGIC ## Bulk processing of many tables

# COMMAND ----------

# Create sample tables for bulk processing demonstration
for i in range(1, 6):  # Create 5 sample tables
    sample_data = [
        [i, f"Item {i}", "2023-01-01"],
        [i+10, f"Item {i+10}", "2023-02-01"],
        [None, f"Item missing", "2023-03-01"]  # Missing ID
    ]
    
    sample_df = spark.createDataFrame(
        sample_data,
        schema="id int, name string, created_on string"
    )
    sample_df.write.mode("overwrite").saveAsTable(f"{demo_catalog_name}.{demo_schema_name}.demo_table_{i}")

# Create a configs for multiple tables in a list
configs = [
    ApplyChecksConfig(
        input_config=InputConfig(location=f"{demo_catalog_name}.{demo_schema_name}.demo_table_{i}"),
        output_config=OutputConfig(location=f"{demo_catalog_name}.{demo_schema_name}.demo_table_{i}_validated"),
        checks=[
            {
                "criticality": "error",
                "check": {
                    "function": "is_not_null",
                    "arguments": {"column": "id"}
                }
            },
            {
                "criticality": "warn",
                "check": {
                    "function": "sql_expression",
                    "arguments": {
                        "expression": "cast(created_on as date) <= current_date()",
                        "msg": "Created on should not be in the future"
                    }
                }
            }
        ]
    )
    for i in range(1, 6)
]

# Apply checks to multiple tables and save the results
dq_engine.apply_checks_and_save_in_tables(
    configs=configs, max_parallelism=3
)

display(spark.table(f"{demo_catalog_name}.{demo_schema_name}.demo_table_5_validated"))
