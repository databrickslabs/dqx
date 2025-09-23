# Databricks notebook source
# MAGIC %md
# MAGIC # DQX Multi-Table Data Quality Checks Demo
# MAGIC
# MAGIC This notebook demonstrates how to apply data quality checks to multiple tables at once using DQX.

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

from databricks.labs.dqx.config import InputConfig, OutputConfig, RunConfig
from databricks.labs.dqx.config import TableChecksStorageConfig
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
dq_engine = DQEngine(ws, spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checking multiple tables by providing specific configuration (run configs)

# COMMAND ----------

# Create a sample users table
users_data = [
    [1, "john@email.com", "John Doe", "2023-01-01"],
    [2, "invalid-email", "Jane Smith", "2023-02-01"],
    [3, "bob@email.com", "Bob Wilson", "2023-03-01"],
    [None, "alice@email.com", "Alice Brown", "2023-04-01"],
]

users_df = spark.createDataFrame(
    users_data,
    schema="user_id int, email string, name string, created_on string"
)
users_table = f"{demo_catalog_name}.{demo_schema_name}.users"
users_df.write.mode("overwrite").saveAsTable(users_table)

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
orders_table = f"{demo_catalog_name}.{demo_schema_name}.users_orders"
orders_df.write.mode("overwrite").saveAsTable(orders_table)

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
                "regex": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
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
            "arguments": {"column": "total_amount", "limit": 0}
        }
    }
]

# Save checks in a table
checks_table = f"{demo_catalog_name}.{demo_schema_name}.checks"
dq_engine.save_checks(user_checks, config=TableChecksStorageConfig(location=checks_table, run_config_name=users_table, mode="overwrite"))
dq_engine.save_checks(order_checks, config=TableChecksStorageConfig(location=checks_table, run_config_name=orders_table, mode="overwrite"))
display(spark.table(f"{demo_catalog_name}.{demo_schema_name}.checks"))

# Define the run configs
run_configs = [
    RunConfig(
        name=users_table,
        input_config=InputConfig(location=users_table),
        output_config=OutputConfig(
            location=f"{demo_catalog_name}.{demo_schema_name}.users_checked",
            mode="overwrite"
        ),
        # quarantine bad data
        quarantine_config=OutputConfig(
            location=f"{demo_catalog_name}.{demo_schema_name}.users_quarantine",
            mode="overwrite"
        ),
        checks_location=checks_table
    ),
    RunConfig(
        name=orders_table,
        input_config=InputConfig(location=orders_table),
        # don't quarantine bad data
        output_config=OutputConfig(
            location=f"{demo_catalog_name}.{demo_schema_name}.users_orders_checked",
            mode="overwrite"
        ),
        checks_location=checks_table
    )
]

# Apply checks to multiple tables and save the results
dq_engine.apply_checks_and_save_in_tables(run_configs=run_configs)

display(spark.table(f"{demo_catalog_name}.{demo_schema_name}.users_checked"))
display(spark.table(f"{demo_catalog_name}.{demo_schema_name}.users_quarantine"))
display(spark.table(f"{demo_catalog_name}.{demo_schema_name}.users_orders_checked"))

# COMMAND ----------

# clean up tables
spark.sql(f"drop table {demo_catalog_name}.{demo_schema_name}.users_checked")
spark.sql(f"drop table {demo_catalog_name}.{demo_schema_name}.users_quarantine")
spark.sql(f"drop table {demo_catalog_name}.{demo_schema_name}.users_orders_checked")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checking multiple tables using wildard pattern

# COMMAND ----------

# apply checks to multiple tables using patterns
dq_engine.apply_checks_and_save_in_tables_from_patterns(
    patterns=[f"{demo_catalog_name}.{demo_schema_name}.users*"],  # apply quality checks for all tables matching the pattern, can use wildcards
    checks_location=checks_table,  # run config name must be equal to the input table name
    quarantine=True,
    output_table_suffix="_checked",  # default _dq_output
    quarantine_table_suffix="_quarantine" # default _dq_quarantine
)

display(spark.table(f"{demo_catalog_name}.{demo_schema_name}.users_checked"))
display(spark.table(f"{demo_catalog_name}.{demo_schema_name}.users_quarantine"))
display(spark.table(f"{demo_catalog_name}.{demo_schema_name}.users_orders_checked"))
display(spark.table(f"{demo_catalog_name}.{demo_schema_name}.users_orders_quarantine"))


# COMMAND ----------

# clean up tables
spark.sql(f"drop table {demo_catalog_name}.{demo_schema_name}.users_checked")
spark.sql(f"drop table {demo_catalog_name}.{demo_schema_name}.users_quarantine")
spark.sql(f"drop table {demo_catalog_name}.{demo_schema_name}.users_orders_checked")
spark.sql(f"drop table {demo_catalog_name}.{demo_schema_name}.users_orders_quarantine")
spark.sql(f"drop table {checks_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## End-to-End approach: generate and apply checks based on wildcard patterns

# COMMAND ----------

# MAGIC %md
# MAGIC Profile input tables, generate and save checks

# COMMAND ----------

from databricks.labs.dqx.profiler.profiler import DQProfiler, DQProfile
from databricks.labs.dqx.profiler.generator import DQGenerator

profiler = DQProfiler(ws, spark)
generator = DQGenerator(ws)

patterns = [f"{demo_catalog_name}.{demo_schema_name}.users*"]
results = profiler.profile_tables(patterns=patterns)

for table, (summary_stats, profiles) in results.items():
    checks = generator.generate_dq_rules(profiles)
    print(f"Generated checks: {checks}")
    # run config name must be equal to the input table name
    dq_engine.save_checks(checks, config=TableChecksStorageConfig(location=checks_table, run_config_name=table, mode="overwrite"))

# COMMAND ----------

display(spark.table(checks_table))

# COMMAND ----------

# MAGIC %md
# MAGIC Apply the generated checks

# COMMAND ----------


# apply checks to multiple tables using patterns
dq_engine.apply_checks_and_save_in_tables_from_patterns(
    patterns=patterns,
    checks_location=checks_table,
    output_table_suffix="_checked",
)

display(spark.table(f"{demo_catalog_name}.{demo_schema_name}.users_checked"))
display(spark.table(f"{demo_catalog_name}.{demo_schema_name}.users_orders_checked"))