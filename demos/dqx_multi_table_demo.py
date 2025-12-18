# Databricks notebook source
# MAGIC %md
# MAGIC # DQX Multi-Table Data Quality Checks Demo
# MAGIC
# MAGIC This notebook demonstrates how to profile and apply data quality checks to multiple tables in a single method call.

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

import yaml
from databricks.labs.dqx.config import InputConfig, OutputConfig, RunConfig
from databricks.labs.dqx.config import TableChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

# Default configuration values
default_catalog = "main"
default_schema = "default"

# Create widgets for configuration
dbutils.widgets.text("demo_catalog", default_catalog, "Catalog Name")
dbutils.widgets.text("demo_schema", default_schema, "Schema Name")

# Get configuration values
demo_catalog_name = dbutils.widgets.get("demo_catalog")
demo_schema_name = dbutils.widgets.get("demo_schema")

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

# Define checks
user_checks = yaml.safe_load("""
    - criticality: error
      check:
        function: is_not_null
        arguments:
          column: user_id
    - criticality: warn
      check:
        function: regex_match
        arguments:
          column: email
          regex: ^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$
    """)

order_checks = yaml.safe_load("""
    - criticality: error
      check:
        function: is_not_null
        arguments:
          column: order_id
    - criticality: warn
      check:
        function: is_not_less_than
        arguments:
          column: total_amount
          limit: 0
    """)

# Save checks in a table
checks_table = f"{demo_catalog_name}.{demo_schema_name}.checks"
dq_engine.save_checks(user_checks, config=TableChecksStorageConfig(location=checks_table, run_config_name=users_table, mode="overwrite"))
dq_engine.save_checks(order_checks, config=TableChecksStorageConfig(location=checks_table, run_config_name=orders_table, mode="overwrite"))
display(spark.table(f"{demo_catalog_name}.{demo_schema_name}.checks"))

# Define run configs
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

# Clean up tables
spark.sql(f"drop table {demo_catalog_name}.{demo_schema_name}.users_checked")
spark.sql(f"drop table {demo_catalog_name}.{demo_schema_name}.users_quarantine")
spark.sql(f"drop table {demo_catalog_name}.{demo_schema_name}.users_orders_checked")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checking multiple tables using wildcard patterns

# COMMAND ----------

# Apply checks to multiple tables using patterns, but skip existing output and quarantine tables based on the suffixes
dq_engine.apply_checks_and_save_in_tables_for_patterns(
    patterns=[f"{demo_catalog_name}.{demo_schema_name}.users*"],  # apply quality checks for all tables matching the patterns
    exclude_patterns=["*_checked", "*_quarantine"], # skip existing output tables
    checks_location=checks_table,  # as delta table or absolute workspace or volume directory. For file based locations, checks are expected to be found under {checks_location}/{table_name}.yml.
    run_config_template=RunConfig(
        # input config is auto-created if not provided; location is skipped in any case and derived from patterns
        input_config=InputConfig(""),
        # input config is auto-created if not provided; location is skipped in any case and derived from patterns + output_table_suffix
        output_config=OutputConfig(location="", mode="overwrite"),
        # (optional) quarantine bad data; location is skipped in any case and derived from patterns + quarantine_table_suffix
        quarantine_config=OutputConfig(location="", mode="overwrite"),
        # skip checks_location of the run config as it is derived separately
    ),
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
# MAGIC Profile input tables, generate and save checks.

# COMMAND ----------

from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator

profiler = DQProfiler(ws, spark)
generator = DQGenerator(ws)

# Include tables matching the patterns, but skip existing output and quarantine tables based on the suffixes
patterns = [f"{demo_catalog_name}.{demo_schema_name}.users*"]
exclude_patterns=["*_checked", "*_quarantine"] # skip existing output tables based on suffixes

results = profiler.profile_tables_for_patterns(
    patterns=patterns,
    exclude_patterns=exclude_patterns,
)

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


# Apply checks on multiple tables using patterns
dq_engine.apply_checks_and_save_in_tables_for_patterns(
    patterns=patterns,
    exclude_patterns=exclude_patterns,  # skip existing output tables
    checks_location=checks_table,
    output_table_suffix="_checked",
    # run_config_template with quarantine_config not provided - don't quarantine bad data
)

display(spark.table(f"{demo_catalog_name}.{demo_schema_name}.users_checked"))
display(spark.table(f"{demo_catalog_name}.{demo_schema_name}.users_orders_checked"))