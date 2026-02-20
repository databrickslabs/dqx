# Databricks notebook source
# MAGIC %md
# MAGIC # DQX - Data Quality Framework Demo
# MAGIC
# MAGIC Welcome to this hands-on introduction to **DQX**, a data quality framework for Apache Spark developed by Databricks Labs.
# MAGIC
# MAGIC ## What is DQX?
# MAGIC
# MAGIC DQX enables you to define, monitor, and address data quality issues in your Python-based data pipelines. It provides:
# MAGIC
# MAGIC - Row and dataset-level quality rules
# MAGIC - Data quality metadata for failed checks
# MAGIC - Support for Spark Batch and Streaming, including Lakeflow Declarative Pipelines
# MAGIC - Automated profiling and rule generation, including AI-Assisted Rules Generation
# MAGIC - Data quality metrics to drive alerts and dashboards
# MAGIC
# MAGIC **DQX Documentation**: https://databrickslabs.github.io/dqx/
# MAGIC
# MAGIC Let's get started!
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install DQX
# MAGIC
# MAGIC For this demo, we install DQX with LLM extension and dbldatagen for sample data generation: `pip install databricks-labs-dqx[llm] dbldatagen`

# COMMAND ----------

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install 'databricks-labs-dqx[llm] @ {dbutils.widgets.get("test_library_ref")}'
else:
    %pip install databricks-labs-dqx[llm]

%pip install dbldatagen
%restart_python

# COMMAND ----------

default_catalog = "main"
default_schema = "default"

dbutils.widgets.text("demo_catalog", default_catalog, "Catalog Name")
dbutils.widgets.text("demo_schema", default_schema, "Schema Name")

demo_catalog_name = dbutils.widgets.get("demo_catalog")
demo_schema_name = dbutils.widgets.get("demo_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Sample Data
# MAGIC
# MAGIC We'll use **Databricks Labs Data Generator** to create some example customer transactions data to validate throughout this notebook.
# MAGIC
# MAGIC **Data Generator Documentation**: https://databrickslabs.github.io/dbldatagen/public_docs/index.html
# MAGIC
# MAGIC In the following cell, we'll generate a dataset with:
# MAGIC - Customer IDs
# MAGIC - Transaction dates
# MAGIC - Transaction amounts
# MAGIC - Email addresses
# MAGIC - Product categories
# MAGIC

# COMMAND ----------

import dbldatagen as dg
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

# Define a data generation specification
row_count = 1000
data_spec = (
    dg.DataGenerator(spark, name="transaction_data", rows=row_count, random=True)
    .withIdOutput()
    .withColumn("transaction_id", IntegerType(), minValue=1, maxValue=10000, uniqueValues=row_count, percentNulls=0.01)
    .withColumn("customer_id", IntegerType(), minValue=1000, maxValue=9999, random=True, percentNulls=0.01)
    .withColumn("email", StringType(), template=r"\w.\w@\w.com", random=True, percentNulls=0.01)
    .withColumn("transaction_date", TimestampType(), 
                begin="2024-01-01 00:00:00", 
                end="2024-12-31 23:59:59",
                interval="1 minute", random=True, percentNulls=0.01)
    .withColumn("amount", DoubleType(), minValue=-1.0, maxValue=5000.0, random=True, step=0.01)
    .withColumn("product_category", StringType(), 
                values=["Electronics", "Clothing", "Food", "Books", "Home", "Sports", ""],
                random=True, weights=[16, 16, 16, 16, 16, 16, 4])
    .withColumn("payment_method", StringType(),
                values=["credit_card", "debit_card", "paypal", "bank_transfer", "BAD_VALUE"],
                random=True, weights=[40, 29, 19, 10, 2])
)

# Build a DataFrame with transactions data
df_transactions = data_spec.build()
input_table = f"{demo_catalog_name}.{demo_schema_name}.transactions"
df_transactions.write.mode("overwrite").saveAsTable(input_table)
display(df_transactions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Basic DQX Rule Structure
# MAGIC
# MAGIC ### Understanding DQX Rules
# MAGIC
# MAGIC A DQX rule consists of:
# MAGIC - **Name**: An optional identifier for the rule
# MAGIC - **Check**: Defines the validation and consists of a *function* (e.g. `is_not_null`) and the associated *arguments* (e.g. `column`)
# MAGIC - **Criticality**: Either `warn` or `error` to indicate severity
# MAGIC - **Filter**: (Optional) Apply the check to rows that satisfy the filter condition
# MAGIC
# MAGIC ### Best Practices:
# MAGIC - Use `warn` level for data issues you want to track but not block
# MAGIC - Use `error` level for critical data quality requirements
# MAGIC - Add descriptive `Name` for easier tracking
# MAGIC
# MAGIC ### Basic Workflow
# MAGIC
# MAGIC 1. Define your data quality rules
# MAGIC 2. Apply rules to your data (can be `DataFrame`, `catalog`, `schema` or `table`) using the `DQEngine`
# MAGIC 4. Review the validation results and metrics
# MAGIC
# MAGIC Let's start with a simple example.
# MAGIC

# COMMAND ----------

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

# Define a simple rule: transaction_id should not be null
rules = yaml.safe_load(
    """
    - name: transaction_id_not_null
      check:
        function: is_not_null
        arguments:
          column: transaction_id
      criticality: error
    """
)

# Create the `DQEngine`
engine = DQEngine(WorkspaceClient())

# Apply data quality checks to the DataFrame
checked_df = engine.apply_checks_by_metadata(df_transactions, rules)
display(checked_df)

# COMMAND ----------

# Apply data quality checks and quarantine bad data
valid_df, quarantine_df = engine.apply_checks_by_metadata_and_split(df_transactions, rules)
display(quarantine_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Row-Level Checks
# MAGIC
# MAGIC Row-level checks validate individual rows against specific criteria. DQX provides many built-in checks, e.g.
# MAGIC
# MAGIC - `is_not_null`: Ensures a column has no null values
# MAGIC - `is_not_less_than`: Validates numeric values meet a minimum threshold
# MAGIC - `regex_match`: Validates strings match a pattern
# MAGIC - `is_not_in_future`: Ensures dates and timestamps are not in the future
# MAGIC - `is_in_range`: Checks if values fall within a specified range
# MAGIC - `is_in_list`: Validates values are in an allowed list
# MAGIC - `sql_expression`: Validates values satisfy sql expression
# MAGIC
# MAGIC For full list of checks, refer to the docs: https://databrickslabs.github.io/dqx/docs/reference/quality_checks/
# MAGIC

# COMMAND ----------

# Define multiple row-level rules
row_rules = yaml.safe_load(
    """
    - name: transaction_id_not_null
      check:
        function: is_not_null
        arguments:
          column: transaction_id
      criticality: error
    - name: customer_id_not_null
      check:
        function: is_not_null
        arguments:
          column: customer_id
      criticality: error
    - name: amount_not_negative
      check:
        function: is_not_less_than
        arguments:
          column: amount
          limit: 0
      criticality: error
    - name: valid_product_category
      check:
        function: is_not_null_and_not_empty
        arguments:
          column: product_category
      criticality: warn
    - name: valid_payment_method
      check:
        function: is_in_list
        arguments:
          column: payment_method
          allowed:
            - credit_card
            - debit_card
            - paypal
            - bank_transfer
      criticality: error
    - name: invalid_payment_method
      check:
        function: sql_expression
        arguments:
          expression: payment_method NOT IN ('debit_card', 'credit_card') OR amount > 50
      criticality: error
      filter: transaction_date >= '2024-07-08T05:38:00.000+00:00'
    """
)

# Apply rules on the DataFrame (table-level methods exist as well)
valid_df, quarantine_df = engine.apply_checks_by_metadata_and_split(df_transactions, row_rules)
display(quarantine_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Dataset-Level Checks
# MAGIC
# MAGIC Dataset-level checks validate properties of the entire dataset or group of rows rather than individual rows. These are useful for:
# MAGIC
# MAGIC - **Aggregate validations**: Checking sums, averages, and counts across a dataset or group of rows
# MAGIC - **Uniqueness of values**: Checking that values are distinct (e.g. for validating primary keys or foreign keys)
# MAGIC - **Referential integrity**: Enforcing foreign key values
# MAGIC - **Schema validation**: Ensuring the processed `DataFrame` has the expected structure
# MAGIC - **Dataset comparisons**: Comparing two datasets for differences
# MAGIC
# MAGIC ### Common Dataset-Level Checks:
# MAGIC - `is_aggr_not_less_than`: Validates aggregate values meet expected limits
# MAGIC - `is_unique`: Validates that values in a column or set of columns (composite key) are unique
# MAGIC - `has_valid_schema` Ensures schema matches expectations
# MAGIC - `sql_query`: Validate that arbitrary SQL query condition is satisfied
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 10
# Define dataset-level rules
dataset_rules = yaml.safe_load(
    """
    - name: amount_per_day_less_than_treshold
      check:
        function: is_aggr_not_less_than
        arguments:
          column: amount
          limit: 1000
          aggr_type: sum
          group_by:
          - transaction_date
      criticality: error
    - name: id_is_unique
      check:
        function: is_unique
        arguments:
          columns:
            - id
      criticality: error
    - name: has_valid_schema
      check:
        function: has_valid_schema
        arguments:
          expected_schema: "id INT, transaction_id INT, customer_id INT, email STRING, transaction_date TIMESTAMP, amount DOUBLE, product_category STRING"
      criticality: error
    - name: weekly_transaction_count_per_product_less_than_treshold
      check:
        function: sql_query
        arguments:
          query: |
            SELECT
              product_category,
              CASE
                WHEN SUM(last_week_count) < 100 THEN TRUE
                ELSE FALSE
              END AS condition
            FROM (
              SELECT
                id,
                product_category,
                -- count transactions in the 7-day window per product_category
                COUNT(*) OVER (
                  PARTITION BY product_category
                  ORDER BY transaction_date
                  RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW
                ) - 1 AS last_week_count  -- exclude the current row itself
              FROM {{ input }}
            ) t
            GROUP BY product_category
          msg: Last week's transaction count for the product category is below the expected threshold of 10.
          condition_column: condition
          merge_columns: 
          - product_category
          input_placeholder: input
        criticality: error
    """
)

# Apply rules on the DataFrame (table-level methods exist as well)
valid_df, quarantine_df = engine.apply_checks_by_metadata_and_split(df_transactions, dataset_rules)
display(quarantine_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Profiling and Rule Generation
# MAGIC
# MAGIC DQX can automatically profile your data and generate quality rules based on the observed statistical patterns. This is incredibly useful for discovering data characteristics, generating baseline rules, and identifying anomalies.
# MAGIC

# COMMAND ----------

from databricks.labs.dqx.profiler.profiler import DQProfiler
from pprint import pprint

profiler = DQProfiler(WorkspaceClient())
stats, profiles = profiler.profile(df_transactions, options={'sample_fraction': 1.0, 'limit': 1000000})

print(profiles)

# COMMAND ----------

from databricks.labs.dqx.profiler.generator import DQGenerator
import yaml

generator = DQGenerator(WorkspaceClient())
generated_rules = generator.generate_dq_rules(profiles)

print(yaml.safe_dump(generated_rules))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. AI-Assisted Rule Generation
# MAGIC
# MAGIC DQX can leverage Large Language Models (LLMs) to generate intelligent data quality rules based on column names, data patterns, and business context. This enables you to create domain-specific rules without deep data knowledge.
# MAGIC

# COMMAND ----------

user_input = """
  email should end with ".com"
  amount should be positive
"""

# Generate new rules
business_rules = generator.generate_dq_rules_ai_assisted(user_input)

# You can combine checks
rules = business_rules + generated_rules

print(yaml.safe_dump(rules))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Storing and Loading Checks from Delta Tables
# MAGIC
# MAGIC For production workflows, you'll want to store your data quality rules in a centralized location. DQX supports saving and loading rules from Delta tables, enabling version control, collaboration, reusability, and audit trails.
# MAGIC

# COMMAND ----------

from databricks.labs.dqx.config import TableChecksStorageConfig

# Define the path for storing rules (adjust based on your environment)
rules_table = f"{demo_catalog_name}.{demo_schema_name}.dq_checks"

# Combine all our rules from previous examples
all_rules = row_rules + dataset_rules + business_rules

# Save rules to Delta table
engine.save_checks(all_rules, config=TableChecksStorageConfig(location=rules_table, mode="overwrite"))

display(spark.table(rules_table))

# COMMAND ----------

# Load the rules from Delta table
loaded_rules = engine.load_checks(config=TableChecksStorageConfig(location=rules_table))

print(yaml.safe_dump(loaded_rules))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Summary Metrics
# MAGIC
# MAGIC DQX provides functionality to capture and store aggregate statistics about your data quality across all tables in a central place. This allows you to track data quality trends over time, monitor the health of your data pipelines, and gain insights into the overall quality of your datasets.
# MAGIC
# MAGIC See more here: https://databrickslabs.github.io/dqx/docs/guide/summary_metrics/

# COMMAND ----------

output_table = f"{demo_catalog_name}.{demo_schema_name}.transactions_valid"
quarantine_table = f"{demo_catalog_name}.{demo_schema_name}.transactions_invalid"
metrics_table = f"{demo_catalog_name}.{demo_schema_name}.dq_metrics"

# COMMAND ----------

from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.labs.dqx.config import InputConfig, OutputConfig

# Create the engine with observer
engine = DQEngine(WorkspaceClient(), observer=DQMetricsObserver(name="metrics"))

# Create configs
input_config = InputConfig(input_table)
output_config = OutputConfig(output_table)
quarantine_config = OutputConfig(quarantine_table)  # optional
metrics_config = OutputConfig(metrics_table)  # optional

# Use End to End method: read the data, apply the checks, write data to valid, quarantine and metrics tables
engine.apply_checks_by_metadata_and_save_in_table(
    checks=loaded_rules,
    input_config=input_config,
    output_config=output_config,
    quarantine_config=quarantine_config,
    metrics_config=metrics_config
)

# COMMAND ----------

display(spark.table(metrics_table))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. DQX Dashboard
# MAGIC
# MAGIC The dashboard makes it easy to monitor and track data quality issues across all tables, with the flexibility to customize it to your specific needs.
# MAGIC
# MAGIC ### Import the dashboard
# MAGIC
# MAGIC Go to Dashboards and import the DQX Dahsboard from: https://github.com/databrickslabs/dqx/tree/v0.13.0/src/databricks/labs/dqx/dashboards
# MAGIC
# MAGIC Then provide your Summary Metrics and output tables. 
# MAGIC
# MAGIC See more here: https://databrickslabs.github.io/dqx/docs/guide/quality_dashboard/

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC ### Production Implementation
# MAGIC
# MAGIC - Guide: https://databrickslabs.github.io/dqx/docs/guide/best_practices/
# MAGIC
# MAGIC ### Additional Resources
# MAGIC
# MAGIC - **DQX Documentation**: https://databrickslabs.github.io/dqx/
# MAGIC - **DQX GitHub**: https://github.com/databrickslabs/dqx
# MAGIC - **Data Generator Docs**: https://databrickslabs.github.io/dbldatagen/
# MAGIC
# MAGIC ### Tips for Success
# MAGIC
# MAGIC 1. **Start small**: Begin with critical tables and rules and expand gradually
# MAGIC 2. **Use profiling**: Let DQX discover patterns in your data by using profiler
# MAGIC 3. **Monitor trends**: Track metrics over time to catch degradation early
# MAGIC 4. **Collaborate**: Leverage the DQX App to build and manage rules centrally. 
# MAGIC 5. **Iterate**: Refine rules based on production experience
# MAGIC
# MAGIC Happy data quality monitoring! 🎯
# MAGIC