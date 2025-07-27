# Databricks notebook source
# MAGIC %md
# MAGIC # Using DQX with Pandas DataFrames
# MAGIC 
# MAGIC This notebook demonstrates how to use DQX for data quality checks with Pandas DataFrames
# MAGIC for local data analysis without requiring a Spark cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites
# MAGIC 
# MAGIC This notebook uses Pandas and requires the DQX library with Pandas backend support.

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx pandas pyarrow --quiet

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import pandas as pd
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx.check_funcs import is_not_null, is_not_null_and_not_empty
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Sample Data
# MAGIC 
# MAGIC Let's create a sample dataset with some data quality issues to demonstrate the functionality.

# COMMAND ----------

# Create sample data with some data quality issues
data = [
    {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'age': 30},
    {'id': 2, 'name': '', 'email': 'jane@example.com', 'age': 25},
    {'id': 3, 'name': 'Bob Smith', 'email': None, 'age': 35},
    {'id': 4, 'name': 'Alice Johnson', 'email': 'alice@example.com', 'age': None},
    {'id': 5, 'name': None, 'email': 'charlie@example.com', 'age': 28},
]

df = pd.DataFrame(data)
print("Sample data:")
print(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Data Quality Checks
# MAGIC 
# MAGIC We'll define some basic data quality checks to identify null or empty values.

# COMMAND ----------

# Define data quality checks
checks = [
    DQRowRule(criticality='error', check_func=is_not_null, column='name'),
    DQRowRule(criticality='error', check_func=is_not_null, column='email'),
    DQRowRule(criticality='warn', check_func=is_not_null_and_not_empty, column='name'),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Data Quality Checks
# MAGIC 
# MAGIC Now we'll apply the checks to our Pandas DataFrame using DQX.

# COMMAND ----------

# Initialize the DQX engine
# Note: For local Pandas usage, we can use a mock workspace client
ws = WorkspaceClient(host='https://example.com', token='dummy')
dq_engine = DQEngine(ws)

# Apply checks to the DataFrame
# TODO: This would need to be updated to support Pandas DataFrames
# For now, this demonstrates the intended API
try:
    checked_df = dq_engine.apply_checks(df, checks)
    print("Data with quality checks applied:")
    print(checked_df)
except Exception as e:
    print(f"Error applying checks: {e}")
    print("This is expected as the current implementation only supports Spark DataFrames.")
    print("The backend abstraction layer will enable Pandas support in future versions.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC To fully support Pandas DataFrames, the DQX engine needs to be updated to use
# MAGIC the backend abstraction layer we've designed. This would involve:
# MAGIC 
# MAGIC 1. Modifying the DQEngine class to detect the DataFrame type
# MAGIC 2. Using the appropriate backend implementation for DataFrame operations
# MAGIC 3. Ensuring all check functions work with Pandas DataFrames
# MAGIC 4. Adding comprehensive tests for Pandas usage
