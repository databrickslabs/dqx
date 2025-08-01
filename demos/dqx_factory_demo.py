# Databricks notebook source
# MAGIC %md
# MAGIC # Using DQX Factory for Automatic Engine Selection
# MAGIC 
# MAGIC This notebook demonstrates how to use the DQX factory to automatically select
# MAGIC the appropriate engine based on the DataFrame type.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites
# MAGIC 
# MAGIC This notebook uses both Spark and Pandas DataFrames and requires the DQX library
# MAGIC with backend abstraction support.

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx pandas pyarrow --quiet

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import pandas as pd
try:
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F
    spark_available = True
except ImportError:
    spark_available = False
    
from databricks.labs.dqx.factory import get_dq_engine
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx.check_funcs import is_not_null, is_not_null_and_not_empty
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Sample Data
# MAGIC 
# MAGIC Let's create sample datasets with some data quality issues to demonstrate the functionality.

# COMMAND ----------

# Create sample data with some data quality issues
data = [
    {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'age': 30},
    {'id': 2, 'name': '', 'email': 'jane@example.com', 'age': 25},
    {'id': 3, 'name': 'Bob Smith', 'email': None, 'age': 35},
    {'id': 4, 'name': 'Alice Johnson', 'email': 'alice@example.com', 'age': None},
    {'id': 5, 'name': None, 'email': 'charlie@example.com', 'age': 28},
]

# Create Pandas DataFrame
pandas_df = pd.DataFrame(data)
print("Pandas DataFrame:")
print(pandas_df)

# Create Spark DataFrame if Spark is available
if spark_available:
    try:
        spark_df = spark.createDataFrame(data)
        print("\nSpark DataFrame:")
        spark_df.show()
    except Exception as e:
        print(f"\nFailed to create Spark DataFrame: {e}")
        spark_df = None
        spark_available = False
else:
    spark_df = None

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
# MAGIC ## Use Factory to Get Appropriate Engine
# MAGIC 
# MAGIC Now we'll use the factory to automatically select the appropriate engine
# MAGIC based on the DataFrame type.

# COMMAND ----------

# Initialize the workspace client (mock for demo purposes)
# In a real scenario, you would use actual workspace credentials
ws = WorkspaceClient(host='https://example.com', token='dummy')

# Get engine for Pandas DataFrame
pandas_engine = get_dq_engine(ws, pandas_df)
print(f"Engine for Pandas DataFrame: {type(pandas_engine).__name__}")

# Get engine for Spark DataFrame if available
if spark_available:
    spark_engine = get_dq_engine(ws, spark_df)
    print(f"Engine for Spark DataFrame: {type(spark_engine).__name__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Data Quality Checks
# MAGIC 
# MAGIC Now we'll apply the checks to our DataFrames using the appropriate engines.

# COMMAND ----------

# Apply checks to Pandas DataFrame
try:
    pandas_result = pandas_engine.apply_checks(pandas_df, checks)
    print("Pandas DataFrame with quality checks applied:")
    print(pandas_result)
except Exception as e:
    print(f"Error applying checks to Pandas DataFrame: {e}")
    print("This is expected as the current implementation is a placeholder.")

# Apply checks to Spark DataFrame if available
if spark_available:
    try:
        spark_result = spark_engine.apply_checks(spark_df, checks)
        print("\nSpark DataFrame with quality checks applied:")
        spark_result.show()
    except Exception as e:
        print(f"Error applying checks to Spark DataFrame: {e}")
        print("This is expected as the current implementation is a placeholder.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC To fully support automatic engine selection, the following work is needed:
# MAGIC 
# MAGIC 1. Complete the implementation of the Pandas engine to actually apply checks
# MAGIC 2. Implement a PyArrow engine or adapt the Pandas engine to work with PyArrow
# MAGIC 3. Ensure all check functions work with different backends
# MAGIC 4. Add comprehensive tests for the factory pattern
# MAGIC 5. Document usage patterns for automatic engine selection
