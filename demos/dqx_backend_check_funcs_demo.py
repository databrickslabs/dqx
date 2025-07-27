# Databricks notebook source
# MAGIC %md
# MAGIC # Using Backend-Agnostic Check Functions
# MAGIC 
# MAGIC This notebook demonstrates how to use the new backend-agnostic check functions
# MAGIC that work with different DataFrame implementations.

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
    # Try to actually create a Spark session to check if Spark is available
    spark = SparkSession.builder.appName("DQX_Demo").getOrCreate()
    spark_available = True
except:
    spark_available = False
    
from databricks.labs.dqx.factory import get_dq_engine
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx.check_funcs_backend import is_not_null, is_not_null_and_not_empty
from databricks.labs.dqx.df_backend import get_backend
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
    spark_df = spark.createDataFrame(data)
    print("\nSpark DataFrame:")
    spark_df.show()
else:
    spark_df = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Data Quality Checks with Backend-Agnostic Functions
# MAGIC 
# MAGIC We'll define some basic data quality checks using the new backend-agnostic functions.

# COMMAND ----------

# Define data quality checks using backend-agnostic functions
checks = [
    DQRowRule(criticality='error', check_func=is_not_null, column='name'),
    DQRowRule(criticality='error', check_func=is_not_null, column='email'),
    DQRowRule(criticality='warn', check_func=is_not_null_and_not_empty, column='name'),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Backend-Agnostic Functions
# MAGIC 
# MAGIC Now we'll demonstrate how the backend-agnostic functions can work with different backends.

# COMMAND ----------

# Get backends
pandas_backend = get_backend('pandas')
print(f"Pandas backend: {type(pandas_backend).__name__}")

if spark_available:
    spark_backend = get_backend('spark')
    print(f"Spark backend: {type(spark_backend).__name__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Backend-Agnostic Check Functions
# MAGIC 
# MAGIC Now we'll apply the backend-agnostic check functions to our data.

# COMMAND ----------

# Apply checks using backend-agnostic functions
try:
    # Test with Pandas backend
    name_not_null_check = is_not_null('name', backend=pandas_backend)
    print(f"Pandas is_not_null check: {name_not_null_check}")
    
    name_not_empty_check = is_not_null_and_not_empty('name', backend=pandas_backend)
    print(f"Pandas is_not_null_and_not_empty check: {name_not_empty_check}")
    
    # Test with Spark backend if available
    if spark_available:
        name_not_null_check_spark = is_not_null('name', backend=spark_backend)
        print(f"Spark is_not_null check: {name_not_null_check_spark}")
        
        name_not_empty_check_spark = is_not_null_and_not_empty('name', backend=spark_backend)
        print(f"Spark is_not_null_and_not_empty check: {name_not_empty_check_spark}")
        
except Exception as e:
    print(f"Error applying backend-agnostic checks: {e}")
    print("This is expected as the current implementation is a placeholder.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC To fully support backend-agnostic check functions, the following work is needed:
# MAGIC 
# MAGIC 1. Complete the implementation of backend-agnostic check functions
# MAGIC 2. Ensure all check functions work with different backends
# MAGIC 3. Add comprehensive tests for backend-agnostic functions
# MAGIC 4. Document usage patterns for backend-agnostic functions
# MAGIC 5. Update existing code to use backend-agnostic functions where appropriate
