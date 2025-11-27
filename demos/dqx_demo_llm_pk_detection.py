# Databricks notebook source
# MAGIC %md
# MAGIC # DQX - LLM-Based Primary Key Detection Demo
# MAGIC
# MAGIC This demo showcases DQX's LLM-based primary key detection capability, which uses AI to automatically identify primary key columns in your tables.
# MAGIC
# MAGIC ## What is LLM-Based Primary Key Detection?
# MAGIC
# MAGIC Primary keys are crucial for data quality. DQX leverages Large Language Models to automatically:
# MAGIC - **Analyze table schemas** and identify potential primary key columns (single or composite)
# MAGIC - **Validate uniqueness** and check for duplicates
# MAGIC - **Provide reasoning** for the detected primary keys
# MAGIC
# MAGIC ## Key Use Cases for Data Quality
# MAGIC
# MAGIC 1. **Detecting Keys for Comparing Datasets**: Automatically identify join keys when comparing source and target datasets (eg. when using `compare_datesets` check), enabling accurate data validation without manual schema analysis.
# MAGIC
# MAGIC 2. **Discovering Keys for Checking Uniqueness**: Generate `is_unique` data quality rules based on detected primary keys to ensure data integrity.
# MAGIC
# MAGIC 3. **Data Profiling**: Understand table relationships and unique identifiers during initial data exploration.
# MAGIC
# MAGIC 4. **Schema Documentation**: Generate documentation for undocumented or legacy tables.
# MAGIC
# MAGIC ## Available methods
# MAGIC
# MAGIC - **Standalone PK detection**:  
# MAGIC   Use `result = profiler.detect_primary_keys_with_llm()` to detect primary keys in tables.
# MAGIC
# MAGIC - **PK detection as part of standard profiling methods**:  
# MAGIC   Use any of the profiling methods, e.g. `summary_stats, profiles = profiler.profile(df)`
# MAGIC
# MAGIC - **Generating uniqueness rules**:  
# MAGIC   Use `rules = generator.generate_dq_rules(profiles)` to create `is_unique` data quality rules based on pk profiles from the profiler.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites
# MAGIC
# MAGIC To use DQX LLM-based features, install DQX with `llm` extras:
# MAGIC
# MAGIC ```
# MAGIC %pip install databricks-labs-dqx[llm]
# MAGIC ```

# COMMAND ----------

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install 'databricks-labs-dqx[llm] @ {dbutils.widgets.get("test_library_ref")}'
else:
    %pip install databricks-labs-dqx[llm]

%restart_python

# COMMAND ----------

model_name = "databricks/databricks-claude-sonnet-4-5"
dbutils.widgets.text("model_name", model_name, "Model Name")
model_name = dbutils.widgets.get("model_name")

# COMMAND ----------

import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.config import LLMModelConfig
from databricks.labs.dqx.rule import DQDatasetRule
from databricks.labs.dqx import check_funcs
import pyspark.sql.functions as F

# COMMAND ----------

ws = WorkspaceClient()

# define LLM model config
llm_model_config = LLMModelConfig(model_name=model_name)

# COMMAND ----------

# Use existing sample table
input_table_name = "samples.tpch.customer"

input_df = spark.table(input_table_name)

# Preview the table structure
display(input_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standalone method for detecting Primary Keys (PKs)

# COMMAND ----------

profiler = DQProfiler(ws, spark, llm_model_config=llm_model_config)
pk_result = profiler.detect_primary_keys_with_llm(table=input_table_name)
pk_columns = pk_result.get('primary_key_columns')

print("=" * 80)
print("PRIMARY KEY DETECTION RESULT")
print("=" * 80)
print(f"Table: {pk_result.get('table')}")
print(f"Success: {pk_result.get('success')}")
print(f"Primary Key Columns: {pk_result.get('primary_key_columns')}")
print(f"Confidence: {pk_result.get('confidence')}")
print(f"Has Duplicates: {pk_result.get('has_duplicates')}")
print(f"\nReasoning:\n{pk_result.get('reasoning')}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example usage of detected PKs for comparing datasets

# COMMAND ----------

checks = [
    DQDatasetRule(
        criticality="error",
        check_func=check_funcs.compare_datasets,
        columns=pk_columns,
        check_func_kwargs={
            "ref_columns": pk_columns,
            "ref_df_name": "ref_df_key",
        },
    ),
]

# prepare reference DataFrame
ref_df = input_df.withColumn("c_name", F.when(F.col("c_custkey") == F.lit("412445"), "fake").otherwise(F.col("c_name")))
ref_dfs = {"ref_df_key": ref_df}

dq_engine = DQEngine(ws, spark)
valid_df, quarantine_df = dq_engine.apply_checks_and_split(input_df, checks, ref_dfs=ref_dfs)
display(quarantine_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profiling and Generation of Uniqueness Rules
# MAGIC
# MAGIC Profile input data and auto-generate rules including `is_unique`.

# COMMAND ----------

profiler = DQProfiler(ws, spark)
generator = DQGenerator(ws, spark)

# COMMAND ----------

# run one of the profiling methods
summary_stats, profiles = profiler.profile(input_df)

print("=" * 80)
print("SUMMARY STATISTICS")
print("=" * 80)
print(summary_stats)

print("=" * 80)
print("PROFILES")
print("=" * 80)
print(profiles)

# COMMAND ----------

checks = generator.generate_dq_rules(profiles)  # with default level "error"
print(yaml.safe_dump(checks))