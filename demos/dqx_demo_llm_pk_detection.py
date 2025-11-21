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
# MAGIC 1. **Detecting Keys for Comparing Datasets**: Automatically identify join keys when comparing source and target datasets, enabling accurate data validation without manual schema analysis.
# MAGIC
# MAGIC 2. **Discovering Keys for Checking Uniqueness**: Generate `is_unique` data quality rules based on detected primary keys to ensure data integrity.
# MAGIC
# MAGIC 3. **Data Profiling**: Understand table relationships and unique identifiers during initial data exploration.
# MAGIC
# MAGIC 4. **Schema Documentation**: Generate documentation for undocumented or legacy tables.

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

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.config import LLMModelConfig, InputConfig

# COMMAND ----------

ws = WorkspaceClient()
profiler = DQProfiler(ws, spark)

llm_model_config = LLMModelConfig(model_name=model_name)
generator = DQGenerator(ws, spark, llm_model_config=llm_model_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## LLM-Based Primary Key Detection
# MAGIC
# MAGIC DQX supports LLM-based primary key detection to intelligently identify primary key columns from table schema and metadata. The following configurations are available:
# MAGIC
# MAGIC - **Model Serving Endpoint**:  
# MAGIC   By default, DQX uses the `databricks/databricks-claude-sonnet-4-5` model serving endpoint for primary key detection. However, users can specify a different model endpoint if they prefer to use another one.
# MAGIC
# MAGIC - **Profiler for Detection**:  
# MAGIC   Use `DQProfiler.detect_primary_keys_with_llm()` to detect primary keys in tables.
# MAGIC
# MAGIC - **Generator for Rules**:  
# MAGIC   Use `DQGenerator.dq_generate_is_unique()` to create `is_unique` validation rules based on detected primary keys.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1: Simple Primary Key Detection
# MAGIC
# MAGIC Detect a single-column primary key using the Databricks `samples.tpch.customer` table.

# COMMAND ----------

# Use existing sample table
table_name_simple = "samples.tpch.customer"

# Preview the table structure
display(spark.table(table_name_simple).limit(5))

# COMMAND ----------

input_config = InputConfig(location=table_name_simple)
pk_result = profiler.detect_primary_keys_with_llm(input_config=input_config, llm_model_config=llm_model_config)

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
# MAGIC ## Example 2: Generate Uniqueness Rules from Detected PKs
# MAGIC
# MAGIC Use detected primary keys to create `is_unique` data quality rules with the generator.

# COMMAND ----------

# First detect PKs using profiler, then create rules using generator
if pk_result.get("success"):
    detected_columns = pk_result.get("primary_key_columns", [])
    
    rule = generator.dq_generate_is_unique(
        column=",".join(detected_columns),
        level="error",
        columns=detected_columns,
        confidence=pk_result.get("confidence", "unknown"),
        reasoning=pk_result.get("reasoning", ""),
        llm_detected=True,
    )
    
    print("=" * 80)
    print("GENERATED UNIQUENESS RULE")
    print("=" * 80)
    print(f"Generated rule for table: {table_name_simple}")
    print(f"\nRule Details:")
    print(f"  - Name: {rule.get('name')}")
    print(f"  - Check: {rule.get('check')}")
    print(f"  - Criticality: {rule.get('criticality')}")
    print(f"  - Metadata: {rule.get('user_metadata')}")
    print("=" * 80)
else:
    print(f"Failed to detect primary keys: {pk_result.get('error')}")
