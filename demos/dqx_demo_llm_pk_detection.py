# Databricks notebook source
# MAGIC %md
# MAGIC # DQX - LLM-Based Primary Key Detection Demo
# MAGIC
# MAGIC This demo showcases DQX's LLM-based primary key detection capability, which uses AI to automatically identify primary key columns in your tables.
# MAGIC The primary key detection enables the generator to automatically create uniqueness rules and facilitates dataset comparison (in `compare_datasets` rule) when matching keys are not explicitly defined.
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
# MAGIC Detect a single-column primary key in a user table.

# COMMAND ----------

data = [
    (1, "Alice", "alice@example.com", "Engineering"),
    (2, "Bob", "bob@example.com", "Sales"),
    (3, "Charlie", "charlie@example.com", "Marketing"),
    (4, "David", "david@example.com", "Engineering"),
    (5, "Eve", "eve@example.com", "HR"),
]

df = spark.createDataFrame(data, ["user_id", "username", "email", "department"])
temp_table = "dqx_demo_simple_pk"
df.write.mode("overwrite").saveAsTable(temp_table)
display(df)

# COMMAND ----------

input_config = InputConfig(location=temp_table)
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
# MAGIC ## Example 2: Composite Primary Key Detection
# MAGIC
# MAGIC Detect a multi-column primary key in an order items table.

# COMMAND ----------

order_data = [
    (101, 1, "Laptop", 1200.00, 1),
    (101, 2, "Mouse", 25.00, 2),
    (102, 1, "Keyboard", 75.00, 1),
    (103, 1, "Monitor", 300.00, 1),
    (103, 2, "Webcam", 50.00, 1),
    (104, 1, "Headphones", 80.00, 2),
]

df_orders = spark.createDataFrame(
    order_data, 
    ["order_id", "line_item", "product_name", "price", "quantity"]
)

temp_table_composite = "dqx_demo_composite_pk"
df_orders.write.mode("overwrite").saveAsTable(temp_table_composite)
display(df_orders)

# COMMAND ----------

input_config_composite = InputConfig(location=temp_table_composite)
pk_result_composite = profiler.detect_primary_keys_with_llm(
    input_config=input_config_composite,
    llm_model_config=llm_model_config
)

print("=" * 80)
print("COMPOSITE PRIMARY KEY DETECTION")
print("=" * 80)
print(f"Table: {pk_result_composite.get('table')}")
print(f"Success: {pk_result_composite.get('success')}")
print(f"Primary Key Columns: {pk_result_composite.get('primary_key_columns')}")
print(f"Confidence: {pk_result_composite.get('confidence')}")
print(f"\nReasoning:\n{pk_result_composite.get('reasoning')}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 3: Generate Uniqueness Rules from Detected PKs
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
    print(f"Generated rule for table: {temp_table}")
    print(f"\nRule Details:")
    print(f"  - Name: {rule.get('name')}")
    print(f"  - Check: {rule.get('check')}")
    print(f"  - Criticality: {rule.get('criticality')}")
    print(f"  - Metadata: {rule.get('user_metadata')}")
    print("=" * 80)
else:
    print(f"Failed to detect primary keys: {pk_result.get('error')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 4: Use Detected PKs for Dataset Comparison
# MAGIC
# MAGIC Detect primary keys and use them to compare two datasets.

# COMMAND ----------

source_data = [
    (1, "Alice", "alice@example.com"),
    (2, "Bob", "bob@example.com"),
    (3, "Charlie", "charlie@example.com"),
]

target_data = [
    (1, "Alice", "alice@example.com"),
    (2, "Robert", "bob@example.com"),  # Name changed
    (4, "David", "david@example.com"),  # New record
]

df_source = spark.createDataFrame(source_data, ["id", "name", "email"])
df_target = spark.createDataFrame(target_data, ["id", "name", "email"])

temp_source = "dqx_demo_source"
temp_target = "dqx_demo_target"
df_source.write.mode("overwrite").saveAsTable(temp_source)
df_target.write.mode("overwrite").saveAsTable(temp_target)

# COMMAND ----------

from databricks.labs.dqx.check_funcs import compare_datasets

input_config_source = InputConfig(location=temp_source)
pk_result_source = profiler.detect_primary_keys_with_llm(
    input_config=input_config_source,
    llm_model_config=llm_model_config
)

if pk_result_source.get("success"):
    detected_keys = pk_result_source.get("primary_key_columns")
    print(f"Detected Primary Keys: {detected_keys}")
    
    # compare_datasets is a DQX check function - it returns (condition, apply_fn)
    condition, apply_fn = compare_datasets(
        columns=detected_keys,           # Primary key columns in source
        ref_columns=detected_keys,       # Primary key columns in target
        ref_df_name="target_df",         # Name for the reference dataframe
        check_missing_records=True,      # Check for missing records
    )
    
    # Apply the comparison
    df_with_comparison = apply_fn(df_source, spark, {"target_df": df_target})
    
    print("\n" + "=" * 80)
    print("DATASET COMPARISON RESULTS")
    print("=" * 80)
    print(f"Using detected primary keys: {detected_keys}")
    print(f"\nComparison results (showing first 20 rows):")
    display(df_with_comparison.limit(20))
else:
    print(f"Failed to detect primary keys: {pk_result_source.get('error')}")

# COMMAND ----------

# Cleanup
spark.sql(f"DROP TABLE IF EXISTS {temp_table}")
spark.sql(f"DROP TABLE IF EXISTS {temp_table_composite}")
spark.sql(f"DROP TABLE IF EXISTS {temp_source}")
spark.sql(f"DROP TABLE IF EXISTS {temp_target}")
