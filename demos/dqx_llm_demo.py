# Databricks notebook source
# MAGIC %md
# MAGIC # Using DQX for LLM-based Primary Key Detection
# MAGIC DQX provides optional LLM-based primary key detection capabilities that can intelligently identify primary key columns from table schema and metadata. This feature uses Large Language Models to analyze table structures and suggest potential primary keys, enhancing the data profiling and quality rules generation process.
# MAGIC
# MAGIC The LLM-based primary key detection is completely optional and only activates when users explicitly request it. Regular DQX functionality works without any LLM dependencies.

# COMMAND ----------

# MAGIC %md
# MAGIC # Install DQX with LLM extras
# MAGIC
# MAGIC To enable LLM-based primary key detection, DQX has to be installed with `llm` extras: 
# MAGIC
# MAGIC `%pip install databricks-labs-dqx[llm]`

# COMMAND ----------

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install 'databricks-labs-dqx[llm] @ {dbutils.widgets.get("test_library_ref")}'
else:
    %pip install databricks-labs-dqx[llm]

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.config import ProfilerConfig, LLMConfig
from databricks.labs.dqx.profiler.profiler import DQProfiler

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regular Profiling (No LLM Dependencies Required)
# MAGIC 
# MAGIC By default, DQX works without any LLM dependencies. Regular profiling functionality is always available.

# COMMAND ----------

# Default configuration - no LLM features
config = ProfilerConfig()
print(f"LLM PK Detection: {config.llm_config.enable_pk_detection}")  # False by default

# This works without any LLM dependencies!
print("✅ Regular profiling works out of the box!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## LLM-Based Primary Key Detection
# MAGIC 
# MAGIC When explicitly requested, DQX can use LLM-based analysis to detect potential primary keys in your tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 1: Configuration-based enablement

# COMMAND ----------

# Enable LLM-based PK detection via configuration
config = ProfilerConfig(llm_config=LLMConfig(enable_pk_detection=True))
print(f"LLM PK Detection: {config.llm_config.enable_pk_detection}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2: Options-based enablement

# COMMAND ----------

ws = WorkspaceClient()
profiler = DQProfiler(ws)

# Enable via options parameter
summary_stats, dq_rules = profiler.profile_table(
    "catalog.schema.table", 
    options={"llm": True}  # Simple LLM enablement
)
print("✅ LLM-based profiling enabled!")

# Check if primary key was detected
if "llm_primary_key_detection" in summary_stats:
    pk_info = summary_stats["llm_primary_key_detection"]
    print(f"Detected PK: {pk_info['detected_columns']}")
    print(f"Confidence: {pk_info['confidence']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 3: Direct detection method

# COMMAND ----------

# Direct LLM-based primary key detection
result = profiler.detect_primary_keys_with_llm(
    table="customers",
    llm=True,  # Explicit LLM enablement required
    options={
        "llm_pk_detection_endpoint": "databricks-meta-llama-3-1-8b-instruct"
    }
)

if result and result.get("success", False):
    print(f"✅ Detected PK: {result['primary_key_columns']}")
    print(f"Confidence: {result['confidence']}")
    print(f"Reasoning: {result['reasoning']}")
else:
    print("❌ Primary key detection failed or returned no results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rules Generation with is_unique
# MAGIC 
# MAGIC Once primary keys are detected via LLM, DQX can automatically generate `is_unique` data quality rules to validate the uniqueness of those columns.

# COMMAND ----------

from databricks.labs.dqx.profiler.generator import DQGenerator

# Example: Generate is_unique rule from LLM-detected primary key
detected_pk_columns = ["customer_id", "order_id"]  # Example detected PK
confidence = "high"
reasoning = "LLM analysis indicates these columns form a composite primary key based on schema patterns"

# Generate is_unique rule using the detected primary key
is_unique_rule = DQGenerator.dq_generate_is_unique(
    column=",".join(detected_pk_columns),
    level="error",
    columns=detected_pk_columns,
    confidence=confidence,
    reasoning=reasoning,
    llm_detected=True,
    nulls_distinct=True  # Default behavior: NULLs are treated as distinct
)

print("Generated is_unique rule:")
print(f"Rule name: {is_unique_rule['name']}")
print(f"Function: {is_unique_rule['check']['function']}")
print(f"Columns: {is_unique_rule['check']['arguments']['columns']}")
print(f"Criticality: {is_unique_rule['criticality']}")
print(f"LLM detected: {is_unique_rule['user_metadata']['llm_based_detection']}")
print(f"Confidence: {is_unique_rule['user_metadata']['pk_detection_confidence']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Integrated Workflow: LLM Detection + Rule Generation
# MAGIC 
# MAGIC Here's how to combine LLM-based primary key detection with automatic rule generation:

# COMMAND ----------

# Create sample data for demonstration
sample_data = [
    (1, "A001", "John", "Doe"),
    (2, "A002", "Jane", "Smith"), 
    (3, "A001", "Bob", "Johnson"),  # Duplicate customer_id - should fail uniqueness
    (4, "A003", "Alice", "Brown")
]

sample_df = spark.createDataFrame(
    sample_data, 
    ["id", "customer_id", "first_name", "last_name"]
)

# Display sample data
sample_df.show()

# COMMAND ----------

# Simulate LLM detection result (in practice, this would come from the LLM)
llm_detection_result = {
    "success": True,
    "primary_key_columns": ["id"],  # LLM detected 'id' as primary key
    "confidence": "high",
    "reasoning": "Column 'id' appears to be an auto-incrementing identifier based on naming patterns and data distribution"
}

if llm_detection_result["success"]:
    # Generate is_unique rule from LLM detection
    pk_columns = llm_detection_result["primary_key_columns"]
    
    generated_rule = DQGenerator.dq_generate_is_unique(
        column=",".join(pk_columns),
        level="error",
        columns=pk_columns,
        confidence=llm_detection_result["confidence"],
        reasoning=llm_detection_result["reasoning"],
        llm_detected=True
    )
    
    print("✅ Generated is_unique rule from LLM detection:")
    print(f"   Rule: {generated_rule['name']}")
    print(f"   Columns: {generated_rule['check']['arguments']['columns']}")
    print(f"   Metadata: LLM-based detection with {generated_rule['user_metadata']['pk_detection_confidence']} confidence")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying the Generated is_unique Rule
# MAGIC 
# MAGIC Now let's apply the generated rule to validate data quality:

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQDatasetRule
from databricks.labs.dqx import check_funcs

# Convert the generated rule to a DQDatasetRule for execution
pk_columns = generated_rule['check']['arguments']['columns']
dq_rule = DQDatasetRule(
    name=generated_rule['name'],
    criticality=generated_rule['criticality'],
    check_func=check_funcs.is_unique,
    columns=pk_columns,
    check_func_kwargs={
        "nulls_distinct": generated_rule['check']['arguments']['nulls_distinct']
    }
)

# Apply the rule using DQEngine
dq_engine = DQEngine(workspace_client=ws)
result_df = dq_engine.apply_checks(sample_df, [dq_rule])

print("✅ Applied is_unique rule to sample data")
print("Result columns:", result_df.columns)

# Show results - the rule should pass since 'id' column has unique values
result_df.select("id", "customer_id", f"dq_check_{generated_rule['name']}").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Composite Primary Key Example
# MAGIC 
# MAGIC Let's demonstrate with a composite primary key detected by LLM:

# COMMAND ----------

# Sample data with composite key scenario
composite_data = [
    ("store_1", "2024-01-01", 100.0),
    ("store_1", "2024-01-02", 150.0),
    ("store_2", "2024-01-01", 200.0),
    ("store_2", "2024-01-02", 175.0),
    ("store_1", "2024-01-01", 120.0),  # Duplicate composite key - should fail
]

composite_df = spark.createDataFrame(
    composite_data,
    ["store_id", "date", "sales_amount"]
)

# Simulate LLM detecting composite primary key
composite_llm_result = {
    "success": True,
    "primary_key_columns": ["store_id", "date"],
    "confidence": "medium",
    "reasoning": "Combination of store_id and date appears to uniquely identify sales records based on business logic patterns"
}

# Generate composite is_unique rule
composite_rule = DQGenerator.dq_generate_is_unique(
    column=",".join(composite_llm_result["primary_key_columns"]),
    level="warn",  # Use warning level for this example
    columns=composite_llm_result["primary_key_columns"],
    confidence=composite_llm_result["confidence"],
    reasoning=composite_llm_result["reasoning"],
    llm_detected=True
)

print("Generated composite is_unique rule:")
print(f"Rule name: {composite_rule['name']}")
print(f"Columns: {composite_rule['check']['arguments']['columns']}")
print(f"Criticality: {composite_rule['criticality']}")

# COMMAND ----------

# Apply composite key validation
composite_dq_rule = DQDatasetRule(
    name=composite_rule['name'],
    criticality=composite_rule['criticality'],
    check_func=check_funcs.is_unique,
    columns=composite_rule['check']['arguments']['columns'],
    check_func_kwargs={
        "nulls_distinct": composite_rule['check']['arguments']['nulls_distinct']
    }
)

composite_result_df = dq_engine.apply_checks(composite_df, [composite_dq_rule])

print("✅ Applied composite is_unique rule")
print("Data with duplicates detected:")
composite_result_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Features
# MAGIC 
# MAGIC - **🔧 Completely Optional**: Not activated by default - requires explicit enablement
# MAGIC - **🤖 Intelligent Detection**: Uses LLM analysis of table schema and metadata
# MAGIC - **✨ Multiple Activation Methods**: Various ways to enable when needed
# MAGIC - **🛡️ Graceful Fallback**: Clear messaging when dependencies unavailable
# MAGIC - **📊 Confidence Scoring**: Provides confidence levels and reasoning
# MAGIC - **🔄 Validation**: Optionally validates detected PKs for duplicates
# MAGIC - **⚡ Automatic Rule Generation**: Converts detected PKs into executable `is_unique` rules
# MAGIC - **🔗 End-to-End Workflow**: From LLM detection to data quality validation
