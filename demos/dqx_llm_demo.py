# Databricks notebook source
# MAGIC %md
# MAGIC # Using DQX for LLM-based Primary Key Detection
# MAGIC DQX provides optional LLM-based primary key detection capabilities that can intelligently identify primary key columns from table schema and metadata, and generate appropriate quality rules. This feature uses Large Language Models to analyze table structures and suggest potential primary keys, enhancing the data profiling and quality rules generation process.
# MAGIC
# MAGIC The LLM-based primary key detection is completely optional and only activates when users explicitly install it. Regular DQX functionality works without any LLM dependencies.

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
    options={"enable_llm_pk_detection": True}  # Enable LLM-based primary key detection
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
    options={
        "enable_llm_pk_detection": True,  # Enable LLM-based detection
        # optional llm endpoint
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
# MAGIC ## Integrated Workflow: LLM Detection + Rule Generation
# MAGIC 
# MAGIC This demonstrates the complete workflow: profiling with LLM-based primary key detection and automatic rule generation using the proper DQX APIs.

# COMMAND ----------

# Create sample data for demonstration
sample_data = [
    (1, "A001", "John", "Doe"),
    (2, "A002", "Jane", "Smith"), 
    (3, "A003", "Bob", "Johnson"),
    (4, "A004", "Alice", "Brown")
]

sample_df = spark.createDataFrame(
    sample_data, 
    ["id", "customer_id", "first_name", "last_name"]
)

# Display sample data
sample_df.show()

# COMMAND ----------

from databricks.labs.dqx.profiler.generator import DQGenerator

# Profile the sample data with LLM-based primary key detection enabled
ws = WorkspaceClient()
profiler = DQProfiler(ws)

# Create a temporary view for profiling
sample_df.createOrReplaceTempView("sample_customers")

# Profile with LLM-based primary key detection
summary_stats, profiles = profiler.profile_table(
    "sample_customers",
    options={"enable_llm_pk_detection": True}
)

print("✅ Profiling completed with LLM-based primary key detection")

# Generate quality rules from the profiles (this will include LLM-detected primary key rules)
generator = DQGenerator(ws)
generated_rules = generator.generate_dq_rules(profiles)

print(f"\n📋 Generated {len(generated_rules)} quality rules:")
for rule in generated_rules:
    print(f"   - {rule['name']} ({rule['criticality']})")
    if 'llm_based_detection' in rule.get('user_metadata', {}):
        print(f"     🤖 LLM-detected primary key rule")

# Check if LLM detected any primary keys
if "llm_primary_key_detection" in summary_stats:
    pk_info = summary_stats["llm_primary_key_detection"]
    print(f"\n🔑 LLM detected primary key: {pk_info['detected_columns']}")
    print(f"   Confidence: {pk_info['confidence']}")
    print(f"   Has duplicates: {pk_info['has_duplicates']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying the Generated Quality Rules
# MAGIC 
# MAGIC Now let's apply the generated rules (including LLM-detected primary key rules) to validate data quality using the proper DQX workflow:

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine

# Apply all generated rules using DQEngine with metadata-based approach
dq_engine = DQEngine(workspace_client=ws)

# Option 1: Apply rules and annotate results as additional columns
annotated_df = dq_engine.apply_checks_by_metadata(sample_df, generated_rules)

print("✅ Applied all generated quality rules to sample data")
print("Result columns:", annotated_df.columns)

# Show results with quality check annotations
annotated_df.show()

# Option 2: Apply rules and split into valid and quarantine DataFrames
valid_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(sample_df, generated_rules)

print(f"\n📊 Data Quality Results:")
print(f"   Valid records: {valid_df.count()}")
print(f"   Quarantine records: {quarantine_df.count()}")

print("\n✅ Valid data:")
valid_df.show()

if quarantine_df.count() > 0:
    print("\n⚠️ Quarantined data:")
    quarantine_df.show()
else:
    print("\n🎉 No data quality issues found!")

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
# MAGIC - **🔀 Dataset Comparison**: Automatic matching key detection for dataset comparisons
