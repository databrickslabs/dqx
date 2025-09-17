# Databricks notebook source
# MAGIC %md
# MAGIC # Using DQX for LLM-based Primary Key Detection
# MAGIC DQX provides optional LLM-based primary key detection capabilities that can intelligently identify primary key columns from table schema and metadata. This feature uses Large Language Models to analyze table structures and suggest potential primary keys, enhancing the data profiling process.
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
    table_name="customers",
    catalog="main",
    schema="sales",
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
# MAGIC ## Key Features
# MAGIC 
# MAGIC - **🔧 Completely Optional**: Not activated by default - requires explicit enablement
# MAGIC - **🤖 Intelligent Detection**: Uses LLM analysis of table schema and metadata
# MAGIC - **✨ Multiple Activation Methods**: Various ways to enable when needed
# MAGIC - **🛡️ Graceful Fallback**: Clear messaging when dependencies unavailable
# MAGIC - **📊 Confidence Scoring**: Provides confidence levels and reasoning
# MAGIC - **🔄 Validation**: Optionally validates detected PKs for duplicates
