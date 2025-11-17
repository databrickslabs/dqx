# Databricks notebook source
# MAGIC %md
# MAGIC # DQX - LLM-Based Primary Key Detection Demo
# MAGIC
# MAGIC This demo showcases DQX's LLM-based primary key detection capability, which uses AI to automatically identify primary key columns in your tables.
# MAGIC
# MAGIC ## What is LLM-Based Primary Key Detection?
# MAGIC
# MAGIC Primary keys are crucial for data quality, but identifying them manually can be time-consuming, especially in large or complex datasets. DQX leverages Large Language Models (LLMs) to:
# MAGIC
# MAGIC - **Analyze table schemas** and metadata
# MAGIC - **Identify potential primary key columns** (single or composite)
# MAGIC - **Validate uniqueness** and check for duplicates
# MAGIC - **Provide reasoning** for the detected primary keys
# MAGIC
# MAGIC ## Use Cases
# MAGIC
# MAGIC - **Data Discovery**: Understand table relationships without manual analysis
# MAGIC - **Data Quality Profiling**: Automatically identify unique identifiers during profiling
# MAGIC - **Schema Documentation**: Generate documentation for undocumented tables
# MAGIC - **Migration Planning**: Identify keys before migrating to Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC # Install DQX with LLM extras
# MAGIC
# MAGIC To use DQX LLM-based features, DQX must be installed with `llm` extras: 
# MAGIC
# MAGIC `%pip install databricks-labs-dqx[llm]`

# COMMAND ----------

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")

if dbutils.widgets.get("test_library_ref") != "":
    %pip install 'databricks-labs-dqx[llm] @ {dbutils.widgets.get("test_library_ref")}'
else:
    %pip install databricks-labs-dqx[llm]

%restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration
# MAGIC
# MAGIC ### Model Configuration
# MAGIC
# MAGIC By default, DQX uses the `databricks-meta-llama-3-1-8b-instruct` foundation model endpoint. You can specify a different model endpoint if needed.
# MAGIC
# MAGIC ### Table Configuration
# MAGIC
# MAGIC You can detect primary keys for any table accessible in your Databricks workspace. Provide the fully qualified table name in the format: `catalog.schema.table`

# COMMAND ----------

# Configuration widgets
model_name = "databricks-meta-llama-3-1-8b-instruct"
default_table_name = "samples.tpch.customer"

dbutils.widgets.text("model_name", model_name, "Model Name")
dbutils.widgets.text("table_name", default_table_name, "Table Name")

model_name = dbutils.widgets.get("model_name")
table_name = dbutils.widgets.get("table_name")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.config import LLMModelConfig, InputConfig

# COMMAND ----------

# Initialize Workspace Client
ws = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1: Simple Primary Key Detection
# MAGIC
# MAGIC Let's create a simple table with a clear single-column primary key and detect it using LLM.

# COMMAND ----------

# Create sample data with a clear primary key
data = [
    (1, "Alice", "alice@example.com", "Engineering"),
    (2, "Bob", "bob@example.com", "Sales"),
    (3, "Charlie", "charlie@example.com", "Marketing"),
    (4, "David", "david@example.com", "Engineering"),
    (5, "Eve", "eve@example.com", "HR"),
]

df = spark.createDataFrame(data, ["user_id", "username", "email", "department"])

# Create temporary table
temp_table = "dqx_demo_simple_pk"
df.write.mode("overwrite").saveAsTable(temp_table)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Detect Primary Key with LLM

# COMMAND ----------

# Configure LLM model
llm_model_config = LLMModelConfig(model_name=model_name)

# Create DQ Generator with LLM support
generator = DQGenerator(ws, spark, llm_model_config=llm_model_config)

# Detect primary keys
input_config = InputConfig(location=temp_table)
pk_result = generator.detect_primary_keys_with_llm(
    input_config=input_config,
    validate_duplicates=True,
    fail_on_duplicates=True
)

print("=" * 80)
print("PRIMARY KEY DETECTION RESULT")
print("=" * 80)
print(f"Table: {pk_result.get('table')}")
print(f"Success: {pk_result.get('success')}")
print(f"Primary Key Columns: {pk_result.get('primary_key_columns')}")
print(f"Confidence: {pk_result.get('confidence')}")
print(f"Has Duplicates: {pk_result.get('has_duplicates')}")
print(f"Duplicate Count: {pk_result.get('duplicate_count')}")
print(f"\nReasoning:\n{pk_result.get('reasoning')}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 2: Composite Primary Key Detection
# MAGIC
# MAGIC Now let's create a table with a composite primary key (multiple columns together form the unique identifier).

# COMMAND ----------

# Create sample order items data with composite primary key
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

# Create temporary table
temp_table_composite = "dqx_demo_composite_pk"
df_orders.write.mode("overwrite").saveAsTable(temp_table_composite)

display(df_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Detect Composite Primary Key

# COMMAND ----------

# Detect primary keys for the composite key table
input_config_composite = InputConfig(location=temp_table_composite)
pk_result_composite = generator.detect_primary_keys_with_llm(
    input_config=input_config_composite,
    validate_duplicates=True
)

print("=" * 80)
print("COMPOSITE PRIMARY KEY DETECTION RESULT")
print("=" * 80)
print(f"Table: {pk_result_composite.get('table')}")
print(f"Success: {pk_result_composite.get('success')}")
print(f"Primary Key Columns: {pk_result_composite.get('primary_key_columns')}")
print(f"Confidence: {pk_result_composite.get('confidence')}")
print(f"Has Duplicates: {pk_result_composite.get('has_duplicates')}")
print(f"\nReasoning:\n{pk_result_composite.get('reasoning')}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 3: Table with No Clear Primary Key
# MAGIC
# MAGIC Let's see how the LLM handles a table where there's no obvious primary key (like a log table).

# COMMAND ----------

# Create sample log data with no clear primary key
log_data = [
    ("2024-01-15 10:30:00", "INFO", "Application started"),
    ("2024-01-15 10:31:00", "WARN", "High memory usage"),
    ("2024-01-15 10:32:00", "INFO", "User logged in"),
    ("2024-01-15 10:33:00", "ERROR", "Database connection failed"),
    ("2024-01-15 10:34:00", "INFO", "Retry successful"),
]

df_logs = spark.createDataFrame(log_data, ["timestamp", "log_level", "message"])

# Create temporary table
temp_table_logs = "dqx_demo_logs_no_pk"
df_logs.write.mode("overwrite").saveAsTable(temp_table_logs)

display(df_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Detect Primary Key (Expected: None or Low Confidence)

# COMMAND ----------

# Detect primary keys for the log table
input_config_logs = InputConfig(location=temp_table_logs)
pk_result_logs = generator.detect_primary_keys_with_llm(
    input_config=input_config_logs,
    validate_duplicates=True,
    fail_on_duplicates=False  # Don't fail if no clear key found
)

print("=" * 80)
print("NO CLEAR PRIMARY KEY SCENARIO")
print("=" * 80)
print(f"Table: {pk_result_logs.get('table')}")
print(f"Success: {pk_result_logs.get('success')}")
print(f"Primary Key Columns: {pk_result_logs.get('primary_key_columns')}")
print(f"Confidence: {pk_result_logs.get('confidence')}")
print(f"\nReasoning:\n{pk_result_logs.get('reasoning')}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 4: Table with Duplicates in Candidate Primary Key
# MAGIC
# MAGIC Let's create a table where the candidate primary key has duplicates.

# COMMAND ----------

# Create sample data with duplicate IDs (data quality issue)
duplicate_data = [
    (1, "Product A", 100.00),
    (2, "Product B", 150.00),
    (1, "Product A Duplicate", 100.00),  # Duplicate ID!
    (3, "Product C", 200.00),
    (4, "Product D", 120.00),
]

df_duplicates = spark.createDataFrame(
    duplicate_data, 
    ["product_id", "product_name", "price"]
)

# Create temporary table
temp_table_duplicates = "dqx_demo_duplicates"
df_duplicates.write.mode("overwrite").saveAsTable(temp_table_duplicates)

display(df_duplicates)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Detect Primary Key with Duplicates

# COMMAND ----------

# Detect primary keys for the table with duplicates
input_config_duplicates = InputConfig(location=temp_table_duplicates)
pk_result_duplicates = generator.detect_primary_keys_with_llm(
    input_config=input_config_duplicates,
    validate_duplicates=True,
    fail_on_duplicates=False  # Don't fail, just report duplicates
)

print("=" * 80)
print("PRIMARY KEY WITH DUPLICATES SCENARIO")
print("=" * 80)
print(f"Table: {pk_result_duplicates.get('table')}")
print(f"Success: {pk_result_duplicates.get('success')}")
print(f"Primary Key Columns: {pk_result_duplicates.get('primary_key_columns')}")
print(f"Confidence: {pk_result_duplicates.get('confidence')}")
print(f"Has Duplicates: {pk_result_duplicates.get('has_duplicates')}")
print(f"Duplicate Count: {pk_result_duplicates.get('duplicate_count')}")
print(f"\nReasoning:\n{pk_result_duplicates.get('reasoning')}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 5: Using with Profiler (Integrated Workflow)
# MAGIC
# MAGIC The LLM-based primary key detection is integrated into the DQX profiler workflow. When you run profiling with LLM enabled, primary keys are automatically detected and included in the summary statistics.

# COMMAND ----------

# Create a simple demo table for profiling
profile_data = [
    (1001, "John Doe", 28, "john@example.com", "New York"),
    (1002, "Jane Smith", 34, "jane@example.com", "San Francisco"),
    (1003, "Bob Johnson", 45, "bob@example.com", "Chicago"),
    (1004, "Alice Williams", 29, "alice@example.com", "Boston"),
]

df_profile = spark.createDataFrame(
    profile_data, 
    ["customer_id", "name", "age", "email", "city"]
)

temp_table_profile = "dqx_demo_profile"
df_profile.write.mode("overwrite").saveAsTable(temp_table_profile)

display(df_profile)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Profiler with LLM-based PK Detection

# COMMAND ----------

from databricks.labs.dqx.profiler.profiler import DQProfiler

# Create profiler to get table statistics
profiler = DQProfiler(ws)
df_to_profile = spark.table(temp_table_profile)
summary_stats, profiles = profiler.profile(df=df_to_profile)

# Add table name to summary stats
summary_stats["table"] = temp_table_profile

# Detect primary keys using LLM
input_config = InputConfig(location=temp_table_profile)
pk_result = generator.detect_primary_keys_with_llm(
    input_config=input_config,
    validate_duplicates=True,
    fail_on_duplicates=False
)

# Add PK results to summary stats
if pk_result.get("success"):
    summary_stats["primary_keys"] = {
        "columns": pk_result.get("primary_key_columns", []),
        "confidence": pk_result.get("confidence", "unknown"),
        "reasoning": pk_result.get("reasoning", ""),
        "has_duplicates": pk_result.get("has_duplicates", False),
        "duplicate_count": pk_result.get("duplicate_count", 0),
    }
else:
    summary_stats["primary_keys"] = {
        "error": pk_result.get("error", "Unknown error"),
    }

print("=" * 80)
print("PROFILER RESULTS WITH PRIMARY KEY DETECTION")
print("=" * 80)
print("\nSummary Statistics:")
print(f"Table: {summary_stats.get('table')}")
print(f"Row Count: {summary_stats.get('row_count')}")
print(f"Column Count: {summary_stats.get('column_count')}")

if "primary_keys" in summary_stats and "error" not in summary_stats["primary_keys"]:
    pk_info = summary_stats["primary_keys"]
    print("\nPrimary Key Information:")
    print(f"  Columns: {pk_info.get('columns')}")
    print(f"  Confidence: {pk_info.get('confidence')}")
    print(f"  Has Duplicates: {pk_info.get('has_duplicates')}")
    print(f"  Duplicate Count: {pk_info.get('duplicate_count')}")
    print(f"\nReasoning:\n{pk_info.get('reasoning')}")
else:
    error_msg = summary_stats.get("primary_keys", {}).get("error", "Unknown")
    print(f"\nPrimary Key Detection Failed: {error_msg}")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Let's clean up the temporary tables we created.

# COMMAND ----------

# Drop temporary tables
spark.sql(f"DROP TABLE IF EXISTS {temp_table}")
spark.sql(f"DROP TABLE IF EXISTS {temp_table_composite}")
spark.sql(f"DROP TABLE IF EXISTS {temp_table_logs}")
spark.sql(f"DROP TABLE IF EXISTS {temp_table_duplicates}")
spark.sql(f"DROP TABLE IF EXISTS {temp_table_profile}")

print("✅ Cleanup complete! All temporary tables have been dropped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to Run This Demo
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC 1. **Databricks Workspace**: You need access to a Databricks workspace with Unity Catalog enabled
# MAGIC 2. **LLM Model Serving Endpoint**: Ensure you have access to the configured LLM endpoint (default: `databricks-dbrx-instruct`)
# MAGIC 3. **Cluster Requirements**:
# MAGIC    - Databricks Runtime 14.3 LTS or higher
# MAGIC    - Recommended: Serverless compute or cluster with Shared access mode
# MAGIC
# MAGIC ### Running the Demo
# MAGIC
# MAGIC #### Option 1: Run in Databricks Workspace
# MAGIC
# MAGIC 1. **Upload this notebook** to your Databricks workspace:
# MAGIC    - Go to **Workspace** → Right-click on a folder → **Import**
# MAGIC    - Select this notebook file
# MAGIC
# MAGIC 2. **Attach to a cluster**:
# MAGIC    - Click **Connect** and select a cluster
# MAGIC    - Or use serverless compute (recommended)
# MAGIC
# MAGIC 3. **Run all cells**:
# MAGIC    - Click **Run All** to execute all cells
# MAGIC    - Or run cells individually by pressing `Shift + Enter`
# MAGIC
# MAGIC #### Option 2: Import from Repository
# MAGIC
# MAGIC 1. **Clone the DQX repository**:
# MAGIC    ```bash
# MAGIC    git clone https://github.com/databrickslabs/dqx.git
# MAGIC    ```
# MAGIC
# MAGIC 2. **Navigate to demos**:
# MAGIC    ```bash
# MAGIC    cd dqx/demos
# MAGIC    ```
# MAGIC
# MAGIC 3. **Upload to Databricks Workspace**:
# MAGIC    ```bash
# MAGIC    databricks workspace import dqx_demo_llm_pk_detection.py \
# MAGIC      /Users/<your-username>/dqx_demo_llm_pk_detection \
# MAGIC      --language PYTHON --format SOURCE
# MAGIC    ```
# MAGIC
# MAGIC #### Option 3: Run via Databricks CLI
# MAGIC
# MAGIC 1. **Install Databricks CLI**:
# MAGIC    ```bash
# MAGIC    pip install databricks-cli
# MAGIC    ```
# MAGIC
# MAGIC 2. **Configure authentication**:
# MAGIC    ```bash
# MAGIC    databricks configure --token
# MAGIC    ```
# MAGIC
# MAGIC 3. **Import and run**:
# MAGIC    ```bash
# MAGIC    databricks workspace import dqx_demo_llm_pk_detection.py \
# MAGIC      /Users/<your-username>/dqx_demo_llm_pk_detection --language PYTHON
# MAGIC    ```
# MAGIC
# MAGIC ### Configuration Options
# MAGIC
# MAGIC You can customize the demo using widgets at the top of the notebook:
# MAGIC
# MAGIC - **Model Name**: Change the LLM model endpoint (default: `databricks-dbrx-instruct`)
# MAGIC - **Table Name**: Specify your own table to analyze
# MAGIC - **Test Library Ref**: For testing unreleased versions of DQX
# MAGIC
# MAGIC ### Troubleshooting
# MAGIC
# MAGIC #### Error: "LLM engine not available"
# MAGIC - **Solution**: Ensure you installed DQX with LLM extras: `%pip install databricks-labs-dqx[llm]`
# MAGIC
# MAGIC #### Error: "Invalid access token" or "Permission denied"
# MAGIC - **Solution**: Check your model serving endpoint permissions and Databricks authentication
# MAGIC
# MAGIC #### Error: "Table not found"
# MAGIC - **Solution**: Ensure the table exists and you have SELECT permissions on it
# MAGIC
# MAGIC #### Slow performance
# MAGIC - **Solution**: Use serverless compute or a larger cluster for better LLM inference performance
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC 1. **Try with your own tables**: Replace the demo tables with your actual data tables
# MAGIC 2. **Integrate with profiling workflows**: Use the profiler runner to automatically detect PKs during profiling
# MAGIC 3. **Explore other LLM features**: Check out AI-assisted DQ rules generation (`dqx_demo_ai_assisted_checks_generation.py`)
# MAGIC 4. **Customize LLM models**: Experiment with different model endpoints for varied results
# MAGIC
# MAGIC ### Learn More
# MAGIC
# MAGIC - **DQX Documentation**: https://databrickslabs.github.io/dqx/
# MAGIC - **DQX GitHub**: https://github.com/databrickslabs/dqx
# MAGIC - **Contributing Guide**: https://databrickslabs.github.io/dqx/docs/dev/contributing/

