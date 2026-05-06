# AI-Assisted Primary Key Detection

DQX provides AI-assisted primary key detection that automatically identifies primary key columns in your tables using Large Language Models (LLMs). This feature analyzes table schemas and metadata to intelligently detect single or composite primary keys, and performs validation by checking for duplicate values.

## Overview[​](#overview "Direct link to Overview")

The LLM-based primary key detection capability supports:

* Analyzing table schemas to identify potential primary key columns
* Detecting both single-column and composite primary keys
* Validating uniqueness and checking for duplicate values
* Providing confidence scores and reasoning for detected keys
* Generating `is_unique` quality rules based on detected keys

When to use LLM-Based PK Detection

LLM-based primary key detection is ideal for:

* **Dataset Comparison**: Automatically identify join keys when comparing source and target datasets
* **Data Quality Rules**: Generate `is_unique` validation rules for detected primary keys
* **Data Profiling**: Understand table relationships and unique identifiers during exploration
* **Schema Documentation**: Document primary keys for undocumented or legacy tables
* **Migration Projects**: Quickly identify keys when migrating data between systems

### Model Access[​](#model-access "Direct link to Model Access")

The feature requires access to an LLM model. DQX supports:

* **Databricks Foundation Model APIs** (recommended): Use Databricks-hosted models like `databricks/databricks-claude-sonnet-4-5` (default)
* **Custom API endpoints**: Any OpenAI-compatible API endpoint

## Prerequisites[​](#prerequisites "Direct link to Prerequisites")

To use the LLM-based primary key detection feature, install DQX with the LLM extra dependencies:

```bash
pip install 'databricks-labs-dqx[llm]'

```

This will install required packages including DSPy and other LLM-related dependencies.

## Basic Usage[​](#basic-usage "Direct link to Basic Usage")

### Standalone method for detecting Primary Keys[​](#standalone-method-for-detecting-primary-keys "Direct link to Standalone method for detecting Primary Keys")

* Python

```python
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.config import InputConfig, LLMModelConfig
from databricks.sdk import WorkspaceClient

# Initialize workspace client and profiler
ws = WorkspaceClient()
profiler = DQProfiler(workspace_client=ws, spark=spark)

# Detect primary keys for a table
input_config = InputConfig(location="catalog.schema.users")
result = profiler.detect_primary_keys_with_llm(input_config=input_config)

# Display detected keys
if "primary_key_columns" in result:
  print(f"Primary Key: {result['primary_key_columns']}")
  print(f"Confidence: {result['confidence']}")
  print(f"Reasoning: {result['reasoning']}")

```

### Using Custom Model Configuration[​](#using-custom-model-configuration "Direct link to Using Custom Model Configuration")

You can configure the profiler to use custom models:

* Python

```python
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.config import InputConfig, LLMModelConfig
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()

# Configure custom model
llm_config = LLMModelConfig(
  model_name="databricks/databricks-llama-4-maverick"
)

profiler = DQProfiler(workspace_client=ws, spark=spark, llm_model_config=llm_config)

input_config = InputConfig(location="catalog.schema.users")
result = profiler.detect_primary_keys_with_llm(input_config=input_config)

print(f"Detected Primary Key: {result.get('primary_key_columns')}")

```

## Profiling and generating quality rules with uniqueness checks[​](#profiling-and-generating-quality-rules-with-uniqueness-checks "Direct link to Profiling and generating quality rules with uniqueness checks")

The DQX generator automatically creates `is_unique` rules for columns identified as primary keys by the profiler:

* Python

```python
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.config import InputConfig, LLMModelConfig
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()

# Create profiler and generator, the llm config is optional
llm_config = LLMModelConfig()
profiler = DQProfiler(workspace_client=ws, spark=spark, llm_model_config=llm_config)
generator = DQGenerator(workspace_client=ws, spark=spark, llm_model_config=llm_config)

# Run one of the profiling methods
input_config = InputConfig(location="catalog.schema.users")
summary_stats, profiles = profiler.profile_table(input_config=input_config)

# Generate quality checks from the profiles
checks = generator.generate_dq_rules(profiles)  # with default level "error"

print(f"Generated {len(checks)} quality checks")
for check in checks:
    print(f"- {check}")

```

## Using Detected Keys for Dataset Comparison[​](#using-detected-keys-for-dataset-comparison "Direct link to Using Detected Keys for Dataset Comparison")

One of the most powerful use cases is automatic join key detection for dataset comparison:

* Python

```python
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.config import InputConfig, LLMModelConfig
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.rule import DQDatasetRule
from databricks.labs.dqx import check_funcs

# Initialize workspace client and profiler
ws = WorkspaceClient()
profiler = DQProfiler(workspace_client=ws, spark=spark)

# Detect primary keys for a table
input_config = InputConfig(location="catalog.schema.users")
result = profiler.detect_primary_keys_with_llm(input_config=input_config)
pk_columns = result['primary_key_columns']

# Prepare compare datasets check
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

# Prepare the reference data
ref_df = spark.table("catalog.schema.ref_table")
ref_dfs = {"ref_df_key": ref_df}

# Create DQ engine and apply checks
dq_engine = DQEngine(ws, spark)
valid_df, quarantine_df = dq_engine.apply_checks_and_split(input_df, checks, ref_dfs=ref_dfs)

display(quarantine_df)

```

This automatically:

1. Detects primary keys from the reference table using LLM
2. Uses detected keys to join and compare the datasets
3. Returns comparison results with match/mismatch details

## Uniqueness rules generation using no-code approach (Profiler Workflow)[​](#uniqueness-rules-generation-using-no-code-approach-profiler-workflow "Direct link to Uniqueness rules generation using no-code approach (Profiler Workflow)")

You can run profiler workflow to run both the statistics-based rules generation and the AI-assisted rules generation. The profiler workflow saves the generated checks automatically in the checks location as defined in the [configuration file](/dqx/docs/installation.md#configuration-file). You need to install DQX as a tool in the workspace to have the profiler workflow available (see [installation guide](/dqx/docs/installation.md)). More information about the profiler workflow can be found [here](/dqx/docs/guide/data_profiling.md#no-code-approach-profiler-workflow).

### Configuration options[​](#configuration-options "Direct link to Configuration options")

The following fields from the [configuration file](/dqx/docs/installation.md#configuration-file) are used for the AI-assisted uniqueness rules generation:

* `llm_primary_key_detection`: whether to use llm-assisted pk detection to generate uniqueness checks (default: `false`)
* `llm_config`: (optional) configuration for the llm-assisted features

The AI-assisted rules generation is only supported within the workflow if DQX is installed with the serverless clusters used for execution, i.e. `serverless_clusters` setting enabled in the configuration file.

Example of the configuration file (relevant fields only):

```yaml
  serverless_clusters: true  # ai-assisted rules generation via workflow requires serverless cluster for execution
  llm_config:
    model:
      model_name: "databricks/databricks-claude-sonnet-4-5"
      api_key: xxx    # optional API key for the model as secret in the format: secret_scope/secret_key. Not required by foundational models
      api_base: xxx   # optional API base for the model as secret in the format: secret_scope/secret_key. Not required by foundational models
  run_configs:
  - name: default
    profiler_config:                     # profiler configuration
        llm_primary_key_detection: true  # whether to use llm-assisted pk detection to generate uniqueness check

```

The `api_key` and `api_base` are unnecessary when utilizing [Databricks foundational models](https://docs.databricks.com/aws/en/machine-learning/model-serving/foundation-model-overview). For enhanced security and streamlined workflow usage, it is advised to store them as [Databricks secrets](https://docs.databricks.com/aws/en/security/secrets/) within the configuration file. Use a slash-separated format for specifying the scope and key, such as `api_key: secret_scope/secret_key`.

## Troubleshooting[​](#troubleshooting "Direct link to Troubleshooting")

### Error: LLM dependencies not available[​](#error-llm-dependencies-not-available "Direct link to Error: LLM dependencies not available")

**Problem**: You receive an error saying "LLM engine not available. Make sure LLM dependencies are installed".

**Solution**: Install the LLM dependencies:

```bash
pip install 'databricks-labs-dqx[llm]'

```

### Low Confidence Scores[​](#low-confidence-scores "Direct link to Low Confidence Scores")

**Problem**: The LLM returns low confidence for detected primary keys.

**Solution**:

* Review the reasoning provided by the LLM
* Manually inspect the table schema and data
* Consider that the table might not have a clear primary key
* Check if the table has proper constraints or metadata

### Model Access Issues[​](#model-access-issues "Direct link to Model Access Issues")

**Problem**: Unable to connect to the LLM model endpoint.

**Solution**:

* Verify you have access to Databricks Foundation Model APIs
* Check your workspace permissions
* Try using the default model (`databricks/databricks-claude-sonnet-4-5`)
* Ensure your cluster has network access to the model endpoint

### Detected Keys Have Duplicates[​](#detected-keys-have-duplicates "Direct link to Detected Keys Have Duplicates")

**Problem**: The detected primary keys have duplicate values in the data.

**Solution**:

* The LLM suggests keys based on schema/metadata, not data content
* Use the generated `is_unique` rules to identify duplicate rows
* Clean the data or update the primary key definition
* Consider if you need a composite key instead of a single column

## Limitations[​](#limitations "Direct link to Limitations")

* Requires network access to the LLM model endpoint
* Detection is based on schema and metadata, not actual data analysis, but validation is performed
* May not detect surrogate keys or technical identifiers without clear naming patterns
* Requires additional LLM dependencies which increases package size
* LLM inference may take a few seconds to complete
