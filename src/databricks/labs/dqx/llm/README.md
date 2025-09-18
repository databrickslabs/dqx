# 🤖 LLM-Assisted Features

This module provides **optional** LLM-based features. The functionality is completely optional and only active when users explicitly request it.

## 🔑 Primary Key Detection

The LLM-based Primary Key Detection uses Large Language Models (via DSPy and Databricks Model Serving) to intelligently identify primary keys from table schema and metadata. 
This enhances the DQX profiling process by automatically detecting primary keys and generating appropriate uniqueness validation rules.

### How it Works

1. **Schema Analysis**: The detection process examines table structure, column names, data types, and constraints.
2. **LLM Processing**: Uses advanced language models to understand naming patterns and relationships.
3. **Confidence Scoring**: Provides confidence levels (high/medium/low) for detected primary keys.
4. **Duplicate Validation**: Optionally validates that detected columns actually contain unique values.
5. **Rule Generation**: Creates appropriate uniqueness quality checks for validating primary keys.

### When to Use Primary Key Detection

- **Data Discovery**: When exploring new datasets without documented primary keys.
- **Data Migration**: When migrating data between systems with different constraint definitions.
- **Data Quality Assessment**: To validate existing primary key assumptions.
- **Automated Profiling**: For large-scale data profiling across multiple tables.
- **Compare Datasets**: To improve accuracy of dataset comparison operations.

### 📦 Installation

```bash
# Install DQX with LLM dependencies using extras
pip install databricks-labs-dqx[llm]

# Now you can enable LLM features when needed
from databricks.labs.dqx.config import ProfilerConfig, LLMConfig

# model endpoint can be specified as needed
llm_config = LLMConfig(enable_pk_detection=True)
config = ProfilerConfig(llm_config=llm_config)
```

### Usage Examples

#### Method 1: Configuration-Based (Profiler Jobs)

```yaml
# config.yml - Configuration for profiler workflow
run_configs:
  - name: "default"
    input_config:
      location: "catalog.schema.table"
    profiler_config:
      # Enable LLM-based primary key detection
      llm_config:
        enable_pk_detection: true
        pk_detection_endpoint: "databricks-meta-llama-3-1-8b-instruct"
```

#### Method 2: Options-Based

```python
from databricks.labs.dqx.profiler.profiler import DQProfiler

profiler = DQProfiler(ws)

# Enable via options parameter
summary_stats, dq_rules = profiler.profile_table(
    "catalog.schema.table",
    options={
        "llm": True,
        "llm_pk_detection_endpoint": "databricks-meta-llama-3-1-8b-instruct"
    }
)

# Or use the explicit flag
summary_stats, dq_rules = profiler.profile_table(
    "catalog.schema.table",
    options={"enable_llm_pk_detection": True}
)
```

#### Method 3: Direct Detection
```python
from databricks.labs.dqx.profiler.profiler import DQProfiler

profiler = DQProfiler(ws)

# Direct LLM-based primary key detection
result = profiler.detect_primary_keys_with_llm(
    table="main.sales.customers",
    llm=True,
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
```

### 📊 Output & Metadata

#### Summary Statistics

```python
summary_stats["llm_primary_key_detection"] = {
    "detected_columns": ["customer_id", "order_id"],  # Detected PK columns
    "confidence": "high",  # LLM confidence level
    "has_duplicates": False,  # Duplicate validation result
    "validation_performed": True,  # Whether validation was run
    "method": "llm_based"  # Detection method
}
```

#### Generated Rules

```python
{
    "check": {
        "function": "is_unique",
        "arguments": {
            "columns": ["customer_id", "order_id"],
            "nulls_distinct": False
        }
    },
    "name": "primary_key_customer_id_order_id_validation",
    "criticality": "error",
    "user_metadata": {
        "pk_detection_confidence": "high",
        "pk_detection_reasoning": "LLM analysis of schema and metadata",
        "detected_primary_key": True,
        "llm_based_detection": True,
        "detection_method": "llm_analysis",
        "requires_llm_dependencies": True
    }
}
```

### 🔧 Troubleshooting

#### Common Issues

1. ImportError: No module named 'dspy'

```bash
pip install dspy-ai databricks_langchain
```

2. LLM Detection Not Running

- Ensure `llm=True` or `enable_llm_pk_detection=True`
- Check that LLM dependencies are installed

3. Low Confidence Results

- Review table schema and metadata quality
- Consider using different LLM endpoints
- Validate results manually

4. Performance Issues

- Use sampling for large tables
- Adjust retry limits
- Consider caching results

#### **Debug Mode**

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Enable detailed logging
profiler.detect_primary_keys_with_llm(table, llm=True)
```
