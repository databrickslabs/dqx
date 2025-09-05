## 🤖 LLM-Assisted Features

This module provides **optional** LLM-based primary key detection capabilities for the DQX data quality framework. The functionality is completely optional and only activates when users explicitly request it. Primary key detection can be used during data profiling and can also be enabled for `compare_datasets` checks to improve data comparison accuracy.

## 🎯 **Overview**

The LLM-based Primary Key Detection uses Large Language Models (via DSPy and Databricks Model Serving) to intelligently identify primary keys from table schema and metadata. This enhances the DQX profiling process by automatically detecting primary keys and generating appropriate uniqueness validation rules.

## ✅ **Key Features**

- **🔧 Completely Optional**: Not activated by default - requires explicit enablement
- **🤖 Intelligent Detection**: Uses LLM analysis of table schema and metadata
- **✨ Multiple Activation Methods**: Various ways to enable when needed
- **🛡️ Graceful Fallback**: Clear messaging when dependencies unavailable
- **⚡ Performance Optimized**: Lazy loading and conditional execution
- **🔍 Duplicate Validation**: Optionally validates detected PKs for duplicates
- **📊 Confidence Scoring**: Provides confidence levels and reasoning
- **🔄 Retry Logic**: Handles cases where initial detection finds duplicates

## 📦 **Installation**

### **LLM-Enhanced Usage**
```bash
# Install DQX with LLM dependencies using extras
databricks labs install dqx[llm]

# Now you can enable LLM features when needed
from databricks.labs.dqx.config import ProfilerConfig, LLMConfig
config = ProfilerConfig(llm_config=LLMConfig(enable_pk_detection=True))
```

## 🚀 **Usage Examples**

### **Method 1: Configuration-Based**
```python
from databricks.labs.dqx.config import ProfilerConfig, LLMConfig
from databricks.labs.dqx.profiler.runner import ProfilerRunner

# Default configuration (LLM features disabled)
config = ProfilerConfig()

# Enable LLM-based PK detection via configuration
config = ProfilerConfig(
    llm_config=LLMConfig(
        enable_pk_detection=True,
        pk_detection_endpoint="databricks-meta-llama-3-1-8b-instruct"
    )
)

runner = ProfilerRunner(ws, spark, installation, profiler, generator)
checks, summary_stats = runner.run(InputConfig(location="catalog.schema.table"), config)

# Check results
if "llm_primary_key_detection" in summary_stats:
    pk_info = summary_stats["llm_primary_key_detection"]
    print(f"Detected PK: {pk_info['detected_columns']}")
    print(f"Confidence: {pk_info['confidence']}")
```

### **Method 2: Options-Based**
```python
from databricks.labs.dqx.profiler.profiler import DQProfiler

profiler = DQProfiler(ws)

# Enable via options parameter
summary_stats, dq_rules = profiler.profile_table(
    "catalog.schema.table",
    options={
        "llm": True,  # Simple LLM enablement
        "llm_pk_detection_endpoint": "databricks-meta-llama-3-1-8b-instruct"
    }
)

# Or use the explicit flag
summary_stats, dq_rules = profiler.profile_table(
    "catalog.schema.table",
    options={"enable_llm_pk_detection": True}
)
```

### **Method 3: Convenience Method**
```python
from databricks.labs.dqx.profiler.profiler import DQProfiler

profiler = DQProfiler(ws)

# Use the convenience method - automatically enables LLM detection
summary_stats, dq_rules = profiler.profile_table_with_llm_pk_detection(
    table="catalog.schema.table",
    llm_endpoint="databricks-meta-llama-3-1-8b-instruct",
    sample_fraction=0.1
)

# Check if primary key was detected
if "llm_primary_key_detection" in summary_stats:
    pk_info = summary_stats["llm_primary_key_detection"]
    print(f"✅ Detected PK: {pk_info['detected_columns']}")
    print(f"Confidence: {pk_info['confidence']}")
    
    # Users can manually create validation rules based on detection results
    # Example: Create a uniqueness check for the detected primary key
    # (This would be done separately using the DQX rule creation APIs)
```

### **Method 4: Direct Detection**
```python
from databricks.labs.dqx.profiler.profiler import DQProfiler

profiler = DQProfiler(ws)

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
```

## ⚙️ **Configuration Options**

### **LLMConfig Fields**
```python
@dataclass
class LLMConfig:
    # LLM-based Primary Key Detection (Optional)
    enable_pk_detection: bool = False  # Enable LLM-based PK detection
    pk_detection_endpoint: str = "databricks-meta-llama-3-1-8b-instruct"  # LLM endpoint
    # Note: pk_validate_duplicates is always True and pk_max_retries is fixed to 3
    # Note: Automatic rule generation has been removed - users must manually create rules

@dataclass
class ProfilerConfig:
    # ... other fields ...
    llm_config: LLMConfig = field(default_factory=LLMConfig)  # LLM configuration
```

### **Options Parameter Flags**
```python
options = {
    # Simple enablement
    "llm": True,
    
    # Or explicit enablement
    "enable_llm_pk_detection": True,
    
    # Additional LLM options
    "llm_pk_detection_endpoint": "databricks-meta-llama-3-1-8b-instruct"
    # Note: llm_pk_validate_duplicates is always True and llm_pk_max_retries is fixed to 3
    # Note: Automatic rule generation has been removed - users must manually create rules
}
```

## 🏗️ **Architecture**

### **Core Components**

1. **`DatabricksPrimaryKeyDetector`** (`pk_identifier.py`)
   - Main LLM-based detection engine
   - Handles table metadata retrieval
   - Manages LLM interactions via DSPy
   - Performs duplicate validation
   - Implements retry logic

2. **`DQProfiler`** (Enhanced)
   - `detect_primary_keys_with_llm()` - Direct LLM detection method
   - `profile_table_with_llm_pk_detection()` - Convenience method
   - Lazy LLM imports and graceful fallback

3. **`DQGenerator`** (Enhanced)
   - `dq_generate_is_primary_key()` - Creates PK validation rules
   - Handles LLM-specific metadata
   - Generates traceability information

4. **`ProfilerRunner`** (Enhanced)
   - Passes LLM configuration options
   - Orchestrates LLM-enhanced profiling

### **Data Flow**

```
Regular Profiling (Default):
Input → ProfilerConfig() → DQProfiler → Standard Rules → Validation
  ↓         ↓                ↓             ↓             ↓
No LLM → Default Config → Regular Profile → DQ Rules → Data Validation

LLM-Enhanced Profiling (When Requested):
Input → ProfilerConfig(llm=True) → DQProfiler → LLM Detection → Enhanced Rules
  ↓         ↓                        ↓             ↓              ↓
LLM Hint → LLM Config → LLM Profile → PK Detection → PK + DQ Rules
```

### **LLM Detection Process**

```
1. Table Metadata Retrieval
   ↓
2. LLM Analysis (via DSPy)
   ↓
3. Primary Key Prediction
   ↓
4. Duplicate Validation (Optional)
   ↓
5. Retry Logic (If Duplicates Found)
   ↓
6. Result Generation
   ↓
7. Rule Creation
```

## 🛡️ **Error Handling & Fallback**

### **Graceful Dependency Handling**
```python
# If LLM dependencies not installed:
try:
    from databricks.labs.dqx.llm.pk_identifier import DatabricksPrimaryKeyDetector
    # ... LLM detection logic
except ImportError:
    logger.warning("LLM-based primary key detection requires additional dependencies. "
                   "Install with: pip install dspy-ai databricks_langchain")
    return None  # Graceful fallback
```

### **Configuration Validation**
```python
# LLM detection only runs when explicitly requested
llm_enabled = (options and 
              (options.get("enable_llm_pk_detection", False) or 
               options.get("llm", False) or 
               options.get("llm_pk_detection", False)))

if not llm_enabled:
    logger.debug("LLM-based PK detection not requested. Use llm=True to enable.")
    return None
```

### **Error Recovery**
- **Import Errors**: Clear messaging about missing dependencies
- **LLM Failures**: Graceful degradation to regular profiling
- **Duplicate Detection**: Retry logic with different approaches
- **Network Issues**: Timeout handling and retry mechanisms

## 📊 **Output & Metadata**

### **Summary Statistics**
```python
summary_stats["llm_primary_key_detection"] = {
    "detected_columns": ["customer_id", "order_id"],  # Detected PK columns
    "confidence": "high",  # LLM confidence level
    "has_duplicates": False,  # Duplicate validation result
    "validation_performed": True,  # Whether validation was run
    "method": "llm_based"  # Detection method
}
```

### **Generated Rules**
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

## 🧪 **Testing**

### **Unit Tests**
- `tests/unit/test_llm_based_pk_identifier.py` - Core LLM detection logic
- `tests/unit/test_llm_utils.py` - LLM utility functions

### **Integration Tests**
- `tests/integration/test_pk_detection_integration.py` - End-to-end integration

### **Running Tests**
```bash
# Run LLM-specific tests
python -m pytest tests/unit/test_llm_based_pk_identifier.py -v
python -m pytest tests/integration/test_pk_detection_integration.py -v

# Run all tests
python -m pytest tests/ -v
```

## 🎯 **Best Practices**

### **1. Start Simple**
```python
# Begin with regular profiling
config = ProfilerConfig()  # No LLM dependencies needed
```

### **2. Enable LLM When Needed**
```python
# Enable LLM for specific use cases
config = ProfilerConfig(llm_config=LLMConfig(enable_pk_detection=True))
```

### **3. Handle Dependencies Gracefully**
```python
try:
    # LLM-enhanced profiling
    summary_stats, dq_rules = profiler.profile_table_with_llm_pk_detection(table)
except ImportError:
    # Fallback to regular profiling
    summary_stats, dq_rules = profiler.profile_table(table)
```

### **4. Validate Results**
```python
if "llm_primary_key_detection" in summary_stats:
    pk_info = summary_stats["llm_primary_key_detection"]
    if pk_info["confidence"] in ["high", "medium"]:
        # Use the detected primary key
        print(f"Using detected PK: {pk_info['detected_columns']}")
    else:
        # Consider manual review
        print("Low confidence - manual review recommended")
```

## 🔧 **Troubleshooting**

### **Common Issues**

1. **ImportError: No module named 'dspy'**
   ```bash
   pip install dspy-ai databricks_langchain
   ```

2. **LLM Detection Not Running**
   - Ensure `llm=True` or `enable_llm_pk_detection=True`
   - Check that LLM dependencies are installed

3. **Low Confidence Results**
   - Review table schema and metadata quality
   - Consider using different LLM endpoints
   - Validate results manually

4. **Performance Issues**
   - Use sampling for large tables
   - Adjust retry limits
   - Consider caching results

### **Debug Mode**
```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Enable detailed logging
profiler.detect_primary_keys_with_llm(table, llm=True)
```

## 📚 **References**

- **DSPy Framework**: https://github.com/stanfordnlp/dspy
- **Databricks Model Serving**: https://docs.databricks.com/machine-learning/model-serving/
- **DQX Documentation**: https://databrickslabs.github.io/dqx/

## 🎉 **Summary**

The LLM-based Primary Key Detection module provides powerful, optional capabilities for automatically identifying primary keys in your data. It's designed to be:

- **✅ Optional by Default**: No impact on regular DQX usage
- **✅ Easy to Enable**: Multiple activation methods
- **✅ Robust**: Graceful fallback and error handling
- **✅ Intelligent**: LLM-powered analysis with confidence scoring
- **✅ Integrated**: Seamlessly works with existing DQX workflows

Whether you're doing regular data quality profiling or need advanced primary key detection, this module adapts to your needs without getting in the way.
