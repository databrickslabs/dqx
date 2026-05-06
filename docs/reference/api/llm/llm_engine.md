# databricks.labs.dqx.llm.llm\_engine

## DQLLMEngine Objects[​](#dqllmengine-objects "Direct link to DQLLMEngine Objects")

```python
class DQLLMEngine()

```

High-level interface for LLM-based data quality rule generation.

This class serves as a Facade pattern, providing a simple interface to the underlying complex LLM system.

### \_\_init\_\_[​](#__init__ "Direct link to __init__")

```python
def __init__(model_config: LLMModelConfig,
             spark: SparkSession | None = None,
             custom_check_functions: dict[str, Callable] | None = None)

```

Initialize the LLM engine.

**Arguments**:

* `model_config` - Configuration for the LLM model.
* `spark` - Optional Spark session. If None, a new session is created.
* `custom_check_functions` - Optional custom check functions to include.

### detect\_business\_rules\_with\_llm[​](#detect_business_rules_with_llm "Direct link to detect_business_rules_with_llm")

```python
def detect_business_rules_with_llm(
    user_input: str = "",
    schema_info: str = "",
    summary_stats: dict[str, Any] | None = None
) -> dspy.primitives.prediction.Prediction

```

Detect DQX rules based on natural language request with optional schema or summary statistics.

If schema\_info is empty (default), it will automatically infer the schema from the user\_input before generating rules.

**Arguments**:

* `user_input` - Optional natural language description of data quality requirements.
* `schema_info` - Optional JSON string containing table schema. If empty (default), triggers schema inference.
* `summary_stats` - Optional dictionary containing summary statistics of the input data.

**Returns**:

A Prediction object containing:

* quality\_rules: The generated DQ rules
* reasoning: Explanation of the rules
* guessed\_schema\_json: The inferred schema (if schema was inferred)
* assumptions\_bullets: Assumptions made (if schema was inferred)
* schema\_info: The final schema used (if schema was inferred)

### detect\_primary\_keys\_with\_llm[​](#detect_primary_keys_with_llm "Direct link to detect_primary_keys_with_llm")

```python
def detect_primary_keys_with_llm(table: str) -> dict[str, Any]

```

Detects primary keys using LLM-based analysis.

This method analyzes table schema and metadata to identify primary key columns.

**Arguments**:

* `table` - The table name to analyze.

**Returns**:

A dictionary containing the primary key detection result with the following keys:

* table: The table name
* success: Whether detection was successful
* primary\_key\_columns: List of detected primary key columns (if successful)
* confidence: Confidence level (high/medium/low)
* reasoning: LLM reasoning for the selection
* has\_duplicates: Whether duplicates were found (if validation performed)
* duplicate\_count: Number of duplicate combinations (if validation performed)
* error: Error message (if failed)
