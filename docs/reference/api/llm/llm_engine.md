# databricks.labs.dqx.llm.llm\_engine

## DQLLMEngine Objects[​](#dqllmengine-objects "Direct link to DQLLMEngine Objects")

```python
class DQLLMEngine()

```

High-level interface for LLM-based data quality rule generation.

This class serves as a Facade pattern, providing a simple interface to the underlying complex LLM system.

**Notes**:

For LLM-based primary key detection, which scans the data to verify uniqueness and requires a Spark session, use *DQLLMPrimaryKeyEngine* instead.

### \_\_init\_\_[​](#__init__ "Direct link to __init__")

```python
def __init__(model_config: LLMModelConfig,
             custom_check_functions: dict[str, Callable] | None = None)

```

Initializes the LLM engine.

**Arguments**:

* `model_config` - Configuration for the LLM model.
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
