# databricks.labs.dqx.llm.llm\_pk\_engine

## DQLLMPrimaryKeyEngine Objects[​](#dqllmprimarykeyengine-objects "Direct link to DQLLMPrimaryKeyEngine Objects")

```python
class DQLLMPrimaryKeyEngine()

```

High-level interface for LLM-based primary key detection.

Primary key detection inspects table metadata and scans the data to verify uniqueness, so it requires a Spark session. It is kept separate from *DQLLMEngine*, which generates quality rules from table metadata alone and needs no Spark session.

### \_\_init\_\_[​](#__init__ "Direct link to __init__")

```python
def __init__(model_config: LLMModelConfig,
             spark: SparkSession | None = None,
             detector: LLMPrimaryKeyDetector | None = None)

```

Initializes the primary key detection engine.

**Arguments**:

* `model_config` - Configuration for the LLM model.
* `spark` - Optional Spark session. If not provided, a new session will be created on first use.
* `detector` - Optional primary key detector. If None, one is created on first use using *spark*.

### spark[​](#spark "Direct link to spark")

```python
@property
def spark() -> SparkSession

```

Gets a Spark session. Gets an available one or creates a new one if none was provided.

**Returns**:

Spark session instance.

### detector[​](#detector "Direct link to detector")

```python
@property
def detector() -> LLMPrimaryKeyDetector

```

Gets the primary key detector, creating one on first use if none was provided.

Resolved lazily so that constructing the engine does not require a Spark session before any detection is actually requested.

**Returns**:

Primary key detector instance.

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
