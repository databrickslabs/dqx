---
sidebar_label: llm_pk_detector
title: databricks.labs.dqx.llm.llm_pk_detector
---

Primary key detection using LLM analysis.

## PredictionResult Objects

```python
@dataclass
class PredictionResult()
```

Result from LLM primary key prediction.

#### to\_dict

```python
def to_dict() -> dict[str, Any]
```

Convert to dictionary format.

## ValidationResult Objects

```python
@dataclass
class ValidationResult()
```

Result from primary key validation.

## TableMetadata Objects

```python
@dataclass
class TableMetadata()
```

Metadata gathered from a table for PK detection.

## PrimaryKeyValidator Objects

```python
class PrimaryKeyValidator(Protocol)
```

Protocol for primary key validation strategies.

#### validate

```python
def validate(table: str, pk_columns: list[str], table_columns: list[str],
             result: dict[str, Any]) -> ValidationResult
```

Validate primary key candidates.

**Arguments**:

- `table` - Fully qualified table name.
- `pk_columns` - Predicted primary key columns.
- `table_columns` - All columns available in the table.
- `result` - Current detection result dictionary.
  

**Returns**:

  ValidationResult indicating if validation passed.

## ColumnExistenceValidator Objects

```python
class ColumnExistenceValidator()
```

Validates that predicted columns exist in the table.

#### validate

```python
def validate(table: str, pk_columns: list[str], table_columns: list[str],
             _result: dict[str, Any]) -> ValidationResult
```

Check if all predicted columns exist in the table.

**Arguments**:

- `table` - Fully qualified table name.
- `pk_columns` - Predicted primary key columns.
- `table_columns` - All columns available in the table.
  

**Returns**:

  ValidationResult with valid=False if columns don&#x27;t exist.

## DuplicateValidator Objects

```python
class DuplicateValidator()
```

Validates that primary key combination is unique.

#### \_\_init\_\_

```python
def __init__(table_manager: TableManager)
```

Initialize the duplicate validator.

**Arguments**:

- `table_manager` - Manager for executing SQL queries on tables.

#### validate

```python
def validate(table: str, pk_columns: list[str], _table_columns: list[str],
             result: dict[str, Any]) -> ValidationResult
```

Check if the primary key combination is unique (no duplicates).

**Arguments**:

- `table` - Fully qualified table name.
- `pk_columns` - Predicted primary key columns.
- `result` - Current detection result dictionary.
  

**Returns**:

  ValidationResult with valid=False if duplicates are found.

## ValidationChain Objects

```python
class ValidationChain()
```

Chains multiple validators together using Chain of Responsibility pattern.

#### \_\_init\_\_

```python
def __init__(validators: list[PrimaryKeyValidator])
```

Initialize the validation chain.

**Arguments**:

- `validators` - List of validators to run in sequence.

#### validate\_all

```python
def validate_all(table: str, pk_columns: list[str], table_columns: list[str],
                 result: dict[str, Any]) -> ValidationResult
```

Run all validators in sequence, stopping at first failure.

**Arguments**:

- `table` - Fully qualified table name.
- `pk_columns` - Predicted primary key columns.
- `table_columns` - All columns available in the table.
- `result` - Current detection result dictionary.
  

**Returns**:

  ValidationResult from first failing validator, or success if all pass.

## PrimaryKeyPredictor Objects

```python
class PrimaryKeyPredictor()
```

Handles LLM predictions for primary key detection.

This class is responsible solely for interacting with the LLM (DSPy)
and parsing the prediction results. It has no knowledge of validation
or retry logic.

#### \_\_init\_\_

```python
def __init__(detector: dspy.Module, show_live_reasoning: bool = True)
```

Initialize the predictor.

**Arguments**:

- `detector` - DSPy module configured for primary key detection.
- `show_live_reasoning` - Whether to display live reasoning during prediction.

#### predict

```python
def predict(table: str, table_definition: str, context: str,
            previous_attempts: str, metadata_info: str) -> PredictionResult
```

Execute a single prediction with the LLM.

**Arguments**:

- `table` - Fully qualified table name.
- `table_definition` - Complete table schema definition.
- `context` - Context about similar tables or patterns.
- `previous_attempts` - Previous failed attempts and why they failed.
- `metadata_info` - Table metadata and column statistics.
  

**Returns**:

  PredictionResult containing the predicted primary key columns.
  

**Raises**:

- `Exception` - If prediction fails.

## RetryStrategy Objects

```python
class RetryStrategy()
```

Manages retry logic and feedback generation for failed predictions.

This class encapsulates the retry policy and generates contextual feedback
for the LLM based on validation failures.

#### \_\_init\_\_

```python
def __init__(max_retries: int = 3)
```

Initialize the retry strategy.

**Arguments**:

- `max_retries` - Maximum number of retries allowed.

#### should\_retry

```python
def should_retry(attempt: int, validation_result: ValidationResult) -> bool
```

Determine if we should retry based on attempt number and validation result.

**Arguments**:

- `attempt` - Current attempt number (0-indexed).
- `validation_result` - Result from validation.
  

**Returns**:

  True if we should retry, False otherwise.

#### generate\_feedback

```python
def generate_feedback(attempt: int, pk_columns: list[str],
                      validation_result: ValidationResult,
                      previous_feedback: str) -> str
```

Generate feedback for LLM based on validation failure.

**Arguments**:

- `attempt` - Current attempt number (0-indexed).
- `pk_columns` - Columns that were predicted.
- `validation_result` - Result from validation.
- `previous_feedback` - Previous feedback string.
  

**Returns**:

  Updated feedback string with new information.

## DetectionResultBuilder Objects

```python
class DetectionResultBuilder()
```

Builds detection result dictionaries using the Builder pattern.

This class provides a fluent API for constructing consistent result
dictionaries with proper structure and all required fields.

#### \_\_init\_\_

```python
def __init__(table: str)
```

Initialize the builder for a specific table.

**Arguments**:

- `table` - Fully qualified table name.

#### with\_success

```python
def with_success(pk_columns: list[str], confidence: str,
                 reasoning: str) -> "DetectionResultBuilder"
```

Mark the detection as successful with primary key details.

**Arguments**:

- `pk_columns` - Detected primary key columns.
- `confidence` - Confidence level (high/medium/low).
- `reasoning` - LLM reasoning for the selection.
  

**Returns**:

  Self for method chaining.

#### with\_validation

```python
def with_validation(has_duplicates: bool,
                    duplicate_count: int = 0) -> "DetectionResultBuilder"
```

Add validation results to the detection result.

**Arguments**:

- `has_duplicates` - Whether duplicates were found.
- `duplicate_count` - Number of duplicate combinations found.
  

**Returns**:

  Self for method chaining.

#### with\_error

```python
def with_error(error: str) -> "DetectionResultBuilder"
```

Mark the detection as failed with an error message.

**Arguments**:

- `error` - Error message describing the failure.
  

**Returns**:

  Self for method chaining.

#### with\_status

```python
def with_status(final_status: str) -> "DetectionResultBuilder"
```

Set the final status of the detection.

**Arguments**:

- `final_status` - Status string (e.g., &#x27;success&#x27;, &#x27;max_retries_reached&#x27;, &#x27;invalid_columns&#x27;).
  

**Returns**:

  Self for method chaining.

#### add\_attempt

```python
def add_attempt(attempt_result: dict[str, Any]) -> "DetectionResultBuilder"
```

Add an attempt result to the history.

**Arguments**:

- `attempt_result` - Dictionary containing attempt details.
  

**Returns**:

  Self for method chaining.

#### build

```python
def build() -> dict[str, Any]
```

Build and return the final result dictionary.

**Returns**:

  Complete detection result dictionary.

## DetectionResultFormatter Objects

```python
class DetectionResultFormatter()
```

Formats and logs detection results for display.

This class separates presentation logic from business logic,
making it easy to test and swap formatters.

#### print\_summary

```python
@staticmethod
def print_summary(result: dict[str, Any]) -> None
```

Print a formatted summary of the detection result.

**Arguments**:

- `result` - Detection result dictionary.

#### format\_reasoning

```python
@staticmethod
def format_reasoning(reasoning: str) -> None
```

Format and print LLM reasoning step by step.

**Arguments**:

- `reasoning` - LLM reasoning text.

#### print\_trace\_if\_available

```python
@staticmethod
def print_trace_if_available() -> None
```

Print DSPy trace information if available.

## DspPrimaryKeyDetectionSignature Objects

```python
class DspPrimaryKeyDetectionSignature(dspy.Signature)
```

Analyze table schema and metadata step-by-step to identify the most likely primary key columns.

## LLMPrimaryKeyDetector Objects

```python
class LLMPrimaryKeyDetector()
```

Coordinates primary key detection using LLM analysis.

This class orchestrates the detection process by delegating to specialized
components for prediction, validation, and retry logic. It follows SOLID
principles with clear separation of concerns.

Note: This class assumes DSPy is already configured with a language model.
The configuration should be done externally before instantiating this class.

#### \_\_init\_\_

```python
def __init__(table_manager: TableManager,
             predictor: PrimaryKeyPredictor | None = None,
             validators: list[PrimaryKeyValidator] | None = None,
             retry_strategy: RetryStrategy | None = None,
             formatter: DetectionResultFormatter | None = None,
             show_live_reasoning: bool = True,
             max_retries: int = 3)
```

Initialize the primary key detector.

Note: DSPy must be configured before creating this instance.

**Arguments**:

- `table_manager` - Manager for table metadata and SQL operations.
- `predictor` - Predictor for LLM interactions (created if not provided).
- `validators` - List of validators to apply (defaults created if not provided).
- `retry_strategy` - Strategy for retry logic (created if not provided).
- `formatter` - Formatter for results (created if not provided).
- `show_live_reasoning` - Whether to display live reasoning during detection.
- `max_retries` - Maximum number of retries for detection.

#### detect\_primary\_keys\_with\_llm

```python
def detect_primary_keys_with_llm(table: str,
                                 context: str = "") -> dict[str, Any]
```

Detect primary keys for a table using LLM analysis.

**Arguments**:

- `table` - Fully qualified table name to analyze.
- `context` - Optional context about similar tables or patterns.
  

**Returns**:

  Dictionary containing detection results with the following keys:
  - table: The table name
  - success: Whether detection was successful
  - primary_key_columns: List of detected primary key columns (if successful)
  - confidence: Confidence level (high/medium/low)
  - reasoning: LLM reasoning for the selection
  - has_duplicates: Whether duplicates were found (if validation performed)
  - duplicate_count: Number of duplicate combinations (if validation performed)
  - error: Error message (if failed)
  - final_status: Final status of the detection
  - all_attempts: List of all attempts (if retries occurred)
  - retries_attempted: Number of retries (if retries occurred)

