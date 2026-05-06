# databricks.labs.dqx.llm.llm\_utils

### get\_check\_function\_definitions[​](#get_check_function_definitions "Direct link to get_check_function_definitions")

```python
def get_check_function_definitions(
    custom_check_functions: dict[str, Callable] | None = None
) -> list[dict[str, str]]

```

A utility function to get the definition of all check functions. This function is primarily used to generate a prompt for the LLM to generate check functions.

If provided, the function will use the custom check functions to resolve the check function. If not provided, the function will use only the built-in check functions.

**Arguments**:

* `custom_check_functions` - A dictionary of custom check functions.

**Returns**:

* `list[dict]` - A list of dictionaries, each containing the definition of a check function.

### get\_required\_check\_functions\_definitions[​](#get_required_check_functions_definitions "Direct link to get_required_check_functions_definitions")

```python
def get_required_check_functions_definitions(
    custom_check_functions: dict[str, Callable] | None = None
) -> list[dict[str, str]]

```

Extract only required function information (name and doc).

**Returns**:

list\[dict\[str, str]]: A list of dictionaries containing the required fields for each check function.

### get\_required\_summary\_stats[​](#get_required_summary_stats "Direct link to get_required_summary_stats")

```python
def get_required_summary_stats(
        summary_stats: dict[str, Any]) -> dict[str, Any]

```

Filters the summary statistics to include only mean, min, and max values, which provide sufficient information for LLM-based rule generation while reducing token usage. Converts all values to JSON-serializable format.

**Arguments**:

* `summary_stats` - Dictionary containing summary statistics for each column.

**Returns**:

* `dict` - A dictionary containing the required fields for each summary stats with JSON-serializable values.

### create\_optimizer\_training\_set[​](#create_optimizer_training_set "Direct link to create_optimizer_training_set")

```python
def create_optimizer_training_set(
    custom_check_functions: dict[str, Callable] | None = None
) -> list[dspy.Example]

```

Get quality check training examples for the dspy optimizer.

**Arguments**:

* `custom_check_functions` - A dictionary of custom check functions.

**Returns**:

* `list[dspy.Example]` - A list of dspy.Example objects created from training examples.

### create\_optimizer\_training\_set\_with\_stats[​](#create_optimizer_training_set_with_stats "Direct link to create_optimizer_training_set_with_stats")

```python
def create_optimizer_training_set_with_stats(
    custom_check_functions: dict[str, Callable] | None = None
) -> list[dspy.Example]

```

Get quality check training examples using data summary statistics for the dspy optimizer.

**Arguments**:

* `custom_check_functions` - A dictionary of custom check functions.

**Returns**:

* `list[dspy.Example]` - A list of dspy.Example objects created from training examples with stats.

### get\_column\_metadata[​](#get_column_metadata "Direct link to get_column_metadata")

```python
def get_column_metadata(spark: SparkSession, input_config: InputConfig) -> str

```

Get the column metadata for a given table.

**Arguments**:

* `input_config` *InputConfig* - Input configuration for the table.
* `spark` *SparkSession* - The Spark session used to access the table.

**Returns**:

* `str` - A JSON string containing the column metadata with columns wrapped in a "columns" key.
