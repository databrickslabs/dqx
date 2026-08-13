# databricks.labs.dqx.llm.llm\_utils

### extract\_json\_rules[​](#extract_json_rules "Direct link to extract_json_rules")

```python
def extract_json_rules(raw: str) -> object

```

Extract the DQX rules JSON value from an LLM rules response, tolerating surrounding noise.

Smaller serving models frequently wrap the rules array in a markdown code fence, prepend a prose preamble, or append a trailing explanation after the closing bracket. A strict *json.loads* rejects all of these (for example with *Extra data* when text trails a valid array) and discards otherwise-valid rules.

The DQX rules payload is always a JSON array of rule objects, so this helper strips a code fence and returns the first array (via *\_first\_rules\_array*) that decodes to a list containing at least one dict. An array that mixes valid rule dicts with malformed entries is still returned (dropping the junk is *\_filter\_unsafe\_sql\_rules*' job, not the extractor's). An empty `[]` is accepted as a fallback; genuinely truncated or non-array output yields no qualifying list and raises, so broken responses are not silently salvaged into a meaningless fragment.

**Arguments**:

* `raw` - The raw *quality\_rules* string returned by the model.

**Returns**:

The decoded rules list.

**Raises**:

* `json.JSONDecodeError` - if no JSON array can be located.

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
