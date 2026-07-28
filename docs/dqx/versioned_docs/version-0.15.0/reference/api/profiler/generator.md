---
sidebar_label: generator
title: databricks.labs.dqx.profiler.generator
---

## DQGenerator Objects

```python
class DQGenerator(DQEngineBase)
```

#### \_\_init\_\_

```python
def __init__(workspace_client: WorkspaceClient,
             spark: SparkSession | None = None,
             llm_model_config: LLMModelConfig | None = None,
             custom_check_functions: dict[str, Callable] | None = None)
```

Initializes the DQGenerator with optional Spark session and LLM model configuration.

**Arguments**:

- `workspace_client` - Databricks WorkspaceClient instance.
- `spark` - Optional SparkSession instance. If not provided, a new session will be created.
- `llm_model_config` - Optional LLM model configuration for AI-assisted rule generation.
- `custom_check_functions` - Optional dictionary of custom check functions.

#### generate\_dq\_rules

```python
@telemetry_logger("generator", "generate_dq_rules")
def generate_dq_rules(profiles: list[DQProfile] | None = None,
                      criticality: str = "error") -> list[dict]
```

Generates a list of data quality rules based on the provided dq profiles.

**Arguments**:

- `profiles` - A list of data quality profiles to generate rules for.
- `criticality` - The criticality of the rules as &quot;warn&quot; or &quot;error&quot; (default is &quot;error&quot;).
  

**Returns**:

  A list of dictionaries representing the data quality rules.

#### generate\_dq\_rules\_ai\_assisted

```python
@telemetry_logger("generator", "generate_dq_rules_ai_assisted")
def generate_dq_rules_ai_assisted(
        user_input: str = "",
        summary_stats: dict | None = None,
        input_config: InputConfig | None = None) -> list[dict]
```

Generates data quality rules using LLM based on natural language input.

**Arguments**:

- `user_input` - Optional Natural language description of data quality requirements.
- `summary_stats` - Optional summary statistics of the input data.
- `input_config` - Optional input config providing input data location as a path or fully qualified table name
  to infer schema. If not provided, LLM will be used to guess the table schema.
  

**Returns**:

  A list of dictionaries representing the generated data quality rules. Rules that fail
  *DQEngine.validate_checks* (e.g. missing required arguments, unknown function names)
  are dropped and logged at *WARNING*; only valid rules are returned.
  

**Raises**:

- `MissingParameterError` - If DSPy compiler is not available.

#### generate\_rules\_from\_contract

```python
@telemetry_logger("generator", "generate_rules_from_contract")
def generate_rules_from_contract(
        contract: "DataContract | None" = None,
        contract_file: str | None = None,
        contract_format: str = "odcs",
        generate_predefined_rules: bool = True,
        process_text_rules: bool = True,
        generate_schema_validation: bool = True,
        strict_schema_validation: bool = True,
        default_criticality: str = "error") -> list[dict]
```

Generate DQX quality rules from a data contract specification.

Parses a data contract (ODCS v3.x; any apiVersion accepted by the library, e.g. v3.0.2, v3.1.0) and generates rules based on
schema properties, explicit quality definitions, and text-based expectations.
When the contract defines a schema and generate_schema_validation is True, one dataset-level
has_valid_schema rule per schema is generated. strict_schema_validation is passed to the check.

**Arguments**:

- `contract` - Pre-loaded DataContract object from datacontract-cli. Can be created with:
  - DataContract(data_contract_file=path) - from a file path
  - DataContract(data_contract_str=yaml_string) - from a YAML/JSON string
  Either `contract` or `contract_file` must be provided.
- `contract_file` - Path to contract YAML file (local, volume, or workspace).
- `contract_format` - Contract format specification (default is &quot;odcs&quot;).
- `generate_predefined_rules` - Whether to generate rules from schema properties.
- `process_text_rules` - Whether to process text-based expectations using LLM.
- `generate_schema_validation` - Whether to generate dataset-level has_valid_schema rules (default True).
- `strict_schema_validation` - Passed as strict to has_valid_schema (default True = exact match; False = permissive).
- `default_criticality` - Default criticality for generated rules as &quot;warn&quot; or &quot;error&quot; (default is &quot;error&quot;).
  

**Returns**:

  A list of dictionaries representing the generated DQX quality rules.
  

**Raises**:

- `contract`0 - If datacontract-cli is not installed.
- `contract`1 - If neither or both parameters are provided, or format not supported.
  

**Notes**:

  Exactly one of &#x27;contract&#x27; or &#x27;contract_file&#x27; must be provided.

#### dq\_generate\_is\_in

```python
@staticmethod
def dq_generate_is_in(column: str, criticality: str = "error", **params: dict)
```

Generates a data quality rule to check if a column&#x27;s value is in a specified list.

**Arguments**:

- `column` - The name of the column to check.
- `criticality` - The criticality of the rule as &quot;warn&quot; or &quot;error&quot;  (default is &quot;error&quot;).
- `params` - Additional parameters, including the list of values to check against.
  

**Returns**:

  A dictionary representing the data quality rule.

#### dq\_generate\_min\_max

```python
@staticmethod
def dq_generate_min_max(column: str,
                        criticality: str = "error",
                        **params: dict)
```

Generates a data quality rule to check if a column&#x27;s value is within a specified range.

**Arguments**:

- `column` - The name of the column to check.
- `criticality` - The criticality of the rule as &quot;warn&quot; or &quot;error&quot;  (default is &quot;error&quot;).
- `params` - Additional parameters, including the minimum and maximum values.
  

**Returns**:

  A dictionary representing the data quality rule, or None if no limits are provided.

#### dq\_generate\_is\_not\_null

```python
@staticmethod
def dq_generate_is_not_null(column: str,
                            criticality: str = "error",
                            **params: dict)
```

Generates a data quality rule to check if a column&#x27;s value is not null.

**Arguments**:

- `column` - The name of the column to check.
- `criticality` - The criticality of the rule as &quot;warn&quot; or &quot;error&quot;  (default is &quot;error&quot;).
- `params` - Additional parameters.
  

**Returns**:

  A dictionary representing the data quality rule.

#### dq\_generate\_is\_not\_null\_or\_empty

```python
@staticmethod
def dq_generate_is_not_null_or_empty(column: str,
                                     criticality: str = "error",
                                     **params: dict)
```

Generates a data quality rule to check if a column&#x27;s value is not null or empty.

**Arguments**:

- `column` - The name of the column to check.
- `criticality` - The criticality of the rule as &quot;warn&quot; or &quot;error&quot;  (default is &quot;error&quot;).
- `params` - Additional parameters, including whether to trim strings.
  

**Returns**:

  A dictionary representing the data quality rule.

#### dq\_generate\_is\_not\_empty

```python
@staticmethod
def dq_generate_is_not_empty(column: str,
                             criticality: str = "error",
                             **params: dict)
```

Generates a data quality rule to check if a column&#x27;s value is not empty (nulls are allowed).

**Arguments**:

- `column` - The name of the column to check.
- `criticality` - The criticality of the rule as &quot;warn&quot; or &quot;error&quot; (default is &quot;error&quot;).
- `params` - Additional parameters, including whether to trim strings.
  

**Returns**:

  A dictionary representing the data quality rule.

#### dq\_generate\_is\_unique

```python
@staticmethod
def dq_generate_is_unique(column: str,
                          criticality: str = "error",
                          **params: dict)
```

Generates a data quality rule to check if specified columns are unique.

Uses is_unique with nulls_distinct=True for uniqueness validation.

**Arguments**:

- `column` - Comma-separated list of column names that form the primary key. Uses all columns if not provided.
- `criticality` - The criticality of the rule as &quot;warn&quot; or &quot;error&quot; (default is &quot;error&quot;).
- `params` - Additional parameters including columns list, confidence, reasoning, etc.
  

**Returns**:

  A dictionary representing the data quality rule.

