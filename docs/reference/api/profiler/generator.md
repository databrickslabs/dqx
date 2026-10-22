# databricks.labs.dqx.profiler.generator

## DQGenerator Objects[​](#dqgenerator-objects "Direct link to DQGenerator Objects")

```python
class DQGenerator(DQEngineBase)

```

### \_\_init\_\_[​](#__init__ "Direct link to __init__")

```python
def __init__(workspace_client: WorkspaceClient,
             spark: SparkSession | None = None,
             llm_model_config: LLMModelConfig | None = None,
             custom_check_functions: dict[str, Callable] | None = None)

```

Initializes the DQGenerator with optional Spark session and LLM model configuration.

**Arguments**:

* `workspace_client` - Databricks WorkspaceClient instance.
* `spark` - Optional SparkSession instance. Required to infer the schema of inputs given as a storage path; Unity Catalog tables are read through the workspace client. If needed and not provided, a new session will be created on first use.
* `llm_model_config` - Optional LLM model configuration for AI-assisted rule generation.
* `custom_check_functions` - Optional dictionary of custom check functions.

### spark[​](#spark "Direct link to spark")

```python
@property
def spark() -> SparkSession

```

Gets a Spark session. Gets an available one or creates a new one if none was provided.

**Returns**:

Spark session instance.

### generate_dq_rules[​](#generate_dq_rules "Direct link to generate_dq_rules")

```python
@telemetry_logger("generator", "generate_dq_rules")
def generate_dq_rules(profiles: list[DQProfile] | None = None,
                      criticality: str = "error") -> list[dict]

```

Generates a list of data quality rules based on the provided dq profiles.

**Arguments**:

* `profiles` - A list of data quality profiles to generate rules for.
* `criticality` - The criticality of the rules as "warn" or "error" (default is "error").

**Returns**:

A list of dictionaries representing the data quality rules.

### generate_dq_rules_ai_assisted[​](#generate_dq_rules_ai_assisted "Direct link to generate_dq_rules_ai_assisted")

```python
@telemetry_logger("generator", "generate_dq_rules_ai_assisted")
def generate_dq_rules_ai_assisted(
        user_input: str = "",
        summary_stats: dict | None = None,
        input_config: InputConfig | None = None) -> list[dict]

```

Generates data quality rules using LLM based on natural language input.

**Arguments**:

* `user_input` - Optional Natural language description of data quality requirements.
* `summary_stats` - Optional summary statistics of the input data.
* `input_config` - Optional input config providing input data location as a path or fully qualified table name to infer schema. If not provided, LLM will be used to guess the table schema.

**Returns**:

A list of dictionaries representing the generated data quality rules. Rules that fail *DQEngine.validate_checks* (e.g. missing required arguments, unknown function names) are dropped and logged at *WARNING*; only valid rules are returned.

**Raises**:

* `MissingParameterError` - If DSPy compiler is not available.

**Notes**:

A Spark session is required when using this method with an *input_config* that is not a Unity Catalog table (e.g. files or temporary views). Schemas can be read without a Spark Session for tables with a valid 3-level name (e.g. catalog.schema.table).

### generate_rules_from_contract[​](#generate_rules_from_contract "Direct link to generate_rules_from_contract")

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
        default_criticality: str = "error",
        process_library_rules: bool = True) -> list[dict]

```

Generate DQX quality rules from a data contract specification.

Parses a data contract (ODCS v3.x; any apiVersion accepted by the library, e.g. v3.0.2, v3.1.0) and generates rules based on schema properties, explicit quality definitions, and text-based expectations. When the contract defines a schema and generate_schema_validation is True, one dataset-level has_valid_schema rule per schema is generated. strict_schema_validation is passed to the check.

**Arguments**:

* `contract` - Pre-loaded DataContract object from datacontract-cli. Can be created with:

  <!-- -->

  * DataContract(data_contract_file=path) - from a file path
  * DataContract(data_contract_str=yaml_string) - from a YAML/JSON string Either `contract` or `contract_file` must be provided.

* `contract_file` - Path to contract YAML file (local, volume, or workspace).

* `contract_format` - Contract format specification (default is "odcs").

* `generate_predefined_rules` - Whether to generate rules from schema properties.

* `process_text_rules` - Whether to process text-based expectations using LLM.

* `generate_schema_validation` - Whether to generate dataset-level has_valid_schema rules (default True).

* `strict_schema_validation` - Passed as strict to has_valid_schema (default True = exact match; False = permissive).

* `default_criticality` - Default criticality for generated rules as "warn" or "error" (default is "error").

* `contract`0 - Whether to generate rules from ODCS library quality metric entries (default True).

**Returns**:

A list of dictionaries representing the generated DQX quality rules.

**Raises**:

* `contract`1 - If datacontract-cli is not installed.
* `contract`2 - If neither or both parameters are provided, or format not supported.

**Notes**:

Exactly one of 'contract' or 'contract_file' must be provided.

### dq_generate_is_in[​](#dq_generate_is_in "Direct link to dq_generate_is_in")

```python
@staticmethod
def dq_generate_is_in(column: str, criticality: str = "error", **params: dict)

```

Generates a data quality rule to check if a column's value is in a specified list.

**Arguments**:

* `column` - The name of the column to check.
* `criticality` - The criticality of the rule as "warn" or "error" (default is "error").
* `params` - Additional parameters, including the list of values to check against.

**Returns**:

A dictionary representing the data quality rule.

### dq_generate_min_max[​](#dq_generate_min_max "Direct link to dq_generate_min_max")

```python
@staticmethod
def dq_generate_min_max(column: str,
                        criticality: str = "error",
                        **params: dict)

```

Generates a data quality rule to check if a column's value is within a specified range.

**Arguments**:

* `column` - The name of the column to check.
* `criticality` - The criticality of the rule as "warn" or "error" (default is "error").
* `params` - Additional parameters, including the minimum and maximum values.

**Returns**:

A dictionary representing the data quality rule, or None if no limits are provided.

### dq_generate_is_not_null[​](#dq_generate_is_not_null "Direct link to dq_generate_is_not_null")

```python
@staticmethod
def dq_generate_is_not_null(column: str,
                            criticality: str = "error",
                            **params: dict)

```

Generates a data quality rule to check if a column's value is not null.

**Arguments**:

* `column` - The name of the column to check.
* `criticality` - The criticality of the rule as "warn" or "error" (default is "error").
* `params` - Additional parameters.

**Returns**:

A dictionary representing the data quality rule.

### dq_generate_is_not_null_or_empty[​](#dq_generate_is_not_null_or_empty "Direct link to dq_generate_is_not_null_or_empty")

```python
@staticmethod
def dq_generate_is_not_null_or_empty(column: str,
                                     criticality: str = "error",
                                     **params: dict)

```

Generates a data quality rule to check if a column's value is not null or empty.

**Arguments**:

* `column` - The name of the column to check.
* `criticality` - The criticality of the rule as "warn" or "error" (default is "error").
* `params` - Additional parameters, including whether to trim strings.

**Returns**:

A dictionary representing the data quality rule.

### dq_generate_is_not_empty[​](#dq_generate_is_not_empty "Direct link to dq_generate_is_not_empty")

```python
@staticmethod
def dq_generate_is_not_empty(column: str,
                             criticality: str = "error",
                             **params: dict)

```

Generates a data quality rule to check if a column's value is not empty (nulls are allowed).

**Arguments**:

* `column` - The name of the column to check.
* `criticality` - The criticality of the rule as "warn" or "error" (default is "error").
* `params` - Additional parameters, including whether to trim strings.

**Returns**:

A dictionary representing the data quality rule.

### dq_generate_is_unique[​](#dq_generate_is_unique "Direct link to dq_generate_is_unique")

```python
@staticmethod
def dq_generate_is_unique(column: str,
                          criticality: str = "error",
                          **params: dict)

```

Generates a data quality rule to check if specified columns are unique.

Uses is_unique with nulls_distinct=True for uniqueness validation.

**Arguments**:

* `column` - Comma-separated list of column names that form the primary key. Uses all columns if not provided.
* `criticality` - The criticality of the rule as "warn" or "error" (default is "error").
* `params` - Additional parameters including columns list, confidence, reasoning, etc.

**Returns**:

A dictionary representing the data quality rule.

### dq_generate_has_no_outliers[​](#dq_generate_has_no_outliers "Direct link to dq_generate_has_no_outliers")

```python
@staticmethod
def dq_generate_has_no_outliers(column: str,
                                criticality: str = "error",
                                **params: dict)

```

Generates a data quality rule to check if a column's values contain no outliers.

Uses the MAD (Median Absolute Deviation) method via the *has_no_outliers* check function. Values outside median ± 3.5 \* MAD are flagged as outliers.

**Arguments**:

* `column` - The name of the column to check.
* `criticality` - The criticality of the rule as "warn" or "error" (default is "error").
* `params` - Additional parameters (unused; left for compatibility with other functions).

**Returns**:

A dictionary representing the data quality rule.

### dq_generate_geometry_type[​](#dq_generate_geometry_type "Direct link to dq_generate_geometry_type")

```python
@staticmethod
def dq_generate_geometry_type(column: str,
                              criticality: str = "error",
                              **params: object) -> dict | None

```

Generates a data quality rule to check that a geometry column holds a single geometry type.

**Arguments**:

* `column` - The name of the column to check.
* `criticality` - The criticality of the rule as "warn" or "error" (default is "error").
* `params` - Additional parameters, including the profiled ST\_ geometry *type*.

**Returns**:

A dictionary representing the data quality rule, or None if the type is not recognized.

### dq_generate_has_x_coordinate_between[​](#dq_generate_has_x_coordinate_between "Direct link to dq_generate_has_x_coordinate_between")

```python
@staticmethod
def dq_generate_has_x_coordinate_between(column: str,
                                         criticality: str = "error",
                                         **params: object) -> dict

```

Generates a data quality rule to check that geometry column's x coordinates (a.k.a. longitude) fall within a specific range.

**Arguments**:

* `column` - The name of the column to check.
* `criticality` - The criticality of the rule as "warn" or "error" (default is "error").
* `params` - Additional parameters, including the *min_value* and *max_value* bounds.

**Returns**:

A dictionary representing the data quality rule.

### dq_generate_has_y_coordinate_between[​](#dq_generate_has_y_coordinate_between "Direct link to dq_generate_has_y_coordinate_between")

```python
@staticmethod
def dq_generate_has_y_coordinate_between(column: str,
                                         criticality: str = "error",
                                         **params: object) -> dict

```

Generates a data quality rule to check that geometry column's y coordinates (a.k.a. latitude) fall within a specific range.

**Arguments**:

* `column` - The name of the column to check.
* `criticality` - The criticality of the rule as "warn" or "error" (default is "error").
* `params` - Additional parameters, including the *min_value* and *max_value* bounds.

**Returns**:

A dictionary representing the data quality rule.

### dq_generate_is_area_not_less_than[​](#dq_generate_is_area_not_less_than "Direct link to dq_generate_is_area_not_less_than")

```python
@staticmethod
def dq_generate_is_area_not_less_than(column: str,
                                      criticality: str = "error",
                                      **params: object) -> dict

```

Generates a data quality rule to check that geometry column's areas are not less than a specified limit.

**Arguments**:

* `column` - The name of the column to check.
* `criticality` - The criticality of the rule as "warn" or "error" (default is "error").
* `params` - Additional parameters, including the *value* limit, optional *srid* (geometry columns), and optional *geodesic* flag (geography columns).

**Returns**:

A dictionary representing the data quality rule.

### dq_generate_is_area_not_greater_than[​](#dq_generate_is_area_not_greater_than "Direct link to dq_generate_is_area_not_greater_than")

```python
@staticmethod
def dq_generate_is_area_not_greater_than(column: str,
                                         criticality: str = "error",
                                         **params: object) -> dict

```

Generates a data quality rule to check that geometry column's areas are not greater than a specified limit.

**Arguments**:

* `column` - The name of the column to check.
* `criticality` - The criticality of the rule as "warn" or "error" (default is "error").
* `params` - Additional parameters, including the *value* limit, optional *srid* (geometry columns), and optional *geodesic* flag (geography columns).

**Returns**:

A dictionary representing the data quality rule.

### dq_generate_is_num_points_not_less_than[​](#dq_generate_is_num_points_not_less_than "Direct link to dq_generate_is_num_points_not_less_than")

```python
@staticmethod
def dq_generate_is_num_points_not_less_than(column: str,
                                            criticality: str = "error",
                                            **params: object) -> dict

```

Generates a data quality rule to check that geometry column's coordinate counts are not less than a specified limit.

**Arguments**:

* `column` - The name of the column to check.
* `criticality` - The criticality of the rule as "warn" or "error" (default is "error").
* `params` - Additional parameters, including the *value* limit.

**Returns**:

A dictionary representing the data quality rule.

### dq_generate_is_num_points_not_greater_than[​](#dq_generate_is_num_points_not_greater_than "Direct link to dq_generate_is_num_points_not_greater_than")

```python
@staticmethod
def dq_generate_is_num_points_not_greater_than(column: str,
                                               criticality: str = "error",
                                               **params: object) -> dict

```

Generates a data quality rule to check that geometry column's coordinate counts are not greater than a specified limit.

**Arguments**:

* `column` - The name of the column to check.
* `criticality` - The criticality of the rule as "warn" or "error" (default is "error").
* `params` - Additional parameters, including the *value* limit.

**Returns**:

A dictionary representing the data quality rule.

### dq_generate_is_non_empty_geometry[​](#dq_generate_is_non_empty_geometry "Direct link to dq_generate_is_non_empty_geometry")

```python
@staticmethod
def dq_generate_is_non_empty_geometry(column: str,
                                      criticality: str = "error",
                                      **params: object) -> dict

```

Generates a data quality rule to check that geometries are not empty.

**Arguments**:

* `column` - The name of the column to check.
* `criticality` - The criticality of the rule as "warn" or "error" (default is "error").
* `params` - Additional parameters (unused; consumed for a uniform dispatch signature).

**Returns**:

A dictionary representing the data quality rule.

### dq_generate_is_ogc_valid[​](#dq_generate_is_ogc_valid "Direct link to dq_generate_is_ogc_valid")

```python
@staticmethod
def dq_generate_is_ogc_valid(column: str,
                             criticality: str = "error",
                             **params: object) -> dict

```

Generates a data quality rule to check that geometries are valid in the OGC sense.

**Arguments**:

* `column` - The name of the column to check.
* `criticality` - The criticality of the rule as "warn" or "error" (default is "error").
* `params` - Additional parameters (unused; consumed for a uniform dispatch signature).

**Returns**:

A dictionary representing the data quality rule.

### dq_generate_is_not_null_island[​](#dq_generate_is_not_null_island "Direct link to dq_generate_is_not_null_island")

```python
@staticmethod
def dq_generate_is_not_null_island(column: str,
                                   criticality: str = "error",
                                   **params: object) -> dict

```

Generates a data quality rule to check that geometries are not null islands (POINT(0 0)).

**Arguments**:

* `column` - The name of the column to check.
* `criticality` - The criticality of the rule as "warn" or "error" (default is "error").
* `params` - Additional parameters (unused; consumed for a uniform dispatch signature).

**Returns**:

A dictionary representing the data quality rule.
