# Quality Checks Definition

Checks are the core of DQX, allowing you to define and apply data quality rules to your datasets.

There are several ways to define quality checks in DQX:

* YAML or JSON file (declarative approach), suitable when applying checks programmatically and using DQX workflows.
* Delta table (most scalable), suitable when applying checks programmatically and using DQX workflows.
* Programmatically as a list of dictionaries or DQX objects, suitable when applying checks programmatically.

Checks can be saved and loaded from various storage systems as described [here](/dqx/docs/guide/quality_checks_storage.md). The loaded checks can be applied to the data using methods as described [here](/dqx/docs/guide/quality_checks_apply.md).

DQX provides a collection of predefined built-in quality rules (checks). The following is a quick guide on defining quality checks using YAML, JSON, Delta tables, or programmatically in code. For a complete list of check functions and detailed examples, see the full reference [here](/dqx/docs/reference/quality_checks.md).

The quality checks can be defined for simple and complex column types such as structs, maps and arrays. Additionally, you can define custom checks using sql or python to meet specific requirements.

## Code format (programmatic approach)[​](#code-format-programmatic-approach "Direct link to Code format (programmatic approach)")

You can define quality checks programmatically using a list of DQX classes (list of `DQRule` instances) or list of dictionaries (declaratively).

### Checks defined with DQX classes[​](#checks-defined-with-dqx-classes "Direct link to Checks defined with DQX classes")

Checks can be defined using DQX classes such as `DQRowRule`, `DQDatasetRule`, and `DQForEachColRule`. This approach provides static type checking and autocompletion in IDEs, making it potentially easier to work with.

Note

The validation of arguments and keyword arguments for the check function is automatically performed upon creating a `DQRowRule`.

* Python

```python
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule, DQForEachColRule

checks = [
  DQRowRule(  # check for a single column
    name="col3_is_null_or_empty",
    criticality="warn",
    check_func=check_funcs.is_not_null_and_not_empty,
    column="col3",
  ),

  *DQForEachColRule(  # apply the same checks to multiple columns
    columns=["col1", "col2"],
    criticality="error",
    check_func=check_funcs.is_not_null).get_rules(),

  DQRowRule(  # check with a filter
    name="col_4_is_null_or_empty",
    criticality="warn",
    filter="col1 < 3",
    check_func=check_funcs.is_not_null_and_not_empty,
    column="col4",
  ),

  DQRowRule(  # check with user metadata
    name="col_5_is_null_or_empty",
    criticality="warn",
    check_func=check_funcs.is_not_null_and_not_empty,
    column="col5",
    user_metadata={
      "check_type": "completeness",
      "responsible_data_steward": "someone@email.com"
    },
  ),

  DQRowRule(  # provide check func arguments using positional arguments
    criticality="warn",
    check_func=check_funcs.is_in_list,
    column="col1",
    check_func_args=[[1, 2]],
  ),

  DQRowRule(  # provide check func arguments using keyword arguments
    criticality="warn",
    check_func=check_funcs.is_in_list,
    column="col2",
    check_func_kwargs={"allowed": [1, 2]},
  ),

  DQRowRule(  # check for a struct field
    # use "error" criticality if not provided
    check_func=check_funcs.is_not_null,
    column="col7.field1",
  ),

  DQRowRule(  # check for a map element
    criticality="error",
    check_func=check_funcs.is_not_null,
    column=F.try_element_at("col5", F.lit("key1")),
  ),

  DQRowRule(  # check for an array element
    criticality="error",
    check_func=check_funcs.is_not_null,
    column=F.try_element_at("col6", F.lit(1)),
  ),

  DQDatasetRule(  # check uniqueness of composite key
    criticality="error",
    check_func=check_funcs.is_unique,
    columns=["col1", "col2"]
  ),

  DQDatasetRule(  # dataset check working across group of rows
    criticality="error",
    check_func=check_funcs.is_aggr_not_greater_than,
    column="col1",
    check_func_kwargs={"aggr_type": "count", "group_by": ["col2"], "limit": 10},
  ),

  DQDatasetRule(  # dataset check working across group of rows
    criticality="error",
    check_func=check_funcs.is_aggr_not_less_than,
    column="col1",
    check_func_kwargs={"aggr_type": "avg", "group_by": ["col2"], "limit": 1.2},
  ),

  DQDatasetRule(  # dataset check working across group of rows
    criticality="error",
    check_func=check_funcs.is_aggr_equal,
    column="col1",
    check_func_kwargs={"aggr_type": "count", "group_by": ["col2"], "limit": 5},
  ),

  DQDatasetRule(  # dataset check working across group of rows
    criticality="error",
    check_func=check_funcs.is_aggr_not_equal,
    column="col1",
    check_func_kwargs={"aggr_type": "avg", "group_by": ["col2"], "limit": 10.5},
  ),

  DQDatasetRule(  # dataset check for distinct value count for groups (each group should have 1 value)
    criticality="error",
    check_func=check_funcs.is_aggr_not_greater_than,
    column="country_code",
    check_func_kwargs={
        "aggr_type": "count_distinct",  # Exact distinct count
        "group_by": ["country"],
        "limit": 1
    },
  ),

  DQDatasetRule(  # dataset check for standard deviation for groups
    criticality="warn",
    check_func=check_funcs.is_aggr_not_greater_than,
    column="temperature",
    check_func_kwargs={
        "aggr_type": "stddev",
        "group_by": ["machine_id"],
        "limit": 5.0
    },
  ),

  DQDatasetRule(  # dataset check for percentile with the percentile value passed using aggr_params
    criticality="error",
    check_func=check_funcs.is_aggr_not_greater_than,
    column="latency_ms",
    check_func_kwargs={
        "aggr_type": "percentile",
        "aggr_params": {"percentile": 0.95},
        "limit": 1000
    },
  ),
]

```

### Checks defined using metadata (list of dictionaries)[​](#checks-defined-using-metadata-list-of-dictionaries "Direct link to Checks defined using metadata (list of dictionaries)")

Checks can be defined using declarative syntax as a list of dictionaries. In the below example we create the list of dictionaries using YAML definition for readability, but you can also define a list of dictionaries directly in python.

```python
  # load list of dict from YAML definition
  checks: list[dict] = yaml.safe_load("""
    # check for a single column
    - criticality: warn
      check:
        function: is_not_null_and_not_empty
        arguments:
          column: col3

    # apply the same checks to multiple columns
    - criticality: error
      check:
        function: is_not_null
        for_each_column:
        - col1
        - col2

    # check with a filter
    - criticality: warn
      filter: col1 < 3
      check:
        function: is_not_null_and_not_empty
        arguments:
          column: col4

    # check with user metadata
    - criticality: warn
      check:
        function: is_not_null_and_not_empty
        arguments:
          column: col5
      user_metadata:
        check_category: completeness
        responsible_data_steward: someone@email.com

    # check with auto-generated name
    - criticality: warn
      check:
        function: is_in_list
        arguments:
          column: col1
          allowed:
            - 1
            - 2

    # check for a struct field
    - check:
        function: is_not_null
        arguments:
          column: col7.field1
      # "error" criticality used if not provided

    # check for a map element
    - criticality: error
      check:
        function: is_not_null
        arguments:
          column: try_element_at(col5, 'key1')

    # check for an array element
    - criticality: error
      check:
        function: is_not_null
        arguments:
          column: try_element_at(col6, 1)

    # check uniqueness of composite key
    - criticality: error
      check:
        function: is_unique
        arguments:
          columns:
          - col1
          - col2

    # dataset checks working across group of rows
    - criticality: error
      filter: col2 < 3  # pushed down as row_filter to the check function
      check:
        function: is_aggr_not_greater_than
        arguments:
          column: col1
          aggr_type: count
          group_by:
          - col2
          limit: 10

    - criticality: error
      check:
        function: is_aggr_not_less_than
        arguments:
          column: col1
          aggr_type: avg
          group_by:
          - col2
          limit: 1.2
          
    - criticality: error
      check:
        function: is_aggr_equal
        arguments:
          column: col1
          aggr_type: count
          group_by:
          - col2
          limit: 5
          
    - criticality: error
      check:
        function: is_aggr_not_equal
        arguments:
          column: col1
          aggr_type: avg
          group_by:
          - col2
          limit: 10.5

    - criticality: error
      check:
        function: is_aggr_not_greater_than
        arguments:
          column: country_code
          aggr_type: count_distinct
          group_by:
          - country
          limit: 1
    
    - criticality: warn
      check:
        function: is_aggr_not_greater_than
        arguments:
          column: temperature
          aggr_type: stddev
          group_by:
          - machine_id
          limit: 5.0
    
    - criticality: error
      check:
        function: is_aggr_not_greater_than
        arguments:
          column: latency_ms
          aggr_type: percentile
          aggr_params:
            percentile: 0.95
          limit: 1000
    """)

```

Checks defined using declarative syntax must contain the following fields:

* `criticality`: either "error" (data going only into "bad/quarantine" dataframe) or "warn" (data going into both "good" and "bad" dataframes). If not provided, the default is "error".

* `check` column expression containing:

  <!-- -->

  * `function`: check function name to apply.
  * `arguments`: keyword arguments passed to the check function. Every parameter without a default in the function’s Python signature must appear here.
  * `for_each_column`: (optional) list of column names or expressions for which the same check should be applied. When saving with `DQEngine.save_checks` to Delta or Lakebase tables, DQX stores such checks in compact format (one row per check, `for_each_column` preserved).

* (optional) `name`: name of the check: autogenerated if not provided.

* (optional) `filter`: spark expression to filter the rows for which the check is applied (e.g. `"business_unit = 'Finance'"`). The check function will run only on the rows matching the filter condition. The condition can reference any column of the validated dataset, not only the one where you apply the check function. When using dataset-level checks, the filter condition is pushed down as `row_filter` to the check function and applied before aggregation, ensuring that the check operates only on the relevant subset of rows rather than on the aggregated results.

* (optional) `user_metadata`: key-value pairs added to the row-level warnings and errors

## YAML format (declarative approach)[​](#yaml-format-declarative-approach "Direct link to YAML format (declarative approach)")

Checks can be defined in a YAML file using declarative syntax.

Checks can be saved and loaded from a YAML file using methods described [here](/dqx/docs/guide/quality_checks_storage.md). Checks defined in YAML files are supported by the DQX workflows.

Example `yaml` file defining several checks:

```yaml
# check for a single column
- criticality: warn
  check:
    function: is_not_null_and_not_empty
    arguments:
      column: col3

# apply the same checks to multiple columns
- criticality: error
  check:
    function: is_not_null
    for_each_column:
    - col1
    - col2

# check with a filter
- criticality: warn
  filter: col1 < 3
  check:
    function: is_not_null_and_not_empty
    arguments:
      column: col4

# check with user metadata
- criticality: warn
  check:
    function: is_not_null_and_not_empty
    arguments:
      column: col5
  user_metadata:
    check_category: completeness
    responsible_data_steward: someone@email.com

# check with auto-generated name
- criticality: warn
  check:
    function: is_in_list
    arguments:
      column: col1
      allowed:
        - 1
        - 2

# check for a struct field
- check:
    function: is_not_null
    arguments:
      column: col7.field1
  # "error" criticality used if not provided

# check for a map element
- criticality: error
  check:
    function: is_not_null
    arguments:
      column: try_element_at(col5, 'key1')

# check for an array element
- criticality: error
  check:
    function: is_not_null
    arguments:
      column: try_element_at(col6, 1)

# check uniqueness of composite key
- criticality: error
  check:
    function: is_unique
    arguments:
      columns:
      - col1
      - col2

# dataset check working across group of rows
- criticality: error
  check:
    function: is_aggr_not_greater_than
    arguments:
      column: col1
      aggr_type: count
      group_by:
      - col2
      limit: 10

- criticality: error
  check:
    function: is_aggr_not_less_than
    arguments:
      column: col1
      aggr_type: avg
      group_by:
      - col2
      limit: 1.2
      
- criticality: error
  check:
    function: is_aggr_equal
    arguments:
      column: col1
      aggr_type: count
      group_by:
      - col2
      limit: 5
      
- criticality: error
  check:
    function: is_aggr_not_equal
    arguments:
      column: col1
      aggr_type: avg
      group_by:
      - col2
      limit: 10.5

- criticality: error
  check:
    function: is_aggr_not_greater_than
    arguments:
      column: country_code
      aggr_type: count_distinct
      group_by: 
      - country
      limit: 1

- criticality: warn
  check:
    function: is_aggr_not_greater_than
    arguments:
      column: temperature
      aggr_type: stddev
      group_by: 
      - machine_id
      limit: 5.0

- criticality: error
  check:
    function: is_aggr_not_greater_than
    arguments:
      column: latency_ms
      aggr_type: percentile
      aggr_params:
        percentile: 0.95
      limit: 1000

```

## JSON format (declarative approach)[​](#json-format-declarative-approach "Direct link to JSON format (declarative approach)")

Checks can be defined in a JSON file using declarative syntax. The structure of the JSON file defining quality checks is the same as the YAML format, but uses JSON syntax.

Checks can be saved and loaded from a JSON file using methods described [here](/dqx/docs/guide/quality_checks_storage.md). Checks defined in JSON files are supported by the DQX workflows.

```json
[
  {
    "criticality": "warn",
    "check": {
      "function": "is_not_null_and_not_empty",
      "arguments": {
        "column": "col3"
      }
    }
  },
  {
    "criticality": "error",
    "check": {
      "function": "is_not_null",
      "for_each_column": ["col1", "col2"]
    }
  },
  {
    "criticality": "warn",
    "filter": "col1 < 3",
    "check": {
      "function": "is_not_null_and_not_empty",
      "arguments": {
        "column": "col4"
      }
    }
  },
  {
    "criticality": "warn",
    "check": {
      "function": "is_not_null_and_not_empty",
      "arguments": {
        "column": "col5"
      }
    },
    "user_metadata": {
      "check_category": "completeness",
      "responsible_data_steward": "someone@email.com"
    }
  },
  {
    "criticality": "warn",
    "check": {
      "function": "is_in_list",
      "arguments": {
        "column": "col1",
        "allowed": [1, 2]
      }
    }
  },
  {
    "check": {
      "function": "is_not_null",
      "arguments": {
        "column": "col7.field1"
      }
    }
  },
  {
    "criticality": "error",
    "check": {
      "function": "is_not_null",
      "arguments": {
        "column": "try_element_at(col5, 'key1')"
      }
    }
  },
  {
    "criticality": "error",
    "check": {
      "function": "is_not_null",
      "arguments": {
        "column": "try_element_at(col6, 1)"
      }
    }
  },
  {
    "criticality": "error",
    "check": {
      "function": "is_unique",
      "arguments": {
        "columns": ["col1", "col2"]
      }
    }
  },
  {
    "criticality": "error",
    "check": {
      "function": "is_aggr_not_greater_than",
      "arguments": {
        "column": "col1",
        "aggr_type": "count",
        "group_by": ["col2"],
        "limit": 10
      }
    }
  },
  {
    "criticality": "error",
    "check": {
      "function": "is_aggr_not_less_than",
      "arguments": {
        "column": "col1",
        "aggr_type": "avg",
        "group_by": ["col2"],
        "limit": 1.2
      }
    }
  },
  {
    "criticality": "error",
    "check": {
      "function": "is_aggr_equal",
      "arguments": {
        "column": "col1",
        "aggr_type": "count",
        "group_by": ["col2"],
        "limit": 5
      }
    }
  },
  {
    "criticality": "error",
    "check": {
      "function": "is_aggr_not_equal",
      "arguments": {
        "column": "col1",
        "aggr_type": "avg",
        "group_by": ["col2"],
        "limit": 10.5
      }
    }
  },
  {
    "criticality": "error",
    "check": {
      "function": "is_aggr_not_greater_than",
      "arguments": {
        "column": "country_code",
        "aggr_type": "count_distinct",
        "group_by": ["country"],
        "limit": 1
      }
    }
  },
  {
    "criticality": "warn",
    "check": {
      "function": "is_aggr_not_greater_than",
      "arguments": {
        "column": "temperature",
        "aggr_type": "stddev",
        "group_by": ["machine_id"],
        "limit": 5.0
      }
    }
  },
  {
    "criticality": "error",
    "check": {
      "function": "is_aggr_not_greater_than",
      "arguments": {
        "column": "latency_ms",
        "aggr_type": "percentile",
        "aggr_params": {
          "percentile": 0.95
        },
        "limit": 1000
      }
    }
  }
]

```

## Delta and Lakebase table format (declarative approach)[​](#delta-and-lakebase-table-format-declarative-approach "Direct link to Delta and Lakebase table format (declarative approach)")

Checks can be defined in a Delta table or Lakebase table where each row represents a check with columns: `name`, `check`, `criticality`, `filter`, and `run_config_name` fields. Both backends use the same logical structure and compact format for `for_each_column`.

The most convenient way to programmatically load and save checks is to use the `load_checks` and `save_checks` methods from the `DQEngine`. See more details [here](/dqx/docs/guide/quality_checks_storage.md). Checks defined in Delta and Lakebase tables are supported by the DQX workflows.

The table used to store checks has the following structure. You can use `for_each_column` in your checks; when saving with `DQEngine.save_checks` to Delta or Lakebase tables, DQX stores such checks in compact format (one row per check). See [quality checks storage](/dqx/docs/guide/quality_checks_storage.md) for details.

| Column                 | Type                  | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| ---------------------- | --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`                 | `string`              | Name to use for the check. Optional.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `criticality`          | `string`              | Either `"error"` (rows go only to "bad" dataset) or `"warn"` (rows go to both "good" and "bad"). Defaults to `"error"`.                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `check`                | `struct`              | Defines the DQX check to apply.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| └─ `function`          | `string`              | Name of the DQX check function to apply.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| └─ `for_each_column`   | `array<string>`       | (Optional) List of columns for which the same check applies. Stored in compact format (one row per check). Manual tables can use compact or expanded (individual rules instead of for\_each\_column) format.                                                                                                                                                                                                                                                                                                                                                               |
| └─ `arguments`         | `map<string, string>` | Keyword arguments for the check function. All parameters without a default must be present (or supplied via `for_each_column` for `column` / `columns`).                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `filter`               | `string`              | (Optional) Spark SQL expression to filter rows to which the check is applied, e.g. `"business_unit = 'Finance'"`. The check function will run only on the rows matching the filter condition. The condition can reference any column of the validated dataset, not only the one where you apply the check function. When using dataset-level checks, the filter condition is pushed down as `row_filter` to the check function and applied before aggregation, ensuring that the check operates only on the relevant subset of rows rather than on the aggregated results. |
| `run_config_name`      | `string`              | Name of the run config name. Could be any string such as input table or job name (use "default" if not provided). Useful for selecting applicable checks.                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `user_metadata`        | `map<string, string>` | (Optional) Custom metadata to add to any row-level warnings or errors generated by the check.                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `created_at`           | `timestamp`           | Current local date and time in UTC when the rule set was saved.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `rule_fingerprint`     | `string`              | Hex-encoded SHA-256 hash string of a single rule definition, allowing checks in the results to be tracked and versioned.                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `rule_set_fingerprint` | `string`              | Hex-encoded SHA-256 hash string of the complete rule set. This will make it easier to track rules, knowing which rules belong to the set.                                                                                                                                                                                                                                                                                                                                                                                                                                  |

Example checks saved in a Delta or Lakebase table (compact format — `for_each_column` preserved when input used it):

```python
+------------------------+-------------+------------------------------------------------------------------------------------------------------------------------+----------+-----------------+-------------------------------------------+-----------------------------------+------------------------------------------------------------------+-------------------------------------------------------------------+
| name                   | criticality | check                                                                                                                  | filter   | run_config_name | user_metadata                             | created_at                        | rule_fingerprint                                                 | rule_set_fingerprint                                              |
+------------------------+-------------+------------------------------------------------------------------------------------------------------------------------+----------+-----------------+-------------------------------------------+-----------------------------------+------------------------------------------------------------------+-------------------------------------------------------------------+
| null                   | error       | {function: "is_not_null", for_each_column: ["col1", "col2"], arguments: {}}                                              | col1 > 0 | "default"       | {"check_owner": "someone@email.com"}      | 2026-03-15 20:29:55.821214+00:00  | cb74c780a689bff2d84fa3cc1ca33f53bb84e7ce967d4a57871cea8167cbb1e4 | 9664332437da274d921cefac60bd509e0aa383292ba695341e1f3fbc2a716e48  |
| column_not_less_than   | warn        | {function: "is_not_less_than", arguments: {column: "col_2", limit: "1"}}                                                 | null     | "default"       | null                                      | 2026-03-15 20:31:21.240723+00:00  | bf0af992f49f1bf46e9b0c454ee033a2840306437602525a2f66fdf2bd725b98 | 9664332437da274d921cefac60bd509e0aa383292ba695341e1f3fbc2a716e48  |
| column_in_list         | warn        | {function: "is_in_list", arguments: {column: "col_2", allowed: "[1, 2]"}}                                                | null     | "default"       | null                                      | 2026-03-15 20:31:46.928271+00:00  | a11ccfc2f7c52171ca51422744db3e8add3cda427eef1f5cae209c1100379e57 | 9664332437da274d921cefac60bd509e0aa383292ba695341e1f3fbc2a716e48  |
+------------------------+-------------+------------------------------------------------------------------------------------------------------------------------+----------+-----------------+-------------------------------------------+-----------------------------------+------------------------------------------------------------------+-------------------------------------------------------------------+

```

If `run_config_name` is not provided, "default" is used. Typically, the input table or job name is used for run config name to establish a one-to-one mapping between tables or jobs and checks.

## Variable Substitution[​](#variable-substitution "Direct link to Variable Substitution")

DQX supports variable substitution in declarative check definitions (YAML, JSON, or Delta tables). This allows you to parameterize your quality rules and inject values at **load time** or **save time** from engine-level defaults and/or via the `variables` parameter in `load_checks` or `save_checks`.

### Syntax and Scope[​](#syntax-and-scope "Direct link to Syntax and Scope")

Placeholders are defined using the `{{ variable_name }}` syntax. Variable substitution is supported in **all string values** within the check definitions, including:

* `name`
* `filter`
* `check` function arguments (`arguments`) and column names (`for_each_column`)
* any other top-level or nested string field

Do not use variable substitution for criticality

The `criticality` field only accepts fixed values (`error` or `warn`). Do not use variable placeholders for `criticality` — the resolved value must be a valid criticality and substituting it defeats the purpose of having an explicit severity level in the check definition.

### Resolution[​](#resolution "Direct link to Resolution")

Variables are resolved when checks are loaded or saved via the engine. To resolve variables, pass a dictionary to the `variables` parameter of `load_checks` or `save_checks`. User can decide whether to provide variables when loading or saving checks.

Resolving variables at save time

When using `save_checks` with variables, placeholders are resolved **before** computing rule fingerprints and persisting. This ensures that stored checks and their fingerprints reflect the actual resolved check logic. Without resolving at save time, fingerprints would be computed on unresolved `{{ }}` placeholders, causing a mismatch between the fingerprints stored in the checks table and those recorded in the summary metrics and per-row detailed results tables.

Note

Variable substitution is only available when defining checks declaratively (as dictionaries or in files/tables). It is not supported when using DQX classes (e.g., `DQRowRule`) directly.

```python
import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import FileChecksStorageConfig, TableChecksStorageConfig
from databricks.sdk import WorkspaceClient

dq_engine = DQEngine(WorkspaceClient())

# Define checks with variable placeholders
checks = yaml.safe_load("""
    - criticality: error
      check:
        function: is_in_range
        arguments:
          column: temperature
          min_limit: "{{ min_temp }}"
          max_limit: "{{ max_temp }}"
      filter: "region = '{{ region }}'"
""")

variables = {
    "min_temp": 0,
    "max_temp": 100,
    "region": "EMEA",
}

# Load checks from file with variable resolution
resolved_checks = dq_engine.load_checks(
    config=FileChecksStorageConfig(location="checks.yml"),
    variables=variables,
)

# Or resolve variables when saving checks (ensures fingerprints are consistent)
dq_engine.save_checks(
    checks=checks,
    config=TableChecksStorageConfig(location="catalog.schema.checks_table"),
    variables=variables,
)

```

## Default Variables[​](#default-variables "Direct link to Default Variables")

In addition to specifying variables during the load or save process, you can define engine-level defaults using the `ExtraParams` class. These constants are automatically applied to all checks unless explicitly overridden.

For technical details and configuration examples, see [Default Variables](/dqx/docs/guide/additional_configuration.md#defining-default-variables-for-substitution) in the Additional Configuration guide.

## Validating syntax of quality checks[​](#validating-syntax-of-quality-checks "Direct link to Validating syntax of quality checks")

You can validate the syntax of checks loaded from a storage system or checks defined programmatically before applying them. This validation ensures that the checks are correctly defined and can be interpreted by the DQX engine.

The validation cannot be used for checks defined programmatically using DQX classes. When checks are defined programmatically with DQX classes, syntax validation is unnecessary because the application will fail to interpret them if the DQX objects are constructed incorrectly.

Usage tips

Validating quality rules are typically done as part of the CI/CD process to ensure checks are ready to use in the application.

* Python
* CLI

```python
import yaml
from databricks.labs.dqx.engine import DQEngine

checks = yaml.safe_load("""
- criticality: error
  check:
    function: is_not_null
    for_each_column:
    - col1
    - col2
""")

status = DQEngine.validate_checks(checks)
print(status)

```

For checks defined in storage, you can use the Databricks CLI to validate them. This requires the DQX to be installed in the workspace as a tool. Checks defined as workspace or Volume file are supported.

Execute the following command to validate the checks:

```commandline
# run for all run configs in the configuration file
databricks labs dqx validate-checks

# run for a specific run config in the configuration file
databricks labs dqx validate-checks --run-config "default"

```

The following DQX field from the run config in [configuration file](/dqx/docs/installation.md#configuration-file) are used:

* 'checks\_location': file or table location of the quality checks
