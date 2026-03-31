---
name: dqx
description: Data quality checks with DQX (databricks-labs-dqx). Use when the user asks about data quality, quality checks, data validation, data profiling, or DQX on DataFrames or tables.
---

# DQX - Data Quality Checks

DQX applies quality rules to PySpark DataFrames and returns results as additional columns (`_errors`, `_warnings`, `_dq_info`).

## Installation

```python
%pip install databricks-labs-dqx
dbutils.library.restartPython()
```

## Quick Start

```python
import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

df = spark.table("catalog.schema.my_table")

checks = yaml.safe_load("""
- criticality: error
  check:
    function: is_not_null
    arguments:
      column: id
- criticality: warn
  check:
    function: is_in_range
    arguments:
      column: price
      min_limit: 0
      max_limit: 10000
""")

dq_engine = DQEngine(WorkspaceClient())

# Option A: single DataFrame with _errors and _warnings columns added
result_df = dq_engine.apply_checks_by_metadata(df, checks)
display(result_df)

# Option B: split into good (no errors) and bad (has errors) DataFrames
good_df, bad_df = dq_engine.apply_checks_by_metadata_and_split(df, checks)
```

## Check YAML Format

```yaml
- criticality: error|warn         # error = quarantine, warn = flag only
  name: optional_name             # auto-generated if omitted
  filter: "SQL WHERE expression"  # optional: check only matching rows
  user_metadata:                  # optional: custom key-value pairs in results
    owner: data-team
    ticket: JIRA-123
  check:
    function: function_name
    arguments:
      column: col_name            # row-level checks
      columns: [col1, col2]       # dataset-level checks (e.g. is_unique)
      # ... function-specific arguments
    for_each_column:              # shortcut: apply same check to many columns
      - col1
      - col2
```

## Output Columns

DQX adds array columns to the result DataFrame:

- `_errors` -- error-level violations (row should be quarantined)
- `_warnings` -- warning-level violations (row flagged for review)
- `_dq_info` -- additional info from dataset-level checks (e.g. anomaly detection metadata)

Each element in `_errors` / `_warnings` is a struct with these fields:

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Check name (auto-generated or user-provided) |
| `message` | string | Failure detail message |
| `columns` | array\<string\> | Columns involved in the check |
| `filter` | string | Filter expression (if the check uses one) |
| `function` | string | Check function name |
| `run_time` | timestamp | When the check was executed |
| `run_id` | string | Unique run identifier |
| `user_metadata` | map\<string, string\> | User-defined key-value pairs (from check or engine) |
| `rule_fingerprint` | string | Hash identifying this specific rule definition |
| `rule_set_fingerprint` | string | Hash identifying the full set of rules applied |
| `skipped` | boolean | True if the check was skipped (e.g. column not found) |

## for_each_column Shortcut

Apply the same check to multiple columns without repeating:

```yaml
- criticality: error
  check:
    function: is_not_null
    for_each_column:
      - id
      - name
      - email
```

For dataset-level checks with `columns` argument (like `is_unique`), use nested lists:

```yaml
- criticality: error
  check:
    function: is_unique
    for_each_column:
      - [order_id]
      - [customer_id, date]
```

## Validate Checks

```python
status = DQEngine.validate_checks(checks)
if status.has_errors:
    print(status.errors)
```

## Customizing Engine Behavior (ExtraParams)

Use `ExtraParams` to customize how the engine runs checks:

```python
from databricks.labs.dqx.config import ExtraParams
from databricks.labs.dqx.engine import DQEngine

extra = ExtraParams(
    suppress_skipped=True,                      # skip checks when columns/filters can't be resolved
    result_column_names={"errors": "my_errors", "warnings": "my_warnings"},  # custom output column names
    user_metadata={"pipeline": "nightly_etl"},  # metadata attached to all check results
)
dq_engine = DQEngine(WorkspaceClient(), extra_params=extra)
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `suppress_skipped` | `False` | When True, checks that can't resolve their columns or filter produce no entry in `_errors`/`_warnings`. When False (default), they appear with `skipped: true`. |
| `result_column_names` | `{}` | Override default column names (`errors`, `warnings`, `info` keys). |
| `user_metadata` | `{}` | Key-value pairs included in every result struct's `user_metadata` field. |
| `run_time_overwrite` | `None` | ISO timestamp string to override `run_time` in results. |
| `run_id_overwrite` | `None` | Custom run ID string to override the auto-generated `run_id`. |

## Sub-Skills Reference

| Task | Skill |
|------|-------|
| Profile data to discover quality rules | `profiling/SKILL.md` |
| Authoring checks: format, apply, split | `checks/SKILL.md` |
| Row-level check functions and examples | `checks/row-level/SKILL.md` |
| Dataset-level check functions and examples | `checks/dataset-level/SKILL.md` |
| Custom checks (SQL expressions, Python) | `checks/custom/SKILL.md` |
| Geospatial check functions and examples | `checks/geospatial/SKILL.md` |
| Runnable examples for all check types | `checks/examples/` |

## Python API Alternative

Instead of YAML dicts, you can use Python classes directly:

```python
from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule
from databricks.labs.dqx import check_funcs

checks = [
    DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="id"),
    DQDatasetRule(criticality="error", check_func=check_funcs.is_unique, columns=["id"]),
]
good_df, bad_df = dq_engine.apply_checks_and_split(df, checks)
```

YAML is recommended for readability. Full docs: https://databrickslabs.github.io/dqx/
