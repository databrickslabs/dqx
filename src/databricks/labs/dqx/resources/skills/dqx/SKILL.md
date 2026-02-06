---
name: dqx
description: Data quality checks with DQX (databricks-labs-dqx). Use when the user asks about data quality, quality checks, data validation, data profiling, or DQX on DataFrames or tables.
---

# DQX - Data Quality Checks

DQX applies quality rules to PySpark DataFrames and returns results as additional columns (`_errors`, `_warnings`).

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

DQX adds two array columns:

- `_errors` -- error-level violations (row should be quarantined)
- `_warnings` -- warning-level violations (row flagged for review)

Each element is a struct with `name` (check name) and `message` (failure detail).

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

## Sub-Skills Reference

| Task | Skill |
|------|-------|
| Profile data to discover quality rules | `profiling/SKILL.md` |
| Authoring checks: format, apply, split | `checks/SKILL.md` |
| Row-level check functions and examples | `checks/row-level/SKILL.md` |
| Dataset-level check functions and examples | `checks/dataset-level/SKILL.md` |
| Custom checks (SQL expressions, Python) | `checks/custom/SKILL.md` |

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
