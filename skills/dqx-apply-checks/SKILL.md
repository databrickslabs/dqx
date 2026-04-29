---
name: dqx-apply-checks
description: >
  Validate a PySpark DataFrame or Delta table against a set of DQX quality rules using
  DQEngine. Use when the user asks to "run data quality checks", "apply DQX rules to a
  DataFrame/table", "split valid and invalid rows", "quarantine bad records", or
  "integrate DQX into a streaming pipeline". Covers apply_checks, apply_checks_and_split,
  the by_metadata variants, and the shape of the result columns.
---

# DQX — Apply quality checks

Apply checks with `DQEngine`. There are six method families — pick by **(1) rule form** and **(2) what output you want**.

| Rule form                                                                      | Append results as columns  | Split valid / invalid                 | End-to-end (read + check + write)            |
|--------------------------------------------------------------------------------|----------------------------|---------------------------------------|----------------------------------------------|
| Classes (`DQRowRule`, …)                                                       | `apply_checks`             | `apply_checks_and_split`              | `apply_checks_and_save_in_table`             |
| Metadata (`list[dict]`, loadable from YAML / JSON / Delta — see `dqx-storage`) | `apply_checks_by_metadata` | `apply_checks_by_metadata_and_split`  | `apply_checks_by_metadata_and_save_in_table` |

For the end-to-end methods see the `dqx-end-to-end` skill. Streaming uses the same methods as batch.

## Examples

### Using DQX Classes

```python
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule
from databricks.sdk import WorkspaceClient
# `spark` is available in Databricks notebooks / jobs. Locally, create it with
# `from pyspark.sql import SparkSession; spark = SparkSession.builder.getOrCreate()`.

dq = DQEngine(WorkspaceClient())

checks = [
    DQRowRule(criticality="warn",  check_func=check_funcs.is_not_null,   column="col3"),
    DQDatasetRule(criticality="error", check_func=check_funcs.is_unique, columns=["col1", "col2"]),
]

df = spark.read.table("catalog.schema.input")

# Option A — one output DataFrame with _warnings / _errors columns appended.
annotated = dq.apply_checks(df, checks)

# Option B — split on error severity. Warnings ride along with valid_df.
valid_df, invalid_df = dq.apply_checks_and_split(df, checks)
```

### Using Metadata (loaded from storage)

```python
dq = DQEngine(WorkspaceClient())

checks = """
- criticality: warn
  check:
    function: is_not_null
    arguments:
      column: col3

- criticality: error
  check:
    function: is_unique
    arguments:
      columns: [col1, col2]
"""

# Option A — one output DataFrame with _warnings / _errors columns appended.
annotated = dq.apply_checks_by_metadata(df, checks_metadata)

# Option B — split on error severity. Warnings ride along with valid_df.
valid_df, invalid_df = dq.apply_checks_by_metadata_and_split(df, checks_metadata)
```

## Result shape

By default two struct columns are appended:

- **`_errors`** — an array of result structs, one per failing error-criticality rule.
- **`_warnings`** — same, for warn-criticality rules.

Each result struct contains: `name`, `message`, `columns`, `filter`, `function`, `run_time`, `run_id`, `user_metadata`, `rule_fingerprint`, `rule_set_fingerprint`, and (for rules that couldn't be evaluated) `skipped=true` with a reason.

Column names and the skipped-entry behavior are configurable — see [Additional Configuration](https://databrickslabs.github.io/dqx/docs/guide/additional_configuration) for `ExtraParams`, `result_column_names`, and `suppress_skipped`.

## Streaming

Use `spark.readStream` / `writeStream` as normal; the same `apply_checks*` calls work on streaming DataFrames. For incremental batch-style execution out of the box, use `apply_checks_and_save_in_table` with the default `AvailableNow` trigger (see `dqx-end-to-end`).

## Multiple inputs

- `apply_checks_and_save_in_tables(run_configs)` — one call processes many `(input, output)` pairs defined by `RunConfig` entries.
- `apply_checks_and_save_in_tables_for_patterns(patterns=["catalog.schema.*"], checks_location="catalog.schema.checks", run_config_template=RunConfig(...))` — expand wildcards against Unity Catalog. `patterns` is a `list[str]`; `checks_location` is required and is used as a template for per-table file lookups or as the shared Delta table. See [Applying Checks](https://databrickslabs.github.io/dqx/docs/guide/quality_checks_apply) for the full signature.

## Do / Don't

- **Do** fail fast: `DQEngine.validate_checks(checks)` raises on bad definitions before you touch data.
- **Do** persist the split result (`invalid_df`) to a quarantine table — not just the annotated DataFrame — so downstream consumers see only good rows.
- **Don't** mix DQX classes and metadata in one call; pick one form per invocation.
- **Don't** drop the `_errors` / `_warnings` columns unless you've materialised the result elsewhere — they're how downstream tooling sees what failed.

Canonical docs: <https://databrickslabs.github.io/dqx/docs/guide/quality_checks_apply>.
