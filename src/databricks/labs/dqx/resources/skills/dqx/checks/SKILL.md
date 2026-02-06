---
name: dqx-checks
description: How to author, apply, and manage DQX quality checks. Covers YAML format, criticality, filters, for_each_column, reference DataFrames, and splitting good/bad data.
---

# Authoring DQX Quality Checks

## Applying Checks

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
""")

dq_engine = DQEngine(WorkspaceClient())

# Single DataFrame with _errors and _warnings columns
result_df = dq_engine.apply_checks_by_metadata(df, checks)

# Or split: good_df has no errors, bad_df has at least one error
good_df, bad_df = dq_engine.apply_checks_by_metadata_and_split(df, checks)
```

## Criticality Levels

- `error` -- row goes to bad/quarantine DataFrame only
- `warn` -- row goes to both good and bad DataFrames (flagged but not quarantined)

## Filtering Checks

Apply a check only to rows matching a SQL condition:

```yaml
- criticality: error
  filter: "country = 'US'"
  check:
    function: is_not_null
    arguments:
      column: zip_code
```

Only US rows are checked; non-US rows pass through without this check.

## for_each_column

Apply the same row-level check to multiple columns:

```yaml
- criticality: error
  check:
    function: is_not_null_and_not_empty
    for_each_column:
      - first_name
      - last_name
      - email
```

For dataset-level checks (like `is_unique`), each entry is a list of columns:

```yaml
- criticality: error
  check:
    function: is_unique
    for_each_column:
      - [order_id]
      - [customer_id, order_date]
```

## Reference DataFrames

Some dataset-level checks (`foreign_key`, `compare_datasets`, `sql_query`, `has_valid_schema`) need reference data. Pass them via `ref_dfs`:

```python
ref_dfs = {
    "countries": spark.table("catalog.schema.country_codes"),
    "products": spark.table("catalog.schema.product_catalog"),
}

result_df = dq_engine.apply_checks_by_metadata(df, checks, ref_dfs=ref_dfs)
```

In YAML, reference the key name:

```yaml
- criticality: error
  check:
    function: foreign_key
    arguments:
      columns: [country_code]
      ref_columns: [code]
      ref_df_name: countries
```

Or reference a table directly (no ref_dfs needed):

```yaml
- criticality: error
  check:
    function: foreign_key
    arguments:
      columns: [country_code]
      ref_columns: [code]
      ref_table: catalog.schema.country_codes
```

## Extracting Results

```python
# Get only invalid rows (have _errors or _warnings)
invalid_df = dq_engine.get_invalid(result_df)

# Get only valid rows (no _errors and no _warnings)
valid_df = dq_engine.get_valid(result_df)
```

## Check Types

- **Row-level**: validate individual values -- see `row-level/SKILL.md`
- **Dataset-level**: validate across rows (uniqueness, aggregations, FK) -- see `dataset-level/SKILL.md`
- **Custom**: SQL expressions or Python functions -- see `custom/SKILL.md`

You can mix row-level and dataset-level checks in the same list.

## Runnable Examples

The `examples/` directory contains runnable scripts for every check category:

| Examples | Files |
|----------|-------|
| Row-level (null, list, comparison, range, regex, datetime, network, SQL, complex) | `examples/01_row_null_empty.py` .. `examples/09_row_complex.py` |
| Dataset-level (unique, aggregation, FK, compare, freshness, schema, SQL query) | `examples/10_dataset_unique.py` .. `examples/16_dataset_sql_query.py` |
| Custom checks (SQL window, Python row, Python dataset) | `examples/17_custom_sql_window.py` .. `examples/19_custom_python_dataset.py` |
| Profiling | `examples/20_profiling_dataframe.py` .. `examples/21_profiling_specific_columns.py` |
