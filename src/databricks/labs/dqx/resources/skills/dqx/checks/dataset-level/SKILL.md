---
name: dqx-dataset-level-checks
description: Dataset-level DQX quality check functions. These checks validate across rows (uniqueness, aggregations, foreign keys, schema, comparisons). Results are still reported per row.
---

# Dataset-Level Check Functions

Dataset-level checks analyze groups of rows or the entire DataFrame. Results are still reported per row in `_errors`/`_warnings`.

**Important:** Dataset-level checks require batch processing (not streaming).

## Uniqueness

Function: `is_unique`

```yaml
# Single column
- criticality: error
  check:
    function: is_unique
    arguments:
      columns:
        - order_id

# Composite key
- criticality: error
  check:
    function: is_unique
    arguments:
      columns:
        - customer_id
        - order_date

# Treat NULLs as equal (duplicates) -- default is NULLs are distinct
- criticality: error
  check:
    function: is_unique
    arguments:
      columns:
        - email
      nulls_distinct: false
```

## Aggregation Checks

Functions: `is_aggr_not_greater_than`, `is_aggr_not_less_than`, `is_aggr_equal`, `is_aggr_not_equal`

Supported `aggr_type` values: `count`, `sum`, `avg`, `min`, `max`, `count_distinct`, `stddev`, `variance`, `median`, `mode`, `percentile`, and more.

```yaml
# Minimum row count
- criticality: error
  check:
    function: is_aggr_not_less_than
    arguments:
      column: '*'
      aggr_type: count
      limit: 100

# Average price sanity
- criticality: warn
  check:
    function: is_aggr_not_greater_than
    arguments:
      column: price
      aggr_type: avg
      limit: 5000

# Group by: max count per category
- criticality: warn
  check:
    function: is_aggr_not_greater_than
    arguments:
      column: product_id
      aggr_type: count
      group_by:
        - category
      limit: 1000

# Row filter: count negative prices (should be 0)
- criticality: error
  check:
    function: is_aggr_equal
    arguments:
      column: price
      aggr_type: count
      limit: 0
      row_filter: "price < 0"

# Percentile check (P95 latency under 1 second)
- criticality: warn
  check:
    function: is_aggr_not_greater_than
    arguments:
      column: latency_ms
      aggr_type: percentile
      aggr_params:
        percentile: 0.95
      limit: 1000
```

## Foreign Key

Function: `foreign_key`

Validates that values exist in a reference DataFrame or table.

```yaml
# Reference a table directly
- criticality: error
  check:
    function: foreign_key
    arguments:
      columns: [country_code]
      ref_columns: [code]
      ref_table: catalog.schema.country_codes

# Reference a DataFrame (must pass ref_dfs dict in Python)
- criticality: error
  check:
    function: foreign_key
    arguments:
      columns: [product_id]
      ref_columns: [id]
      ref_df_name: products_ref

# Composite key + negated (blocklist: fail if found)
- criticality: error
  check:
    function: foreign_key
    arguments:
      columns: [email]
      ref_columns: [blocked_email]
      ref_df_name: blocklist
      negate: true
```

Passing reference DataFrames in Python:

```python
ref_dfs = {
    "products_ref": spark.table("catalog.schema.products"),
    "blocklist": spark.table("catalog.schema.blocked_emails"),
}
result_df = dq_engine.apply_checks_by_metadata(df, checks, ref_dfs=ref_dfs)
```

## Dataset Comparison

Function: `compare_datasets`

Compares source and reference DataFrames row-by-row and column-by-column.

```yaml
- criticality: error
  check:
    function: compare_datasets
    arguments:
      columns:
        - id
        - date
      ref_columns:
        - id
        - date
      ref_table: catalog.schema.reference_table
      exclude_columns:
        - last_modified
      check_missing_records: true
```

## Data Freshness

Function: `is_data_fresh_per_time_window`

Validates that at least N records arrive within every Y-minute time window.

```yaml
- criticality: error
  check:
    function: is_data_fresh_per_time_window
    arguments:
      column: event_timestamp
      window_minutes: 5
      min_records_per_window: 1
      lookback_windows: 12
```

## Schema Validation

Function: `has_valid_schema`

```yaml
# Non-strict: expected columns must exist with compatible types (extra cols OK)
- criticality: error
  check:
    function: has_valid_schema
    arguments:
      expected_schema: "id INT, name STRING, price DOUBLE"

# Strict: exact schema match required
- criticality: error
  check:
    function: has_valid_schema
    arguments:
      ref_table: catalog.schema.reference_table
      strict: true
```

## SQL Query (Custom Dataset Check)

Function: `sql_query`

Run arbitrary SQL for dataset-level validation. Must return a boolean `condition` column (True = fail).

```yaml
# Row-level validation via SQL (join results back by merge_columns)
- criticality: error
  check:
    function: sql_query
    arguments:
      query: >
        SELECT customer_id, SUM(amount) > 100000 AS condition
        FROM {{ input }}
        GROUP BY customer_id
      input_placeholder: input
      merge_columns:
        - customer_id
      condition_column: condition
      msg: "customer total amount exceeds 100k"

# Dataset-level validation (no merge -- applies to all rows)
- criticality: error
  check:
    function: sql_query
    arguments:
      query: "SELECT COUNT(*) = 0 AS condition FROM {{ input }}"
      input_placeholder: input
      condition_column: condition
      msg: "dataset is empty"
```

Full reference: https://databrickslabs.github.io/dqx/docs/reference/quality_checks/#dataset-level-checks-reference
