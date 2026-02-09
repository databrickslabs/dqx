---
name: dqx-dataset-level-checks
description: Dataset-level DQX quality check functions. These checks validate across rows (uniqueness, aggregations, foreign keys, schema, comparisons). Results are still reported per row.
---

# Dataset-Level Check Functions

Dataset-level checks analyze groups of rows or the entire DataFrame. Results are still reported per row in `_errors`/`_warnings`.

**Important:** Dataset-level checks require batch processing (not streaming).

## Parameter Reference

**IMPORTANT:** Parameters listed as **required** MUST be provided -- omitting them causes a runtime error.

### Uniqueness

| Function | Required | Optional |
|----------|----------|----------|
| `is_unique` | `columns` (list) | `nulls_distinct` (default: true), `row_filter` |

### Aggregation

| Function | Required | Optional |
|----------|----------|----------|
| `is_aggr_not_greater_than` | `column`, **`limit`** | `aggr_type` (default: "count"), `group_by`, `row_filter`, `aggr_params` |
| `is_aggr_not_less_than` | `column`, **`limit`** | `aggr_type` (default: "count"), `group_by`, `row_filter`, `aggr_params` |
| `is_aggr_equal` | `column`, **`limit`** | `aggr_type` (default: "count"), `group_by`, `row_filter`, `aggr_params`, `abs_tolerance`, `rel_tolerance` |
| `is_aggr_not_equal` | `column`, **`limit`** | `aggr_type` (default: "count"), `group_by`, `row_filter`, `aggr_params`, `abs_tolerance`, `rel_tolerance` |

**`limit` is always required** for aggregation checks. It is the threshold to compare the aggregate against.

Supported `aggr_type` values: `count`, `sum`, `avg`, `min`, `max`, `count_distinct`, `stddev`, `variance`, `median`, `mode`, `percentile`, and more.

For `percentile`/`approx_percentile`, `aggr_params` with `percentile` key is **required**: `aggr_params: {percentile: 0.95}`.

### Foreign Key

| Function | Required | Optional |
|----------|----------|----------|
| `foreign_key` | `columns` (list), `ref_columns` (list), + one of `ref_df_name` or `ref_table` | `negate` (default: false), `row_filter` |

Either `ref_df_name` or `ref_table` must be provided (not both).

### Dataset Comparison

| Function | Required | Optional |
|----------|----------|----------|
| `compare_datasets` | `columns` (list), `ref_columns` (list), + one of `ref_df_name` or `ref_table` | `check_missing_records` (default: false), `exclude_columns`, `null_safe_row_matching` (default: true), `null_safe_column_value_matching` (default: true), `row_filter`, `abs_tolerance`, `rel_tolerance` |

### Data Freshness

| Function | Required | Optional |
|----------|----------|----------|
| `is_data_fresh_per_time_window` | `column`, **`window_minutes`**, **`min_records_per_window`** | `lookback_windows`, `row_filter` |

### Schema Validation

| Function | Required | Optional |
|----------|----------|----------|
| `has_valid_schema` | one of `expected_schema`, `ref_df_name`, or `ref_table` | `columns`, `strict` (default: false), `exclude_columns` |

### Outlier Detection

| Function | Required | Optional |
|----------|----------|----------|
| `has_no_outliers` | `column` | `row_filter` |

### SQL Query

| Function | Required | Optional |
|----------|----------|----------|
| `sql_query` | **`query`** | `merge_columns`, `msg`, `name`, `negate` (default: false), `condition_column` (default: "condition"), `input_placeholder` (default: "input_view"), `row_filter` |

---

## Examples

### Uniqueness

```yaml
# Single column
- criticality: error
  check:
    function: is_unique
    arguments:
      columns:                       # REQUIRED (list)
        - order_id

# Composite key
- criticality: error
  check:
    function: is_unique
    arguments:
      columns:                       # REQUIRED (list)
        - customer_id
        - order_date

# Treat NULLs as equal (duplicates) -- default is NULLs are distinct
- criticality: error
  check:
    function: is_unique
    arguments:
      columns:                       # REQUIRED (list)
        - email
      nulls_distinct: false
```

### Aggregation Checks

```yaml
# Minimum row count -- limit is REQUIRED
- criticality: error
  check:
    function: is_aggr_not_less_than
    arguments:
      column: '*'
      aggr_type: count
      limit: 100                     # REQUIRED

# Average price sanity -- limit is REQUIRED
- criticality: warn
  check:
    function: is_aggr_not_greater_than
    arguments:
      column: price
      aggr_type: avg
      limit: 5000                    # REQUIRED

# Group by: max count per category -- limit is REQUIRED
- criticality: warn
  check:
    function: is_aggr_not_greater_than
    arguments:
      column: product_id
      aggr_type: count
      group_by:
        - category
      limit: 1000                    # REQUIRED

# Row filter: count negative prices (should be 0) -- limit is REQUIRED
- criticality: error
  check:
    function: is_aggr_equal
    arguments:
      column: price
      aggr_type: count
      limit: 0                       # REQUIRED
      row_filter: "price < 0"

# Percentile check (P95 latency under 1 second)
# aggr_params with 'percentile' key is REQUIRED for percentile aggr_type
- criticality: warn
  check:
    function: is_aggr_not_greater_than
    arguments:
      column: latency_ms
      aggr_type: percentile
      aggr_params:
        percentile: 0.95             # REQUIRED for percentile aggr_type
      limit: 1000                    # REQUIRED
```

### Foreign Key

```yaml
# Reference a table directly -- ref_table is REQUIRED (or ref_df_name)
- criticality: error
  check:
    function: foreign_key
    arguments:
      columns: [country_code]        # REQUIRED (list)
      ref_columns: [code]            # REQUIRED (list)
      ref_table: catalog.schema.country_codes

# Reference a DataFrame (must pass ref_dfs dict in Python)
- criticality: error
  check:
    function: foreign_key
    arguments:
      columns: [product_id]          # REQUIRED (list)
      ref_columns: [id]              # REQUIRED (list)
      ref_df_name: products_ref

# Composite key + negated (blocklist: fail if found)
- criticality: error
  check:
    function: foreign_key
    arguments:
      columns: [email]               # REQUIRED (list)
      ref_columns: [blocked_email]   # REQUIRED (list)
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

### Dataset Comparison

```yaml
- criticality: error
  check:
    function: compare_datasets
    arguments:
      columns:                       # REQUIRED (list)
        - id
        - date
      ref_columns:                   # REQUIRED (list)
        - id
        - date
      ref_table: catalog.schema.reference_table
      exclude_columns:
        - last_modified
      check_missing_records: true
```

### Data Freshness

```yaml
- criticality: error
  check:
    function: is_data_fresh_per_time_window
    arguments:
      column: event_timestamp              # REQUIRED
      window_minutes: 5                    # REQUIRED
      min_records_per_window: 1            # REQUIRED
      lookback_windows: 12
```

### Schema Validation

```yaml
# Non-strict: expected columns must exist with compatible types (extra cols OK)
- criticality: error
  check:
    function: has_valid_schema
    arguments:
      expected_schema: "id INT, name STRING, price DOUBLE"  # REQUIRED (or ref_table/ref_df_name)

# Strict: exact schema match required
- criticality: error
  check:
    function: has_valid_schema
    arguments:
      ref_table: catalog.schema.reference_table  # REQUIRED (or expected_schema/ref_df_name)
      strict: true
```

### SQL Query (Custom Dataset Check)

Run arbitrary SQL for dataset-level validation. Must return a boolean `condition` column (True = fail).

```yaml
# Row-level validation via SQL (join results back by merge_columns)
- criticality: error
  check:
    function: sql_query
    arguments:
      query: >                               # REQUIRED
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
      query: "SELECT COUNT(*) = 0 AS condition FROM {{ input }}"  # REQUIRED
      input_placeholder: input
      condition_column: condition
      msg: "dataset is empty"
```

## Runnable Examples

| Example | File |
|---------|------|
| Uniqueness checks | `examples/10_dataset_unique.py` |
| Aggregation checks | `examples/11_dataset_aggr.py` |
| Foreign key validation | `examples/12_dataset_fk.py` |
| Dataset comparison | `examples/13_dataset_compare.py` |
| Data freshness | `examples/14_dataset_freshness.py` |
| Schema validation | `examples/15_dataset_schema.py` |
| SQL query (custom dataset) | `examples/16_dataset_sql_query.py` |

Full reference: https://databrickslabs.github.io/dqx/docs/reference/quality_checks/#dataset-level-checks-reference
