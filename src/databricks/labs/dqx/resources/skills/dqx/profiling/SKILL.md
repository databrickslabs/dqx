---
name: dqx-profiling
description: Profile data to discover quality rules using DQX DQProfiler or manual SQL. Use when the user wants to understand data characteristics or auto-generate quality checks.
---

# Data Profiling with DQX

## Automated Profiling with DQProfiler

DQProfiler analyzes a DataFrame or table and generates candidate quality rules (null checks, range checks, allowed-value lists) based on data statistics.

### Profile a DataFrame

```python
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.profiler.profiler import DQProfiler

df = spark.table("catalog.schema.my_table")
profiler = DQProfiler(WorkspaceClient())

summary_stats, profiles = profiler.profile(df)
```

### Profile a Table Directly

```python
summary_stats, profiles = profiler.profile_table("catalog.schema.my_table")
```

### Profile Multiple Tables by Pattern

```python
results = profiler.profile_tables_for_patterns(
    patterns=["catalog.schema.*"],
    exclude_patterns=["catalog.schema.tmp_*"],
)
# results is dict: {table_name: (summary_stats, profiles)}
```

### Understanding Results

`summary_stats` is a dict of per-column statistics:

```python
# {'col_name': {'count': 1000, 'count_non_null': 995, 'count_null': 5,
#               'min': 0.5, 'max': 99.9, 'mean': 45.2, 'stddev': 12.3}, ...}
```

`profiles` is a list of `DQProfile` objects (candidate rules):

```python
for p in profiles:
    print(f"{p.column}: {p.name} {p.parameters or ''}")
# id: is_not_null
# status: is_in {'allowed': ['active', 'inactive', 'pending']}
# price: min_max {'min': 0.5, 'max': 99.9}
```

### Profiler Options

```python
summary_stats, profiles = profiler.profile(df, options={
    "sample_fraction": 0.3,    # sample 30% of data (default)
    "limit": 1000,             # max rows to sample (default)
    "max_null_ratio": 0.01,    # generate is_not_null if nulls < 1%
    "max_empty_ratio": 0.01,   # generate is_not_null_or_empty if empties < 1%
    "max_in_count": 10,        # generate is_in if distinct values < 10
    "distinct_ratio": 0.05,    # generate is_in if distinct ratio < 5%
    "remove_outliers": True,   # remove outliers from min/max ranges
    "num_sigmas": 3,           # sigma threshold for outlier removal
    "trim_strings": True,      # trim whitespace before profiling strings
})
```

### Profile Specific Columns

```python
summary_stats, profiles = profiler.profile(df, columns=["id", "price", "status"])
```

## Manual SQL Profiling

When you want to understand data before writing checks, use SQL:

```sql
-- Column-level statistics
SELECT
  COUNT(*) as total_rows,
  COUNT(DISTINCT status) as distinct_statuses,
  COUNT(*) - COUNT(price) as null_prices,
  MIN(price) as min_price,
  MAX(price) as max_price,
  AVG(price) as avg_price,
  STDDEV(price) as stddev_price,
  PERCENTILE(price, 0.95) as p95_price
FROM catalog.schema.my_table
```

```sql
-- Frequency distribution
SELECT status, COUNT(*) as cnt
FROM catalog.schema.my_table
GROUP BY status
ORDER BY cnt DESC
```

```sql
-- Schema inspection
DESCRIBE TABLE EXTENDED catalog.schema.my_table
```

## Runnable Examples

| Example | File |
|---------|------|
| Profile a DataFrame | `../checks/examples/20_profiling_dataframe.py` |
| Profile specific columns | `../checks/examples/21_profiling_specific_columns.py` |
