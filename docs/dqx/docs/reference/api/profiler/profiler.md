---
sidebar_label: profiler
title: databricks.labs.dqx.profiler.profiler
---

## DQProfiler Objects

```python
class DQProfiler(DQEngineBase)
```

Data Quality Profiler class to profile input data.

#### get\_columns\_or\_fields

```python
@staticmethod
def get_columns_or_fields(columns: list[T.StructField]) -> list[T.StructField]
```

Extracts all fields from a list of StructField objects, including nested fields from StructType columns.

**Arguments**:

- `columns`: A list of StructField objects to process.

**Returns**:

A list of StructField objects, including nested fields with prefixed names.

#### profile

```python
def profile(
    df: DataFrame,
    columns: list[str] | None = None,
    options: dict[str, Any] | None = None
) -> tuple[dict[str, Any], list[DQProfile]]
```

Profiles a DataFrame to generate summary statistics and data quality rules.

**Arguments**:

- `df`: The DataFrame to profile.
- `columns`: An optional list of column names to include in the profile. If None, all columns are included.
- `options`: An optional dictionary of options for profiling.

**Returns**:

A tuple containing a dictionary of summary statistics and a list of data quality profiles.

#### profile\_table

```python
def profile_table(
    table: str,
    columns: list[str] | None = None,
    options: dict[str, Any] | None = None
) -> tuple[dict[str, Any], list[DQProfile]]
```

Profiles a table to generate summary statistics and data quality rules.

**Arguments**:

- `table`: The fully-qualified table name (`catalog.schema.table`) to be profiled
- `columns`: An optional list of column names to include in the profile. If None, all columns are included.
- `options`: An optional dictionary of options for profiling.

**Returns**:

A tuple containing a dictionary of summary statistics and a list of data quality profiles.

#### profile\_tables

```python
def profile_tables(
    tables: list[str] | None = None,
    patterns: list[str] | None = None,
    exclude_matched: bool = False,
    columns: dict[str, list[str]] | None = None,
    options: list[dict[str, Any]] | None = None
) -> dict[str, tuple[dict[str, Any], list[DQProfile]]]
```

Profiles Delta tables in Unity Catalog to generate summary statistics and data quality rules.

**Arguments**:

- `tables`: An optional list of table names to include.
- `patterns`: An optional list of table names or filesystem-style wildcards (e.g. &#x27;schema.*&#x27;) to include.
If None, all tables are included. By default, tables matching the pattern are included.
- `exclude_matched`: Specifies whether to include tables matched by the pattern. If True, matched tables
are excluded. If False, matched tables are included.
- `columns`: A dictionary with column names to include in the profile. Keys should be fully-qualified table
names (e.g. `catalog.schema.table`) and values should be lists of column names to include in profiling.
- `options`: A dictionary with options for profiling each table. Keys should be fully-qualified table names
(e.g. `catalog.schema.table`) and values should be options for profiling.

**Returns**:

A dictionary mapping table names to tuples containing summary statistics and data quality profiles.

