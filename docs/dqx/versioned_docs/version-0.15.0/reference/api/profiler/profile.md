---
sidebar_label: profile
title: databricks.labs.dqx.profiler.profile
---

## DQProfile Objects

```python
@dataclass(frozen=True)
class DQProfile()
```

Data quality profile class representing a data quality rule candidate.

## DQProfileBuilder Objects

```python
@dataclass(frozen=True)
class DQProfileBuilder()
```

Data quality profile builder class: a named builder that may produce a DQProfile for a column.

**Attributes**:

- `name` - Profile type identifier (e.g. &quot;null_or_empty&quot;, &quot;is_in&quot;, &quot;min_max&quot;). Used to
  look up the builder in the registry and in generated rule metadata.
- `builder` - Callable that inspects column data and options and returns a DQProfile when
  the column matches the profile criteria, otherwise None. Signature:
  
  (df, column_name, column_type, profiler_metrics, profiler_options) -&gt; DQProfile | None
  
  - df: DataFrame for this column (non-null rows only; strings trimmed
  when profiler_options[&quot;trim_strings&quot;] is True). Used for distinct/min/max etc.
  - column_name: Name of the column being profiled.
  - column_type: Spark DataType of the column (e.g. StringType(), LongType()).
  - profiler_metrics: Column-level statistics from the profiler (e.g. count,
  count_null, empty_count, count_non_null). Same key set as summary_stats[column_name].
  - profiler_options: Profiler options for this run (e.g. max_null_ratio,
  max_empty_ratio, max_in_count, trim_strings, filter).

