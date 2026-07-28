---
sidebar_label: profile_builder
title: databricks.labs.dqx.profiler.profile_builder
---

#### make\_null\_or\_empty\_profile

```python
@register_profile_builder("null_or_empty")
def make_null_or_empty_profile(
        _: DataFrame, column_name: str, column_type: T.DataType,
        profiler_metrics: dict[str, Any],
        profiler_options: dict[str, Any]) -> DQProfile | None
```

Creates an &#x27;is_not_null_or_empty&#x27;, &#x27;is_not_null&#x27;, or &#x27;is_not_empty&#x27; profile by checking the input column type,
profiled metrics, and profiler options.

**Arguments**:

- `column_name` - Input column name
- `column_type` - Input column type
- `profiler_metrics` - Column-level statistics computed by the DQProfiler
- `profiler_options` - Configuration options for the DQProfiler
  

**Returns**:

  A DQProfile if the correct conditions are met, otherwise None

#### make\_is\_in\_profile

```python
@register_profile_builder("is_in")
def make_is_in_profile(df: DataFrame, column_name: str,
                       column_type: T.DataType, profiler_metrics: dict[str,
                                                                       Any],
                       profiler_options: dict[str, Any]) -> DQProfile | None
```

Creates an &#x27;is_in&#x27; profile by checking the input column type, profiled metrics, and profiler options.

**Arguments**:

- `df` - Single-column DataFrame
- `column_name` - Input column name
- `column_type` - Input column type
- `profiler_metrics` - Column-level statistics computed by the DQProfiler
- `profiler_options` - Configuration options for the DQProfiler
  

**Returns**:

  A DQProfile if the correct conditions are met, otherwise None

#### make\_min\_max\_profile

```python
@register_profile_builder("min_max")
def make_min_max_profile(df: DataFrame, column_name: str,
                         column_type: T.DataType, profiler_metrics: dict[str,
                                                                         Any],
                         profiler_options: dict[str, Any]) -> DQProfile | None
```

Creates a &#x27;min_max&#x27; profile by checking the input column type, profiled metrics, and profiler options.

**Arguments**:

- `df` - Single-column DataFrame
- `column_name` - Input column name (used for DQProfile output)
- `column_type` - Input column type
- `profiler_metrics` - Column-level statistics computed by the DQProfiler (includes summary stats)
- `profiler_options` - Configuration options for the DQProfiler
  

**Returns**:

  A DQProfile if the correct conditions are met, otherwise None

