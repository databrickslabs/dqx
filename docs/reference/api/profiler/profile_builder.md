# databricks.labs.dqx.profiler.profile\_builder

### make\_null\_or\_empty\_profile[​](#make_null_or_empty_profile "Direct link to make_null_or_empty_profile")

```python
@register_profile_builder("null_or_empty")
def make_null_or_empty_profile(
        _: DataFrame, column_name: str, column_type: T.DataType,
        profiler_metrics: dict[str, Any],
        profiler_options: dict[str, Any]) -> DQProfile | None

```

Creates an 'is\_not\_null\_or\_empty', 'is\_not\_null', or 'is\_not\_empty' profile by checking the input column type, profiled metrics, and profiler options.

**Arguments**:

* `column_name` - Input column name
* `column_type` - Input column type
* `profiler_metrics` - Column-level statistics computed by the DQProfiler
* `profiler_options` - Configuration options for the DQProfiler

**Returns**:

A DQProfile if the correct conditions are met, otherwise None

### make\_is\_in\_profile[​](#make_is_in_profile "Direct link to make_is_in_profile")

```python
@register_profile_builder("is_in")
def make_is_in_profile(df: DataFrame, column_name: str,
                       column_type: T.DataType, profiler_metrics: dict[str,
                                                                       Any],
                       profiler_options: dict[str, Any]) -> DQProfile | None

```

Creates an 'is\_in' profile by checking the input column type, profiled metrics, and profiler options.

**Arguments**:

* `df` - Single-column DataFrame
* `column_name` - Input column name
* `column_type` - Input column type
* `profiler_metrics` - Column-level statistics computed by the DQProfiler
* `profiler_options` - Configuration options for the DQProfiler

**Returns**:

A DQProfile if the correct conditions are met, otherwise None

### make\_min\_max\_profile[​](#make_min_max_profile "Direct link to make_min_max_profile")

```python
@register_profile_builder("min_max")
def make_min_max_profile(df: DataFrame, column_name: str,
                         column_type: T.DataType, profiler_metrics: dict[str,
                                                                         Any],
                         profiler_options: dict[str, Any]) -> DQProfile | None

```

Creates a 'min\_max' profile by checking the input column type, profiled metrics, and profiler options.

**Arguments**:

* `df` - Single-column DataFrame
* `column_name` - Input column name (used for DQProfile output)
* `column_type` - Input column type
* `profiler_metrics` - Column-level statistics computed by the DQProfiler (includes summary stats)
* `profiler_options` - Configuration options for the DQProfiler

**Returns**:

A DQProfile if the correct conditions are met, otherwise None
