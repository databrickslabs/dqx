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

### validate\_profile\_options[​](#validate_profile_options "Direct link to validate_profile_options")

```python
def validate_profile_options(profiler_options: dict[str, Any]) -> None

```

Validates profiler options once, up front, before any profiling work is done.

Currently checks the allow/deny column-list pairs that are mutually exclusive, so a misconfiguration fails fast at option-build time rather than partway through a profiling run (which would happen if the check only ran per column inside *\_is\_profile\_enabled*).

**Arguments**:

* `profiler_options` - Configuration options for the DQProfiler (merged with defaults).

**Raises**:

* `InvalidParameterError` - if both the allow-columns and deny-columns option of a builder are set.

### make\_has\_no\_outliers\_profile[​](#make_has_no_outliers_profile "Direct link to make_has_no_outliers_profile")

```python
@register_profile_builder("has_no_outliers")
def make_has_no_outliers_profile(
        df: DataFrame, column_name: str, column_type: T.DataType,
        profiler_metrics: dict[str, Any],
        profiler_options: dict[str, Any]) -> DQProfile | None

```

Creates a *has\_no\_outliers* profile using the same MAD method as the *has\_no\_outliers* check rule.

A profile is returned when all the following conditions are met:

* The column type is child of `pyspark.sql.types.NumericType`.
* The DataFrame is non-empty.
* The fraction of outliers (values outside *median* ± 3.5 × MAD) is at or below *outliers\_ratio*.
* Profile generation is enabled at configuration level for all columns or given column.

**Arguments**:

* `df` - The DataFrame to create the profile for.
* `column_name` - Input column name
* `column_type` - Input column type
* `profiler_metrics` - Column-level statistics computed by the DQProfiler
* `profiler_options` - Configuration options for the DQProfiler

**Returns**:

A DQProfile if all conditions are met, otherwise None.
