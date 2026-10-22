# databricks.labs.dqx.profiler.profile_builder

### register_profile_builder[​](#register_profile_builder "Direct link to register_profile_builder")

```python
def register_profile_builder(
        profile_type: str,
        *,
        kind: Literal["legacy", "context"] | None = None) -> Callable

```

Register a profile builder in *PROFILE_BUILDER_REGISTRY*.

**Arguments**:

* `profile_type` - Registry key (e.g. *min_max*).

* `kind` - Callback shape.

  <!-- -->

  * *None* (default) or *"legacy"* — builder is a 5-argument callback matching the *ProfileBuilder* type alias. Registered as *DQProfileBuilder(name=..., builder=fn)*. Backward-compatible path.
  * *"context"* — builder is a single-argument callback matching *ContextualProfileBuilder*. Registered as *DQProfileBuilder(name=..., contextual_builder=fn)*.

### deregister_profile_builder[​](#deregister_profile_builder "Direct link to deregister_profile_builder")

```python
def deregister_profile_builder(profile_type: str) -> None

```

Removes a previously registered profile builder from *PROFILE_BUILDER_REGISTRY*. No-op if no builder is registered under the given key.

**Arguments**:

* `profile_type` - Key under which the builder was registered.

### make_null_or_empty_profile[​](#make_null_or_empty_profile "Direct link to make_null_or_empty_profile")

```python
@register_profile_builder("null_or_empty", kind="context")
def make_null_or_empty_profile(ctx: DQProfileContext) -> DQProfile | None

```

Creates an *is_not_null_or_empty*, *is_not_null*, or *is_not_empty* profile by checking the input column type, profiled metrics, and profiler options.

**Arguments**:

* `ctx` - Profile context (column, type, metrics, options).

**Returns**:

A DQProfile if the correct conditions are met, otherwise None.

### make_is_in_profile[​](#make_is_in_profile "Direct link to make_is_in_profile")

```python
@register_profile_builder("is_in", kind="context")
def make_is_in_profile(ctx: DQProfileContext) -> DQProfile | None

```

Creates an *is_in* profile.

When a semantic registry is configured and the column was classified as *enum*, the distinct values collected by the enum detector are reused (no extra Spark action). Columns classified as other semantic types (*key*, *measurement*, *text*, user-defined) are skipped. When no semantic type is present, the builder applies the shared applicability gate: the type must satisfy *\_supports_distinct* and the *distinct_count / count_non_null* ratio must fall below the configured *distinct_ratio* threshold — the same denominator used by the semantic-enum detector so the two paths agree on low-repetition columns.

**Notes**:

The ratio uses *count_non_null* as the denominator, so null-heavy low-cardinality columns may fail the gate and not emit *is_in*.

**Arguments**:

* `ctx` - Profile context (column, type, metrics, options, semantic_type).

**Returns**:

A DQProfile if the correct conditions are met, otherwise None.

### make_min_max_profile[​](#make_min_max_profile "Direct link to make_min_max_profile")

```python
@register_profile_builder("min_max", kind="context")
def make_min_max_profile(ctx: DQProfileContext) -> DQProfile | None

```

Creates a *min_max* profile.

Gated on semantic type when semantic profiling is enabled: skipped unless *ctx.semantic_type* is *None* or its name is *"measurement"*.

**Arguments**:

* `ctx` - Profile context (column, type, metrics, options, semantic_type).

**Returns**:

A DQProfile if the correct conditions are met, otherwise None.

### validate_profile_options[​](#validate_profile_options "Direct link to validate_profile_options")

```python
def validate_profile_options(profiler_options: dict[str, Any]) -> None

```

Validates profiler options once, up front, before any profiling work is done.

Currently checks the allow/deny column-list pairs that are mutually exclusive, so a misconfiguration fails fast at option-build time rather than partway through a profiling run (which would happen if the check only ran per column inside *\_is_profile_enabled*).

**Arguments**:

* `profiler_options` - Configuration options for the DQProfiler (merged with defaults).

**Raises**:

* `InvalidParameterError` - if both the allow-columns and deny-columns option of a builder are set.

### make_has_no_outliers_profile[​](#make_has_no_outliers_profile "Direct link to make_has_no_outliers_profile")

```python
@register_profile_builder("has_no_outliers", kind="context")
def make_has_no_outliers_profile(ctx: DQProfileContext) -> DQProfile | None

```

Creates a *has_no_outliers* profile using the same MAD method as the *has_no_outliers* check rule.

A profile is returned when all the following conditions are met:

* The column type is child of *pyspark.sql.types.NumericType*.
* The DataFrame is non-empty.
* The fraction of outliers (values outside *median* ± 3.5 × MAD) is at or below *outliers_ratio*.
* Profile generation is enabled at configuration level for all columns or given column.
* When semantic profiling is enabled, the semantic type is unset or *measurement*.

**Arguments**:

* `ctx` - Profile context (column, type, metrics, options, semantic_type).

**Returns**:

A DQProfile if all conditions are met, otherwise None.

### make_geospatial_profile[​](#make_geospatial_profile "Direct link to make_geospatial_profile")

```python
@register_profile_builder("geospatial")
def make_geospatial_profile(
        df: DataFrame, column_name: str, column_type: T.DataType,
        profiler_metrics: dict[str, Any],
        profiler_options: dict[str, Any]) -> list[DQProfile] | None

```

Creates geospatial profiles for native GEOMETRY/GEOGRAPHY columns.

The column is referenced directly (it is already a native geometry/geography value), so the profiled bounds line up with the checks the profiler generates, which set *convert_column=False*. Requires Databricks serverless compute or classic compute with Databricks Runtime Version >17.1. If the runtime does not meet these requirements, a warning is logged and no geospatial profiles are produced.

**Arguments**:

* `df` - Single-column DataFrame (nulls already dropped)
* `column_name` - Input column name
* `column_type` - Input column type
* `profiler_metrics` - Column-level statistics computed by the DQProfiler
* `profiler_options` - Configuration options for the DQProfiler

**Returns**:

A list of DQProfiles, or None when profiling is disabled, the column is not geospatial, the column is entirely null, or the spatial functions are unavailable.

**Notes**:

Because spatial aggregations required for profiling can be expensive at scale, geospatial profiling must be enabled via the *profile_geospatial* option (default: *False*).

When geospatial profiling is enabled and the column is a geometry or geography type, a single aggregation computes the bounding box, area range, point-count range, geometry-type distribution and quality counts. Summary stats are added to the *profiler_metrics* (and thus the summary statistics) and used to emit the matching row-level geospatial profiles.
