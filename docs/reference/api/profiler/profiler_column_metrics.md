# databricks.labs.dqx.profiler.profiler_column_metrics

### register_profile_column_metric[​](#register_profile_column_metric "Direct link to register_profile_column_metric")

```python
def register_profile_column_metric(
    profile_column_metric_type: str
) -> Callable[[DQProfileColumnMetricFunc], DQProfileColumnMetricFunc]

```

Registers data quality profile column metric function. The function that may create a column metric depending on the column type of the input column or other internal logic. Result column is used in an aggregation function resulting in a single value for a given column and data frame. The aggregation value will be used further to at the profiling stage to supply common column level metrics to construct corresponding builders.

Expected signature of the function is as follows: (field,column_label) -> Column | None where:

* field: struct field of the profiling column
* column_label: name of the column that is present in the dataframe to be aggregated The function may return *None* if aggregation is not applicable.

The following keys are reserved for always-on profiler internals and cannot be used as a registration key: *count*, *count_non_null*, *count_null*. Registering under any of these keys raises *InvalidParameterError*.

**Arguments**:

* `profile_column_metric_type` - Key under which the metric is registered and exposed to profile builders.

**Raises**:

* `InvalidParameterError` - If *profile_column_metric_type* is one of the reserved keys.

### deregister_profile_column_metric[​](#deregister_profile_column_metric "Direct link to deregister_profile_column_metric")

```python
def deregister_profile_column_metric(profile_column_metric_type: str) -> None

```

Removes a previously registered profile column metric from *PROFILE_COLUMN_METRIC_REGISTRY*. No-op if no metric is registered under the given key.

The reserved keys *count*, *count_non_null*, *count_null* are always-on and cannot be deregistered; passing any of them raises *InvalidParameterError* so callers do not silently assume a reserved metric has been disabled.

**Arguments**:

* `profile_column_metric_type` - Key under which the metric was registered.

**Raises**:

* `InvalidParameterError` - If *profile_column_metric_type* is one of the reserved keys.

### build_registered_metric_aggregations[​](#build_registered_metric_aggregations "Direct link to build_registered_metric_aggregations")

```python
def build_registered_metric_aggregations(field: T.StructField,
                                         column_label: str) -> list[Column]

```

Return aliased aggregation columns from *PROFILE_COLUMN_METRIC_REGISTRY*, skipping reserved keys and any metric function that returns *None* for the given field.

Reserved keys (*count*, *count_non_null*, *count_null*) are computed inline by the profiler and must never be shadowed by a registry entry — see *RESERVED_PROFILE_COLUMN_METRIC_KEYS*. The reserved-key filter here is defence-in-depth for entries injected directly into the registry (bypassing the *register_profile_column_metric* guard).

**Arguments**:

* `field` - Struct field of the profiling column.
* `column_label` - Name of the column present in the dataframe to be aggregated.

### empty_count[​](#empty_count "Direct link to empty_count")

```python
@register_profile_column_metric("empty_count")
def empty_count(field: T.StructField, column_label: str) -> Column

```

Profiling column metric for empty count. Applicable for text columns only, otherwise returns literal *0* for backward compatibility.

### count_distinct[​](#count_distinct "Direct link to count_distinct")

```python
@register_profile_column_metric("count_distinct")
def count_distinct(field: T.StructField, column_label: str) -> Column | None

```

Profiling column metric for count distinct. Applicable for all columns except native GEOMETRY/GEOGRAPHY columns, which do not support a meaningful SQL distinct count.
