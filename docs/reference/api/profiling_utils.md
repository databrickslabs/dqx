# databricks.labs.dqx.profiling_utils

Shared profiling utilities.

### compute_null_and_distinct_counts[​](#compute_null_and_distinct_counts "Direct link to compute_null_and_distinct_counts")

```python
def compute_null_and_distinct_counts(
        df: DataFrame,
        column_names: collections.abc.Iterable[str],
        distinct_columns: collections.abc.Iterable[str],
        *,
        approx: bool = True,
        rsd: float = 0.05) -> tuple[dict[str, int], dict[str, int]]

```

Compute null counts and (approx) distinct counts in a single aggregation.

### compute_exact_distinct_counts[​](#compute_exact_distinct_counts "Direct link to compute_exact_distinct_counts")

```python
def compute_exact_distinct_counts(
        df: DataFrame,
        columns: collections.abc.Iterable[str]) -> dict[str, int]

```

Compute exact distinct counts for provided columns.

### calculate_median_absolute_deviation_bounds[​](#calculate_median_absolute_deviation_bounds "Direct link to calculate_median_absolute_deviation_bounds")

```python
def calculate_median_absolute_deviation_bounds(
    df: DataFrame,
    column: str,
    filter_condition: str | Column | None = None
) -> tuple[float, float] | None

```

Calculates the lower and upper bounds using the median absolute deviation of a numeric column.

Bounds are defined as *median* ± 3.5 × MAD. Returns None if the filtered DataFrame is empty and the median cannot be computed.

**Arguments**:

* `df` - PySpark DataFrame
* `column` - Name of the numeric column to calculate MAD for
* `filter_condition` - Filter to apply before calculation (optional), as a SQL expression string or a pre-compiled Column (e.g. a validated filter from *safe_filter_expr*).

**Returns**:

A (lower_bound, upper_bound) tuple, or None if bounds cannot be calculated.

### calculate_median_absolute_deviation[​](#calculate_median_absolute_deviation "Direct link to calculate_median_absolute_deviation")

```python
def calculate_median_absolute_deviation(
    df: DataFrame, column: str, filter_condition: str | Column | None
) -> tuple[float | None, float | None]

```

Calculates the Median Absolute Deviation (MAD) for a numeric column.

MAD is a robust measure of variability: MAD = median(|X_i - median(X)|). Computation applies *filter_condition* first, then computes the column median, then the median of the absolute deviations from that median.

**Arguments**:

* `df` - PySpark DataFrame
* `column` - Name of the numeric column to calculate MAD for
* `filter_condition` - Filter to apply before calculation (optional), as a SQL expression string or a pre-compiled Column (e.g. a validated filter from *safe_filter_expr*).

**Returns**:

A (median, mad) tuple. Both values are None when the filtered DataFrame is empty.

### median_absolute_deviation_bounds_df[​](#median_absolute_deviation_bounds_df "Direct link to median_absolute_deviation_bounds_df")

```python
def median_absolute_deviation_bounds_df(df: DataFrame, column: str,
                                        filter_condition: Column | None,
                                        lower_bound_col: str,
                                        upper_bound_col: str) -> DataFrame

```

Lazily builds the median absolute deviation bounds of a numeric column.

**Arguments**:

* `df` - PySpark DataFrame
* `column` - Name of the numeric column to calculate MAD for
* `filter_condition` - Filter to apply before calculation (optional), as a pre-compiled Column.
* `lower_bound_col` - Name of the output column holding the lower bound.
* `upper_bound_col` - Name of the output column holding the upper bound.

**Returns**:

A one-row DataFrame with *lower_bound_col* and *upper_bound_col*. Both are null when the filtered DataFrame is empty or the column has only nulls.
