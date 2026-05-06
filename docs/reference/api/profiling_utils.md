# databricks.labs.dqx.profiling\_utils

Shared profiling utilities.

### compute\_null\_and\_distinct\_counts[​](#compute_null_and_distinct_counts "Direct link to compute_null_and_distinct_counts")

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

### compute\_exact\_distinct\_counts[​](#compute_exact_distinct_counts "Direct link to compute_exact_distinct_counts")

```python
def compute_exact_distinct_counts(
        df: DataFrame,
        columns: collections.abc.Iterable[str]) -> dict[str, int]

```

Compute exact distinct counts for provided columns.
