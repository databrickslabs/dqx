---
name: dqx-custom-checks
description: Create custom DQX quality checks using SQL expressions, Python row-level functions, or Python dataset-level functions. Use when built-in checks are insufficient.
---

# Custom DQX Quality Checks

## SQL Expression (Simplest)

Use `sql_expression` for any row-level logic without writing Python:

```yaml
- criticality: error
  check:
    function: sql_expression
    arguments:
      expression: "end_date >= start_date"
      msg: "end_date must be on or after start_date"

- criticality: warn
  check:
    function: sql_expression
    arguments:
      expression: "quantity > 0 OR status = 'cancelled'"
      msg: "non-cancelled orders must have positive quantity"
```

## SQL Expression with Window Functions

For checks that need to look at groups of rows:

```yaml
# Flag all rows from a sensor if ANY reading exceeds threshold
- criticality: error
  name: sensor_reading_exceeded
  check:
    function: sql_expression
    arguments:
      expression: "MAX(reading_value) OVER (PARTITION BY sensor_id) > 100"
      msg: "sensor has reading above threshold"
      negate: true
```

## SQL Query (Custom Dataset Check)

For complex dataset-level validation with SQL:

```yaml
- criticality: error
  check:
    function: sql_query
    arguments:
      query: >
        SELECT department,
               COUNT(*) > 50 AS condition
        FROM {{ input }}
        GROUP BY department
      input_placeholder: input
      merge_columns: [department]
      condition_column: condition
      msg: "department has too many employees"
```

## Custom Python Row-Level Check

Define a reusable check function with `@register_rule("row")`:

```python
import pyspark.sql.functions as F
from pyspark.sql import Column
from databricks.labs.dqx.check_funcs import make_condition
from databricks.labs.dqx.rule import register_rule

@register_rule("row")
def not_ends_with(column: str, suffix: str) -> Column:
    """Check that column value does not end with the given suffix."""
    col_expr = F.col(column)
    return make_condition(
        col_expr.endswith(suffix),
        f"Column {column} ends with {suffix}",
        f"{column}_ends_with_{suffix}"
    )
```

Use in YAML (pass function name via `custom_check_functions`):

```yaml
- criticality: error
  check:
    function: not_ends_with
    arguments:
      column: email
      suffix: "@test.com"
```

Apply with custom functions:

```python
custom_fns = {"not_ends_with": not_ends_with}
result_df = dq_engine.apply_checks_by_metadata(df, checks, custom_fns)
```

Or use Python API directly:

```python
from databricks.labs.dqx.rule import DQRowRule

checks = [
    DQRowRule(
        criticality="error",
        check_func=not_ends_with,
        column="email",
        check_func_kwargs={"suffix": "@test.com"}
    ),
]
result_df = dq_engine.apply_checks(df, checks)
```

## Custom Python Dataset-Level Check

For checks that need aggregation or cross-DataFrame logic:

```python
import uuid
import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, SparkSession
from databricks.labs.dqx.check_funcs import make_condition
from databricks.labs.dqx.rule import register_rule

@register_rule("dataset")
def max_group_size(column: str, group_by: str, max_size: int) -> tuple[Column, callable]:
    """Check that no group exceeds max_size rows."""
    condition_col = f"_condition_{uuid.uuid4().hex[:8]}"

    def apply(df: DataFrame, spark: SparkSession) -> DataFrame:
        aggr = (
            df.groupBy(group_by)
            .agg((F.count(column) > max_size).alias(condition_col))
        )
        return df.join(aggr, on=group_by, how="left")

    return (
        make_condition(
            F.col(condition_col),
            f"Group {group_by} has more than {max_size} rows",
            f"{group_by}_max_group_size"
        ),
        apply
    )
```

The function must return a tuple of `(Column, Callable)`:

1. `Column` -- the condition expression (True = fail, from `make_condition`)
2. `Callable` -- closure that takes `(df, spark?, ref_dfs?)` and returns a DataFrame with the condition column added

Use in YAML:

```yaml
- criticality: warn
  check:
    function: max_group_size
    arguments:
      column: order_id
      group_by: customer_id
      max_size: 100
```

## Key Rules

- `make_condition(condition, message, alias)` creates the DQX condition column
- Row checks return a single `Column`
- Dataset checks return `(Column, Callable)` -- the callable must join results back to the input DataFrame
- Use `uuid.uuid4().hex` for unique temp column names to avoid collisions
- The closure can accept `df`, optionally `spark: SparkSession`, and optionally `ref_dfs: dict[str, DataFrame]`
- Custom functions must be registered with `@register_rule("row")` or `@register_rule("dataset")`

Full reference: https://databrickslabs.github.io/dqx/docs/reference/quality_checks/#creating-custom-row-level-checks
