---
name: dqx-define-checks
description: >
  Create DQX quality rules (checks) for a PySpark DataFrame or Delta table. Use when the
  user asks to "add a DQX check", "define a data quality rule", "validate that column X
  is not null / unique / in a set", or wants checks expressed in YAML/JSON for storage.
  Covers DQRowRule, DQDatasetRule, DQForEachColRule, built-in check_funcs, filters,
  user_metadata, custom SQL/Python checks, and the declarative metadata form.
---

# DQX — Define quality checks

DQX rules come in two interchangeable forms. **Pick based on where the checks will live.**

- **DQX classes** (`DQRowRule`, `DQDatasetRule`, `DQForEachColRule`) — use when checks are authored in code next to the pipeline. Static typing + IDE autocomplete.
- **Dict / YAML / JSON metadata** — use when checks are loaded from a file, workspace path, volume, or Delta table. Required for the `apply_checks_by_metadata*` path.

Every check has a **`criticality`** of `error` (failing row quarantined) or `warn` (failing row passes but flagged). Default is `error`.

## Minimal — class form

```python
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule, DQForEachColRule

checks = [
    # row-level: one column
    DQRowRule(
        name="col3_is_not_null",
        criticality="warn",
        check_func=check_funcs.is_not_null_and_not_empty,
        column="col3",
    ),

    # same check across many columns
    *DQForEachColRule(
        columns=["col1", "col2"],
        criticality="error",
        check_func=check_funcs.is_not_null,
    ).get_rules(),

    # dataset-level: uniqueness across a composite key
    DQDatasetRule(
        criticality="error",
        check_func=check_funcs.is_unique,
        columns=["order_id", "line_item_id"],
    ),
]
```

## Minimal — metadata form (YAML)

Load into Python via `yaml.safe_load(...)`, then pass the resulting `list[dict]` to any `apply_checks_by_metadata*` call, or save through a storage config (see `dqx-storage`).

```yaml
- name: col3_is_not_null
  criticality: warn
  check:
    function: is_not_null_and_not_empty
    arguments:
      column: col3

- criticality: error
  check:
    function: is_not_null
    for_each_column: [col1, col2]

- criticality: error
  check:
    function: is_unique
    arguments:
      columns: [order_id, line_item_id]
```

## Common variants

- **Filtered check** — evaluate only when a predicate holds: add `filter="col1 < 3"` (class) or `filter: "col1 < 3"` (YAML).
- **Positional args** — `check_func_args=[[1, 2]]`; **keyword args** — `check_func_kwargs={"allowed": [1, 2]}`.
- **Struct / map / array element** — use `F.try_element_at(...)` or dotted path (`col7.field1`) as the `column` value.
- **User metadata** — annotate the rule with a `user_metadata` dict (e.g. `{"check_type": "completeness"}`) that flows into the result struct.
- **Custom check** — pass any function that returns a PySpark `Column` as `check_func`, or use `sql_expression` / `sql_query` for inline SQL.
- **Aggregate dataset-level** — `is_aggr_not_greater_than`, `is_aggr_not_less_than`, `is_aggr_equal`, `is_aggr_not_equal`; supply `aggr_type` (`count`, `avg`, `stddev`, `percentile`, `count_distinct`…), optional `group_by`, and `limit`.
- **Uniqueness dataset-level** — `is_unique`, with `columns`, `nulls_distinct` (bool), and optional `row_filter`. Not an aggregate check — no `aggr_type`.

Full reference: <https://databrickslabs.github.io/dqx/docs/reference/quality_checks>.

## Converting between forms

```python
from databricks.labs.dqx.checks_serializer import serialize_checks, deserialize_checks
checks_metadata = serialize_checks(checks)              # classes → list[dict]
checks_classes  = deserialize_checks(checks_metadata)   # list[dict] → classes
```

## Validation before apply

Catch syntax errors without running the pipeline:

```python
from databricks.labs.dqx.engine import DQEngine
status = DQEngine.validate_checks(checks)   # raises / returns ValidationStatus
```

## Do / Don't

- **Do** give each rule a stable, snake_case `name` — it ends up in result columns and dashboards.
- **Do** put shared rules in a YAML/JSON/Delta file and load them (see `dqx-storage`) — classes are fine for a handful, metadata scales.
- **Don't** use a regex check for null / empty / range / referential / aggregate cases — DQX has a built-in for each; see `check_funcs`.
- **Don't** put side effects in a custom `check_func` — it must return a `Column` expression only.

Canonical docs: <https://databrickslabs.github.io/dqx/docs/guide/quality_checks_definition>.
