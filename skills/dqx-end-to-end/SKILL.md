---
name: dqx-end-to-end
description: >
  Run DQX validation end-to-end — read an input table or path, apply checks, and write
  valid and quarantined rows to output locations — in a single call. Use when the user
  asks for "apply and save", "quality-check a table and split the output", "DQX on a
  whole table", "save valid and invalid rows", or wants to drop DQX into a Lakeflow /
  workflow that runs on a table or path. Covers apply_checks_and_save_in_table,
  the by_metadata variant, InputConfig / OutputConfig, and incremental streaming mode.
---

# DQX — End-to-end apply + save

One method call: read, check, write valid rows, (optionally) write quarantined rows.

```python
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule
from databricks.labs.dqx.config import InputConfig, OutputConfig
from databricks.sdk import WorkspaceClient
# `spark` is available in Databricks notebooks / jobs. Locally, create it with
# `from pyspark.sql import SparkSession; spark = SparkSession.builder.getOrCreate()`.

dq = DQEngine(WorkspaceClient())

checks = [
    DQRowRule(criticality="warn",  check_func=check_funcs.is_not_null,   column="col3"),
    DQDatasetRule(criticality="error", check_func=check_funcs.is_unique, columns=["col1", "col2"]),
]

# Split into valid + quarantine tables
dq.apply_checks_and_save_in_table(
    checks=checks,
    input_config=InputConfig(location="catalog.schema.input"),
    output_config=OutputConfig(location="catalog.schema.valid"),
    quarantine_config=OutputConfig(location="catalog.schema.quarantine"),
)

# Or — keep everything in a single annotated output table
dq.apply_checks_and_save_in_table(
    checks=checks,
    input_config=InputConfig(location="catalog.schema.input"),
    output_config=OutputConfig(location="catalog.schema.annotated"),
)
```

## Metadata form (checks loaded from storage)

```python
dq.apply_checks_by_metadata_and_save_in_table(
    checks=checks_metadata,      # list[dict] — see dqx-define-checks
    input_config=InputConfig(location="catalog.schema.input"),
    output_config=OutputConfig(location="catalog.schema.valid"),
    quarantine_config=OutputConfig(location="catalog.schema.quarantine"),
)
```

If `checks` is omitted and `checks_location` is set on a `RunConfig` (workspace install flow), checks are loaded automatically from that storage — see `dqx-storage`.

## InputConfig / OutputConfig — common options

- **`location`** — Unity Catalog table (`catalog.schema.table`) or Unity Catalog Volume path (`/Volumes/catalog/schema/volume/...`).
- **`format`** — default `delta`; set when writing to a volume as parquet/json/csv.
- **`options`** — dict passed through to reader/writer (e.g. `{"mergeSchema": "true"}`).
- **`mode`** — `"append"` (default for batch) or `"overwrite"`.
- **Streaming** — set `input_config.is_streaming=True` to opt into incremental reads. Provide `options={"checkpointLocation": "/Volumes/.../checkpoints/xxx"}` on the output config. DQX's batch-style incremental reader uses an `AvailableNow`-style trigger by default; set a different trigger via the output config if you need continuous streaming. Full details: [Applying Quality Checks](https://databrickslabs.github.io/dqx/docs/guide/quality_checks_apply).

## Multi-table / pattern execution

- **`apply_checks_and_save_in_tables(run_configs)`** — accept a list of `RunConfig` and fan out over each `(input, output, quarantine)` triple.
- **`apply_checks_and_save_in_tables_for_patterns(patterns, checks_location, run_config_template=...)`** — expand wildcard patterns (Python `list[str]`, e.g. `["main.product001.*", "main.product002"]`) against Unity Catalog and reuse one `RunConfig` template for every matched table. Output / quarantine names are derived from the input name + `output_table_suffix` / `quarantine_table_suffix` (defaults `_dq_output` / `_dq_quarantine`). The semicolon-delimited form is a convention of the `databricks labs dqx` CLI, **not** the Python API.

## Do / Don't

- **Do** set a unique `checkpointLocation` per output when using streaming — DQX uses it to track watermarks; sharing one across pipelines causes silent data loss.
- **Do** point `quarantine_config` at a separate table. Without it, failed rows get the `_errors` column but remain in the single output table.
- **Do** use the same method signature for batch and streaming — the difference is one `is_streaming` flag on `InputConfig`.
- **Don't** manually pre-filter `_errors.isNull()` before writing — DQX already routes error-criticality rows to `quarantine_config` when you provide it.
- **Don't** mix `checks=` and `checks_location` in a `RunConfig` — pick one source of truth.

Canonical docs:
- End-to-end apply patterns: <https://databrickslabs.github.io/dqx/docs/guide/quality_checks_apply#applying-checks-defined-with-dqx-classes>
- Config dataclasses: <https://databrickslabs.github.io/dqx/docs/reference/engine>
- Demos: <https://databrickslabs.github.io/dqx/docs/demos>
