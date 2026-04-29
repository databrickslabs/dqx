---
name: dqx-profile-and-generate
description: >
  Profile a DataFrame or table and generate DQX quality rule candidates with summary
  statistics. Use when the user asks to "profile a table", "generate DQX rules from
  data", "suggest data quality checks", "bootstrap a checks.yml", or "generate DLT
  expectations". Covers DQProfiler, DQGenerator, DQDltGenerator, the profiler workflow,
  sampling / filter options, and AI-assisted variants.
---

# DQX — Profile and generate rule candidates

Typical one-shot bootstrap for a new table:

```python
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()
profiler = DQProfiler(ws)
generator = DQGenerator(ws)

df = spark.read.table("catalog.schema.input")

# Step 1 — profile. Returns summary stats + DQProfile candidates per column.
# Three entry points, pick by what you have on hand:
#   - profiler.profile(df, ...)                       — in-memory DataFrame
#   - profiler.profile_table(input_config=..., ...)   — single Unity Catalog table by InputConfig
#   - profiler.profile_tables_for_patterns(           — many tables; returns
#         patterns=["catalog.schema.*"], ...)              dict[table_fqn -> (stats, profiles)]
summary_stats, profiles = profiler.profile(df)

# Step 2 — turn candidates into DQX checks (declarative list[dict]).
checks = generator.generate_dq_rules(profiles)   # default criticality="error"

# Step 3 — inspect / edit, then persist. See dqx-storage for save targets.
for c in checks:
    print(c)
```

Profiling is **a one-time bootstrap action** per dataset. The candidate checks need human review before apply — don't auto-apply the raw output to production data.

## Scoping the profile

`DQProfiler.profile(df, columns=None, options=None)` — `columns` is a top-level kwarg limiting the profiled columns; the following optional keys are set via the `options` dict:

- **`sample_fraction`** — float 0–1 (e.g. `0.1` for 10% sample). Use on large tables.
- **`sample_seed`** — int; pair with `sample_fraction` for reproducible runs.
- **`limit`** — absolute row cap (e.g. `1_000_000`).
- **`filter`** — SQL string applied before profiling (`"event_date >= '2026-01-01'"`).
- **`criticality`** — default for every generated rule (`"error"` or `"warn"`, default `"error"`).

```python
summary_stats, profiles = profiler.profile(
    df,
    columns=["order_id", "total_amount", "country_code"],
    options={"sample_fraction": 0.1, "sample_seed": 42, "criticality": "warn"},
)
```

## Generating DLT / Lakeflow expectations

```python
from databricks.labs.dqx.profiler.dlt_generator import DQDltGenerator
dlt_expectations = DQDltGenerator(ws).generate_dlt_rules(profiles, language="python")
# language can be "python" or "sql"
```

## AI-assisted rule generation

DQX can generate rules from natural-language requirements via DSPy-backed LLMs — see the companion skills / docs rather than hand-rolling prompts:

- Natural-language rules → <https://databrickslabs.github.io/dqx/docs/guide/ai_assisted_quality_checks_generation>
- Primary-key detection → <https://databrickslabs.github.io/dqx/docs/guide/ai_assisted_primary_key_detection>
- Data-contract rules → <https://databrickslabs.github.io/dqx/docs/guide/data_contract_quality_rules_generation>

## No-code / workflow path (DQX installed as a workspace tool)

```bash
databricks labs dqx install                         # once per workspace
databricks labs dqx profile                         # all run configs
databricks labs dqx profile --run-config default    # one run config
databricks labs dqx profile --run-config default \
    --patterns "main.product001.*;main.product002" \
    --exclude-patterns "*_output;*_quarantine"
```

The workflow writes the generated candidates + summary stats to the `checks_location` on the run config (see `dqx-storage`).

## Do / Don't

- **Do** review the generated checks and tighten `criticality` / bounds before rolling to production.
- **Do** re-run profiling after a schema change or a large distribution shift — not on a schedule.
- **Don't** profile the output / quarantine table — the CLI auto-excludes `_dq_output` / `_dq_quarantine` suffixes; keep the convention.
- **Don't** run profiling on the full streaming firehose — use `limit` or `sample_fraction` against the current backfill.

Canonical docs: <https://databrickslabs.github.io/dqx/docs/guide/data_profiling>.
