---
name: dqx-storage
description: >
  Load and save DQX checks (quality rules) to a file, workspace path, Unity Catalog
  volume, Delta table, Lakebase, or the DQX installation folder. Use when the user asks
  to "load DQX checks from YAML", "save checks to a Delta table", "read checks from a
  volume", "share checks across notebooks", or "use the DQX workspace install's default
  checks location". Covers every *ChecksStorageConfig and the matching load/save calls.
---

# DQX — Load and store quality checks

DQX persists checks as **metadata** (`list[dict]`) — that's the portable form. Use class-form (`DQRowRule`) in code, then convert via `serialize_checks(...)` from `databricks.labs.dqx.checks_serializer` before saving (see `dqx-define-checks`).

All operations go through `DQEngine`:

```python
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

dq = DQEngine(WorkspaceClient())

dq.save_checks(checks_metadata, config=...)   # any ChecksStorageConfig below
checks_metadata = dq.load_checks(config=...)
```

## Pick a storage config

| Config | Location example | When to use |
|---|---|---|
| `FileChecksStorageConfig` | `"checks.yml"` | Local / repo-relative path. Simplest; also resolves to workspace paths when run from a Databricks notebook or job. |
| `WorkspaceFileChecksStorageConfig` | `"/Shared/App1/checks.yml"` | Absolute workspace path outside your installation. Good for team-shared rule libraries. |
| `VolumeFileChecksStorageConfig` | `"/Volumes/cat/schema/vol/checks.yml"` | Unity Catalog volume — versioned, auditable, works across workspaces. |
| `TableChecksStorageConfig` | `"catalog.schema.checks"` | Most scalable. Schema includes a `run_config_name` discriminator so multiple pipelines can share one table. |
| `InstallationChecksStorageConfig` | `run_config_name="default"` | Resolves to whatever `checks_location` is set in the DQX workspace install's `RunConfig`. Use this inside workflows so a single config change re-points every pipeline. |
| `LakebaseChecksStorageConfig` | `"database.schema.checks"` | Store checks in a Lakebase Postgres-compatible catalog. |

## YAML / JSON file example

```python
import yaml
from databricks.labs.dqx.config import FileChecksStorageConfig, WorkspaceFileChecksStorageConfig

checks = yaml.safe_load("""
- criticality: warn
  check:
    function: is_not_null_and_not_empty
    arguments:
      column: col3
- criticality: error
  check:
    function: is_not_null
    for_each_column: [col1, col2]
""")

# Relative / repo path
dq.save_checks(checks, config=FileChecksStorageConfig(location="checks.yml"))

# Absolute workspace path (shared across users)
dq.save_checks(checks, config=WorkspaceFileChecksStorageConfig(location="/Shared/App1/checks.yml"))
```

## Delta table example

```python
from databricks.labs.dqx.config import TableChecksStorageConfig

cfg = TableChecksStorageConfig(
    location="catalog.schema.checks",
    run_config_name="orders_pipeline",  # partition key — one row per (rule, run_config)
    mode="overwrite",                   # or "append"
)
dq.save_checks(checks, config=cfg)
loaded = dq.load_checks(config=cfg)
```

See [Table Schemas and Relationships](https://databrickslabs.github.io/dqx/docs/reference/table_schemas) for the table schema.

## Installation-based config

When DQX is installed in a workspace (`databricks labs dqx install`), every `RunConfig` in `.dqx/config.yml` has a `checks_location` field. Use `InstallationChecksStorageConfig` so notebooks, workflows, and ad-hoc code all resolve to the same place:

```python
from databricks.labs.dqx.config import InstallationChecksStorageConfig

cfg = InstallationChecksStorageConfig(run_config_name="default")
checks = dq.load_checks(config=cfg)
```

## CLI equivalents

```bash
databricks labs dqx open-remote-config                         # open the config file in the UI
databricks labs dqx profile --run-config default --patterns "main.product001.*"  # generate + save via the profiler workflow
```

## Do / Don't

- **Do** keep one authoritative store per pipeline — a Delta table or a volume YAML — and point every apply call at it. Don't scatter checks across notebooks.
- **Do** use `InstallationChecksStorageConfig` from workflows; it makes the pipeline portable across workspaces.
- **Do** version rule changes — if you're on Delta, `TIME TRAVEL` gives you that for free; if you're on YAML in a repo, use git.
- **Don't** try to save class-form checks (`DQRowRule`) directly — call `serialize_checks(...)` from `databricks.labs.dqx.checks_serializer` first.
- **Don't** use `FileChecksStorageConfig` with a relative path inside a Databricks job cluster unless you understand where that resolves — prefer `WorkspaceFileChecksStorageConfig` or a volume.

Canonical docs: <https://databrickslabs.github.io/dqx/docs/guide/quality_checks_storage>.
