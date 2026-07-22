# Temp-view cleanup + Genie schema move — Design

**Date:** 2026-07-22
**Worktree:** `/Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/bug-bash-v4`
**Branch:** `dqx/bug-bash-v4`

## Problem

Two related pieces of UC housekeeping in DQX Studio:

**Part A — Temp views leak.** Every dry-run / profiler / binding / scheduled run
creates a `tmp_view_<hex>` view (`CREATE OR REPLACE VIEW … AS SELECT * FROM <src>
[LIMIT n]`) as a stable handle for the async task-runner to read. These views
accumulate and are never reliably cleaned up. Confirmed on the live workspace
(`fe-sandbox-dq-demo-2`): ~17 orphaned `tmp_view_*` views, some in the **wrong
schema** (`dqx_studio` instead of `dqx_studio_tmp`).

Two independent root causes, both verified against the live `information_schema`:

1. **The weekly orphan-view GC is broken.** `SchedulerService._gc_orphan_views`
   (`services/scheduler_service.py`) lists candidates from
   `` `<cat>`.information_schema.views `` filtering on a **`created_at`** column.
   That column does not exist on `information_schema.views` (columns are
   `check_option, is_insertable_into, is_materialized, is_updatable, sql_path,
   table_catalog, table_name, table_schema, view_definition`). The creation
   timestamp is **`created`** and lives on `information_schema.**tables**`.
   Running the exact query returns `UNRESOLVED_COLUMN.WITH_SUGGESTION` for
   `created_at`. The surrounding `try/except` swallows it and the sweep reaps
   nothing — on every run, forever. The GC unit tests mock `self._tmp_sql.query`,
   so the bad column name never surfaced in CI.

2. **The demo-seed run path creates views in the main schema.**
   `dependencies.py` (`get_demo_seed_service`) builds
   `sp_view = ViewService(sql=sp_sql, sp_sql=sp_sql)`, where `sp_sql` is bound to
   `conf.schema_name` (the main `dqx_studio` schema). `ViewService.create_view`
   derives the view FQN from `self._sql.schema`, so the demo's `BindingRunService`
   creates its temp views in `dqx_studio`, not `dqx_studio_tmp`. The
   `profiler_view` constructed immediately below it does it correctly (explicit
   `tmp_schema_name` executor) — so this is an oversight, not intent.

**Net effect:** views leak on every non-browser path (Cause 1 disables the only
backstop), and demo runs put a subset in the main schema where even a fixed GC —
scoped to `dqx_studio_tmp` — would never see them (Cause 2).

**Part B — Genie objects clutter the main schema.** Seven SP-owned *derived*
objects built for the Genie space currently live in `dqx_studio` alongside the
app-core tables. They should live in a dedicated `genie` schema for clarity.

## Goals

- Temp views are dropped promptly (within ~60s) after their run reaches ANY
  terminal state (SUCCESS / FAILED / ERROR / SKIPPED / TIMEOUT).
- New temp views always land in `dqx_studio_tmp`.
- The weekly GC works as a genuine backstop (correct query, tmp schema).
- The 7 Genie-derived views/dims live in a new `genie` schema; base Delta tables
  stay in `dqx_studio`; every reference is repointed; the Genie space still works.

## Non-goals

- Moving base Delta tables (`dq_validation_runs`, `dq_metrics`,
  `dq_quarantine_records`, `dq_profiling_results`) — they stay in `dqx_studio`.
  The task-runner write path is untouched.
- Changing the browser status-poll cleanup path (`routes/v1/dryrun.py`,
  `routes/v1/profiler.py`) — it already works for OBO-owned views.
- Reworking how views are created / named / granted.

## Global Constraints

- **SQL safety:** interpolated identifiers pass `validate_fqn` + `quote_fqn`
  (`sql_utils.py`); string literals via `escape_sql_string` (ANSI doubled quotes).
  No user input reaches these paths (names are app-generated / constants).
- **Backend type hints:** every param + return annotated; `str | None` not
  `Optional`; `list`/`dict` generics; no `Any`. Enforced by basedpyright
  (`make app-check`).
- **Only-drop-own:** dropping a view requires MANAGE. The scheduler + task-runner
  run as the app SP (owns SP-created views). OBO-created views (manual runs) are
  owned by the user — the SP may lack MANAGE — so those stay covered by the
  browser status-poll (primary) and the SP best-effort drop is allowed to fail
  benignly (logged, not raised), exactly as the existing task-runner `finally`
  already does.
- **Never drop an in-use view.** A view may only be dropped once its run's
  `dq_validation_runs` / `dq_profiling_results` row is non-`RUNNING`. The existing
  terminal-detection gate provides this; tests must lock it in.
- **Tests must exercise real SQL shape.** GC / drop-on-completion tests must
  assert the emitted SQL references the correct catalog objects/columns
  (`information_schema.tables`, `table_type = 'VIEW'`, `created`) — a mock that
  only checks call counts is what let the `created_at` bug ship.
- **Backups:** existing behaviour (score-cache refresh, timestamp denorm) driven
  by the same scheduler tick must be preserved unchanged.

---

## Part A: Temp-view cleanup

### A1 — Primary: drop-on-completion in the scheduler tick

The scheduler already runs a 60s tick that:
- tracks every run it launches (`_track_run_for_score_refresh`) AND sweeps
  manual UI runs from the last 24h (`_sweep_recent_run_sets`) into
  `_pending_score_runs`;
- detects which tracked runs reached a terminal state via
  `_collect_completed_score_run_fqns`, whose query is:
  ```sql
  SELECT DISTINCT run_id, source_table_fqn FROM <runs_table>
  WHERE run_id IN (...) AND UPPER(status) <> 'RUNNING'
  ```

**Change:** extend that SELECT to also return `view_fqn` (confirmed present on
`dq_validation_runs`), and for each completed run drop its view as the SP —
best-effort, `tmp_view_`-guarded, permission-failure benign.

- The method currently returns `set[str]` of FQNs and is consumed by the score
  refresh + reconcile. To avoid entangling view-dropping with score logic, the
  cleanest split is: `_collect_completed_score_run_fqns` gathers per-run rows
  (run_id, source_table_fqn, view_fqn); it feeds the score `fqns` set as today
  AND collects `view_fqn`s into a list handed to a new
  `_drop_completed_run_views(view_fqns)` helper, called from
  `_refresh_scores_for_completed_runs` right after the run is popped from
  tracking. Preserve the existing return contract for the reconcile caller.
- `_drop_completed_run_views`:
  - filters to values containing `tmp_view_` (skip the synthetic cross-table
    `view_fqn` which is the quoted source table, not a temp view — same
    `_SQL_CHECK_PREFIX` / `tmp_view_` discrimination the codebase already uses);
  - `validate_fqn` + `quote_fqn` each, `DROP VIEW IF EXISTS` via `self._tmp_sql`
    (the SP tmp-schema executor the GC already uses);
  - wraps each drop in try/except, logs failure at WARNING with the view name,
    never raises (one bad drop must not wedge the tick or block score refresh).
- Runs whose terminal row never lands still expire from tracking via the
  existing `_SCORE_REFRESH_TTL`; their views fall to the GC backstop (A3).

**Coverage:** scheduled, binding, demo, and browser-closed manual runs get their
view dropped within ~60s of completion. Manual runs with the browser open are
still dropped immediately by the OBO status-poll (unchanged) — the SP tick then
finds the view already gone (`DROP … IF EXISTS` = no-op).

### A2 — Fix the demo `sp_view` schema

`dependencies.py` `get_demo_seed_service`: change
`sp_view = ViewService(sql=sp_sql, sp_sql=sp_sql)` so its `sql` executor is bound
to `conf.tmp_schema_name` (a fresh SP `SqlExecutor` on the tmp schema), mirroring
the `profiler_view` constructed just below. `sp_sql` stays the DDL/grant executor
(`sp_sql=` slot). This makes demo binding-run views land in `dqx_studio_tmp`.

### A3 — Fix the GC query (backstop)

`_gc_orphan_views` list query becomes:
```sql
SELECT table_name
FROM `<cat>`.information_schema.tables
WHERE table_schema = '<tmp_schema>'
  AND table_type = 'VIEW'
  AND table_name LIKE 'tmp\_view\_%' ESCAPE '\'
  AND created < current_timestamp() - INTERVAL <GC_AGE_HOURS> HOUR
ORDER BY created ASC
LIMIT <GC_MAX_DROPS_PER_RUN>
```
- `information_schema.views` → `information_schema.tables`; add
  `table_type = 'VIEW'`; `created_at` → `created` (both WHERE and ORDER BY).
- Verified against the live workspace: this query parses and returns the aged
  candidates. Everything else in `_gc_orphan_views` (name regex, RUNNING
  cross-check, per-view drop loop) stays as-is. Scope remains `dqx_studio_tmp`.

### A4 — One-off cleanup of existing orphans

After deploy, manually `DROP VIEW IF EXISTS` the existing `tmp_view_*` in BOTH
`dqx_studio` and `dqx_studio_tmp` via the SQL warehouse (all confirmed >45h old,
none tied to a RUNNING run). Not code — an operational step performed once.

### A5 — Test hardening

- Drop-on-completion: a test that seeds a completed run row carrying a
  `tmp_view_*` `view_fqn` and asserts a `DROP VIEW IF EXISTS` is issued for it;
  a RUNNING run's view is NOT dropped; a synthetic (non-`tmp_view_`) `view_fqn`
  is skipped; a drop failure is swallowed and doesn't block score refresh.
- GC: assert the emitted list SQL references `information_schema.tables`,
  `table_type = 'VIEW'`, and `created` (guards against a column-name regression).
- Demo wiring: assert `sp_view`'s create path targets `dqx_studio_tmp`.

---

## Part B: Move Genie derived views/dims to a `genie` schema

### Objects to move (7, all SP-owned derivations)

| Constant | Name | Owner service |
|---|---|---|
| `METRIC_VIEW_NAME` | `mv_dq_scores` | `score_view_service` |
| `SHAPING_VIEW_NAME` | `v_dq_check_results` | `score_view_service` |
| `ASOF_VIEW_NAME` | `v_dq_check_results_asof` | `score_view_service` |
| `ATTRIBUTION_VIEW_NAME` | `v_dq_check_attribution` | `score_view_service` |
| `DIM_RULES_TABLE_NAME` | `dim_dq_rules` | `metadata_dim_service` |
| `DIM_MONITORED_TABLES_TABLE_NAME` | `dim_dq_monitored_tables` | `metadata_dim_service` |
| `FAILING_ROWS_VIEW_NAME` | `v_dq_failing_rows` | `entitlement_service` |

Base tables (`dq_validation_runs`, `dq_metrics`, `dq_quarantine_records`,
`dq_profiling_results`) and `dq_user_table_entitlements` stay in `dqx_studio`.

### Config + schema provisioning

- New config field `genie_schema_name` (default `genie`, env
  `DQX_GENIE_SCHEMA`), mirroring `tmp_schema_name` in `config.py`.
- `databricks.yml`: new `var.genie_schema_name` (default `genie`), a
  `resources.schemas.genie_schema` with `lifecycle.prevent_destroy: true` and
  native `grants:` matching `main_schema` (app SP + task-runner SP as needed +
  `account users` for SELECT — the Genie space queries these as the SP and end
  users read them). New env var `DQX_GENIE_SCHEMA` on the app resource.
- Runtime fallback: `CREATE SCHEMA IF NOT EXISTS `<cat>`.`genie`` at startup as
  SP (same place/pattern the tmp schema is ensured), so local dev / a
  not-yet-applied deploy still works.

### The two-schema split (the core mechanic)

Today `ScoreViewService`, `MetadataDimService`, and the failing-rows view in
`EntitlementService` all derive both their OWN object names AND their base-table
references from ONE main-schema executor (`self._schema_q`). After the move:

- **Own object name** → `genie` schema.
- **Base-table references inside the view body** (`dq_validation_runs`,
  `dq_metrics`, etc.) → explicit `dqx_studio` FQN.

Implementation: give each service an explicit `genie_schema` (or a dedicated
genie-schema executor) for its object FQNs, while base-table FQNs are built from
the main schema it already holds. Concretely:
- `ScoreViewService.__init__(sql, genie_schema)` — `sql` stays the main-schema
  executor (used for base-table FQNs + running DDL); a new `genie_schema` string
  builds the 4 view FQNs. The `metric_view_fqn(catalog, schema)` module helper
  and `_curated_sqls(catalog, schema)` / `_diagnose_sqls` in `genie_space_service`
  take the genie schema for object names.
- `MetadataDimService` — same shape: dims created in `genie`, but any base-table
  reads inside their refresh SQL use `dqx_studio`.
- `EntitlementService` — `v_dq_failing_rows` created in `genie`; it reads
  `dq_quarantine_records` (stays `dqx_studio`) and `dq_user_table_entitlements`
  (stays `dqx_studio`).

### Reference repointing

Repoint every reader to the genie FQNs. Files identified:
`routes/v1/genie.py`, `routes/v1/dq_results.py`, `routes/v1/dq_score.py`,
`services/genie_chat_service.py`, `services/genie_space_service.py`,
`services/dq_results_service.py`, `services/score_cache_service.py`,
`services/run_sets.py`, `services/table_data_service.py`, plus `app.py` /
`dependencies.py` wiring and `migrations/__init__.py` (the object list the
startup ensure/refresh iterates). Each of these currently resolves the object
via the shared main schema; they must resolve via the genie schema instead.
The Genie space `data_sources` / curated-SQL FQNs (`_plain_fqn`,
`quote_object_fqn`) use the genie schema.

### Old-copy removal + reset service

- **A4-adjacent one-off:** after deploy, manually `DROP` the stale
  `dqx_studio` copies of the 7 objects (bundled with the temp-view orphan drop).
- `database_reset_service`: the DELETE/DROP scope + any genie-object recreation
  must target the `genie` schema for these 7 objects. The Genie reprovision hook
  is unchanged.

### Migration / startup ensure

The startup path that ensures these views/dims exist (idempotent
`CREATE OR REPLACE`) must (a) ensure the `genie` schema first, then (b) create
the objects in `genie`. No Delta/OLTP data migration is needed — the objects are
rebuilt from base tables on creation. `information_schema`-based existence checks
(if any) must look in the genie schema.

### Part B tests

- `score_view_service` / `metadata_dim_service` / `entitlement_service`: assert
  created object FQNs use the genie schema AND base-table references inside the
  bodies use the main schema (the cross-schema split is the risk).
- `genie_space_service`: assert curated-SQL + data-source FQNs use the genie
  schema (existing payload tests updated).
- Config: `genie_schema_name` default `genie`, `DQX_GENIE_SCHEMA` override.

---

## Sequencing / ordering

Part A and Part B are independent (different objects, different code). Build A
first (it's the reported bug), then B. Both land before deploy; the app is
brought up for user verification BEFORE squash-merge.

## Deploy / verification

- `make app-check` (bun tsc -b + basedpyright) clean.
- `make app-test` (backend pytest) green.
- Build wheel with the `.cloud` pypi-proxy fallback env vars (dev host flaky).
- Deploy to `fe-sandbox-dq-demo-2` (`make app-deploy PROFILE=fe-sandbox-dq-demo-2
  TARGET=dev`); app RUNNING.
- Perform A4 + B old-copy one-off drops via warehouse `43a1704b9dfa9ebc`.
- Report to user for verification. Do NOT squash-merge / push until told.

## Test gates summary

- Backend: `make app-test` green (new tests for A1/A3/A5/B).
- Types: `make app-check` clean (the 5 pre-existing `monitored_table_service.py`
  basedpyright errors are unrelated and out of scope).
- Conventions: `app/CLAUDE.md`, `app/src/databricks_labs_dqx_app/backend/CLAUDE.md`,
  root `AGENTS.md`.
