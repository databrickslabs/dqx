# DQX Studio

Web application for the DQX framework — a UI for authoring and managing data quality rules. Built with FastAPI (backend) and React (frontend), deployed as a Databricks App.

- **[Local Development →](DEVELOPMENT.md)** — set up your environment, run dev servers, test changes
- **[Deployment →](DEPLOYMENT.md)** — deploy to Databricks Apps via DABs or Marketplace

> **Breaking change: fresh installs only.** This release changes the storage layout and permission model. There is no upgrade path for DAB or Marketplace installs; remove the previous app, job, warehouse, schemas, and Lakebase project before installing. See [DEPLOYMENT.md](DEPLOYMENT.md#removing-a-previous-installation).

## Marketplace release artifacts

The main branch tracks the canonical application source and Marketplace templates, while the generated `app/marketplace/` artifact remains untracked. An annotated signed Studio version tag is the immutable input for a complete Marketplace release. A releaser runs `app/scripts/release_marketplace.sh studio-vX.Y.Z`; the script verifies the tag and matching app version, builds and force-stages the self-contained artifact on local branch `dqx-studio/marketplace/vX.Y.Z`, signs and verifies its commit, and never pushes. After inspection, publish the branch and tag explicitly with `git push origin dqx-studio/marketplace/vX.Y.Z` and `git push origin studio-vX.Y.Z`.

DAB deployments are independent and continue to build and consume `app/.build/`.

## Architecture

- **Backend**: FastAPI (`src/databricks_labs_dqx_app/backend/`) — REST API under `/api/v1`, no Spark in the app process
- **Frontend**: React + TypeScript (`src/databricks_labs_dqx_app/ui/`) — compiled by Vite into `__dist__/`, served as static files by FastAPI
- **Task Runner**: Serverless Databricks Job (`tasks/src/`) — handles profiler, dry-run, and scheduled operations that require Spark
- **Scheduler**: In-process asyncio loop inside the FastAPI worker (`backend/services/scheduler_service.py`); single-worker via an exclusive file lock (`/tmp/.dqx_scheduler.lock`) so multi-worker deployments only run the loop once
- **Production**: Deployed as a Databricks App; FastAPI serves both API (`/api/v1/*`) and UI (`/*`)

### Authentication Model

The app uses a two-tier model — no admin-scoped REST calls are made by the app itself.

#### OBO (On-Behalf-Of) — user identity

Operations that must respect the logged-in user's permissions use the `X-Forwarded-Access-Token` header, injected automatically by Databricks when running on the platform:

- **Unity Catalog browsing** (catalogs, schemas, tables, columns)
- **Temporary view creation** — source access is checked as the user; each view grants per-view `MANAGE` to the app SP for orphan cleanup, while the task runner reads views through schema-level `SELECT` on `<prefix>_tmp` (so it can read any view in that schema; no per-view runner grant). A failed `MANAGE` grant drops the view and blocks submission; no broad built-in audience grant is used.
- **Schedule permission checks** — OBO SQL inspects grants and verifies source catalog/schema usage plus `SELECT` for the app scheduler and runner; failed runner grants block scheduling.

#### SP (Service Principal) — app identity

Operations the app owns and manages run as the app's own service principal:

- **Job submission** for profiler and dry-run tasks
- **Rules catalog CRUD** (reading and writing rules in Lakebase Postgres)
- **Schema migrations** (creating and evolving Delta analytical and Lakebase application tables)
- **App settings** (reading and writing settings in Lakebase Postgres)
- **Wheel upload** — on startup the app uploads DQX wheels to the `<prefix>.wheels` UC volume and patches the task-runner job environment

This ensures:
- Users only see data they have permission to access
- No elevated privileges required for browsing or profiling
- Internal app state managed consistently under the SP identity
- Audit logs correctly attribute actions to individual users

### Async Job Pattern (Profiler, Dry-Run, Scheduled Runs)

Profiler, dry-run, and scheduled-run operations require Spark, which cannot run inside the app process. They all submit to the same task-runner job (`task_type` discriminates between `profile`, `dryrun`, and `scheduled`):

```
Trigger (user request OR scheduler tick)
    │
    ├─ (OBO for user-initiated; SP for scheduled) Create temporary VIEW over the target table
    │         └─ View inherits the requesting principal's table permissions
    │
    ├─ (SP) Submit Databricks Job with task_type + view_fqn + config
    │        └─ tasks/src/dqx_task_runner/runner.py runs on serverless compute
    │              ├─ Reads from the temporary view
    │              ├─ Runs profiler / dry-run / scheduled checks (PySpark)
    │              ├─ Writes results, metrics, quarantine rows to Delta tables
    │              └─ Cleanup drops the view; app SP has per-view MANAGE for orphan cleanup
    │
    └─ Return run_id + job_run_id
           └─ Frontend (or scheduler) polls /status until complete
                  └─ Frontend fetches /results, /metrics, /quarantine from Delta
```

`DQX_JOB_ID` identifies which job to submit runs to (injected by DABs in production; set manually in `.env` for local dev).

### Setup Permissions and Audience

Studio's Unity Catalog storage is derived from an existing catalog and a validated prefix (default `dqx_studio`): main schema `<prefix>` (with the `wheels` volume), `<prefix>_tmp`, `<prefix>_genie`, and `<prefix>_demo`. DAB supplies catalog, prefix, and audience through the bundle (`make app-deploy STUDIO_PREFIX=... STUDIO_USER_GROUP=...`) and the setup page shows them read-only. For Marketplace, a workspace administrator (member of the admin group or workspace `admins`) submits a **setup form** with catalog, prefix, and audience group; the choices persist in Lakebase and lock once storage exists. Marketplace binds only the SQL warehouse (`CAN_MANAGE` for the app SP) and Lakebase.

The setup workflow applies the grants and ACL updates it has authority for, then **verifies** them by re-reading the state. Missing grants and grants that cannot be inspected both block readiness (fail closed); the only exception is app sharing, which produces a warning when the app ACL is unreadable. ACL updates are additive and never replace existing entries. Verified access covers: the app SP's catalog access and warehouse `CAN_MANAGE`; the audience's and a custom admin group's catalog, temporary-schema, Genie-allowlist, demo-schema, warehouse, app, Genie-space, and dashboard access; and the task-runner's least-privilege access. See the [permission matrix](DEPLOYMENT.md#permission-matrix).

The audience is a dedicated existing group, or `users` in DAB **broad mode** (workspace ACL `users` plus Unity Catalog `account users`, which is account-wide; the Marketplace form never accepts it). Members of `DQX_ADMIN_GROUP` or the workspace `admins` group may run setup; Unity Catalog cannot grant to `admins`, so UC grants for administrators go only to a custom admin account group, and with `admins` administrators must also be audience members to use OBO features. Per-user SQL entitlement, OAuth consent, and source-data privileges are checked for the active user, never for the group.

On every cold startup, setup resolves the task-runner job's actual `run_as`, grants it `USE SCHEMA` on the main and temporary schemas, schema-level `SELECT` on the temporary schema (how it reads OBO temp views), `READ VOLUME` on the wheels volume, and schema-level `SELECT` / `MODIFY` on the main schema (never `ALL PRIVILEGES`, catalog, Genie, or demo access), and verifies these plus its `USE CATALOG`. Schema grants cover current and future tables; table-specific grants alone do not satisfy setup. The runner needs no Lakebase access: run configs too large for job parameters are staged in the Delta `dq_run_configs` table in the main schema, which the main-schema `SELECT` / `MODIFY` grants already cover.

Genie audience access requires space `CAN_RUN`, Consumer access or Databricks SQL access entitlement, parent usages, and only the five approved view plus two dimension-table grants, never whole-schema `SELECT` or access to the entitlement table. Genie uses embedded compute credentials; Studio's OBO SQL workflows additionally require SQL access entitlement and warehouse `CAN_USE`. Both DAB and Marketplace use the `genie` OAuth scope. Warehouse rebind re-runs the warehouse checks and reconciles Genie compute. See [the deployment grants reference](DEPLOYMENT.md#grants-reference).

### Startup Wheel Sync

On every cold start the FastAPI lifespan (`backend/app.py`) hashes the locally bundled DQX wheels, compares against a `.wheels_hash` marker on the UC volume, uploads any changed wheels, and patches the task-runner job's `environments` dependencies to point at the new versions. This keeps the app process and the job's serverless environment version-locked.

### Routing

- **`/api/v1/*`** — FastAPI handles all API requests
- **`/*`** — FastAPI serves the compiled React SPA; TanStack Router handles client-side navigation

### Internal Storage (Hybrid Backend)

The app uses a **hybrid storage architecture**: high-volume append/analytical tables stay on Delta in Unity Catalog, while OLTP tables (rules catalog, app settings, RBAC, comments, schedule configs) live in **Lakebase Postgres** for sub-millisecond reads (see [DEPLOYMENT.md → Lakebase backend](DEPLOYMENT.md#lakebase-backend)).

For DAB, the schemas, wheels volume, and Lakebase Postgres **project** are declared as bundle resources in `databricks.yml` with `lifecycle.prevent_destroy: true`. The bundle creates them on first deploy (Marketplace setup creates the schemas and volume); `databricks bundle destroy` is blocked from dropping them. The app's `dqx_studio` Postgres schema (inside the `databricks_postgres` admin database on the Lakebase project) is created at startup and is not itself a bundle resource, but is protected transitively by the project-level guard.

> **Note:** the default SQL warehouse (`Small`) and Lakebase project (0.5–1 CU, scale-to-zero) are deliberately small — a safe, low-cost starting point rather than a tuned production config. Monitor under real load and tune (`sql_warehouse_size`, `lakebase_max_cu`) — see [DEPLOYMENT.md](DEPLOYMENT.md#variable-reference).

```
{catalog} (Unity Catalog)
 ├── <prefix>                         ← main schema (bundle- or setup-provisioned; tables managed by MigrationRunner)
 │   ├── dq_profiling_results         (Delta, always) profiler runs (suggestions in generated_rules_json)
 │   ├── dq_validation_runs           (Delta, always) dryrun + scheduled run lifecycle (1 row/run)
 │   ├── dq_quarantine_records        (Delta, always) invalid rows captured by runs
 │   ├── dq_metrics                   (Delta, always) long-format observability events
 │   │                                  (matches DQX OBSERVATION_TABLE_SCHEMA so AI/BI
 │   │                                  dashboards target the spec directly)
 │   ├── dq_app_settings              (OLTP*) key/value app configuration
 │   ├── dq_quality_rules             (OLTP*) active/approved rules
 │   ├── dq_quality_rules_history     (OLTP*) rule change audit log
 │   ├── dq_role_mappings             (OLTP*) role → workspace group mappings (RBAC)
 │   ├── dq_comments                  (OLTP*) comment threads on rules/runs
 │   ├── dq_schedule_configs          (OLTP*) per-schedule config (cron/interval, target rules)
 │   ├── dq_schedule_configs_history  (OLTP*) schedule change audit log
 │   ├── dq_schedule_runs             (OLTP*) scheduler last/next run state
 │   ├── dq_migrations                ← Delta migration version tracker
 │   └── wheels (UC volume)           ← DQX + task-runner wheels uploaded at app startup
 ├── <prefix>_tmp                     ← temp views created via OBO for profiler/dryrun jobs
 ├── <prefix>_genie                   ← derived views exposed to Genie
 └── <prefix>_demo                    ← demo source tables

Lakebase (Postgres) — required:
 dqx-studio-db (postgres project; `var.lakebase_project_id`)
 └── databricks_postgres (database)    ← always-present admin DB; no per-app logical DB provisioned
     └── dqx_studio (schema)           ← created by PgMigrationRunner on first start (DQX_LAKEBASE_SCHEMA)
         ├── dq_app_settings, dq_role_mappings, dq_quality_rules,
         ├── dq_quality_rules_history, dq_comments, dq_schedule_configs,
         ├── dq_schedule_configs_history, dq_schedule_runs
         └── dq_migrations              ← Lakebase migration version tracker
```

`(OLTP*)` = lives in **Lakebase Postgres**. Lakebase is mandatory: the previous Delta-backed application state was removed without a migration path. The split is invisible to service code: `SqlExecutor` (Delta) and `PgExecutor` (Lakebase) share an identical public surface — `execute`, `query`, `query_dicts`, `upsert`, plus the dialect helpers `q(identifier)`, `json_literal_expr(json_str)`, and `ts_text(col)` that emit dialect-correct SQL fragments.

### Role-Based Access Control

Roles (`ADMIN`, `RULE_APPROVER`, `RULE_AUTHOR`, `VIEWER`) are defined in `backend/common/authorization.py`. There is no separate `RUNNER` role — `run_rules` is granted to Admin and Author (`CAN_RUN_ROLES`). Roles resolve from Databricks workspace-group membership in `dq_role_mappings` (plus the bootstrap `DQX_ADMIN_GROUP`). Routes enforce roles via `require_role(*roles)` from `backend/dependencies.py`.

### Metrics architecture

The app aligns with the [DQX Summary Metrics spec](https://github.com/databrickslabs/dqx/blob/main/docs/dqx/docs/guide/summary_metrics.mdx). Two complementary tables back the runs/dashboard surfaces:

| Table | Cardinality | Mutability | Purpose |
|---|---|---|---|
| `dq_validation_runs` | 1 row per run | Mutable (`RUNNING → SUCCESS/FAILED/CANCELED`) | Lifecycle: status polling, cancellation, sample data, ownership gating |
| `dq_metrics` | N rows per run (one per metric) | Append-only | Trend dashboarding, alerting; matches `OBSERVATION_TABLE_SCHEMA` |

**Why not merge them?** Different cardinalities (1:N), different mutability (lifecycle vs. event), different access patterns (status polling vs. cross-table aggregation), and merging would break drop-in compatibility with future Databricks AI/BI dashboard templates targeting the spec's schema.

**How metrics are produced.** `tasks/dqx_task_runner/runner.py` attaches a `DQMetricsObserver` to the engine and triggers a single Spark action (`invalid_df.count()`). The observer collects `input_row_count`, `error_row_count`, `warning_row_count`, `valid_row_count`, a per-check `check_metrics` JSON breakdown, and any admin-defined custom-metric SQL expressions — all in one pass. Each observed metric is then written to `dq_metrics` as its own long-format row via `DQMetricsObserver.build_metrics_df`.

**Provenance.** Every metric row carries `run_id`, `run_name`, `input_location`, `quarantine_location`, `checks_location`, and `rule_set_fingerprint` (DQX-computed SHA-256). The same `rule_set_fingerprint` is stamped on `dq_validation_runs`, so dashboards can join the two tables on `(run_id, rule_set_fingerprint)` to drill from a metric back to its lifecycle row. Run-level provenance like `run_type` and `requesting_user` lives **only** on `dq_validation_runs` — never copied into `dq_metrics.user_metadata` — so the read path joins on `run_id` to surface them.

**`user_metadata` = rule labels, not run provenance.** The `user_metadata` map on each `dq_metrics` row carries the rule labels (the `user_metadata` field on each check definition), aggregated as the *intersection-with-equal-values* across every rule in the run. A key only flows through if every rule in the run carries that key with the same value (e.g. ten rules all tagged `team=finance` → `user_metadata.team = "finance"`; conflicting or missing values drop the key). This keeps the column meaningful for label-based dashboard slicing without silently merging conflicts.

**Custom metrics.** Admins manage a global SQL-expression list at `PUT /api/v1/config/custom-metrics`. Each entry must be `<aggregate_expression> as <alias>` and pass DQX's `is_sql_query_safe` denylist. Both the dryrun and scheduler paths fetch the list and forward it to the runner via `config_json["custom_metrics"]`, which threads it into `DQMetricsObserver(custom_metrics=…)`.

**Read path.** `GET /api/v1/metrics/{table_fqn}` joins `dq_metrics` to `dq_validation_runs` on `run_id` and pivots the long-format rows back into the wide-format `MetricSnapshotOut` the existing UI consumes — the chart and table components keep working unchanged. New optional fields (`check_metrics`, `custom_metrics`, `rule_set_fingerprint`, `error_row_count`, `warning_row_count`) are exposed for future UI surfaces.

**Quarantine for SQL / cross-table rules.** Cross-table SQL checks now persist their full violation set to `dq_quarantine_records` (with a `[{"name": "<check_name>", "message": "SQL check violation"}]` synthetic `errors` payload that mirrors DQX's public `dq_result_item_schema` — list-of-structs, same shape row-level checks produce — so the Pydantic `QuarantineRecordOut.errors: list[Any]` validates cleanly and the UI's per-row Errors column renders without special-casing SQL checks), capped at `_SQL_QUARANTINE_MAX_ROWS=100_000` to bound storage on runaway rules whose violation set is the entire joined dataset. The true violation count remains accurate in `dq_metrics.error_row_count` even when truncation kicks in. The runs UI's full CSV/Excel export (which reads from `dq_quarantine_records`) now works for SQL checks; previously they only had the 10-row `sample_invalid_json` fallback and 99 %+ of violations were lost. Legacy rows written with the old `{<check_name>: <message>}` dict shape are coerced to the new list shape by `quarantine._row_to_record` so historical data continues to display.

## Stack

- **Backend**: Python 3.12, FastAPI ~0.119, Pydantic 2, Databricks SDK ~0.120, Databricks SQL Connector 4.2.5 (data-plane queries), psycopg 3 (Lakebase/Postgres)
- **Frontend**: React 19, TypeScript, TanStack Router + React Query, shadcn/ui, Tailwind CSS 4, Vite 7
- **Code generation**: orval (OpenAPI → TypeScript types + React Query hooks)
