# DQX Studio

Web application for the DQX framework — a UI for authoring and managing data quality rules. Built with FastAPI (backend) and React (frontend), deployed as a Databricks App.

- **[Local Development →](DEVELOPMENT.md)** — set up your environment, run dev servers, test changes
- **[Deployment →](DEPLOYMENT.md)** — deploy to Databricks Apps via DABs

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
- **Temporary view creation** — the view inherits the user's table permissions so the job can only read data the user can access

#### SP (Service Principal) — app identity

Operations the app owns and manages run as the app's own service principal:

- **Job submission** for profiler and dry-run tasks
- **Rules catalog CRUD** (reading and writing rules — Lakebase Postgres by default, Delta in fallback mode)
- **Schema migrations** (creating and evolving both Delta and Lakebase tables)
- **App settings** (reading and writing settings — Lakebase Postgres by default)
- **Wheel upload** — on startup the app uploads DQX wheels to the UC volume and patches the task-runner job environment

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
    │              └─ Drops the temporary view (finally block)
    │
    └─ Return run_id + job_run_id
           └─ Frontend (or scheduler) polls /status until complete
                  └─ Frontend fetches /results, /metrics, /quarantine from Delta
```

`DQX_JOB_ID` identifies which job to submit runs to (injected by DABs in production; set manually in `.env` for local dev).

### Startup Wheel Sync

On every cold start the FastAPI lifespan (`backend/app.py`) hashes the locally bundled DQX wheels, compares against a `.wheels_hash` marker on the UC volume, uploads any changed wheels, and patches the task-runner job's `environments` dependencies to point at the new versions. This keeps the app process and the job's serverless environment version-locked.

### Routing

- **`/api/v1/*`** — FastAPI handles all API requests
- **`/*`** — FastAPI serves the compiled React SPA; TanStack Router handles client-side navigation

### Internal Storage (Hybrid Backend)

The app uses a **hybrid storage architecture**: high-volume append/analytical tables stay on Delta in Unity Catalog, while OLTP tables (rules catalog, app settings, RBAC, comments, schedule configs) live in **Lakebase Postgres** for sub-millisecond reads (see [DEPLOYMENT.md → Lakebase backend](DEPLOYMENT.md#lakebase-backend)).

All stateful resources — schemas, wheels volume, Lakebase instance, and Lakebase logical Postgres database — are declared as bundle resources in `databricks.yml` with `lifecycle.prevent_destroy: true`. The bundle creates them on first deploy; `databricks bundle destroy` is blocked from dropping them. For workspaces where these resources were created out-of-band, run `make app-bind` once per target to adopt them into bundle management before the first deploy (see [DEPLOYMENT.md → Migrating an existing workspace](DEPLOYMENT.md#migrating-an-existing-workspace)).

```
{catalog} (Unity Catalog)
 ├── dqx_studio                       ← main schema (provisioned out-of-band; tables managed by MigrationRunner)
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
 │   └── dq_migrations                ← Delta migration version tracker
 ├── dqx_studio_tmp                   ← temp views created via OBO for profiler/dryrun jobs
 └── wheels (UC volume)               ← DQX + task-runner wheels uploaded at app startup

Lakebase (Postgres) — when enabled (default):
 dqx-studio-lakebase (database_instance)
 └── dqx_studio (database)
     └── public (schema)               ← provisioned by PgMigrationRunner on first start
         ├── dq_app_settings, dq_role_mappings, dq_quality_rules,
         ├── dq_quality_rules_history, dq_comments, dq_schedule_configs,
         ├── dq_schedule_configs_history, dq_schedule_runs
         └── dq_migrations              ← Lakebase migration version tracker
```

`(OLTP*)` = lives in **Lakebase Postgres** when `lakebase_instance_name` is set in `databricks.yml`, otherwise in **Delta** (the `v2: Delta OLTP fallback` migration). The split is invisible to service code: `SqlExecutor` (Delta) and `PgExecutor` (Lakebase) share an identical public surface — `execute`, `query`, `query_dicts`, `upsert`, plus the dialect helpers `q(identifier)`, `json_literal_expr(json_str)`, and `ts_text(col)` that emit dialect-correct SQL fragments.

### Role-Based Access Control

Roles (`ADMIN`, `RULE_APPROVER`, `RULE_AUTHOR`, `VIEWER`, plus the orthogonal `RUNNER`) are defined in `backend/common/authorization.py` and resolved from Databricks workspace-group membership in `dq_role_mappings` (plus the bootstrap `DQX_ADMIN_GROUP`). Routes enforce roles via `require_role(*roles)` from `backend/dependencies.py`.

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

- **Backend**: Python 3.11+, FastAPI ~0.119, Pydantic 2, Databricks SDK ~0.73, Databricks Connect ~15.4
- **Frontend**: React 19, TypeScript, TanStack Router + React Query, shadcn/ui, Tailwind CSS 4, Vite 7
- **Code generation**: orval (OpenAPI → TypeScript types + React Query hooks)
