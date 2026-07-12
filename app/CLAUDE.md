# DQX Studio — CLAUDE.md

## Purpose

The DQX Studio is a **UI for authoring and managing data quality rules**. It lowers the barrier from writing code (YAML/Python) to a visual, self-service experience — making rule creation accessible to non-technical users while keeping technical users efficient.

**Scope:** Creating/validating rules, AI/profiler rule generation, rule lifecycle management, internal storage, approval workflows, export to execution systems, dry-run validation, scheduled in-app rule execution, run history + quality metrics + quarantine review.

**Not in scope:** Running rules as part of customer production data pipelines (the app's runs target dev/UAT data and write results to the app's own catalog).

## Deployment

- Deploys as a **Databricks App** (FastAPI backend + React frontend in a single Python wheel)
- Must be publishable to **Databricks Marketplace**
- Uses a **hybrid auth model**: data-plane reads (catalog/schema/table browse, dry-run preview, query execution against the user's tables) run as the logged-in user via **On-Behalf-Of (OBO)** tokens so Unity Catalog perms are enforced. Control-plane writes (rules CRUD, RBAC mappings, migrations, wheel sync, task-runner job submission) run as the app's **service principal** so they don't require every end user to hold those workspace permissions. See `README.md` for the full split.

## Target Personas

| Role | Description | Key Permissions |
|------|-------------|-----------------|
| `ADMIN` | Platform owner / data engineer | All permissions including configure storage, manage roles, approve rules, run rules |
| `RULE_APPROVER` | Reviews and approves rule submissions | View/create/edit/submit rules, approve/reject, configure storage, view quarantine |
| `RULE_AUTHOR` | Defines and maintains rules (data steward) | View/create/edit/submit rules, AI/profiler generation |
| `VIEWER` | Observability only | View rules |
| `RUNNER` *(orthogonal)* | Operator who triggers manual or scheduled runs | `run_rules` only — does not affect primary role; admins inherit it implicitly |

RBAC is enforced — routes use `require_role(*roles)` from `backend/dependencies.py` and roles resolve from Databricks workspace-group membership in `dq_role_mappings` (plus the bootstrap `DQX_ADMIN_GROUP`). See `backend/common/authorization.py`.

## Core User Journeys

1. **Business user generates rules via natural language** — select table → enter description → review AI candidates → optional dry-run → save
2. **Business user adjusts existing rules** — load → edit → optional dry-run → save (creates new version + approval request)
3. **Engineer reviews and approves rules** — review GUI/YAML → optional dry-run → configure checks storage → approve → export to Delta table
4. **Engineer generates rules via profiler** — select table → configure sampling → run profiler → review candidates → save
5. **Engineer pins a schema contract** — single-table editor → pick target table → add a `has_valid_schema` check → expected schema as DDL or reference table → strict/compatible mode → dry-run → save
6. **Data product owner imports rules** — Import rules page → pick **From DQX YAML** or **From data contract** tab → review preview → save drafts
7. **User browses and discovers rules** — filter by table/domain/owner/status → view versions → compare → import/export

### Synthetic FQN convention (`__sql_check__/<name>`)

Per-table rules carry a real `table_fqn`. **Cross-table SQL checks** are the only rules without a single home table, so they use the synthetic prefix `__sql_check__/<name>` and bucket under the **Cross-table rules** group in the UI catalog and edit-router. The runner reads their query body from `arguments.sql_query` and builds the input view from it (SQL fast-path, `is_sql_check=True`).

Reference checks such as `has_valid_schema` and `foreign_key` are **per-table** — they carry a real `table_fqn`, are authored/edited in the single-table editor, group under their target table, and run through the standard row-level engine via the normal `create_view(table_fqn)` path. They are *not* synthetic and need no special dispatch.

The cross-table dispatch lives in `backend/routes/v1/dryrun.py` and `backend/services/scheduler_service.py`. If you add another table-less rule kind, follow the synthetic-FQN convention and update both dispatchers in lock-step.

## Internal Storage

App uses a **hybrid backend** — analytical/append tables in Delta, OLTP
tables in Lakebase Postgres. Both backends are managed by their own
migration runner in `backend/migrations/`. Schemas, volume, and Lakebase
instance are declared as bundle resources in `databricks.yml` with
`lifecycle.prevent_destroy: true`, so `databricks bundle destroy` cannot
drop them — see "Bundle conventions" below. The app's `dqx_studio`
Postgres schema (inside the `databricks_postgres` admin database on the
Lakebase instance) is created at startup, not provisioned by the bundle,
but is protected transitively by the instance-level guard.

```
{user_catalog}
 ├── dqx_studio                       ← main schema (SP-managed)
 │   ├── dq_profiling_results         (Delta) profiler run results
 │   ├── dq_validation_runs           (Delta) dryrun + scheduled run history
 │   ├── dq_quarantine_records        (Delta) invalid rows captured by runs
 │   ├── dq_metrics                   (Delta) per-run quality metrics for trend tracking
 │   ├── dq_app_settings              (OLTP*) key/value app configuration
 │   ├── dq_quality_rules             (OLTP*) active/approved rules
 │   ├── dq_quality_rules_history     (OLTP*) rule change audit log
 │   ├── dq_role_mappings             (OLTP*) role → workspace group mappings (RBAC)
 │   ├── dq_comments                  (OLTP*) comment threads on rules/runs
 │   ├── dq_schedule_configs          (OLTP*) per-schedule config (cron/interval, target rules)
 │   ├── dq_schedule_configs_history  (OLTP*) schedule config change audit log
 │   ├── dq_schedule_runs             (OLTP*) scheduler last/next run state (survives restarts)
 │   └── dq_migrations                (Delta) Delta migration version tracker
 ├── dqx_studio_tmp                   ← temp views created via OBO for profiler/dryrun jobs
 └── dqx_studio.wheels (volume)       ← DQX + task-runner wheels uploaded at app startup

Lakebase project (when enabled, default project = `dqx-studio-db`, connected via its branch 'primary' endpoint — DQX_LAKEBASE_ENDPOINT):
 └── databricks_postgres              (database — always-present admin DB; no per-app DB provisioned)
     └── dqx_studio                   (schema — created by PgMigrationRunner on first start; configurable via DQX_LAKEBASE_SCHEMA)
         ├── dq_app_settings, dq_role_mappings, dq_quality_rules,
         │   dq_quality_rules_history, dq_comments, dq_schedule_configs,
         │   dq_schedule_configs_history, dq_schedule_runs
         └── dq_migrations             (Postgres migration version tracker)
```

`(OLTP*)` = lives in **Lakebase Postgres** when
`lakebase_endpoint` is set, otherwise **Delta** (the
`v2: Delta OLTP fallback` migration).

## Key Decisions

- **No config.yaml** — all settings stored in Delta or Lakebase tables.
- **Dedicated catalog** — user selects at install; `dqx_studio` and `dqx_studio_tmp` schemas are declared as bundle resources and created by `databricks bundle deploy`.
- **Hybrid storage** — high-volume append tables in Delta; transactional/low-latency tables in Lakebase Postgres.
- **Rule promotion** — export rules then deploy separately to prod; or save directly to prod checks table.
- **Target environments** — Dev, UAT/QA (prod-like data); app is not intended for production rule execution.

## Bundle conventions

Stateful resources declared in `databricks.yml`:

- `resources.schemas.main_schema` — `dqx_studio` schema
- `resources.schemas.tmp_schema` — `dqx_studio_tmp` schema
- `resources.volumes.wheels` — wheels volume
- `resources.postgres_projects.dqx_studio` — Lakebase Postgres project (autoscaling, scale-to-zero)

Each carries `lifecycle.prevent_destroy: true` (Databricks CLI 0.268+), which blocks `databricks bundle destroy` and any deploy that would force-replace the resource. To intentionally tear something down: drop the flag, `databricks bundle deployment unbind <key> -t <target>`, then destroy.

The app connects to the always-present `databricks_postgres` admin database on the Lakebase project (set as the default `lakebase_database_name`) via the `DQX_LAKEBASE_ENDPOINT` endpoint path and creates its own `dqx_studio` Postgres schema there on first start. The app SP's Postgres role (`resources.postgres_roles.app_sp`, a `DATABRICKS_SUPERUSER` member) grants the CREATE-schema privilege. We deliberately do not use `database_catalogs` because it also creates a Unity Catalog catalog and therefore requires `CREATE CATALOG` on the metastore — a permission most app deployers don't hold.

UC privileges for the app SP and task-runner SP are declared **natively** as `grants:` on the schema/volume resources (using `${resources.apps.dqx-studio.service_principal_client_id}` and `${var.dqx_service_principal_application_id}`), so `databricks bundle deploy` applies them — there is no post-deploy grant script. The one exception is `USE CATALOG` on the pre-existing (user-selected) catalog, which the bundle can't grant because it doesn't manage the catalog; grant it once per catalog as a documented prerequisite (see `DEPLOYMENT.md`).

## Architecture

```
app/
├── CLAUDE.md                  ← You are here (product context)
├── DESIGN.md                  ← Server-Driven UI (SDUI) design doc (planned, not yet implemented)
├── pyproject.toml             ← Python package config (FastAPI, Pydantic, SDK deps)
├── databricks.yml             ← Databricks Asset Bundle config
└── src/databricks_labs_dqx_app/
    ├── backend/               ← FastAPI REST API (see backend/CLAUDE.md)
    │   ├── routes/v1/         ← Versioned API routes
    │   ├── services/          ← Business logic services
    │   ├── common/            ← Auth, authorization, connectors
    │   └── ...
    └── ui/                    ← React SPA (see ui/CLAUDE.md)
        ├── routes/            ← File-based routing (TanStack Router)
        ├── components/        ← shadcn/ui + app components
        ├── lib/api.ts         ← Auto-generated API hooks (orval)
        └── ...
```

## Stack

- **Backend:** Python 3.11+, FastAPI, Pydantic 2, Databricks SDK, Databricks SQL Connector, psycopg (Lakebase/Postgres), DQX library
- **Frontend:** React 19, TypeScript, TanStack Router + React Query, shadcn/ui, Tailwind CSS 4, Vite 7
- **Code generation:** orval (OpenAPI → TypeScript types + React Query hooks)

## References

- [Mini-PRD](https://docs.google.com/document/d/1oLeL1SuhBq66cx3lg5rAuN652Ol9HhpWsc6JZgTkvHU/edit)
- [Architecture diagram (Excalidraw)](https://drive.google.com/file/d/1oQ61cDDZcLwOyI9iIR47PsOQZLVnsdMD/view)
