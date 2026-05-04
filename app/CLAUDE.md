# DQX App — CLAUDE.md

## Purpose

The DQX App is a **UI for authoring and managing data quality rules**. It lowers the barrier from writing code (YAML/Python) to a visual, self-service experience — making rule creation accessible to non-technical users while keeping technical users efficient.

**Scope:** Creating/validating rules, AI/profiler rule generation, rule lifecycle management, internal storage, approval workflows, export to execution systems, dry-run validation, scheduled in-app rule execution, run history + quality metrics + quarantine review.

**Not in scope:** Running rules as part of customer production data pipelines (the app's runs target dev/UAT data and write results to the app's own catalog).

## Deployment

- Deploys as a **Databricks App** (FastAPI backend + React frontend in a single Python wheel)
- Must be publishable to **Databricks Marketplace**
- Uses **On-Behalf-Of (OBO)** authentication — all operations run as the logged-in user

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
5. **User browses and discovers rules** — filter by table/domain/owner/status → view versions → compare → import/export

## Internal Storage

App uses a dedicated catalog selected at install time, with two schemas (managed by `MigrationRunner` in `backend/migrations/`):

```
{user_catalog}
 ├── dqx_app                          ← main schema (SP-managed)
 │   ├── dq_app_settings              ← key/value app configuration
 │   ├── dq_quality_rules             ← active/approved rules
 │   ├── dq_quality_rules_history     ← rule change audit log
 │   ├── dq_role_mappings             ← role → workspace group mappings (RBAC)
 │   ├── dq_comments                  ← comment threads on rules/runs
 │   ├── dq_profiling_results         ← profiler run results (suggestions in generated_rules_json)
 │   ├── dq_validation_runs           ← dryrun + scheduled run history
 │   ├── dq_quarantine_records        ← invalid rows captured by runs
 │   ├── dq_metrics                   ← per-run quality metrics for trend tracking
 │   ├── dq_schedule_configs          ← per-schedule config (cron/interval, target rules)
 │   ├── dq_schedule_configs_history  ← schedule config change audit log
 │   ├── dq_schedule_runs             ← scheduler last/next run state (survives restarts)
 │   └── dq_migrations                ← migration version tracker
 └── dqx_app_tmp                      ← temp views created via OBO for profiler/dryrun jobs
```

A separate UC volume (`{catalog}.dqx_app.wheels` by default) holds the DQX + task-runner wheels uploaded at app startup.

## Key Decisions

- **No config.yaml** — all settings stored in Delta tables
- **Dedicated catalog** — user selects at install, `dqx_app` schema created by the app
- **Rule promotion** — export rules then deploy separately to prod; or save directly to prod checks table
- **Internal storage** — Delta table (preferred); Lakebase also supported as option at install time
- **Target environments** — Dev, UAT/QA (prod-like data); app is not intended for production rule execution

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

- **Backend:** Python 3.11+, FastAPI, Pydantic 2, Databricks SDK, Databricks Connect, DQX library
- **Frontend:** React 19, TypeScript, TanStack Router + React Query, shadcn/ui, Tailwind CSS 4, Vite 7
- **Code generation:** orval (OpenAPI → TypeScript types + React Query hooks)

## References

- [Mini-PRD](https://docs.google.com/document/d/1oLeL1SuhBq66cx3lg5rAuN652Ol9HhpWsc6JZgTkvHU/edit)
- [Architecture diagram (Excalidraw)](https://drive.google.com/file/d/1oQ61cDDZcLwOyI9iIR47PsOQZLVnsdMD/view)
