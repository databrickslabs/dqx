# DQX App — CLAUDE.md

## Purpose

The DQX App is a **UI for authoring and managing data quality rules**. It lowers the barrier from writing code (YAML/Python) to a visual, self-service experience — making rule creation accessible to non-technical users while keeping technical users efficient.

**Scope:** Creating/validating rules, AI/profiler rule generation, rule lifecycle management, internal storage, approval workflows, export to execution systems, dry-run validation.

**Not in scope:** Running rules in production, storing results, scheduling runs.

## Deployment

- Deploys as a **Databricks App** (FastAPI backend + React frontend in a single Python wheel)
- Must be publishable to **Databricks Marketplace**
- Uses **On-Behalf-Of (OBO)** authentication — all operations run as the logged-in user

## Target Personas

| Role | Description | Key Permissions |
|------|-------------|-----------------|
| Admin (Engineer) | Platform owner / data engineer | All permissions including export to checks storage, approval |
| Data Steward (Business User) | Defines and maintains rules | Create/edit rules, submit for approval, AI/profiler generation |
| Viewer | Observability only | View rules |

Role-based access is defined but **not yet enforced** — currently all users are ADMIN (see `backend/common/authorization.py`).

## Core User Journeys

1. **Business user generates rules via natural language** — select table → enter description → review AI candidates → optional dry-run → save
2. **Business user adjusts existing rules** — load → edit → optional dry-run → save (creates new version + approval request)
3. **Engineer reviews and approves rules** — review GUI/YAML → optional dry-run → configure checks storage → approve → export to Delta table
4. **Engineer generates rules via profiler** — select table → configure sampling → run profiler → review candidates → save
5. **User browses and discovers rules** — filter by table/domain/owner/status → view versions → compare → import/export

## Internal Storage

App uses a dedicated catalog selected at install time:

```
{user_catalog}
 └── dqx_app
     ├── dq_profiling_results       ← Data profile statistics
     ├── dq_profiling_suggestions   ← AI-generated rule suggestions
     ├── dq_quality_rules           ← Active/approved rules
     ├── dq_validation_runs         ← All execution runs
     └── dq_app_settings            ← App configuration
```

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
