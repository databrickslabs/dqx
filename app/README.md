# DQX App

Web application for the DQX framework — a UI for authoring and managing data quality rules. Built with FastAPI (backend) and React (frontend), deployed as a Databricks App.

- **[Local Development →](DEVELOPMENT.md)** — set up your environment, run dev servers, test changes
- **[Deployment →](DEPLOYMENT.md)** — deploy to Databricks Apps via DABs

## Architecture

- **Backend**: FastAPI (`src/databricks_labs_dqx_app/backend/`) — REST API under `/api`, no Spark in the app process
- **Frontend**: React + TypeScript (`src/databricks_labs_dqx_app/ui/`) — compiled by Vite into `__dist__/`, served as static files by FastAPI
- **Task Runner**: Serverless Databricks Job (`tasks/src/`) — handles profiler and dry-run operations that require Spark
- **Production**: Deployed as a Databricks App; FastAPI serves both API (`/api/*`) and UI (`/*`)

### Authentication Model

The app uses a two-tier model — no admin-scoped REST calls are made by the app itself.

#### OBO (On-Behalf-Of) — user identity

Operations that must respect the logged-in user's permissions use the `X-Forwarded-Access-Token` header, injected automatically by Databricks when running on the platform:

- **Unity Catalog browsing** (catalogs, schemas, tables, columns)
- **Temporary view creation** — the view inherits the user's table permissions so the job can only read data the user can access

#### SP (Service Principal) — app identity

Operations the app owns and manages run as the app's own service principal:

- **Job submission** for profiler and dry-run tasks
- **Rules catalog CRUD** (reading and writing the rules Delta table)
- **Schema migrations** (creating and evolving Delta tables)
- **App settings** (reading and writing settings from the Delta table)
- **Wheel upload** — on startup the app uploads DQX wheels to the UC volume and patches the task-runner job environment

This ensures:
- Users only see data they have permission to access
- No elevated privileges required for browsing or profiling
- Internal app state managed consistently under the SP identity
- Audit logs correctly attribute actions to individual users

### Async Job Pattern (Profiler & Dry-Run)

Profiler and dry-run operations require Spark, which cannot run inside the app process:

```
User request
    │
    ├─ (OBO) Create temporary VIEW over the target table
    │         └─ View inherits user's table permissions
    │
    ├─ (SP) Submit Databricks Job with view_fqn + config
    │        └─ dqx_task_runner.py runs on serverless compute
    │              ├─ Reads from the temporary view
    │              ├─ Runs profiler / dry-run (PySpark)
    │              ├─ Writes results to Delta table
    │              └─ Drops the temporary view (finally block)
    │
    └─ Return run_id + job_run_id to the frontend
           └─ Frontend polls /status until complete
                  └─ Frontend fetches /results from Delta
```

`DQX_JOB_ID` identifies which job to submit runs to (injected by DABs in production; set manually in `.env` for local dev).

### Routing

- **`/api/*`** — FastAPI handles all API requests
- **`/*`** — FastAPI serves the compiled React SPA; TanStack Router handles client-side navigation

### Internal Storage

The app uses a dedicated catalog (selected at install time):

```
{catalog}
 └── dqx_app
     ├── dq_profiling_results       ← profiler run results
     ├── dq_profiling_suggestions   ← AI-generated rule suggestions
     ├── dq_quality_rules           ← active/approved rules
     ├── dq_validation_runs         ← dry-run execution history
     └── dq_app_settings            ← app configuration and settings
```

## Stack

- **Backend**: Python 3.11+, FastAPI ~0.119, Pydantic 2, Databricks SDK ~0.73, Databricks Connect ~15.4
- **Frontend**: React 19, TypeScript, TanStack Router + React Query, shadcn/ui, Tailwind CSS 4, Vite 7
- **Code generation**: orval (OpenAPI → TypeScript types + React Query hooks)
