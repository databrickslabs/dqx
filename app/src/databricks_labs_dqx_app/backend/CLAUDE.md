# Backend — CLAUDE.md

## Overview

FastAPI REST API serving as the DQX Studio backend. Deployed as a **Databricks App** with On-Behalf-Of (OBO) authentication. Serves both API endpoints (`/api/v1/*`) and the compiled React frontend as static files.

## Architecture

```
backend/
├── app.py                 # FastAPI app factory, lifespan, static file mount
├── cache.py               # CacheFactory — async in-memory TTL cache + @cached decorator
├── config.py              # AppConfig (Pydantic BaseSettings, DQX_ env prefix)
├── dependencies.py        # FastAPI Depends() — OBO/SP auth, RBAC, services
├── migrations/            # MigrationRunner — versioned DDL applied at startup
├── models.py              # Pydantic request/response models
├── run_status_manager.py  # Helpers for reading/updating dq_validation_runs status
├── settings.py            # SettingsManager — per-user prefs in ~/.dqx/app.yml
├── sql_executor.py        # SqlExecutor — Databricks Statement Execution API wrapper
├── sql_utils.py           # Shared SQL helpers: escape_sql_string, validate_fqn, quote_fqn
├── runtime.py             # Runtime singleton (lazy WorkspaceClient)
├── logger.py              # Custom logging formatter
├── spa_static.py          # SPA static file handler (asset-extension allowlist for SPA fallback)
├── routes/
│   └── v1/
│       ├── comments.py    # Comment threads on rules/runs
│       ├── config.py      # Workspace config + RunConfig CRUD
│       ├── discovery.py   # Unity Catalog browsing (catalogs/schemas/tables/columns)
│       ├── dryrun.py      # Submit/poll/cancel dry-run jobs, list validation runs
│       ├── generate.py    # POST /ai/generate-checks
│       ├── import_rules.py # POST /validate-checks
│       ├── me.py          # /version, /current-user, /current-user/role
│       ├── metrics.py     # Quality metrics over time
│       ├── profiler.py    # Submit/poll/cancel profiler jobs
│       ├── quarantine.py  # List/export quarantine records (RULE_APPROVER+)
│       ├── roles.py       # Manage role-to-group mappings (ADMIN only)
│       ├── rules.py       # Rules CRUD + status transitions (submit/approve/reject)
│       ├── schedules.py   # Schedule config CRUD
│       └── settings.py    # GET/POST /settings (per-user install folder)
├── services/              # Business logic layer (one class per concern)
│   ├── ai_rules_service.py
│   ├── app_settings_service.py
│   ├── comments_service.py
│   ├── discovery.py
│   ├── job_service.py
│   ├── role_service.py
│   ├── rules_catalog_service.py
│   ├── schedule_config_service.py
│   ├── scheduler_service.py    # Background scheduler (asyncio task, file-locked to one worker)
│   └── view_service.py         # Temp-view lifecycle for dry-run / profiler
└── common/
    ├── authorization.py   # UserRole enum + permission matrix (real RBAC)
    ├── authentication/
    │   └── sql.py         # SQLAuthentication (bearer token resolution)
    └── connectors/
        └── sql.py         # SQLConnector (SQL Warehouse query execution)
```

## Key Patterns

### OBO + SP Authentication

User-facing operations run as the calling user via `X-Forwarded-Access-Token` (OBO).
Operations that need elevated permissions (catalog DDL, scheduler, migrations, job
submission) run as the app's service principal. Dependencies expose both:

```
get_obo_ws()              → WorkspaceClient(token=header_token, auth_type="pat")
  ├─ get_obo_sql_executor() → SqlExecutor on tmp schema (user permissions)
  ├─ get_view_service()     → user creates/drops their own temp views
  ├─ get_discovery_service()→ user-scoped UC browsing
  └─ get_user_catalog_names() → cached per token-hash, drives catalog filtering

get_sp_ws()               → WorkspaceClient() (SP credentials, cached 45 min)
  ├─ get_sp_sql_executor()  → SqlExecutor on main schema
  ├─ get_job_service()      → submits/polls task-runner job
  ├─ get_rules_catalog_service()
  ├─ get_role_service()
  └─ get_app_settings_service()
```

User identity comes from `X-Forwarded-Email`; the OBO `me()` SCIM call is the
fallback for local dev. `X-Forwarded-User` is **not** trusted (spoofable by upstream
proxies).

### Role-Based Access Control (RBAC)

Defined in `common/authorization.py`:

| Role | Permissions |
|------|-------------|
| `ADMIN` | All actions, including configure storage, manage roles, approve rules |
| `RULE_APPROVER` | Create/edit rules, approve/reject submissions, configure storage, view quarantine |
| `RULE_AUTHOR` | Create/edit/submit rules, generate via AI/profiler |
| `VIEWER` | Read-only |

Roles resolve from Databricks workspace group membership in `dq_role_mappings`
(plus the bootstrap `DQX_ADMIN_GROUP`). `get_user_role` (in `dependencies.py`)
performs resolution and degrades gracefully to `VIEWER` if SCIM/role-mapping is
transiently unavailable.

Routes enforce roles via `require_role(*roles)` either on the router
(`APIRouter(dependencies=[require_role(...)])`) or per-route (`@router.get(..., dependencies=[require_role(...)])`).
Handler-level ownership checks (e.g. `cancel_dry_run`) supplement role guards
when a role alone isn't enough.

### Dependency Injection

All route handlers receive dependencies via `Annotated[T, Depends(get_T)]`. Dependencies are created per-request. Never instantiate services inline in route handlers.

### Async Pattern

Databricks SDK calls are synchronous. Wrap them with `asyncio.to_thread()` in service methods to avoid blocking the event loop. See `services/discovery.py` for the pattern.

### Route Conventions

```python
@router.get("/path", response_model=ResponseModel, operation_id="camelCaseId")
async def handler(dep: Annotated[Service, Depends(get_service)]) -> ResponseModel:
    ...
```

- All routes use Pydantic response models (type-safe serialization)
- `operation_id` is camelCase — orval uses it to generate frontend hook names
- Routes raise `HTTPException` with 401/403/404/400/500 as appropriate

### Config Serialization

Use `ConfigSerializer` from the DQX library to load/save workspace configs. Never use `dataclasses.asdict()`.

## Stack

- **FastAPI** ~0.119 (ASGI)
- **Pydantic** 2.11 (validation, settings, response models)
- **Databricks SDK** ~0.73 (workspace API)
- **Databricks Connect** ~15.4 (Spark sessions)
- **DQX library** (imported as `databricks-labs-dqx[llm]`)
- **Uvicorn** (ASGI server)
- **Python 3.11+**

## Commands

```bash
# From app/ directory
uv sync                    # Install dependencies
uv run uvicorn databricks_labs_dqx_app.backend.app:app --reload  # Dev server
```

## Adding a New Route

1. Create `routes/v1/<name>.py` with an `APIRouter(prefix="/<name>", tags=["<Name>"])`
2. Add route handlers with Pydantic response models and `operation_id`
3. Include the router in `routes/v1/__init__.py`
4. Add request/response models to `models.py`
5. Add any new dependencies to `dependencies.py`
6. Regenerate the OpenAPI spec so orval can update frontend hooks

## Adding a New Service

1. Create `services/<name>.py` with a class that accepts injected dependencies
2. Add a `get_<name>()` dependency function in `dependencies.py`
3. Wrap sync SDK calls with `asyncio.to_thread()` for async routes

## Important Notes

- **SQL safety:** all interpolated identifiers must pass `validate_fqn` and be wrapped with `quote_fqn` from `sql_utils.py`. All string literals must be escaped with `escape_sql_string` (ANSI doubled quotes — never backslash). User-supplied SQL bodies must pass `is_sql_query_safe()` from the DQX library and raise `UnsafeSqlQueryError` on rejection.
- **Migration startup:** SP authentication and `MigrationRunner.run_all()` are *required* — failure aborts the lifespan and the app refuses to start. Best-effort startup steps (tmp-schema creation, USE CATALOG grant, wheel sync) log warnings and continue.
- **Scheduler:** runs in-process as an asyncio task, gated by an exclusive file lock (`/tmp/.dqx_scheduler.lock`) so only one uvicorn worker drives it. Disable with `DQX_SCHEDULER_DISABLED=1`.
- **Caches:** `app_cache` (`cache.py`) is per-process in-memory with TTL. SP `WorkspaceClient`, OBO `WorkspaceClient`, and per-user catalog list are all cached. Use the `MISS` sentinel — never `is None` — to detect cache absence.
- **SPA static files:** `spa_static.py` falls through to `index.html` only for non-asset paths (positive allowlist of asset extensions), so SPA routes containing dots still work.
