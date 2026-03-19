# Backend — CLAUDE.md

## Overview

FastAPI REST API serving as the DQX App backend. Deployed as a **Databricks App** with On-Behalf-Of (OBO) authentication. Serves both API endpoints (`/api/v1/*`) and the compiled React frontend as static files.

## Architecture

```
backend/
├── app.py                 # FastAPI app factory, lifespan, static file mount
├── config.py              # AppConfig (Pydantic BaseSettings, DQX_ env prefix)
├── dependencies.py        # FastAPI Depends() — OBO auth, Spark, DQEngine, services
├── models.py              # Pydantic request/response models
├── settings.py            # SettingsManager — per-user prefs in ~/.dqx/app.yml
├── runtime.py             # Runtime singleton (lazy WorkspaceClient)
├── logger.py              # Custom logging formatter
├── utils.py               # SPA fallback (404 → index.html)
├── routes/
│   └── v1/
│       ├── me.py          # GET /version, GET /current-user
│       ├── settings.py    # GET/POST /settings (install folder)
│       ├── config.py      # Config + RunConfig + Checks CRUD
│       ├── discovery.py   # Unity Catalog browsing (catalogs/schemas/tables/columns)
│       └── generate.py    # POST /ai/generate-checks
├── services/
│   └── discovery.py       # DiscoveryService (async UC listing via asyncio.to_thread)
└── common/
    ├── authorization.py   # UserRole enum, role-based access (stubbed — all users ADMIN)
    ├── authentication/
    │   └── sql.py         # SQLAuthentication (bearer token resolution)
    └── connectors/
        └── sql.py         # SQLConnector (SQL Warehouse query execution)
```

## Key Patterns

### OBO Authentication

All workspace operations use the user's identity via `X-Forwarded-Access-Token` header. The dependency chain:

```
get_obo_ws()           → WorkspaceClient(token=header_token, auth_type="pat")
  ├─ get_spark()       → SparkSession with OBO token (strips OAuth env vars)
  ├─ get_engine()      → DQEngine(obo_ws, spark)
  ├─ get_generator()   → DQGenerator(obo_ws, spark, LLMModelConfig)
  ├─ get_discovery_service() → DiscoveryService(obo_ws)
  └─ get_config_serializer() → ConfigSerializer(obo_ws)
```

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

- The `common/authorization.py` role system is **stubbed** — all users are currently ADMIN. The permission matrix (Admin/Data Steward/Viewer) is planned but not yet enforced.
- OAuth env vars (`DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`) are temporarily stripped during Spark session creation to avoid multi-auth conflicts. See `_without_oauth_env_vars()` in `dependencies.py`.
- The SPA fallback in `utils.py` serves `index.html` for any non-API 404, enabling client-side routing.
