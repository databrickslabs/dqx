# Local Development

## Prerequisites

- **Python 3.11+**
- **Node.js 18+** and **yarn**
- **uv** — Python package manager
- **Databricks CLI** — `pip install databricks-cli`
- Access to a Databricks workspace

## Command Reference

`apx` is the project's development CLI, installed as part of the app's Python dependencies. Every `uv run apx` command has a root-level `make` equivalent that also handles environment setup. **Prefer `make` from the project root.**

| `make` (from root) | `apx` equivalent (from `app/`) | What it does |
|---|---|---|
| `make app-install` | `yarn install --frozen-lockfile` | Install JS dependencies |
| `make app-build` | `uv run apx build` | Compile UI, generate OpenAPI schema, package wheels |
| `make app-start-dev` | `uv run apx dev start` | Build then start all dev servers |
| `make app-stop-dev` | `uv run apx dev stop` | Stop all dev servers |
| `make app-check` | `uv run apx dev check` | TypeScript + Python type-check and lint |
| `make fmt` | — | Format Python (run from root before committing) |
| `make test` | — | Unit tests |
| `make integration` | — | Integration tests (requires live workspace) |

> Lock files (`yarn.lock` and `uv.lock`) must be committed to ensure reproducible builds.

## 1. Configure Authentication

**Option A — Databricks CLI (recommended):**
```bash
databricks auth login --host https://your-workspace.cloud.databricks.com
```

**Option B — `.env` file (useful when working with multiple profiles):**

Create a file at `app/.env` (git-ignored) with the variables below, filling in your own values:

```bash
DATABRICKS_CONFIG_PROFILE=<your-profile>    # matches a profile in ~/.databrickscfg
DATABRICKS_WAREHOUSE_ID=<your-warehouse-id>
DQX_CATALOG=dqx                             # Unity Catalog catalog name
DQX_SCHEMA=dqx_app                          # schema inside the catalog
DQX_JOB_ID=<task-runner-job-id>             # required for profiler/dry-run
DQX_WHEELS_VOLUME=/Volumes/dqx/dqx_app/wheels  # UC volume path; auto-set by DABs in production
DQX_ADMIN_GROUP=admins                      # workspace group granted bootstrap Admin access
```

`DQX_JOB_ID` and `DQX_WHEELS_VOLUME` are injected automatically when deployed via DABs. For local dev, set them manually if you want profiler and dry-run to work.

## 2. Install Dependencies

From the **project root**:
```bash
make app-install   # JS dependencies (yarn)
cd app && uv sync  # Python dependencies
```

Or from the `app/` directory:
```bash
uv sync
yarn install --frozen-lockfile
```

## 3. Build

The React frontend must be compiled before the backend can serve it.

From the **project root**:
```bash
make app-build
```

Or from the `app/` directory:
```bash
uv run apx build
```

This generates the OpenAPI schema, compiles the React/TypeScript UI into `__dist__/`, and packages everything into a wheel.

## 4. Start Dev Servers

From the **project root**:
```bash
make app-start-dev   # builds first, then starts all servers
```

Or from the `app/` directory:
```bash
uv run apx dev start
```

This starts the FastAPI backend, Vite HMR frontend, and OpenAPI schema watcher as background processes.

Access the app at:
- **UI**: http://localhost:9001
- **API**: http://localhost:9001/api
- **OpenAPI docs**: http://localhost:9001/docs

## Monitoring & Logs

```bash
uv run apx dev logs          # view all logs
uv run apx dev logs -f       # stream logs in real time
uv run apx dev status        # check server status
make app-stop-dev            # stop all servers
```

## Development Workflow

**Adding a new API endpoint:**
1. Define the endpoint in `backend/routes/v1/` with a Pydantic response model and `operation_id`
2. Add request/response models to `backend/models.py` if needed
3. Run `make app-build` to regenerate the OpenAPI schema and `ui/lib/api.ts`
4. Use the generated React Query hooks in your components

**Making UI changes:**
1. Edit components in `ui/components/` or routes in `ui/routes/`
2. Vite HMR reloads the browser automatically — no manual refresh needed
3. Run `make app-build` after backend changes to update `ui/lib/api.ts`

## Code Quality

```bash
make app-check   # TypeScript + Python type-check and lint
make fmt          # format Python (run before every commit)
```

## Testing

```bash
make test          # unit tests
make integration   # integration tests (requires live workspace)
```

## Permissions

The profiler creates a temporary view using your OBO token and submits a Databricks Job as the service principal. You need `USE CATALOG` + `USE SCHEMA` + `SELECT` on the tables you want to profile, plus `DQX_JOB_ID` set to a deployed task-runner job. See [README.md](README.md) for the full authentication model.

If the wheel upload fails locally with a `403`, grant your user write access:
```bash
databricks volumes grant <catalog>.dqx_app.wheels WRITE_VOLUME --user <your-email> -p <your-profile>
```

## Troubleshooting

**Port already in use:**
```bash
lsof -i :9001
make app-stop-dev
```

**Missing static assets (`__dist__` does not exist):**
```bash
make app-build
```

**TypeScript type errors in the UI:**
```bash
make app-build   # regenerates ui/lib/api.ts from the current OpenAPI spec
```

**Build artifacts are stale:**
```bash
rm -rf .build .apx __dist__
make app-build
```

**uv hangs:**
```bash
uv sync -v         # verbose output to diagnose
rm -rf .venv/.lock # remove stale lock if needed
```

**OBO token missing locally:**
The `X-Forwarded-Access-Token` header is only injected by Databricks Apps. When running locally the backend falls back to the SDK default auth for OBO operations. Ensure you've authenticated via `databricks auth login` or configured `.env`.
