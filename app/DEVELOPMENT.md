# Local Development

## Prerequisites

- **Python 3.12+**
- **Node.js 18+** (provides `npm`) — install via `brew install node`, [nvm](https://github.com/nvm-sh/nvm), or [nodejs.org](https://nodejs.org/en/download) — and **yarn** classic v1 (`npm install -g yarn`, used for the committed `app/yarn.lock`; `bun.lock` / `package-lock.json` are gitignored)
- **bun** — used by `make app-check` to run `tsc -b` (TypeScript incremental compile). Install via `curl -fsSL https://bun.sh/install | bash` or `brew install oven-sh/bun/bun`.
- **uv** — Python package manager
- **Databricks CLI** v0.268+ — install per the [official guide](https://docs.databricks.com/aws/en/dev-tools/cli/install) (the legacy `databricks-cli` PyPI package is unrelated and not supported). Verify with `databricks --version`.
- Access to a Databricks workspace

## Command Reference

Every workflow runs through `make` targets at the project root. The
underlying invocations are pure-Python orchestrators (`scripts/build_app.py`,
`scripts/dev.py`) plus `bun` / `uv` / `node_modules/.bin/*` — no
project-specific CLI required. **Prefer `make` from the project root.**

| `make` (from root) | What it does |
|---|---|
| `make app-install` | Install JS dependencies (yarn) |
| `make app-build` | Compile UI, generate OpenAPI schema, package wheels (runs `app/scripts/build_app.py`) |
| `make app-start-dev` | Build then start uvicorn + vite via `app/scripts/dev.py` (foreground; Ctrl+C to stop) |
| `make app-stop-dev` | Stop dev servers started in another shell (`pkill`-based) |
| `make app-check` | TypeScript (`tsc -b`) + Python (`basedpyright`) type-check |
| `make app-regen-api` | Regenerate `ui/lib/api.ts` after backend model changes (no full wheel rebuild) |
| `make app-test` | Backend pytest suite |
| `make fmt` | Format Python (run from root before committing) |
| `make test` | Library unit tests |
| `make integration` | Integration tests (requires live workspace) |

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
DQX_SCHEMA=dqx_studio                       # schema inside the catalog
DQX_JOB_ID=<task-runner-job-id>             # required for profiler/dry-run
DQX_WHEELS_VOLUME=/Volumes/dqx/dqx_studio/wheels  # UC volume path; auto-set by DABs in production
DQX_ADMIN_GROUP=admins                      # workspace group granted bootstrap Admin access; AppConfig default is unset (no bootstrap admin locally) — set this to your test admin group

# Lakebase (optional — leave DQX_LAKEBASE_ENDPOINT empty to run OLTP tables on Delta locally)
DQX_LAKEBASE_ENDPOINT=                      # e.g. projects/dqx-studio-db/branches/dqx/endpoints/primary; empty = Delta-only mode
DQX_LAKEBASE_DATABASE_NAME=databricks_postgres  # logical Postgres DB; defaults to the always-present admin DB
DQX_LAKEBASE_SCHEMA=dqx_studio              # Postgres schema (default: dqx_studio)
DQX_LAKEBASE_POOL_MIN_SIZE=1                # psycopg connection pool floor
DQX_LAKEBASE_POOL_MAX_SIZE=10               # psycopg connection pool ceiling
DQX_LAKEBASE_TOKEN_REFRESH_MINUTES=50       # OAuth token refresh cadence (token expires at 60)
```

`DQX_JOB_ID`, `DQX_WHEELS_VOLUME`, `DQX_LAKEBASE_ENDPOINT`, and `DQX_LAKEBASE_DATABASE_NAME` are injected automatically when deployed via DABs. For local dev, set them manually only if you want to exercise the corresponding feature locally:

| Want to test... | Set... |
|---|---|
| Profiler / dry-run | `DQX_JOB_ID` (and the wheel volume must exist) |
| Lakebase OLTP path | `DQX_LAKEBASE_ENDPOINT` (empty = falls back to Delta — fine for most local dev) |
| Wheel sync | `DQX_WHEELS_VOLUME` |

> **Lakebase locally:** The same OAuth token-refresh logic that runs in production also runs locally. The app authenticates as your CLI user (via `databricks-sdk` default auth chain), so your CLI principal must have a Postgres role on the project branch. Easiest path: run the bundle once against your dev workspace so the `postgres_roles` block provisions the role, then point `DQX_LAKEBASE_ENDPOINT` at the project's endpoint path (`projects/<project>/branches/<branch>/endpoints/primary`).

### Bring your own SQL warehouse, catalog, and Lakebase

Local dev **never provisions** anything — it always points at resources that already exist (created by a `bundle deploy` or by hand). So the production "bundle-managed vs. bring-your-own" choice collapses locally to "which value do I put in `app/.env`":

| Resource | Local knob | Notes |
|---|---|---|
| **SQL warehouse** | `DATABRICKS_WAREHOUSE_ID=<existing-id>` | Any warehouse you have `CAN_USE` on. Required for queries, profiling, and dry-runs. |
| **Catalog** | `DQX_CATALOG=<existing-catalog>` (+ `DQX_SCHEMA`, `DQX_TMP_SCHEMA`) | The catalog and schemas must already exist; local dev does **not** create them. You need `USE CATALOG` + `USE SCHEMA` (+ `SELECT` to profile tables). |
| **Lakebase** | `DQX_LAKEBASE_ENDPOINT=<existing-endpoint-path>` | Point at an existing project endpoint you have a Postgres role on (`projects/<project>/branches/<branch>/endpoints/primary`). **Leave it empty** to skip Lakebase and run OLTP tables on Delta — the simplest local setup. |

In production the bundle always provisions its own SQL warehouse and Lakebase project (the catalog is always pre-existing). The fastest way to get a matching warehouse + catalog + Lakebase for local dev is to run `make app-deploy` once against a dev workspace, then copy the resulting IDs/endpoint path into `app/.env`.

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

Or directly from the `app/` directory:
```bash
uv run python scripts/build_app.py
```

This generates the OpenAPI schema, compiles the React/TypeScript UI into `__dist__/`, and packages everything into a wheel. The wheel filename and METADATA both carry a build-tag local-version segment (e.g. `.b20260530t012345`) so successive deploys at the same git commit force a fresh pip install in Databricks Apps' persistent venv.

## 4. Start Dev Servers

From the **project root**:
```bash
make app-start-dev   # builds first, then starts uvicorn + vite in the foreground
```

Or directly from the `app/` directory:
```bash
uv run python scripts/dev.py
```

This spawns:

* **uvicorn** on `http://localhost:9002` (FastAPI, `--reload` for backend hot reload)
* **vite** on `http://localhost:9001` (UI + HMR, with built-in proxy forwarding `/api`, `/docs`, `/redoc`, `/openapi.json` to uvicorn)

Access the app at:
- **UI**: http://localhost:9001
- **API**: http://localhost:9001/api
- **OpenAPI docs**: http://localhost:9001/docs

Logs stream to the foreground terminal. **Ctrl+C** sends `SIGINT` to both children (via process group) for a clean shutdown — typically under one second.

## Monitoring & Logs

```bash
make app-start-dev    # all logs stream to stdout (foreground)
make app-stop-dev     # stop dev servers started in another shell
```

To background the dev loop and tail logs separately, redirect to a file:

```bash
nohup make app-start-dev > /tmp/dqx-dev.log 2>&1 &
tail -f /tmp/dqx-dev.log
```

## Development Workflow

**Adding a new API endpoint:**
1. Define the endpoint in `backend/routes/v1/` with a Pydantic response model and `operation_id`
2. Add request/response models to `backend/models.py` if needed
3. Run `make app-regen-api` to regenerate the OpenAPI schema and `ui/lib/api.ts` (fast — no full wheel rebuild)
4. Use the generated React Query hooks in your components

**Making UI changes:**
1. Edit components in `ui/components/` or routes in `ui/routes/`
2. Vite HMR reloads the browser automatically — no manual refresh needed
3. Run `make app-regen-api` after backend changes to update `ui/lib/api.ts`

## Code Quality

```bash
make app-check   # TypeScript (`bun run tsc -b`) + Python (`basedpyright --level error`) type-check
make fmt          # format Python (run before every commit)
```

`make app-check` is **type-check only** — it does not run ESLint or ruff. Run linters separately if you need them: `make lint` from the project root covers ruff + mypy at the library level.

## Testing

```bash
make test          # unit tests
make integration   # integration tests (requires live workspace)
```

## Permissions

The profiler creates a temporary view using your OBO token and submits a Databricks Job as the service principal. You need `USE CATALOG` + `USE SCHEMA` + `SELECT` on the tables you want to profile, plus `DQX_JOB_ID` set to a deployed task-runner job. See [README.md](README.md) for the full authentication model.

If the wheel upload fails locally with a `403`, grant your user write access:
```bash
databricks volumes grant <catalog>.dqx_studio.wheels WRITE_VOLUME --user <your-email> -p <your-profile>
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
make app-regen-api   # regenerates ui/lib/api.ts from the current OpenAPI spec
```

**Build artifacts are stale:**
```bash
rm -rf .build src/databricks_labs_dqx_app/__dist__
make app-build
```

**uv hangs:**
```bash
uv sync -v         # verbose output to diagnose
rm -rf .venv/.lock # remove stale lock if needed
```

**OBO token missing locally:**
The `X-Forwarded-Access-Token` header is only injected by the Databricks Apps platform; it is absent when running locally. In that case the backend falls back to the **SDK default auth** for OBO operations — i.e. your configured CLI profile / `.env` (`DATABRICKS_CONFIG_PROFILE` or `DATABRICKS_TOKEN`). No extra setup is needed beyond [Configure Authentication](#1-configure-authentication): just `databricks auth login` or an `app/.env`, and OBO endpoints run as your own identity.

This fallback only activates locally — production deployments authenticate via `DATABRICKS_CLIENT_ID`/`DATABRICKS_CLIENT_SECRET` (no profile/PAT present), so the header is always required there. Note OBO calls then run as **your** identity, not an arbitrary end user, so it can't simulate per-user permission differences.
