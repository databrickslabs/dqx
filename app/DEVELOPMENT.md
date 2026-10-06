# Local Development

## Prerequisites

- **Python 3.12**
- **Node.js 18+** (provides `npm`) — install via `brew install node`, [nvm](https://github.com/nvm-sh/nvm), or [nodejs.org](https://nodejs.org/en/download) — and **yarn** classic v1 (`npm install -g yarn`, used for the committed `app/yarn.lock`; `bun.lock` / `package-lock.json` are gitignored)
- **bun** — used by `make app-check` to run `tsc -b` (TypeScript incremental compile). Install via `curl -fsSL https://bun.sh/install | bash` or `brew install oven-sh/bun/bun`.
- **uv** — Python package manager
- **Databricks CLI** v1.4.0+ — install per the [official guide](https://docs.databricks.com/aws/en/dev-tools/cli/install) (the legacy `databricks-cli` PyPI package is unrelated and not supported). Verify with `databricks --version`. v1.4.0 is the minimum for `make app-deploy` because the bundle's `postgres_projects` / `postgres_roles` resources are only accepted by CLI ≥ 1.4.0.
- Access to a Databricks workspace

The `make app-*` commands use a Unix shell on macOS or Linux. On Windows, the experimental `make.ps1` helper supports source deployment with `.\make.ps1 app-deploy -Profile <profile> -Target <target>` from the repository root. Tagged release deployment uses only Git and the Databricks CLI; see [DEPLOYMENT.md](DEPLOYMENT.md#deploy-a-tagged-studio-release-without-building-locally).

## Command Reference

Every workflow runs through `make` targets at the project root. The
underlying invocations are pure-Python orchestrators (`scripts/build_app.py`,
`scripts/dev.py`) plus `bun` / `uv` / `node_modules/.bin/*` — no
project-specific CLI required. **Prefer `make` from the project root.**

| `make` (from root) | What it does |
|---|---|
| `make app-install` | Install JS dependencies (yarn) |
| `make app-build` | Compile UI, generate OpenAPI schema, assemble the `.build/` deploy tree (runs `app/scripts/build_app.py`) |
| `make app-build-marketplace` | Generate a complete local `app/marketplace/` artifact for inspection |
| `make app-check-marketplace` | Validate Marketplace generation and the signed-release tooling |
| `app/scripts/release_marketplace.sh studio-vX.Y.Z` | Create and verify a local signed branch and tag for `dqx-studio/marketplace/vX.Y.Z`; never pushes |
| `make app-start-dev` | Build then start uvicorn + vite via `app/scripts/dev.py` (foreground; Ctrl+C to stop) |
| `make app-stop-dev` | Stop dev servers started in another shell (`pkill`-based) |
| `make app-check` | TypeScript (`tsc -b`) + Python (`basedpyright`) type-check |
| `make app-regen-api` | Regenerate `ui/lib/api.ts` after backend model changes (no full wheel rebuild) |
| `make app-test` | Backend pytest suite |
| `make fmt` | Format Python (run from root before committing) |
| `make test` | Library unit tests |
| `make integration` | Integration tests (requires live workspace) |

> Lock files (`yarn.lock` and `uv.lock`) must be committed to ensure reproducible builds.

### Marketplace release workflow

Main intentionally excludes the generated `app/marketplace/` artifact. Canonical application source and Marketplace templates remain tracked. Start a release from the committed `HEAD` whose `app/pyproject.toml` version matches the requested tag:

```bash
make app-release-marketplace TAG=studio-v0.1.0
git push origin dqx-studio/marketplace/v0.1.0
git push origin studio-v0.1.0
```

The first command creates the release branch in a temporary worktree, builds and validates its complete self-contained Marketplace source, signs and verifies the local commit, then creates and verifies the annotated signed tag on that generated commit. Consequently, the Marketplace source path `app/marketplace/` exists at the tagged revision. The command never pushes. The Studio package/tag version and the published DQX Core pin are separate: the requested tag must match `app/pyproject.toml`, while the artifact reads its DQX pin from `app/databricks.yml`. Inspect the branch before running the two explicit push commands. Normal DAB builds continue to use `.build/`.

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
DQX_CATALOG=dqx                             # existing Unity Catalog catalog; setting it means "deployment-supplied storage" (no setup form)
DQX_PREFIX=dqx_studio                       # storage prefix: <prefix>, <prefix>_tmp, <prefix>_genie, <prefix>_demo + <prefix>.wheels volume
DQX_JOB_ID=<task-runner-job-id>             # required for profiler/dry-run
DQX_ADMIN_GROUP=admins                      # workspace group granted bootstrap Admin access; AppConfig defaults to workspace admins in every path, and this overrides it for a test or deployment group
DQX_USER_GROUPS='["dqx-studio-users"]'       # existing scoped audience; default [] means administrator-managed access

# Lakebase (required — DQX Studio stores transactional state in Postgres)
DQX_LAKEBASE_ENDPOINT=projects/<project>/branches/<branch>/endpoints/primary
DQX_LAKEBASE_DATABASE_NAME=databricks_postgres  # logical Postgres DB; defaults to the always-present admin DB
DQX_LAKEBASE_SCHEMA=dqx_studio              # Postgres schema (default: dqx_studio)
DQX_LAKEBASE_POOL_MIN_SIZE=1                # psycopg connection pool floor
DQX_LAKEBASE_POOL_MAX_SIZE=10               # psycopg connection pool ceiling
DQX_LAKEBASE_TOKEN_REFRESH_MINUTES=50       # OAuth token refresh cadence (token expires at 60)
```

`DQX_CATALOG`, `DQX_PREFIX`, `DQX_SCHEMA`, `DQX_TMP_SCHEMA`, `DQX_GENIE_SCHEMA`, `DQX_DEMO_SCHEMA`, `DQX_JOB_ID`, `DQX_LAKEBASE_ENDPOINT`, and `DQX_LAKEBASE_DATABASE_NAME` are injected automatically when deployed via DABs; the schema variables default to names derived from `DQX_PREFIX`, and the wheels volume is always `<catalog>.<main schema>.wheels` (there is no `DQX_WHEELS_VOLUME`). For local development, provide the values you need from resources you manage:

| Want to test... | Set... |
|---|---|
| Profiler / dry-run | `DQX_JOB_ID` (and the `<prefix>.wheels` volume must exist) |
| Lakebase OLTP path | `DQX_LAKEBASE_ENDPOINT` (required) |
| Wheel sync | `DQX_CATALOG` + `DQX_PREFIX` (the volume is `<catalog>.<prefix>.wheels`) |

**Local development without deployment storage.** If you leave `DQX_CATALOG` unset, the app behaves like a Marketplace install: setup shows the **setup form** (catalog, prefix, audience group) instead of reading deployment configuration. The choices are saved in your Lakebase schema, storage is created under the prefix, and the catalog and prefix are then locked. Your CLI identity must be a workspace `admins` member or a member of `DQX_ADMIN_GROUP` to submit it, and the audience must be an existing dedicated group (`users` is not accepted by the form). Set `DQX_CATALOG` (and optionally `DQX_PREFIX`) to skip the form and use deployment-style configuration, including broad mode (`DQX_USER_GROUPS=users`).

> **Lakebase locally:** The same OAuth token-refresh logic runs in production and locally. Local app operations authenticate as your CLI user, so a Lakebase administrator must provision that user's OAuth role and application-schema migration privileges on the development branch; deploying the bundle's app-SP role does not provision your CLI user's role. The job runner is a separate identity: provision its OAuth `SERVICE_PRINCIPAL` role with `LOGIN`, effective database `CONNECT`, schema `USAGE`, and `SELECT` / `DELETE` on `dq_run_configs`. Follow [Task-runner Lakebase access](DEPLOYMENT.md#task-runner-lakebase-access); never grant superuser membership to the runner.

### Bring your own SQL warehouse, catalog, and Lakebase

Local dev **never provisions** anything — it always points at resources that already exist (created by a `bundle deploy` or by hand). So the production "bundle-managed vs. bring-your-own" choice collapses locally to "which value do I put in `app/.env`":

| Resource | Local knob | Notes |
|---|---|---|
| **SQL warehouse** | `DATABRICKS_WAREHOUSE_ID=<existing-id>` | Any warehouse you have `CAN_USE` on. Required for queries, profiling, and dry-runs. |
| **Catalog** | `DQX_CATALOG=<existing-catalog>` + `DQX_PREFIX` (schema overrides: `DQX_SCHEMA`, `DQX_TMP_SCHEMA`, `DQX_GENIE_SCHEMA`, `DQX_DEMO_SCHEMA`) | The catalog must already exist and local dev does **not** create it. With `DQX_CATALOG` set, the prefix schemas and `wheels` volume must already exist (setup reports `storage_missing` instead of creating them); without it, the setup form creates them. You need `USE CATALOG` + `USE SCHEMA` (+ `SELECT` to profile tables). |
| **Lakebase** | `DQX_LAKEBASE_ENDPOINT=<existing-endpoint-path>` | Required. Point at a project endpoint where your CLI identity has a Postgres role (`projects/<project>/branches/<branch>/endpoints/primary`). |

In production the bundle always provisions its own SQL warehouse and Lakebase project (the catalog is always pre-existing). The fastest way to get a matching warehouse + catalog + Lakebase for local development is to run `make app-deploy` once against a development workspace, then copy the resulting IDs and endpoint path into `app/.env`. Delta-backed application state has been removed and has no migration path.

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

This generates the OpenAPI schema, compiles the React/TypeScript UI into `__dist__/`, and assembles `.build/` — the source tree Databricks Apps runs via `uv run` (no application wheel). The tree carries `pyproject.toml`, `uv.lock`, the package `src/`, and `requirements.txt` = `uv`, so the container resolves the locked environment (and its own Python) at launch.

While `pyproject.toml` resolves `databricks-labs-dqx` from the parent checkout, the build also copies that library into `.build/_vendor/dqx` and retargets the *copied* `pyproject.toml` / `uv.lock` at it. The container only ever receives the `app/` directory, so a path source pointing outside it cannot resolve there — `uv run` fails at launch with `does not appear to be a Python project`. The tracked lock is never rewritten, so the deployed resolution is the one that was tested. Once a DQX release carries the symbols the backend imports, drop `[tool.uv.sources]` and the vendoring step becomes a no-op automatically.

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
make app-test
make app-test-ui
make app-integration PROFILE=<setup-admin-profile>
```

The opt-in Studio integration suite creates factory-managed workspace and
Lakebase resources. Use a dedicated development workspace and checkout. Prepare
the generated Marketplace runner wheel under `app/marketplace/tasks/` first;
the suite stages it into `.build/tasks/` and refuses to overwrite an existing
build wheel. Complete-readiness coverage additionally needs a distinct
`DQX_TEST_APP_PROFILE` authenticated as an app service principal and
`DQX_TEST_RUNNER_SERVICE_PRINCIPAL` identifying a usable external runner. The
setup profile needs resource creation and grant authority; fixture-created
OAuth roles receive scoped privileges, not superuser membership. An optional
short-lived `DQX_TEST_APPS_OBO_TOKEN` exercises real Apps-scoped administrator
inspection. Supply tokens securely and never commit them. Without an explicitly
supplied CLI opt-in, the suite skips before creating resources. The Make target
passes `--studio-integration` automatically; direct pytest invocations must pass
it explicitly.

## Permissions

The profiler creates a temporary view using your OBO token (your CLI identity locally) and submits a Databricks Job under its configured `run_as` identity. You need source `USE CATALOG` + `USE SCHEMA` + `SELECT`, warehouse `CAN_USE`, and `USE SCHEMA` + `CREATE TABLE` on the temporary schema. `DQX_JOB_ID` must identify a deployed task-runner job. Each view grants `SELECT` directly to the actual runner and per-view `MANAGE` to the app identity for orphan cleanup; failed grants block submission. Do not use broad built-in groups or schema-wide cleanup grants.

`DQX_USER_GROUPS` accepts a JSON `list[str]` of existing groups or simple unquoted comma-separated names, for example `studio-authors,studio-viewers`; use JSON for names containing commas or quotes. `account users` and `admins` are rejected; `users` selects DAB-style broad mode (workspace ACL `users`, Unity Catalog `account users`, account-wide) and is accepted only with deployment-supplied storage. The list must not be empty when `DQX_CATALOG` is set. DAB uses `studio_user_group` (default `dqx-studio-users`, which must exist). This audience is separate from `DQX_ADMIN_GROUP` and in-app role mappings. See [DEPLOYMENT.md](DEPLOYMENT.md#permission-matrix) for the full permission matrix.

Every cold startup applies, then rechecks, the runner's schema and volume grants and verifies its catalog usage, main and temporary schema usage, wheel-volume `READ VOLUME`, and schema-level `SELECT` / `MODIFY` on the main schema. Schema grants cover current and future tables; table-specific grants alone do not satisfy setup. Runner Lakebase roles and privileges are not checked by setup, and removing those checks does not change the current oversized-config runtime requirement. Verification is not cached; setup never applies PostgreSQL runner grants or catalog privileges. Runtime derives the Postgres username from Jobs `run_as`; any legacy `DQX_TASK_RUNNER_POSTGRES_ROLE` value must match it.

Schedules inspect grants through OBO SQL, not the grants REST API, and verify source catalog/schema usage and table `SELECT` for both app scheduler and runner. Failed runner grants block scheduling. Genie consumers separately need space `CAN_RUN`, Consumer access or Databricks SQL access entitlement, parent usages, and only the approved five view / two dimension-table grants. Genie uses embedded compute credentials; Studio's OBO SQL workflows also need SQL access entitlement and warehouse `CAN_USE`. Both deployment paths use the `genie` scope and require renewed user consent after scope changes. See [DEPLOYMENT.md](DEPLOYMENT.md#grants-reference) for exact grants. This release has no upgrade path; previously granted broad access is not automatically removed.

If the wheel upload fails locally with a `403`, grant your user write access:
```bash
databricks volumes grant <catalog>.<prefix>.wheels WRITE_VOLUME --user <your-email> -p <your-profile>
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
