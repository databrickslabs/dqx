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
| `make app-e2e PROFILE=<profile>` | Playwright smoke tests and customer journeys against a running Studio deployment (see [Pre-release browser tests](#pre-release-browser-tests)) |
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

### Pre-release browser tests

Before cutting a tag, deploy the release candidate (`make app-deploy PROFILE=<profile> TARGET=<target>`), finish setup in the Studio UI, then run the Playwright suite against it.

> **The suite must point at a real, fully set-up DQX Studio instance.** It has no mocks or fixtures standing in for the backend. It talks to a live app, its Lakebase database, warehouse and Unity Catalog storage. A deployment that is still on the setup screen fails every test that needs Studio to be ready, and the failure lists the pending setup steps. Deploy, open the app once, and finish every setup step (catalog, prefix, user group, task runner) before running it.

```bash
make app-install                          # once, installs @playwright/test
make app-e2e PROFILE=<profile>            # smoke + journeys; app name defaults to dqx-studio (APP=<name>)
make app-e2e PROFILE=<profile> SUITE=smoke   # read-only checks only
```

The suite lives in `app/e2e/`. It looks up the app URL with `databricks apps get` and authenticates with an OAuth token from `databricks auth token`. The profile must use OAuth (`databricks auth login`), because the Apps proxy does not accept personal access tokens. The token is sent only to the app's own origin. Run it as a Studio admin with approvals mode set to **Require approval**.

The suite runs as Playwright projects, in order:

| Project | What it does |
|---|---|
| `smoke` | Read-only: backend health, deployed version matches `app/pyproject.toml`, setup is complete, identity and role resolve, every sidebar page renders without uncaught errors or 5xx API responses. |
| `accessibility` | Read-only axe-core WCAG 2 A/AA scan of every sidebar page: no serious or critical violations beyond a documented known list. Runs alongside the others; nothing depends on it. |
| `journeys-setup` | Sweeps leftovers from an aborted earlier run (see below). |
| `journeys` | Real customer journeys, one spec per area, run in parallel: author, approve, reject and delete registry rules; write custom-condition (visual builder) and SQL rules and run them; write a cross-table SQL check and run it with a table; export a table's checks (DQX YAML, ODCS) and a rule, and import the export back; publish v2 of a rule used by a table following latest and one pinned to v1; YAML rule import; monitor a table from the catalog browser, apply and map a rule, submit, approve in Review & Approve, stop monitoring; run an approved table and triage the run (Runs History, results, review status, comments); browse, explain and download (CSV, Excel) a run's failed records; runs that go wrong (a check that can't be evaluated, cancelling); collections (build from approved tables, approve, run as one unit, rolled-up results); multi-table (monitor a whole schema with Select all, bulk approve, bulk run, compare on Results, bulk delete); schedules (create, pause, resume, remove, for a table and a collection; profiling + data quality on a sample); a real scheduled run (a 5-minute cron fires, the run succeeds, the schedule is turned off); sharing a table from its Permissions tab; UI language switch. |
| `isolated` | Journeys that touch shared state, one at a time: approvals mode (including Skip when self-approvable), the draft-run requirement, rule labels and run review statuses in Admin Settings, marketplace import, ODCS data-contract import, profiling and its suggested checks. |

Journeys create, change and delete real objects, so point them at a release-candidate deployment, not one people are using. Everything they create is named `e2e-<run tag>-…` and removed when the test ends, pass or fail; settings are restored. They monitor tables from the read-only `samples` catalog every workspace has (from `samples.tpch`, `samples.tpcds_sf1` and all of `samples.bakehouse`), one set of tables per spec; see `app/e2e/support/sample-tables.ts`. Runs and profiling start real Databricks jobs (about a minute each), and the scheduled-run journey waits for the next 5-minute scheduler slot, so the full suite takes roughly 10–15 minutes.

Known Studio bugs are captured with `knownBug()` (`app/e2e/support/known-bugs.ts`), which wraps only the assertion the bug breaks: setup failures still fail the test, the test passes while the bug exists and prints `KNOWN BUG: …`, and it fails with "Known bug appears fixed — remove the knownBug() wrapper" once Studio is fixed. The comment above each call says what's broken.

If a run is interrupted before cleanup, the next run's `journeys-setup` deletes `e2e-` rules, collections, review statuses and label keys, and un-monitors those sample tables. It only removes a sample table if you created it and all its rules came from the suite. If someone else monitors one of those tables, the journeys stop and list it rather than touching it. Rules created by an interrupted marketplace or contract import keep Studio's own names and aren't swept; delete them from **Rules** if that happens.

- `BASE_URL=http://localhost:9001` runs it against `make app-start-dev` instead (no token needed).
- `ARGS="--headed"` watches the browser; `ARGS="-g schedules"` runs a subset.
- Failure screenshots and the HTML report go to `app/e2e/test-results/` and `app/e2e/playwright-report/` (git-ignored). Traces are off by default because they record request headers, including the token. If you enable them with `ARGS="--trace on"`, delete `test-results/` afterwards.

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

> **Lakebase locally:** The same OAuth token-refresh logic runs in production and locally. Local app operations authenticate as your CLI user, so a Lakebase administrator must provision that user's OAuth role and application-schema migration privileges on the development branch; deploying the bundle's app-SP role does not provision your CLI user's role. The job runner does not connect to Lakebase.

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

Local development and app tests resolve `databricks-labs-dqx` from the parent checkout, but deploys never do. The `.build/` tree gets a release `pyproject.toml` pinned to the published `dqx_version` from `databricks.yml`, and the committed Marketplace lock (`marketplace_templates/uv.lock`). A DAB deploy therefore installs the same published DQX and dependency versions as Marketplace, and the task runner installs the same pin. Unreleased DQX Core changes reach a workspace only after a DQX release and a Studio bump of `dqx_version` with the lock (see `DEPLOYMENT.md`). The build fails if the lock resolves a different DQX than `dqx_version`.

This means Studio code (`app/src` and `app/tasks`) can only use DQX features that exist in the pinned `dqx_version`. Tests run against the checkout, so they won't catch an import of a helper that's on `main` but not yet released: it fails with an `ImportError` when the deployed app starts. If a Studio change needs something new in Core, land the Core change, release DQX, and then bump `dqx_version` and the lock in the Studio release that uses it.

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

The profiler creates a temporary view using your OBO token (your CLI identity locally) and submits a Databricks Job under its configured `run_as` identity. You need source `USE CATALOG` + `USE SCHEMA` + `SELECT`, warehouse `CAN_USE`, and `USE SCHEMA` + `CREATE TABLE` on the temporary schema. `DQX_JOB_ID` must identify a deployed task-runner job. The runner reads views through schema-level `SELECT` on the temporary schema (it can read any view there; no per-view runner grants), and each view grants per-view `MANAGE` to the app identity for orphan cleanup; a failed `MANAGE` grant blocks submission. Do not use broad built-in groups or schema-wide cleanup grants.

`DQX_USER_GROUPS` accepts a JSON `list[str]` of existing groups or simple unquoted comma-separated names, for example `studio-authors,studio-viewers`; use JSON for names containing commas or quotes. `account users` and `admins` are rejected; `users` selects DAB-style broad mode (workspace ACL `users`, Unity Catalog `account users`, account-wide) and is accepted only with deployment-supplied storage. The list must not be empty when `DQX_CATALOG` is set. DAB uses `studio_user_group` (default `dqx-studio-users`, which must exist). This audience is separate from `DQX_ADMIN_GROUP` and in-app role mappings. See [DEPLOYMENT.md](DEPLOYMENT.md#permission-matrix) for the full permission matrix.

Every cold startup applies, then rechecks, the runner's schema and volume grants and verifies its catalog usage, main and temporary schema usage, schema-level `SELECT` on the temporary schema (how the runner reads OBO temp views), wheel-volume `READ VOLUME`, and schema-level `SELECT` / `MODIFY` on the main schema. Schema grants cover current and future tables; table-specific grants alone do not satisfy setup. Runner Lakebase roles and privileges are not checked by setup, and removing those checks does not change the current oversized-config runtime requirement. Verification is not cached; setup never applies PostgreSQL runner grants or catalog privileges. Runtime derives the Postgres username from Jobs `run_as`; any legacy `DQX_TASK_RUNNER_POSTGRES_ROLE` value must match it.

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
