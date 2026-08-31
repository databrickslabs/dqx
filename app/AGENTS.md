# AI Agent Guidelines for DQX Studio

Studio-specific guidance for agents working under `app/`. For repo-wide DQX Core conventions, see the root [`AGENTS.md`](../AGENTS.md).

Human-oriented setup and deploy docs: [`README.md`](README.md), [`DEVELOPMENT.md`](DEVELOPMENT.md), [`DEPLOYMENT.md`](DEPLOYMENT.md).

---


## Purpose

The DQX Studio is a **UI for authoring and managing data quality rules**. It lowers the barrier from writing code (YAML/Python) to a visual, self-service experience — making rule creation accessible to non-technical users while keeping technical users efficient.

**Scope:** Creating/validating rules, AI/profiler rule generation, rule lifecycle management, internal storage, approval workflows, export to execution systems, dry-run validation, scheduled in-app rule execution, run history + quality metrics + quarantine review.

**Not in scope:** Running rules as part of customer production data pipelines (the app's runs target dev/UAT data and write results to the app's own catalog).

## Deployment

- Deploys as a **Databricks App** (FastAPI backend + React frontend in a single Python wheel)
- Must be publishable to **Databricks Marketplace**
- Uses a **hybrid auth model**: data-plane reads (catalog/schema/table browse, dry-run preview, query execution against the user's tables) run as the logged-in user via **On-Behalf-Of (OBO)** tokens so Unity Catalog perms are enforced. Control-plane writes (rules CRUD, RBAC mappings, migrations, wheel sync, task-runner job submission) run as the app's **service principal** so they don't require every end user to hold those workspace permissions. See `README.md` for the full split.

## Target Personas

Four primary roles in `UserRole` (`backend/common/authorization.py`). Author and Approver are complementary (four-eyes), not ranked — see user docs at `/docs/studio/governance/permissions-and-entitlements`.

| Role | Description | Key permissions (`PERMISSIONS`) |
|------|-------------|----------------------------------|
| `ADMIN` | Platform owner / data engineer | All permissions (author, approve, configure storage, manage roles, run) |
| `RULE_APPROVER` | Reviews and approves submissions | `view_rules`, `approve_rules`, `export_rules`, `configure_storage` (quarantine list/export is Approver+) |
| `RULE_AUTHOR` | Defines and maintains rules | create/edit/submit/generate rules + `run_rules` |
| `VIEWER` | Observability only | `view_rules` |

There is **no** separate `RUNNER` role. Manual/scheduled runs require `run_rules`, which is granted to `ADMIN` and `RULE_AUTHOR` only (`CAN_RUN_ROLES`). The API field `UserRoleOut.is_runner` is a backward-compat flag derived as `"run_rules" in permissions`.

RBAC is enforced — routes use `require_role(*roles)` from `backend/dependencies.py` and roles resolve from Databricks workspace-group membership in `dq_role_mappings` (plus the bootstrap `DQX_ADMIN_GROUP`).

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

Lakebase project (when enabled, default `lakebase_project_id` = `dqx-studio-db`):
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
├── AGENTS.md                  ← You are here (product + backend + UI agent context)
├── DESIGN.md                  ← Server-Driven UI (SDUI) design doc (planned, not yet implemented)
├── pyproject.toml             ← Python package config (FastAPI, Pydantic, SDK deps)
├── databricks.yml             ← Databricks Asset Bundle config
└── src/databricks_labs_dqx_app/
    ├── backend/               ← FastAPI REST API (see Backend below)
    │   ├── routes/v1/         ← Versioned API routes
    │   ├── services/          ← Business logic services
    │   ├── common/            ← Auth, authorization, connectors
    │   └── ...
    └── ui/                    ← React SPA (see Frontend below)
        ├── routes/            ← File-based routing (TanStack Router)
        ├── components/        ← shadcn/ui + app components
        ├── lib/api.ts         ← Auto-generated API hooks (orval)
        └── ...
```

## Stack

- **Backend:** Python 3.12+, FastAPI, Pydantic 2, Databricks SDK, Databricks SQL Connector, psycopg (Lakebase/Postgres), DQX library
- **Frontend:** React 19, TypeScript, TanStack Router + React Query, shadcn/ui, Tailwind CSS 4, Vite 7
- **Code generation:** orval (OpenAPI → TypeScript types + React Query hooks)

## References

- [Mini-PRD](https://docs.google.com/document/d/1oLeL1SuhBq66cx3lg5rAuN652Ol9HhpWsc6JZgTkvHU/edit)
- [Architecture diagram (Excalidraw)](https://drive.google.com/file/d/1oQ61cDDZcLwOyI9iIR47PsOQZLVnsdMD/view)

---

# Frontend


## Overview

React 19 SPA for authoring and managing DQX data quality rules. Deployed as static files served by the FastAPI backend within a Databricks App.

## Architecture

```
ui/
├── main.tsx                    # App bootstrap (QueryClient, Router, AuthGuard)
├── routes/                     # File-based routing (TanStack Router)
│   ├── __root.tsx              # Root layout (ThemeProvider, AIAssistantProvider, Toaster)
│   ├── index.tsx               # Home redirect
│   └── _sidebar/              # Sidebar layout group (prefix _ = layout route)
│       ├── route.tsx           # Sidebar nav + persistent in-app docs link
│       ├── home.tsx            # Landing page (welcome, primary CTAs)
│       ├── settings.tsx        # Admin / workspace settings (entitlements, retention, labels, …)
│       ├── discovery.tsx       # Catalog browser (catalog → schema → table → columns)
│       ├── profile.tsx         # User profile + language preference
│       ├── profiler.tsx        # Profiler launch + Profiler & Generate results modal
│       ├── marketplace.tsx     # Rule-pack marketplace
│       ├── results.tsx         # Results / quality score surfaces
│       ├── registry-rules.*    # Rules Registry (index / new / $ruleId / import / bulk-import)
│       ├── monitored-tables.*  # Monitored tables (index / new / $bindingId)
│       ├── collections.*       # Collections / data products (index / new / $productId)
│       ├── data-products.*     # Legacy → Navigate redirect to /collections
│       ├── table-spaces.*      # Legacy → Navigate redirect to /collections
│       ├── rules.*             # Legacy / alternate editors (active, drafts, single-table, create-sql, import, …)
│       ├── runs.*              # Manual-run launcher + run editor
│       └── runs-history.tsx    # Run history + schedules
├── components/                 # shadcn/ui + layout + feature components
├── lib/
│   ├── api.ts                  # ⚠️ AUTO-GENERATED by orval — types + React Query hooks
│   ├── axios-config.ts         # Axios interceptor (error logging)
│   ├── utils.ts                # cn() — clsx + tailwind-merge
│   ├── selector.ts             # Extracts .data from React Query responses
│   └── i18n/                   # react-i18next + locales/*.json (en, fr, pt-BR, it, es)
├── hooks/                      # Feature hooks (viewport, bindings, registry, …)
├── styles/
│   └── globals.css             # Tailwind imports, CSS variables (oklch), dark/light themes
└── types/
    ├── routeTree.gen.ts        # ⚠️ AUTO-GENERATED by TanStack Router
    └── vite-env.d.ts
```

Treat `routes/_sidebar/` as the source of truth for pages — prefer listing files there over copying this tree when it drifts.

## Auto-Generated Files — Do Not Edit

| File | Generator | Trigger |
|------|-----------|---------|
| `lib/api.ts` | **orval** (from `.build/openapi.json`, config at `app/orval.config.ts`) | Backend schema changes |
| `types/routeTree.gen.ts` | **TanStack Router** (from `routes/` folder) | Adding/removing route files |

To regenerate `api.ts` after backend changes:
```bash
make app-regen-api   # dumps fresh OpenAPI + runs orval, no wheel rebuild
```

Route tree regenerates automatically while the Vite dev server is running (the `tanstackRouter` plugin watches for route file changes). It also regenerates during `make app-build`.

> **Common issue — new route not found / silently 404ing:** `routeTree.gen.ts` only regenerates while Vite is running. If a route file is added while the dev server is stopped — e.g. by an AI agent between sessions — the file is stale and the route silently does not exist at runtime. Fix: restart `make app-start-dev` and the Vite watcher will detect the new file and regenerate immediately. Alternatively run `make app-build`.

## Stack

- **React 19** + TypeScript 5.9 (strict mode)
- **TanStack Router** — file-based, type-safe client routing
- **TanStack React Query** — server state (fetch, cache, invalidate, mutate)
- **Radix UI** + **shadcn/ui** (New York style) — headless component primitives
- **Tailwind CSS 4** — utility-first styling with CSS variables
- **Axios** — HTTP client (all requests go to `/api/v1/*`)
- **Vite 7** — dev server + bundler
- **Lucide React** — icons
- **Motion** — animations
- **Sonner** — toast notifications
- **react-i18next** — internationalization (see [Internationalization (i18n)](#internationalization-i18n))
- **js-yaml** — YAML parsing for config editing

## Commands

Prefer `make` from the project root — it spawns the correct pair of processes (uvicorn + Vite) and threads the right env vars in. Direct yarn invocations from `app/` are available for one-off frontend-only tasks.

```bash
# From project root (preferred)
make app-install       # yarn install --frozen-lockfile
make app-start-dev     # builds, then runs uvicorn (:9002) + Vite (:9001) in the foreground
make app-build         # full build (OpenAPI dump + orval + Vite + wheel)
make app-check         # tsc -b (via bun) + basedpyright + bun UI unit tests
make app-test-ui       # bun test (UI unit tests only)
make app-regen-api     # dump OpenAPI + run orval (no wheel rebuild)

# From app/ directory (frontend-only)
yarn vite              # Vite dev server, no backend
yarn vite build        # Production build → __dist__/
yarn eslint .          # ESLint
yarn vite preview      # Preview production build
```

`bun` is used by `make app-check` for `tsc -b --incremental`; it is **not** the project's package manager (the committed `app/yarn.lock` is the source of truth — `bun.lock` and `package-lock.json` are gitignored).

## Key Patterns

### Data Flow

1. Route component mounts → calls React Query hook (e.g., `useGetConfig()`)
2. Hook makes Axios request to `/api/v1/*` (OBO token in header automatically)
3. orval-generated hook transforms response via `selector` (extracts `.data`)
4. Component renders with cached data

### State Management

- **Server state**: React Query (no Redux/Zustand)
- **Theme**: React Context (`ThemeProvider`)
- **AI assistant modal**: React Context (`AIAssistantProvider`)
- **Local UI**: React `useState`/`useReducer`

### Authentication

`AuthGuard` wraps the entire app. It polls `GET /api/v1/current-user` with exponential backoff (1s → 3s, max 15 retries) until the Databricks OBO token is available. Nothing renders until auth succeeds.

### Adding a New Route

1. Create `routes/_sidebar/<name>.tsx` (or `routes/<name>.tsx` for non-sidebar pages)
2. Export a `Route` using `createFileRoute` from TanStack Router
3. Add nav item to `_sidebar/route.tsx` if it should appear in the sidebar
4. `types/routeTree.gen.ts` regenerates automatically while the Vite dev server is running. If the dev server was stopped when the file was created, restart `make app-start-dev` (or run `make app-build`) to pick up the new route.

### Adding a New API-Backed Feature

1. Backend: add route + response model (see Backend below)
2. Regenerate OpenAPI spec
3. Run orval to regenerate `lib/api.ts` — new hooks appear automatically
4. Use the generated hook in your component (e.g., `useMyNewEndpoint()`)

### Component Conventions

- Use shadcn/ui components from `components/ui/` — don't create custom primitives
- Import path alias: `@/` maps to `src/databricks_labs_dqx_app/ui/`
- Use `cn()` from `@/lib/utils` for conditional class merging
- Wrap async data components in `Suspense` + error boundaries

### Internationalization (i18n)

The UI is fully localized with **react-i18next**. Locale bundles live in `lib/i18n/locales/*.json`. **Any user-facing string must be translated — never hard-code display text.**

- **Use `t()` for all display text.** Get it from `useTranslation()` (`const { t } = useTranslation()`); reference strings by key (`t("discovery.title")`), never as literals in JSX. This includes `toast` messages, `aria-label`s, placeholders, and error strings.
- **Add every new key to all locales** — `en.json`, `fr.json`, `pt-BR.json`, `it.json`, `es.json`. `en.json` is the source of truth. A key present in `en` but missing from the others falls back to English at runtime (a silent partial-translation bug), so keep the key sets in sync and translate the value in each file — don't leave the English string behind in a non-English file.
- **Pluralize with native i18next, not string concatenation.** Use `_one` / `_other` suffix keys with `{{count}}` (e.g. `columnsCount_one` / `columnsCount_other`). Never build plurals with a hard-coded `"s"` suffix or an interpolated `{{somethingPlural}}` placeholder — that bakes English grammar into the translation layer and breaks other locales.
- **Adding a new language:** add it to `SUPPORTED_LANGUAGES` and register a loader in `localeLoaders` (both in `lib/i18n/index.ts`), then create the matching `locales/<code>.json`. Only `en` ships in the initial JS bundle; other locales lazy-load on demand via `ensureLocaleLoaded`, so don't statically import them.

### Theming (CSS custom properties)

Themes are CSS custom properties on `:root` and `.dark` in `styles/globals.css`. shadcn `Button` (and friends) read `--<role>` (background) and `--<role>-foreground` (text) — **both must contrast**. We've already shipped a bug where `--destructive-foreground` matched `--destructive` and the "Delete" button text was invisible on red. If you change a `--*-foreground` token, eyeball the corresponding role in both light and dark themes before merging.

### Dataset-level rules: routing & editing

Only **cross-table SQL checks** use the synthetic `__sql_check__/<name>` `table_fqn`, so they're the only rules bucketed under the **Cross-table rules** group on the Active Rules page. Reference checks (`has_valid_schema`, `foreign_key`) carry a **real target-table FQN** — they group under their target table and are authored *and* edited in the single-table editor. The Edit/View dispatch (in `routes/_sidebar/rules.active.tsx` and `rules.drafts.tsx`) keys off the FQN prefix only:

- synthetic `__sql_check__/` FQN → `/rules/create-sql?...`
- real table FQN → `/rules/single-table?...` (loads every check on the table, schema validation included)

There is **no** separate schema-validation route — `has_valid_schema` is just another check in the single-table editor's catalog. When you add another dataset-level (table-less) rule kind, give it a synthetic FQN and extend the cross-table dispatch; per-table reference checks need no special routing.

### Schema rule subset filtering (DDL trimming)

`has_valid_schema` only filters the *actual* DataFrame when you pass `columns` / `exclude_columns` — it does **not** trim the *expected* schema. To keep both sides aligned in DDL mode, `routes/_sidebar/rules.single-table.tsx#checkToDict()` calls `filterDdlByColumns()` from `lib/format-utils.ts` to trim `expected_schema` before saving. Reference-table mode can't trim a remote schema client-side, so it's left as-is. Don't remove this without porting the trimming server-side first.

### Import rules: tabbed page (?tab=yaml|contract)

`/rules/import` is a single tabbed page hosting two flows; `/rules/from-contract` is now just a `Navigate` redirect to `/rules/import?tab=contract` to keep old bookmarks working. The contract flow's main component (`ContractWorkspace`) is exported from `rules.from-contract.tsx` and imported by `rules.import.tsx`. If you split or rename either file, update both the redirect and the import — and remember to re-run `make app-build` (or the dev server) so `routeTree.gen.ts` picks up new files.

## Vite Config Notes

- **Dev server proxy**: Vite listens on `:9001` and forwards `/api`, `/docs`, `/redoc`, `/openapi.json` to uvicorn (target read from `DQX_APP_BACKEND_PORT` env var, default `9002`). Spawned by `scripts/dev.py`.
- **Build output**: `../__dist__/` (relative to ui folder — ends up in the Python package)
- **Path alias**: `@/` → `./src/databricks_labs_dqx_app/ui/`
- **App metadata**: Read from `[tool.dqx_app.metadata]` in `pyproject.toml` at config-load time

## TypeScript Config

- `strict: true`, `noUnusedLocals`, `noUnusedParameters`
- JSX: React transform (no explicit `import React`)
- Path alias: `@/*` → `./src/databricks_labs_dqx_app/ui/*`

---

# Backend


## Overview

FastAPI REST API serving as the DQX Studio backend. Deployed as a **Databricks App** with On-Behalf-Of (OBO) authentication. Serves both API endpoints (`/api/v1/*`) and the compiled React frontend as static files.

## Architecture

```
backend/
├── app.py                 # FastAPI app factory, lifespan, static file mount
├── cache.py               # CacheFactory — async in-memory TTL cache + @cached decorator
├── config.py              # AppConfig (Pydantic BaseSettings, DQX_ env prefix)
├── dependencies.py        # FastAPI Depends() — OBO/SP auth, RBAC, services
├── migrations/            # MigrationRunner (Delta) + PgMigrationRunner (Lakebase)
├── models.py              # Pydantic request/response models
├── rule_enums.py          # Shared enums (e.g. RuleSource / RuleStatus) used by models + services
├── sql_executor.py        # SqlExecutor — Databricks Statement Execution API wrapper
├── pg_executor.py         # PgExecutor — Lakebase Postgres wrapper (parity API w/ SqlExecutor)
├── sql_utils.py           # Shared SQL helpers: escape_sql_string(_strict), validate_fqn, quote_fqn
├── spa_static.py          # SPA static file handler (asset-extension allowlist for SPA fallback)
├── routes/v1/             # Versioned API routers — see directory (registry, monitored tables,
│                          #   collections/data products, marketplace, scores, genie, …)
├── services/              # Business logic — see directory (materializer, registry, scores,
│                          #   monitored tables, entitlements, scheduler, …)
└── common/
    ├── authorization.py   # UserRole enum + PERMISSIONS / CAN_RUN_ROLES
    ├── authentication/
    └── connectors/
```

Do not treat a frozen file list as authoritative — `ls routes/v1/` and `ls services/` (and `routes/v1/__init__.py` router includes) are the source of truth.

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

Defined in `common/authorization.py` (`PERMISSIONS` / `CAN_RUN_ROLES`):

| Role | Permissions |
|------|-------------|
| `ADMIN` | All actions (author, approve, configure storage, manage roles, run) |
| `RULE_APPROVER` | `view_rules`, `approve_rules`, `export_rules`, `configure_storage` (does **not** author or run) |
| `RULE_AUTHOR` | create/edit/submit/generate rules + `run_rules` |
| `VIEWER` | `view_rules` only |

`run_rules` is **not** a separate role — only Admin and Author get it. `UserRoleOut.is_runner` is derived from that permission for UI backward-compat.

Roles resolve from Databricks workspace group membership in `dq_role_mappings`
(plus the bootstrap `DQX_ADMIN_GROUP`). `get_user_role` (in `dependencies.py`)
performs resolution and degrades gracefully to `VIEWER` if SCIM/role-mapping is
transiently unavailable.

Routes enforce roles via `require_role(*roles)` either on the router
(`APIRouter(dependencies=[require_role(...)])`) or per-route (`@router.get(..., dependencies=[require_role(...)])`).
Object-level grants (steward / View / Modify / Apply / Execute) are enforced separately via `permissions_service` — see `/docs/studio/governance/permissions-and-entitlements`.
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
- **Pydantic** 2.x (validation, settings, response models)
- **Databricks SDK** ~0.120 (workspace API)
- **Databricks SQL Connector** (data-plane queries)
- **psycopg** 3 (Lakebase/Postgres)
- **DQX library** (path / released package; Spark via DQX extras — not a direct Databricks Connect pin in the app)
- **Uvicorn** (ASGI server)
- **Python 3.12+**

## Commands

Prefer `make` from the **repo root** (see root `AGENTS.md`). Do not run casual `uv sync` / `uv lock` — that bypasses `UV_FROZEN=1` and may rewrite lockfiles.

```bash
# From repo root (preferred)
make app-install       # yarn install --frozen-lockfile
make app-start-dev     # uvicorn (:9002) + Vite (:9001)
make app-test          # backend pytest
make app-check         # tsc + basedpyright + UI unit tests
make app-regen-api     # OpenAPI dump + orval
```

See `DEVELOPMENT.md` for local `.env` and Lakebase notes.

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
- **Synthetic-FQN dispatch (`__sql_check__/<name>`):** rules whose `table_fqn` starts with `__sql_check__/` are **cross-table SQL checks** — the only table-less rule kind. `arguments.sql_query` is set; build the input view with `view_svc.create_view_from_sql(...)` and set `is_sql_check=True`. A synthetic rule with no `sql_query` is malformed (surface a per-table error). Keep this dispatch in sync across `routes/v1/dryrun.py` (manual / batch) and `services/scheduler_service.py` (scheduled). Per-table errors raised during dispatch are surfaced to the UI via the run-submission response payload (consumed in `ui/routes/_sidebar/runs.tsx`). Reference checks like `has_valid_schema` / `foreign_key` carry a **real** `table_fqn` and flow through the normal `view_svc.create_view(table_fqn)` path (`is_sql_check=False`) — they need no special handling here.
- **Lakebase `ON CONFLICT DO UPDATE SET` column references:** PostgreSQL refuses bare column references on the RHS of `DO UPDATE SET` (`column reference "version" is ambiguous`), and a *schema-qualified* reference (`"dq"."tbl"."version"`) is **not** a valid existing-row reference there either — Postgres treats it as a FROM-clause entry and errors with `invalid reference to FROM-clause entry for table "dq"` on the *first* save, not just on conflict. `PgExecutor.upsert_with_audit` therefore aliases the conflict target (`INSERT INTO <fqn> AS "dqx_upsert_target"`) and qualifies `increment_on_update` references against the alias (`"{qcol} = "dqx_upsert_target".{qcol} + 1"`). The regression test in `tests/test_pg_executor.py` asserts the alias form *and* the absence of both the bare and schema-qualified forms — do not relax it.

## Hybrid Storage Backend (Delta + Lakebase)

The DQX Studio data model is split across two physical backends and the
choice is driven entirely by `databricks.yml`:

| Backend | Tables | Why |
|---------|--------|-----|
| **Delta Lake** (always) | `dq_validation_runs`, `dq_profiling_results`, `dq_quarantine_records`, `dq_metrics` | Spark task runner writes these; high-volume append-mostly; columnar reads. |
| **Lakebase Postgres** *(default — opt-out via `lakebase_endpoint="-"`)* | `dq_app_settings`, `dq_role_mappings`, `dq_quality_rules`, `dq_quality_rules_history`, `dq_comments`, `dq_schedule_configs`, `dq_schedule_configs_history`, `dq_schedule_runs` | Low-latency point reads/writes from FastAPI request handlers; row-level upserts; primary-key/foreign-key semantics. |

When Lakebase is **disabled** (no `lakebase_endpoint` set), the OLTP
tables fall back to Delta — `MigrationRunner` runs both
`v1: Delta analytical baseline` *and* `v2: Delta OLTP fallback`. When
Lakebase is **enabled**, only `v1` runs on Delta and `PgMigrationRunner`
provisions the OLTP tables in Postgres.

### Key types

- `SqlExecutor` (`sql_executor.py`) wraps the Databricks Statement
  Execution API for Delta.
- `PgExecutor` (`pg_executor.py`) wraps `psycopg` + a `psycopg_pool.ConnectionPool`
  for Lakebase. It mirrors `SqlExecutor`'s public surface: `execute`,
  `query`, `query_dicts`, `upsert`, plus the dialect helpers
  `q(identifier)`, `json_literal_expr(json_str)`, `ts_text(col)`. A
  background daemon thread refreshes the OAuth password every
  `DQX_LAKEBASE_TOKEN_REFRESH_MINUTES` minutes (default 50; tokens
  expire at 60). The pool's `kwargs["password"]` is mutated in place
  so subsequent connects pick up the new credential, and existing
  connections age out via `max_lifetime`.
- Services keep their `sql: SqlExecutor` annotation; the dependency
  injection layer (`dependencies.get_sp_oltp_executor`) hands back
  whichever executor is registered, casting to `SqlExecutor` because
  the two classes share an identical method surface.
- The `SchedulerService` accepts `oltp_sql: SqlExecutor | PgExecutor | None`
  and routes OLTP-table SQL (schedule configs, settings, rules) to
  the OLTP executor while keeping retention/GC against the Delta
  executor.

### Retention sweep (daily)

The scheduler runs a `DELETE` pass against the analytical tables once
per `_RETENTION_INTERVAL_HOURS` (24h). Two knobs, both stored in
`dq_app_settings` and surfaced via `GET/PUT /api/v1/config/retention`:

| Setting key                  | Default | Tables affected |
|------------------------------|--------:|-----------------|
| `retention_days`             | 90      | `dq_validation_runs`, `dq_profiling_results`, `dq_metrics`, plus the OLTP history tables (`dq_quality_rules_history`, `dq_schedule_configs_history`). Picked to match what trend dashboards expect. |
| `quarantine_retention_days`  | 30      | `dq_quarantine_records` only. Tighter because that table holds the full source row payload (PII surface). |

Both resolvers share a `_RETENTION_DAYS_MIN = 7` floor so a
mis-typed setting can never wipe data inside the safety window. Reads
swallow exceptions and fall back to the compiled-in default so a
SQL-warehouse hiccup never crashes the scheduler tick.

### Writing portable SQL inside services

Always go through the executor's dialect helpers — never hard-code
backticks, `parse_json(...)`, or `CAST(... AS STRING)`:

```python
self._sql.q("check")              # `check` (Delta) | "check" (Postgres)
self._sql.json_literal_expr(j)    # parse_json('...') | '...'::jsonb
self._sql.ts_text("created_at")   # CAST(created_at AS STRING) | created_at
```

For upserts, `SqlExecutor.upsert(table, key_cols, value_cols)` and
`PgExecutor.upsert` take the same arguments. Pass
`RawSql("current_timestamp()")` for timestamps — both backends rewrite
to their native syntax.

### Bundle / DAB conventions

Stateful resources declared in `databricks.yml` with
`lifecycle.prevent_destroy: true` (Databricks CLI 0.268+):

* `resources.schemas.main_schema` — `dqx_studio` schema
* `resources.schemas.tmp_schema` — `dqx_studio_tmp` schema
* `resources.volumes.wheels` — wheels volume
* `resources.postgres_projects.dqx_studio` — Lakebase Postgres project
  (autoscaling + scale-to-zero per [Lakebase Autoscaling](https://docs.databricks.com/aws/en/oltp/upgrade-to-autoscaling)),
  paired with `resources.postgres_roles.app_sp` (the app SP's Postgres role)

The app connects to the always-present `databricks_postgres` admin
database on the Lakebase project via the `DQX_LAKEBASE_ENDPOINT`
endpoint path (`projects/<project>/branches/<branch>/endpoints/primary`) —
`databricks_postgres` is the default value of `lakebase_database_name`.
The endpoint drives both host resolution (`postgres.get_endpoint`) and
OAuth credential issuance (`postgres.generate_database_credential`). On
first start, the app creates its own `dqx_studio` Postgres schema inside
`databricks_postgres` and runs migrations against it. Multiple apps can
therefore share the same `databricks_postgres` on one Lakebase project
safely; each gets its own schema namespace.

The bundle deliberately does NOT use `database_catalogs`. That DAB
resource is the only way to *create* a custom logical Postgres
database, but it also creates a Unity Catalog catalog as a side
effect and therefore requires `CREATE CATALOG` on the metastore — a
permission most app deployers don't hold. Connecting to the
pre-existing `databricks_postgres` instead keeps the bundle fully
declarative with no out-of-band bootstrap step and no metastore-level
permissions assumed.

`prevent_destroy` blocks `databricks bundle destroy` and any deploy
that would force-replace a bundle-managed resource — the alternative
is silent data loss. To intentionally tear one down: remove the flag,
run `databricks bundle deployment unbind <key>`, then destroy. The
app's `dqx_studio` Postgres schema lives below the resource layer
DABs models, so `prevent_destroy` doesn't apply to it directly; the
project-level guard is what protects it.

UC privileges for the app SP and task-runner SP are declared
**natively** as `grants:` on the schema/volume resources (via
`${resources.apps.dqx-studio.service_principal_client_id}` and
`${var.dqx_service_principal_application_id}`), so `bundle deploy`
applies them — there is no post-deploy grant script. The one manual
step is `USE CATALOG` on the pre-existing (user-selected) catalog,
which the bundle can't grant because it doesn't manage the catalog;
grant it once per catalog as a documented prerequisite (see
`DEPLOYMENT.md`).
