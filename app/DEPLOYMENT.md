# Deployment (Declarative Automation Bundles)

Production deployment uses [Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/) (DABs, formerly known as Databricks Asset Bundles) via the Databricks CLI (`databricks bundle deploy`). For local development, see [DEVELOPMENT.md](DEVELOPMENT.md).

## Prerequisites

Before you start, confirm you have **all** of the items below. The single most common deployment failure is missing one permission — and the error you see is almost always downstream of the missing grant, not on the grant itself.

### Tooling

- **Databricks CLI** v1.4.0+ installed and authenticated against your workspace (`databricks auth login -p <profile>`). `make app-deploy` enforces this via a preflight `app-check-cli` step (`databricks --version`) and aborts before building if the CLI is older. v1.4.0 is required because the `postgres_projects` / `postgres_roles` resources (used to provision Lakebase and the app SP's Postgres role) are only accepted by CLI ≥ 1.4.0; `lifecycle.prevent_destroy` itself needs only v0.268+.
- **`make`** (drives the one-command deploy target)
- **App build toolchain** — `make app-deploy` first runs `make app-build` to produce the wheel, which needs **uv**, **Node.js 18+** (provides `npm`; `brew install node` / nvm / [nodejs.org](https://nodejs.org/en/download)), **yarn** (`npm install -g yarn`), and **bun** (`curl -fsSL https://bun.sh/install | bash`). See [DEVELOPMENT.md → Prerequisites](DEVELOPMENT.md#prerequisites).

### Required permissions

The deploying user (you) needs the permissions below. They are **all** consumed by `make app-deploy`; deployment will halt the first time it hits a missing one. We've listed which step in the flow each permission unblocks so you can debug surgically if a grant gets missed.

| # | Permission | Granted on | Used by | What fails without it |
|---|---|---|---|---|
| 1 | **Workspace access** entitlement | You, in the workspace | All CLI calls | `databricks` CLI can't reach the workspace |
| 2 | **Databricks SQL access** entitlement | You, in the workspace | `bundle deploy` calling the create-warehouse API (the bundle always manages its own warehouse) | `Error: not authorized to create SQL Endpoint` |
| 3 | **Allow cluster create** entitlement | You, in the workspace | `bundle deploy` for the warehouse and job compute | Warehouse / job creation rejected |
| 4 | **Databricks Apps: Can Manage** workspace permission | You, in the workspace | `bundle deploy` of the App resource | App creation rejected |
| 5 | **Databricks Database (Lakebase): Manager** entitlement | You, in the workspace | `bundle deploy` of the `postgres_projects` / `postgres_roles` resources | `Error: User does not have permission to create database instances` |
| 6 | **USE CATALOG** + **CREATE SCHEMA** on `<catalog_name>` | Your user or an admin group you're in | `bundle deploy` of the `schemas` and `volumes` resources | `Error: User does not have CREATE_SCHEMA on catalog '<catalog>'` |
| 7 | **MANAGE** on `<catalog_name>` (or be the catalog owner) | Your user or an admin group you're in | The one-time `GRANT USE CATALOG` prerequisite (the bundle can't grant catalog-level access on a catalog it doesn't manage — see [The USE CATALOG prerequisite](#the-use-catalog-prerequisite)) | `Error: User does not have privilege MANAGE on catalog '<catalog>'` |
| 8 | **Service Principal: User** role on the task-runner SP | Your user, on the SP you'll use as `dqx_service_principal_application_id` | `bundle deploy` of the `jobs.dqx_task_runner` resource (sets `run_as.service_principal_name`) | `Error: User is not authorized to use this service principal` |
| 9 | **Service Principal: Manager** role on the task-runner SP, *or* a pre-shared OAuth client secret | Your user, on the same SP | Only needed if you want to **mint a fresh OAuth secret yourself** for the task-runner (e.g. via `databricks service-principal-secrets-proxy create <sp-id>`) | `Error: User is not authorized to perform this operation` when minting a new secret |
| 10 | **Account admin** (one-time, post-deploy) | Account level | Updating the app's OAuth custom-app integration to include the `all-apis` scope (see [Expand OAuth Scopes](#optional-expand-oauth-scopes)) | Some app features (job submission, advanced SCIM lookups) return 403 |

**Two convenience patterns** that reduce the per-user grants in rows 6 and 7:

- **Make the catalog ownership easy:** ask an admin to add you to an existing UC-admin group that already holds `MANAGE` (or `ALL PRIVILEGES`) on `<catalog_name>`. This unlocks rows 6 and 7 in one membership change instead of two per-object grants.
- **Workspace admin shortcut:** if you become workspace admin, rows 1–5 + 8–9 collapse automatically. Rows 6 and 7 (UC) and 10 (account admin) still need to be granted explicitly — workspace admin does **not** confer Unity Catalog or account-level rights.

### Workspace features that must be enabled

These are configured at the workspace or account level — not by you, not by the bundle. Confirm with your admin before the first deploy:

- **Databricks Apps** is enabled on the workspace
- **User token passthrough** (a.k.a. user authorization / OBO) is enabled for Databricks Apps — see [Step 2](#step-2-enable-user-token-passthrough). Without this the app can't make OBO calls and Unity Catalog browsing fails.
- **Serverless compute** is enabled on the workspace — the task-runner job runs exclusively on serverless
- **Lakebase Postgres** is enabled on the workspace (default OLTP backend). Lakebase is declared as a Postgres *project* bundle resource (`resources.postgres_projects.dqx_studio`, plus `resources.postgres_roles.app_sp` for the app SP's role) with `lifecycle.prevent_destroy: true` so a `bundle destroy` cannot drop it and wipe OLTP state — see [Stateful storage and destroy protection](#step-3-stateful-storage-and-destroy-protection). The app connects to the always-present `databricks_postgres` admin database via the project endpoint (`DQX_LAKEBASE_ENDPOINT`) and creates its own `dqx_studio` Postgres schema inside it on first connection — no separate logical-DB provisioning step.

### The catalog must already exist

The bundle **does not create the catalog itself** — that's deliberate. Catalogs are typically owned by a governance team and creating them requires `CREATE CATALOG` on the metastore. Pick an existing catalog you (or an admin group you're in) have rights on, and set `catalog_name` in [Step 4](#step-4-configure-databricksyml). The bundle creates the schemas (`dqx_studio`, `dqx_studio_tmp`) and the wheels volume *inside* that catalog — no `CREATE CATALOG` permission required at the metastore level.

## Step 1: Create a Service Principal

The bundle requires a service principal to run the task-runner job. This is separate from the app's auto-created SP — Jobs require a workspace-level SP as the `run_as` identity because the app-scoped SP cannot be used outside the Apps framework.

**Create a new SP:**
1. Go to **Settings → Identity and Access → Service Principals**
2. Click **Add service principal → Create new**
3. Give it a name (e.g., `dqx-task-runner-sp`)
4. Note the **Application ID** — you'll use it in [Step 4](#step-4-configure-databricksyml) as `dqx_service_principal_application_id`
5. **Grant yourself (or the identity you'll deploy the bundle with) the `User` role on this new SP.** Open the SP you just created, go to the **Permissions** tab, click **Add permissions**, search for your user (or deploy-time principal), and assign the role **`User`** (equivalent to `servicePrincipal.user` in the SCIM API).

   This lets your deploying identity configure jobs with `run_as: service_principal_name` pointing at this SP. Without it, `databricks bundle deploy` will fail with a permission error when it tries to set up the task-runner job.

**Find an existing SP's Application ID:**
```bash
databricks service-principals list -p <your-profile>
```

## Step 2: Enable User Token Passthrough

The app uses On-Behalf-Of (OBO) tokens to access Unity Catalog resources with the end user's identity. This requires the **Databricks Apps user token passthrough** feature to be enabled on your workspace.

Contact your workspace admin or enable it via the workspace settings if not already active.

## Step 3: Stateful storage and destroy protection

DQX Studio's stateful resources — the two schemas (`dqx_studio`, `dqx_studio_tmp`), the wheels volume, and the Lakebase Postgres project — are all declared with `lifecycle.prevent_destroy: true` (Databricks CLI 0.268+), which **blocks `databricks bundle destroy` from dropping the resource** and wiping the data. All are declared at the base level in `app/databricks.yml`:

```bash
grep -A1 'lifecycle:' app/databricks.yml | head
```

You'll see `prevent_destroy: true` on `schemas.main_schema`, `schemas.tmp_schema`, `volumes.wheels`, and `postgres_projects.dqx_studio`.

> **The app's `dqx_studio` Postgres schema** (inside the `databricks_postgres` admin database on the Lakebase project) is created by the app at first start. It's stateful but lives below the resource layer DABs models, so `prevent_destroy` doesn't apply to it directly. The project-level guard above is what protects it: as long as `postgres_projects.dqx_studio` survives, the schema and its tables survive.

What this means in practice:

- **Deploy** — `make app-deploy` does everything in one command: `databricks bundle deploy` provisions the schemas → volume → Lakebase project (+ endpoint + the app SP's Postgres role) → SQL warehouse → job → app in dependency order, and applies all Unity Catalog grants **natively** (no post-deploy script). Then `bundle run` starts the app.
- **Schema drift** — if you change `catalog_name`, `schema_name`, or the Lakebase project id in a way that would force the bundle to delete and recreate the resource, `prevent_destroy` blocks the destroy step and the deploy fails fast (good — the alternative is silent data loss). Treat those names as immutable.
- **Intentional teardown** — to drop a protected resource, remove `lifecycle.prevent_destroy: true` from `databricks.yml`, run `databricks bundle deployment unbind <key> -t <target>` to detach it from bundle state, then destroy it manually.

### The USE CATALOG prerequisite

`bundle deploy` applies every schema- and volume-level grant natively (via `grants:` on the resources). The **one** privilege it cannot grant is `USE CATALOG` on your chosen catalog — the bundle does not manage the (pre-existing, user-selected) catalog, so it has no handle to grant catalog-level access on it. Grant it once per catalog (the app SP's client id is shown by `databricks apps get dqx-studio` after the first deploy):

```sql
GRANT USE CATALOG ON CATALOG <catalog> TO `account users`;
GRANT USE CATALOG ON CATALOG <catalog> TO `<app-sp-client-id>`;
GRANT USE CATALOG ON CATALOG <catalog> TO `<task-runner-sp-application-id>`;
```

This is the **only** manual grant in the whole deployment. Everything else (ALL PRIVILEGES on the schemas + volume for both SPs, USE SCHEMA + CREATE TABLE on the tmp schema for `account users`, the deployer's dashboard SELECT, and warehouse `CAN_USE`) is declared in `databricks.yml` and applied by `bundle deploy`.

## Step 4: Configure `databricks.yml`

Add or update a deploy target. Only two variables are **required**:

- `catalog_name` — the existing Unity Catalog catalog to create schemas/volume in
- `dqx_service_principal_application_id` — the task-runner SP from [Step 1](#step-1-create-a-service-principal)

The bundle manages its own SQL warehouse and Lakebase project, so there is nothing else to supply. Everything else has a sensible default and can be overridden per target. The repo ships a single canonical target, **`dev`** (marked `default: true`). A minimal target looks like this:

```yaml
targets:
  dev:
    default: true
    workspace:
      profile: <your-profile>
    variables:
      catalog_name: <your-catalog>
      dqx_service_principal_application_id: <your-sp-application-id>
    presets:
      trigger_pause_status: PAUSED
```

### SQL warehouse

The bundle **always** creates and manages a dedicated serverless warehouse (`resources.sql_warehouses.dqx_sql_warehouse`) — there is no "bring your own warehouse" mode. Its `permissions:` block grants `CAN_USE` to the app SP (explicitly, so the app binding doesn't override it) and to `account users` (for end-user OBO dry-run/preview queries), all applied by `bundle deploy`. Tune it per target with `sql_warehouse_name` and `sql_warehouse_size`; `bundle destroy` deletes it.

### Lakebase

Lakebase is a bundle-managed Postgres **project** (`resources.postgres_projects.dqx_studio`) plus the app SP's Postgres role (`resources.postgres_roles.app_sp`, a `DATABRICKS_SUPERUSER` member so the app can create its own schema). The project auto-creates its default branch (`lakebase_branch`, default `dqx`) and a `primary` read/write endpoint; the app connects via that endpoint path (`DQX_LAKEBASE_ENDPOINT`). The endpoint scales to zero after `lakebase_suspend_timeout` of inactivity — the app's connection pool pre-pings on checkout and transparently reconnects (waking the endpoint) on the next request.

To run **without** Lakebase (OLTP on Delta): remove the `postgres_projects` and `postgres_roles` blocks from `databricks.yml` and set `lakebase_endpoint: "-"`.

> **The default warehouse and Lakebase sizes are deliberately small.** The bundle ships a `Small` SQL warehouse and a 0.5–1 CU autoscaling, scale-to-zero Lakebase project — sized for a typical rules catalog (low-thousands of rows) and light concurrent use, and chosen to keep idle cost near zero. They are a sensible **starting point, not a tuned production configuration.** Watch the app logs and the warehouse / Lakebase metrics under real load and raise `sql_warehouse_size`, `lakebase_max_cu`, or `DQX_LAKEBASE_POOL_MAX_SIZE` if you see query queueing or connection-pool exhaustion (see [Troubleshooting](#troubleshooting)).

### Variable reference

All target-level variables, their defaults, and what they control:

| Variable | Default | Required? | Purpose |
|---|---|---|---|
| `catalog_name` | `dqx` | **Yes** | Unity Catalog catalog where schemas and the wheels volume are created. **Must already exist** — the bundle does not create the catalog itself. |
| `dqx_service_principal_application_id` | `00000000-…` | **Yes** | Application ID of the service principal that runs the task-runner job. Created in [Step 1](#step-1-create-a-service-principal). The placeholder default fails validation. |
| `admin_group` | `proj_dbw_dev_dg_admins-data_ug` (bundle) / unset (local Python) | Yes for prod | Workspace group whose members get the in-app `ADMIN` role unconditionally (bootstrap admin path). The bundle ships with a non-production placeholder — override per target with your real admin group (e.g. `dqx-admins-prod`). Locally, `AppConfig` defaults to `None` and skips bootstrap admin assignment; set `DQX_ADMIN_GROUP` in your shell if you need it for local testing. Additional roles are assigned at runtime via the in-app Role Management UI. |
| `app_name` | `dqx-studio` | No | Deployed Databricks App name. Override per target (e.g. `dqx-studio-dev`, `dqx-studio-prod`) when deploying multiple targets to the same workspace, or for personal sandboxes. |
| `sql_warehouse_name` | `dqx-studio-sql-warehouse` | No | Name of the bundle-managed SQL warehouse. Override per target to avoid duplicates in shared workspaces. |
| `sql_warehouse_size` | `Small` | No | Cluster size of the bundle-managed warehouse (e.g. `2X-Small`, `Small`, `Medium`). |
| `schema_name` | `dqx_studio` | No | Main schema — holds run history, profiling, metrics, quarantine, and OLTP fallback tables. Declared as `resources.schemas.main_schema` in the bundle with `lifecycle.prevent_destroy: true`. |
| `tmp_schema_name` | `dqx_studio_tmp` | No | Per-user temp-view schema. Declared as `resources.schemas.tmp_schema` with `lifecycle.prevent_destroy: true`. |
| `wheels_volume_name` | `wheels` | No | UC volume under `<catalog>.<schema_name>` for the DQX + task-runner wheels. Declared as `resources.volumes.wheels` with `lifecycle.prevent_destroy: true`. |
| `lakebase_project_id` | `dqx-studio-db` | No | Lakebase Postgres project id for OLTP state. Declared as `resources.postgres_projects.dqx_studio` with `lifecycle.prevent_destroy: true`. Autoscaling + scale-to-zero per [Lakebase Autoscaling](https://docs.databricks.com/aws/en/oltp/upgrade-to-autoscaling). |
| `lakebase_branch` | `dqx` | No | Project branch the app uses; auto-created with a `primary` endpoint on first deploy. |
| `lakebase_endpoint` | `projects/<project>/branches/<branch>/endpoints/primary` | No | Endpoint resource path (`DQX_LAKEBASE_ENDPOINT`) driving host resolution + OAuth. Derived from project + branch. Set to `-` (and remove the postgres_projects/roles blocks) to disable Lakebase. |
| `lakebase_database_name` | `databricks_postgres` | No | Logical Postgres database inside the Lakebase instance the app connects to. Defaults to `databricks_postgres` (always present, no provisioning step). All DQX tables live in a dedicated `dqx_studio` Postgres schema inside this database, so multiple apps can safely share the same `databricks_postgres` on one Lakebase instance. Override only if you've manually created a different logical DB you want to use. |
| `lakebase_min_cu` / `lakebase_max_cu` | `0.5` / `1` | No | Autoscaling compute-unit range for the project endpoint. Raise the max if Lakebase queries queue in the app logs. |
| `lakebase_suspend_timeout` | `300s` | No | Idle window before the endpoint scales to zero (60s–604800s). The app pre-pings and reconnects transparently on the next request after suspension. |

> **Note on duplicate names in Databricks:** SQL warehouses, jobs, and apps within the same workspace are tracked by ID, not by name, so technically duplicates are allowed. Operators browse the Jobs / Apps / Warehouses / Databases UI by name, so distinct names per target are strongly recommended when you deploy more than one target to the same workspace.

## Step 5: One-Command Deploy (recommended)

Build, deploy, and start the app in a single command:

```bash
make app-deploy PROFILE=<your-profile> TARGET=<your-target>
```

`make app-deploy` runs the following steps automatically:
1. `make app-build` — builds the frontend and wheels.
2. `databricks bundle deploy` — provisions or updates the schemas, wheels volume, Lakebase project (+ endpoint + the app SP's Postgres role), the SQL warehouse, the task-runner job, and the Databricks App in dependency order, and applies **all Unity Catalog grants natively** via the `grants:` / `permissions:` blocks in `databricks.yml`. Stateful resources carry `lifecycle.prevent_destroy: true` so a future destroy can't drop them — see [Step 3](#step-3-stateful-storage-and-destroy-protection).
3. `databricks bundle run` — starts the app.

Remember the one manual prerequisite: [`GRANT USE CATALOG`](#the-use-catalog-prerequisite) on your catalog to the app SP, task-runner SP, and `account users` (the bundle can't grant it because it doesn't manage the catalog).

> **First start**: The app runs both Delta and Lakebase database migrations on startup, and uploads DQX wheels to the UC volume. If the task-runner job runs before the app has started at least once, it will fail to find its wheels. Wait for `"Uploaded databricks_labs_dqx-<version>..."` in the logs before triggering runs. If Lakebase is enabled, also wait for `"Lakebase OLTP routing enabled"` before opening the UI — when Lakebase is configured and init fails, the app refuses to start (logged as `"Lakebase initialisation failed ... Refusing to start"`) and the Apps platform will restart the container. Silent fallback to Delta is intentionally disallowed because it would split OLTP writes across two physical stores and orphan prior Lakebase data on every flap. To intentionally run on Delta only, unset `DQX_LAKEBASE_ENDPOINT`.

### Step-by-step alternative

If you prefer to run each step individually:

```bash
# Build
make app-build

# Deploy the bundle (creates / updates schemas, volume, Lakebase project +
# endpoint + role, the SQL warehouse, task-runner job, and app, and applies
# all UC grants natively)
cd app && databricks bundle deploy -p <your-profile> -t <your-target>

# Start the app
cd app && databricks bundle run dqx-studio -p <your-profile> -t <your-target>
```

### Grants reference

`bundle deploy` applies all of these natively — this section is just for reference / manual recovery. The **only** grant you must run by hand is [`USE CATALOG`](#the-use-catalog-prerequisite) (the bundle doesn't manage the catalog).

These are the UC grants `bundle deploy` applies from the `grants:` blocks in `databricks.yml` — reproduced here only so you can reapply them manually if you ever need to. `<app-sp-id>` is the app's auto-created SP (`databricks apps get dqx-studio` → `service_principal_client_id`); `<job-sp-id>` is the task-runner SP from [Step 1](#step-1-create-a-service-principal).

```sql
-- App SP + task-runner SP: full privileges on the schemas + volume
GRANT ALL PRIVILEGES ON SCHEMA <catalog>.dqx_studio     TO `<app-sp-id>`;
GRANT ALL PRIVILEGES ON SCHEMA <catalog>.dqx_studio_tmp TO `<app-sp-id>`;
GRANT ALL PRIVILEGES ON VOLUME <catalog>.dqx_studio.wheels TO `<app-sp-id>`;
GRANT ALL PRIVILEGES ON SCHEMA <catalog>.dqx_studio     TO `<job-sp-id>`;
GRANT ALL PRIVILEGES ON SCHEMA <catalog>.dqx_studio_tmp TO `<job-sp-id>`;
GRANT ALL PRIVILEGES ON VOLUME <catalog>.dqx_studio.wheels TO `<job-sp-id>`;

-- End users create dry-run / preview temp views (via their OBO token) in the
-- tmp schema, so they need USE SCHEMA + CREATE TABLE there.
GRANT USE SCHEMA, CREATE TABLE ON SCHEMA <catalog>.dqx_studio_tmp TO `account users`;

-- Deployer needs SELECT for the embed-credentials Insights dashboard
-- (bundle uses ${workspace.current_user.userName}).
GRANT USE SCHEMA, SELECT ON SCHEMA <catalog>.dqx_studio TO `<deployer>`;

-- USE CATALOG is the ONLY grant the bundle cannot apply (it doesn't manage the
-- catalog) — you must run this once per catalog. See "The USE CATALOG prerequisite".
GRANT USE CATALOG ON CATALOG <catalog> TO `account users`;
GRANT USE CATALOG ON CATALOG <catalog> TO `<app-sp-id>`;
GRANT USE CATALOG ON CATALOG <catalog> TO `<job-sp-id>`;
```

Warehouse `CAN_USE` (app SP + `account users`) is likewise applied natively via the `permissions:` block on the bundle-managed warehouse.

> **Lakebase access.** The app SP's Postgres role is created by the `postgres_roles.app_sp` resource with `DATABRICKS_SUPERUSER` membership, which confers the `CREATE SCHEMA` privilege the app needs on first connect (`PgMigrationRunner` runs `CREATE SCHEMA dqx_studio` inside `databricks_postgres`). This is fully declarative — a single `bundle deploy` provisions the project, the app, *and* the role, with no follow-up `psql`/console step. It requires **Databricks CLI v1.4.0 or newer**; on older CLIs the `postgres_projects` / `postgres_roles` fields are rejected at `bundle validate`.

To grant app access to end users, go to **Apps → `<app-name>` → Permissions** and assign `Can Use`. Replace `<app-name>` with the value of `app_name` configured for your target (default `dqx-studio`).

Access the app at:
```
https://<your-workspace-url>/apps/<app-name>
```

## Lakebase backend

DQX Studio stores its **OLTP state** — rules catalog, app settings, RBAC, comments, schedule configs, and scheduler bookkeeping — in a Lakebase Postgres instance for sub-millisecond reads and to avoid SQL warehouse cold starts. **Append-mostly observability tables** (`dq_validation_runs`, `dq_profiling_results`, `dq_metrics`, `dq_quarantine_records`) live in Delta because they're written by the Spark task runner and queried by AI/BI dashboards.

| Backend | Tables | Why |
|---|---|---|
| Delta Lake | `dq_validation_runs`, `dq_profiling_results`, `dq_quarantine_records`, `dq_metrics` | High-volume append; Spark task runner writes them; columnar reads. |
| Lakebase Postgres | `dq_app_settings`, `dq_role_mappings`, `dq_quality_rules`, `dq_quality_rules_history`, `dq_comments`, `dq_schedule_configs`, `dq_schedule_configs_history`, `dq_schedule_runs` | OLTP — sub-ms reads from FastAPI handlers, row-level upserts, primary keys. |

Lakebase is declared as a Postgres *project* bundle resource (`postgres_projects.dqx_studio`, plus `postgres_roles.app_sp`) and provisioned by `databricks bundle deploy` with `lifecycle.prevent_destroy: true` — see [Step 3](#step-3-stateful-storage-and-destroy-protection). The app connects to the always-present `databricks_postgres` admin database via the project endpoint (`DQX_LAKEBASE_ENDPOINT`) and creates its own `dqx_studio` Postgres schema there on first start; nothing else needs to be provisioned.

### Lakebase token rotation

Lakebase OAuth tokens expire after one hour. The app's `PgExecutor` runs a background daemon thread that refreshes the password every `DQX_LAKEBASE_TOKEN_REFRESH_MINUTES` minutes (default 50). Existing connections age out via `psycopg_pool.ConnectionPool.max_lifetime` so a long-running app can stay up indefinitely without reconnecting.

## (Optional) Expand OAuth Scopes

> **Most deployments don't need this step.** The OAuth scopes configured automatically by DABs (`sql`, `catalog.catalogs:read`, `catalog.schemas:read`, `catalog.tables:read`, `serving.serving-endpoints`) plus the identity scopes Databricks Apps grants implicitly are sufficient for all DQX Studio features on a standard workspace.
>
> Only follow this section if, after deploying, you see specific features returning `403` / permission errors in the app logs that look like missing OAuth scopes (for example, REST calls the baseline scopes do not cover). Expanding scopes requires **account admin** access.

The scopes are managed at the **account** level, not the workspace. You need a CLI profile authenticated against the accounts host.

**1. Log in at the account level:**

```bash
databricks auth login \
  --host https://accounts.cloud.databricks.com \
  --account-id <your-databricks-account-id> \
  --profile <account-profile-name>
```

Replace `accounts.cloud.databricks.com` with the account host for your cloud (`accounts.azuredatabricks.net` for Azure, `accounts.gcp.databricks.com` for GCP).

**2. Find the app's OAuth client ID:**

```bash
databricks account custom-app-integration list -p <account-profile-name>
```

Look for the integration whose name matches your deployed app (default `dqx-studio`, or whatever you set `app_name` to). Copy its `integration_id` — this is the `<oauth2-app-client-id>` used in the next step.

You can also find it in the workspace UI under **Apps → `<app-name>` → User authorization**.

**3. Update the OAuth scopes:**

```bash
databricks account custom-app-integration update '<oauth2-app-client-id>' \
  -p <account-profile-name> \
  --json '{
    "scopes": [
      "openid",
      "profile",
      "email",
      "all-apis",
      "offline_access",
      "iam.current-user"
    ]
  }'
```

**4. Restart the app** so the new scopes take effect:

```bash
databricks apps stop  <app-name> -p <your-profile>
databricks apps start <app-name> -p <your-profile>
```

After the app restarts, sign in again in the browser — you'll be prompted to re-consent to the expanded scopes.

> **Why this isn't a default step:** Declarative Automation Bundles can only configure a baseline set of scopes. Broader scopes (`all-apis`, `iam.current-user`) are governed at the account level and can only be set by an account admin. In practice, the baseline is sufficient for the DQX Studio features in tree — this section exists as a fallback for workspaces where stricter OAuth-integration defaults require explicit scope grants.

## Redeploying After Code Changes

```bash
make app-deploy PROFILE=<your-profile> TARGET=<your-target>
```

Or manually:
```bash
make app-build
cd app && databricks bundle deploy -p <your-profile> -t <your-target>
# The app restarts automatically after deployment
```

## Monitor and Manage

Replace `<app-name>` with the deployed app name (the value of `app_name` for your target — default `dqx-studio`):

```bash
databricks apps get <app-name> -p <your-profile>    # status
databricks apps logs <app-name> -p <your-profile>   # logs
databricks apps stop <app-name> -p <your-profile>   # stop
```

## Troubleshooting

**"App with name X does not exist or is deleted":**
```bash
rm -rf .databricks                                          # clean local bundle state
databricks bundle deploy -p <your-profile> --force          # or force deploy
```

**Profiler or dry-run not starting:**
1. Check `DQX_JOB_ID` is set (visible in the app's environment config in the UI)
2. Confirm the job exists: `databricks jobs list -p <your-profile>`
3. Confirm the SP has `CAN_MANAGE` on the job (set automatically by DABs)

**Job fails with "file not found" on wheel:**
The task-runner job installs wheels from the UC volume. If the volume is empty the job fails. Start the app and wait for the wheel upload to complete:
```bash
databricks apps logs <app-name> -p <your-profile>
# Look for: "Uploaded databricks_labs_dqx-<version>-py3-none-any.whl"
```

**App says `"schema dqx_studio does not exist"` (or similar) on first start:**
The schemas didn't deploy, or the bundle is pointing at a different catalog than the app. Confirm with `databricks bundle validate -p <profile> -t <target>` that `catalog_name` and `schema_name` resolve to the expected values, then redeploy:
```bash
make app-deploy PROFILE=<your-profile> TARGET=<your-target>
```

**App logs `"Lakebase initialisation failed ... Refusing to start"` and the container restart-loops:**
The app deliberately refuses to start when Lakebase is configured (`DQX_LAKEBASE_ENDPOINT` non-empty) and init fails — silently falling back to Delta would split OLTP writes across two physical stores and orphan prior Lakebase data. Diagnose with the steps below; the Apps platform will pick up the next successful start automatically.

1. Confirm the Lakebase project + endpoint exist and are running (Compute → Database Instances in the workspace UI). If missing, re-run `databricks bundle deploy`; if the endpoint is still `STARTING`, wait and the next restart will succeed. (A suspended endpoint is fine — the app's pre-ping pool wakes it on connect.)
2. Confirm the app SP's Postgres role exists on the project branch — it's created by the `postgres_roles.app_sp` resource. Redeploy if the role is missing.
3. If the failure is specifically a Postgres `permission denied for database databricks_postgres` (or `permission denied to create schema`), the app SP can connect but lacks `CREATE` on the system `databricks_postgres` database — that privilege comes from the `DATABRICKS_SUPERUSER` membership in `postgres_roles.app_sp`. Confirm that block deployed (CLI ≥ 1.4.0), or run a one-time `GRANT CREATE ON DATABASE databricks_postgres TO "<app-sp-client-id>"` against the project endpoint.
4. Confirm OAuth token issuance is healthy — Lakebase tokens currently expire after one hour; a misconfigured OAuth integration or revoked SP credential will surface here.
5. If you intentionally want to run on Delta only (no Lakebase), remove the `postgres_projects` / `postgres_roles` blocks and set `lakebase_endpoint: "-"`, then redeploy. The app will start in legacy UC-only mode and OLTP tables will live on Delta.

**`databricks bundle deploy` fails with `"already exists"` on the first deploy of a target:**
A schema, volume, or Lakebase project of the same name was created out-of-band. Either rename it via the corresponding variable (`schema_name`, `wheels_volume_name`, `lakebase_project_id`) or `databricks bundle deployment bind <key> <existing-id> -t <target>` to adopt the existing resource, then redeploy.

**`databricks bundle destroy` fails with `"cannot destroy resource: prevent_destroy is set"`:**
This is the safety guard doing its job — see [Step 3](#step-3-stateful-storage-and-destroy-protection). To intentionally tear down a stateful resource, remove `lifecycle.prevent_destroy: true` from the relevant block in `databricks.yml`, run `databricks bundle deployment unbind <key> -t <target>` to detach it from bundle state, then destroy it manually (`databricks schemas delete` / `databricks volumes delete`, and delete the Lakebase project from the workspace UI).

**Lakebase queries time out / app logs show pool exhaustion:**
Raise `lakebase_max_cu` in `databricks.yml` and redeploy. You can also raise `DQX_LAKEBASE_POOL_MAX_SIZE` (default 10) on the app's environment if many concurrent requests are hitting the OLTP path.

## Insights dashboard

The bundle ships a starter AI/BI dashboard (`dashboards/dqx_quality_overview.lvdash.json`) declared as `resources.dashboards.dqx_quality_overview` in `databricks.yml`. It's automatically created on deploy and pinned to the app's **Insights** page via the `DQX_DEFAULT_DASHBOARD_ID` env var, so the page works out-of-the-box.

**What you get**: a four-row layout with KPI counters (total runs, monitored tables, total errors, pass rate), trend charts (runs over time by status; errors & warnings over time), drilldowns (top failing tables; quarantined rows over time), and a recent-runs table.

**Customising the starter**: open it in **Databricks → AI/BI Dashboards**, add or change widgets, and save. The iframe inside DQX Studio picks up changes immediately — no redeploy needed. You can also point the Insights page at a completely different dashboard via **Configuration → Insights dashboard**; clearing that override reverts to the starter.

**Query identity**: the dashboard is configured with `embed_credentials: true`, so queries run as the bundle deployer rather than the iframe viewer. This is deliberate — the bundle only grants `USE CATALOG` (not table-level `SELECT`) to `account users`, keeping `dq_quarantine_records` (potentially PII row payloads) off the workspace UC surface. The widgets in the starter only expose aggregated counts and run metadata, so deployer-credentialed queries don't leak anything a viewer couldn't already see in the Runs History page. To switch to viewer-credentials, flip `embed_credentials` to `false` and grant `SELECT` on the DQX tables to the audience you want to expose.

The bundle grants the deployer `USE SCHEMA` + `SELECT ON SCHEMA <catalog>.<schema>` natively via a `grants:` entry that resolves `${workspace.current_user.userName}` at deploy time, so the dashboard works end-to-end without manual UC plumbing. It resolves for both human deploys (grants the email) and SP-based deploys (grants the application ID).

**One operational caveat**: the "deployer" identity is whoever ran `databricks bundle deploy` (the human or service principal authenticated to the workspace at deploy time). If that identity later loses access to the DQX tables, dashboard tiles will fail to render until someone with access redeploys. For production, deploy the bundle as a stable service principal so the dashboard identity doesn't follow individual humans.

## Run review status

DQX Studio lets reviewers attach a per-run **review status** (e.g. *Pending review*, *Acknowledged*, *Resolved*, *False positive*) to each validation run from the expanded row on the **Runs History** page. The same value is filterable from the toolbar so a business owner can ask "what's still pending?" in one click.

- **Configurable catalogue** — admins manage the list of allowed values (label, description, colour) under **Configuration → Run review statuses**. Exactly one entry must be marked **Default**; that value is what unreviewed runs surface virtually (no row is written until someone explicitly reviews). The backend enforces the single-default invariant on save.
- **Audit trail** — every change appends to `dq_run_review_status_history`, surfaced as an "Activity" timeline inside the review-status panel. The current value lives in `dq_run_review_status` (one row per reviewed run).
- **Storage** — both tables are OLTP-shaped (single-key lookups, frequent mutation), so they live in Lakebase when it's enabled and fall back to Delta otherwise via the same `oltp_fallback` plumbing used by comments and role mappings. No extra deployment configuration is needed.
- **Permissions** — any authenticated app user can set or change a review status, mirroring how comments work. Only admins can edit the catalogue itself.

