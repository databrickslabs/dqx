# Deployment (Declarative Automation Bundles)

Production deployment uses [Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/) (DABs, formerly known as Databricks Asset Bundles) via the Databricks CLI (`databricks bundle deploy`). For local development, see [DEVELOPMENT.md](DEVELOPMENT.md).

## Prerequisites

Before you start, confirm you have **all** of the items below. The single most common deployment failure is missing one permission — and the error you see is almost always downstream of the missing grant, not on the grant itself.

### Tooling

- **Databricks CLI** v0.268+ installed and authenticated against your workspace (`databricks auth login -p <profile>`). v0.268 is the minimum that supports `lifecycle.prevent_destroy` on bundle resources.
- **`jq`** (used by the post-deploy grants script and the resource-bind helper)
- **`make`** (drives the one-command deploy target)

### Required permissions

The deploying user (you) needs the permissions below. They are **all** consumed by `make app-deploy`; deployment will halt the first time it hits a missing one. We've listed which step in the flow each permission unblocks so you can debug surgically if a grant gets missed.

| # | Permission | Granted on | Used by | What fails without it |
|---|---|---|---|---|
| 1 | **Workspace access** entitlement | You, in the workspace | All CLI calls | `databricks` CLI can't reach the workspace |
| 2 | **Databricks SQL access** entitlement | You, in the workspace | `bundle deploy` — **only** when the target uses the *managed* SQL warehouse pattern (see [SQL warehouse: managed vs. BYO](#sql-warehouse-managed-vs-byo)). Not needed for BYO. | `Error: not authorized to create SQL Endpoint` |
| 3 | **Allow cluster create** entitlement | You, in the workspace | `bundle deploy` — only for the managed warehouse + job clusters. Not needed for BYO warehouse. | Warehouse / job creation rejected |
| 4 | **Databricks Apps: Can Manage** workspace permission | You, in the workspace | `bundle deploy` of the App resource | App creation rejected |
| 5 | **Databricks Database (Lakebase): Manager** entitlement | You, in the workspace | `bundle deploy` of the `database_instances` resource | `Error: User does not have permission to create database instances` |
| 6 | **USE CATALOG** + **CREATE SCHEMA** on `<catalog_name>` | Your user or an admin group you're in | `bundle deploy` of the `schemas` and `volumes` resources | `Error: User does not have CREATE_SCHEMA on catalog '<catalog>'` |
| 7 | **MANAGE** on `<catalog_name>` (or be the catalog owner) | Your user or an admin group you're in | `post_deploy_grants.sh` (issues `GRANT USE CATALOG / ALL PRIVILEGES … TO <app SP>` and `… TO account users`) | `Error: User does not have privilege MANAGE on catalog '<catalog>'` |
| 8 | **CAN_MANAGE** on the SQL warehouse | Your user, on the warehouse bound to the app | `post_deploy_grants.sh` (PATCHes the warehouse permissions API to grant `CAN_USE` to the app SP and task-runner SP). For the *managed* pattern this is automatic — you become the warehouse owner. For the *BYO* pattern an admin must grant you CAN_MANAGE on the existing warehouse. | Logged as `WARNING: PERMISSION_DENIED` from the grants script; UC grants still succeed, but the SPs won't be able to use the warehouse until granted CAN_USE another way. |
| 9 | **Service Principal: User** role on the task-runner SP | Your user, on the SP you'll use as `dqx_service_principal_application_id` | `bundle deploy` of the `jobs.dqx_task_runner` resource (sets `run_as.service_principal_name`) | `Error: User is not authorized to use this service principal` |
| 10 | **Service Principal: Manager** role on the task-runner SP, *or* a pre-shared OAuth client secret | Your user, on the same SP | Only needed if you want to **mint a fresh OAuth secret yourself** for the task-runner (e.g. via `databricks service-principal-secrets-proxy create <sp-id>`) | `Error: User is not authorized to perform this operation` when minting a new secret |
| 11 | **Account admin** (one-time, post-deploy) | Account level | Updating the app's OAuth custom-app integration to include the `all-apis` scope (see [Expand OAuth Scopes](#optional-expand-oauth-scopes)) | Some app features (job submission, advanced SCIM lookups) return 403 |

**Two convenience patterns** that reduce the per-user grants in rows 6, 7, and 8:

- **Make the catalog ownership easy:** ask an admin to add you to an existing UC-admin group that already holds `MANAGE` (or `ALL PRIVILEGES`) on `<catalog_name>`. This unlocks rows 6 and 7 in one membership change instead of two per-object grants.
- **Workspace admin shortcut:** if you become workspace admin, rows 1–5 + 9–10 collapse automatically, and row 8 collapses on any **managed** SQL warehouse (you'll own it). Rows 6 and 7 (UC) and 11 (account admin) still need to be granted explicitly — workspace admin does **not** confer Unity Catalog or account-level rights. For a **BYO** warehouse, row 8 (CAN_MANAGE on that specific warehouse) must still be granted by its current owner.

### Workspace features that must be enabled

These are configured at the workspace or account level — not by you, not by the bundle. Confirm with your admin before the first deploy:

- **Databricks Apps** is enabled on the workspace
- **User token passthrough** (a.k.a. user authorization / OBO) is enabled for Databricks Apps — see [Step 2](#step-2-enable-user-token-passthrough). Without this the app can't make OBO calls and Unity Catalog browsing fails.
- **Serverless compute** is enabled on the workspace — the task-runner job runs exclusively on serverless
- **Lakebase Postgres** is enabled on the workspace (default OLTP backend). The Lakebase instance is declared as a bundle resource (`resources.database_instances.lakebase`) with `lifecycle.prevent_destroy: true` so a `bundle destroy` cannot drop it and wipe OLTP state — see [Stateful storage and destroy protection](#step-3-stateful-storage-and-destroy-protection). The app connects to the always-present `databricks_postgres` admin database on the instance and creates its own `dqx_studio` Postgres schema inside it on first connection — no separate logical-DB provisioning step.

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

DQX Studio's stateful resources — the two schemas (`dqx_studio`, `dqx_studio_tmp`), the wheels volume, and the Lakebase instance — are all declared as bundle resources in `app/databricks.yml`. Each one carries `lifecycle.prevent_destroy: true` (Databricks CLI 0.268+), which **blocks `databricks bundle destroy` from dropping the resource** and wiping the data. Use this command line to verify:

```bash
grep -A1 'lifecycle:' app/databricks.yml | head
```

You'll see one `prevent_destroy: true` for each of: `schemas.main_schema`, `schemas.tmp_schema`, `volumes.wheels`, `database_instances.lakebase`.

> **The app's `dqx_studio` Postgres schema** (inside the `databricks_postgres` admin database on the Lakebase instance) is created by the app at first start. It's stateful but lives below the resource layer DABs models, so `prevent_destroy` doesn't apply to it directly. The instance-level guard above is what protects it: as long as `database_instances.lakebase` survives, the schema and its tables survive.

What this means in practice:

- **Fresh workspace** — `make app-deploy` does everything in one command: `databricks bundle deploy` provisions the schemas → volume → Lakebase instance → SQL warehouse → job → app in dependency order, then `post_deploy_grants.sh` issues catalog/schema/volume GRANTs.
- **Existing workspace** where these resources were created out-of-band (e.g. from a previous version of this app that used a bootstrap script) — you must **bind** them into bundle management once per target. See [Migrating an existing workspace](#migrating-an-existing-workspace).
- **Schema drift** — if you change `catalog_name`, `schema_name`, or `lakebase_instance_name` in a way that would force the bundle to delete and recreate the resource, `prevent_destroy` blocks the destroy step and the deploy fails fast (good — the alternative is silent data loss). Treat those names as immutable.
- **Intentional teardown** — to drop a protected resource, remove `lifecycle.prevent_destroy: true` from `databricks.yml`, run `databricks bundle deployment unbind <key> -t <target>` to detach it from bundle state, then destroy it manually.

### Migrating an existing workspace

If the workspace was previously deployed with the old bootstrap-script flow (or if the resources were created manually), `databricks bundle deploy` will fail with "already exists" / "Instance name is not unique" on the first run because it's trying to CREATE resources that already exist. Fix this in one command:

```bash
make app-bind PROFILE=<your-profile> TARGET=<your-target>
```

`make app-bind` invokes `app/scripts/bind_resources.sh`, which calls `databricks bundle deployment bind` for each stateful resource (one bind per target). After bind, `bundle deploy` sees the resource as already-managed and does diff-and-update instead of CREATE. Bind is a one-time operation per target — once bound, subsequent deploys don't need it. Re-running `make app-bind` on a fully-bound target is a safe no-op.

## Step 4: Configure `databricks.yml`

Update a deploy target. The minimum required is a `catalog_name`, `dqx_service_principal_application_id`, and a SQL warehouse choice (see below); everything else has a sensible default and can be overridden per target. In `app/databricks.yml`:

```yaml
targets:
  dev:
    workspace:
      profile: <your-profile>
    variables:
      catalog_name: <your-catalog>
      dqx_service_principal_application_id: <your-sp-application-id>
      # Pick ONE of the two SQL warehouse patterns below
      # — see "SQL warehouse: managed vs. BYO" right after this block.
    presets:
      trigger_pause_status: PAUSED
```

### SQL warehouse: managed vs. BYO

The app needs exactly one SQL warehouse to run UC queries against the Delta side. The bundle supports two patterns — pick whichever matches the permissions you actually have. Both patterns end up at the same place: the warehouse ID is set into the `sql_warehouse_id` variable, the app binds to it, and `post_deploy_grants.sh` grants `CAN_USE` to the app SP and the task-runner SP.

**Pattern A — Managed warehouse (default for `dev`, `kaizen-dev`, `e2-demo`, `bdf-vo`)**

The bundle declares an X-Small serverless warehouse as `resources.sql_warehouses.dqx_sql_warehouse` inside the target, and `sql_warehouse_id` is wired to `${resources.sql_warehouses.dqx_sql_warehouse.id}`. Nothing for you to fill in.

Required permissions on top of the table above: rows 2 (Databricks SQL access) and 3 (Allow cluster create). The deploying user automatically becomes the warehouse owner, so row 8 (CAN_MANAGE) is satisfied for free.

**Pattern B — Bring Your Own (BYO) warehouse (default for `kaizen-app`)**

Use this when you don't have permission to create SQL warehouses, or when your platform team standardises on a single shared warehouse. The target keeps `sql_warehouse_id` set to an existing warehouse ID, and the base-level `sql_warehouses` resource is *not* declared.

```yaml
targets:
  kaizen-app:
    workspace:
      profile: <your-profile>
    variables:
      catalog_name: <your-catalog>
      dqx_service_principal_application_id: <your-sp-application-id>
      sql_warehouse_id: <existing-warehouse-id>   # e.g. "abc123def"
    presets:
      trigger_pause_status: PAUSED
```

Required permissions on top of the table above: row 8 (CAN_MANAGE on that existing warehouse) so the post-deploy script can apply `CAN_USE` grants to both service principals. Rows 2 and 3 are not needed — the bundle doesn't try to create or refresh a warehouse.

> **What happens if the deployer only has CAN_USE on the BYO warehouse?** The bundle deploy itself still succeeds (the app and job bind to the warehouse ID without touching its permissions). `post_deploy_grants.sh` logs one `WARNING: PERMISSION_DENIED` per missing grant and continues; UC grants still apply. The fix is to either ask the warehouse owner to grant you CAN_MANAGE and re-run `make app-grant-permissions`, or to have them grant `CAN_USE` directly to the app SP and the task-runner SP. Until both SPs hold CAN_USE on the warehouse, any feature in the app that issues SQL via that warehouse will fail with `not authorized to use or monitor this SQL Endpoint`.

### Variable reference

All target-level variables, their defaults, and what they control:

| Variable | Default | Required? | Purpose |
|---|---|---|---|
| `catalog_name` | `dqx` | **Yes** | Unity Catalog catalog where schemas and the wheels volume are created. **Must already exist** — the bundle does not create the catalog itself. |
| `dqx_service_principal_application_id` | `00000000-…` | **Yes** | Application ID of the service principal that runs the task-runner job. Created in [Step 1](#step-1-create-a-service-principal). The placeholder default fails validation. |
| `admin_group` | `admins` | No | Workspace group whose members get the in-app `ADMIN` role unconditionally (bootstrap admin path). The default `admins` is the built-in workspace admins group — every workspace admin becomes a DQX admin automatically. Override with a dedicated group (e.g. `dqx-admins-prod`) for narrower bootstrap access. Additional roles are assigned at runtime via the in-app Role Management UI. |
| `app_name` | `dqx-studio` | No | Deployed Databricks App name. Override per target (e.g. `dqx-studio-dev`, `dqx-studio-prod`) when deploying multiple targets to the same workspace, or for personal sandboxes. |
| `sql_warehouse_id` | `${resources.sql_warehouses.dqx_sql_warehouse.id}` (in managed targets) | **Yes** for BYO targets, **No** for managed targets | The warehouse ID the app and task runner connect to. In managed targets this is wired to the bundle-created warehouse. In BYO targets (e.g. `kaizen-app`) set it to the existing warehouse ID you want to share. See [SQL warehouse: managed vs. BYO](#sql-warehouse-managed-vs-byo). |
| `sql_warehouse_name` | `dqx-studio-sql-warehouse` | No | Name of the bundle-created warehouse — **managed pattern only** (ignored under BYO). Override per target to avoid name clashes in shared workspaces. |
| `schema_name` | `dqx_studio` | No | Main schema — holds run history, profiling, metrics, quarantine, and OLTP fallback tables. Declared as `resources.schemas.main_schema` in the bundle with `lifecycle.prevent_destroy: true`. |
| `tmp_schema_name` | `dqx_studio_tmp` | No | Per-user temp-view schema. Declared as `resources.schemas.tmp_schema` with `lifecycle.prevent_destroy: true`. |
| `wheels_volume_name` | `wheels` | No | UC volume under `<catalog>.<schema_name>` for the DQX + task-runner wheels. Declared as `resources.volumes.wheels` with `lifecycle.prevent_destroy: true`. |
| `lakebase_instance_name` | `dqx-studio-lakebase` | No | Lakebase Postgres instance for OLTP state. Declared as `resources.database_instances.lakebase` with `lifecycle.prevent_destroy: true`. Autoscaling by default per [Lakebase Autoscaling](https://docs.databricks.com/aws/en/oltp/upgrade-to-autoscaling). |
| `lakebase_database_name` | `databricks_postgres` | No | Logical Postgres database inside the Lakebase instance the app connects to. Defaults to `databricks_postgres` (always present, no provisioning step). All DQX tables live in a dedicated `dqx_studio` Postgres schema inside this database, so multiple apps can safely share the same `databricks_postgres` on one Lakebase instance. Override only if you've manually created a different logical DB you want to use. |
| `lakebase_capacity` | `CU_1` | No | Lakebase compute capacity. Valid values: `CU_1`, `CU_2`, `CU_4`, `CU_8`. To resize an existing instance, change this value and redeploy. Bump up if Lakebase queries queue in the app logs. |

> **Note on duplicate names in Databricks:** SQL warehouses, jobs, and apps within the same workspace are tracked by ID, not by name, so technically duplicates are allowed. Lakebase database instances are looked up by name by the app at runtime, so they're effectively unique-per-workspace. Operators browse the Jobs / Apps / Warehouses / Databases UI by name, so distinct names per target are strongly recommended when you deploy more than one target to the same workspace.

## Step 5: One-Command Deploy (recommended)

Build, deploy, grant permissions, and start the app in a single command:

```bash
# Existing workspace where the storage was previously bootstrapped
# out-of-band (e.g. with the older bootstrap script): bind once.
make app-bind PROFILE=<your-profile> TARGET=<your-target>

# Every deploy (fresh or otherwise):
make app-deploy PROFILE=<your-profile> TARGET=<your-target>
```

`make app-deploy` runs the following steps automatically:
1. `make app-build` — builds the frontend and wheels.
2. `databricks bundle deploy` — provisions or updates the schemas, wheels volume, Lakebase instance, task-runner job, and Databricks App in dependency order, plus the SQL warehouse if the target uses the managed pattern. Stateful resources carry `lifecycle.prevent_destroy: true` so a future destroy can't drop them — see [Step 3](#step-3-stateful-storage-and-destroy-protection).
3. `app/scripts/post_deploy_grants.sh` — discovers both service principals, then (a) executes `GRANT` statements on the catalog / schemas / volume, and (b) PATCHes the SQL warehouse permissions to add `CAN_USE` for both SPs. The grants script is idempotent and re-runnable. The auto-created app SP's UUID isn't known at bundle-write time, which is why all grants live in the post-deploy script. Lakebase grants are handled directly by the bundle's `database` resource binding.
4. `databricks bundle run` — starts the app.

> **First start**: The app runs both Delta and Lakebase database migrations on startup, and uploads DQX wheels to the UC volume. If the task-runner job runs before the app has started at least once, it will fail to find its wheels. Wait for `"Uploaded databricks_labs_dqx-<version>..."` in the logs before triggering runs. If Lakebase is enabled, also wait for `"Lakebase OLTP routing enabled"` before opening the UI — the app falls back to UC-only mode if Lakebase init fails (logged as `"Lakebase initialisation failed — falling back to Delta for OLTP tables"`).

### Step-by-step alternative

If you prefer to run each step individually:

```bash
# Build
make app-build

# (One-time, only on a workspace whose storage was created out-of-band)
make app-bind PROFILE=<your-profile> TARGET=<your-target>

# Deploy the bundle (creates / updates schemas, volume, Lakebase
# instance, task-runner job, app — plus the SQL warehouse in
# managed targets; BYO targets reuse the warehouse ID you set)
cd app && databricks bundle deploy -p <your-profile> -t <your-target>

# Grant permissions to the app SP and task-runner SP (auto-discovered
# after deploy): UC catalog/schema/volume grants + warehouse CAN_USE.
make app-grant-permissions PROFILE=<your-profile> TARGET=<your-target>

# Start the app
cd app && databricks bundle run dqx-studio -p <your-profile> -t <your-target>
```

### Manual grants (if the script doesn't work for your setup)

The grant script discovers both SPs automatically. If you need to apply the grants by hand instead, you need to do **two** things — the UC grants (SQL) and the warehouse `CAN_USE` grants (Permissions API).

**1. Unity Catalog grants (SQL):**

```sql
-- <app-sp-id>: the app's auto-created SP (find it in Apps → Settings → Service principal)
-- <job-sp-id>: the SP you created in Step 1

-- App service principal
GRANT USE CATALOG ON CATALOG <catalog> TO `<app-sp-id>`;
GRANT ALL PRIVILEGES ON SCHEMA <catalog>.dqx_studio TO `<app-sp-id>`;
GRANT ALL PRIVILEGES ON SCHEMA <catalog>.dqx_studio_tmp TO `<app-sp-id>`;
GRANT ALL PRIVILEGES ON VOLUME <catalog>.dqx_studio.wheels TO `<app-sp-id>`;

-- Job service principal (task runner)
GRANT USE CATALOG ON CATALOG <catalog> TO `<job-sp-id>`;
GRANT ALL PRIVILEGES ON SCHEMA <catalog>.dqx_studio TO `<job-sp-id>`;
GRANT ALL PRIVILEGES ON SCHEMA <catalog>.dqx_studio_tmp TO `<job-sp-id>`;
GRANT ALL PRIVILEGES ON VOLUME <catalog>.dqx_studio.wheels TO `<job-sp-id>`;

-- End users need USE CATALOG to create temporary views for dry runs
GRANT USE CATALOG ON CATALOG <catalog> TO `account users`;
```

**2. SQL warehouse `CAN_USE` grants (Permissions API):**

Both SPs need `CAN_USE` on the warehouse the app is bound to (the same warehouse ID set via `sql_warehouse_id`). In the workspace UI, open **SQL Warehouses → `<warehouse>` → Permissions** and add `CAN_USE` for each SP. Or via the CLI:

```bash
databricks api patch /api/2.0/permissions/warehouses/<warehouse-id> --json '{
  "access_control_list": [
    {"service_principal_name": "<app-sp-id>", "permission_level": "CAN_USE"},
    {"service_principal_name": "<job-sp-id>", "permission_level": "CAN_USE"}
  ]
}'
```

The PATCH verb is additive (it does not replace existing ACLs), and the call is safe to re-run — this is the exact call `post_deploy_grants.sh` makes.

> **Lakebase grants are handled differently.** When Lakebase is enabled, the bundle binds the database to the app via a `database` resource block (`permission: CAN_CONNECT_AND_CREATE`). DABs translates that into the equivalent Postgres role grants automatically — there is no separate SQL to run. The first time the app connects, `PgMigrationRunner` creates its own schema and tables inside the Lakebase database.

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

The Lakebase instance is declared as a bundle resource (`database_instances.lakebase`) and provisioned by `databricks bundle deploy` with `lifecycle.prevent_destroy: true` — see [Step 3](#step-3-stateful-storage-and-destroy-protection). The app connects to the always-present `databricks_postgres` admin database on the instance and creates its own `dqx_studio` Postgres schema there on first start; nothing else needs to be provisioned.

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

## Insights dashboard

The bundle ships a starter AI/BI dashboard (`dashboards/dqx_quality_overview.lvdash.json`) declared as `resources.dashboards.dqx_quality_overview` in `databricks.yml`. It's automatically created on deploy and pinned to the app's **Insights** page via the `DQX_DEFAULT_DASHBOARD_ID` env var, so the page works out-of-the-box.

**What you get**: a four-row layout with KPI counters (total runs, monitored tables, total errors, pass rate), trend charts (runs over time by status; errors & warnings over time), drilldowns (top failing tables; quarantined rows over time), and a recent-runs table.

**Customising the starter**: open it in **Databricks → AI/BI Dashboards**, add or change widgets, and save. The iframe inside DQX Studio picks up changes immediately — no redeploy needed. You can also point the Insights page at a completely different dashboard via **Configuration → Insights dashboard**; clearing that override reverts to the starter.

**Query identity**: the dashboard is configured with `embed_credentials: true`, so queries run as the bundle deployer rather than the iframe viewer. This is deliberate — the bundle only grants `USE CATALOG` (not table-level `SELECT`) to `account users`, keeping `dq_quarantine_records` (potentially PII row payloads) off the workspace UC surface. The widgets in the starter only expose aggregated counts and run metadata, so deployer-credentialed queries don't leak anything a viewer couldn't already see in the Runs History page. To switch to viewer-credentials, flip `embed_credentials` to `false` and grant `SELECT` on the DQX tables to the audience you want to expose.

`scripts/post_deploy_grants.sh` automatically grants the deployer `USE SCHEMA` + `SELECT ON SCHEMA <catalog>.<schema>` after every `make app-deploy`, so the dashboard works end-to-end without manual UC plumbing. It resolves the deployer identity from `databricks current-user me` so it works for both human deploys (grants the email) and SP-based deploys (grants the application ID).

**One operational caveat**: the "deployer" identity is whoever ran `databricks bundle deploy` (the human or service principal authenticated to the workspace at deploy time). If that identity later loses access to the DQX tables, dashboard tiles will fail to render until someone with access redeploys. For production, deploy the bundle as a stable service principal so the dashboard identity doesn't follow individual humans.

## Run review status

DQX Studio lets reviewers attach a per-run **review status** (e.g. *Pending review*, *Acknowledged*, *Resolved*, *False positive*) to each validation run from the expanded row on the **Runs History** page. The same value is filterable from the toolbar so a business owner can ask "what's still pending?" in one click.

- **Configurable catalogue** — admins manage the list of allowed values (label, description, colour) under **Configuration → Run review statuses**. Exactly one entry must be marked **Default**; that value is what unreviewed runs surface virtually (no row is written until someone explicitly reviews). The backend enforces the single-default invariant on save.
- **Audit trail** — every change appends to `dq_run_review_status_history`, surfaced as an "Activity" timeline inside the review-status panel. The current value lives in `dq_run_review_status` (one row per reviewed run).
- **Storage** — both tables are OLTP-shaped (single-key lookups, frequent mutation), so they live in Lakebase when it's enabled and fall back to Delta otherwise via the same `oltp_fallback` plumbing used by comments and role mappings. No extra deployment configuration is needed.
- **Permissions** — any authenticated app user can set or change a review status, mirroring how comments work. Only admins can edit the catalogue itself.

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

**App logs `"Lakebase initialisation failed — falling back to Delta for OLTP tables"`:**
The app has degraded to UC-only mode. Confirm the Lakebase instance exists and is `AVAILABLE`:
```bash
databricks database list-database-instances -p <your-profile>
```
If the instance is missing, re-run `databricks bundle deploy`. If it's there but the app SP doesn't have `CAN_CONNECT_AND_CREATE`, check the bundle's `database` resource block under `resources.apps.dqx-studio.resources` and redeploy.

**`databricks bundle deploy` fails with `"already exists"` / `"Instance name is not unique"` on the first deploy of a target:**
The schemas, volume, or Lakebase instance were created out-of-band before this version of the bundle (e.g. by the older bootstrap script). Run the bind step once per target to adopt them into bundle management:
```bash
make app-bind PROFILE=<your-profile> TARGET=<your-target>
make app-deploy PROFILE=<your-profile> TARGET=<your-target>
```
See [Migrating an existing workspace](#migrating-an-existing-workspace).

If the conflict is specifically `"Instance name is not unique"` for the Lakebase instance and the instance does NOT appear in `databricks database list-database-instances`, it's likely in the ~7-day soft-delete retention window (the name stays reserved). Edit your target in `databricks.yml` and override `lakebase_instance_name: <fresh-name>`, then deploy.

**`databricks bundle deploy` fails with `not authorized to create SQL Endpoint`:**
The target is configured for the managed warehouse pattern but the deployer doesn't have the `Databricks SQL access` + `Allow cluster create` entitlements (rows 2 and 3). Either ask an admin to grant those, or switch the target to **BYO** by setting `sql_warehouse_id: <existing-warehouse-id>` in `databricks.yml` (and removing or commenting out the target's `resources.sql_warehouses.dqx_sql_warehouse` block). See [SQL warehouse: managed vs. BYO](#sql-warehouse-managed-vs-byo).

**`post_deploy_grants.sh` logs `WARNING: PERMISSION_DENIED` on `/api/2.0/permissions/warehouses/...`:**
The deployer doesn't hold `CAN_MANAGE` on the warehouse bound to the target — row 8 in the permissions table. The script reports the warning and continues; the rest of the grants (UC catalog / schema / volume / account users) still apply. To fix: ask the warehouse owner to either (a) grant you CAN_MANAGE and re-run `make app-grant-permissions`, or (b) grant `CAN_USE` directly to the app SP and the task-runner SP shown in the script's preceding log lines. App features that rely on the warehouse will return `not authorized to use or monitor this SQL Endpoint` until both SPs have CAN_USE.

**App returns `not authorized to use or monitor this SQL Endpoint`:**
The app SP doesn't have `CAN_USE` on the warehouse pointed to by `sql_warehouse_id`. Re-run `make app-grant-permissions PROFILE=<your-profile> TARGET=<your-target>`; if you see `PERMISSION_DENIED` in that re-run, see the previous entry — the deployer needs CAN_MANAGE on the warehouse. The same applies symmetrically to the task-runner SP when the job hits it.

**`databricks bundle destroy` fails with `"cannot destroy resource: prevent_destroy is set"`:**
This is the safety guard doing its job — see [Step 3](#step-3-stateful-storage-and-destroy-protection). To intentionally tear down a stateful resource, remove `lifecycle.prevent_destroy: true` from the relevant block in `databricks.yml`, run `databricks bundle deployment unbind <key> -t <target>` to detach it from bundle state, then destroy it manually with `databricks schemas delete` / `databricks volumes delete` / `databricks database delete-database-instance`.

**Lakebase queries time out / app logs show pool exhaustion:**
Bump `lakebase_capacity` from `CU_1` to `CU_2` (or higher) in `databricks.yml` and redeploy. You can also raise `DQX_LAKEBASE_POOL_MAX_SIZE` (default 10) on the app's environment if many concurrent requests are hitting the OLTP path.

**Insights page shows "No dashboard configured" right after deploy:**
The Insights page reads `DQX_DEFAULT_DASHBOARD_ID` (set by the bundle to `${resources.dashboards.dqx_quality_overview.id}`). If it's empty in the deployed app, the dashboards resource didn't materialise — usually because the Databricks CLI is older than `0.283.0` (required for `dataset_catalog` / `dataset_schema`). Upgrade the CLI, redeploy, and confirm:
```bash
databricks --version  # expect ≥ 0.283.0
databricks bundle validate -p <your-profile> -t <your-target> -o json | jq '.resources.dashboards'
databricks apps get <app-name> -p <your-profile> -o json | jq '.config.env[] | select(.name == "DQX_DEFAULT_DASHBOARD_ID")'
```

**Insights iframe is blank / shows a Databricks login screen:**
The viewer's session cookies aren't being passed through to the iframe. Usually one of: (a) the user opened DQX Studio in an incognito window where they aren't signed into Databricks, (b) the workspace is on a different host than the app expects, or (c) the workspace blocks cross-frame embedding for that path. Confirm the constructed embed URL works directly:
```bash
# In the running app, the iframe loads:
# https://<workspace-host>/embed/dashboardsv3/<dashboard-id>
# Test this URL directly in the same browser session.
```
If it loads in a normal tab but not in the iframe, the workspace has a Content-Security-Policy that blocks iframe embedding — contact your workspace admin.

**Insights tiles show `[INSUFFICIENT_PERMISSIONS] Principal '...' does not have SELECT on Table ...`:**
The deployer identity executing the dashboard queries (because `embed_credentials: true`) doesn't have SELECT on the DQX tables. `post_deploy_grants.sh` covers this by default, but it can fail silently if `databricks current-user me` doesn't return a `userName` / `applicationId`, or if the script wasn't re-run after switching deploy identities. Either re-run the script, or grant manually against any warehouse you can use:
```bash
databricks api post /api/2.0/sql/statements -p <your-profile> \
  --json '{
    "warehouse_id": "<warehouse-id>",
    "statement": "GRANT USE SCHEMA, SELECT ON SCHEMA `<catalog>`.`<schema>` TO `<deployer-email-or-spn-id>`",
    "wait_timeout": "30s"
  }'
```
`SELECT ON SCHEMA` propagates to every existing and future table in the schema, so you don't need to repeat it for new DQX tables added by migrations.
