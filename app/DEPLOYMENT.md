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
| 2 | **Databricks SQL access** entitlement | You, in the workspace | `bundle deploy` (creates the X-Small SQL warehouse) | `Error: not authorized to create SQL Endpoint` |
| 3 | **Allow cluster create** entitlement | You, in the workspace | `bundle deploy` (warehouse + job clusters) | Warehouse / job creation rejected |
| 4 | **Databricks Apps: Can Manage** workspace permission | You, in the workspace | `bundle deploy` of the App resource | App creation rejected |
| 5 | **Databricks Database (Lakebase): Manager** entitlement | You, in the workspace | `bundle deploy` of the `database_instances` and `database_catalogs` resources | `Error: User does not have permission to create database instances` |
| 6 | **USE CATALOG** + **CREATE SCHEMA** on `<catalog_name>` | Your user or an admin group you're in | `bundle deploy` of the `schemas` and `volumes` resources | `Error: User does not have CREATE_SCHEMA on catalog '<catalog>'` |
| 7 | **MANAGE** on `<catalog_name>` (or be the catalog owner) | Your user or an admin group you're in | `post_deploy_grants.sh` (issues `GRANT USE CATALOG / ALL PRIVILEGES … TO <app SP>` and `… TO account users`) | `Error: User does not have privilege MANAGE on catalog '<catalog>'` |
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
- **Lakebase Postgres** is enabled on the workspace (default OLTP backend). The Lakebase instance, logical Postgres database, and surrounding UC catalog are declared as bundle resources and provisioned by `databricks bundle deploy`. They carry `lifecycle.prevent_destroy: true` so a `bundle destroy` cannot drop them and wipe OLTP state — see [Stateful storage and destroy protection](#stateful-storage-and-destroy-protection).

### The catalog must already exist

The bundle **does not create the catalog itself** — that's deliberate. Catalogs are typically owned by a governance team and creating them requires `CREATE CATALOG` on the metastore. Pick an existing catalog you (or an admin group you're in) have rights on, and set `catalog_name` in [Step 4](#step-4-configure-databricksyml). The bundle creates the schemas (`dqx_studio`, `dqx_studio_tmp`) and the wheels volume *inside* that catalog. The Lakebase-backed UC catalog (`dqx_studio_lakebase` by default) is created at the metastore level by the `database_catalogs` resource — you need `CREATE CATALOG` on the metastore for the first deploy if it doesn't already exist.

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

DQX Studio's stateful resources — the two schemas (`dqx_studio`, `dqx_studio_tmp`), the wheels volume, the Lakebase instance, and the Lakebase logical Postgres database — are all declared as bundle resources in `app/databricks.yml`. Each one carries `lifecycle.prevent_destroy: true` (Databricks CLI 0.268+), which **blocks `databricks bundle destroy` from dropping the resource** and wiping the data. Use this command line to verify:

```bash
grep -A1 'lifecycle:' app/databricks.yml | head
```

You'll see one `prevent_destroy: true` for each of: `schemas.main_schema`, `schemas.tmp_schema`, `volumes.wheels`, `database_instances.lakebase`, `database_catalogs.lakebase_db`.

What this means in practice:

- **Fresh workspace** — `databricks bundle deploy` creates everything in the right order (schemas → volume → Lakebase instance → Lakebase logical DB → SQL warehouse → job → app). No extra bootstrap step.
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

Update a deploy target. The minimum required is a `catalog_name` and `dqx_service_principal_application_id`; everything else has a sensible default and can be overridden per target. In `app/databricks.yml`:

```yaml
targets:
  dev:
    workspace:
      profile: <your-profile>
    variables:
      catalog_name: <your-catalog>
      dqx_service_principal_application_id: <your-sp-application-id>
    presets:
      trigger_pause_status: PAUSED
```

### Variable reference

All target-level variables, their defaults, and what they control:

| Variable | Default | Required? | Purpose |
|---|---|---|---|
| `catalog_name` | `dqx` | **Yes** | Unity Catalog catalog where schemas and the wheels volume are created. **Must already exist** — the bundle does not create the catalog itself. |
| `dqx_service_principal_application_id` | `00000000-…` | **Yes** | Application ID of the service principal that runs the task-runner job. Created in [Step 1](#step-1-create-a-service-principal). The placeholder default fails validation. |
| `admin_group` | `admins` | No | Workspace group whose members get the in-app `ADMIN` role unconditionally (bootstrap admin path). The default `admins` is the built-in workspace admins group — every workspace admin becomes a DQX admin automatically. Override with a dedicated group (e.g. `dqx-admins-prod`) for narrower bootstrap access. Additional roles are assigned at runtime via the in-app Role Management UI. |
| `app_name` | `dqx-studio` | No | Deployed Databricks App name. Override per target (e.g. `dqx-studio-dev`, `dqx-studio-prod`) when deploying multiple targets to the same workspace, or for personal sandboxes. |
| `sql_warehouse_name` | `dqx-studio-sql-warehouse` | No | Deployed SQL warehouse name (the bundle creates an X-Small serverless warehouse for app queries). Override per target to avoid duplicates in shared workspaces. |
| `schema_name` | `dqx_studio` | No | Main schema — holds run history, profiling, metrics, quarantine, and OLTP fallback tables. Declared as `resources.schemas.main_schema` in the bundle with `lifecycle.prevent_destroy: true`. |
| `tmp_schema_name` | `dqx_studio_tmp` | No | Per-user temp-view schema. Declared as `resources.schemas.tmp_schema` with `lifecycle.prevent_destroy: true`. |
| `wheels_volume_name` | `wheels` | No | UC volume under `<catalog>.<schema_name>` for the DQX + task-runner wheels. Declared as `resources.volumes.wheels` with `lifecycle.prevent_destroy: true`. |
| `lakebase_instance_name` | `dqx-studio-lakebase` | No | Lakebase Postgres instance for OLTP state. Declared as `resources.database_instances.lakebase` with `lifecycle.prevent_destroy: true`. Autoscaling by default per [Lakebase Autoscaling](https://docs.databricks.com/aws/en/oltp/upgrade-to-autoscaling). |
| `lakebase_database_name` | `dqx_studio` | No | Logical Postgres database inside the Lakebase instance. Created by `resources.database_catalogs.lakebase_db` (`create_database_if_not_exists: true`). |
| `lakebase_uc_catalog_name` | `dqx_studio_lakebase` | No | UC catalog created by the `database_catalogs` resource. The app connects to Postgres directly via psycopg, so this UC catalog is informational only — it lets you ad-hoc query the Postgres tables via UC SQL. |
| `lakebase_capacity` | `CU_1` | No | Lakebase compute capacity. Valid values: `CU_1`, `CU_2`, `CU_4`, `CU_8`. To resize an existing instance, change this value and redeploy. Bump up if Lakebase queries queue in the app logs. |

> **Note on duplicate names in Databricks:** SQL warehouses, jobs, and apps within the same workspace are tracked by ID, not by name, so technically duplicates are allowed. Lakebase database instances are looked up by name in the bootstrap script, so they're effectively unique-per-workspace. Operators browse the Jobs / Apps / Warehouses / Databases UI by name, so distinct names per target are strongly recommended when you deploy more than one target to the same workspace.

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
2. `databricks bundle deploy` — provisions or updates the schemas, wheels volume, Lakebase instance, Lakebase logical Postgres database, SQL warehouse, task-runner job, and Databricks App in dependency order. Stateful resources carry `lifecycle.prevent_destroy: true` so a future destroy can't drop them — see [Step 3](#step-3-stateful-storage-and-destroy-protection).
3. `app/scripts/post_deploy_grants.sh` — discovers both service principals and executes the `GRANT` statements on the catalog, schemas, and volume (the auto-created app SP's UUID isn't known at bundle-write time, which is why grants live in a post-deploy script). Lakebase grants are handled by the bundle's `database` resource binding.
4. `databricks bundle run` — starts the app.

> **First start**: The app runs both Delta and Lakebase database migrations on startup, and uploads DQX wheels to the UC volume. If the task-runner job runs before the app has started at least once, it will fail to find its wheels. Wait for `"Uploaded databricks_labs_dqx-<version>..."` in the logs before triggering runs. If Lakebase is enabled, also wait for `"Lakebase OLTP routing enabled"` before opening the UI — the app falls back to UC-only mode if Lakebase init fails (logged as `"Lakebase initialisation failed — falling back to Delta for OLTP tables"`).

### Step-by-step alternative

If you prefer to run each step individually:

```bash
# Build
make app-build

# (One-time, only on a workspace whose storage was created out-of-band)
make app-bind PROFILE=<your-profile> TARGET=<your-target>

# Deploy the bundle (creates / updates all resources, including
# schemas, volume, Lakebase instance and logical DB)
cd app && databricks bundle deploy -p <your-profile> -t <your-target>

# Grant permissions to the app SP (auto-discovered after deploy)
make app-grant-permissions PROFILE=<your-profile> TARGET=<your-target>

# Start the app
cd app && databricks bundle run dqx-studio -p <your-profile> -t <your-target>
```

### Manual grants (if the script doesn't work for your setup)

The grant script discovers both SPs automatically. If you need to run the SQL manually instead:

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

The Lakebase instance, logical Postgres database, and surrounding UC catalog are declared as bundle resources (`database_instances.lakebase`, `database_catalogs.lakebase_db`) and provisioned by `databricks bundle deploy`. All three carry `lifecycle.prevent_destroy: true` — see [Step 3](#step-3-stateful-storage-and-destroy-protection).

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

**`databricks bundle destroy` fails with `"cannot destroy resource: prevent_destroy is set"`:**
This is the safety guard doing its job — see [Step 3](#step-3-stateful-storage-and-destroy-protection). To intentionally tear down a stateful resource, remove `lifecycle.prevent_destroy: true` from the relevant block in `databricks.yml`, run `databricks bundle deployment unbind <key> -t <target>` to detach it from bundle state, then destroy it manually with `databricks schemas delete` / `databricks volumes delete` / `databricks database delete-database-instance`.

**Lakebase queries time out / app logs show pool exhaustion:**
Bump `lakebase_capacity` from `CU_1` to `CU_2` (or higher) in `databricks.yml` and redeploy. You can also raise `DQX_LAKEBASE_POOL_MAX_SIZE` (default 10) on the app's environment if many concurrent requests are hitting the OLTP path.
