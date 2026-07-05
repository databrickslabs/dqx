# Deployment (Declarative Automation Bundles)

Production deployment uses [Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/) (DABs, formerly known as Databricks Asset Bundles) via the Databricks CLI (`databricks bundle deploy`). For local development, see [DEVELOPMENT.md](DEVELOPMENT.md).

## Prerequisites

Before you start, confirm you have **all** of the items below. The single most common deployment failure is missing one permission — and the error you see is almost always downstream of the missing grant, not on the grant itself.

### Tooling

- **Databricks CLI** v1.4.0+ installed and authenticated against your workspace (`databricks auth login -p <profile>`). `make app-deploy` and `make app-bind` enforce this via a preflight `app-check-cli` step (`databricks --version`) and abort before building if the CLI is older. v1.4.0 is required because the one-button `postgres_roles` resource (used to provision a fresh Lakebase) is only accepted by CLI ≥ 1.4.0; `lifecycle.prevent_destroy` itself needs only v0.268+.
- **`jq`** (used by the post-deploy grants script and the resource-bind helper)
- **`make`** (drives the one-command deploy target)
- **App build toolchain** — `make app-deploy` first runs `make app-build` to produce the wheel, which needs **uv**, **Node.js 18+** (provides `npm`; `brew install node` / nvm / [nodejs.org](https://nodejs.org/en/download)), **yarn** (`npm install -g yarn`), and **bun** (`curl -fsSL https://bun.sh/install | bash`). See [DEVELOPMENT.md → Prerequisites](DEVELOPMENT.md#prerequisites).

### Required permissions

The deploying user (you) needs the permissions below. They are **all** consumed by `make app-deploy`; deployment will halt the first time it hits a missing one. We've listed which step in the flow each permission unblocks so you can debug surgically if a grant gets missed.

| # | Permission | Granted on | Used by | What fails without it |
|---|---|---|---|---|
| 1 | **Workspace access** entitlement | You, in the workspace | All CLI calls | `databricks` CLI can't reach the workspace |
| 2 | **Databricks SQL access** entitlement | You, in the workspace | Mode A targets only — `bundle deploy` calling the create-warehouse API. (Mode B targets reuse an existing warehouse and need only `CAN_USE` on it, not workspace-level SQL-create rights.) | `Error: not authorized to create SQL Endpoint` |
| 3 | **Allow cluster create** entitlement | You, in the workspace | `bundle deploy` for job clusters always; for the warehouse Mode A only | Warehouse / job creation rejected |
| 4 | **Databricks Apps: Can Manage** workspace permission | You, in the workspace | `bundle deploy` of the App resource | App creation rejected |
| 5 | **Databricks Database (Lakebase): Manager** entitlement | You, in the workspace | `bundle deploy` of the `database_instances` resource | `Error: User does not have permission to create database instances` |
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

DQX Studio's stateful resources — the two schemas (`dqx_studio`, `dqx_studio_tmp`), the wheels volume, and the Lakebase instance — are all declared with `lifecycle.prevent_destroy: true` (Databricks CLI 0.268+), which **blocks `databricks bundle destroy` from dropping the resource** and wiping the data.

The schemas and volume are declared at the **base level** in `app/databricks.yml`. The Lakebase instance is declared **inside the target** (`database_instances.lakebase` under `targets.dqx-app-demo`) so a target that doesn't want Lakebase can simply omit the block rather than carry a stub instance. Use:

```bash
grep -A1 'lifecycle:' app/databricks.yml | head
```

You'll see `prevent_destroy: true` on `schemas.main_schema`, `schemas.tmp_schema`, `volumes.wheels` (base), and `database_instances.lakebase` (under the `dqx-app-demo` target).

> **The app's `dqx_studio` Postgres schema** (inside the `databricks_postgres` admin database on the Lakebase instance) is created by the app at first start. It's stateful but lives below the resource layer DABs models, so `prevent_destroy` doesn't apply to it directly. The instance-level guard above is what protects it: as long as `database_instances.lakebase` survives, the schema and its tables survive.

What this means in practice:

- **Fresh workspace** — `make app-deploy` does everything in one command: `databricks bundle deploy` provisions the schemas → volume → Lakebase instance → SQL warehouse (Mode A targets only, see [Choosing a SQL warehouse mode](#choosing-a-sql-warehouse-mode)) → job → app in dependency order, then `post_deploy_grants.sh` issues catalog/schema/volume GRANTs (and, in Mode B, warehouse `CAN_USE` GRANTs).
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

Add or update a deploy target. The minimum required variables are:

- `catalog_name`
- `dqx_service_principal_application_id`
- `sql_warehouse_id` — picks the warehouse mode (see [Choosing a SQL warehouse mode](#choosing-a-sql-warehouse-mode) immediately below)

Everything else has a sensible default and can be overridden per target. A minimal target skeleton looks like this:

```yaml
targets:
  my-target:
    workspace:
      profile: <your-profile>
    variables:
      catalog_name: <your-catalog>
      dqx_service_principal_application_id: <your-sp-application-id>
      sql_warehouse_id: <literal ID, or a ${resources...} ref — see below>
    presets:
      trigger_pause_status: PAUSED
```

> The repo ships a single canonical target, **`dqx-app-demo`** (Mode B, marked `default: true`).

The resource-specific blocks (the warehouse `sql_warehouses` block, the Lakebase `database_instances` block, and any app-resources overrides) are described next in [Choosing a SQL warehouse mode](#choosing-a-sql-warehouse-mode) and assembled per use-case in [Deployment scenarios](#deployment-scenarios-warehouse-and-lakebase-combinations).

### Choosing a SQL warehouse mode

The bundle does **not** declare a SQL warehouse at the top level on purpose — each target picks one of two patterns. Pick one per target; you cannot mix.

#### Mode A — Bundle-managed (the bundle CREATES a dedicated warehouse)

Use when you want a fresh, dedicated warehouse provisioned and owned by the bundle. The warehouse's lifecycle is tied to the bundle: `bundle deploy` creates/updates it, `bundle destroy` deletes it. You specify the spec (size, scaling, auto-stop) because the bundle is what's calling the create-warehouse API.

```yaml
targets:
  my-mode-a-target:
    workspace:
      host: https://<your-workspace>.cloud.databricks.com/
    variables:
      catalog_name: dqx
      dqx_service_principal_application_id: <sp-app-id>
      # Wire the bundle-managed warehouse's runtime ID back into the
      # ``sql_warehouse_id`` variable that the app's resource block reads.
      sql_warehouse_id: ${resources.sql_warehouses.dqx_sql_warehouse.id}
    resources:
      sql_warehouses:
        dqx_sql_warehouse:
          name: ${var.sql_warehouse_name}     # dqx-studio-sql-warehouse by default
          cluster_size: "X-Small"
          enable_serverless_compute: true
          max_num_clusters: 1
          min_num_clusters: 1
          auto_stop_mins: 10
          permissions:
            - group_name: "users"
              level: "CAN_USE"
```

Pros: zero pre-existing infra needed; warehouse spec is version-controlled; `bundle destroy` cleans it up. Cons: every target that wants this pattern must repeat the `sql_warehouses` block (DABs doesn't support inheriting a partial resource from top level when other targets opt out).

#### Mode B — External / reuse (the bundle REFERENCES an existing warehouse)

Use when you want the app to point at a warehouse that already exists — typically a shared workspace warehouse used by multiple apps or teams. The bundle never creates, mutates, or destroys the warehouse; it only records "the app uses warehouse `<id>`". You provide just the ID.

This is the pattern the repo's canonical `dqx-app-demo` target uses. Because the **base** app definition already binds the warehouse via `id: ${var.sql_warehouse_id}`, switching to an existing warehouse needs **only the literal ID in `variables`** — no `apps.dqx-studio.resources` override:

```yaml
targets:
  dqx-app-demo:
    default: true
    workspace:
      profile: dqx-app-demo
    variables:
      catalog_name: <your-catalog>
      dqx_service_principal_application_id: <sp-app-id>
      sql_warehouse_id: "<existing-warehouse-id>"   # ← just the existing warehouse's ID
    presets:
      trigger_pause_status: PAUSED
```

Pros: safe for shared warehouses (`bundle destroy` can't touch them); no wasted compute; warehouse settings are managed by whoever owns the warehouse, not by this bundle; the target stays tiny (just `variables`). Cons: requires the warehouse to exist already, and `CAN_USE` is granted post-deploy by `post_deploy_grants.sh` rather than by Terraform (see [How permissions get granted](#how-permissions-get-granted-on-each-mode)).

> **When you _do_ need the `apps.dqx-studio.resources:` override:** only when a target changes the *shape* of a binding rather than a value it reads from a variable — e.g. dropping or repointing the Lakebase binding (see [Deployment scenarios](#deployment-scenarios-warehouse-and-lakebase-combinations) below). DABs replaces that list wholesale on override, so you must repeat the remaining entries. Plain Mode B (existing warehouse) does **not** trigger this because the base binding already reads `${var.sql_warehouse_id}`.

#### Side-by-side comparison

| | Mode A (bundle-managed) | Mode B (external) |
|---|---|---|
| **YAML in target** | `sql_warehouses.dqx_sql_warehouse: { name, cluster_size, ... permissions }` + `sql_warehouse_id: ${resources...id}` | just `sql_warehouse_id: "<existing-id>"` (the base app binding already reads this var — no list override needed) |
| **Who owns the warehouse** | The bundle (Terraform state) | Whoever created it (out of band) |
| **`bundle deploy` effect** | Creates / mutates the warehouse to match spec | Never touches the warehouse |
| **`bundle destroy` effect** | DELETES the warehouse | Leaves the warehouse intact |
| **Permissions on the warehouse** | Granted via the `permissions:` block under `sql_warehouses.dqx_sql_warehouse` (Terraform sets them) | Granted post-deploy by `post_deploy_grants.sh` calling the warehouses permissions API (the bundle doesn't own the warehouse, so `databricks_permissions` can't be used) |
| **Good for** | Dev/sandbox targets, per-environment dedicated warehouses | Production with a shared warehouse, vending-machine workspaces with a pre-existing warehouse, demo workspaces where you don't want a second warehouse to appear |
| **Example target in the repo** | _(none — illustrative only)_ | `dqx-app-demo` (the only shipped target) |

#### How permissions get granted on each mode

- **Mode A**: the `permissions:` block under `sql_warehouses.dqx_sql_warehouse` is applied by Terraform during `bundle deploy`. To grant additional principals, edit the YAML and redeploy.
- **Mode B**: `post_deploy_grants.sh` reads `sql_warehouse_id` from the validated bundle config and, when it resolves to a non-empty literal ID (i.e. the bundle isn't managing the warehouse), PATCHes the warehouses permissions API to add `CAN_USE` for the app SP, the job SP, and the workspace `users` group. The PATCH is additive — existing grants on the warehouse are preserved. (In Mode A the variable resolves to `${resources.sql_warehouses.dqx_sql_warehouse.id}` which the script treats as bundle-owned and skips this step, since Terraform already covered it via the `permissions:` block.) If you need different principals, edit the relevant block in `app/scripts/post_deploy_grants.sh`.

### Deployment scenarios: warehouse and Lakebase combinations

DQX Studio depends on three workspace resources, and you can independently choose whether the **bundle provisions** each one or whether you **bring your own** (BYO) existing resource:

- **Catalog — always BYO.** The bundle never creates a catalog; it only creates the two schemas (`dqx_studio`, `dqx_studio_tmp`) and the `wheels` volume *inside* a catalog you name. So `catalog_name` must point at an existing catalog in every scenario (you need `USE CATALOG` + `CREATE SCHEMA` on it — see [Required permissions](#required-permissions) and [The catalog must already exist](#the-catalog-must-already-exist)).
- **SQL warehouse — 2 modes.** Bundle-managed (**Mode A**) or BYO existing (**Mode B**). See [Choosing a SQL warehouse mode](#choosing-a-sql-warehouse-mode).
- **Lakebase — 3 modes.** Bundle-managed (bundle CREATEs the instance), BYO existing (adopt an existing instance), or none (OLTP falls back to Delta).

Because the warehouse has 2 modes and Lakebase has 3, there are **6 combinations**. The catalog is the same (existing) in all of them.

#### The 5 building blocks

Compose a target from these fragments. The **base** `resources` block already binds the warehouse via `${var.sql_warehouse_id}` and the Lakebase via `${resources.database_instances.lakebase.name}`, so most modes are just a variable change.

**Catalog (required in every scenario):**
```yaml
    variables:
      catalog_name: <existing-catalog>          # must already exist
      dqx_service_principal_application_id: <sp-app-id>
```

**Warehouse — Mode A (bundle-managed):** set `sql_warehouse_id: ${resources.sql_warehouses.dqx_sql_warehouse.id}` and add the `sql_warehouses` block — full YAML in [Mode A above](#mode-a--bundle-managed-the-bundle-creates-a-dedicated-warehouse).

**Warehouse — Mode B (BYO existing):** set `sql_warehouse_id: "<existing-warehouse-id>"` — no extra blocks (the base app binding reads the variable).

**Lakebase — managed (bundle creates a fresh instance):** keep the base `database_instances.lakebase` block and name the new instance. On a fresh workspace, also provision the app SP's CREATE-schema privilege (one-button `postgres_roles` or a manual grant — see the [one-button Postgres note](#step-4-configure-databricksyml)).
```yaml
    variables:
      lakebase_instance_name: <new-instance-name>
```

**Lakebase — BYO existing (adopt via bind):** keep the base block, name the existing instance, and run `make app-bind` once so the bundle adopts it instead of trying to CREATE it.
```yaml
    variables:
      lakebase_instance_name: <existing-instance-name>
```

**Lakebase — none (Delta fallback):** set the sentinel `"-"`, remove the base `database_instances.lakebase` resource, and drop the `dqx-lakebase` app binding (which means overriding the app-resources list — DABs replaces it wholesale, so repeat the remaining two entries).
```yaml
    variables:
      lakebase_instance_name: "-"
    resources:
      apps:
        dqx-studio:
          resources:
            - name: "dqx-sql-warehouse"
              sql_warehouse: { id: "${var.sql_warehouse_id}", permission: "CAN_USE" }
            - name: "dqx-task-runner-job"
              job: { id: "${resources.jobs.dqx_task_runner.id}", permission: "CAN_MANAGE" }
    # ...and delete the database_instances.lakebase block from the base resources.
```

#### The 6 combinations

| # | Warehouse | Lakebase | Variables | Extra blocks | `make app-bind` first? |
|---|---|---|---|---|---|
| 1 | Managed (A) | Managed | `sql_warehouse_id: ${resources…id}`, `lakebase_instance_name: <new>` | add `sql_warehouses` | No (fresh) / Yes if storage pre-exists |
| 2 | Managed (A) | BYO existing | as #1 but `lakebase_instance_name: <existing>` | add `sql_warehouses` | **Yes** (adopts the instance) |
| 3 | Managed (A) | None | `sql_warehouse_id: ${resources…id}`, `lakebase_instance_name: "-"` | add `sql_warehouses`; drop `dqx-lakebase` binding; remove base `database_instances` | No / Yes if storage pre-exists |
| 4 | BYO (B) | Managed | `sql_warehouse_id: "<id>"`, `lakebase_instance_name: <new>` | none | No (fresh) / Yes if storage pre-exists |
| 5 | BYO (B) | BYO existing | `sql_warehouse_id: "<id>"`, `lakebase_instance_name: <existing>` | none | **Yes** — **this is the canonical `dqx-app-demo` target** |
| 6 | BYO (B) | None | `sql_warehouse_id: "<id>"`, `lakebase_instance_name: "-"` | drop `dqx-lakebase` binding; remove base `database_instances` | No / Yes if storage pre-exists |

> **When is `make app-bind` needed?** Whenever any stateful resource the bundle would CREATE *already exists* (the two schemas, the wheels volume, or — for the managed/BYO Lakebase modes — the instance). Adopting an existing Lakebase instance (scenarios #2 and #5) **always** requires it; for a brand-new workspace where nothing exists yet, skip bind and let `make app-deploy` create everything. `make app-bind` is idempotent, so running it when nothing needs adopting is a safe no-op. See [Migrating an existing workspace](#migrating-an-existing-workspace).

#### Worked example: the canonical `dqx-app-demo` target

**Scenario #5 — BYO warehouse + BYO Lakebase.** Everything already exists; the target is just `variables` (compose the other scenarios from the [building blocks](#the-5-building-blocks) above and the [combination table](#the-6-combinations)):

```yaml
targets:
  dqx-app-demo:
    default: true
    workspace:
      profile: <your-profile>
    variables:
      catalog_name: <existing-catalog>
      dqx_service_principal_application_id: <sp-app-id>
      sql_warehouse_id: "<existing-warehouse-id>"
      lakebase_instance_name: <existing-lakebase-instance>
    presets:
      trigger_pause_status: PAUSED
```
```bash
make app-bind   PROFILE=<your-profile> TARGET=dqx-app-demo   # one-time adoption
make app-deploy PROFILE=<your-profile> TARGET=dqx-app-demo
```

### Variable reference

All target-level variables, their defaults, and what they control:

| Variable | Default | Required? | Purpose |
|---|---|---|---|
| `catalog_name` | `dqx` | **Yes** | Unity Catalog catalog where schemas and the wheels volume are created. **Must already exist** — the bundle does not create the catalog itself. |
| `dqx_service_principal_application_id` | `00000000-…` | **Yes** | Application ID of the service principal that runs the task-runner job. Created in [Step 1](#step-1-create-a-service-principal). The placeholder default fails validation. |
| `admin_group` | `proj_dbw_dev_dg_admins-data_ug` (bundle) / unset (local Python) | Yes for prod | Workspace group whose members get the in-app `ADMIN` role unconditionally (bootstrap admin path). The bundle ships with a non-production placeholder — override per target with your real admin group (e.g. `dqx-admins-prod`). Locally, `AppConfig` defaults to `None` and skips bootstrap admin assignment; set `DQX_ADMIN_GROUP` in your shell if you need it for local testing. Additional roles are assigned at runtime via the in-app Role Management UI. |
| `app_name` | `dqx-studio` | No | Deployed Databricks App name. Override per target (e.g. `dqx-studio-dev`, `dqx-studio-prod`) when deploying multiple targets to the same workspace, or for personal sandboxes. |
| `sql_warehouse_id` | `""` (empty) | **Yes** (every target must set it) | ID of the SQL warehouse the app queries. Two ways to populate it: chain it to a bundle-managed warehouse (Mode A: `sql_warehouse_id: ${resources.sql_warehouses.dqx_sql_warehouse.id}`) or point at an existing warehouse (Mode B: `sql_warehouse_id: "<existing-id>"`). See [Choosing a SQL warehouse mode](#choosing-a-sql-warehouse-mode). |
| `sql_warehouse_name` | `dqx-studio-sql-warehouse` | Mode A only | Name passed to Terraform when CREATING the bundle-managed warehouse. **Ignored in Mode B** (the warehouse already exists with its own name). Override per target to avoid duplicates in shared workspaces. |
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
2. `databricks bundle deploy` — provisions or updates the schemas, wheels volume, Lakebase instance, the SQL warehouse (**Mode A only** — Mode B reuses an existing one and the bundle never touches it), the task-runner job, and the Databricks App in dependency order. Stateful resources carry `lifecycle.prevent_destroy: true` so a future destroy can't drop them — see [Step 3](#step-3-stateful-storage-and-destroy-protection).
3. `app/scripts/post_deploy_grants.sh` — discovers both service principals and executes the `GRANT` statements on the catalog, schemas, and volume (the auto-created app SP's UUID isn't known at bundle-write time, which is why grants live in a post-deploy script). Lakebase grants are handled by the bundle's `database` resource binding. **In Mode B**, the script additionally PATCHes the warehouses permissions API to grant `CAN_USE` on the external warehouse to the app SP, job SP, and the `users` workspace group (Mode A grants those via Terraform during step 2).
4. `databricks bundle run` — starts the app.

> **First start**: The app runs both Delta and Lakebase database migrations on startup, and uploads DQX wheels to the UC volume. If the task-runner job runs before the app has started at least once, it will fail to find its wheels. Wait for `"Uploaded databricks_labs_dqx-<version>..."` in the logs before triggering runs. If Lakebase is enabled, also wait for `"Lakebase OLTP routing enabled"` before opening the UI — when Lakebase is configured and init fails, the app refuses to start (logged as `"Lakebase initialisation failed ... Refusing to start"`) and the Apps platform will restart the container. Silent fallback to Delta is intentionally disallowed because it would split OLTP writes across two physical stores and orphan prior Lakebase data on every flap. To intentionally run on Delta only, unset `DQX_LAKEBASE_INSTANCE_NAME`.

### Step-by-step alternative

If you prefer to run each step individually:

```bash
# Build
make app-build

# (One-time, only on a workspace whose storage was created out-of-band)
make app-bind PROFILE=<your-profile> TARGET=<your-target>

# Deploy the bundle (creates / updates schemas, volume, Lakebase
# instance, task-runner job, app, and the SQL warehouse in Mode A targets
# — Mode B targets reuse an existing warehouse, see "Choosing a SQL
# warehouse mode")
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

-- End users need USE CATALOG plus USE SCHEMA / CREATE TABLE on the tmp
-- schema to create the temporary views used by dry-run / preview. The
-- view is created with the user's OBO token (so their own table read
-- perms are enforced) but lives in <catalog>.dqx_studio_tmp, so they
-- need to be able to write there. Without the schema-level grants,
-- every dry-run fails with PERMISSION_DENIED on CREATE OR REPLACE VIEW.
GRANT USE CATALOG ON CATALOG <catalog> TO `account users`;
GRANT USE SCHEMA, CREATE TABLE ON SCHEMA <catalog>.dqx_studio_tmp TO `account users`;

-- Mode B only — bundle-managed warehouse (Mode A) has these grants applied
-- automatically by Terraform via the `permissions:` block in the YAML.
-- For Mode B targets the bundle doesn't own the warehouse, so grant
-- CAN_USE out of band (Databricks UI: Warehouses → <warehouse> →
-- Permissions, or via the warehouses permissions API):
--   App SP, Job SP, and `users` workspace group → CAN_USE
-- `post_deploy_grants.sh` does this automatically when `sql_warehouse_id`
-- is set to a literal ID; the SQL above is only needed if you bypass it.
```

> **Lakebase grants are handled differently.** When Lakebase is enabled, the bundle binds the database to the app via a `database` resource block (`permission: CAN_CONNECT_AND_CREATE`). DABs translates that into the equivalent Postgres role grants automatically — there is no separate SQL to run. The first time the app connects, `PgMigrationRunner` creates its own schema and tables inside the Lakebase database.
>
> **One-button Postgres (CLI ≥ 1.4.0).** `PgMigrationRunner` does `CREATE SCHEMA dqx_studio` inside the system `databricks_postgres` database, and `CREATE` there is owned by `databricks_superuser`. On some workspaces the `CAN_CONNECT_AND_CREATE` binding alone doesn't confer that, and the schema-create fails with a Postgres permission error until someone grants it by hand. For a **fresh workspace**, add a [`postgres_roles`](https://github.com/databricks/cli/pull/5467) block to your target that makes the app SP a member of `DATABRICKS_SUPERUSER`, so a single `bundle deploy` provisions the instance, the app, *and* the CREATE-schema privilege — no follow-up `psql`/console step:

```yaml
targets:
  my-fresh-target:
    postgres_roles:
      app_sp:
        parent: "projects/<lakebase-project>/branches/<branch>"
        role_id: "sp-${resources.apps.dqx-studio.service_principal_client_id}"
        postgres_role: ${resources.apps.dqx-studio.service_principal_client_id}
        identity_type: SERVICE_PRINCIPAL
        auth_method: LAKEBASE_OAUTH_V1
        membership_roles:
          - DATABRICKS_SUPERUSER
```

This requires **Databricks CLI v1.4.0 or newer** (`databricks --version`); on older CLIs the `postgres_roles` field is rejected at `bundle validate`, so either upgrade the CLI or delete the block and grant `CREATE ON DATABASE databricks_postgres` to the app SP once manually. The shipped `dqx-app-demo` target intentionally **omits** this block because its app SP's Postgres role already exists (created out of band on an earlier deploy), so a declarative create would conflict; the pre-existing role already confers the needed CREATE-schema access.

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
The app deliberately refuses to start when Lakebase is configured (`DQX_LAKEBASE_INSTANCE_NAME` non-empty) and init fails — silently falling back to Delta would split OLTP writes across two physical stores and orphan prior Lakebase data. Diagnose with the steps below; the Apps platform will pick up the next successful start automatically.

1. Confirm the Lakebase instance exists and is `AVAILABLE`:
   ```bash
   databricks database list-database-instances -p <your-profile>
   ```
   If the instance is missing, re-run `databricks bundle deploy`. If the instance is there but the state is `STARTING` / `UPDATING`, wait for it to reach `AVAILABLE` and the next restart will succeed.
2. Confirm the app SP has `CAN_CONNECT_AND_CREATE` on the bound logical database. Check the bundle's `database` resource block under `resources.apps.dqx-studio.resources` (it should bind `database_name: ${var.lakebase_database_name}`) and redeploy.
3. If the failure is specifically a Postgres `permission denied for database databricks_postgres` (or `permission denied to create schema`), the app SP can connect but lacks `CREATE` on the system `databricks_postgres` database — that privilege is owned by `databricks_superuser`. Either deploy the `postgres_roles` block (CLI ≥ 1.4.0; see the [Lakebase grants note](#step-4-configure-databricksyml) above) so the bundle grants `DATABRICKS_SUPERUSER` membership, or run a one-time `GRANT CREATE ON DATABASE databricks_postgres TO "<app-sp-client-id>"` against the Lakebase instance.
4. Confirm OAuth token issuance is healthy — Lakebase tokens currently expire after one hour; a misconfigured OAuth integration or revoked SP credential will surface here.
5. If you intentionally want to run on Delta only (no Lakebase), redeploy with `DQX_LAKEBASE_INSTANCE_NAME=""` (or remove the override from your target in `databricks.yml`). The app will start in legacy UC-only mode and OLTP tables will live on Delta.

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

