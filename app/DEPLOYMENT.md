# Deployment (Declarative Automation Bundles and Marketplace)

Production deployment uses [Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/) (DABs, formerly known as Databricks Asset Bundles) via the Databricks CLI (`databricks bundle deploy`), or the Databricks Marketplace listing. For local development, see [DEVELOPMENT.md](DEVELOPMENT.md).

## Choose an installation path

**DAB deployment** of a tagged Studio release uses the prebuilt `app/marketplace/` artifact with the `release` target; local build tools are not required. Use `make app-deploy PROFILE=<profile> TARGET=release` on macOS/Linux or the experimental `make.ps1 -Release` helper on Windows. Developers building from source use the same helpers with a source target. On first start, DQX Studio runs the same readiness workflow used by Marketplace before it serves the normal API.

**Marketplace installation** requires three existing resource bindings and a workspace service principal for the task-runner job. Bind a SQL warehouse, a Lakebase Postgres endpoint, and a Unity Catalog volume; the bound volume determines the main catalog and schema. Create the workspace service principal before installation and grant the installing identity the Service Principal: User role on it. The principal is assigned as the job's `run_as` identity in the Jobs UI, not in the Marketplace resource picker. Before opening the app, ensure an administrator is a member of the workspace group named by `DQX_ADMIN_GROUP`. The setup wizard verifies the three bindings, creates the sibling schemas, runs migrations, publishes wheels, gives precise grant instructions when a capability is missing, and links to the Jobs UI for the service principal assignment.

The Marketplace app derives sibling schema names from the volume's schema: a volume under `/Volumes/<catalog>/<schema>/<volume>` uses `<schema>_tmp` for temporary views and `<schema>_genie` for Genie-facing views. The data schema is always `<schema>` from that bound path. `DQX_TMP_SCHEMA` and `DQX_GENIE_SCHEMA` remain available as deployment environment overrides; changing the volume binding or an override after installation changes where Studio looks for its objects. DAB deployments retain their explicit schema overrides. Setup checks the app service principal's `USE SCHEMA` and `CREATE TABLE` privileges on both sibling schemas, including schemas that already existed. A failed score or entitlement object creation keeps setup from reporting ready and reports the required Unity Catalog privileges.

**Marketplace install permissions gaps after removing `account users`:** configure a scoped audience and provision runner access explicitly; do not restore broad grants to make setup pass. The installing administrator needs grant authority (`MANAGE` on the bound catalog, schema, and volume, or corresponding ownership/admin authority), not just `USE CATALOG`, `USE SCHEMA`, and volume data access. This lets the installer establish the app's required access when broad inherited grants are absent.

`DQX_USER_GROUPS` accepts a JSON list of scoped workspace group names, for example `["dqx-studio-users"]`, or simple unquoted comma-separated names, for example `studio-authors,studio-viewers`. Use JSON for names containing commas or quotes. Its default is `[]`: audience permissions are then administrator-managed, not granted to everyone. Startup warns that no audience grants will be applied when the list is empty; existing grants are not revoked. Built-in `users` and `account users` groups are rejected. For configured groups, setup attempts `USE SCHEMA` and `CREATE TABLE` on the temporary schema; activation attempts `USE SCHEMA` on the Genie schema and `SELECT` only on five approved views and two metadata tables (`dim_dq_rules` and `dim_dq_monitored_tables`). Audience grants are best effort and do not block readiness. Administrators must also provide audience app access, warehouse `CAN_USE`, SQL access entitlement, and parent catalog usage. No whole-schema Genie `SELECT` or entitlement-table grant is applied.

After assigning the task-runner service principal in the Jobs UI, setup resolves the job's actual `run_as` and verifies these requirements on **every cold startup**:

| Resource | Required runner access |
|---|---|
| Bound catalog | `USE CATALOG` |
| Main and temporary schemas | `USE SCHEMA` on both |
| Wheels volume | `READ VOLUME` |
| Main schema | Schema-level `SELECT` and `MODIFY`, covering current and future tables |

Checks include inherited permissions and ownership; table-specific output grants alone do not satisfy the main-schema requirement. Schema ownership alone does not establish `SELECT` or `MODIFY` on its tables. Schema-level runner access avoids maintaining a per-table checklist, and applies to every table in that schema, so bind a dedicated Studio schema rather than one containing unrelated data. Missing or uninspectable UC permissions keep setup action-required. Setup neither automatically grants UC runner privileges nor changes `run_as`; verification is never cached across startups. Source-data access remains run-specific. The runner needs no Lakebase access; oversized run configs are staged in the Delta `dq_run_configs` table in the main schema.

Setup first tries effective UC grant inspection with app credentials. If unavailable, the setup administrator's request-scoped OBO SQL executor inspects `SHOW GRANTS` on the target and parent containers using the supported `sql` OAuth scope. Column names are matched case-insensitively, and group grants require independently verified runner membership. SQL inspection reads every result chunk and rejects truncated or incomplete results. Job management and writes remain under the app SP. The administrator needs ownership, `READ METADATA`, `MANAGE`, or metastore administration to inspect grants, plus warehouse `CAN_USE` and parent `USE CATALOG` / `USE SCHEMA` (including the temporary schema). If unattended startup cannot inspect access as the app SP, open setup as an authorized administrator and verify again after each restart. Unknown permissions are distinguished from confirmed missing grants. DAB grants do not replace the catalog prerequisite.

An explicitly configured `DQX_SCHEMA` that differs from the bound volume schema produces a warning; the volume still determines application storage. Successful metadata-dimension refreshes retry the two explicit metadata-table SELECT grants, including when a startup refresh failed before creating the tables.

On upgrade, existing broad grants are **not automatically revoked**. An administrator must review and revoke legacy `users` / `account users` ACLs and UC grants on the catalog, schemas, volume, temporary views, warehouse, app, dashboard, and Genie space as applicable, including schema-wide Genie `SELECT`. Reapply only the scoped audience and approved object grants below. For existing OBO-created views that need orphan cleanup, explicitly grant runner `SELECT` and app-SP `MANAGE` on each view; do not restore broad access or grant schema-wide cleanup rights.

Lakebase is mandatory. Delta-backed application (OLTP) state was removed and cannot be migrated. Marketplace currently supports replacing the SQL warehouse; rebind reconciles the Genie space's compute with the new warehouse. Swapping the Lakebase endpoint or volume, and smoke-validating the task runner, are planned follow-up capabilities.

Marketplace releases use published, pinned DQX Core packages from public PyPI. Main tracks the canonical application source and Marketplace templates but excludes the generated `app/marketplace/` artifact. `app/scripts/release_marketplace.sh studio-vX.Y.Z` validates the requested version against the application at `HEAD`, creates `dqx-studio/marketplace/vX.Y.Z`, builds and force-stages the complete self-contained source, signs and verifies its local commit, then creates and verifies the annotated signed Studio version tag on that generated commit. This ordering is required because Marketplace checks out the tag and deploys the configured `app/marketplace/` source path. The build **copies** the committed, self-contained lock at `app/marketplace_templates/uv.lock` (a tracked template) rather than re-resolving: it runs no `uv lock` and downloads no wheels, so the artifact can't drift with the package index or a `uv` version, and needs no network. A test (`test_marketplace_template_lock_matches_app_runtime_closure`) keeps that template in step with the app's runtime dependencies. To refresh it after a dependency or DQX-version change, regenerate it deliberately in an environment with public-PyPI (or proxy) access — resolve the release dependencies with `uv lock` and normalize the URLs to public PyPI — then commit the result. Because `databricks-labs-dqx` currently pins `litellm<=1.82.6` (which has no Python 3.13/3.14 wheels), that regeneration must temporarily cap the app's `requires-python` to `<3.13` to avoid an unsolvable resolver fork; revert the cap after regenerating. The script never pushes; inspect the branch, then explicitly push both refs with `git push origin dqx-studio/marketplace/vX.Y.Z` and `git push origin studio-vX.Y.Z`. The DAB `release` target consumes this same prebuilt artifact; source-build targets continue to use `.build/`.

## Resource ownership tags

DQX Studio marks resources it owns with the ungoverned ownership tag
`app=dqx-studio`. The deployment path determines whether a resource is
Studio-owned: Marketplace-bound resources may be shared with other workloads
and are deliberately untouched.

| Resource | DAB | Marketplace | Tag behavior |
|---|---|---|---|
| Task-runner job | Created | Created/reconciled | `app=dqx-studio` |
| SQL warehouse | Created and dedicated | Existing binding | Tagged only for DAB |
| Lakebase project | Created and dedicated | Existing binding | Tagged only for DAB |
| Main UC schema and wheels volume | Created | Existing binding | Tagged only for DAB |
| Temporary and Genie schemas | Created | Created by setup | Tagged in both paths |
| Persistent Studio UC tables/views | Created by startup/migrations | Created by startup/migrations | Tagged in both paths |
| Demo schema/tables | Created by bundle/demo workflow | Created only when demo is deployed | Tagged when Studio-owned |
| App, dashboard, Genie Agent | Created | App/Genie created | Not implemented: no native DAB tag support in CLI 1.17.0 |
| Service principals, roles, grants, ACLs, bindings | Mixed | Mixed | Not tagged |

Temporary and Genie schemas, persistent Studio Unity Catalog tables and views,
and Studio-created demo resources are reconciled by the applicable deployment,
startup, setup, and demo paths. The task-runner job is tagged in both DAB and
Marketplace paths.

App, dashboard, and Genie Agent tagging is not implemented: Databricks CLI
1.17.0 provides no native DAB tag field for those resource types. The separate
Beta workspace entity-tag API may support them, but DQX Studio intentionally
does not call it. Service principals, roles, grants, ACLs, and bindings are
also not tagged.

The Lakebase-internal Postgres schema and its tables do not receive Unity
Catalog tags.

## Prerequisites

Before you start, confirm you have **all** of the items below. The single most common deployment failure is missing one permission — and the error you see is almost always downstream of the missing grant, not on the grant itself.

### Tooling

- **Databricks CLI** v1.4.0+ installed and authenticated against your workspace (`databricks auth login -p <profile>`). `make app-deploy` enforces this via a preflight `app-check-cli` step (`databricks --version`) and aborts before building if the CLI is older. v1.4.0 is required because the `postgres_projects` / `postgres_roles` resources (used to provision Lakebase and the app SP's Postgres role) are only accepted by CLI ≥ 1.4.0; `lifecycle.prevent_destroy` itself needs only v0.268+.
- **Git** to check out a Studio release tag. The prebuilt DAB release target needs no local `make`, uv, Node.js, yarn, or bun.

Building from source requires **`make`** on macOS/Linux or the experimental PowerShell helper on Windows, plus **uv**, **Node.js 18+**, **yarn**, and **bun**. See [DEVELOPMENT.md → Prerequisites](DEVELOPMENT.md#prerequisites).

### Required permissions

The deploying user (you) needs the permissions below. Both `databricks bundle deploy -t release` and `make app-deploy` use them; deployment will halt the first time it hits a missing one. We've listed which step in the flow each permission unblocks so you can debug surgically if a grant gets missed.

| # | Permission | Granted on | Used by | What fails without it |
|---|---|---|---|---|
| 1 | **Workspace access** entitlement | You, in the workspace | All CLI calls | `databricks` CLI can't reach the workspace |
| 2 | **Databricks SQL access** entitlement | You, in the workspace | `bundle deploy` calling the create-warehouse API (the bundle always manages its own warehouse) | `Error: not authorized to create SQL Endpoint` |
| 3 | **Allow cluster create** entitlement | You, in the workspace | `bundle deploy` for the warehouse and job compute | Warehouse / job creation rejected |
| 4 | **Databricks Apps: Can Manage** workspace permission | You, in the workspace | `bundle deploy` of the App resource | App creation rejected |
| 5 | **Databricks Database (Lakebase): Manager** entitlement | You, in the workspace | `bundle deploy` of the `postgres_projects` / `postgres_roles` resources | `Error: User does not have permission to create database instances` |
| 6 | **USE CATALOG** + **CREATE SCHEMA** on `<catalog_name>` | Your user or an admin group you're in | `bundle deploy` of the `schemas` and `volumes` resources | `Error: User does not have CREATE_SCHEMA on catalog '<catalog>'` |
| 7 | **MANAGE** on `<catalog_name>` (or be the catalog owner) | Your user or an admin group you're in | The one-time `GRANT USE CATALOG` to the intended **user groups**, the **app SP**, and the **task-runner SP** (see [The USE CATALOG prerequisite](#the-use-catalog-prerequisite)) | `Error: User does not have privilege MANAGE on catalog '<catalog>'` |
| 8 | **Service Principal: User** role on the task-runner SP | Your user, on the SP you'll use as `dqx_service_principal_application_id` | `bundle deploy` of the `jobs.dqx_task_runner` resource (sets `run_as.service_principal_name`) | `Error: User is not authorized to use this service principal` |

**Two convenience patterns** that reduce the per-user grants in rows 6 and 7:

- **Use a scoped UC-admin group:** ask an admin for membership in a group with the required catalog usage, schema creation, and `MANAGE` privileges. `ALL PRIVILEGES` does not include `MANAGE`.
- **Keep permission domains separate:** workspace administration does not automatically confer Unity Catalog ownership or Lakebase role/grant authority.

### Workspace features that must be enabled

These are configured at the workspace or account level — not by you, not by the bundle. Confirm with your admin before the first deploy:

- **Databricks Apps** is enabled on the workspace
- **User token passthrough** (a.k.a. user authorization / OBO) is enabled for Databricks Apps — see [Step 2](#step-2-enable-user-token-passthrough). Without this the app can't make OBO calls and Unity Catalog browsing fails.
- **Serverless compute** is enabled on the workspace — the task-runner job runs exclusively on serverless
- **Lakebase Postgres** is enabled on the workspace. Lakebase is mandatory for DQX Studio's transactional state. In the DAB path it is declared as a Postgres *project* bundle resource (`resources.postgres_projects.dqx_studio`, plus `resources.postgres_roles.app_sp` for the app SP's role) with `lifecycle.prevent_destroy: true` so a `bundle destroy` cannot drop it and wipe OLTP state — see [Stateful storage and destroy protection](#step-3-stateful-storage-and-destroy-protection). The app connects to the always-present `databricks_postgres` admin database via the project endpoint (`DQX_LAKEBASE_ENDPOINT`) and creates its own `dqx_studio` Postgres schema inside it on first connection — no separate logical-DB provisioning step.

### The catalog must already exist

The bundle **does not create the catalog itself** — that's deliberate. Catalogs are typically owned by a governance team and creating them requires `CREATE CATALOG` on the metastore. Pick an existing catalog you (or an admin group you're in) have rights on, and set `catalog_name` in [Step 4](#step-4-configure-databricksyml). The bundle creates the main, temporary, Genie, and demo schemas and the wheels volume *inside* that catalog — no `CREATE CATALOG` permission required at the metastore level.

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

DQX Studio's stateful resources — the main, temporary, Genie, and demo schemas, the wheels volume, and the Lakebase Postgres project — are all declared with `lifecycle.prevent_destroy: true` (Databricks CLI 0.268+), which **blocks `databricks bundle destroy` from dropping the resource** and wiping the data. All are declared at the base level in `app/databricks.yml`:

```bash
grep -A1 'lifecycle:' app/databricks.yml | head
```

You'll see `prevent_destroy: true` on `schemas.main_schema`, `schemas.tmp_schema`, `volumes.wheels`, and `postgres_projects.dqx_studio`.

> **The app's `dqx_studio` Postgres schema** (inside the `databricks_postgres` admin database on the Lakebase project) is created by the app at first start. It's stateful but lives below the resource layer DABs models, so `prevent_destroy` doesn't apply to it directly. The project-level guard above is what protects it: as long as `postgres_projects.dqx_studio` survives, the schema and its tables survive.

What this means in practice:

- **Deploy** — `make app-deploy` builds, deploys the resources in dependency order, applies bundle-declared grants natively, then starts the app. Catalog usage remains an administrator prerequisite.
- **Schema drift** — if you change `catalog_name`, `schema_name`, or the Lakebase project id in a way that would force the bundle to delete and recreate the resource, `prevent_destroy` blocks the destroy step and the deploy fails fast (good — the alternative is silent data loss). Treat those names as immutable.
- **Intentional teardown** — to drop a protected resource, remove `lifecycle.prevent_destroy: true` from `databricks.yml`, run `databricks bundle deployment unbind <key> -t <target>` to detach it from bundle state, then destroy it manually.

### The USE CATALOG prerequisite

`bundle deploy` applies the declared schema- and volume-level grants natively (via `grants:` on the resources); startup attempts `SELECT` on five approved Genie views and two metadata tables for configured audience groups. The bundle does not manage the pre-existing, user-selected catalog, so grant `USE CATALOG` once per catalog to the app and task-runner principals and the scoped audience. Get the app SP's client ID with `databricks apps get dqx-studio` after deployment:

```sql
GRANT USE CATALOG ON CATALOG <catalog> TO `<studio-user-group>`;
GRANT USE CATALOG ON CATALOG <catalog> TO `<app-sp-client-id>`;
GRANT USE CATALOG ON CATALOG <catalog> TO `<task-runner-sp-application-id>`;
```

The app SP also receives `USE CATALOG` through its wheels-volume binding; keep the explicit grant alongside the other principals. Bundle audience grants use `studio_user_group`, not broad built-in groups.

## Step 4: Configure `databricks.yml`

Add or update a deploy target. Only two variables are **required**:

- `catalog_name` — the existing Unity Catalog catalog to create schemas/volume in
- `dqx_service_principal_application_id` — the task-runner SP from [Step 1](#step-1-create-a-service-principal)

The bundle manages its own SQL warehouse and Lakebase project. Its `studio_user_group` defaults to `dqx-studio-users`, which **must already exist**; override it with your approved scoped group if needed. It is wired into `DQX_USER_GROUPS` as a JSON audience list and used for all bundle warehouse, app, dashboard, and schema audience grants. The repo ships a canonical source target, **`dev`** (marked `default: true`). A minimal target looks like this:

```yaml
targets:
  dev:
    default: true
    workspace:
      profile: <your-profile>
    variables:
      catalog_name: <your-catalog>
      dqx_service_principal_application_id: <your-sp-application-id>
      studio_user_group: dqx-studio-users
    presets:
      trigger_pause_status: PAUSED
```

### SQL warehouse

The bundle **always** creates and manages a dedicated serverless warehouse (`resources.sql_warehouses.dqx_sql_warehouse`) — there is no "bring your own warehouse" mode. Its `permissions:` block grants `CAN_USE` to the app SP and `studio_user_group` for end-user OBO queries. Tune it per target with `sql_warehouse_name` and `sql_warehouse_size`; `bundle destroy` deletes it. Marketplace uses an existing bound warehouse and administrator-managed ACLs.

### Lakebase

Lakebase is a bundle-managed Postgres **project** (`resources.postgres_projects.dqx_studio`) plus the app SP's Postgres role (`resources.postgres_roles.app_sp`, a `DATABRICKS_SUPERUSER` member so the app can create its own schema). The project auto-creates its default branch (`lakebase_branch`, default `dqx`) and a `primary` read/write endpoint; the app connects via that endpoint path (`DQX_LAKEBASE_ENDPOINT`). The endpoint scales to zero after `lakebase_suspend_timeout` of inactivity — the app's connection pool pre-pings on checkout and transparently reconnects (waking the endpoint) on the next request.

> **The default warehouse and Lakebase sizes are deliberately small.** The bundle ships a `Small` SQL warehouse and a 0.5–1 CU autoscaling, scale-to-zero Lakebase project — sized for a typical rules catalog (low-thousands of rows) and light concurrent use, and chosen to keep idle cost near zero. They are a sensible **starting point, not a tuned production configuration.** Watch the app logs and the warehouse / Lakebase metrics under real load and raise `sql_warehouse_size`, `lakebase_max_cu`, or `DQX_LAKEBASE_POOL_MAX_SIZE` if you see query queueing or connection-pool exhaustion (see [Troubleshooting](#troubleshooting)).

### Variable reference

All target-level variables, their defaults, and what they control:

| Variable | Default | Required? | Purpose |
|---|---|---|---|
| `catalog_name` | `dqx` | **Yes** | Unity Catalog catalog where schemas and the wheels volume are created. **Must already exist** — the bundle does not create the catalog itself. |
| `dqx_service_principal_application_id` | `00000000-…` | **Yes** | Application ID of the service principal that runs the task-runner job. Created in [Step 1](#step-1-create-a-service-principal). The placeholder default fails validation. |
| `studio_user_group` | `dqx-studio-users` | Must exist | Scoped audience group for warehouse, app, dashboard, and schema access; supplied to `DQX_USER_GROUPS` as a JSON list. Built-in `users` / `account users` are not valid audiences. |
| `admin_group` | `proj_dbw_dev_dg_admins-data_ug` (bundle) / `admins` (local Python) | Yes for prod | Workspace group whose members get the in-app `ADMIN` role unconditionally (bootstrap admin path). The bundle ships with a non-production placeholder — override per target with your real admin group (e.g. `dqx-admins-prod`). In every path, `AppConfig` defaults to the workspace `admins` group; `DQX_ADMIN_GROUP` overrides that default for local testing or deployment. Additional roles are assigned at runtime via the in-app Role Management UI. |
| `app_name` | `dqx-studio` | No | Deployed Databricks App name. Override per target (e.g. `dqx-studio-dev`, `dqx-studio-prod`) when deploying multiple targets to the same workspace, or for personal sandboxes. |
| `sql_warehouse_name` | `dqx-studio-sql-warehouse` | No | Name of the bundle-managed SQL warehouse. Override per target to avoid duplicates in shared workspaces. |
| `sql_warehouse_size` | `Small` | No | Cluster size of the bundle-managed warehouse (e.g. `2X-Small`, `Small`, `Medium`). |
| `schema_name` | `dqx_studio` | No | Main schema — holds run history, profiling, metrics, and quarantine tables. Declared as `resources.schemas.main_schema` in the bundle with `lifecycle.prevent_destroy: true`. |
| `tmp_schema_name` | `dqx_studio_tmp` | No | Per-user temp-view schema. Declared as `resources.schemas.tmp_schema` with `lifecycle.prevent_destroy: true`. |
| `genie_schema_name` | `genie` | No | Genie-facing derived views and dimensions. Declared as `resources.schemas.genie_schema` with `lifecycle.prevent_destroy: true`. Existing DAB targets retain this default to avoid replacing a protected schema; for new targets, set it to a dedicated name such as `dqx_studio_genie`. |
| `wheels_volume_name` | `wheels` | No | UC volume under `<catalog>.<schema_name>` for the DQX + task-runner wheels. Declared as `resources.volumes.wheels` with `lifecycle.prevent_destroy: true`. |
| `lakebase_project_id` | `dqx-studio-db` | No | Lakebase Postgres project id for OLTP state. Declared as `resources.postgres_projects.dqx_studio` with `lifecycle.prevent_destroy: true`. Autoscaling + scale-to-zero per [Lakebase Autoscaling](https://docs.databricks.com/aws/en/oltp/upgrade-to-autoscaling). |
| `lakebase_branch` | `dqx` | No | Project branch the app uses; auto-created with a `primary` endpoint on first deploy. |
| `lakebase_endpoint` | `projects/<project>/branches/<branch>/endpoints/primary` | No | Required endpoint resource path (`DQX_LAKEBASE_ENDPOINT`) driving host resolution + OAuth. Derived from project + branch. |
| `lakebase_database_name` | `databricks_postgres` | No | Logical Postgres database inside the Lakebase instance the app connects to. Defaults to `databricks_postgres` (always present, no provisioning step). Override only if you've manually created a different logical DB you want to use. |
| `lakebase_schema_name` | `dqx_studio` | No | Postgres schema inside that database where OLTP tables live (`DQX_LAKEBASE_SCHEMA`). Created by the app on first start. Isolate per target (e.g. `dqx_studio_v2`) when two apps share a Lakebase project — otherwise migrations fail with `must be owner of table …` against tables owned by another app SP. |
| `lakebase_min_cu` / `lakebase_max_cu` | `0.5` / `1` | No | Autoscaling compute-unit range for the project endpoint. Raise the max if Lakebase queries queue in the app logs. |
| `lakebase_suspend_timeout` | `300s` | No | Idle window before the endpoint scales to zero (60s–604800s). The app pre-pings and reconnects transparently on the next request after suspension. |

> **Note on duplicate names in Databricks:** SQL warehouses, jobs, and apps within the same workspace are tracked by ID, not by name, so technically duplicates are allowed. Operators browse the Jobs / Apps / Warehouses / Databases UI by name, so distinct names per target are strongly recommended when you deploy more than one target to the same workspace.

## Deploy a tagged Studio release without building locally

Use a Studio release tag that contains both `app/marketplace/` and the `release` bundle target. Tags published before this target was added require a source build. For direct CLI deployment:

```bash
git clone --branch studio-vX.Y.Z https://github.com/databrickslabs/dqx.git
cd dqx/app
databricks bundle deploy -p <your-profile> -t release --var catalog_name=<your-catalog> --var dqx_service_principal_application_id=<your-sp-application-id>
```

The release target uses the compiled app source and task-runner wheel in `marketplace/`. The bundle still provisions and grants its own workspace resources. Grant `USE CATALOG` on the pre-existing catalog to the app and task-runner service principals after deploy, and to the intended user groups for OBO access, then start the app:

```bash
databricks bundle run dqx-studio -p <your-profile> -t release --var catalog_name=<your-catalog> --var dqx_service_principal_application_id=<your-sp-application-id>
```

See [The `USE CATALOG` prerequisite](#the-use-catalog-prerequisite) for the grant statements.

For upgrades after the catalog grants are in place, the helper commands deploy and start the tagged release without running `app-build`. Run one from the repository root (`cd ..` if you followed the CLI example above). On macOS or Linux:

```bash
make app-deploy PROFILE=<your-profile> TARGET=release BUNDLE_VARS='--var catalog_name=<your-catalog> --var dqx_service_principal_application_id=<your-sp-application-id>'
```

On Windows, use the experimental PowerShell helper:

```powershell
.\make.ps1 app-deploy -Profile <your-profile> -Release -BundleVars @('catalog_name=<your-catalog>', 'dqx_service_principal_application_id=<your-sp-application-id>')
```

Apply the catalog grants above before using Studio. The PowerShell helper also accepts `-Target release` instead of `-Release`.

## Step 5: One-Command Source Deploy

Build, deploy, and start the app in a single command:

```bash
make app-deploy PROFILE=<your-profile> TARGET=<your-target>
```

On Windows, run the experimental PowerShell helper from the repository root:

```powershell
.\make.ps1 app-deploy -Profile <your-profile> -Target <your-target>
```

The script requires `uv`, Node.js 18+, yarn classic v1, and Databricks CLI v1.4.0+ on `PATH`. It builds the app, deploys the bundle, and starts it. Pass `-Force` to overwrite remote bundle edits, `-AppName` to set the bundle's `app_name` variable (the deployed app name), or `-BundleVars 'catalog_name=foo','other_name=value'` to forward bundle variables. The `bundle run` resource key remains `dqx-studio`.

`make app-deploy` runs the following steps automatically:
1. `make app-build` — builds the frontend and wheels.
2. `databricks bundle deploy` — provisions or updates the schemas, wheels volume, Lakebase project (+ endpoint + the app SP's Postgres role), the SQL warehouse, the task-runner job, and the Databricks App in dependency order, and applies **bundle-declared grants** via the `grants:` / `permissions:` blocks in `databricks.yml`. Stateful resources carry `lifecycle.prevent_destroy: true` so a future destroy can't drop them — see [Step 3](#step-3-stateful-storage-and-destroy-protection).
3. `databricks bundle run` — starts the app.

Remember the manual prerequisites: [`GRANT USE CATALOG`](#the-use-catalog-prerequisite) for the app SP, task-runner SP, and scoped audience. Cold-start UC checks must pass before Studio is ready.

> **First start**: The app runs Delta analytical and Lakebase application migrations on startup, and publishes the task-runner wheel to the UC volume. Wait for the setup checks to report that wheel publishing is ready before triggering runs. Also wait for `"Lakebase OLTP routing enabled"` before opening the UI. If Lakebase initialization fails, the app refuses to start and the Apps platform restarts the container. It never falls back to Delta-backed application state.

### Step-by-step alternative

If you prefer to run each step individually:

```bash
# Build
make app-build

# Deploy the bundle (creates / updates schemas, volume, Lakebase project +
# endpoint + role, the SQL warehouse, task-runner job, and app, and applies
# bundle-declared grants natively)
cd app && databricks bundle deploy -p <your-profile> -t <your-target>

# Start the app
cd app && databricks bundle run dqx-studio -p <your-profile> -t <your-target>
```

### Grants reference

`bundle deploy` applies its declared schema and volume grants natively; activation attempts the five Genie view and two metadata-table audience grants best effort. The narrow runner grants below are sufficient for setup and manual recovery even if a DAB deployment declares wider SP access. Grant [`USE CATALOG`](#the-use-catalog-prerequisite) separately, and reapply audience object grants manually if the app lacks grant authority. Substitute your configured main, temporary, and Genie schema names.

The reference below includes bundle-managed grants and the best-effort Genie view grants applied during activation. `<app-sp-id>` is the app's auto-created SP (`databricks apps get dqx-studio` → `service_principal_client_id`); `<job-sp-id>` is the task-runner SP from [Step 1](#step-1-create-a-service-principal).

```sql
-- App SP: application storage and migration privileges.
GRANT ALL PRIVILEGES ON SCHEMA <catalog>.dqx_studio     TO `<app-sp-id>`;
GRANT ALL PRIVILEGES ON SCHEMA <catalog>.dqx_studio_tmp TO `<app-sp-id>`;
GRANT ALL PRIVILEGES ON SCHEMA <catalog>.<genie_schema_name> TO `<app-sp-id>`;
GRANT ALL PRIVILEGES ON VOLUME <catalog>.dqx_studio.wheels TO `<app-sp-id>`;

-- Runner: schema-wide storage access, applied by an authorized administrator.
-- Use a dedicated Studio schema: SELECT/MODIFY cover its current and future tables.
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA <catalog>.dqx_studio TO `<job-sp-id>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.dqx_studio_tmp TO `<job-sp-id>`;
GRANT READ VOLUME ON VOLUME <catalog>.dqx_studio.wheels TO `<job-sp-id>`;

-- End users create dry-run / preview temp views (via their OBO token) in the
-- tmp schema, so they need USE SCHEMA + CREATE TABLE there.
GRANT USE SCHEMA, CREATE TABLE ON SCHEMA <catalog>.dqx_studio_tmp TO `<studio-user-group>`;
-- Genie SELECT is restricted to these five views and two metadata tables.
-- Never grant schema-wide SELECT or access to dq_user_table_entitlements.
GRANT USE SCHEMA ON SCHEMA <catalog>.<genie_schema_name> TO `<studio-user-group>`;
GRANT SELECT ON TABLE <catalog>.<genie_schema_name>.mv_dq_scores TO `<studio-user-group>`;
GRANT SELECT ON TABLE <catalog>.<genie_schema_name>.v_dq_check_results TO `<studio-user-group>`;
GRANT SELECT ON TABLE <catalog>.<genie_schema_name>.v_dq_check_results_asof TO `<studio-user-group>`;
GRANT SELECT ON TABLE <catalog>.<genie_schema_name>.v_dq_check_attribution TO `<studio-user-group>`;
GRANT SELECT ON TABLE <catalog>.<genie_schema_name>.v_dq_failing_rows TO `<studio-user-group>`;
GRANT SELECT ON TABLE <catalog>.<genie_schema_name>.dim_dq_rules TO `<studio-user-group>`;
GRANT SELECT ON TABLE <catalog>.<genie_schema_name>.dim_dq_monitored_tables TO `<studio-user-group>`;

-- Deployer needs SELECT for the embed-credentials Insights dashboard
-- (bundle uses ${workspace.current_user.userName}).
GRANT USE SCHEMA, SELECT ON SCHEMA <catalog>.dqx_studio TO `<deployer>`;

-- USE CATALOG on the app SP is also auto-granted by its wheels-volume binding;
-- keep the explicit grant alongside the other principals as in the install steps.
GRANT USE CATALOG ON CATALOG <catalog> TO `<studio-user-group>`;
GRANT USE CATALOG ON CATALOG <catalog> TO `<app-sp-client-id>`;
GRANT USE CATALOG ON CATALOG <catalog> TO `<job-sp-id>`;
```

Warehouse `CAN_USE` (app SP + `studio_user_group`) is applied natively by the bundle. Its app and dashboard audience ACLs also use that scoped group.

### Temporary views and schedules

User-initiated views are created under OBO. Each view grants `SELECT` directly to the job's resolved runner and `MANAGE` to the app SP for orphan cleanup. Grant failures block submission. Cleanup rights are **per view**, not schema-wide, and no `account users` grant is restored:

```sql
-- Apply to an existing OBO-owned view only if these narrow grants are missing.
GRANT SELECT ON VIEW <catalog>.<tmp_schema>.<view_name> TO `<job-sp-id>`;
GRANT MANAGE ON VIEW <catalog>.<tmp_schema>.<view_name> TO `<app-sp-id>`;
```

Schedule creation/update inspects access using OBO SQL `SHOW GRANTS`, not the grants REST API. Source catalog `USE CATALOG`, source schema `USE SCHEMA`, and table `SELECT` are verified for the app scheduler and actual job runner per schedule. If missing, an authorized caller must establish those grants; failed runner grants block the schedule rather than allowing a later run to fail. These source grants are separate from the fixed startup output checks.

### Genie audience access

Configured `DQX_USER_GROUPS` receive scoped Genie space `CAN_RUN` ACLs; with `[]`, an administrator manages the audience ACLs and UC object grants. If reconciliation fails, an administrator must apply the scoped ACLs. Genie consumers need Consumer access or Databricks SQL access entitlement, parent catalog/schema usage, and the five approved view plus two dimension-table grants above. Genie embeds the author's compute credentials; consumers do not need direct warehouse access for Genie alone. Studio's OBO previews and SQL workflows additionally need SQL access entitlement and warehouse `CAN_USE`. In-app roles or object permissions alone do not provide this external access. Never grant whole-schema Genie `SELECT` or audience access to `dq_user_table_entitlements`.

Both DAB and Marketplace use the `genie` user-authorization scope. Existing installations need renewed user consent after the scope changes; restart/redeploy and sign in again. Replacing the bound warehouse reconciles the Genie space's compute; verify app-SP `CAN_USE` and the OBO audience's SQL access on the replacement warehouse.

To grant app access in Marketplace or recover an ACL, go to **Apps → `<app-name>` → Permissions** and assign `Can Use` to the scoped audience. DAB applies this via `studio_user_group`. Replace `<app-name>` with the configured app name (default `dqx-studio`).

Access the app at:
```
https://<your-workspace-url>/apps/<app-name>
```

## Lakebase backend

DQX Studio stores its **OLTP state** — rules catalog, app settings, RBAC, comments, schedule configs, and scheduler bookkeeping — in a Lakebase Postgres instance for sub-millisecond reads and to avoid SQL warehouse cold starts. **Append-mostly observability tables** (`dq_validation_runs`, `dq_profiling_results`, `dq_metrics`, `dq_quarantine_records`) live in Delta because they're written by the Spark task runner and queried by AI/BI dashboards.

| Backend | Tables | Why |
|---|---|---|
| Delta Lake | `dq_validation_runs`, `dq_profiling_results`, `dq_quarantine_records`, `dq_metrics`, `dq_run_configs` | High-volume append; Spark task runner reads and writes them; columnar reads. |
| Lakebase Postgres | `dq_app_settings`, `dq_role_mappings`, `dq_quality_rules`, `dq_quality_rules_history`, `dq_comments`, `dq_schedule_configs`, `dq_schedule_configs_history`, `dq_schedule_runs` | OLTP — sub-ms reads from FastAPI handlers, row-level upserts, primary keys. |

Lakebase is declared as a Postgres *project* bundle resource (`postgres_projects.dqx_studio`, plus `postgres_roles.app_sp`) and provisioned by `databricks bundle deploy` with `lifecycle.prevent_destroy: true` — see [Step 3](#step-3-stateful-storage-and-destroy-protection). The app connects to the always-present `databricks_postgres` admin database via the project endpoint (`DQX_LAKEBASE_ENDPOINT`) and creates its own `dqx_studio` Postgres schema there on first start. The bundle creates the app SP's migration role. The task runner does not connect to Lakebase.

### Lakebase token rotation

Lakebase OAuth tokens expire after one hour. The app's `PgExecutor` runs a background daemon thread that refreshes the password every `DQX_LAKEBASE_TOKEN_REFRESH_MINUTES` minutes (default 50). Existing connections age out via `psycopg_pool.ConnectionPool.max_lifetime` so a long-running app can stay up indefinitely without reconnecting.

## OAuth scopes and consent

DAB and Marketplace declare the supported user scopes, including `sql` for OBO queries and grant inspection and `genie` for Ask Genie. Check **Apps → `<app-name>` → User authorization** if a feature reports missing consent, then sign in again to approve updated scopes after deployment. OAuth consent does not grant UC privileges, warehouse access, or Genie space access; apply the scoped permissions described above. Runner setup and schedule grant inspection use SQL rather than requiring broader grants REST API scopes.

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
3. Confirm the app SP has `CAN_MANAGE` on the job (set automatically by DABs), and the actual `run_as` runner passes the cold-start UC checks.
4. For OBO views, confirm runner `SELECT` and app-SP `MANAGE` were granted on the specific view; do not restore `account users` access.

**An oversized-config run fails while reading its staged configuration:**
The runner reads the row from `<catalog>.<schema>.dq_run_configs`. Confirm the Delta migrations have run (the table exists) and that the actual Jobs `run_as` principal has schema-level `SELECT` / `MODIFY` on the main schema.

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
The app deliberately refuses to start when Lakebase initialization fails — it never falls back to Delta-backed application state. Diagnose with the steps below; the Apps platform will pick up the next successful start automatically.

1. Confirm the Lakebase project + endpoint exist and are running (Compute → Database Instances in the workspace UI). If missing, re-run `databricks bundle deploy`; if the endpoint is still `STARTING`, wait and the next restart will succeed. (A suspended endpoint is fine — the app's pre-ping pool wakes it on connect.)
2. Confirm the app SP's Postgres role exists on the project branch — it's created by the `postgres_roles.app_sp` resource. Redeploy if the role is missing.
3. If the failure is specifically a Postgres `permission denied for database databricks_postgres` (or `permission denied to create schema`), the app SP can connect but lacks `CREATE` on the system `databricks_postgres` database — that privilege comes from the `DATABRICKS_SUPERUSER` membership in `postgres_roles.app_sp`. Confirm that block deployed (CLI ≥ 1.4.0), or run a one-time `GRANT CREATE ON DATABASE databricks_postgres TO "<app-sp-client-id>"` against the project endpoint.
4. If the failure is `must be owner of table <name>` during startup migrations, the Lakebase `dqx_studio` schema objects are owned by a Postgres role other than the app's service principal — most often the human deployer after local dev (`make app-start-dev`) or `seed_demo.py` against the same Lakebase project. Postgres requires table ownership for `ALTER TABLE`; the app SP's `DATABRICKS_SUPERUSER` membership grants broad DML but does not let a non-owner add columns. Avoid pointing local dev at production Lakebase endpoints, and resolve ownership before retrying the deployment.
5. Confirm OAuth token issuance is healthy — Lakebase tokens currently expire after one hour; a misconfigured OAuth integration or revoked SP credential will surface here.
6. Recheck that the Lakebase endpoint and the caller's Postgres role are valid. DQX Studio requires Lakebase; there is no Delta-only mode or migration from the removed Delta-backed application state.

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

**Query identity**: the dashboard is configured with `embed_credentials: true`, so queries run as the bundle deployer rather than the iframe viewer. This is deliberate — end users receive no SELECT on the main-schema tables, keeping `dq_quarantine_records` (potentially PII row payloads) off the workspace UC surface. Genie access is limited to five approved views and two metadata tables. The widgets in the starter only expose aggregated counts and run metadata, so deployer-credentialed queries don't leak anything a viewer couldn't already see in the Runs History page. To switch to viewer-credentials, flip `embed_credentials` to `false` and grant `SELECT` on the DQX tables to the audience you want to expose.

The bundle grants the deployer `USE SCHEMA` + `SELECT ON SCHEMA <catalog>.<schema>` natively via a `grants:` entry that resolves `${workspace.current_user.userName}` at deploy time, so the dashboard works end-to-end without manual UC plumbing. It resolves for both human deploys (grants the email) and SP-based deploys (grants the application ID).

**One operational caveat**: the "deployer" identity is whoever ran `databricks bundle deploy` (the human or service principal authenticated to the workspace at deploy time). If that identity later loses access to the DQX tables, dashboard tiles will fail to render until someone with access redeploys. For production, deploy the bundle as a stable service principal so the dashboard identity doesn't follow individual humans.

## Run review status

DQX Studio lets reviewers attach a per-run **review status** (e.g. *Pending review*, *Acknowledged*, *Resolved*, *False positive*) to each validation run from the expanded row on the **Runs History** page. The same value is filterable from the toolbar so a business owner can ask "what's still pending?" in one click.

- **Configurable catalogue** — admins manage the list of allowed values (label, description, colour) under **Configuration → Run review statuses**. Exactly one entry must be marked **Default**; that value is what unreviewed runs surface virtually (no row is written until someone explicitly reviews). The backend enforces the single-default invariant on save.
- **Audit trail** — every change appends to `dq_run_review_status_history`, surfaced as an "Activity" timeline inside the review-status panel. The current value lives in `dq_run_review_status` (one row per reviewed run).
- **Storage** — both tables are OLTP-shaped (single-key lookups, frequent mutation), so they live in required Lakebase Postgres alongside comments and role mappings. No extra deployment configuration is needed.
- **Permissions** — any authenticated app user can set or change a review status, mirroring how comments work. Only admins can edit the catalogue itself.
