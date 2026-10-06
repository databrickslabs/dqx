# Deployment (Declarative Automation Bundles and Marketplace)

Production deployment uses [Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/) (DABs, formerly known as Databricks Asset Bundles) via the Databricks CLI (`databricks bundle deploy`), or the Databricks Marketplace listing. For local development, see [DEVELOPMENT.md](DEVELOPMENT.md).

> **Breaking change: fresh installs only, no upgrade path.** This release changes Studio's storage layout and permission model for both DAB and Marketplace. Existing installations cannot be upgraded or migrated. Before installing, remove the previous app, its task-runner job, its SQL warehouse (DAB-managed), its Unity Catalog schemas and wheels volume, and its Lakebase project. See [Removing a previous installation](#removing-a-previous-installation).

## Choose an installation path

**DAB deployment** of a tagged Studio release uses the prebuilt `app/marketplace/` artifact with the `release` target; local build tools are not required. Use `make app-deploy PROFILE=<profile> TARGET=release` on macOS/Linux or the experimental `make.ps1 -Release` helper on Windows. Developers building from source use the same helpers with a source target. The bundle owns the deployment configuration (catalog, prefix, audience); the setup page shows it read-only.

**Marketplace installation** binds only two resources: a SQL warehouse (the app service principal is granted `CAN_MANAGE`) and a Lakebase Postgres endpoint. There is no Unity Catalog volume binding. After installation, a workspace administrator opens Studio and submits a **setup form** with the catalog, a storage prefix, and the audience group. A workspace service principal for the task-runner job is still required; it is assigned as the job's `run_as` identity in the Jobs UI.

Both paths finish in the same readiness workflow (setup page). Studio APIs stay gated until every step passes.

## Minimal setup

### Marketplace

1. **Install** the listing and bind the SQL warehouse and the Lakebase endpoint. Have an existing catalog and a dedicated audience group (for example `dqx-studio-users`) ready; the audience must be an existing workspace group. The built-in `users`, `account users`, and `admins` groups are rejected. Also create the task-runner service principal and grant the installing identity the Service Principal: User role on it.
2. **Share the app** with the audience group and the administrator group: **Compute > Apps > `<app-name>` > Permissions**, assign `Can Use`. Studio cannot always read or change its own app sharing, so this is a manual step (see [App sharing](#app-sharing)).
3. **Open Studio as a workspace administrator** (a member of `admins` or of the group named by `DQX_ADMIN_GROUP`).
4. In the setup form, enter the **catalog**, the **storage prefix** (default `dqx_studio`), and the **audience group**, then submit.
5. **Run the printed `GRANT` statements** if setup reports missing access. The app service principal needs `USE CATALOG` and `CREATE SCHEMA` on the catalog, and the audience (and a custom admin group) needs `USE CATALOG`. Setup attempts the audience grants first; the app service principal and runner grants need an administrator with grant authority. Choose **Verify again** after each change. Assign the task-runner service principal as the job's `run_as` identity when prompted.
6. **Assign roles**: in Studio open **Admin Settings > Entitlements** and map the audience groups to the Author, Approver, and Viewer roles. Onboarding afterwards is group membership.

### DAB

```bash
make app-deploy PROFILE=<profile> TARGET=<target> \
  STUDIO_PREFIX=<prefix> STUDIO_USER_GROUP=<group|users>
```

Both variables are optional. `STUDIO_PREFIX` sets the bundle variable `prefix` (default `dqx_studio`) and `STUDIO_USER_GROUP` sets `studio_user_group` (default `dqx-studio-users`, which must already exist). An explicit `--var prefix=...` or `--var studio_user_group=...` in `BUNDLE_VARS` always wins. The experimental PowerShell helper has no equivalent arguments; pass `-BundleVars @('prefix=<prefix>', 'studio_user_group=<group>')` (broad mode on Windows also needs `'studio_uc_principal=account users'`). After the deploy, grant the catalog prerequisites that setup prints (see [The USE CATALOG prerequisite](#the-use-catalog-prerequisite)) and assign roles in Studio.

## Storage layout

An installation selects an existing catalog and a validated prefix. The prefix must start with a lower-case letter and contain only lower-case letters, digits, and underscores (at most 64 characters). Studio derives:

```text
<catalog>
  <prefix>                  main Studio schema
    wheels                  managed UC volume (app and runner wheels)
  <prefix>_tmp              OBO temporary views
  <prefix>_genie            approved Genie views and metadata dimensions
  <prefix>_demo             demo content
```

The wheels volume always lives in the main schema. The Lakebase Postgres schema (`DQX_LAKEBASE_SCHEMA`) does **not** follow the prefix. Setup rejects derived names that are invalid or duplicated, refuses to adopt an existing schema that the app service principal does not own or manage (`storage_collision`: pick another prefix or drop/rename the schema), and rejects changes to the catalog or prefix once storage exists. For Marketplace, setup creates the schemas and the volume; for DAB, the bundle creates them.

## Audience, broad mode, and administrators

**Audience mapping.**

| Audience input | Workspace ACL principal | Unity Catalog principal |
|---|---|---|
| `users` (DAB broad mode only) | `users` | `account users` |
| A dedicated group | the group | the same group |

**Broad mode** (`STUDIO_USER_GROUP=users` on DAB) shares the app, warehouse, dashboard, and Genie space with the workspace `users` group and grants Unity Catalog access to `account users`, which is **account-wide**. It is identified as such in configuration and is never implemented by nesting built-in groups. The Marketplace setup form never accepts it. An explicit `studio_user_group` in `BUNDLE_VARS` suppresses the automatic `account users` UC principal.

**Administrator access.** Members of the group named by `DQX_ADMIN_GROUP` **or** the workspace `admins` group may run setup (form submission and reconcile); authorization is a fresh OBO SCIM lookup of the caller, never forwarded headers or in-app role mappings. Unity Catalog cannot grant to the workspace-local `admins` group, so UC grants for administrators are made **only** to a custom `DQX_ADMIN_GROUP` account group, which receives the same warehouse, temporary, Genie, and demo access as the audience plus app `CAN_USE`. When `DQX_ADMIN_GROUP` is `admins` (the default), no administrator UC grants are made and administrators must also be members of the audience group to use OBO features. Audience membership never implies the in-app `ADMIN` role, and the `ADMIN` role supplies no Unity Catalog access.

## Permission matrix

| Resource | App SP | Runner SP | Audience | Admin group |
| --- | --- | --- | --- | --- |
| Catalog | `USE CATALOG`, `CREATE SCHEMA` | `USE CATALOG` | `USE CATALOG` | `USE CATALOG` |
| Main schema | Owner (Marketplace); `ALL_PRIVILEGES` + `MANAGE` (DAB) | `USE SCHEMA`, `SELECT`, `MODIFY` | None | None |
| Wheels volume | Owner / `ALL_PRIVILEGES` | `READ VOLUME` | None | None |
| `_tmp` schema | Owner / `ALL_PRIVILEGES` + `MANAGE` | `USE SCHEMA`; per-view `SELECT` | `USE SCHEMA`, `CREATE TABLE` | Same as audience |
| `_genie` schema | Owner / `ALL_PRIVILEGES` + `MANAGE` | None | `USE SCHEMA`; `SELECT` on allowlist only | Same as audience |
| `_demo` schema | Owner / `ALL_PRIVILEGES` | None | `USE SCHEMA`, `SELECT` | Same as audience |
| SQL warehouse | `CAN_MANAGE` | Not needed | `CAN_USE` | `CAN_USE` |
| App | Runtime identity | None | `CAN_USE` | `CAN_USE` |
| Task-runner job | `CAN_MANAGE` | Configured `run_as` | None | Managed via setup |
| Genie space (if configured) | Manage | None | `CAN_RUN` | `CAN_RUN` |
| Dashboard (if configured) | Publisher | None | `CAN_READ` | `CAN_READ` |
| Lakebase app schema | Owner, migrations, CRUD | None | None | None |
| User source data | Authorized scheduled workloads only | Run-specific verified access | Caller's own OBO access | Caller's own OBO access |

DAB schemas are owned by the deploying identity (the bundle has no declarative owner override), so the app service principal gets `ALL_PRIVILEGES` plus `MANAGE` on the Studio schemas instead; `ALL_PRIVILEGES` alone does not confer permission management. The Genie allowlist is exactly five approved views (`mv_dq_scores`, `v_dq_check_results`, `v_dq_check_results_asof`, `v_dq_check_attribution`, `v_dq_failing_rows`) plus the `dim_dq_rules` and `dim_dq_monitored_tables` dimensions. Studio never grants whole-schema Genie `SELECT`, quarantine tables, or `dq_user_table_entitlements`. In-app roles (Author, Approver, Viewer) are separate from these infrastructure permissions.

## How setup applies and verifies access

Setup applies the grants and ACL updates it has authority for, then **re-reads** the resulting state; only the re-read decides the outcome. Missing grants and grants that cannot be inspected are distinct `action_required` results and **both block readiness**. ACL updates are additive: Studio never replaces a warehouse, Genie space, dashboard, or app ACL, and preserves unrelated principals on shared resources. Ordered steps: identity, Lakebase, configuration, Unity Catalog (catalog access), storage, warehouse, task runner, wheels, migrations, activation, access, app sharing. The access step runs after activation because the Genie objects must exist first. Reconciles are serialized, idempotent, and re-check every step even when ready; failures never delete persistent storage.

Setup first tries effective UC grant inspection with app credentials. If unavailable, the setup administrator's request-scoped OBO SQL executor inspects `SHOW GRANTS` on the target and parent containers using the supported `sql` OAuth scope. The administrator needs ownership, `READ METADATA`, `MANAGE`, or metastore administration to inspect grants, plus warehouse `CAN_USE` and parent `USE CATALOG` / `USE SCHEMA`. If unattended startup cannot inspect access as the app SP, open setup as an authorized administrator and verify again after each restart.

**Audience and administrators.** For the configured audience principals (and a custom admin group), setup grants and verifies catalog `USE CATALOG`; `USE SCHEMA` and `CREATE TABLE` on the temporary schema; `USE SCHEMA` on the Genie schema and `SELECT` on each allowlisted object; `USE SCHEMA` and `SELECT` on the demo schema; warehouse `CAN_USE`; Genie space `CAN_RUN` and dashboard `CAN_READ` when those are configured (otherwise reported as not applicable). Studio does not create dashboards for Marketplace installs. If the app service principal lacks grant authority on the catalog, the printed `GRANT` statements must be run by an administrator.

**Task-runner service principal.** Setup resolves the job's actual `run_as` and checks the following on **every cold startup**; it applies the schema and volume grants itself (never `ALL PRIVILEGES`, catalog privileges, or anything on the Genie or demo schemas) and verifies the result:

| Resource | Required runner access |
|---|---|
| Catalog | `USE CATALOG` (administrator grant) |
| Main and temporary schemas | `USE SCHEMA` on both |
| Wheels volume | `READ VOLUME` |
| Main schema | Schema-level `SELECT` and `MODIFY`, covering current and future tables |

Checks include inherited permissions and ownership; table-specific output grants alone do not satisfy the main-schema requirement, and schema ownership alone does not establish `SELECT` or `MODIFY` on its tables. Because the grant covers every table in the main schema, keep unrelated data out of the Studio schema. Verification is never cached across startups and setup never changes `run_as`. Source-data access remains run-specific. Runner Lakebase roles and privileges are outside setup verification (see [Task-runner Lakebase access](#task-runner-lakebase-access)). Successful metadata-dimension refreshes retry the two explicit metadata-table `SELECT` grants.

## App sharing

The audience and administrator groups need `CAN_USE` on the Databricks App itself. The last setup step (`app_sharing`) reads the app ACL as the app service principal, then as the setup administrator. If it is readable and a group lacks `CAN_USE`, readiness is blocked. If no identity can read it, this is the **one exception**: the step reports a **warning** naming the groups to share with, and readiness is not blocked. Setup never writes the app ACL for Marketplace installs; share the app manually in **Compute > Apps > `<app-name>` > Permissions**. DAB applies the app ACL from `studio_user_group` and `admin_group` natively.

## Per-user boundaries

Some access cannot be proven for a group and is checked for the active user on the relevant workflow instead: the user's **Databricks SQL access entitlement**, their **OAuth consent** for the declared user scopes (sign in again after a scope change), and **source-data privileges** on the tables they profile or validate. Setup never grants source data. Per-view runner `SELECT` and app-SP cleanup `MANAGE` failures block run submission, and scheduled source-access checks verify the app scheduler and the runner.

## Removing a previous installation

There is no in-place upgrade. Remove, in this order: the previous app, the task-runner job, the DAB-managed SQL warehouse (or detach the Marketplace binding), the Unity Catalog schemas and wheels volume of the old layout, and the Lakebase project (or its Studio Postgres schema). Previously granted broad audience access (`users` / `account users` ACLs and UC grants) is not revoked automatically; review it before reinstalling. See [Uninstall](https://databrickslabs.github.io/dqx/docs/installation#uninstall-dqx-studio) in the installation guide for the commands.

## Configuration notes

`DQX_USER_GROUPS` accepts a JSON list of audience groups, for example `["dqx-studio-users"]`, or simple unquoted comma-separated names. DAB wires it from `studio_user_group`; a Marketplace installation stores the single group from the setup form in Lakebase and resolves it from there on restart. Built-in `account users` and `admins` are rejected everywhere, and `users` is accepted only by DAB (broad mode, on its own). An empty list is invalid when the deployment supplies storage (`DQX_CATALOG` set), because the audience is required.

Lakebase is mandatory. Delta-backed application (OLTP) state was removed and cannot be migrated. Marketplace currently supports replacing the SQL warehouse; a rebind re-runs the warehouse checks and reconciles the Genie space's compute with the new warehouse. Swapping the Lakebase endpoint and smoke-validating the task runner are planned follow-up capabilities. Changing catalog or prefix after storage exists is rejected.

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
| Main UC schema and wheels volume | Created by bundle | Created by setup | Tagged only for DAB |
| Temporary and Genie schemas | Created by bundle | Created by setup | Tagged in both paths |
| Persistent Studio UC tables/views | Created by startup/migrations | Created by startup/migrations | Tagged in both paths |
| Demo schema/tables | Created by bundle/demo workflow | Schema created by setup; tables when demo is deployed | Schema tagged only for DAB |
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
| 7 | **MANAGE** on `<catalog_name>` (or be the catalog owner) | Your user or an admin group you're in | The one-time `GRANT USE CATALOG` to the intended **audience** (and a custom admin group), the **app SP**, and the **task-runner SP** (see [The USE CATALOG prerequisite](#the-use-catalog-prerequisite)) | `Error: User does not have privilege MANAGE on catalog '<catalog>'` |
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

`bundle deploy` applies the declared schema- and volume-level grants natively (via `grants:` on the resources). The bundle does not manage the pre-existing, user-selected catalog, so an administrator with grant authority on it runs the catalog grants once. Get the app SP's client ID with `databricks apps get dqx-studio` after deployment. Setup verifies catalog access on every start for both paths, and the app SP needs both `USE CATALOG` and `CREATE SCHEMA` (DAB's wheels-volume binding also auto-grants `USE CATALOG`, but Marketplace has no such binding, so grant both explicitly):

```sql
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG <catalog> TO `<app-sp-client-id>`;
GRANT USE CATALOG ON CATALOG <catalog> TO `<task-runner-sp-application-id>`;
GRANT USE CATALOG ON CATALOG <catalog> TO `<uc-audience-principal>`;
-- Only with a custom DQX_ADMIN_GROUP account group (never for `admins`):
GRANT USE CATALOG ON CATALOG <catalog> TO `<admin-group>`;
```

`<uc-audience-principal>` is `studio_user_group`, or `account users` in broad mode (`studio_uc_principal`). Setup attempts the audience grants itself when the app SP has grant authority, then verifies them; anything it cannot apply is printed as a `GRANT` statement and blocks readiness until done. Separately provision the runner's Lakebase role and scoped grants below.

## Step 4: Configure `databricks.yml`

Add or update a deploy target. Only two variables are **required**:

- `catalog_name` — the existing Unity Catalog catalog to create schemas/volume in
- `dqx_service_principal_application_id` — the task-runner SP from [Step 1](#step-1-create-a-service-principal)

The bundle manages its own SQL warehouse and Lakebase project. Its `studio_user_group` defaults to `dqx-studio-users`, which **must already exist**; override it with your approved scoped group if needed (or `users` for [broad mode](#audience-broad-mode-and-administrators)). It is wired into `DQX_USER_GROUPS` as a JSON audience list and used for the bundle's warehouse, app, dashboard, and schema audience grants; `studio_uc_principal` is the Unity Catalog grantee (the same group, or `account users` in broad mode). Storage names derive from `prefix`. The repo ships a canonical source target, **`dev`** (marked `default: true`). A minimal target looks like this:

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

The bundle **always** creates and manages a dedicated serverless warehouse (`resources.sql_warehouses.dqx_sql_warehouse`) — there is no "bring your own warehouse" mode. Its `permissions:` block grants `CAN_MANAGE` to the app SP and `CAN_USE` to `studio_user_group` and `admin_group` for end-user OBO queries. Tune it per target with `sql_warehouse_name` and `sql_warehouse_size`; `bundle destroy` deletes it. Marketplace binds an existing warehouse with `CAN_MANAGE` for the app SP, and setup adds the audience and administrator `CAN_USE` ACLs additively.

### Lakebase

Lakebase is a bundle-managed Postgres **project** (`resources.postgres_projects.dqx_studio`) plus the app SP's Postgres role (`resources.postgres_roles.app_sp`, a `DATABRICKS_SUPERUSER` member so the app can create its own schema). The project auto-creates its default branch (`lakebase_branch`, default `dqx`) and a `primary` read/write endpoint; the app connects via that endpoint path (`DQX_LAKEBASE_ENDPOINT`). The endpoint scales to zero after `lakebase_suspend_timeout` of inactivity — the app's connection pool pre-pings on checkout and transparently reconnects (waking the endpoint) on the next request.

> **The default warehouse and Lakebase sizes are deliberately small.** The bundle ships a `Small` SQL warehouse and a 0.5–1 CU autoscaling, scale-to-zero Lakebase project — sized for a typical rules catalog (low-thousands of rows) and light concurrent use, and chosen to keep idle cost near zero. They are a sensible **starting point, not a tuned production configuration.** Watch the app logs and the warehouse / Lakebase metrics under real load and raise `sql_warehouse_size`, `lakebase_max_cu`, or `DQX_LAKEBASE_POOL_MAX_SIZE` if you see query queueing or connection-pool exhaustion (see [Troubleshooting](#troubleshooting)).

### Variable reference

All target-level variables, their defaults, and what they control:

| Variable | Default | Required? | Purpose |
|---|---|---|---|
| `catalog_name` | `dqx` | **Yes** | Unity Catalog catalog where schemas and the wheels volume are created. **Must already exist** — the bundle does not create the catalog itself. |
| `dqx_service_principal_application_id` | `00000000-…` | **Yes** | Application ID of the service principal that runs the task-runner job. Created in [Step 1](#step-1-create-a-service-principal). The placeholder default fails validation. |
| `studio_user_group` | `dqx-studio-users` | Must exist | Audience group for warehouse, app, dashboard, and schema access; supplied to `DQX_USER_GROUPS` as a JSON list. Accepts `users` for DAB broad mode (workspace ACL `users`, UC `account users`); `account users` and `admins` are rejected. Set via `make app-deploy STUDIO_USER_GROUP=...`. |
| `admin_group` | `admins` (bundle and local Python) | Yes for prod | Workspace group whose members get the in-app `ADMIN` role unconditionally (bootstrap admin path), may run setup, and receives app/warehouse `CAN_USE`, dashboard `CAN_READ`, and (when it is a custom account group, not `admins`) the admin UC grants. Workspace `admins` members may always run setup. Override per target with a dedicated account group (e.g. `dqx-admins-prod`) so administrators also receive Unity Catalog access; with `admins`, administrators must also belong to the audience group to use OBO features. In every path, `AppConfig` defaults to the workspace `admins` group; `DQX_ADMIN_GROUP` overrides that default for local testing or deployment. Additional roles are assigned at runtime via the in-app Role Management UI. |
| `app_name` | `dqx-studio` | No | Deployed Databricks App name. Override per target (e.g. `dqx-studio-dev`, `dqx-studio-prod`) when deploying multiple targets to the same workspace, or for personal sandboxes. |
| `sql_warehouse_name` | `dqx-studio-sql-warehouse` | No | Name of the bundle-managed SQL warehouse. Override per target to avoid duplicates in shared workspaces. |
| `sql_warehouse_size` | `Small` | No | Cluster size of the bundle-managed warehouse (e.g. `2X-Small`, `Small`, `Medium`). |
| `schema_name` | `${var.prefix}` | No | Main schema — holds run history, profiling, metrics, and quarantine tables. Derived from `prefix`; declared as `resources.schemas.main_schema` in the bundle with `lifecycle.prevent_destroy: true`. |
| `tmp_schema_name` | `${var.prefix}_tmp` | No | Per-user temp-view schema. Derived from `prefix`; declared as `resources.schemas.tmp_schema` with `lifecycle.prevent_destroy: true`. |
| `genie_schema_name` | `${var.prefix}_genie` | No | Genie-facing derived views and dimensions. Derived from `prefix`; declared as `resources.schemas.genie_schema` with `lifecycle.prevent_destroy: true`. |
| `demo_schema_name` | `${var.prefix}_demo` | No | Demo source-table schema (`resources.schemas.demo_schema`). Derived from `prefix`. |
| `prefix` | `dqx_studio` | No | Storage prefix: main schema `<prefix>`, plus `<prefix>_tmp`, `<prefix>_genie`, `<prefix>_demo`; passed to the app as `DQX_PREFIX`. Lower-case letter first, then lower-case letters, digits, and underscores (max 64). Set via `make app-deploy STUDIO_PREFIX=...`. The wheels volume is always `<main schema>.wheels` (`resources.volumes.wheels`, `lifecycle.prevent_destroy: true`). The Lakebase schema does not follow the prefix. |
| `studio_uc_principal` | `${var.studio_user_group}` | No | UC grantee for the audience on the catalog-adjacent schemas. `make app-deploy STUDIO_USER_GROUP=users` sets it to `account users` (account-wide broad mode) unless `studio_uc_principal` or `studio_user_group` is passed explicitly in `BUNDLE_VARS`. |
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

The release target uses the compiled app source and task-runner wheel in `marketplace/`. The bundle still provisions and grants its own workspace resources. Grant the catalog prerequisites on the pre-existing catalog after deploy, then start the app:

```bash
databricks bundle run dqx-studio -p <your-profile> -t release --var catalog_name=<your-catalog> --var dqx_service_principal_application_id=<your-sp-application-id>
```

See [The `USE CATALOG` prerequisite](#the-use-catalog-prerequisite) for the grant statements.

Once the catalog grants are in place, the helper commands deploy and start the tagged release without running `app-build`. Run one from the repository root (`cd ..` if you followed the CLI example above). On macOS or Linux:

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

Remember the manual prerequisites: the [catalog grants](#the-use-catalog-prerequisite) for the app SP (`USE CATALOG`, `CREATE SCHEMA`), task-runner SP, and audience, plus [runner Lakebase role and grants](#task-runner-lakebase-access) for the current oversized-config path. Cold-start UC checks must pass before Studio is ready; they do not verify runner Lakebase access.

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

`bundle deploy` applies its declared schema and volume grants natively; setup applies the audience, administrator, and runner grants it has authority for and then verifies them. The statements below are the manual equivalents for recovery when the app SP lacks grant authority, or for Marketplace installs where setup prints them. They follow the [permission matrix](#permission-matrix). `<app-sp-id>` is the app's auto-created SP (`databricks apps get dqx-studio` → `service_principal_client_id`); `<job-sp-id>` is the task-runner SP from [Step 1](#step-1-create-a-service-principal). `<prefix>` is your storage prefix and `<uc-audience>` is the audience group (`account users` in DAB broad mode); repeat the audience statements for a custom admin account group.

```sql
-- App SP, DAB only (Marketplace: the app SP owns the schemas it creates).
-- ALL PRIVILEGES does not include MANAGE, so grant both.
GRANT ALL PRIVILEGES, MANAGE ON SCHEMA <catalog>.<prefix>        TO `<app-sp-id>`;
GRANT ALL PRIVILEGES, MANAGE ON SCHEMA <catalog>.<prefix>_tmp    TO `<app-sp-id>`;
GRANT ALL PRIVILEGES, MANAGE ON SCHEMA <catalog>.<prefix>_genie  TO `<app-sp-id>`;
GRANT ALL PRIVILEGES, MANAGE ON SCHEMA <catalog>.<prefix>_demo   TO `<app-sp-id>`;
GRANT ALL PRIVILEGES ON VOLUME <catalog>.<prefix>.wheels         TO `<app-sp-id>`;

-- Runner (least privilege): schema-wide storage access.
-- Use a dedicated Studio schema: SELECT/MODIFY cover its current and future tables.
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA <catalog>.<prefix> TO `<job-sp-id>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<prefix>_tmp             TO `<job-sp-id>`;
GRANT READ VOLUME ON VOLUME <catalog>.<prefix>.wheels         TO `<job-sp-id>`;

-- Audience: end users create dry-run / preview temp views (via their OBO token)
-- in the tmp schema, so they need USE SCHEMA + CREATE TABLE there.
GRANT USE SCHEMA, CREATE TABLE ON SCHEMA <catalog>.<prefix>_tmp TO `<uc-audience>`;
-- Genie SELECT is restricted to these five views and two metadata tables.
-- Never grant schema-wide SELECT or access to dq_user_table_entitlements.
GRANT USE SCHEMA ON SCHEMA <catalog>.<prefix>_genie TO `<uc-audience>`;
GRANT SELECT ON TABLE <catalog>.<prefix>_genie.mv_dq_scores             TO `<uc-audience>`;
GRANT SELECT ON TABLE <catalog>.<prefix>_genie.v_dq_check_results       TO `<uc-audience>`;
GRANT SELECT ON TABLE <catalog>.<prefix>_genie.v_dq_check_results_asof  TO `<uc-audience>`;
GRANT SELECT ON TABLE <catalog>.<prefix>_genie.v_dq_check_attribution   TO `<uc-audience>`;
GRANT SELECT ON TABLE <catalog>.<prefix>_genie.v_dq_failing_rows        TO `<uc-audience>`;
GRANT SELECT ON TABLE <catalog>.<prefix>_genie.dim_dq_rules             TO `<uc-audience>`;
GRANT SELECT ON TABLE <catalog>.<prefix>_genie.dim_dq_monitored_tables  TO `<uc-audience>`;
-- Demo content is meant to be explored.
GRANT USE SCHEMA, SELECT ON SCHEMA <catalog>.<prefix>_demo TO `<uc-audience>`;

-- DAB only: the deployer needs SELECT for the embed-credentials Insights dashboard
-- (the bundle uses ${workspace.current_user.userName}).
GRANT USE SCHEMA, SELECT ON SCHEMA <catalog>.<prefix> TO `<deployer>`;

-- Catalog access (see the USE CATALOG prerequisite).
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG <catalog> TO `<app-sp-id>`;
GRANT USE CATALOG ON CATALOG <catalog> TO `<job-sp-id>`;
GRANT USE CATALOG ON CATALOG <catalog> TO `<uc-audience>`;
```

Warehouse ACLs (app SP `CAN_MANAGE`; audience and admin group `CAN_USE`) are applied natively by the bundle, and by setup for Marketplace. DAB's authoritative grants: the bundle declares only the audience and app/runner grants; the **administrator-group** UC grants are intentionally not declared (the admin group may be the built-in `admins`, which can never receive UC grants) and are re-applied and verified by the app after each deploy restart.

### Temporary views and schedules

User-initiated views are created under OBO. Each view grants `SELECT` directly to the job's resolved runner and `MANAGE` to the app SP for orphan cleanup. Grant failures block submission. Cleanup rights are **per view**, not schema-wide, and no `account users` grant is restored:

```sql
-- Apply to an existing OBO-owned view only if these narrow grants are missing.
GRANT SELECT ON VIEW <catalog>.<tmp_schema>.<view_name> TO `<job-sp-id>`;
GRANT MANAGE ON VIEW <catalog>.<tmp_schema>.<view_name> TO `<app-sp-id>`;
```

Schedule creation/update inspects access using OBO SQL `SHOW GRANTS`, not the grants REST API. Source catalog `USE CATALOG`, source schema `USE SCHEMA`, and table `SELECT` are verified for the app scheduler and actual job runner per schedule. If missing, an authorized caller must establish those grants; failed runner grants block the schedule rather than allowing a later run to fail. These source grants are separate from the fixed startup output checks.

### Genie audience access

Setup reconciles the Genie space (when one is configured) by **additively** granting `CAN_RUN` to the audience and a custom admin group, then re-reading the ACL; a missing or unreadable ACL blocks readiness. Genie consumers need Consumer access or Databricks SQL access entitlement, parent catalog/schema usage, and the five approved view plus two dimension-table grants above. Genie embeds the author's compute credentials; consumers do not need direct warehouse access for Genie alone. Studio's OBO previews and SQL workflows additionally need SQL access entitlement and warehouse `CAN_USE`. In-app roles or object permissions alone do not provide this external access. Never grant whole-schema Genie `SELECT` or audience access to `dq_user_table_entitlements`.

Both DAB and Marketplace use the `genie` user-authorization scope; sign in again to renew consent when the scope changes. Replacing the bound warehouse re-runs the warehouse checks and reconciles the Genie space's compute; verify the OBO audience's SQL access on the replacement warehouse.

To share the app in Marketplace (or recover an ACL), go to **Compute > Apps > `<app-name>` > Permissions** and assign `Can Use` to the audience and administrator groups; see [App sharing](#app-sharing). DAB applies this via `studio_user_group` and `admin_group`. Replace `<app-name>` with the configured app name (default `dqx-studio`).

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

**App says a Studio schema does not exist on first start (`storage_missing`):**
The schemas didn't deploy, or the bundle is pointing at a different catalog than the app. Confirm with `databricks bundle validate -p <profile> -t <target>` that `catalog_name` and `prefix` resolve to the expected values, then redeploy:
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
A schema, volume, or Lakebase project of the same name was created out-of-band. Either rename it via the corresponding variable (`prefix`, `lakebase_project_id`) or `databricks bundle deployment bind <key> <existing-id> -t <target>` to adopt the existing resource, then redeploy.

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
