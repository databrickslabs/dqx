# Deployment (Declarative Automation Bundles)

Production deployment uses [Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/) (DABs, formerly known as Databricks Asset Bundles) via the Databricks CLI (`databricks bundle deploy`). For local development, see [DEVELOPMENT.md](DEVELOPMENT.md).

## Prerequisites

Before starting, make sure you have all of the following — several steps require elevated permissions, so confirm access before you begin.

**Tooling**
- **Databricks CLI** installed and authenticated against your workspace
- **`jq`** (used by the post-deploy grants script)
- **`make`** (used by the one-command deploy target)

**Access**
- **Workspace admin** — required to create service principals, grant catalog permissions, and enable workspace-level features

**Workspace configuration**
- An **existing Unity Catalog catalog** where the app's schemas and volumes will be created — the bundle does not create the catalog itself
- **Databricks Apps** feature enabled on the workspace
- **User token passthrough** enabled for Databricks Apps (see [Step 2](#step-2-enable-user-token-passthrough))
- **Serverless compute** enabled on the workspace (the task-runner job runs on serverless)

## Step 1: Create a Service Principal

The bundle requires a service principal to run the task-runner job. This is separate from the app's auto-created SP — Jobs require a workspace-level SP as the `run_as` identity because the app-scoped SP cannot be used outside the Apps framework.

**Create a new SP:**
1. Go to **Settings → Identity and Access → Service Principals**
2. Click **Add service principal → Create new**
3. Give it a name (e.g., `dqx-task-runner-sp`)
4. Note the **Application ID** — you'll use it in Step 3 as `dqx_service_principal_application_id`
5. **Grant yourself (or the identity you'll deploy the bundle with) the `User` role on this new SP.** Open the SP you just created, go to the **Permissions** tab, click **Add permissions**, search for your user (or deploy-time principal), and assign the role **`User`** (equivalent to `servicePrincipal.user` in the SCIM API).

   This lets your deploying identity configure jobs with `run_as: service_principal_name` pointing at this SP. Without it, `databricks bundle deploy` will fail with a permission error when it tries to set up the task-runner job.

**Find an existing SP's Application ID:**
```bash
databricks service-principals list -p <your-profile>
```

## Step 2: Enable User Token Passthrough

The app uses On-Behalf-Of (OBO) tokens to access Unity Catalog resources with the end user's identity. This requires the **Databricks Apps user token passthrough** feature to be enabled on your workspace.

Contact your workspace admin or enable it via the workspace settings if not already active.

## Step 3: Configure `databricks.yml`

Update a deploy target. The minimum required is a `catalog_name` and `dqx_service_principal_application_id`; everything else has a sensible default and can be overridden per target. In `app/databricks.yml`:

```yaml
targets:
  dev:
    workspace:
      profile: <your-profile>
    variables:
      # Required
      catalog_name: <your-catalog>
      dqx_service_principal_application_id: <your-sp-application-id>

      # Optional — uncomment and override defaults per target as needed
      # admin_group: <your-admin-group>
      # app_name: <your-app-name>
      # sql_warehouse_name: <your-sql-warehouse-name>
      # schema_name: <your-schema-name>
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
| `schema_name` | `dqx_app` | No | Main schema inside the catalog — holds rules, run history, role mappings, and other app state. Override only if you need a non-default schema layout. |

> **Note on duplicate names in Databricks:** SQL warehouses, jobs, and apps within the same workspace are tracked by ID, not by name, so technically duplicates are allowed. But operators browse the Jobs / Apps / Warehouses UI by name, so distinct names per target are strongly recommended when you deploy more than one target to the same workspace.

## Step 4: One-Command Deploy (recommended)

Build, deploy, grant permissions, and start the app in a single command:

```bash
make app-deploy PROFILE=<your-profile> TARGET=<your-target>
```

This runs the following steps automatically:
1. `make app-build` — builds the frontend and wheels
2. `databricks bundle deploy` — creates the warehouse, schemas, volume, job, and app
3. `app/scripts/post_deploy_grants.sh` — discovers both SPs and executes all `GRANT` statements
4. `databricks bundle run` — starts the app

> **First start**: The app runs database migrations and uploads DQX wheels to the UC volume. If the task-runner job runs before the app has started at least once, it will fail to find its wheels. Wait for `"Uploaded databricks_labs_dqx-<version>..."` in the logs before triggering runs.

### Step-by-step alternative

If you prefer to run each step individually:

```bash
# Build
make app-build

# Deploy the bundle
cd app && databricks bundle deploy -p <your-profile> -t <your-target>

# Grant permissions (auto-discovers SP IDs and catalog from bundle config)
make app-grant-permissions PROFILE=<your-profile>

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
GRANT ALL PRIVILEGES ON SCHEMA <catalog>.dqx_app TO `<app-sp-id>`;
GRANT ALL PRIVILEGES ON SCHEMA <catalog>.dqx_app_tmp TO `<app-sp-id>`;
GRANT ALL PRIVILEGES ON VOLUME <catalog>.dqx_app.wheels TO `<app-sp-id>`;

-- Job service principal (task runner)
GRANT USE CATALOG ON CATALOG <catalog> TO `<job-sp-id>`;
GRANT ALL PRIVILEGES ON SCHEMA <catalog>.dqx_app TO `<job-sp-id>`;
GRANT ALL PRIVILEGES ON SCHEMA <catalog>.dqx_app_tmp TO `<job-sp-id>`;
GRANT ALL PRIVILEGES ON VOLUME <catalog>.dqx_app.wheels TO `<job-sp-id>`;

-- End users need USE CATALOG to create temporary views for dry runs
GRANT USE CATALOG ON CATALOG <catalog> TO `account users`;
```

To grant app access to end users, go to **Apps → `<app-name>` → Permissions** and assign `Can Use`. Replace `<app-name>` with the value of `app_name` configured for your target (default `dqx-studio`).

Access the app at:
```
https://<your-workspace-url>/apps/<app-name>
```

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
