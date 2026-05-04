# Deployment (Databricks Asset Bundles)

Production deployment uses the Databricks CLI (`databricks bundle deploy`). For local development, see [DEVELOPMENT.md](DEVELOPMENT.md).

## Prerequisites

- **Databricks CLI** installed and authenticated
- **Databricks Apps** with user token passthrough enabled (see Step 2)

## Step 1: Create a Service Principal

The bundle requires a service principal to run the task-runner job. This is separate from the app's auto-created SP — Jobs require a workspace-level SP as the `run_as` identity because the app-scoped SP cannot be used outside the Apps framework.

**Create a new SP:**
1. Go to **Settings → Identity and Access → Service Principals**
2. Click **Add service principal → Create new**
3. Give it a name (e.g., `dqx-task-runner-sp`)
4. Note the **Application ID**
5. Assign the **`servicePrincipal.user`** role to the SP — this is required before it can be used as a `run_as` identity on jobs

**Find an existing SP's Application ID:**
```bash
databricks service-principals list -p <your-profile>
```

## Step 2: Enable User Token Passthrough

The app uses On-Behalf-Of (OBO) tokens to access Unity Catalog resources with the end user's identity. This requires the **Databricks Apps user token passthrough** feature to be enabled on your workspace.

Contact your workspace admin or enable it via the workspace settings if not already active.

## Step 3: Configure `databricks.yml`

Add a deploy target with your catalog and service principal. In `app/databricks.yml`:

```yaml
targets:
  dev:
    workspace:
      profile: <your-profile>
    variables:
      catalog_name: <your-catalog>
      dqx_service_principal_application_id: "<your-sp-application-id>"
    presets:
      trigger_pause_status: PAUSED
```

The `catalog_name` must reference an **existing** Unity Catalog catalog — the bundle creates schemas and volumes inside it but does not create the catalog itself.

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
cd app && databricks bundle run databricks-labs-dqx-app -p <your-profile> -t <your-target>
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

To grant app access to end users, go to **Apps → databricks-labs-dqx-app → Permissions** and assign `Can Use`.

Access the app at:
```
https://<your-workspace-url>/apps/databricks-labs-dqx-app
```

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

```bash
databricks apps get databricks-labs-dqx-app -p <your-profile>    # status
databricks apps logs databricks-labs-dqx-app -p <your-profile>   # logs
databricks apps stop databricks-labs-dqx-app -p <your-profile>   # stop
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
databricks apps logs databricks-labs-dqx-app -p <your-profile>
# Look for: "Uploaded databricks_labs_dqx-<version>-py3-none-any.whl"
```
