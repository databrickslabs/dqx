# Deployment (Databricks Asset Bundles)

Production deployment uses the Databricks CLI (`databricks bundle deploy`). The `apx` tool is for local development only — see [DEVELOPMENT.md](DEVELOPMENT.md).

## Prerequisites

- **Databricks CLI** installed and authenticated
- A **Databricks service principal** (see Step 1)
- The `app/` directory built at least once (`make app-build` from the project root)

## Step 1: Create a Service Principal

The bundle requires a service principal to run the task-runner job. The SP executes profiler and dry-run tasks on serverless compute under its own identity.

**Create a new SP:**
1. Go to **Settings → Identity and Access → Service Principals**
2. Click **Add service principal → Create new**
3. Give it a name (e.g. `dqx-app-runner`)
4. Note the **Application ID**

**Find an existing SP's Application ID:**
```bash
databricks service-principals list -p <your-profile>
```

**Permissions granted automatically by DABs** — no manual configuration needed:

| Resource | Permission | Why |
|---|---|---|
| `<catalog>.dqx_app` schema | `ALL_PRIVILEGES` | Read/write profiling results, rules, settings |
| `<catalog>.dqx_app_tmp` schema | `ALL_PRIVILEGES` | Create temporary views for job isolation |
| `<catalog>.dqx_app.wheels` volume | `ALL_PRIVILEGES` | Store wheels for the task-runner job |
| Task runner job | `run_as` identity | Job tasks execute as this SP |

## Step 2: Build

Before deploying, build the project to produce the wheel and compiled frontend assets:

```bash
make app-build   # from the project root
```

This compiles the React/TypeScript UI, generates the OpenAPI schema, and packages everything into a wheel. The bundle deploy step will also trigger a build, but running it manually first is useful to catch errors early.

## Step 3: Deploy the Bundle

From the `app/` directory:
```bash
databricks bundle deploy -p <your-profile> \
  --var="dqx_service_principal_application_id=<your-sp-application-id>"
```

To avoid passing the variable every time, pin it in `databricks.yml`:
```yaml
targets:
  dev:
    variables:
      dqx_service_principal_application_id: "<your-sp-application-id>"
```

This single command:
- Builds the project (`make app-build`) — compiles frontend, generates OpenAPI schema, packages wheels
- Uploads `.build/` to the workspace
- Provisions all resources: App, SQL Warehouse, schemas, UC volume, task-runner job
- Wires environment variables: `DQX_JOB_ID`, `DQX_WHEELS_VOLUME`, warehouse ID

## Step 4: Start the App

```bash
databricks apps start databricks-labs-dqx-app -p <your-profile>
```

Or via the UI: **Apps → databricks-labs-dqx-app → Start**.

> **Wheel upload on first start**: The app uploads DQX wheels to the UC volume on startup. If the task-runner job runs before the app has started at least once after a fresh deploy, it will fail to find its wheels — start the app first and wait for `"Uploaded databricks_labs_dqx-<version>..."` in the logs.

## Step 5: Grant User Permissions

Grant users access via the Databricks UI or CLI by assigning the `Can Use` permission to users or groups.

## Step 6: Access the App

```
https://<your-workspace-url>/apps/databricks-labs-dqx-app
```

- **UI**: Apps → databricks-labs-dqx-app → copy URL
- **CLI**: `databricks apps get databricks-labs-dqx-app -p <your-profile>` → `url` field

## Redeploying After Code Changes

```bash
make app-build   # from project root — rebuild wheel and frontend assets
cd app && databricks bundle deploy -p <your-profile>
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
# Clean local bundle state
rm -rf .databricks

# Or force deploy
databricks bundle deploy -p <your-profile> --force
```

**Profiler or dry-run not starting:**
1. Check `DQX_JOB_ID` is set (visible in the app's environment config in the UI)
2. Confirm the job exists: `databricks jobs list -p <your-profile>`
3. Confirm the SP has `CAN_MANAGE` on the job (set automatically by DABs)

**Job fails with "file not found" on wheel:**
The task-runner job installs wheels from the UC volume. If the volume is empty the job fails.

Fix: start the app and wait for it to finish the wheel upload, then retry the job.
```bash
databricks apps logs databricks-labs-dqx-app -p <your-profile>
# Look for: "Uploaded databricks_labs_dqx-<version>-py3-none-any.whl"
```
