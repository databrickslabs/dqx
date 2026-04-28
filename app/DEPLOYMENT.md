# Deployment (Databricks Asset Bundles)

Production deployment uses the Databricks CLI (`databricks bundle deploy`). For local development, see [DEVELOPMENT.md](DEVELOPMENT.md).

## Prerequisites

- **Databricks CLI** installed and authenticated
- A **Databricks service principal** (see Step 1)

## Step 1: Create a Service Principal

The bundle requires a service principal to run the task-runner job. The SP executes profiler and dry-run tasks on serverless compute under its own identity.

**Create a new SP:**
1. Go to **Settings → Identity and Access → Service Principals**
2. Click **Add service principal → Create new**
3. Give it a name 
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

```bash
make app-build   # from the project root
```

See [DEVELOPMENT.md](DEVELOPMENT.md#3-build) for details on what the build produces.

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

## Step 4: Start the App

```bash
databricks apps start databricks-labs-dqx-app -p <your-profile>
```

Or via the UI: **Apps → databricks-labs-dqx-app → Start**.

> **First start**: The app uploads DQX wheels to the UC volume on startup. If the task-runner job runs before the app has started at least once after a fresh deploy, it will fail to find its wheels — start the app first and wait for `"Uploaded databricks_labs_dqx-<version>..."` in the logs.

## Step 5: Grant Permissions

The DABs deploy creates schemas and volumes but does not grant catalog-level access. Run the following SQL to grant the service principal and app principal the required permissions (replace the catalog name and principal ID with your own):

```sql
-- Grant to the service principal and/or app principal
GRANT USE CATALOG ON CATALOG <catalog> TO `<principal-id>`;
GRANT ALL PRIVILEGES ON SCHEMA <catalog>.dqx_app TO `<principal-id>`;
GRANT ALL PRIVILEGES ON SCHEMA <catalog>.dqx_app_tmp TO `<principal-id>`;
GRANT ALL PRIVILEGES ON VOLUME <catalog>.dqx_app.wheels TO `<principal-id>`;
```

You can find the principal IDs in the Databricks UI under **Settings → Identity and Access → Service Principals**, or via:
```bash
databricks service-principals list -p <your-profile>
```

To grant app access to end users, go to **Apps → databricks-labs-dqx-app → Permissions** and assign `Can Use`.

Access the app at:
```
https://<your-workspace-url>/apps/databricks-labs-dqx-app
```

You can also find the URL via **Apps → databricks-labs-dqx-app** or:
```bash
databricks apps get databricks-labs-dqx-app -p <your-profile>   # url field
```

## Redeploying After Code Changes

```bash
make app-build   # from project root
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
