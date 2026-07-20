# Redeploy runbook — after workspace loss (2026-07-19)

The `fe-sandbox-dq-demo` workspace was destroyed by scheduled maintenance. Its
compute, SQL warehouse, Lakebase project (`dqx-studio-db-v2`), and the deployed
app are gone. The **`dqx` Unity Catalog catalog survives** (it lives on the
metastore, not the workspace). OLTP state (rules, settings, RBAC, comments,
schedules) lived in the destroyed Lakebase project and is **gone** — the new
deploy starts with empty OLTP tables (fresh Lakebase, per decision).

**Branch:** `dqx/rules-threshold-rework` (builds clean; wheels verified offline
at commit `ea3732e1`). **Decisions:** you auth + give the host; fresh Lakebase.

---

## Blocker (why this isn't already done)
Remote deploy needs a reachable workspace host + valid auth. The new VM host was
given as a placeholder (`XXX`) and no profile authenticates to it yet. Local dev
also can't run: the app's startup lifespan requires SP auth + DB migrations
against a live workspace (by design — it refuses to start otherwise), so there
is no offline/local-only mode. Everything below is ready to go the moment you
provide host + auth.

---

## Step 1 — Authenticate to the new workspace (YOU)
In this session, prefix with `!` so the output lands here, or run in your own shell:

```
databricks auth login --host https://<NEW-HOST>.cloud.databricks.com --profile <NEW-PROFILE>
```

Then tell me the **host** and the **profile name**. (If you re-create it under the
old name `fe-sandbox-dq-demo`, the existing `app/target.dev.yml` already points
at it — just re-auth that profile and skip Step 2.)

Verify:
```
databricks auth profiles | grep <NEW-PROFILE>       # Valid = YES
databricks catalogs get dqx -p <NEW-PROFILE>        # confirm the dqx catalog is reachable
```

## Step 2 — Point the deploy target at the new workspace (ME, once you give host+profile)
Edit `app/target.dev.yml` (untracked):
- `workspace.profile: <NEW-PROFILE>`
- keep `catalog_name: dqx` (survived), `admin_group: admins`
- `dqx_service_principal_application_id:` — the task-runner SP. **If the old SP
  (`84dc3b65-1ad3-4298-abd5-07af01be0d70`) was workspace-scoped and died with the
  workspace, create a new one** (DEPLOYMENT.md Step 1) and put its app id here.
  Grant yourself the `User` role on it.
- **Fresh Lakebase:** leave the `postgres_projects`/`postgres_roles` blocks in
  `databricks.yml` as-is; the bundle provisions a new project on first deploy.
  `lakebase_project_id` can stay `dqx-studio-db-v2` (created fresh on the new
  workspace) or change to a clean name — either works since nothing exists there yet.

## Step 3 — Prerequisites on the new workspace (YOU / an admin)
From DEPLOYMENT.md — the deploy halts on the first missing one:
- Workspace access, Databricks SQL access, Allow cluster create entitlements
- Databricks Apps: Can Manage
- Databricks Database (Lakebase): Manager entitlement
- USE CATALOG + CREATE SCHEMA on `dqx`; MANAGE on `dqx` (or catalog owner)
- Service Principal: User role on the task-runner SP
- Workspace features: Databricks Apps, user token passthrough (OBO), serverless
  compute, Lakebase Postgres — all enabled

## Step 4 — Deploy (ME)
```
make app-deploy PROFILE=<NEW-PROFILE> TARGET=dev
```
This runs `make app-build` → `databricks bundle deploy` (schemas, wheels volume,
fresh Lakebase project+endpoint+role, SQL warehouse, task-runner job, app; all UC
grants applied natively) → `databricks bundle run dqx-studio`.

## Step 5 — One manual grant the bundle can't do (YOU, once)
The bundle can't grant catalog-level access on a catalog it doesn't manage. After
the first deploy, get the app SP id from `databricks apps get dqx-studio -p <NEW-PROFILE>`
(`service_principal_client_id`) and run:
```sql
GRANT USE CATALOG ON CATALOG dqx TO `account users`;
GRANT USE CATALOG ON CATALOG dqx TO `<app-sp-client-id>`;
GRANT USE CATALOG ON CATALOG dqx TO `<task-runner-sp-application-id>`;
```

## Step 6 — Verify first start (ME)
```
databricks apps logs dqx-studio -p <NEW-PROFILE>
```
Wait for: `"Uploaded databricks_labs_dqx-<version>..."` and (Lakebase on)
`"Lakebase OLTP routing enabled"`. Then open
`https://<NEW-HOST>/apps/dqx-studio`. If it restart-loops on
`"Lakebase initialisation failed ... Refusing to start"`, see DEPLOYMENT.md
Troubleshooting (usually the endpoint is still STARTING, or the app SP's
Postgres role/`DATABRICKS_SUPERUSER` membership didn't provision — needs CLI ≥ 1.4.0; we have 1.6.0).

## Optional — local dev against the new workspace (ME)
Update `app/.env`: `DATABRICKS_CONFIG_PROFILE=<NEW-PROFILE>`,
`DATABRICKS_WAREHOUSE_ID=<new-warehouse-id>` (from `databricks warehouses list`
post-deploy or the bundle output), keep `DQX_CATALOG=dqx`. Then
`make app-start-dev`. (Local dev still needs the live workspace — it 500s without one.)

---

## Deploy gotchas carried from prior sessions
- Always `git checkout app/uv.lock uv.lock` before staging — test/build churn them.
- NULL `schedule_kind` rows in `_dev` tables have blocked deploy before → backfill `'dq_only'` if hit (moot on a fresh Lakebase).
- `DQX_ADMIN_GROUP=admins` needed in gitignored `app/.env` for local admin bootstrap.
- Toolchain confirmed present: node 24, yarn 1.22, bun 1.3, uv 0.11, Databricks CLI 1.6.0.
