# DQX Studio — Customer Q&A

A reference for the customer Q&A session. Questions are grouped by theme; answers are written to be **honest and specific** (with limitations called out) rather than marketing prose. Skip what you don't need.

> "DQX" = the open-source [`databrickslabs/dqx`](https://github.com/databrickslabs/dqx) data-quality engine. "DQX Studio" = this app — a UI + control plane on top of DQX, deployed as a Databricks App in the customer's own workspace.

---

## 1. Product positioning

### 1.1 What is DQX Studio in one sentence?
A workspace-native UI for authoring, approving, scheduling, and reviewing DQX data-quality rules — running entirely inside the customer's Databricks account, using the customer's own compute and storage.

### 1.2 How is it different from running DQX in a notebook?
DQX (the library) gives you the engine. DQX Studio gives you the workflow around it:

- **Rule authoring UI** (form-based + YAML) instead of editing notebooks
- **Approval workflow** with `RULE_AUTHOR` / `RULE_APPROVER` separation
- **Schedules** that submit runs to a serverless job — no notebook orchestration to maintain
- **Run history** with status, sample bad rows, metrics, comments, and review status per run
- **Insights dashboard** — a Lakeview/AI-BI dashboard embedded as an iframe, customisable per workspace
- **Hybrid storage** (Lakebase OLTP for rules/settings, Delta for run history) so the UI feels like a real app, not a Spark query under every click

### 1.3 Is this a Databricks product or open source?
It's an open-source companion to the `databrickslabs/dqx` library — both live under `databrickslabs/` (Databricks Labs is the field-maintained ecosystem, not the core product). You get the source, you deploy it into your own workspace via a Declarative Asset Bundle, and you own the operating model.

### 1.4 What does it *not* do?
Stated up front so there are no surprises:

- **No cross-workspace rules.** DQX Studio runs inside one workspace and authors rules for tables visible in that workspace's Unity Catalog metastore. If you have multiple metastores (regions, business units), you deploy one instance per metastore.
- **No vendor SaaS plane.** All metadata stays in your account. That's a feature (sovereignty) and a constraint (no cross-customer benchmarks, no external data-quality marketplace).
- **No anomaly detection / ML quality models.** Today it executes rules you (or the profiler) author. Anomaly-style "this column drifted" checks would be net-new work.
- **Not a catalog or lineage tool.** It reads from Unity Catalog and writes results to it; it does not replace Atlan, Collibra, or Purview.

---

## 2. Architecture

### 2.1 What does deployment look like?
One Databricks Asset Bundle (`databricks.yml`) provisions everything in one `make app-deploy`:

- **Databricks App** (FastAPI + React, single process, served by the Apps runtime)
- **Serverless Job** for Spark work — the *task runner* — invoked for profiler, dry-run, and scheduled runs
- **Lakebase Postgres instance** (`database_instances.lakebase`) for OLTP state
- **Two UC schemas** (`dqx_studio`, `dqx_studio_tmp`) under a customer-supplied catalog
- **UC volume** (`wheels`) the app uses to ship DQX wheels into the job
- **SQL warehouse** — managed by the bundle, or BYO (bring-your-own) if you already have one
- **Lakeview dashboard** (`dashboards.dqx_quality_overview`) pinned to the app's *Insights* page

All stateful resources carry `lifecycle.prevent_destroy: true`, so a stray `bundle destroy` cannot drop them and wipe state.

### 2.2 Why a separate job for the Spark work?
Databricks Apps run in a container without Spark. We use the FastAPI process for the UI, REST API, and short OLTP queries — anything that needs Spark (profiling, dry runs, scheduled validation) is submitted to a serverless job we provision and pin to a wheels volume. The app polls job status and reads results back from Delta.

That gives you:

- **Predictable autoscaling** — serverless handles the spiky workload
- **Tight cost control** — the app process stays small; nothing Spark-shaped is running when no one is validating
- **Identity isolation** — Spark work runs as the task-runner SP with the user's OBO token threaded through, so UC permissions still apply

### 2.3 Why a hybrid storage model (Lakebase + Delta)?
Data quality has two very different workloads, and we picked the right tool for each:

| Workload | Backend | Why |
|---|---|---|
| Rules CRUD, app settings, RBAC, comments, schedules | **Lakebase Postgres** | Sub-millisecond reads, real transactions, indexes on small tables. Delta's not made for "give me one row by primary key, fast." |
| Profiling results, validation runs, metrics, quarantine rows | **Delta on UC** | High-volume append, time-series scans, dashboard JOINs, AI/BI integration. Postgres would melt under quarantine volumes. |

The split is invisible to service code — `SqlExecutor` (Delta) and `PgExecutor` (Lakebase) share the same surface. **If a workspace can't or won't enable Lakebase, the OLTP tables fall back to Delta** via the `oltp_fallback` migration. The app still works; latency on rule CRUD is just higher.

### 2.4 How does authentication work?
Two identities, each with a deliberate scope:

- **User identity (OBO)** — every request from the browser carries an `X-Forwarded-Access-Token`. UC browsing (catalogs/schemas/tables), temporary view creation for profiler/dry-runs, and user-initiated job submissions all run as the user. This is how we enforce "you can only validate tables you can already read."
- **App / task-runner SP** — owns the app's own state (rules catalog, settings, OLTP migrations, wheel uploads) and runs scheduled jobs that have no user attached. Critically, the app SP is **only granted what it needs inside the DQX schemas** — it never gets blanket UC access.

There are no admin-scoped REST calls made by the app itself.

### 2.5 What's the data residency story?
Everything lives in the customer's Databricks account:

- Source tables stay where they are — DQX Studio never moves them.
- Quarantine rows (potentially PII) land in Delta tables inside the customer's catalog.
- OLTP state (rules, comments, schedules) lives in the customer's Lakebase instance.
- Wheels and app code live in the customer's UC volume + workspace files.

No data, metadata, or telemetry leaves the account. There is no DQX Studio SaaS plane to phone home to.

---

## 3. Rule authoring & execution

### 3.1 What kinds of checks does DQX support?
Three families, in increasing power:

1. **Column-level row checks** — `is_not_null`, `is_in_range`, `regex_match`, `is_unique`, etc. Configured per column.
2. **Row-level dataset checks** — composite expressions on multiple columns of the same row.
3. **Cross-table SQL checks** — full SQL queries (referential integrity, aggregate reconciliation, business-logic joins). Whatever you can write as a `SELECT` returning the offending rows, DQX can run.

All three are first-class in the engine, the UI, and the metrics pipeline.

### 3.2 How do users author rules?
Two surfaces, same backing store:

- **Form mode** — pick a table, pick columns, pick a check function, set parameters, optional labels. Good for the long tail.
- **YAML mode** — paste/edit DQX YAML directly. Good for power users and copy-paste from existing notebooks.

Rules are versioned in `dq_quality_rules` + `dq_quality_rules_history` (audit log). They flow through statuses (`draft → proposed → approved`) gated by RBAC.

### 3.3 What's the profiler?
A one-click way to bootstrap rules for a table. You point at a table; the profiler runs a serverless job that samples the data, infers candidates (null thresholds, value ranges, uniqueness), and writes them to `dq_profiling_results`. You then pick the ones you want and promote them to draft rules. It's not magic — it's a starting point.

### 3.4 What's a "dry run"?
Validate a rule (or rule set) against the live table **before** approving or scheduling it. It returns:

- Pass/fail counts (valid rows, error rows, warning rows, total rows)
- A 10-row sample of failing data inline
- A full quarantine table written to `dq_quarantine_records` (capped at 100k rows for SQL checks to bound storage)
- Per-check metrics + custom metrics into `dq_metrics`

Dry runs use OBO, so the user can only validate tables they have `SELECT` on.

### 3.5 How do schedules work?
Configure a per-rule-set schedule (cron or interval) in the UI. An in-process asyncio loop inside the FastAPI worker ticks every minute, picks up due schedules, and submits the same task-runner job that dry runs use — `task_type` discriminates. Scheduled runs use the **app SP**, not OBO, because there's no user at 3 AM. The schedule's *target rules* honour the rule-set the author selected.

A `/tmp/.dqx_scheduler.lock` exclusive file lock guarantees the loop runs in exactly one app worker even if Databricks Apps autoscales to multiple replicas.

### 3.6 What's "review status"?
A per-run label set by reviewers (the business owner, the on-call SA, whoever) after a run completes. Values are admin-configurable in **Configuration → Run review statuses**; the default catalogue ships with *Pending review*, *Acknowledged*, *Resolved*, *False positive*. The Runs History page filters on it as multi-select chips, so a steward can ask "what's still pending review?" in one click. Every change is audited in `dq_run_review_status_history`.

It sits next to free-text comments on the run detail row — the dropdown is the structured action, the comment is the explanation.

---

## 4. Permissions & governance

### 4.1 What roles exist inside the app?
Four primary roles plus one orthogonal:

- **`ADMIN`** — configure the app (review-status catalogue, custom metrics, labels, role mappings, retention, embedded dashboard, role↔group mapping)
- **`RULE_APPROVER`** — move rules from `proposed` to `approved`
- **`RULE_AUTHOR`** — create/edit drafts and propose rules
- **`VIEWER`** — read runs, dashboards, results
- **`RUNNER`** (orthogonal) — can execute approved rules on-demand

Roles are resolved from **Databricks workspace-group membership** via the `dq_role_mappings` table. Customers map their existing IAM groups onto DQX roles — we never invent a new identity system.

### 4.2 Who can do what on a table they don't own?
- **Browse a table in the UI**: the user needs `USE CATALOG` + `USE SCHEMA` + `SELECT` on the table (standard UC). The UI uses OBO, so the catalog tree only shows what the user can already see.
- **Author a rule against a table**: same — OBO read access is required to author and to dry-run.
- **Approve a rule**: the `RULE_APPROVER` role inside DQX Studio, regardless of table ownership. This is governance-by-process; the data owner does *not* need to use the app.
- **See a run's quarantine rows**: catalog-filtered server-side. The runs list and the quarantine table both gate on the user's UC-accessible catalogs.

### 4.3 What permissions does the app's service principal need?
Scoped tight:

- `USE CATALOG` + `USE SCHEMA` + `ALL PRIVILEGES` on the two DQX schemas (`dqx_studio`, `dqx_studio_tmp`) only
- `READ VOLUME` + `WRITE VOLUME` on the wheels volume only
- `CAN USE` on the SQL warehouse the app is bound to
- `Service Principal: User` role on the task-runner SP (so the app can submit jobs as it)

It is **not** a workspace admin and **not** a metastore admin. If you remove the app, those grants are the only blast radius.

### 4.4 What's the deployer's permission burden?
Documented as a table in `DEPLOYMENT.md` — about 10 line items, the bulk of which collapse if the deployer is added to a UC-admin group. The single most common failure on first deploy is missing `MANAGE` on the target catalog (the `post_deploy_grants.sh` script needs it to GRANT to the app SP). We surface that error explicitly with a fix.

### 4.5 Is everything audited?
Yes, but the auditing is **distributed** across the platform rather than in one DQX log:

- **Rule edits** → `dq_quality_rules_history`
- **Schedule edits** → `dq_schedule_configs_history`
- **Review-status changes** → `dq_run_review_status_history`
- **Job submissions** → Databricks Jobs UI / `system.lakeflow.jobs`
- **Table reads (OBO)** → UC audit logs (the standard ones)
- **App requests** → Databricks Apps logs

We didn't invent a parallel audit store because UC + Jobs already audit everything we'd put in one.

---

## 5. Operations

### 5.1 What's the upgrade story?
`make app-deploy` is idempotent. Bundle changes go through `databricks bundle deploy`; schema changes go through versioned, idempotent migration runners (`MigrationRunner` for Delta, `PgMigrationRunner` for Lakebase). On every app start the wheel hash gates whether a re-upload + job-environment patch is needed.

You re-run the same command. If you forgot to update a variable, the bundle complains; it doesn't silently mutate state.

### 5.2 What happens if `bundle destroy` runs accidentally?
The three stateful resource keys — `schemas.main_schema`, `volumes.wheels`, `database_instances.lakebase` — carry `lifecycle.prevent_destroy: true`. The CLI refuses to drop them. To intentionally tear those down you have to edit `databricks.yml`, `unbind` the resource, and destroy it manually — a three-step opt-in.

### 5.3 What about backup?
Three layers, all leveraging native Databricks:

- **Delta tables** (runs, metrics, quarantine): UC's time travel + Delta retention. Standard backup story.
- **Lakebase Postgres**: managed by the Lakebase service (snapshots, PITR depending on the tier). The instance is the unit of backup.
- **Bundle source of truth**: `databricks.yml` lives in your VCS. The whole deployment is reproducible from source + a Postgres restore.

### 5.4 What's the cost footprint?
Three line items:

- **App compute** — Databricks Apps charges a flat hourly rate per running app. The FastAPI process is small (one or two replicas).
- **Lakebase instance** — runs continuously; size is small (the rules catalog is in the low-thousands of rows for most customers).
- **Task-runner job (serverless)** — only billed when validations run. Cost scales with row count and check complexity; profiler/dryrun is bounded, scheduled is what you make it.

The app does **not** keep a SQL warehouse warm. The Lakeview dashboard auto-stops between uses.

### 5.5 How do we monitor it?
- App-level logs in Databricks Apps UI
- Job-level logs in Jobs UI (each task-runner submission shows up as a run)
- UC audit logs for table access
- `dq_validation_runs` is itself a run history table — `SELECT … WHERE status = 'FAILED'` is your alert query if you want one
- The Insights dashboard ships KPIs for run health, error trends, top-failing tables

---

## 6. Comparisons (FAQ-style)

### 6.1 How does DQX Studio compare to Soda / Great Expectations?
Both are rule-driven DQ frameworks; Soda is the closer comparison (commercial product with a UI; Great Expectations is library-first). For Soda specifically, see [Section 11 — Soda deep dive](#11-soda-comparison--deep-dive) which is the answer this customer is actually here for.

The short version: Soda is broader (multi-source, anomaly detection, alerts/incidents, SaaS UI); DQX Studio is **deeper inside Databricks** (PySpark-native, UC-aware OBO, Delta-resident quarantine, single workspace plane). Customers serving multiple warehouses often pick Soda; customers all-in on Databricks tend to land on DQX.

### 6.2 How does it compare to Monte Carlo / Anomalo (SaaS observability)?
Those tools shine at automatic anomaly detection across many sources by indexing metadata in a vendor cloud. DQX Studio is the opposite end of the spectrum: rule-driven, single-source-of-truth-is-UC, sovereign. Customers tend to use both — DQX for *explicit* rules they care about, MC/Anomalo for *implicit* drift detection across the catalog. We do not pretend to replace anomaly detection today.

### 6.3 How does it compare to Collibra / Atlan?
Different layer. Collibra/Atlan are governance catalogs. DQX Studio is a quality control plane. They integrate cleanly: Atlan/Collibra reference UC tables; DQX Studio writes quality scores into UC; the catalog renders them.

### 6.4 What about SDP / DLT Expectations?
Expectations are inline with pipelines. DQX rules sit outside the pipeline and validate the *result* — useful when:

- You don't own the pipeline (it's a vendor / partner load)
- You want post-hoc validation independent of pipeline framework
- You want a UI for non-data-engineers to author rules

The two coexist. DLT Expectations are great inside DLT pipelines; DQX Studio is the catch-all for everything else.

---

## 7. Roadmap-ish questions

### 7.1 What's the cross-metastore story?
Today: one DQX Studio per metastore. We're not solving multi-metastore authoring in this version — the trade-off would be moving metadata out of Databricks, which kills the sovereignty story. If a customer needs a federated view across metastores, they roll up `dq_metrics` from each instance into a central Delta share and dashboard on it.

### 7.2 Anomaly / ML quality?
On the roadmap conceptually, not committed. DQX has the metric infrastructure (`dq_metrics` is a long-format event store with `rule_set_fingerprint` provenance), so a Z-score / forecast-residual layer on top is feasible. We'd prefer the customer's appetite to drive that than ship a half-baked anomaly detector.

### 7.3 API surface for external automation?
Already there. Everything the UI does is REST under `/api/v1/*`. The OpenAPI spec is published, the React frontend is generated from it (`orval`), so external scripts get the same typed surface. Customers have used this to scaffold rules from dbt manifests.

### 7.4 Embedded dashboards beyond the default?
The bundle ships a starter Lakeview dashboard pinned to the *Insights* page. Customers customise it in **AI/BI Dashboards** (the iframe picks up changes immediately, no redeploy) or point Insights at a completely different dashboard ID via **Configuration → Insights dashboard**.

---

## 8. Security & compliance

### 8.1 Where's PII?
Two places, both Delta tables in the customer catalog:

- `dq_quarantine_records` — failing rows, by definition contain the raw values that broke the rule
- `dq_profiling_results.sample_invalid_json` — a 10-row sample from profiler/dry-run for UI preview

Both inherit UC's access control. The app filters quarantine reads by the user's UC-accessible catalogs server-side. To hide PII from the UI, restrict the `SELECT` grant on those tables; the app respects that automatically (OBO).

### 8.2 Network egress?
None. The app's outbound calls are:

- Databricks REST API (UC, Jobs, Apps, Secrets) over the workspace's hostname
- Lakebase Postgres (over the workspace VNet)
- That's it. There is no third-party CDN, telemetry endpoint, or vendor cloud.

### 8.3 How are secrets managed?
The task-runner SP's OAuth client secret is the only one. It's stored in Databricks Secret Scopes, injected into the job's environment, never logged. The bundle's `make app-deploy` flow walks the deployer through minting it (or reuses an existing one).

### 8.4 Compliance certifications?
DQX Studio inherits the workspace's certifications — there is no separate compliance scope because there is no separate plane. If your workspace is HIPAA / FedRAMP / etc., DQX Studio inherits those controls because every byte lives inside it.

---

## 9. The customer's likely follow-ups

Pre-empt these — we've heard them:

- **"Can we run this in our own VPC?"** — Yes, automatically. It's a Databricks App in your workspace.
- **"Does it work with Unity Catalog Federation?"** — UC Federation tables (foreign tables to Snowflake/BigQuery/Postgres) show up in the catalog tree and can be authored against. Performance depends on the federation source, not on DQX.
- **"Can rules be promoted across environments (dev → prod)?"** — Today: export YAML from one deployment, commit, import to another. We don't have an automated promotion pipeline in the UI yet.
- **"What if Lakebase isn't enabled in our workspace?"** — Set `lakebase_enabled: false` (or leave `lakebase_instance_name` empty). Migrations fall back to Delta automatically. The app still works.
- **"Why a service principal instead of just our personal token?"** — Scheduled runs at 3 AM can't carry a user token. We need a stable identity. The OBO model still ensures *interactive* requests use the user.

---

## 10. Things to demo (in order)

If the customer wants a 20-minute walkthrough:

1. **Profiler** — point at one of their actual tables, get suggestions in 30 seconds
2. **Promote → Author → Dry run** — go from suggestion to approved rule with one round-trip
3. **Schedule** — set it to run hourly; show the run hit immediately
4. **Run detail** — sample bad rows, comments, review status dropdown, label by team
5. **Insights dashboard** — the embedded AI/BI iframe, then jump out to AI/BI and edit a tile to show the live update path
6. **Configuration page** — show how every operational knob (labels, statuses, custom metrics, retention, role mappings, dashboard ID) is admin-configurable without redeploy

---

## 11. Soda comparison — deep dive

Most customers evaluating both have already piloted Soda Core / Soda Cloud and want to know: *what changes if we switch?* This section is structured around the questions Soda users actually ask. **It's written to be honest about where Soda wins**, because the customer has almost certainly already read Soda's marketing and will catch handwaving immediately.

### 11.1 What is Soda, briefly, so we're talking about the same thing?
Three products under one brand:

- **Soda Core** (OSS) — Python library + CLI; you author checks in `SodaCL` (their YAML DSL) and run `soda scan` against a data source. Results print or POST to Soda Cloud.
- **Soda Library** (commercial) — the same engine plus the connector to Soda Cloud and additional check types (reconciliation, anomaly detection, MLOps).
- **Soda Cloud** (SaaS) — the web UI: datasets, incidents, alerts, anomaly dashboards, Slack/PagerDuty integrations, collaboration features.

Soda's compute model: the library translates `SodaCL` into SQL, runs it against your warehouse (Databricks via JDBC is one of ~15 connectors), and ships the **metric values + failing-row samples** back to Soda Cloud. Your raw data stays in the warehouse; the *results* live in their SaaS.

### 11.2 What does the customer actually gain by picking Soda over DQX Studio?
Stated up front, because it's real:

1. **Multi-source out of the box.** ~15 connectors (Snowflake, BigQuery, Redshift, Postgres, MSSQL, Spark, Databricks, …). If the customer has a heterogeneous stack — Snowflake + Databricks + Postgres — Soda monitors all three from one UI. DQX Studio is Databricks-only.
2. **Anomaly detection** (Sherlock algorithm). Soda Cloud auto-detects "row count dropped 30 % vs the last 14 days" without you writing a rule. DQX is rule-driven; you'd need to write the equivalent expectation manually (and tune it).
3. **Incident management.** Soda Cloud has first-class incidents — assign to a person, link to a Slack thread, mark resolved, see MTTR over time. DQX has comments + review status, which is enough for triage but isn't an incident workflow.
4. **Alerting.** Soda Cloud routes check failures to Slack / Teams / PagerDuty / email / webhooks. DQX has scheduled runs and a dashboard; **there is no built-in alerting today** — you'd build it from `dq_validation_runs` (e.g. a SQL alert on a Databricks dashboard, or a downstream Lakeflow job). Honest gap.
5. **Time-tested at scale.** Soda has been shipping since ~2020 with a dedicated team. DQX is younger and field-maintained (Databricks Labs).

If the customer needs any of those five things, **Soda is the better choice** and we should say so. The next question (11.3) is where DQX usually wins back.

### 11.3 What does the customer gain by picking DQX Studio?

1. **No data ever leaves the workspace.** Soda Cloud is SaaS — even with the on-prem Soda Agent, metric values and failed-row samples travel to their cloud. DQX Studio is *entirely* in the customer's Databricks account; quarantine rows land in their UC, OLTP state in their Lakebase, no telemetry phones home. For regulated industries (banking, healthcare, public sector) this is often the decisive factor.
2. **OBO permission enforcement.** A user authoring or dry-running a rule against table X can only do it if **they** have `SELECT` on X. Soda runs every scan as the **connection identity** — typically one service account with broad SELECT. DQX is fine-grained; Soda is coarse.
3. **PySpark-native execution.** DQX checks run as Spark jobs in the customer's serverless compute, parallelised, with native Delta predicates. Soda Library translates checks into SQL strings shipped over JDBC — for a billion-row table this is slower and more expensive than a serverless Spark scan, especially for distinct-count / referential-integrity / cross-table checks.
4. **Quarantine is a Delta table.** When a DQX check fails, the offending rows are persisted to `dq_quarantine_records` in UC. Downstream teams can `JOIN dq_quarantine_records WITH the original source` in plain SQL to investigate, build remediation pipelines, feed bug-tracker tickets — anything you'd do with a normal Delta table. Soda stores **samples** (default 100 rows) in Soda Cloud; the full failing set is not retained.
5. **Cost.** DQX Studio is open source — the customer pays Databricks Apps + serverless job + Lakebase, all of which they already have on their bill. Soda Cloud is per-dataset / per-check / per-seat pricing on top of the warehouse compute they pay Soda's scan to consume. For Databricks-heavy estates this difference is material.
6. **Integration with the surrounding stack.** DQX writes to UC; UC feeds AI/BI dashboards; rule metadata sits next to lineage; access is governed by the same UC permissions every other tool respects. Soda is an island that integrates *with* the warehouse, not *as* part of it.
7. **UC-native authoring UX.** Browsing catalogs/schemas/tables in DQX Studio uses live UC reads as the logged-in user, so authors see *exactly* what they have access to. Soda's UI builds against the connections an admin configured — there's a layer of translation between "what's in the warehouse" and "what's in Soda."

### 11.4 Direct feature-by-feature
The table the customer is going to ask for, side by side:

| Capability | Soda Cloud + Library | DQX Studio |
|---|---|---|
| Rule DSL | SodaCL (YAML) | DQX YAML (compatible with [`databrickslabs/dqx`](https://github.com/databrickslabs/dqx)) |
| UI for rule authoring | Yes (Soda Cloud) | Yes (form + YAML modes in app) |
| Profiler / suggested rules | Yes (column profile) | Yes (`dq_profiling_results`) |
| Cross-table SQL rules | `failed_rows_query` | Native `sql` check type, persisted to `dq_quarantine_records` |
| Anomaly detection | Yes (Sherlock, on metric history) | **No** (rule-driven only today) |
| Reconciliation checks | Yes (Soda Library) | Yes via cross-table SQL |
| Schedules | Soda Agent + cron, or CI/CD | In-process scheduler (cron/interval) + serverless job |
| Approval workflow (author → approver) | No formal split | Yes (`RULE_AUTHOR` / `RULE_APPROVER` roles) |
| Run history | Yes (Soda Cloud retention) | Yes (Delta — retention is whatever you set, default unbounded) |
| Quarantine of failing rows | Samples (default 100 in Soda Cloud) | Full set in Delta UC table (capped at 100k for SQL checks) |
| Comments / review workflow | Incidents (first-class) | Comments + per-run review status with audit |
| Alerts / notifications | Slack / Teams / PagerDuty / email / webhook | **None built-in** (use Databricks SQL Alerts on `dq_validation_runs`) |
| Dashboards | Soda Cloud built-in | Embedded Lakeview/AI-BI; fully customisable |
| Multi-source | ~15 warehouses | Databricks (UC) only |
| Identity for scans | Single connection identity (typically a SP) | OBO per-user for interactive; SP for scheduled |
| Data residency | Soda Cloud SaaS (or Agent if licensed) | 100 % customer Databricks account |
| Audit trail | Soda Cloud audit | Distributed: `*_history` tables + UC + Jobs |
| Cost model | Per-dataset/check/seat (SaaS) + warehouse compute | Databricks compute only (open source app) |
| Open source | Soda Core only (limited) | Yes, all of it |
| Cross-workspace / cross-metastore | Yes (single Soda Cloud over many connections) | No — one instance per metastore |
| API / programmatic surface | Soda Cloud API + Python | OpenAPI under `/api/v1/*`, typed React client generated |

### 11.5 "Can we migrate rules from Soda to DQX?"
**Mostly yes, with rewrites — there is no one-shot importer.** SodaCL and DQX YAML are structurally similar (both are check-list YAML rooted at a table) but the check function names and parameters differ. A typical migration path:

1. Export the SodaCL checks for the Databricks-targeted datasets.
2. For each check, map it: `missing_count = 0` → DQX `is_not_null`, `invalid_count(col) using regex … = 0` → DQX `regex_match`, `failed_rows_query` → DQX `sql` check. Most map 1:1.
3. The non-mappable checks are exactly the ones DQX doesn't have today: anomaly detection, change-detection-vs-history. Those need a manual replacement (write the rule explicitly, or accept it as a gap).
4. Bulk-import the rewritten YAML through the DQX Studio rule API or the rules UI's YAML mode.

A field engineer can usually rewrite ~50 SodaCL checks in an afternoon if they're standard shapes. For very large estates we'd consider scripting it, but there's no productised tool today.

### 11.6 "Can we run both side-by-side during the migration?"
Yes, with no conflicts. Soda reads through JDBC; DQX runs Spark jobs. They don't share state or coordinate on rule definitions, so there's nothing to deconflict — the only consideration is double-billed warehouse / serverless compute during the overlap period. We recommend running both for one to two weeks against the same critical tables to verify equivalent rule outcomes before turning Soda off.

### 11.7 "Soda has anomaly detection. Is the lack of it a blocker?"
Honest answer: **it depends on what they're using it for**.

- If they rely on anomaly detection as the *primary* DQ signal ("we don't write rules, we let Soda find weirdness"), DQX is not a like-for-like swap today. They should keep an anomaly tool — Soda, Monte Carlo, Anomalo — alongside DQX.
- If they use anomaly detection as a *safety net* on top of explicit rules ("our rules catch 80 %, Sherlock catches the 20 % we forgot"), they can get most of that 20 % through good rule discipline + the DQX profiler. The metric infrastructure in DQX (`dq_metrics` with `rule_set_fingerprint` provenance) is anomaly-detection-ready — a Z-score or forecast-residual layer on top is feasible as a customer-side notebook today.

The roadmap honest answer: anomaly detection is conceptually on our roadmap, not committed. We'd rather not promise a date.

### 11.8 "Soda has alerts to Slack. We need that."
Real gap. The pragmatic answer in Databricks today:

- **`dq_validation_runs`** is a normal Delta table. Run a **Databricks SQL Alert** on `SELECT count(*) FROM dq_validation_runs WHERE status = 'FAILED' AND created_at > current_timestamp() - INTERVAL 1 HOUR` and route it to Slack/Teams/email via the standard SQL Alerts destination plumbing.
- Same for "run with > N error rows": `SELECT … WHERE error_rows > <threshold>`.
- For richer routing (per-table owner, per-severity), a Lakeflow job downstream of `dq_metrics` is a few hours of work.

Native alerting *inside* DQX Studio is a fair future ask; right now it's "use the platform's alerting layer."

### 11.9 "Soda Cloud has incidents — do you?"
DQX Studio has the two pieces that matter for triage:

- **Comments** on every run (a `dq_comments` thread, just like comments on a rule)
- **Review status** per run with an audit trail (the new dropdown we added — see [§3.6](#36-whats-review-status))

That covers "acknowledge / under investigation / resolved / false positive" with attribution and history. What it does not cover is:

- Auto-creating an external ticket (Jira / ServiceNow) — that's a webhook we don't have today.
- A separate incident object that aggregates multiple runs of the same failing rule into one open issue.

If the customer's DQ model is incident-driven (an on-call rotation triages failures across many checks), Soda has a more polished surface today. If their model is rule-owner-driven ("the team that owns the rule fixes the rule"), DQX is sufficient.

### 11.10 "What about Soda Agent — they say data doesn't leave our network either?"
The Soda Agent is a containerised executor a customer hosts in their own infrastructure to run scans without the data passing through Soda's cloud. **The check definitions, metric results, and failed-row samples still travel to Soda Cloud** — that's how the UI works. The Agent improves the data path; it does not eliminate the SaaS plane.

DQX Studio has no SaaS plane at all. Every byte — definitions, results, samples, audit logs — lives in the customer's Databricks account.

### 11.11 "Pricing — how should we model the comparison?"
Approximate, not a quote:

- **Soda Cloud**: per-dataset, per-check, or per-seat depending on the contract. Plus the Databricks warehouse compute that runs the scans.
- **DQX Studio**: Databricks Apps (small flat hourly), one small Lakebase instance, serverless job billed only when validating. All on the customer's existing Databricks bill.

The honest framing: for a small estate (~50 monitored tables), Soda Cloud is often cost-comparable to DQX. For a large estate (hundreds to thousands of tables), DQX scales sub-linearly because there's no per-dataset SaaS fee — only compute, which the customer already accepts. Get the customer to share approximate counts before quoting.

### 11.12 "Is Soda integration with DQX possible?"
There's no first-party integration. If a customer really wants both — Soda's anomaly detection + alerts plus DQX's UC integration and OBO authoring — the practical pattern is:

- Run DQX as the authoring + execution layer (its strength)
- Use Soda Cloud against the **same source tables** with the anomaly-detection check types only (its strength)
- Don't double-write the explicit rules — pick one tool per rule

This is unusual but real for customers who can absorb both bills and want best-of-both. Most pick one.

### 11.13 The two-line elevator answer
If the customer asks you to summarise the whole comparison in two sentences:

> **Soda is the right answer if you need multi-source coverage, anomaly detection, or built-in alerting today — and you're comfortable with metadata in their SaaS plane.** **DQX Studio is the right answer if you're Databricks-centric, want every byte to stay in your account, want OBO-level permission enforcement, and prefer rule-driven over anomaly-driven DQ.**

Most Databricks-all-in customers land on DQX. Most heterogeneous customers (Snowflake + Databricks + Postgres) land on Soda. Neither answer is wrong.
