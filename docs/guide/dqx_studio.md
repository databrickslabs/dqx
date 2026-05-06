# DQX Studio (No-Code UI)

**DQX Studio** is the web application for authoring, reviewing, running, and monitoring data quality rules without writing code. It runs as a [Databricks App](https://docs.databricks.com/en/dev-tools/databricks-apps/index.html) inside your workspace and exposes all the core DQX capabilities — profiling, rule authoring, approval workflows, scheduled runs, and results tracking — through a browser.

When to use DQX Studio

DQX Studio is the recommended entry point for:

* Data stewards and analysts who want to manage and run quality rules without code-level integration (see [post-factum monitoring](/dqx/docs/motivation.md#post-factum-monitoring)).
* Teams that need an approval workflow before rules go live.
* Any user who prefers to browse Unity Catalog and author checks through a UI.

For code-level integration into pipelines or notebooks, see the [Programmatic approach](/dqx/docs/guide/quality_checks_apply.md#programmatic-approach).

## Key concepts[​](#key-concepts "Direct link to Key concepts")

Before opening DQX Studio, it helps to understand a few terms used across the UI.

| Term            | Meaning                                                                                                                                                                  |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Rule**        | A single data quality check — a named condition evaluated against a single or multiple tables and columns. Rules have a severity (error or warning) and optional labels. |
| **Draft**       | A rule that has been authored but not yet approved. Drafts are editable and do not run on schedules.                                                                     |
| **Active rule** | An approved rule. Only active rules are executed by runs and schedules.                                                                                                  |
| **Run**         | A single execution of one or more active rules against their target tables. Runs produce pass/fail results and optional metrics.                                         |
| **Schedule**    | A cadence (e.g. hourly, daily, weekly) that automatically submits runs for a set of rules.                                                                               |
| **Label**       | A free-form tag (such as `pii`, `gold`, `finance`) attached to rules for organisation and filtering.                                                                     |

## Accessing DQX Studio[​](#accessing-dqx-studio "Direct link to Accessing DQX Studio")

Your workspace administrator deploys DQX Studio and shares its URL with you. Once deployed, open it in a browser — authentication uses your Databricks identity, so no separate login is required.

If DQX Studio isn't available in your workspace yet, ask your admin to follow the [installation instructions](/dqx/docs/installation.md#dqx-studio-installation).

What you can see is what you can access

DQX Studio browses Unity Catalog using your own identity (on-behalf-of). You will only see catalogs, schemas, tables, and columns that you have been granted access to. Profiling and dry-runs are also executed on your behalf, ensuring the studio can never read data from tables you don’t have permission to access.

## Roles and permissions[​](#roles-and-permissions "Direct link to Roles and permissions")

Every user has a **primary role** that governs what they can do, plus an optional **runner** privilege that is independent of the primary role.

| Primary role           | Typical audience          | Can do                                                                        |
| ---------------------- | ------------------------- | ----------------------------------------------------------------------------- |
| **Viewer** *(default)* | Anyone with access        | Browse active rules, drafts, runs, and run history. Read-only.                |
| **Rule author**        | Data engineers, analysts  | Everything a viewer can, plus create, edit, and delete their own draft rules. |
| **Rule approver**      | Tech leads, data stewards | Everything an author can, plus approve or reject drafts from anyone.          |
| **Admin**              | Platform owners           | Everything, plus manage role mappings, labels, timezone, and studio settings. |

The **Runner** privilege is additive — it adds the ability to trigger manual runs and manage schedules on top of any primary role. Admins implicitly have it; other users need their admin to grant it.

Figuring out your role

Open the **Profile** page (top-right user menu) at any time to see your primary role, whether the Runner privilege is granted, and which groups you belong to that mapped you to those roles.

## The home page[​](#the-home-page "Direct link to The home page")

The landing page is a quick overview with two primary calls to action:

* **Get Started** — jumps into the rule creation flow.
* **View Rules** — opens the active rules library.

Use the sidebar to navigate between functional areas. Menu items hide automatically if your role does not grant access to them (for example, **Run Rules** only appears if you have the Runner privilege).

## Creating rules[​](#creating-rules "Direct link to Creating rules")

Expand **Create Rules** in the sidebar to pick the authoring flow that best matches what you want to express.

### Single-table rules[​](#single-table-rules "Direct link to Single-table rules")

Use this flow when your check operates on the columns of one table — for example, "this column must not be null", "this column must be unique", or "values must be within a list".

1. Browse Unity Catalog and pick a target table.
2. Choose a column (or leave it empty for whole-row or custom checks).
3. Pick a check from the catalog of built-in DQX checks (such as `is_not_null`, `is_in_list`, `is_unique`, `regex_match`, `is_in_range`).
4. Fill in any arguments the check needs.
5. Set the severity — **error** (fails the run) or **warning** (reports but doesn't fail).
6. Add labels if you want to group the rule (for example `pii`, `finance`, `critical`).
7. Save the rule as a draft.

For a complete list of built-in checks and their arguments, see [Quality Checks Reference](/dqx/docs/reference/quality_checks.md).

### Cross-table rules[​](#cross-table-rules "Direct link to Cross-table rules")

Use this flow when a check needs to compare data across multiple tables — for example, referential integrity ("every `order_id` must exist in `orders`"), reconciliation ("sum of `amount` in `payments` equals total in `invoices`"), or any custom SQL condition.

1. Give the rule a name and description.
2. Write a SQL query that returns rows violating the check (or zero rows when everything is healthy).
3. Set severity and labels.
4. Save the rule as a draft.

Cross-table SQL tips

* The SQL is executed on your behalf using your Databricks identity — it can reference any tables you have access to.
* Keep checks focused: one rule per invariant is easier to triage when it fails.
* Prefer fully qualified table names (`catalog.schema.table`) for clarity.

### Profile and generate[​](#profile-and-generate "Direct link to Profile and generate")

If you're starting from scratch and don't yet know what rules to write, profiling a table is the fastest way to get a draft set.

1. Pick a catalog, schema, and table in the browser.
2. Click **Run profiler**.
3. Wait for the job to complete — profiling runs as a serverless Databricks Job and uses your own table permissions, so no elevated access is required.
4. Review the suggested rules. The profiler produces candidate checks based on actual data patterns (null rates, uniqueness, value ranges, regex fits, and more).
5. Keep the ones you like and discard the rest. Everything you keep is saved as drafts for review.

See [Data Profiling](/dqx/docs/guide/data_profiling.md) for the full list of statistics and candidate check types the profiler produces.

AI-assisted generation

DQX Studio also supports AI-assisted rule suggestions — you describe the business context in natural language and the studio proposes relevant checks. See [AI-Assisted Generation](/dqx/docs/guide/ai_assisted_quality_checks_generation.md) for the underlying capabilities.

### Import rules[​](#import-rules "Direct link to Import rules")

If you already maintain rules in YAML or JSON, the **Import rules** flow lets you paste them in or upload a file. Imported rules land as drafts so you can review them before they go live.

The accepted formats are the same as the programmatic storage formats — see [Quality Checks Definition](/dqx/docs/guide/quality_checks_definition.md) for the schema.

### Dry-run before committing[​](#dry-run-before-committing "Direct link to Dry-run before committing")

While authoring, each rule supports a **dry run** that evaluates the check against a small sample of the target table without persisting results. Use it to sanity-check that your rule behaves as expected before submitting it for approval.

## Reviewing and approving drafts[​](#reviewing-and-approving-drafts "Direct link to Reviewing and approving drafts")

Drafts live on the **Drafts & Review** page. The workflow depends on your role:

* **Authors** see their drafts and can edit, delete, or submit them for approval. They cannot approve their own drafts unless they are admins or approvers.
* **Approvers and admins** see all drafts across the workspace. They can approve or reject each one with an optional comment.

Approved drafts disappear from this page and show up on **Active Rules**. Rejected drafts remain visible so the author can edit and resubmit.

Four-eyes principle

By design, a draft author who does not also hold approval rights cannot approve their own draft. This enforces a minimum of two people touching every rule before it becomes active — reducing accidental outages caused by a single contributor. Admins and approvers, who already hold approval rights for the workspace, are exempt and can approve drafts they authored themselves.

## Browsing active rules[​](#browsing-active-rules "Direct link to Browsing active rules")

The **Active Rules** page lists every approved rule grouped by target table (or under **Cross-table rules** for SQL checks). From here you can:

* **Filter** by catalog, schema, table, or label.
* **Expand** any rule to see its definition, labels, author, approval history, and comments.
* **Download** one or more rules as YAML — useful for version control or sharing with another environment.
* **Delete** a rule (admins and approvers only). Deleted rules are removed from all schedules.

## Running rules[​](#running-rules "Direct link to Running rules")

DQX Studio executes rules in two ways:

* **Manual runs** — you trigger a one-off run on demand.
* **Scheduled runs** — DQX Studio triggers runs automatically on a cadence you configure.

Both are managed from the **Run Rules** page, which is visible to users with the Runner privilege (admins are implicit runners).

### Manual runs[​](#manual-runs "Direct link to Manual runs")

1. Open the **Run Rules** page.
2. Pick the rules you want to run — filter by table or labels to select a coherent set.
3. Choose the target tables (if a rule applies to multiple) and any per-run parameters.
4. Click **Run** to submit. DQX Studio creates a Databricks Job run and shows you a progress indicator.

You can leave the page while a run is in flight — it will continue in the background and surface in **Runs History** when it finishes.

### Scheduled runs[​](#scheduled-runs "Direct link to Scheduled runs")

Users with `Runner` privilege can put any set of active rules on a schedule so they execute automatically.

1. From **Run Rules**, click **New schedule**.
2. Pick the rules and targets.
3. Choose a cadence — hourly, daily at a specific time, weekly on selected days, or a custom cron expression.
4. Save. DQX Studio now triggers runs automatically on that cadence.

Schedules survive restarts and deduplicate — if a scheduled run is still in flight when the next tick arrives, the studio will not double-submit.

Timezones

All schedule times are interpreted in the timezone configured on the **Configuration** page. Ask your admin to set this to your team's primary timezone before creating schedules.

#### Pausing and editing schedules[​](#pausing-and-editing-schedules "Direct link to Pausing and editing schedules")

Any schedule can be paused (without losing its configuration) and resumed later. Editing a schedule immediately reschedules the next run — there is no need to delete and recreate.

## Runs history[​](#runs-history "Direct link to Runs history")

The **Runs History** page is read-only and visible to everyone. It shows every run DQX Studio has executed, sorted by most recent first.

For each run you can see:

* When it started and how long it took.
* Whether it was triggered manually or by a schedule (and which schedule).
* Which rules it covered.
* Pass, warning, and error counts per rule.
* A link to drill into individual row-level failures.

Use this view to spot regressions, audit rule behaviour over time, and share results with stakeholders. For aggregated, cross-table monitoring of pass/fail trends and quality KPIs, complement Runs History with the [Quality Dashboard](/dqx/docs/guide/quality_dashboard.md).

## Configuration (admin only)[​](#configuration-admin-only "Direct link to Configuration (admin only)")

The **Configuration** page is where admins manage DQX Studio-level settings that affect all DQX Studio users.

| Setting           | What it controls                                                                      |
| ----------------- | ------------------------------------------------------------------------------------- |
| **Timezone**      | The timezone used to interpret and display all schedule times and run timestamps.     |
| **Labels**        | Labels users can attach to rules. Defining labels centrally keeps tagging consistent. |
| **Role mappings** | Which workspace groups map to which DQX roles (author, approver, runner, admin).      |

Changes take effect immediately for all users — no restart is required.

## Getting help[​](#getting-help "Direct link to Getting help")

* **Docs**: you are reading them. Start at the [User Guide](/dqx/docs/guide.md) or [Quality Checks Reference](/dqx/docs/reference/quality_checks.md) when you need to know what a specific check does.
* **Issues and discussions**: file an issue or ask a question on [GitHub](https://github.com/databrickslabs/dqx).
* **Contributing**: want to fix something or suggest a feature? See the [contributing guide](/dqx/docs/dev/contributing.md).
