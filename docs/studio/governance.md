# Governance & security

This section is for governance admins accountable for *how* DQX Studio is run. It covers configuring the app once it's installed.

If you're setting DQX Studio up for the first time, the rollout playbook below walks you through it. For the day-to-day detail behind each step, follow the links into the rest of this section.

## Configure DQX Studio[​](#configure-dqx-studio "Direct link to Configure DQX Studio")

You set a handful of org-wide policies once — how roles map to your teams, your approval rules, your shared standards (governed tags, quality dimensions, default thresholds), and data retention. From then on, every author, approver, and steward works within them.

You'll need the Admin role

Everything below lives under **Admin Settings** and is only available to people with the **Admin** role. If you can't see Admin Settings, ask an existing admin.

### Step 1 — Map roles to your teams[​](#step-1--map-roles-to-your-teams "Direct link to Step 1 — Map roles to your teams")

Map your existing Databricks **workspace groups** to DQX Studio's four roles — Viewer, Rule Author, Rule Approver, Admin. Running checks is part of the Author role, so authors can run what they write; approvers review rather than run. Assign groups rather than individuals, so access follows people as they join and leave. Start narrow: most people as Viewer or Author, a small group as Approver and Admin.

![Admin Settings — Entitlements tab: workspace groups mapped to app roles](/dqx/img/studio/settings_entitlements_light.png)![Admin Settings — Entitlements tab: workspace groups mapped to app roles](/dqx/img/studio/settings_entitlements_dark.png)

Full detail: [Permissions & entitlements](/dqx/docs/studio/governance/permissions-and-entitlements.md).

### Step 2 — Decide your approval policy[​](#step-2--decide-your-approval-policy "Direct link to Step 2 — Decide your approval policy")

By default DQX Studio uses **four-eyes approval**: an author submits a draft and someone else approves it before it goes live. Keep this on for production; for a sandbox you can allow auto-approval. The setting is itself audited.

![Admin Settings — Governance tab: approvals mode and data retention](/dqx/img/studio/settings_governance_thumb_light.png)![Admin Settings — Governance tab: approvals mode and data retention](/dqx/img/studio/settings_governance_thumb_dark.png)

Full detail: [The approval workflow](/dqx/docs/studio/governance/approval-workflow.md).

### Step 3 — Define your standards[​](#step-3--define-your-standards "Direct link to Step 3 — Define your standards")

Set the shared vocabulary authors will reuse:

* **Governed tags** — reusable labels such as `domain`, `pii`, or `class.location`. These are defined in **Unity Catalog**, not in DQX Studio, and authors pick from them (or create their own in Unity Catalog) when tagging a rule's column. A tagged rule then auto-applies to matching columns. See [Governed tags](/dqx/docs/studio/authoring/create-a-rule.md#governed-tags).
* **Quality dimensions** — the categories you report against (completeness, validity, accuracy, consistency, uniqueness, timeliness).
* **Default pass thresholds** — how strict a check must be to count as passing. Set a sensible default; authors can override per table. See [Set quality thresholds](/dqx/docs/studio/monitoring/assign-rules.md#quality-thresholds).

### Step 4 — Set retention and handle sensitive data[​](#step-4--set-retention-and-handle-sensitive-data "Direct link to Step 4 — Set retention and handle sensitive data")

In Admin Settings you choose how long runs, results, and metrics are kept, and — separately — how long quarantined rows (which may contain sensitive data) are kept. Keep the evidence long enough to investigate, then let it clear to minimize the app data.

Now invite your team

With roles, approvals, and standards in place, point authors at the [Quickstart](/dqx/docs/studio/quickstart.md) and stewards at [Review results](/dqx/docs/studio/running.md).

## How DQX Studio respects your data access[​](#how-dqx-studio-respects-your-data-access "Direct link to How DQX Studio respects your data access")

DQX Studio browses Unity Catalog **on your behalf** — you only ever see catalogs and tables you already have permission to, and profiling and tests run under your own access. It never widens anyone's permissions. Quarantined rows are visible only to approvers and admins, and still only for data they can already read.

Every change to a rule, role, permission, or schedule is **audited** — who changed it, when, and the before-and-after values. Review a rule's history on its **History** tab (and a collection's on the collection's **History** tab); the complete trail is kept in append-only history tables in the app's own catalog.

## In this section[​](#in-this-section "Direct link to In this section")

* [Permissions & entitlements](/dqx/docs/studio/governance/permissions-and-entitlements.md) — roles across the app, and per-object access.
* [The approval workflow](/dqx/docs/studio/governance/approval-workflow.md) — draft → review → approve, and the four-eyes rule.
* [Try the built-in demo](/dqx/docs/demos.md#dqx-studio) — deploy sample content from Admin Settings to explore every feature.
